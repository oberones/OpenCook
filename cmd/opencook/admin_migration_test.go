package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/version"
)

func TestAdminMigrationPreflightReportsDependenciesAndRedactsFailures(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			PostgresDSN:          "postgres://user:supersecret@postgres.example/opencook",
			BlobBackend:          "s3",
			BlobS3Endpoint:       "https://minio-secret.example",
			BlobS3Region:         "us-test-1",
			BlobS3AccessKeyID:    "access-secret",
			BlobS3SecretKey:      "secret-secret",
			BlobS3SessionToken:   "token-secret",
			BlobS3RequestTimeout: config.DefaultBlobS3RequestTimeout,
			BlobS3MaxRetries:     config.DefaultBlobS3MaxRetries,
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return nil, nil, errors.New("postgres failure with supersecret")
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--all-orgs", "--json", "--with-timing"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration preflight) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["ok"] != false {
		t.Fatalf("ok = %v, want false in output %v", out["ok"], out)
	}
	if out["command"] != "migration_preflight" {
		t.Fatalf("command = %v, want migration_preflight", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["all_organizations"] != true {
		t.Fatalf("target.all_organizations = %v, want true", target["all_organizations"])
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "postgres", "error")
	requireAdminMigrationDependency(t, deps, "blob", "error")
	requireAdminMigrationDependency(t, deps, "opensearch", "unconfigured")
	inventory := requireAdminMigrationMap(t, out, "inventory")
	requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationArray(t, out, "planned_mutations")
	errors := requireAdminMigrationArray(t, out, "errors")
	if len(errors) < 2 {
		t.Fatalf("errors = %v, want dependency errors", errors)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("output missing duration_ms: %v", out)
	}

	for _, secret := range []string{"supersecret", "minio-secret", "access-secret", "secret-secret", "token-secret"} {
		if strings.Contains(stdout.String(), secret) || strings.Contains(stderr.String(), secret) {
			t.Fatalf("migration preflight leaked secret %q; stdout = %s stderr = %s", secret, stdout.String(), stderr.String())
		}
	}
}

func TestAdminMigrationPreflightSucceedsWithPostgresBlobAndOpenSearch(t *testing.T) {
	openSearch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch","number":"2.12.0"},"tagline":"OpenSearch"}`))
		case r.Method == http.MethodHead && r.URL.Path == "/chef":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Fatalf("unexpected OpenSearch request %s %s", r.Method, r.URL.String())
		}
	}))
	defer openSearch.Close()

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		if raw != openSearch.URL {
			t.Fatalf("opensearch URL = %q, want %s", raw, openSearch.URL)
		}
		return newFakeAdminSearchTarget("ponyville/client/ponyville-validator", "ponyville/environment/_default"), nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			OpenSearchURL:           openSearch.URL,
			BlobBackend:             "filesystem",
			BlobStorageURL:          filepath.Join(t.TempDir(), "blobs"),
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	var closed bool
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://opencook" {
			t.Fatalf("postgres dsn = %q, want postgres://opencook", dsn)
		}
		bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
		return &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState}, func() error {
			closed = true
			return nil
		}, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--org", "ponyville", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(migration preflight healthy) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if !closed {
		t.Fatal("migration preflight did not close offline store")
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["ok"] != true {
		t.Fatalf("ok = %v, want true in output %v", out["ok"], out)
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["organization"] != "ponyville" {
		t.Fatalf("target.organization = %v, want ponyville", target["organization"])
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	for _, name := range []string{"postgres", "postgres_bootstrap_core", "postgres_core_objects", "postgres_cookbooks", "blob", "opensearch", "runtime_config"} {
		requireAdminMigrationDependency(t, deps, name, "ok")
	}
	requireAdminMigrationDependency(t, deps, "opensearch_consistency", "ok")
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "clients", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "client_keys", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "groups", 4)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "containers", 12)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_expected_documents", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_missing_documents", 0)
	warnings := requireAdminMigrationArray(t, out, "warnings")
	if !adminMigrationArrayContainsString(warnings, "deferred") {
		t.Fatalf("warnings = %v, want deferred source-family warning", warnings)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("output missing duration_ms: %v", out)
	}
}

func TestAdminMigrationPreflightReportsPostgresInventoryAndConsistencyFindings(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "memory",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore: &fakeOfflineStore{
				bootstrap: adminOfflineTestState(),
				objects: bootstrap.CoreObjectState{Orgs: map[string]bootstrap.CoreObjectOrganizationState{
					"ponyville": {
						Environments: map[string]bootstrap.Environment{
							"prod": {Name: "prod"},
						},
						Nodes: map[string]bootstrap.Node{
							"web01": {Name: "web01"},
						},
						Roles: map[string]bootstrap.Role{
							"web": {Name: "web"},
						},
						DataBags: map[string]bootstrap.DataBag{
							"secrets": {Name: "secrets"},
						},
						DataBagItems: map[string]map[string]bootstrap.DataBagItem{
							"secrets": {"db": {ID: "db"}},
						},
						Sandboxes: map[string]bootstrap.Sandbox{
							"sandbox1": {ID: "sandbox1", Checksums: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
						},
						Policies: map[string]map[string]bootstrap.PolicyRevision{
							"app": {"rev1": {Name: "app", RevisionID: "rev1"}},
						},
						PolicyGroups: map[string]bootstrap.PolicyGroup{
							"prod": {Name: "prod", Policies: map[string]string{"app": "rev1"}},
						},
						ACLs: map[string]authz.ACL{
							"environment:prod":  {},
							"data_bag:secrets":  {},
							"policy:app":        {},
							"policy_group:prod": {},
						},
					},
				}},
			},
			cookbookInventory: map[string]adminMigrationCookbookInventory{
				"ponyville": {Versions: 2, Artifacts: 1, ChecksumReferences: 3},
			},
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--org", "ponyville", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration preflight inventory) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "clients", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "nodes", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "data_bag_items", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "policy_assignments", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "checksum_references", 4)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "cookbook_versions", 2)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "cookbook_artifacts", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "missing_default_container")
	requireAdminMigrationFinding(t, findings, "missing_validator_client")
	requireAdminMigrationFinding(t, findings, "missing_object_acl")
	requireAdminMigrationFinding(t, findings, "unsupported_source_families_deferred")
}

func TestAdminMigrationPreflightValidatesFilesystemBlobReferences(t *testing.T) {
	root := t.TempDir()
	reachable := writeAdminMigrationTestBlob(t, root, "rainbow")
	_ = writeAdminMigrationTestBlob(t, root, "orphan")
	missingSandbox := "11111111111111111111111111111111"
	missingCookbook := "22222222222222222222222222222222"

	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	orgObjects := coreObjectState.Orgs["ponyville"]
	orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox1": {ID: "sandbox1", Checksums: []string{reachable, missingSandbox}},
	}
	coreObjectState.Orgs["ponyville"] = orgObjects

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "filesystem",
			BlobStorageURL:          root,
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore: &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
			cookbookInventory: map[string]adminMigrationCookbookInventory{
				"ponyville": {
					Versions:           1,
					Artifacts:          1,
					ChecksumReferences: 2,
					Checksums:          []string{reachable, missingCookbook},
				},
			},
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--org", "ponyville", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration preflight blob validation) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "referenced_blobs", 3)
	requireAdminMigrationInventoryFamily(t, families, "", "reachable_blobs", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "missing_blobs", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "provider_unavailable_checks", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "content_verified_blobs", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "checksum_mismatch_blobs", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "candidate_orphan_blobs", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "missing_blob")
	requireAdminMigrationFinding(t, findings, "candidate_orphan_blobs")
}

func TestAdminMigrationPreflightReportsUnavailableBlobValidation(t *testing.T) {
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	orgObjects := coreObjectState.Orgs["ponyville"]
	orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox1": {ID: "sandbox1", Checksums: []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}},
	}
	coreObjectState.Orgs["ponyville"] = orgObjects

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
			err:    blob.ErrUnavailable,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "filesystem",
			BlobStorageURL:          t.TempDir(),
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--org", "ponyville", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration preflight unavailable blob) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "blob", "error")
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "referenced_blobs", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "provider_unavailable_checks", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "blob_provider_unavailable")
}

func TestAdminMigrationPreflightReportsOpenSearchConsistencyDrift(t *testing.T) {
	openSearch := newAdminMigrationOpenSearchServer(t)
	defer openSearch.Close()

	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		if raw != openSearch.URL {
			t.Fatalf("opensearch URL = %q, want %s", raw, openSearch.URL)
		}
		return newFakeAdminSearchTarget("ponyville/client/ponyville-validator", "ponyville/cookbooks/unindexed"), nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			OpenSearchURL:           openSearch.URL,
			BlobBackend:             "memory",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--org", "ponyville", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration preflight opensearch drift) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "opensearch", "ok")
	requireAdminMigrationDependency(t, deps, "opensearch_consistency", "warning")
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_expected_documents", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_observed_documents", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_missing_documents", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_stale_documents", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_unsupported_scopes", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "opensearch_missing_documents")
	requireAdminMigrationFinding(t, findings, "opensearch_stale_documents")
	requireAdminMigrationFinding(t, findings, "opensearch_unsupported_documents")
	mutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, mutations, "opencook admin search repair --org ponyville --dry-run")
	requireAdminMigrationMutationMessage(t, mutations, "opencook admin search repair --org ponyville --yes")
}

func TestAdminMigrationBackupBundleFormatWritesLogicalPayloads(t *testing.T) {
	root := t.TempDir()
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	checksum := writeAdminMigrationTestBlob(t, t.TempDir(), "rainbow")
	orgObjects := coreObjectState.Orgs["ponyville"]
	orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox1": {ID: "sandbox1", Organization: "ponyville", Checksums: []string{checksum}},
	}
	coreObjectState.Orgs["ponyville"] = orgObjects
	cookbookChecksum := "11111111111111111111111111111111"
	cookbooks := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{
		"ponyville": {
			Versions: []bootstrap.CookbookVersion{{
				Name:         "app",
				CookbookName: "app",
				Version:      "1.2.3",
				AllFiles:     []bootstrap.CookbookFile{{Name: "default.rb", Path: "recipes/default.rb", Checksum: cookbookChecksum, Specificity: "default"}},
			}},
			Artifacts: []bootstrap.CookbookArtifact{{
				Name:       "artifact-app",
				Identifier: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Version:    "1.2.3",
				AllFiles:   []bootstrap.CookbookFile{{Name: "metadata.rb", Path: "metadata.rb", Checksum: cookbookChecksum, Specificity: "default"}},
			}},
		},
	}}
	inventory := adminMigrationInventoryFromState(
		bootstrapState,
		coreObjectState,
		map[string]adminMigrationCookbookInventory{"ponyville": {Versions: 1, Artifacts: 1, Checksums: []string{cookbookChecksum}}},
		"",
	)

	manifest, err := adminMigrationWriteBackupBundle(root, adminMigrationBackupBundleInput{
		Build:       version.Info{Version: "test-version", Commit: "test-commit", BuiltAt: "test-built-at"},
		CreatedAt:   mustParseAdminMigrationTime(t, "2026-04-28T12:00:00Z"),
		Config:      config.Config{PostgresDSN: "postgres://user:secret@example/opencook", BlobS3SecretKey: "secret-key"},
		Bootstrap:   bootstrapState,
		CoreObjects: coreObjectState,
		Cookbooks:   cookbooks,
		Inventory:   inventory,
		Warnings:    []string{"z warning", "a warning"},
	})
	if err != nil {
		t.Fatalf("adminMigrationWriteBackupBundle() error = %v", err)
	}
	if manifest.FormatVersion != adminMigrationBackupFormatVersion {
		t.Fatalf("manifest format = %q, want %q", manifest.FormatVersion, adminMigrationBackupFormatVersion)
	}
	if manifest.CreatedAt != "2026-04-28T12:00:00Z" {
		t.Fatalf("manifest created_at = %q", manifest.CreatedAt)
	}
	for _, path := range []string{
		adminMigrationBackupManifestPath,
		adminMigrationBackupBootstrapPath,
		adminMigrationBackupObjectsPath,
		adminMigrationBackupCookbooksPath,
		adminMigrationBackupBlobsPath,
		adminMigrationBackupRunbookPath,
	} {
		if _, err := os.Stat(filepath.Join(root, path)); err != nil {
			t.Fatalf("bundle missing %s: %v", path, err)
		}
	}

	rawManifest := readAdminMigrationTestFile(t, root, adminMigrationBackupManifestPath)
	for _, forbidden := range []string{"user:secret@example", "secret-key"} {
		if strings.Contains(string(rawManifest), forbidden) {
			t.Fatalf("manifest leaked secret material %q: %s", forbidden, string(rawManifest))
		}
	}
	var persisted adminMigrationBackupManifest
	if err := json.Unmarshal(rawManifest, &persisted); err != nil {
		t.Fatalf("manifest json.Unmarshal error = %v", err)
	}
	requireAdminMigrationPayloadHash(t, root, persisted.Payloads, adminMigrationBackupBootstrapPath)
	requireAdminMigrationPayloadHash(t, root, persisted.Payloads, adminMigrationBackupObjectsPath)
	requireAdminMigrationPayloadHash(t, root, persisted.Payloads, adminMigrationBackupCookbooksPath)
	requireAdminMigrationPayloadHash(t, root, persisted.Payloads, adminMigrationBackupBlobsPath)
	requireAdminMigrationPayloadHash(t, root, persisted.Payloads, adminMigrationBackupRunbookPath)
	if !sameAdminStrings(persisted.Warnings, []string{"a warning", "z warning"}) {
		t.Fatalf("warnings = %v, want sorted warnings", persisted.Warnings)
	}
	if len(persisted.Excluded) != 1 || persisted.Excluded[0].Family != "opensearch" {
		t.Fatalf("excluded = %#v, want opensearch exclusion", persisted.Excluded)
	}

	var blobs adminMigrationBackupBlobManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationBackupBlobsPath), &blobs); err != nil {
		t.Fatalf("blob manifest json.Unmarshal error = %v", err)
	}
	if !sameAdminStrings(blobs.ReferencedChecksums, []string{cookbookChecksum, checksum}) {
		t.Fatalf("referenced checksums = %v, want sandbox and cookbook checksums", blobs.ReferencedChecksums)
	}
	if notes := string(readAdminMigrationTestFile(t, root, adminMigrationBackupRunbookPath)); !strings.Contains(notes, "opencook admin reindex --all-orgs --complete") {
		t.Fatalf("runbook notes missing reindex command: %s", notes)
	}
	var roundTripCookbooks adminMigrationCookbookExport
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationBackupCookbooksPath), &roundTripCookbooks); err != nil {
		t.Fatalf("cookbook export json.Unmarshal error = %v", err)
	}
	if got := len(roundTripCookbooks.Orgs["ponyville"].Versions); got != 1 {
		t.Fatalf("round-trip cookbook versions = %d, want 1", got)
	}
}

func TestAdminMigrationBackupCreateWritesAndInspectValidatesBundle(t *testing.T) {
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	if err := os.MkdirAll(blobRoot, 0o755); err != nil {
		t.Fatalf("MkdirAll(blob root) error = %v", err)
	}
	checksum := writeAdminMigrationTestBlob(t, blobRoot, "rainbow")
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	orgObjects := coreObjectState.Orgs["ponyville"]
	orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox1": {ID: "sandbox1", Organization: "ponyville", Checksums: []string{checksum}},
	}
	coreObjectState.Orgs["ponyville"] = orgObjects
	cookbooks := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{
		"ponyville": {
			Versions: []bootstrap.CookbookVersion{{
				Name:         "app-1.2.3",
				CookbookName: "app",
				Version:      "1.2.3",
				AllFiles:     []bootstrap.CookbookFile{{Name: "default.rb", Path: "recipes/default.rb", Checksum: checksum, Specificity: "default"}},
			}},
		},
	}}
	outputPath := filepath.Join(t.TempDir(), "opencook-backup")

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://user:supersecret@postgres.example/opencook",
			BlobBackend:             "filesystem",
			BlobStorageURL:          blobRoot,
			BlobS3SecretKey:         "secret-key",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore: &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
			cookbookInventory: map[string]adminMigrationCookbookInventory{
				"ponyville": {Versions: 1, ChecksumReferences: 1, Checksums: []string{checksum}},
			},
			cookbookExport: cookbooks,
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "backup", "create", "--output", outputPath, "--offline", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration backup create) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["ok"] != true {
		t.Fatalf("ok = %v, want true in output %v", out["ok"], out)
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "postgres", "ok")
	requireAdminMigrationDependency(t, deps, "blob", "ok")
	requireAdminMigrationDependency(t, deps, "backup_blob_copy", "ok")
	requireAdminMigrationPayloadHash(t, outputPath, mustReadAdminMigrationBackupManifest(t, outputPath).Payloads, adminMigrationBackupBootstrapPath)

	var blobs adminMigrationBackupBlobManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, outputPath, adminMigrationBackupBlobsPath), &blobs); err != nil {
		t.Fatalf("blob manifest json.Unmarshal error = %v", err)
	}
	if !sameAdminStrings(blobs.ReferencedChecksums, []string{checksum}) {
		t.Fatalf("referenced checksums = %v, want %s", blobs.ReferencedChecksums, checksum)
	}
	if len(blobs.Copied) != 1 || blobs.Copied[0].Checksum != checksum {
		t.Fatalf("copied blobs = %#v, want copied checksum %s", blobs.Copied, checksum)
	}
	requireAdminMigrationPayloadHash(t, outputPath, mustReadAdminMigrationBackupManifest(t, outputPath).Payloads, blobs.Copied[0].Path)

	for _, forbidden := range []string{"supersecret", "secret-key", "BEGIN RSA PRIVATE KEY", "BEGIN PRIVATE KEY"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(readAdminMigrationBundleText(t, outputPath), forbidden) {
			t.Fatalf("backup create leaked forbidden material %q", forbidden)
		}
	}

	inspectCmd, inspectStdout, inspectStderr := newTestCommand(t)
	inspectCmd.loadOffline = func() (config.Config, error) {
		t.Fatal("backup inspect must not load provider configuration")
		return config.Config{}, nil
	}
	code = inspectCmd.Run(context.Background(), []string{"admin", "migration", "backup", "inspect", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration backup inspect) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, inspectStdout.String(), inspectStderr.String())
	}
	inspectOut := decodeAdminMigrationOutput(t, inspectStdout.String())
	if inspectOut["ok"] != true {
		t.Fatalf("inspect ok = %v, want true in output %v", inspectOut["ok"], inspectOut)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, inspectOut, "dependencies"), "backup_bundle", "ok")

	if err := os.WriteFile(filepath.Join(outputPath, adminMigrationBackupRunbookPath), []byte("tampered\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(tampered runbook) error = %v", err)
	}
	tamperedCmd, tamperedStdout, tamperedStderr := newTestCommand(t)
	code = tamperedCmd.Run(context.Background(), []string{"admin", "migration", "backup", "inspect", outputPath, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration backup inspect tampered) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, tamperedStdout.String(), tamperedStderr.String())
	}
	tamperedOut := decodeAdminMigrationOutput(t, tamperedStdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, tamperedOut, "findings"), "backup_payload_integrity_failed")
}

func TestAdminMigrationBackupCreateRefusesMissingOrUnavailableBlobs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		blobRoot     string
		newBlobStore func(config.Config) (blob.Store, error)
		wantFinding  string
	}{
		{
			name:        "missing blob",
			blobRoot:    t.TempDir(),
			wantFinding: "missing_blob",
		},
		{
			name: "provider unavailable",
			newBlobStore: func(config.Config) (blob.Store, error) {
				return fakeMigrationBlobStore{
					status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
					err:    blob.ErrUnavailable,
				}, nil
			},
			wantFinding: "blob_provider_unavailable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
			orgObjects := coreObjectState.Orgs["ponyville"]
			orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
				"sandbox1": {ID: "sandbox1", Organization: "ponyville", Checksums: []string{checksum}},
			}
			coreObjectState.Orgs["ponyville"] = orgObjects
			outputPath := filepath.Join(t.TempDir(), "opencook-backup")

			cmd, stdout, stderr := newTestCommand(t)
			if tc.newBlobStore != nil {
				cmd.newBlobStore = tc.newBlobStore
			} else {
				cmd.newBlobStore = blob.NewStore
			}
			cmd.loadOffline = func() (config.Config, error) {
				return config.Config{
					DefaultOrganization:     "ponyville",
					PostgresDSN:             "postgres://opencook",
					BlobBackend:             "filesystem",
					BlobStorageURL:          tc.blobRoot,
					BootstrapRequestorName:  "pivotal",
					BootstrapRequestorType:  "user",
					BootstrapRequestorKeyID: "default",
				}, nil
			}
			cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
				return &fakeMigrationInventoryStore{
					fakeOfflineStore: &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
					cookbookExport:   adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
				}, nil, nil
			}

			code := cmd.Run(context.Background(), []string{"admin", "migration", "backup", "create", "--output", outputPath, "--offline", "--yes", "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(migration backup create %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			if _, err := os.Stat(filepath.Join(outputPath, adminMigrationBackupManifestPath)); !os.IsNotExist(err) {
				t.Fatalf("manifest stat err = %v, want not exist", err)
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantFinding)
		})
	}
}

func TestAdminMigrationRestorePreflightAcceptsEmptyTarget(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://user:supersecret@postgres.example/opencook",
			BlobBackend:             "memory",
			BlobS3SecretKey:         "secret-key",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore: &fakeOfflineStore{
				bootstrap: bootstrap.BootstrapCoreState{},
				objects:   bootstrap.CoreObjectState{},
			},
			cookbookInventory: map[string]adminMigrationCookbookInventory{},
			cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "preflight", bundlePath, "--offline", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration restore preflight empty target) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["ok"] != true {
		t.Fatalf("ok = %v, want true in output %v", out["ok"], out)
	}
	for _, forbidden := range []string{"supersecret", "secret-key"} {
		if strings.Contains(stdout.String(), forbidden) {
			t.Fatalf("restore preflight leaked forbidden material %q", forbidden)
		}
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "backup_bundle", "ok")
	requireAdminMigrationDependency(t, deps, "backup_blobs", "ok")
	requireAdminMigrationDependency(t, deps, "postgres", "ok")
	requireAdminMigrationDependency(t, deps, "restore_target", "ok")
	requireAdminMigrationDependency(t, deps, "blob", "ok")
	requireAdminMigrationDependency(t, deps, "opensearch", "unconfigured")
	inventory := requireAdminMigrationMap(t, out, "inventory")
	families := requireAdminMigrationArray(t, inventory, "families")
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "clients", 1)
	mutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, mutations, "would restore PostgreSQL-backed logical state from the backup bundle")
	requireAdminMigrationMutationMessage(t, mutations, "would rebuild OpenSearch derived state from restored PostgreSQL state after restore")
}

func TestAdminMigrationRestorePreflightRefusesNonEmptyTarget(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "memory",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
			cookbookInventory: map[string]adminMigrationCookbookInventory{},
			cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "preflight", bundlePath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration restore preflight non-empty target) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_target", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "restore_target_not_empty")
}

func TestAdminMigrationRestorePreflightRejectsInvalidBundleWithoutProviders(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	manifest := mustReadAdminMigrationBackupManifest(t, bundlePath)
	manifest.FormatVersion = "opencook.migration.backup.v0"
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent(manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(bundlePath, adminMigrationBackupManifestPath), append(data, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(manifest) error = %v", err)
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		t.Fatal("restore preflight must not load provider configuration for invalid bundles")
		return config.Config{}, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "preflight", bundlePath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration restore preflight invalid bundle) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_bundle", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "backup_manifest_unsupported_format")
}

func TestAdminMigrationRestorePreflightReportsTargetBlobUnavailable(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
			err:    blob.ErrUnavailable,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "filesystem",
			BlobStorageURL:          t.TempDir(),
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore: &fakeOfflineStore{
				bootstrap: bootstrap.BootstrapCoreState{},
				objects:   bootstrap.CoreObjectState{},
			},
			cookbookInventory: map[string]adminMigrationCookbookInventory{},
			cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "preflight", bundlePath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration restore preflight blob unavailable) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob", "error")
}

func TestAdminMigrationRestoreApplyRestoresStateAndRecommendsReindex(t *testing.T) {
	bundlePath, checksum := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore: &fakeOfflineStore{
			bootstrap: bootstrap.BootstrapCoreState{},
			objects:   bootstrap.CoreObjectState{},
		},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobExists := map[string]bool{}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "memory-compat", Configured: true, Message: "test blob backend"},
			exists: blobExists,
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     "ponyville",
			PostgresDSN:             "postgres://opencook",
			BlobBackend:             "memory",
			BootstrapRequestorName:  "pivotal",
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "apply", bundlePath, "--offline", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration restore apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_restore_apply" {
		t.Fatalf("command = %v, want migration_restore_apply", out["command"])
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_write", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_blobs", "ok")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "opencook admin reindex --all-orgs --complete")
	if _, ok := targetStore.bootstrap.Orgs["ponyville"]; !ok {
		t.Fatalf("restored bootstrap orgs = %#v, want ponyville", targetStore.bootstrap.Orgs)
	}
	if _, ok := targetStore.objects.Orgs["ponyville"]; !ok {
		t.Fatalf("restored core object orgs = %#v, want ponyville", targetStore.objects.Orgs)
	}
	if got := string(blobPuts[checksum]); got != "rainbow" {
		t.Fatalf("restored blob body = %q, want rainbow", got)
	}
	if got := len(targetStore.restoredCookbooks.Orgs["ponyville"].Versions); got != 1 {
		t.Fatalf("restored cookbook versions = %d, want 1", got)
	}
}

func TestAdminMigrationRestoreApplyDryRunDoesNotMutate(t *testing.T) {
	bundlePath, _ := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore: &fakeOfflineStore{
			bootstrap: bootstrap.BootstrapCoreState{},
			objects:   bootstrap.CoreObjectState{},
		},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "memory-compat", Configured: true, Message: "test blob backend"},
			exists: map[string]bool{},
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "memory"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "apply", bundlePath, "--offline", "--dry-run", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration restore apply dry-run) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("dry-run mutated bootstrap=%d objects=%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
}

func TestAdminMigrationRestoreApplyBlobFailureDoesNotSaveMetadata(t *testing.T) {
	bundlePath, _ := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore: &fakeOfflineStore{
			bootstrap: bootstrap.BootstrapCoreState{},
			objects:   bootstrap.CoreObjectState{},
		},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "memory-compat", Configured: true, Message: "test blob backend"},
			exists: map[string]bool{},
			putErr: blob.ErrUnavailable,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "memory"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "apply", bundlePath, "--offline", "--yes", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration restore apply blob failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_blobs", "error")
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 {
		t.Fatalf("blob failure saved metadata bootstrap=%d objects=%d", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func TestAdminMigrationRestoreApplyCoreFailureRollsBackBootstrap(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore: &fakeOfflineStore{
			bootstrap:     bootstrap.BootstrapCoreState{},
			objects:       bootstrap.CoreObjectState{},
			objectSaveErr: errors.New("core save failed"),
		},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "memory"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "apply", bundlePath, "--offline", "--yes", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration restore apply core failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "restore_write_failed")
	if got := len(targetStore.bootstrap.Orgs); got != 0 {
		t.Fatalf("bootstrap orgs after rollback = %d, want 0", got)
	}
	if targetStore.bootstrapSaves != 2 {
		t.Fatalf("bootstrap saves = %d, want restore plus rollback", targetStore.bootstrapSaves)
	}
}

func TestAdminMigrationSourceInventoryReadsManifestFixture(t *testing.T) {
	root := t.TempDir()
	supported := true
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationSourceFormatV1,
		SourceType:    "generated_json_export",
		Families: []adminMigrationInventoryFamily{
			{Family: "users", Count: 2},
			{Family: "organizations", Count: 1},
			{Organization: "ponyville", Family: "nodes", Count: 3},
		},
		Artifacts: []adminMigrationSourceManifestArtifact{
			{Family: "bookshelf", Count: 2, Supported: &supported},
			{Family: "opensearch", Count: 1, Deferred: true},
		},
	}
	writeAdminMigrationSourceJSON(t, filepath.Join(root, "opencook-source-manifest.json"), manifest)

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", root, "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(migration source inventory manifest) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_inventory" {
		t.Fatalf("command = %v, want migration_source_inventory", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["source_path"] != root {
		t.Fatalf("target.source_path = %v, want %s", target["source_path"], root)
	}
	dep := requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_artifact", "ok")
	details := requireAdminMigrationMap(t, dep, "details")
	if details["source_type"] != "generated_json_export" {
		t.Fatalf("source_type = %v, want generated_json_export", details["source_type"])
	}
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "nodes", 3)
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", 2)
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_source_artifacts", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "source_import_not_implemented")
	requireAdminMigrationFinding(t, findings, "source_search_rebuild_required")
	requireAdminMigrationFinding(t, findings, "source_artifact_unsupported")
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("output missing duration_ms: %v", out)
	}
}

func TestAdminMigrationSourceInventoryScansExtractedArtifact(t *testing.T) {
	root := t.TempDir()
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "pivotal.json"), map[string]string{"username": "pivotal"})
	writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "nodes", "web01.json"), map[string]string{"name": "web01"})
	writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "roles", "web.json"), map[string]string{"name": "web"})
	writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "data_bags", "secrets", "db.json"), map[string]string{"id": "db"})
	writeAdminMigrationSourceFile(t, filepath.Join(root, "bookshelf", checksum), "cookbook bytes")
	writeAdminMigrationSourceFile(t, filepath.Join(root, "elasticsearch", "chef", "node", "web01"), "{}")
	writeAdminMigrationSourceFile(t, filepath.Join(root, "postgresql", "dump.sql"), "select 1;")
	writeAdminMigrationSourceFile(t, filepath.Join(root, "oc-id", "users.json"), "{}")

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", root, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source inventory extracted) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "nodes", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "roles", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "data_bags", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "data_bag_items", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "source_import_not_implemented")
	requireAdminMigrationFinding(t, findings, "source_search_rebuild_required")
	requireAdminMigrationFinding(t, findings, "source_database_artifact_deferred")
	unsupported := requireAdminMigrationFinding(t, findings, "source_artifact_unsupported")
	if unsupported["family"] != "oc_id" {
		t.Fatalf("unsupported family = %v, want oc_id", unsupported["family"])
	}
}

func TestAdminMigrationSourceInventoryScansChefBackupArchive(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "chef-server-backup.tar.gz")
	writeAdminMigrationSourceArchive(t, archivePath, map[string]string{
		"users/pivotal.json": `{"username":"pivotal"}`,
		"organizations/ponyville/clients/ponyville-validator.json":      `{"name":"ponyville-validator"}`,
		"organizations/ponyville/cookbooks/apache2-1.0.0/metadata.json": `{}`,
		"bookshelf/objects/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb":            "cookbook bytes",
		"opensearch/chef/node/web01.json":                               `{}`,
	})

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", archivePath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source inventory archive) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	dep := requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_artifact", "ok")
	details := requireAdminMigrationMap(t, dep, "details")
	if details["source_type"] != "chef_server_backup_archive" {
		t.Fatalf("source_type = %v, want chef_server_backup_archive", details["source_type"])
	}
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "clients", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "cookbook_versions", 1)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_search_rebuild_required")
}

func TestAdminMigrationSourceInventoryRejectsMissingPath(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	missing := filepath.Join(t.TempDir(), "missing-source")
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", missing, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source inventory missing) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_artifact", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_artifact_unavailable")
}

func TestAdminMigrationCutoverRehearseValidatesLiveTargetAndDownloadsBlob(t *testing.T) {
	bundlePath, checksum := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	downloadURL := "http://opencook.test/_blob/checksums/" + checksum + "?signature=secret"
	cookbookPath := "/organizations/ponyville/cookbooks/app/1.2.3"
	fake := &fakeMigrationRehearsalClient{
		responses: map[string]any{
			cookbookPath: map[string]any{
				"all_files": []any{map[string]any{
					"checksum": checksum,
					"url":      downloadURL,
				}},
			},
		},
		downloads: map[string][]byte{downloadURL: []byte("rainbow")},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{
			ServerURL:        "http://env.example",
			RequestorName:    "env-user",
			RequestorType:    "user",
			PrivateKeyPath:   "/env/key.pem",
			ServerAPIVersion: "1",
		}
	}
	cmd.newAdmin = func(cfg admin.Config) (adminJSONClient, error) {
		if cfg.ServerURL != "http://opencook.test" || cfg.RequestorName != "pivotal" || cfg.PrivateKeyPath != "/keys/pivotal.pem" {
			t.Fatalf("admin config = %+v, want cutover overrides", cfg)
		}
		return fake, nil
	}

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "cutover", "rehearse",
		"--manifest", filepath.Join(bundlePath, adminMigrationBackupManifestPath),
		"--server-url", "http://opencook.test",
		"--requestor-name", "pivotal",
		"--private-key", "/keys/pivotal.pem",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(migration cutover rehearse) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_cutover_rehearse" {
		t.Fatalf("command = %v, want migration_cutover_rehearse", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["manifest_path"] != filepath.Join(bundlePath, adminMigrationBackupManifestPath) {
		t.Fatalf("target.manifest_path = %v, want manifest path", target["manifest_path"])
	}
	if target["server_url"] == "http://opencook.test" {
		t.Fatalf("target.server_url = %v, want redacted URL", target["server_url"])
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_bundle", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "cutover_rehearsal", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_failed", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_downloads", 1)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "shadow_read_advisory")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "compare read-only source Chef responses to restored OpenCook responses with documented compatibility normalizers")
	if !fake.calledPath(cookbookPath) {
		t.Fatalf("cutover rehearsal did not read cookbook path; calls = %+v", fake.calls)
	}
	if len(fake.downloadCalls) != 1 || fake.downloadCalls[0] != downloadURL {
		t.Fatalf("download calls = %v, want %s", fake.downloadCalls, downloadURL)
	}
}

func TestAdminMigrationCutoverRehearseReportsLiveReadFailures(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	fake := &fakeMigrationRehearsalClient{
		errPaths: map[string]error{"/readyz": errors.New("target not ready")},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{
			ServerURL:        "http://opencook.test",
			RequestorName:    "pivotal",
			RequestorType:    "user",
			PrivateKeyPath:   "/keys/pivotal.pem",
			ServerAPIVersion: "1",
		}
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return fake, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "cutover", "rehearse", "--manifest", bundlePath, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration cutover rehearse failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "cutover_rehearsal", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "cutover_rehearsal_check_failed")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_failed", 1)
}

func TestAdminMigrationScaffoldParsesCommandShapes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		args          []string
		wantCode      int
		command       string
		targetKey     string
		targetWant    string
		wantDryRun    bool
		wantOffline   bool
		wantConfirmed bool
	}{
		{
			name:      "default preflight",
			args:      []string{"admin", "migration", "preflight", "--json"},
			wantCode:  exitOK,
			command:   "migration_preflight",
			targetKey: "all_organizations",
		},
		{
			name:       "org preflight",
			args:       []string{"admin", "migration", "preflight", "--org", "ponyville", "--json"},
			wantCode:   exitOK,
			command:    "migration_preflight",
			targetKey:  "organization",
			targetWant: "ponyville",
		},
		{
			name:          "backup create dry run",
			args:          []string{"admin", "migration", "backup", "create", "--output", "/tmp/opencook-backup", "--offline", "--dry-run", "--json"},
			wantCode:      exitOK,
			command:       "migration_backup_create",
			targetKey:     "output_path",
			targetWant:    "/tmp/opencook-backup",
			wantDryRun:    true,
			wantOffline:   true,
			wantConfirmed: false,
		},
		{
			name:          "backup create confirmed",
			args:          []string{"admin", "migration", "backup", "create", "--output", "/tmp/opencook-backup", "--offline", "--yes"},
			wantCode:      exitOK,
			command:       "migration_backup_create",
			targetKey:     "output_path",
			targetWant:    "/tmp/opencook-backup",
			wantOffline:   true,
			wantConfirmed: true,
		},
		{
			name:       "backup inspect path before flags",
			args:       []string{"admin", "migration", "backup", "inspect", "/tmp/opencook-backup", "--json"},
			wantCode:   exitDependencyUnavailable,
			command:    "migration_backup_inspect",
			targetKey:  "bundle_path",
			targetWant: "/tmp/opencook-backup",
		},
		{
			name:        "restore preflight path before flags",
			args:        []string{"admin", "migration", "restore", "preflight", "/tmp/opencook-backup", "--offline", "--with-timing"},
			wantCode:    exitDependencyUnavailable,
			command:     "migration_restore_preflight",
			targetKey:   "bundle_path",
			targetWant:  "/tmp/opencook-backup",
			wantOffline: true,
		},
		{
			name:        "restore apply dry run",
			args:        []string{"admin", "migration", "restore", "apply", "/tmp/opencook-backup", "--offline", "--dry-run"},
			wantCode:    exitDependencyUnavailable,
			command:     "migration_restore_apply",
			targetKey:   "bundle_path",
			targetWant:  "/tmp/opencook-backup",
			wantDryRun:  true,
			wantOffline: true,
		},
		{
			name:       "source inventory path before flags",
			args:       []string{"admin", "migration", "source", "inventory", "/tmp/chef-server-source", "--json"},
			wantCode:   exitDependencyUnavailable,
			command:    "migration_source_inventory",
			targetKey:  "source_path",
			targetWant: "/tmp/chef-server-source",
		},
		{
			name:       "cutover rehearse",
			args:       []string{"admin", "migration", "cutover", "rehearse", "--manifest", "/tmp/manifest.json", "--server-url", "https://admin:secret@opencook.example", "--json"},
			wantCode:   exitDependencyUnavailable,
			command:    "migration_cutover_rehearse",
			targetKey:  "manifest_path",
			targetWant: "/tmp/manifest.json",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string(nil), tc.args...)
			targetWant := tc.targetWant
			if tc.targetKey == "output_path" && targetWant == "/tmp/opencook-backup" {
				targetWant = filepath.Join(t.TempDir(), "opencook-backup")
				for i := 0; i < len(args)-1; i++ {
					if args[i] == "--output" {
						args[i+1] = targetWant
					}
				}
			}
			cmd, stdout, stderr := newTestCommand(t)
			cmd.newBlobStore = blob.NewStore
			cmd.loadOffline = func() (config.Config, error) {
				return config.Config{
					PostgresDSN:            "postgres://example",
					DefaultOrganization:    "ponyville",
					BootstrapRequestorName: "pivotal",
					BootstrapRequestorType: "user",
				}, nil
			}
			cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
				bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
				return &fakeMigrationInventoryStore{
					fakeOfflineStore: &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
					cookbookExport:   adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
				}, nil, nil
			}

			code := cmd.Run(context.Background(), args)
			if code != tc.wantCode {
				t.Fatalf("Run(%v) exit = %d, want %d; stdout = %s stderr = %s", args, code, tc.wantCode, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			if out["command"] != tc.command {
				t.Fatalf("command = %v, want %s", out["command"], tc.command)
			}
			target := requireAdminMigrationMap(t, out, "target")
			switch tc.targetKey {
			case "all_organizations":
				if target[tc.targetKey] != true {
					t.Fatalf("target[%s] = %v, want true", tc.targetKey, target[tc.targetKey])
				}
			default:
				if target[tc.targetKey] != targetWant {
					t.Fatalf("target[%s] = %v, want %q", tc.targetKey, target[tc.targetKey], targetWant)
				}
			}
			if got := boolField(out, "dry_run"); got != tc.wantDryRun {
				t.Fatalf("dry_run = %t, want %t in %v", got, tc.wantDryRun, out)
			}
			if got := boolField(out, "offline"); got != tc.wantOffline {
				t.Fatalf("offline = %t, want %t in %v", got, tc.wantOffline, out)
			}
			if got := boolField(out, "confirmed"); got != tc.wantConfirmed {
				t.Fatalf("confirmed = %t, want %t in %v", got, tc.wantConfirmed, out)
			}
			if tc.wantDryRun && tc.targetKey == "output_path" {
				if _, err := os.Stat(filepath.Join(targetWant, adminMigrationBackupManifestPath)); !os.IsNotExist(err) {
					t.Fatalf("dry-run manifest stat err = %v, want not exist", err)
				}
			}
			if strings.Contains(stdout.String(), "admin:secret") {
				t.Fatalf("cutover scaffold leaked server URL credentials: %s", stdout.String())
			}
		})
	}
}

type fakeMigrationInventoryStore struct {
	*fakeOfflineStore
	cookbookInventory  map[string]adminMigrationCookbookInventory
	cookbookExport     adminMigrationCookbookExport
	restoredCookbooks  adminMigrationCookbookExport
	cookbookRestoreErr error
}

// LoadCookbookInventory lets migration preflight tests exercise cookbook
// inventory counts without needing a live PostgreSQL cookbook repository.
func (s *fakeMigrationInventoryStore) LoadCookbookInventory([]string) (map[string]adminMigrationCookbookInventory, error) {
	return s.cookbookInventory, nil
}

// LoadCookbookExport lets backup-create tests exercise the offline cookbook
// export path without activating a real PostgreSQL repository.
func (s *fakeMigrationInventoryStore) LoadCookbookExport([]string) (adminMigrationCookbookExport, error) {
	if s.cookbookExport.Orgs == nil {
		return adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}}, nil
	}
	return s.cookbookExport, nil
}

// RestoreCookbookExport records cookbook restore requests for migration apply
// tests without needing a live PostgreSQL cookbook repository.
func (s *fakeMigrationInventoryStore) RestoreCookbookExport(_ bootstrap.BootstrapCoreState, export adminMigrationCookbookExport) error {
	if s.cookbookRestoreErr != nil {
		return s.cookbookRestoreErr
	}
	s.restoredCookbooks = export
	return nil
}

type fakeMigrationRehearsalClient struct {
	calls         []fakeAdminCall
	downloadCalls []string
	responses     map[string]any
	errPaths      map[string]error
	downloads     map[string][]byte
}

// DoJSON records signed rehearsal reads and returns route-specific fixtures so
// cutover tests can assert representative path coverage.
func (f *fakeMigrationRehearsalClient) DoJSON(_ context.Context, method, path string, in, out any) error {
	f.calls = append(f.calls, fakeAdminCall{method: method, path: path, payload: cloneJSONValue(in)})
	if err := f.errPaths[path]; err != nil {
		return err
	}
	var response any = map[string]any{"ok": true}
	if f.responses != nil {
		if configured, ok := f.responses[path]; ok {
			response = configured
		}
	}
	if out == nil {
		return nil
	}
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// DoUnsigned records signed-URL downloads and returns checksum-addressed blob
// fixture bytes without needing a live HTTP server.
func (f *fakeMigrationRehearsalClient) DoUnsigned(_ context.Context, method, rawURL string) (admin.RawResponse, error) {
	f.downloadCalls = append(f.downloadCalls, rawURL)
	if method != http.MethodGet {
		return admin.RawResponse{}, errors.New("unexpected unsigned method")
	}
	body, ok := f.downloads[rawURL]
	if !ok {
		return admin.RawResponse{}, errors.New("missing download fixture")
	}
	return admin.RawResponse{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
}

// calledPath reports whether a live rehearsal check attempted a specific path.
func (f *fakeMigrationRehearsalClient) calledPath(path string) bool {
	for _, call := range f.calls {
		if call.path == path {
			return true
		}
	}
	return false
}

// newAdminMigrationOpenSearchServer exposes the provider discovery endpoints
// migration preflight needs before it delegates consistency checks to fakes.
func newAdminMigrationOpenSearchServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"version":{"distribution":"opensearch","number":"2.12.0"},"tagline":"OpenSearch"}`))
		case r.Method == http.MethodHead && r.URL.Path == "/chef":
			w.WriteHeader(http.StatusNotFound)
		default:
			t.Fatalf("unexpected OpenSearch request %s %s", r.Method, r.URL.String())
		}
	}))
}

type fakeMigrationBlobStore struct {
	status blob.Status
	err    error
	exists map[string]bool
	puts   map[string][]byte
	putErr error
}

// Name identifies the fake blob store in the same shape as real blob adapters.
func (s fakeMigrationBlobStore) Name() string {
	return "fake-migration-blob-store"
}

// Status returns the configured provider status used by migration preflight.
func (s fakeMigrationBlobStore) Status() blob.Status {
	return s.status
}

// Exists returns the injected error so tests can pin unavailable-provider output.
func (s fakeMigrationBlobStore) Exists(_ context.Context, checksum string) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	return s.exists[strings.TrimSpace(checksum)], nil
}

// Put records restored blob bodies or returns the injected error so restore
// apply tests can prove blob failures happen before metadata writes.
func (s fakeMigrationBlobStore) Put(_ context.Context, req blob.PutRequest) (blob.PutResult, error) {
	if s.putErr != nil {
		return blob.PutResult{}, s.putErr
	}
	key := strings.TrimSpace(req.Key)
	if s.puts != nil {
		s.puts[key] = append([]byte(nil), req.Body...)
	}
	if s.exists != nil {
		s.exists[key] = true
	}
	return blob.PutResult{Location: key}, nil
}

// writeAdminMigrationTestBlob writes content under its Chef MD5 checksum so
// filesystem validation can prove both existence and local content checks.
func writeAdminMigrationTestBlob(t *testing.T, root, body string) string {
	t.Helper()
	sum := md5.Sum([]byte(body))
	checksum := hex.EncodeToString(sum[:])
	if err := os.WriteFile(filepath.Join(root, checksum), []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", checksum, err)
	}
	return checksum
}

// writeAdminMigrationSourceJSON stores a compact JSON source fixture while
// keeping test setup focused on source inventory behavior.
func writeAdminMigrationSourceJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal(%T) error = %v", value, err)
	}
	writeAdminMigrationSourceFile(t, path, string(data))
}

// writeAdminMigrationSourceFile creates parent directories for source fixtures
// so tests can model extracted Chef Server artifacts tersely.
func writeAdminMigrationSourceFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

// writeAdminMigrationSourceArchive writes a tiny tar.gz fixture so archive
// inventory tests do not depend on external chef-server-ctl tooling.
func writeAdminMigrationSourceArchive(t *testing.T, path string, files map[string]string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", filepath.Dir(path), err)
	}
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create(%s) error = %v", path, err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()
	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()
	for _, name := range adminMigrationSortedMapKeys(files) {
		body := []byte(files[name])
		if err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(body))}); err != nil {
			t.Fatalf("WriteHeader(%s) error = %v", name, err)
		}
		if _, err := tarWriter.Write(body); err != nil {
			t.Fatalf("Write(%s) error = %v", name, err)
		}
	}
}

// adminMigrationHealthyStates seeds a full organization through bootstrap
// service code so the preflight happy path validates realistic default rows.
func adminMigrationHealthyStates(t *testing.T) (bootstrap.BootstrapCoreState, bootstrap.CoreObjectState) {
	t.Helper()
	bootstrapStore := bootstrap.NewMemoryBootstrapCoreStore(bootstrap.BootstrapCoreState{})
	coreObjectStore := bootstrap.NewMemoryCoreObjectStore(bootstrap.CoreObjectState{})
	service := bootstrap.NewService(nil, bootstrap.Options{
		BootstrapCoreStoreFactory: func(*bootstrap.Service) bootstrap.BootstrapCoreStore {
			return bootstrapStore
		},
		CoreObjectStoreFactory: func(*bootstrap.Service) bootstrap.CoreObjectStore {
			return coreObjectStore
		},
	})
	if _, _, _, err := service.CreateOrganization(bootstrap.CreateOrganizationInput{Name: "ponyville", FullName: "Ponyville", OwnerName: "pivotal"}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	bootstrapState, err := bootstrapStore.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	coreObjectState, err := coreObjectStore.LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects() error = %v", err)
	}
	return bootstrapState, coreObjectState
}

func TestAdminMigrationSafetyGatesAndUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing subcommand", args: []string{"admin", "migration"}, want: "admin migration requires a subcommand"},
		{name: "unknown subcommand", args: []string{"admin", "migration", "unknown"}, want: `unknown admin migration command "unknown"`},
		{name: "ambiguous scope", args: []string{"admin", "migration", "preflight", "--org", "ponyville", "--all-orgs"}, want: "cannot combine --all-orgs with --org"},
		{name: "backup create missing output", args: []string{"admin", "migration", "backup", "create", "--offline", "--dry-run"}, want: "requires --output PATH"},
		{name: "backup create missing offline", args: []string{"admin", "migration", "backup", "create", "--output", "/tmp/out", "--yes"}, want: "requires --offline"},
		{name: "backup create missing confirmation", args: []string{"admin", "migration", "backup", "create", "--output", "/tmp/out", "--offline"}, want: "requires --dry-run or --yes"},
		{name: "backup inspect missing path", args: []string{"admin", "migration", "backup", "inspect"}, want: "backup inspect PATH"},
		{name: "restore preflight missing offline", args: []string{"admin", "migration", "restore", "preflight", "/tmp/bundle"}, want: "requires --offline"},
		{name: "restore apply missing confirmation", args: []string{"admin", "migration", "restore", "apply", "/tmp/bundle", "--offline"}, want: "requires --dry-run or --yes"},
		{name: "source inventory missing path", args: []string{"admin", "migration", "source", "inventory"}, want: "source inventory PATH"},
		{name: "cutover missing manifest", args: []string{"admin", "migration", "cutover", "rehearse"}, want: "requires --manifest PATH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), tc.args)
			if code != exitUsage {
				t.Fatalf("Run(%v) exit = %d, want %d; stdout = %s stderr = %s", tc.args, code, exitUsage, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestAdminMigrationInheritedJSONBypassesLiveAdminClient(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "https://opencook.example"}
	}

	code := cmd.Run(context.Background(), []string{"admin", "--json", "migration", "backup", "inspect", "/tmp/opencook-backup"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(admin --json migration backup inspect) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_backup_inspect" {
		t.Fatalf("command = %v, want migration_backup_inspect", out["command"])
	}
}

func decodeAdminMigrationOutput(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", raw, err)
	}
	return out
}

// requireAdminMigrationMap keeps JSON shape assertions concise while producing
// clear test failures when a scaffold field changes type.
func requireAdminMigrationMap(t *testing.T, source map[string]any, key string) map[string]any {
	t.Helper()
	value, ok := source[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %#v, want object", key, source[key])
	}
	return value
}

// requireAdminMigrationArray asserts that stable migration output collections
// remain arrays even when they are empty in the initial scaffold.
func requireAdminMigrationArray(t *testing.T, source map[string]any, key string) []any {
	t.Helper()
	value, ok := source[key].([]any)
	if !ok {
		t.Fatalf("%s = %#v, want array", key, source[key])
	}
	return value
}

// requireAdminMigrationArrayMap extracts an object from a JSON array so tests
// can inspect structured errors and future validation findings.
func requireAdminMigrationArrayMap(t *testing.T, source []any, index int) map[string]any {
	t.Helper()
	if index >= len(source) {
		t.Fatalf("array length = %d, want index %d", len(source), index)
	}
	value, ok := source[index].(map[string]any)
	if !ok {
		t.Fatalf("array[%d] = %#v, want object", index, source[index])
	}
	return value
}

// requireAdminMigrationDependency finds a dependency by name and status so tests
// can assert preflight readiness without depending on array ordering.
func requireAdminMigrationDependency(t *testing.T, source []any, name, status string) map[string]any {
	t.Helper()
	for _, item := range source {
		dep, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("dependency = %#v, want object", item)
		}
		if dep["name"] == name {
			if dep["status"] != status {
				t.Fatalf("dependency %s status = %v, want %s in %v", name, dep["status"], status, dep)
			}
			return dep
		}
	}
	t.Fatalf("dependencies = %v, want %s/%s", source, name, status)
	return nil
}

// requireAdminMigrationInventoryFamily finds a scoped family count so inventory
// assertions do not depend on the stable-but-verbose output ordering.
func requireAdminMigrationInventoryFamily(t *testing.T, source []any, organization, family string, count int) map[string]any {
	t.Helper()
	for _, item := range source {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("inventory family = %#v, want object", item)
		}
		entryOrg, _ := entry["organization"].(string)
		if entryOrg == organization && entry["family"] == family {
			if entry["count"] != float64(count) {
				t.Fatalf("inventory %s/%s count = %v, want %d in %v", organization, family, entry["count"], count, entry)
			}
			return entry
		}
	}
	t.Fatalf("inventory families = %v, want %s/%s count %d", source, organization, family, count)
	return nil
}

// requireAdminMigrationFinding finds a finding by stable code so consistency
// tests can ignore message wording unless compatibility requires exact text.
func requireAdminMigrationFinding(t *testing.T, source []any, code string) map[string]any {
	t.Helper()
	for _, item := range source {
		finding, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("finding = %#v, want object", item)
		}
		if finding["code"] == code {
			return finding
		}
	}
	t.Fatalf("findings = %v, want code %s", source, code)
	return nil
}

// requireAdminMigrationMutationMessage finds a recommended command in the
// planned-mutation envelope without coupling tests to other recommendations.
func requireAdminMigrationMutationMessage(t *testing.T, source []any, message string) map[string]any {
	t.Helper()
	for _, item := range source {
		mutation, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("planned mutation = %#v, want object", item)
		}
		if mutation["message"] == message {
			return mutation
		}
	}
	t.Fatalf("planned mutations = %v, want message %q", source, message)
	return nil
}

// requireAdminMigrationPayloadHash verifies the manifest checksum and size for
// a bundle payload against the bytes actually written on disk.
func requireAdminMigrationPayloadHash(t *testing.T, root string, payloads []adminMigrationBackupPayload, path string) adminMigrationBackupPayload {
	t.Helper()
	for _, payload := range payloads {
		if payload.Path != path {
			continue
		}
		data := readAdminMigrationTestFile(t, root, path)
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); payload.SHA256 != got {
			t.Fatalf("payload %s sha256 = %s, want %s", path, payload.SHA256, got)
		}
		if payload.Bytes != int64(len(data)) {
			t.Fatalf("payload %s bytes = %d, want %d", path, payload.Bytes, len(data))
		}
		return payload
	}
	t.Fatalf("payloads = %#v, want %s", payloads, path)
	return adminMigrationBackupPayload{}
}

// writeAdminMigrationRestoreTestBundle creates a valid logical backup bundle
// that restore-preflight tests can inspect without invoking backup create first.
func writeAdminMigrationRestoreTestBundle(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "opencook-backup")
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	inventory := adminMigrationInventoryFromState(
		bootstrapState,
		coreObjectState,
		map[string]adminMigrationCookbookInventory{},
		"",
	)
	if _, err := adminMigrationWriteBackupBundle(root, adminMigrationBackupBundleInput{
		Build:       version.Info{Version: "test-version", Commit: "test-commit", BuiltAt: "test-built-at"},
		CreatedAt:   mustParseAdminMigrationTime(t, "2026-04-28T12:00:00Z"),
		Config:      config.Config{PostgresDSN: "postgres://source:secret@example/opencook"},
		Bootstrap:   bootstrapState,
		CoreObjects: coreObjectState,
		Cookbooks:   adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
		Inventory:   inventory,
	}); err != nil {
		t.Fatalf("adminMigrationWriteBackupBundle() error = %v", err)
	}
	return root
}

// writeAdminMigrationRestoreTestBundleWithBlobAndCookbook creates a restore
// fixture that exercises blob byte restore and cookbook metadata import paths.
func writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t *testing.T) (string, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "opencook-backup")
	body := []byte("rainbow")
	sum := md5.Sum(body)
	checksum := hex.EncodeToString(sum[:])
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	orgObjects := coreObjectState.Orgs["ponyville"]
	orgObjects.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox1": {ID: "sandbox1", Organization: "ponyville", Checksums: []string{checksum}},
	}
	coreObjectState.Orgs["ponyville"] = orgObjects
	cookbooks := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{
		"ponyville": {
			Versions: []bootstrap.CookbookVersion{{
				Name:         "app",
				CookbookName: "app",
				Version:      "1.2.3",
				AllFiles:     []bootstrap.CookbookFile{{Name: "default.rb", Path: "recipes/default.rb", Checksum: checksum, Specificity: "default"}},
			}},
		},
	}}
	inventory := adminMigrationInventoryFromState(
		bootstrapState,
		coreObjectState,
		map[string]adminMigrationCookbookInventory{"ponyville": {Versions: 1, ChecksumReferences: 1, Checksums: []string{checksum}}},
		"",
	)
	if _, err := adminMigrationWriteBackupBundle(root, adminMigrationBackupBundleInput{
		Build:       version.Info{Version: "test-version", Commit: "test-commit", BuiltAt: "test-built-at"},
		CreatedAt:   mustParseAdminMigrationTime(t, "2026-04-28T12:00:00Z"),
		Config:      config.Config{PostgresDSN: "postgres://source:secret@example/opencook"},
		Bootstrap:   bootstrapState,
		CoreObjects: coreObjectState,
		Cookbooks:   cookbooks,
		BlobCopies:  []adminMigrationBackupBlobData{{Checksum: checksum, Body: body}},
		Inventory:   inventory,
	}); err != nil {
		t.Fatalf("adminMigrationWriteBackupBundle() error = %v", err)
	}
	return root, checksum
}

// mustReadAdminMigrationBackupManifest loads the bundle manifest for tests that
// need to assert payload hashes after invoking the CLI.
func mustReadAdminMigrationBackupManifest(t *testing.T, root string) adminMigrationBackupManifest {
	t.Helper()
	var manifest adminMigrationBackupManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationBackupManifestPath), &manifest); err != nil {
		t.Fatalf("manifest json.Unmarshal error = %v", err)
	}
	return manifest
}

// readAdminMigrationTestFile reads a bundle file relative to the test root and
// fails the test with the relative path for easier diagnosis.
func readAdminMigrationTestFile(t *testing.T, root, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	return data
}

// readAdminMigrationBundleText concatenates bundle files so redaction tests can
// prove secrets and private keys are absent from every generated payload.
func readAdminMigrationBundleText(t *testing.T, root string) string {
	t.Helper()
	var builder strings.Builder
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		builder.Write(data)
		return nil
	}); err != nil {
		t.Fatalf("WalkDir(%s) error = %v", root, err)
	}
	return builder.String()
}

// mustParseAdminMigrationTime keeps backup manifest tests deterministic without
// obscuring parsing failures in fixture timestamps.
func mustParseAdminMigrationTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("time.Parse(%q) error = %v", raw, err)
	}
	return parsed
}

// adminMigrationArrayContainsString checks warning text by substring so tests
// stay resilient to more descriptive operational wording.
func adminMigrationArrayContainsString(source []any, want string) bool {
	for _, item := range source {
		text, _ := item.(string)
		if strings.Contains(text, want) {
			return true
		}
	}
	return false
}

// boolField treats omitted boolean fields as false, matching the CLI JSON
// omitempty behavior used for dry-run, offline, and confirmation gates.
func boolField(source map[string]any, key string) bool {
	value, _ := source[key].(bool)
	return value
}
