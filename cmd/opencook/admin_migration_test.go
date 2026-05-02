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
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authn"
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

func TestAdminMigrationMaintenancePolicyWarnings(t *testing.T) {
	for _, tc := range []struct {
		name string
		out  adminMigrationCLIOutput
		want string
	}{
		{
			name: "confirmed backup warns offline",
			out:  adminMigrationCLIOutput{Command: "migration_backup_create", Confirmed: true, Offline: true},
			want: "offline-gated",
		},
		{
			name: "confirmed restore warns follow-up reindex",
			out:  adminMigrationCLIOutput{Command: "migration_restore_apply", Confirmed: true, Offline: true},
			want: "follow-up reindex",
		},
		{
			name: "confirmed source import warns stopped target",
			out:  adminMigrationCLIOutput{Command: "migration_source_import_apply", Confirmed: true, Offline: true},
			want: "stopped OpenCook target",
		},
		{
			name: "confirmed source sync warns frozen writes",
			out:  adminMigrationCLIOutput{Command: "migration_source_sync_apply", Confirmed: true, Offline: true},
			want: "writes frozen",
		},
		{
			name: "cutover rehearsal warns source freeze",
			out:  adminMigrationCLIOutput{Command: "migration_cutover_rehearse"},
			want: "source Chef writes frozen",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			warnings := adminMigrationMaintenancePolicyWarnings(tc.out)
			if len(warnings) != 1 || !strings.Contains(warnings[0], tc.want) {
				t.Fatalf("warnings = %v, want one warning containing %q", warnings, tc.want)
			}
		})
	}

	if warnings := adminMigrationMaintenancePolicyWarnings(adminMigrationCLIOutput{Command: "migration_restore_apply", DryRun: true, Offline: true}); len(warnings) != 0 {
		t.Fatalf("dry-run warnings = %v, want none", warnings)
	}
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

func TestAdminMigrationInspectBackupBundleRequiresRestorePayloads(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	manifest := mustReadAdminMigrationBackupManifest(t, bundlePath)
	filteredPayloads := make([]adminMigrationBackupPayload, 0, len(manifest.Payloads))
	for _, payload := range manifest.Payloads {
		if payload.Path == adminMigrationBackupCookbooksPath {
			continue
		}
		filteredPayloads = append(filteredPayloads, payload)
	}
	manifest.Payloads = filteredPayloads
	rawManifest, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent(manifest) error = %v", err)
	}
	rawManifest = append(rawManifest, '\n')
	if err := os.WriteFile(filepath.Join(bundlePath, adminMigrationBackupManifestPath), rawManifest, 0o644); err != nil {
		t.Fatalf("WriteFile(manifest without cookbooks payload) error = %v", err)
	}

	_, findings, err := adminMigrationInspectBackupBundle(bundlePath)
	if err == nil {
		t.Fatalf("adminMigrationInspectBackupBundle() error = nil, want missing required payload")
	}
	foundMissingPayload := false
	for _, finding := range findings {
		if finding.Code == "backup_required_payload_missing" {
			foundMissingPayload = true
			break
		}
	}
	if !foundMissingPayload {
		t.Fatalf("findings = %#v, want backup_required_payload_missing", findings)
	}
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

func TestAdminMigrationRestoreApplyRefusesCookbookOnlyTargetState(t *testing.T) {
	bundlePath, _ := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore: &fakeOfflineStore{
			bootstrap: bootstrap.BootstrapCoreState{},
			objects:   bootstrap.CoreObjectState{},
		},
		cookbookInventory: map[string]adminMigrationCookbookInventory{"ponyville": {Versions: 1}},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "memory-compat", Configured: true, Message: "test blob backend"},
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
		t.Fatalf("Run(migration restore apply cookbook-only target) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_target", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "restore_target_not_empty")
	if targetStore.restoredCookbooks.Orgs != nil {
		t.Fatalf("restore should not import cookbooks into non-empty target: %#v", targetStore.restoredCookbooks)
	}
}

func TestPostgresAdminOfflineStoreRestoreCookbookExportRollsBackPartialCookbooks(t *testing.T) {
	errArtifact := errors.New("artifact conflict")
	cookbookStore := &fakeMigrationCookbookStore{
		artifactErr:       errArtifact,
		artifactErrForID:  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		artifactDeleteErr: nil,
	}
	store := postgresAdminOfflineStore{cookbooks: cookbookStore}
	bootstrapState, _ := adminMigrationHealthyStates(t)
	export := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{
		"ponyville": {
			Versions: []bootstrap.CookbookVersion{{
				Name:         "app-1.2.3",
				CookbookName: "app",
				Version:      "1.2.3",
			}},
			Artifacts: []bootstrap.CookbookArtifact{{
				Name:       "artifact-app",
				Identifier: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Version:    "1.2.3",
			}, {
				Name:       "artifact-app",
				Identifier: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Version:    "1.2.4",
			}},
		},
	}}

	err := store.RestoreCookbookExport(bootstrapState, export)
	if !errors.Is(err, errArtifact) {
		t.Fatalf("RestoreCookbookExport() error = %v, want artifact conflict", err)
	}
	if got, want := cookbookStore.deletedArtifacts, []string{"ponyville/artifact-app/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}; !sameAdminStrings(got, want) {
		t.Fatalf("deleted artifacts = %v, want %v", got, want)
	}
	if got, want := cookbookStore.deletedVersions, []string{"ponyville/app/1.2.3"}; !sameAdminStrings(got, want) {
		t.Fatalf("deleted versions = %v, want %v", got, want)
	}
}

func TestPostgresAdminOfflineStoreSyncCookbooksRejectsMissingBootstrapOrg(t *testing.T) {
	cookbookStore := &fakeMigrationCookbookStore{}
	store := postgresAdminOfflineStore{cookbooks: cookbookStore}
	current := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}}
	desired := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{
		"phantom": {
			Versions: []bootstrap.CookbookVersion{{
				Name:         "app-1.2.3",
				CookbookName: "app",
				Version:      "1.2.3",
			}},
		},
	}}
	scopes := map[adminMigrationSourcePayloadKey]bool{{Organization: "phantom", Family: "cookbook_versions"}: true}

	err := store.applySyncedCookbookExport(bootstrap.BootstrapCoreState{Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{}}, current, desired, scopes)
	if err == nil || !strings.Contains(err.Error(), "missing from bootstrap core state") {
		t.Fatalf("applySyncedCookbookExport() error = %v, want missing bootstrap org", err)
	}
	if len(cookbookStore.ensuredOrgs) != 0 {
		t.Fatalf("ensured orgs = %#v, want none for missing bootstrap org", cookbookStore.ensuredOrgs)
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

func TestAdminMigrationSourceInventoryReadsChefSourceContractFixture(t *testing.T) {
	root := adminMigrationSourceFixturePath(t, "chef-source-import", "v1")

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", root, "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(migration source inventory chef source fixture) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	dep := requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_artifact", "ok")
	details := requireAdminMigrationMap(t, dep, "details")
	if details["source_type"] != "normalized_chef_source_fixture" {
		t.Fatalf("source_type = %v, want normalized_chef_source_fixture", details["source_type"])
	}
	if details["format_version"] != adminMigrationChefSourceFormatV1 {
		t.Fatalf("format_version = %v, want %s", details["format_version"], adminMigrationChefSourceFormatV1)
	}

	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	for _, want := range []struct {
		org    string
		family string
		count  int
	}{
		{family: "users", count: 1},
		{family: "user_acls", count: 1},
		{family: "user_keys", count: 1},
		{family: "server_admin_memberships", count: 1},
		{family: "organizations", count: 1},
		{org: "ponyville", family: "clients", count: 1},
		{org: "ponyville", family: "client_keys", count: 1},
		{org: "ponyville", family: "groups", count: 4},
		{org: "ponyville", family: "group_memberships", count: 4},
		{org: "ponyville", family: "containers", count: 12},
		{org: "ponyville", family: "acls", count: 24},
		{org: "ponyville", family: "nodes", count: 1},
		{org: "ponyville", family: "environments", count: 1},
		{org: "ponyville", family: "roles", count: 1},
		{org: "ponyville", family: "data_bags", count: 1},
		{org: "ponyville", family: "data_bag_items", count: 2},
		{org: "ponyville", family: "policy_revisions", count: 1},
		{org: "ponyville", family: "policy_groups", count: 1},
		{org: "ponyville", family: "policy_assignments", count: 1},
		{org: "ponyville", family: "sandboxes", count: 1},
		{org: "ponyville", family: "checksum_references", count: 2},
		{org: "ponyville", family: "cookbook_versions", count: 1},
		{org: "ponyville", family: "cookbook_artifacts", count: 1},
		{family: "cookbook_blob_references", count: 1},
		{family: "opensearch_source_artifacts", count: 1},
	} {
		requireAdminMigrationInventoryFamily(t, families, want.org, want.family, want.count)
	}

	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "source_import_not_implemented")
	requireAdminMigrationFinding(t, findings, "source_search_rebuild_required")
	requireAdminMigrationFindingFamily(t, findings, "source_artifact_unsupported", "oc_id")
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

func TestAdminMigrationSourceInventoryScansChefSourceArchiveTaxonomy(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "chef-source-taxonomy.tar.gz")
	writeAdminMigrationSourceArchive(t, archivePath, map[string]string{
		"users/pivotal.json":                                                `{"username":"pivotal"}`,
		"user_acls/pivotal.json":                                            `{}`,
		"user_keys/pivotal/default.json":                                    `{}`,
		"server_admin_memberships/pivotal.json":                             `{}`,
		"organizations/ponyville/clients/ponyville-validator.json":          `{}`,
		"organizations/ponyville/client_keys/ponyville-validator.json":      `{}`,
		"organizations/ponyville/groups/admins.json":                        `{}`,
		"organizations/ponyville/group_memberships/admins-pivotal.json":     `{}`,
		"organizations/ponyville/containers/clients.json":                   `{}`,
		"organizations/ponyville/acls/organization.json":                    `{}`,
		"organizations/ponyville/nodes/web01.json":                          `{}`,
		"organizations/ponyville/environments/_default.json":                `{}`,
		"organizations/ponyville/roles/web.json":                            `{}`,
		"organizations/ponyville/data_bags/secrets/":                        "",
		"organizations/ponyville/data_bags/secrets/db.json":                 `{}`,
		"organizations/ponyville/policies/base/revision.json":               `{}`,
		"organizations/ponyville/policy_groups/prod.json":                   `{}`,
		"organizations/ponyville/policy_assignments/prod-base.json":         `{}`,
		"organizations/ponyville/sandboxes/sandbox-fixture.json":            `{}`,
		"organizations/ponyville/checksum_references/aaaaaaaaaaaaaaaa.json": `{}`,
		"organizations/ponyville/cookbooks/apache2-1.0.0/metadata.json":     `{}`,
		"organizations/ponyville/cookbook_artifacts/apache2/artifact.json":  `{}`,
		"bookshelf/objects/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":                "fixture bytes",
		"opensearch/chef/node/web01.json":                                   `{}`,
		"oc-id/users.json":                                                  `{}`,
	})

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "inventory", archivePath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source inventory taxonomy archive) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "user_acls", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "user_keys", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "server_admin_memberships", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	for _, family := range []string{"clients", "client_keys", "groups", "group_memberships", "containers", "acls", "nodes", "environments", "roles", "data_bags", "data_bag_items", "policy_revisions", "policy_groups", "policy_assignments", "sandboxes", "checksum_references", "cookbook_versions", "cookbook_artifacts"} {
		requireAdminMigrationInventoryFamily(t, families, "ponyville", family, 1)
	}
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", 1)
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "source_search_rebuild_required")
	requireAdminMigrationFindingFamily(t, findings, "source_artifact_unsupported", "oc_id")
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

func TestAdminMigrationSourceNormalizeWritesDeterministicBundleFromFixture(t *testing.T) {
	sourceRoot := adminMigrationSourceFixturePath(t, "chef-source-import", "v1")
	outputPath := filepath.Join(t.TempDir(), "normalized-source")

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize fixture) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_normalize" {
		t.Fatalf("command = %v, want migration_source_normalize", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["source_path"] != sourceRoot {
		t.Fatalf("target.source_path = %v, want %s", target["source_path"], sourceRoot)
	}
	if target["output_path"] != outputPath {
		t.Fatalf("target.output_path = %v, want %s", target["output_path"], outputPath)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_artifact", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "ok")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "wrote normalized source manifest and deterministic payload files")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_search_rebuild_required")
	requireAdminMigrationFindingFamily(t, requireAdminMigrationArray(t, out, "findings"), "source_artifact_unsupported", "oc_id")

	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	if manifest.FormatVersion != adminMigrationChefSourceFormatV1 {
		t.Fatalf("format_version = %s, want %s", manifest.FormatVersion, adminMigrationChefSourceFormatV1)
	}
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/users.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/user_keys.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/client_keys.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/groups.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/containers.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/acls.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/nodes.json")
	users := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/bootstrap/users.json")
	if got := users[0]["username"]; got != "pivotal" {
		t.Fatalf("users[0].username = %v, want pivotal", got)
	}
	userKeys := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/bootstrap/user_keys.json")
	if publicKey, _ := userKeys[0]["public_key"].(string); !strings.Contains(publicKey, "BEGIN PUBLIC KEY") {
		t.Fatalf("user key public_key = %q, want PEM public key", publicKey)
	}
	if _, ok := userKeys[0]["private_key"]; ok {
		t.Fatalf("user key payload leaked private_key: %v", userKeys[0])
	}
	if got := userKeys[0]["expiration_date"]; got != "infinity" {
		t.Fatalf("user key expiration_date = %v, want infinity", got)
	}
	groups := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/groups.json")
	if got := len(groups); got != 4 {
		t.Fatalf("groups count = %d, want 4 default groups", got)
	}
	containers := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/containers.json")
	if got := len(containers); got != 12 {
		t.Fatalf("containers count = %d, want 12 default containers", got)
	}
	acls := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/acls.json")
	requireAdminMigrationSourceACLResource(t, acls, "organization:ponyville")
	requireAdminMigrationSourceACLResource(t, acls, "client:ponyville-validator")
	requireAdminMigrationSourceACLResource(t, acls, "group:admins")
	requireAdminMigrationSourceACLResource(t, acls, "container:nodes")
	dataBagItems := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/data_bag_items.json")
	encryptedItem := requireAdminMigrationSourcePayloadObject(t, dataBagItems, "id", "db")
	encryptedPayload := requireAdminMigrationMap(t, encryptedItem, "payload")
	if got := encryptedPayload["encrypted_data"]; got != "fixture" {
		t.Fatalf("encrypted data bag payload encrypted_data = %v, want fixture", got)
	}
	plainItem := requireAdminMigrationSourcePayloadObject(t, dataBagItems, "id", "plain")
	plainPayload := requireAdminMigrationMap(t, plainItem, "payload")
	if got := plainPayload["value"]; got != "cleartext" {
		t.Fatalf("plain data bag payload value = %v, want cleartext", got)
	}
	policies := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/policy_revisions.json")
	if got := policies[0]["revision_id"]; got != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("policy revision_id = %v, want canonical hex revision", got)
	}
	sandboxes := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/sandboxes.json")
	if got := sandboxes[0]["completed"]; got != true {
		t.Fatalf("sandbox completed = %v, want true", got)
	}
	cookbookVersions := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/cookbook_versions.json")
	cookbookVersion := requireAdminMigrationSourcePayloadObject(t, cookbookVersions, "cookbook_name", "apache2")
	versionMetadata := requireAdminMigrationMap(t, cookbookVersion, "metadata")
	versionDependencies := requireAdminMigrationMap(t, versionMetadata, "dependencies")
	if got := versionDependencies["apt"]; got != ">= 1.0.0" {
		t.Fatalf("cookbook metadata dependency apt = %v, want >= 1.0.0", got)
	}
	versionFiles := requireAdminMigrationArray(t, cookbookVersion, "all_files")
	if len(versionFiles) != 1 {
		t.Fatalf("cookbook all_files count = %d, want 1", len(versionFiles))
	}
	versionFile, ok := versionFiles[0].(map[string]any)
	if !ok || versionFile["checksum"] != "364c08d77216cb1c1c5b3826de020dc3" {
		t.Fatalf("cookbook all_files[0] = %#v, want normalized checksum file", versionFiles[0])
	}
	artifacts := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/cookbook_artifacts.json")
	artifact := requireAdminMigrationSourcePayloadObject(t, artifacts, "identifier", "1111111111111111111111111111111111111111")
	artifactFiles := requireAdminMigrationArray(t, artifact, "all_files")
	if len(artifactFiles) != 1 {
		t.Fatalf("artifact all_files count = %d, want 1", len(artifactFiles))
	}
	if _, err := os.Stat(filepath.Join(outputPath, "blobs", "checksums", "364c08d77216cb1c1c5b3826de020dc3")); err != nil {
		t.Fatalf("normalized blob artifact missing: %v", err)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("output missing duration_ms: %v", out)
	}
}

func TestAdminMigrationSourcePayloadMaterializationPreservesEmptyFamilies(t *testing.T) {
	key := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "nodes"}
	files := map[string][]byte{}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(map[adminMigrationSourcePayloadKey][]json.RawMessage{
		key: []json.RawMessage{},
	}, files)
	if err != nil {
		t.Fatalf("adminMigrationMaterializeSourcePayloadFiles() error = %v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("payloads = %#v, want one zero-count family", payloads)
	}
	payload := payloads[0]
	if payload.Path != "payloads/organizations/ponyville/nodes.json" || payload.Count != 0 {
		t.Fatalf("payload = %#v, want zero-count ponyville nodes payload", payload)
	}
	if got := string(files[payload.Path]); got != "[\n]\n" {
		t.Fatalf("payload file = %q, want empty canonical array", got)
	}

	read := adminMigrationSourceImportRead{PayloadValues: map[adminMigrationSourcePayloadKey][]json.RawMessage{key: []json.RawMessage{}}}
	scopes := adminMigrationSourceSyncCoveredScopes(read)
	if !scopes[key] {
		t.Fatalf("source sync scopes = %#v, want empty nodes family covered", scopes)
	}
	diff := adminMigrationSourceSyncDiffStates(
		adminMigrationSourceImportState{CoreObjects: bootstrap.CoreObjectState{Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {Nodes: map[string]bootstrap.Node{}},
		}}},
		adminMigrationSourceImportState{CoreObjects: bootstrap.CoreObjectState{Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {Nodes: map[string]bootstrap.Node{"stale": {Name: "stale"}}},
		}}},
		scopes,
	)
	if diff.DeleteCount != 1 || !diff.HasChanges {
		t.Fatalf("diff = %#v, want target-only node delete from empty covered family", diff)
	}
}

func TestAdminMigrationSourceNormalizeRejectsInvalidIdentityFamilies(t *testing.T) {
	for _, tc := range []struct {
		name        string
		build       func(t *testing.T, root string)
		wantCode    string
		useManifest bool
	}{
		{
			name: "duplicate users",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "one.json"), map[string]string{"username": "pivotal"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "two.json"), map[string]string{"username": "pivotal"})
			},
			wantCode: "source_duplicate_user",
		},
		{
			name: "invalid user key",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "pivotal.json"), map[string]string{"username": "pivotal"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "user_keys", "pivotal", "default.json"), map[string]string{"username": "pivotal", "key_name": "default", "public_key": "ssh-rsa nope"})
			},
			wantCode: "source_key_invalid",
		},
		{
			name: "orphan group membership",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "pivotal.json"), map[string]string{"username": "pivotal"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "groups", "admins.json"), map[string]string{"name": "admins"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "group_memberships", "admins-missing.json"), map[string]string{"group": "admins", "actor": "missing", "type": "user"})
			},
			wantCode: "source_orphan_group_membership",
		},
		{
			name: "invalid acl document",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "users", "pivotal.json"), map[string]string{"username": "pivotal"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "user_acls", "pivotal.json"), map[string]any{"resource": "user:pivotal", "read": []string{"not", "an", "object"}})
			},
			wantCode: "source_acl_invalid",
		},
		{
			name: "missing organization reference",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "payloads", "organizations", "ponyville", "clients.json"), []map[string]any{{"name": "ponyville-validator", "validator": true}})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, adminMigrationSourceManifestPath), adminMigrationSourceManifest{
					FormatVersion: adminMigrationChefSourceFormatV1,
					SourceType:    "invalid_missing_org",
					Payloads: []adminMigrationSourceManifestPayload{
						{Organization: "ponyville", Family: "clients", Path: "payloads/organizations/ponyville/clients.json", Count: 1},
					},
				})
			},
			wantCode: "source_missing_organization",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceRoot := t.TempDir()
			outputPath := filepath.Join(t.TempDir(), "normalized-source")
			tc.build(t, sourceRoot)

			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(migration source normalize %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantCode)
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("output stat error = %v, want not exist", err)
			}
		})
	}
}

func TestAdminMigrationSourceNormalizeRejectsInvalidCoreObjectFamilies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    func(t *testing.T, root string)
		wantCode string
	}{
		{
			name: "invalid policy revision",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "policies", "base", "revision.json"), map[string]string{"name": "base", "revision_id": "not-a-revision"})
			},
			wantCode: "source_policy_revision_invalid",
		},
		{
			name: "orphan policy assignment",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "policy_groups", "prod.json"), map[string]string{"name": "prod"})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "policy_assignments", "prod-base.json"), map[string]string{
					"group":       "prod",
					"policy":      "base",
					"revision_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				})
			},
			wantCode: "source_orphan_policy_assignment",
		},
		{
			name: "incomplete sandbox",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "sandboxes", "sandbox-fixture.json"), map[string]any{
					"id":        "sandbox-fixture",
					"completed": false,
					"checksums": []string{},
				})
			},
			wantCode: "source_sandbox_incomplete",
		},
		{
			name: "missing checksum reference",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "sandboxes", "sandbox-fixture.json"), map[string]any{
					"id":        "sandbox-fixture",
					"completed": true,
					"checksums": []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				})
			},
			wantCode: "source_checksum_reference_missing",
		},
		{
			name: "orphan object acl",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "acls", "node-missing.json"), map[string]string{"resource": "node:missing"})
			},
			wantCode: "source_acl_target_missing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceRoot := t.TempDir()
			outputPath := filepath.Join(t.TempDir(), "normalized-source")
			tc.build(t, sourceRoot)

			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(migration source normalize %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantCode)
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("output stat error = %v, want not exist", err)
			}
		})
	}
}

func TestAdminMigrationSourceNormalizeReportsUnknownObjectFamilies(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "organizations", "ponyville", "mystery_objects", "thing.json"), map[string]string{"name": "thing"})

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize unknown family) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_family_unsupported")
	mustReadAdminMigrationSourceManifest(t, outputPath)
}

func TestAdminMigrationSourceNormalizeWarnsWhenCookbookBlobPayloadIsMissing(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "organizations", "ponyville", "cookbooks", "app-1.0.0", "metadata.json"), adminMigrationTestCookbookVersionPayload("app", "1.0.0", checksum))

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize missing blob) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_blob_payload_missing")
	cookbooks := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/cookbook_versions.json")
	files := requireAdminMigrationArray(t, cookbooks[0], "all_files")
	if len(files) != 1 {
		t.Fatalf("cookbook all_files count = %d, want 1", len(files))
	}
}

func TestAdminMigrationSourceNormalizeRejectsInvalidCookbookFamilies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    func(t *testing.T, root string)
		wantCode string
	}{
		{
			name: "duplicate cookbook version",
			build: func(t *testing.T, root string) {
				payload := adminMigrationTestCookbookVersionPayload("app", "1.0.0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "cookbooks", "app-1.0.0", "one.json"), payload)
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "cookbooks", "app-1.0.0", "two.json"), payload)
			},
			wantCode: "source_duplicate_cookbook_version",
		},
		{
			name: "route payload mismatch",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "cookbooks", "app-1.0.0", "metadata.json"), adminMigrationTestCookbookVersionPayload("other", "1.0.0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
			},
			wantCode: "source_cookbook_route_mismatch",
		},
		{
			name: "malformed cookbook version",
			build: func(t *testing.T, root string) {
				payload := adminMigrationTestCookbookVersionPayload("app", "1.0.0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
				payload["version"] = "1.two.0"
				payload["name"] = "app-1.two.0"
				metadata := payload["metadata"].(map[string]any)
				metadata["version"] = "1.two.0"
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "payloads", "cookbook_versions.json"), []map[string]any{payload})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, adminMigrationSourceManifestPath), adminMigrationSourceManifest{
					FormatVersion: adminMigrationChefSourceFormatV1,
					SourceType:    "invalid_cookbook_version",
					Payloads: []adminMigrationSourceManifestPayload{
						{Organization: "ponyville", Family: "organizations", Path: "payloads/organizations/ponyville/organization.json", Count: 1},
						{Organization: "ponyville", Family: "cookbook_versions", Path: "organizations/ponyville/payloads/cookbook_versions.json", Count: 1},
					},
				})
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "payloads", "organizations", "ponyville", "organization.json"), []map[string]any{{"name": "ponyville"}})
			},
			wantCode: "source_cookbook_version_invalid",
		},
		{
			name: "invalid artifact identifier",
			build: func(t *testing.T, root string) {
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "cookbook_artifacts", "app.json"), map[string]any{
					"name":       "app",
					"identifier": "not-a-valid-identifier",
					"version":    "1.0.0",
					"metadata":   map[string]any{"name": "app", "version": "1.0.0"},
				})
			},
			wantCode: "source_cookbook_artifact_invalid",
		},
		{
			name: "copied blob checksum mismatch",
			build: func(t *testing.T, root string) {
				checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
				writeAdminMigrationSourceJSON(t, filepath.Join(root, "organizations", "ponyville", "cookbooks", "app-1.0.0", "metadata.json"), adminMigrationTestCookbookVersionPayload("app", "1.0.0", checksum))
				writeAdminMigrationSourceFile(t, filepath.Join(root, "bookshelf", "objects", checksum), "wrong blob bytes")
			},
			wantCode: "source_blob_checksum_mismatch",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceRoot := t.TempDir()
			outputPath := filepath.Join(t.TempDir(), "normalized-source")
			tc.build(t, sourceRoot)

			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(migration source normalize %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantCode)
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("output stat error = %v, want not exist", err)
			}
		})
	}
}

func TestAdminMigrationSourceNormalizeReportsUnsupportedCookbookSourceLayouts(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	writeAdminMigrationSourceFile(t, filepath.Join(sourceRoot, "organizations", "ponyville", "cookbooks", "app", "recipes", "default.rb"), "package 'apache2'")

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize unsupported cookbook layout) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_cookbook_layout_unsupported")
	mustReadAdminMigrationSourceManifest(t, outputPath)
}

func TestAdminMigrationSourceNormalizeWritesPayloadsFromExtractedDirectory(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "users", "pivotal.json"), map[string]string{"username": "pivotal"})
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "organizations", "ponyville", "nodes", "web01.json"), map[string]string{"name": "web01"})
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "organizations", "ponyville", "data_bags", "secrets", "db.json"), map[string]string{"id": "db"})
	writeAdminMigrationSourceFile(t, filepath.Join(sourceRoot, "bookshelf", "ecb15403db3c9ae6cd17a6ac822d2c55"), "cookbook bytes")
	writeAdminMigrationSourceFile(t, filepath.Join(sourceRoot, "opensearch", "chef", "node", "web01.json"), `{"derived":true}`)
	writeAdminMigrationSourceFile(t, filepath.Join(sourceRoot, "oc-id", "users.json"), `{"ignored":true}`)

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize directory) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "nodes", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "data_bags", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "data_bag_items", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", 1)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_search_rebuild_required")
	requireAdminMigrationFindingFamily(t, requireAdminMigrationArray(t, out, "findings"), "source_artifact_unsupported", "oc_id")

	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/users.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/organization.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/data_bags.json")
	dataBags := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/data_bags.json")
	if got := dataBags[0]["name"]; got != "secrets" {
		t.Fatalf("data_bags[0].name = %v, want secrets", got)
	}
	dataBagItems := decodeAdminMigrationSourcePayload(t, outputPath, "payloads/organizations/ponyville/data_bag_items.json")
	if got := dataBagItems[0]["bag"]; got != "secrets" {
		t.Fatalf("data_bag_items[0].bag = %v, want secrets from source path", got)
	}
	itemPayload := requireAdminMigrationMap(t, dataBagItems[0], "payload")
	if got := itemPayload["id"]; got != "db" {
		t.Fatalf("data_bag_items[0].payload.id = %v, want db", got)
	}
	if _, err := os.Stat(filepath.Join(outputPath, "derived", "opensearch", "opensearch", "chef", "node", "web01.json")); err != nil {
		t.Fatalf("normalized derived search artifact missing: %v", err)
	}
}

func TestAdminMigrationSourceNormalizeScansArchiveAndRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	archivePath := filepath.Join(root, "chef-source.tar.gz")
	outputPath := filepath.Join(root, "normalized-source")
	writeAdminMigrationSourceArchive(t, archivePath, map[string]string{
		"users/pivotal.json":                                       `{"username":"pivotal"}`,
		"organizations/ponyville/nodes/web01.json":                 `{"name":"web01"}`,
		"bookshelf/objects/ecb15403db3c9ae6cd17a6ac822d2c55":       "cookbook bytes",
		"organizations/ponyville/data_bags/secrets/":               "",
		"organizations/ponyville/data_bags/secrets/db.json":        `{"id":"db"}`,
		"organizations/ponyville/policy_groups/prod.json":          `{"name":"prod"}`,
		"organizations/ponyville/policies/base/revision.json":      `{"name":"base","revision_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
		"organizations/ponyville/policy_assignments/prod-app.json": `{"group":"prod","policy":"base","revision_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`,
	})

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", archivePath, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize archive) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/users.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/nodes.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/policy_assignments.json")

	traversalArchive := filepath.Join(root, "chef-source-traversal.tar.gz")
	traversalOutput := filepath.Join(root, "traversal-output")
	writeAdminMigrationSourceArchive(t, traversalArchive, map[string]string{"../evil.json": `{}`})
	traversalCmd, traversalStdout, traversalStderr := newTestCommand(t)
	code = traversalCmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", traversalArchive, "--output", traversalOutput, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source normalize traversal) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, traversalStdout.String(), traversalStderr.String())
	}
	out := decodeAdminMigrationOutput(t, traversalStdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_path_unsafe")
	if _, err := os.Stat(traversalOutput); !os.IsNotExist(err) {
		t.Fatalf("traversal output stat error = %v, want not exist", err)
	}
}

func TestAdminMigrationSourceNormalizeRejectsMalformedJSONAndExistingOutput(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	writeAdminMigrationSourceFile(t, filepath.Join(sourceRoot, "users", "pivotal.json"), `{"username":"pivotal"} trailing`)

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source normalize malformed) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_payload_invalid_json")
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("malformed output stat error = %v, want not exist", err)
	}

	sourceRoot = adminMigrationSourceFixturePath(t, "chef-source-import", "v1")
	existingOutput := filepath.Join(t.TempDir(), "existing-normalized-source")
	writeAdminMigrationSourceFile(t, filepath.Join(existingOutput, "sentinel.txt"), "keep me")
	existingCmd, existingStdout, existingStderr := newTestCommand(t)
	code = existingCmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", existingOutput, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source normalize existing) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, existingStdout.String(), existingStderr.String())
	}
	existingOut := decodeAdminMigrationOutput(t, existingStdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, existingOut, "findings"), "source_normalize_output_exists")
	if got := string(readAdminMigrationTestFile(t, existingOutput, "sentinel.txt")); got != "keep me" {
		t.Fatalf("sentinel = %q, want keep me", got)
	}
	overwriteCmd, overwriteStdout, overwriteStderr := newTestCommand(t)
	code = overwriteCmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", existingOutput, "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize overwrite) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, overwriteStdout.String(), overwriteStderr.String())
	}
	if _, err := os.Stat(filepath.Join(existingOutput, "sentinel.txt")); !os.IsNotExist(err) {
		t.Fatalf("sentinel stat error = %v, want not exist after --yes overwrite", err)
	}
	mustReadAdminMigrationSourceManifest(t, existingOutput)
}

func TestAdminMigrationSourceNormalizeRedactsSecretLookingPaths(t *testing.T) {
	sourceRoot := filepath.Join(t.TempDir(), "token-secret-source")
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "users", "pivotal.json"), map[string]string{"username": "pivotal"})

	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "normalize", sourceRoot, "--output", outputPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source normalize redaction) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "token-secret-source") {
		t.Fatalf("stdout leaked secret-looking source path: %s", stdout.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	target := requireAdminMigrationMap(t, out, "target")
	if target["source_path"] != "redacted_path" {
		t.Fatalf("target.source_path = %v, want redacted_path", target["source_path"])
	}
	mustReadAdminMigrationSourceManifest(t, outputPath)
}

func TestAdminMigrationSourceImportPreflightPlansEmptyTarget(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
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
		return config.Config{
			DefaultOrganization:    "ponyville",
			PostgresDSN:            "postgres://opencook",
			BlobBackend:            "memory",
			BootstrapRequestorName: "pivotal",
			BootstrapRequestorType: "user",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source import preflight empty target) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_import_preflight" {
		t.Fatalf("command = %v, want migration_source_import_preflight", out["command"])
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "source_bundle", "ok")
	requireAdminMigrationDependency(t, deps, "postgres", "ok")
	requireAdminMigrationDependency(t, deps, "source_import_target", "ok")
	requireAdminMigrationDependency(t, deps, "blob", "ok")
	requireAdminMigrationDependency(t, deps, "opensearch", "unconfigured")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 1)
	requireAdminMigrationInventoryFamily(t, families, "ponyville", "cookbook_versions", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "referenced_blobs", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "copied_blobs", 1)
	mutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, mutations, "would create PostgreSQL-backed users records from normalized source payloads")
	requireAdminMigrationMutationMessage(t, mutations, "would copy checksum-addressed source blob bytes into the configured OpenCook blob backend before metadata import")
	requireAdminMigrationMutationMessage(t, mutations, "would rebuild OpenSearch derived state from imported PostgreSQL state after import")
	requireAdminMigrationMutationMessage(t, mutations, "opencook admin reindex --all-orgs --complete")
	requireAdminMigrationMutationMessage(t, mutations, "opencook admin search check --all-orgs")
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("source import preflight mutated bootstrap=%d objects=%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
}

func TestAdminMigrationSourceImportPreflightRefusesNonEmptyTarget(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	bootstrapState, coreObjectState := adminMigrationHealthyStates(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrapState, objects: coreObjectState},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "memory-compat", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "memory"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source import preflight non-empty target) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_target", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_import_target_not_empty")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "source import is blocked because the PostgreSQL-backed target is not empty")
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 {
		t.Fatalf("source import preflight saved metadata bootstrap=%d objects=%d", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func TestAdminMigrationSourceImportTargetIgnoresEmptyCookbookInventoryPlaceholders(t *testing.T) {
	target := adminMigrationInventoryFromState(
		bootstrap.BootstrapCoreState{},
		bootstrap.CoreObjectState{},
		map[string]adminMigrationCookbookInventory{"ponyville": {}},
		"",
	)
	dep, findings := adminMigrationSourceImportTargetDependency(target, adminMigrationInventory{})
	if dep.Status != "ok" {
		t.Fatalf("source import target dependency status = %s with details %#v, want ok", dep.Status, dep.Details)
	}
	if len(findings) != 0 {
		t.Fatalf("findings = %#v, want none for empty cookbook placeholder", findings)
	}
	if got := adminMigrationInventoryTotalCount(target); got != 0 {
		t.Fatalf("inventory total = %d, want 0 for empty cookbook placeholder", got)
	}
}

func TestAdminMigrationSourceImportPreflightRejectsTamperedSourceWithoutProviders(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	if err := os.WriteFile(filepath.Join(sourcePath, "payloads", "bootstrap", "users.json"), []byte(`[{"username":"tampered"}]`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(tampered users payload) error = %v", err)
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		t.Fatal("source import preflight must not load provider configuration for invalid source bundles")
		return config.Config{}, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source import preflight tampered source) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bundle", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_payload_hash_mismatch")
}

func TestAdminMigrationSourceImportPreflightRequiresBlobProviderForUncopiedReferences(t *testing.T) {
	sourceRoot := t.TempDir()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	writeAdminMigrationSourceJSON(t, filepath.Join(sourceRoot, "organizations", "ponyville", "cookbooks", "app-1.0.0", "metadata.json"), adminMigrationTestCookbookVersionPayload("app", "1.0.0", checksum))
	normalizeOut := buildAdminMigrationSourceNormalize(sourceRoot, &adminMigrationFlagValues{outputPath: outputPath})
	if !normalizeOut.OK {
		t.Fatalf("source normalize output = %+v, want ok", normalizeOut)
	}
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
			exists: map[string]bool{},
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", outputPath, "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source import preflight missing blob) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob", "ok")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_blob_payload_missing")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "missing_blobs", 1)
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("missing blob preflight mutated bootstrap=%d objects=%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
}

func TestAdminMigrationSourceImportApplyWritesStateBlobsProgressAndRehydrates(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	progressPath := filepath.Join(t.TempDir(), "source-import-progress.json")
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	checksum := "364c08d77216cb1c1c5b3826de020dc3"
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
			exists: map[string]bool{},
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source import apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_import_apply" {
		t.Fatalf("command = %v, want migration_source_import_apply", out["command"])
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "source_import_blobs", "ok")
	requireAdminMigrationDependency(t, deps, "source_import_write", "ok")
	if len(blobPuts[checksum]) == 0 {
		t.Fatalf("copied blob %s not written; puts = %#v", checksum, blobPuts)
	}
	if targetStore.bootstrapSaves != 1 || targetStore.objectSaves != 1 {
		t.Fatalf("source import saves bootstrap=%d objects=%d, want 1/1", targetStore.bootstrapSaves, targetStore.objectSaves)
	}

	bootstrapState := targetStore.bootstrap
	coreState := targetStore.objects
	org := bootstrapState.Orgs["ponyville"]
	if bootstrapState.Users["pivotal"].DisplayName != "Pivotal User" {
		t.Fatalf("imported user = %#v, want pivotal display name", bootstrapState.Users["pivotal"])
	}
	if !org.Clients["ponyville-validator"].Validator || org.Clients["ponyville-validator"].PublicKey == "" {
		t.Fatalf("imported validator client = %#v, want validator with public key", org.Clients["ponyville-validator"])
	}
	if !adminMigrationTestStringSliceContains(org.Groups["admins"].Users, "pivotal") {
		t.Fatalf("admins users = %v, want pivotal", org.Groups["admins"].Users)
	}
	coreOrg := coreState.Orgs["ponyville"]
	if coreOrg.Nodes["web01"].ChefEnvironment != "_default" {
		t.Fatalf("node = %#v, want default environment", coreOrg.Nodes["web01"])
	}
	if got := coreOrg.DataBagItems["secrets"]["db"].RawData["encrypted_data"]; got != "fixture" {
		t.Fatalf("data bag db encrypted_data = %v, want fixture", got)
	}
	if coreOrg.PolicyGroups["prod"].Policies["base"] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("policy group prod = %#v, want base assignment", coreOrg.PolicyGroups["prod"])
	}
	if len(coreOrg.Sandboxes["sandbox-fixture"].Checksums) != 1 || coreOrg.Sandboxes["sandbox-fixture"].Checksums[0] != checksum {
		t.Fatalf("sandbox = %#v, want checksum %s", coreOrg.Sandboxes["sandbox-fixture"], checksum)
	}
	if len(targetStore.restoredCookbooks.Orgs["ponyville"].Versions) != 1 || len(targetStore.restoredCookbooks.Orgs["ponyville"].Artifacts) != 1 {
		t.Fatalf("restored cookbooks = %#v, want one version and one artifact", targetStore.restoredCookbooks)
	}

	progress := mustReadAdminMigrationSourceImportProgress(t, progressPath)
	if !progress.MetadataImported || !adminMigrationTestStringSliceContains(progress.CopiedBlobs, checksum) {
		t.Fatalf("progress = %#v, want metadata imported and copied checksum", progress)
	}
	keyStore := authn.NewMemoryKeyStore()
	service := bootstrap.NewService(keyStore, bootstrap.Options{InitialBootstrapCoreState: &bootstrapState, InitialCoreObjectState: &coreState})
	if err := service.RehydrateKeyStore(); err != nil {
		t.Fatalf("RehydrateKeyStore() error = %v", err)
	}
	if keys, err := keyStore.Lookup(context.Background(), "pivotal", ""); err != nil || len(keys) == 0 {
		t.Fatalf("Lookup(pivotal) keys=%v err=%v, want imported user key", keys, err)
	}
	if keys, err := keyStore.Lookup(context.Background(), "ponyville-validator", "ponyville"); err != nil || len(keys) == 0 {
		t.Fatalf("Lookup(validator) keys=%v err=%v, want imported client key", keys, err)
	}
	if _, ok := service.GetOrganization("ponyville"); !ok {
		t.Fatal("GetOrganization(ponyville) ok = false, want true after rehydrate")
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "web01"); !orgOK || !nodeOK {
		t.Fatalf("GetNode(web01) orgOK=%v nodeOK=%v, want true/true after rehydrate", orgOK, nodeOK)
	}

	reindexTarget := &fakeAdminReindexTarget{}
	reindexCmd, reindexStdout, reindexStderr := newTestCommand(t)
	reindexCmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", OpenSearchURL: "http://opensearch.test"}, nil
	}
	reindexCmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}
	reindexCmd.newReindexTarget = func(string) (search.ReindexTarget, error) {
		return reindexTarget, nil
	}
	setTestMaintenanceStore(reindexCmd, activeAdminWorkflowMaintenanceStore(t), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	if code := reindexCmd.Run(context.Background(), []string{"admin", "reindex", "--all-orgs", "--complete", "--json"}); code != exitOK {
		t.Fatalf("Run(admin reindex imported state) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, reindexStdout.String(), reindexStderr.String())
	}
	if len(reindexTarget.upsertedRefs()) == 0 {
		t.Fatal("reindex imported state upserted no OpenSearch documents")
	}
	for _, ref := range []search.DocumentRef{
		{Organization: "ponyville", Index: "client", Name: "ponyville-validator"},
		{Organization: "ponyville", Index: "environment", Name: "_default"},
		{Organization: "ponyville", Index: "node", Name: "web01"},
		{Organization: "ponyville", Index: "role", Name: "web"},
		{Organization: "ponyville", Index: "secrets", Name: "db"},
		{Organization: "ponyville", Index: "secrets", Name: "plain"},
	} {
		requireAdminMigrationImportedSearchRef(t, reindexTarget.upsertedRefs(), ref)
	}

	reindexIDs := adminMigrationSearchIDsFromRefs(reindexTarget.upsertedRefs())
	searchTarget := newFakeAdminSearchTarget(reindexIDs...)
	searchCmd, searchStdout, searchStderr := newAdminSearchTestCommand(t, targetStore.fakeOfflineStore, searchTarget)
	if code := searchCmd.Run(context.Background(), []string{"admin", "search", "check", "--all-orgs", "--json"}); code != exitOK {
		t.Fatalf("Run(admin search check imported state) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, searchStdout.String(), searchStderr.String())
	}
	searchOut := decodeAdminReindexOutput(t, searchStdout.String())
	assertAdminReindexCount(t, searchOut, "clean", 1)
	assertAdminReindexCount(t, searchOut, "missing", 0)
	assertAdminReindexCount(t, searchOut, "unsupported", 0)

	driftIDs := append(append([]string(nil), reindexIDs...), adminUnsupportedProviderIDs()...)
	driftTarget := newFakeAdminSearchTarget(driftIDs...)
	driftCmd, driftStdout, driftStderr := newAdminSearchTestCommand(t, targetStore.fakeOfflineStore, driftTarget)
	if code := driftCmd.Run(context.Background(), []string{"admin", "search", "check", "--all-orgs", "--json"}); code != exitPartial {
		t.Fatalf("Run(admin search check imported state with stale unsupported docs) exit = %d, want %d; stdout = %s stderr = %s", code, exitPartial, driftStdout.String(), driftStderr.String())
	}
	driftOut := decodeAdminReindexOutput(t, driftStdout.String())
	assertAdminReindexCount(t, driftOut, "missing", 0)
	assertAdminReindexCount(t, driftOut, "stale", len(adminUnsupportedProviderIDs()))
	assertAdminReindexCount(t, driftOut, "unsupported", len(adminUnsupportedProviderScopes()))
	requireAdminOutputStrings(t, driftOut, "stale_documents", adminUnsupportedProviderIDs())
	requireAdminOutputStrings(t, driftOut, "unsupported_scopes", adminUnsupportedProviderScopes())

	unavailableTarget := &fakeAdminSearchTarget{
		ids:       map[string]struct{}{},
		searchErr: errors.New("raw provider body from internal cluster correct horse battery staple"),
	}
	unavailableCmd, unavailableStdout, unavailableStderr := newAdminSearchTestCommand(t, targetStore.fakeOfflineStore, unavailableTarget)
	if code := unavailableCmd.Run(context.Background(), []string{"admin", "search", "check", "--all-orgs", "--json"}); code != exitDependencyUnavailable {
		t.Fatalf("Run(admin search check unavailable imported state) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, unavailableStdout.String(), unavailableStderr.String())
	}
	assertAdminReindexCount(t, decodeAdminReindexOutput(t, unavailableStdout.String()), "failed", 1)
	if strings.Contains(unavailableStdout.String(), "correct horse battery staple") || strings.Contains(unavailableStderr.String(), "correct horse battery staple") {
		t.Fatalf("provider-unavailable output leaked provider internals: stdout=%s stderr=%s", unavailableStdout.String(), unavailableStderr.String())
	}
}

func TestAdminMigrationSourceImportApplyDryRunDoesNotMutate(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	progressPath := filepath.Join(t.TempDir(), "source-import-progress.json")
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: blobPuts}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--dry-run", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source import apply dry-run) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("dry-run mutated bootstrap=%d objects=%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Fatalf("progress stat err = %v, want not exist", err)
	}
}

func TestAdminMigrationSourceImportApplyBlobFailureDoesNotSaveMetadata(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: map[string][]byte{}, putErr: blob.ErrUnavailable}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source import apply blob failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_blobs", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_import_blob_copy_failed")
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(targetStore.restoredCookbooks.Orgs) != 0 {
		t.Fatalf("blob failure saved metadata bootstrap=%d objects=%d cookbooks=%#v", targetStore.bootstrapSaves, targetStore.objectSaves, targetStore.restoredCookbooks)
	}
}

func TestAdminMigrationSourceImportApplyWriteFailureRollsBackMetadata(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}, objectSaveErr: errors.New("objects failed")},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: map[string][]byte{}}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source import apply write failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_import_write_failed")
	if len(targetStore.bootstrap.Users) != 0 || len(targetStore.objects.Orgs) != 0 || len(targetStore.restoredCookbooks.Orgs) != 0 {
		t.Fatalf("write failure left state bootstrap=%#v objects=%#v cookbooks=%#v", targetStore.bootstrap, targetStore.objects, targetStore.restoredCookbooks)
	}
	if targetStore.bootstrapSaves != 2 || targetStore.objectSaves != 0 {
		t.Fatalf("write failure saves bootstrap=%d objects=%d, want rollback bootstrap save and no object save", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func TestAdminMigrationSourceSyncPreflightPlansStableRepeatedSnapshot(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead() error = %v", err)
	}
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: sourceState.Bootstrap, objects: sourceState.CoreObjects},
		cookbookInventory: adminMigrationCookbookInventoryFromExport(sourceState.Cookbooks),
		cookbookExport:    sourceState.Cookbooks,
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "preflight", sourcePath, "--offline", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source sync preflight stable) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_sync_preflight" {
		t.Fatalf("command = %v, want migration_source_sync_preflight", out["command"])
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "source_bundle", "ok")
	requireAdminMigrationDependency(t, deps, "source_sync_progress", "ok")
	requireAdminMigrationDependency(t, deps, "postgres", "ok")
	requireAdminMigrationDependency(t, deps, "blob", "ok")
	stableMutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, stableMutations, "source sync found no PostgreSQL metadata changes for manifest-covered families")
	requireAdminMigrationMutationMessage(t, stableMutations, "opencook admin search check --all-orgs")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "users_unchanged", 1)
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 {
		t.Fatalf("source sync preflight saved metadata bootstrap=%d objects=%d", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func TestAdminMigrationSourceSyncApplyReconcilesCoveredFamiliesAndWritesCursor(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	progressPath := filepath.Join(t.TempDir(), "source-sync-progress.json")
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead() error = %v", err)
	}
	targetBootstrap := bootstrap.CloneBootstrapCoreState(sourceState.Bootstrap)
	targetUser := targetBootstrap.Users["pivotal"]
	targetUser.DisplayName = "Old Source"
	targetBootstrap.Users["pivotal"] = targetUser
	targetBootstrap.Orgs["canterlot"] = bootstrap.BootstrapCoreOrganizationState{
		Organization: bootstrap.Organization{Name: "canterlot", FullName: "Canterlot", OrgType: "Business", GUID: "canterlot"},
		Clients:      map[string]bootstrap.Client{},
		ClientKeys:   map[string]map[string]bootstrap.KeyRecord{},
		Groups:       map[string]bootstrap.Group{},
		Containers:   map[string]bootstrap.Container{},
		ACLs:         map[string]authz.ACL{},
	}
	targetCore := bootstrap.CloneCoreObjectState(sourceState.CoreObjects)
	ponyvilleCore := targetCore.Orgs["ponyville"]
	ponyvilleCore.Nodes["stale"] = bootstrap.Node{Name: "stale", JSONClass: "Chef::Node", ChefType: "node", ChefEnvironment: "_default", Override: map[string]any{}, Normal: map[string]any{}, Default: map[string]any{}, Automatic: map[string]any{}, RunList: []string{}}
	targetCore.Orgs["ponyville"] = ponyvilleCore
	targetCore.Orgs["canterlot"] = bootstrap.CoreObjectOrganizationState{Nodes: map[string]bootstrap.Node{"castle": {Name: "castle"}}, ACLs: map[string]authz.ACL{}}
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: targetBootstrap, objects: targetCore},
		cookbookInventory: adminMigrationCookbookInventoryFromExport(sourceState.Cookbooks),
		cookbookExport:    sourceState.Cookbooks,
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: blobPuts}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source sync apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_sync_write", "ok")
	syncMutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, syncMutations, "opencook admin reindex --all-orgs --complete")
	requireAdminMigrationMutationMessage(t, syncMutations, "opencook admin search check --all-orgs")
	if targetStore.bootstrap.Users["pivotal"].DisplayName != "Pivotal User" {
		t.Fatalf("synced user = %#v, want source display name", targetStore.bootstrap.Users["pivotal"])
	}
	if _, ok := targetStore.objects.Orgs["ponyville"].Nodes["stale"]; ok {
		t.Fatalf("stale node still present after manifest-covered node sync: %#v", targetStore.objects.Orgs["ponyville"].Nodes)
	}
	if _, ok := targetStore.bootstrap.Orgs["canterlot"]; !ok {
		t.Fatal("target-only canterlot org was removed despite no source scope")
	}
	progress := mustReadAdminMigrationSourceSyncProgress(t, progressPath)
	cursor := adminMigrationSourceSyncCursor(read)
	if progress.SourceCursor != cursor || progress.LastStatus != "applied" || !adminMigrationTestStringSliceContains(progress.AppliedCursors, cursor) {
		t.Fatalf("progress = %#v, want applied cursor %s", progress, cursor)
	}
	if targetStore.bootstrapSaves != 1 || targetStore.objectSaves != 1 {
		t.Fatalf("sync saves bootstrap=%d objects=%d, want 1/1", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func TestAdminMigrationSourceSyncApplyDryRunDoesNotMutate(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	progressPath := filepath.Join(t.TempDir(), "source-sync-progress.json")
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead() error = %v", err)
	}
	targetBootstrap := bootstrap.CloneBootstrapCoreState(sourceState.Bootstrap)
	targetUser := targetBootstrap.Users["pivotal"]
	targetUser.DisplayName = "Old Source"
	targetBootstrap.Users["pivotal"] = targetUser
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: targetBootstrap, objects: sourceState.CoreObjects},
		cookbookInventory: adminMigrationCookbookInventoryFromExport(sourceState.Cookbooks),
		cookbookExport:    sourceState.Cookbooks,
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: blobPuts}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--dry-run", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source sync apply dry-run) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if targetStore.bootstrap.Users["pivotal"].DisplayName != "Old Source" || targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("dry-run mutated state user=%#v saves=%d/%d blobs=%d", targetStore.bootstrap.Users["pivotal"], targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Fatalf("progress stat err = %v, want not exist", err)
	}
}

func TestAdminMigrationSourceSyncApplyWriteFailureIsRetryable(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	progressPath := filepath.Join(t.TempDir(), "source-sync-progress.json")
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead() error = %v", err)
	}
	targetBootstrap := bootstrap.CloneBootstrapCoreState(sourceState.Bootstrap)
	targetUser := targetBootstrap.Users["pivotal"]
	targetUser.DisplayName = "Old Source"
	targetBootstrap.Users["pivotal"] = targetUser
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: targetBootstrap, objects: sourceState.CoreObjects, objectSaveErr: errors.New("objects failed")},
		cookbookInventory: adminMigrationCookbookInventoryFromExport(sourceState.Cookbooks),
		cookbookExport:    sourceState.Cookbooks,
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"}, exists: map[string]bool{}, puts: map[string][]byte{}}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration source sync apply write failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_sync_write_failed")
	if targetStore.bootstrap.Users["pivotal"].DisplayName != "Old Source" {
		t.Fatalf("failed sync left user = %#v, want rollback to old source", targetStore.bootstrap.Users["pivotal"])
	}
	if _, err := os.Stat(progressPath); !os.IsNotExist(err) {
		t.Fatalf("progress stat err = %v, want no success cursor after failed write", err)
	}

	targetStore.objectSaveErr = nil
	stdout.Reset()
	stderr.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration source sync apply retry) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if targetStore.bootstrap.Users["pivotal"].DisplayName != "Pivotal User" {
		t.Fatalf("retry synced user = %#v, want source display name", targetStore.bootstrap.Users["pivotal"])
	}
	if progress := mustReadAdminMigrationSourceSyncProgress(t, progressPath); progress.LastStatus != "applied" {
		t.Fatalf("retry progress = %#v, want applied", progress)
	}
}

func TestAdminMigrationCutoverRehearseValidatesLiveTargetAndDownloadsBlob(t *testing.T) {
	bundlePath, checksum := writeAdminMigrationRestoreTestBundleWithBlobAndCookbook(t)
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	sourceRead, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	importProgressPath := adminMigrationSourceImportProgressFile(sourcePath, "")
	if err := adminMigrationWriteSourceImportProgress(sourcePath, importProgressPath, adminMigrationSourceImportProgress{
		MetadataImported: true,
		CopiedBlobs:      []string{checksum},
		VerifiedBlobs:    []string{checksum},
	}); err != nil {
		t.Fatalf("adminMigrationWriteSourceImportProgress() error = %v", err)
	}
	syncProgressPath := adminMigrationSourceSyncProgressFile(sourcePath, "")
	cursor := adminMigrationSourceSyncCursor(sourceRead)
	if err := adminMigrationWriteSourceSyncProgress(sourcePath, syncProgressPath, adminMigrationSourceSyncProgress{
		SourceCursor:   cursor,
		LastStatus:     "applied",
		AppliedCursors: []string{cursor},
	}); err != nil {
		t.Fatalf("adminMigrationWriteSourceSyncProgress() error = %v", err)
	}
	searchResultPath := filepath.Join(t.TempDir(), "search-check.json")
	writeAdminMigrationSourceJSON(t, searchResultPath, map[string]any{
		"ok":      true,
		"command": "search_check",
		"counts": map[string]any{
			"missing":     0,
			"stale":       0,
			"unsupported": 0,
			"failed":      0,
			"clean":       1,
		},
	})
	shadowResultPath := filepath.Join(t.TempDir(), "shadow-compare.json")
	writeAdminMigrationSourceJSON(t, shadowResultPath, map[string]any{
		"ok":      true,
		"command": "migration_shadow_compare",
		"inventory": map[string]any{
			"families": []map[string]any{{"family": "shadow_failed", "count": 0}},
		},
		"errors": []any{},
	})
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
		"--source", sourcePath,
		"--source-import-progress", importProgressPath,
		"--source-sync-progress", syncProgressPath,
		"--search-check-result", searchResultPath,
		"--shadow-result", shadowResultPath,
		"--rollback-ready",
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
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "cutover_source_bundle", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_progress", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_sync_freshness", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "search_cleanliness", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "shadow_read_evidence", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "rollback_readiness", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "signed_auth", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob_reachability", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "cutover_rehearsal", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_failed", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_downloads", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "cutover_blockers", 0)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "shadow_read_advisory")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "compare read-only source Chef responses to restored OpenCook responses with documented compatibility normalizers")
	if !fake.calledPath(cookbookPath) {
		t.Fatalf("cutover rehearsal did not read cookbook path; calls = %+v", fake.calls)
	}
	if len(fake.downloadCalls) != 1 || fake.downloadCalls[0] != downloadURL {
		t.Fatalf("download calls = %v, want %s", fake.downloadCalls, downloadURL)
	}
}

func TestAdminMigrationCutoverRehearsePromotesEvidenceFailuresToBlockers(t *testing.T) {
	bundlePath := writeAdminMigrationRestoreTestBundle(t)
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	sourceRead, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	importProgressPath := adminMigrationSourceImportProgressFile(sourcePath, "")
	if err := adminMigrationWriteSourceImportProgress(sourcePath, importProgressPath, adminMigrationSourceImportProgress{}); err != nil {
		t.Fatalf("adminMigrationWriteSourceImportProgress() error = %v", err)
	}
	syncProgressPath := adminMigrationSourceSyncProgressFile(sourcePath, "")
	if err := adminMigrationWriteSourceSyncProgress(sourcePath, syncProgressPath, adminMigrationSourceSyncProgress{
		SourceCursor:   "older-source",
		LastStatus:     "applied",
		AppliedCursors: []string{"older-source"},
	}); err != nil {
		t.Fatalf("adminMigrationWriteSourceSyncProgress() error = %v", err)
	}
	if cursor := adminMigrationSourceSyncCursor(sourceRead); cursor == "older-source" {
		t.Fatalf("test source cursor unexpectedly matched stale cursor")
	}
	searchResultPath := filepath.Join(t.TempDir(), "search-check.json")
	writeAdminMigrationSourceJSON(t, searchResultPath, map[string]any{
		"ok":      true,
		"command": "search_check",
		"counts": map[string]any{
			"missing":     1,
			"stale":       0,
			"unsupported": 0,
			"failed":      0,
			"clean":       0,
		},
	})
	shadowResultPath := filepath.Join(t.TempDir(), "shadow-compare.json")
	writeAdminMigrationSourceJSON(t, shadowResultPath, map[string]any{
		"ok":      false,
		"command": "migration_shadow_compare",
		"inventory": map[string]any{
			"families": []map[string]any{{"family": "shadow_failed", "count": 1}},
		},
		"errors": []map[string]string{{"code": "shadow_payload_mismatch"}},
	})
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
		return &fakeMigrationRehearsalClient{}, nil
	}

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "cutover", "rehearse",
		"--manifest", bundlePath,
		"--source", sourcePath,
		"--source-import-progress", importProgressPath,
		"--source-sync-progress", syncProgressPath,
		"--search-check-result", searchResultPath,
		"--shadow-result", shadowResultPath,
		"--rollback-ready",
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(migration cutover rehearse evidence failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "source_import_progress", "error")
	requireAdminMigrationDependency(t, deps, "source_sync_freshness", "error")
	requireAdminMigrationDependency(t, deps, "search_cleanliness", "error")
	requireAdminMigrationDependency(t, deps, "shadow_read_evidence", "error")
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFinding(t, findings, "source_import_incomplete")
	requireAdminMigrationFinding(t, findings, "source_sync_stale")
	requireAdminMigrationFinding(t, findings, "cutover_search_not_clean")
	requireAdminMigrationFinding(t, findings, "cutover_shadow_result_failed")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	for _, item := range families {
		entry, _ := item.(map[string]any)
		if entry["family"] == "cutover_blockers" && entry["count"].(float64) > 0 {
			return
		}
	}
	t.Fatalf("inventory families = %v, want positive cutover_blockers", families)
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

func TestAdminMigrationShadowCompareNormalizesReadOnlyTargetAndDownloads(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	fake := adminMigrationShadowClientForSource(t, sourcePath, nil)
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
		if cfg.ServerURL != "https://admin:secret@opencook.test" || cfg.RequestorName != "pivotal" || cfg.PrivateKeyPath != "/keys/pivotal.pem" {
			t.Fatalf("admin config = %+v, want shadow overrides", cfg)
		}
		return fake, nil
	}

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "shadow", "compare",
		"--source", sourcePath,
		"--target-server-url", "https://admin:secret@opencook.test",
		"--requestor-name", "pivotal",
		"--private-key", "/keys/pivotal.pem",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(migration shadow compare) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "admin:secret") || strings.Contains(stdout.String(), "/keys/pivotal.pem") {
		t.Fatalf("shadow compare leaked target credentials or private key path: %s", stdout.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_shadow_compare" {
		t.Fatalf("command = %v, want migration_shadow_compare", out["command"])
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bundle", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "shadow_read_compare", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "shadow_failed", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "shadow_downloads", 2)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "shadow_read_scope_skipped")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "do not proxy writes, validator registration, cookbook upload, sandbox commit, or key/client mutations during shadow comparison")
	if len(fake.downloadCalls) != 2 {
		t.Fatalf("download calls = %v, want signed cookbook and artifact blob downloads", fake.downloadCalls)
	}
	for _, call := range fake.calls {
		if call.method != http.MethodGet {
			t.Fatalf("shadow compare issued %s %s, want read-only GET calls", call.method, call.path)
		}
		if call.payload != nil {
			t.Fatalf("shadow compare sent payload for %s: %#v", call.path, call.payload)
		}
	}
}

func TestAdminMigrationShadowCompareReportsPayloadAndSearchMismatches(t *testing.T) {
	sourcePath := writeAdminMigrationNormalizedSourceFixture(t)
	for _, tc := range []struct {
		name     string
		mutate   func(*fakeMigrationRehearsalClient)
		wantCode string
	}{
		{
			name: "payload mismatch",
			mutate: func(fake *fakeMigrationRehearsalClient) {
				fake.responses["/organizations/ponyville/nodes/web01"] = map[string]any{"name": "wrong-node"}
			},
			wantCode: "shadow_payload_mismatch",
		},
		{
			name: "search count mismatch",
			mutate: func(fake *fakeMigrationRehearsalClient) {
				fake.responses["/organizations/ponyville/search/node?q=*:*"] = map[string]any{"start": 0, "total": 999, "rows": []any{}}
			},
			wantCode: "shadow_search_count_mismatch",
		},
		{
			name: "auth failure",
			mutate: func(fake *fakeMigrationRehearsalClient) {
				fake.errPaths["/organizations/ponyville/nodes/web01"] = errors.New("GET /organizations/ponyville/nodes/web01 returned HTTP 403")
			},
			wantCode: "shadow_auth_failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := adminMigrationShadowClientForSource(t, sourcePath, nil)
			tc.mutate(fake)
			cmd, stdout, stderr := newTestCommand(t)
			cmd.loadAdminConfig = func() admin.Config {
				return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "/keys/pivotal.pem"}
			}
			cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
				return fake, nil
			}

			code := cmd.Run(context.Background(), []string{"admin", "migration", "shadow", "compare", "--source", sourcePath, "--target-server-url", "http://opencook.test", "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(migration shadow compare %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "shadow_read_compare", "error")
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantCode)
			requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "shadow_failed", 1)
			if strings.Contains(stdout.String(), "wrong-node") || strings.Contains(stdout.String(), "HTTP 403") {
				t.Fatalf("shadow mismatch output leaked raw target details: %s", stdout.String())
			}
		})
	}
}

func TestAdminMigrationShadowNormalizePayloadKeepsRunListOrderSignificant(t *testing.T) {
	source := map[string]any{
		"actors":     []any{"zebra", "applejack"},
		"run_list":   []any{"recipe[first]", "recipe[second]"},
		"url":        "/_blob/checksums/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?source=1",
		"updated_at": "source timestamp",
	}
	target := map[string]any{
		"actors":      []any{"applejack", "zebra"},
		"run_list":    []any{"recipe[first]", "recipe[second]"},
		"url":         "https://opencook.test/_blob/checksums/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?signature=secret",
		"private_key": "secret",
		"updated_at":  "target timestamp",
		"requestor": map[string]any{
			"type": "user",
			"name": "pivotal",
		},
		"authn_status":   "verified",
		"storage_status": "memory-bootstrap",
	}
	sourceCanonical, err := adminMigrationShadowCanonicalPayload("roles", source)
	if err != nil {
		t.Fatalf("source canonical error = %v", err)
	}
	targetCanonical, err := adminMigrationShadowCanonicalPayload("roles", target)
	if err != nil {
		t.Fatalf("target canonical error = %v", err)
	}
	if sourceCanonical != targetCanonical {
		t.Fatalf("canonical payloads differ after normalization:\nsource=%s\ntarget=%s", sourceCanonical, targetCanonical)
	}
	target["run_list"] = []any{"recipe[second]", "recipe[first]"}
	changedCanonical, err := adminMigrationShadowCanonicalPayload("roles", target)
	if err != nil {
		t.Fatalf("changed canonical error = %v", err)
	}
	if sourceCanonical == changedCanonical {
		t.Fatalf("run_list reordering was normalized away; source=%s target=%s", sourceCanonical, changedCanonical)
	}
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
			name:        "source import preflight path before flags",
			args:        []string{"admin", "migration", "source", "import", "preflight", "/tmp/chef-server-source-import", "--offline", "--json"},
			wantCode:    exitDependencyUnavailable,
			command:     "migration_source_import_preflight",
			targetKey:   "source_path",
			targetWant:  "/tmp/chef-server-source-import",
			wantOffline: true,
		},
		{
			name:       "cutover rehearse",
			args:       []string{"admin", "migration", "cutover", "rehearse", "--manifest", "/tmp/manifest.json", "--server-url", "https://admin:secret@opencook.example", "--json"},
			wantCode:   exitDependencyUnavailable,
			command:    "migration_cutover_rehearse",
			targetKey:  "manifest_path",
			targetWant: "/tmp/manifest.json",
		},
		{
			name:       "shadow compare",
			args:       []string{"admin", "migration", "shadow", "compare", "--source", "/tmp/chef-source", "--target-server-url", "https://admin:secret@opencook.example", "--json"},
			wantCode:   exitDependencyUnavailable,
			command:    "migration_shadow_compare",
			targetKey:  "source_path",
			targetWant: "/tmp/chef-source",
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

// RestoreCookbookExport records cookbook restore requests and updates the fake
// export snapshot so later migration phases see the same metadata.
func (s *fakeMigrationInventoryStore) RestoreCookbookExport(_ bootstrap.BootstrapCoreState, export adminMigrationCookbookExport) error {
	if s.cookbookRestoreErr != nil {
		return s.cookbookRestoreErr
	}
	s.restoredCookbooks = export
	s.cookbookExport = adminMigrationSourceSyncMergeCookbookExport(s.cookbookExport, export, adminMigrationAllCookbookScopes(export))
	s.cookbookInventory = adminMigrationCookbookInventoryFromExport(s.cookbookExport)
	return nil
}

// SyncCookbookExport lets source-sync tests exercise cookbook reconciliation
// without a live PostgreSQL cookbook repository.
func (s *fakeMigrationInventoryStore) SyncCookbookExport(_ bootstrap.BootstrapCoreState, export adminMigrationCookbookExport, scopes map[adminMigrationSourcePayloadKey]bool) error {
	if s.cookbookRestoreErr != nil {
		return s.cookbookRestoreErr
	}
	s.restoredCookbooks = export
	s.cookbookExport = adminMigrationSourceSyncMergeCookbookExport(s.cookbookExport, export, scopes)
	s.cookbookInventory = adminMigrationCookbookInventoryFromExport(s.cookbookExport)
	return nil
}

// adminMigrationAllCookbookScopes treats restore imports as full cookbook
// creates for every org present in the export.
func adminMigrationAllCookbookScopes(export adminMigrationCookbookExport) map[adminMigrationSourcePayloadKey]bool {
	scopes := map[adminMigrationSourcePayloadKey]bool{}
	for orgName := range export.Orgs {
		scopes[adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_versions"}] = true
		scopes[adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_artifacts"}] = true
	}
	return scopes
}

type fakeMigrationCookbookStore struct {
	artifactErr       error
	artifactErrForID  string
	artifactDeleteErr error
	versionDeleteErr  error
	deletedArtifacts  []string
	deletedVersions   []string
	ensuredOrgs       []bootstrap.Organization
}

func (s *fakeMigrationCookbookStore) EnsureOrganization(org bootstrap.Organization) {
	s.ensuredOrgs = append(s.ensuredOrgs, org)
}

func (s *fakeMigrationCookbookStore) HasCookbookVersion(string, string, string) (bool, bool) {
	return false, true
}

func (s *fakeMigrationCookbookStore) ListCookbookArtifacts(string) (map[string][]bootstrap.CookbookArtifact, bool) {
	return nil, true
}

func (s *fakeMigrationCookbookStore) ListCookbookArtifactsByName(string, string) ([]bootstrap.CookbookArtifact, bool, bool) {
	return nil, true, false
}

func (s *fakeMigrationCookbookStore) GetCookbookArtifact(string, string, string) (bootstrap.CookbookArtifact, bool, bool) {
	return bootstrap.CookbookArtifact{}, true, false
}

func (s *fakeMigrationCookbookStore) CreateCookbookArtifact(orgName string, artifact bootstrap.CookbookArtifact) (bootstrap.CookbookArtifact, error) {
	if s.artifactErr != nil && artifact.Identifier == s.artifactErrForID {
		return bootstrap.CookbookArtifact{}, s.artifactErr
	}
	return artifact, nil
}

func (s *fakeMigrationCookbookStore) DeleteCookbookArtifactWithReleasedChecksums(orgName, name, identifier string) (bootstrap.CookbookArtifact, []string, error) {
	if s.artifactDeleteErr != nil {
		return bootstrap.CookbookArtifact{}, nil, s.artifactDeleteErr
	}
	s.deletedArtifacts = append(s.deletedArtifacts, orgName+"/"+name+"/"+identifier)
	return bootstrap.CookbookArtifact{Name: name, Identifier: identifier}, nil, nil
}

func (s *fakeMigrationCookbookStore) ListCookbookVersions(string) (map[string][]bootstrap.CookbookVersionRef, bool) {
	return nil, true
}

func (s *fakeMigrationCookbookStore) ListCookbookVersionsByName(string, string) ([]bootstrap.CookbookVersionRef, bool, bool) {
	return nil, true, false
}

func (s *fakeMigrationCookbookStore) ListCookbookVersionModelsByName(string, string) ([]bootstrap.CookbookVersion, bool, bool) {
	return nil, true, false
}

func (s *fakeMigrationCookbookStore) GetCookbookVersion(string, string, string) (bootstrap.CookbookVersion, bool, bool) {
	return bootstrap.CookbookVersion{}, true, false
}

func (s *fakeMigrationCookbookStore) UpsertCookbookVersionWithReleasedChecksums(orgName string, version bootstrap.CookbookVersion, force bool) (bootstrap.CookbookVersion, []string, bool, error) {
	return version, nil, true, nil
}

func (s *fakeMigrationCookbookStore) DeleteCookbookVersionWithReleasedChecksums(orgName, name, version string) (bootstrap.CookbookVersion, []string, error) {
	if s.versionDeleteErr != nil {
		return bootstrap.CookbookVersion{}, nil, s.versionDeleteErr
	}
	s.deletedVersions = append(s.deletedVersions, orgName+"/"+name+"/"+version)
	return bootstrap.CookbookVersion{CookbookName: name, Version: version}, nil, nil
}

func (s *fakeMigrationCookbookStore) DeleteCookbookChecksumReferencesFromRemaining(map[string]struct{}) {
}

func (s *fakeMigrationCookbookStore) CookbookChecksumReferenced(string) bool {
	return false
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

// adminMigrationShadowClientForSource builds target responses from normalized
// source state, then adds volatile fields so shadow tests exercise normalizers.
func adminMigrationShadowClientForSource(t *testing.T, sourcePath string, overrides map[string]any) *fakeMigrationRehearsalClient {
	t.Helper()
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle() error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead() error = %v", err)
	}
	blobManifest := adminMigrationSourceReadBlobManifest(sourceState, read)
	checks, _ := adminMigrationShadowComparableChecks(adminMigrationCutoverRehearsalChecks(sourceState.Bootstrap, sourceState.CoreObjects, sourceState.Cookbooks, blobManifest))
	responses := map[string]any{}
	downloads := map[string][]byte{}
	for _, check := range checks {
		if check.Family == "search" {
			responses[check.Path] = map[string]any{
				"start": 0,
				"total": adminMigrationShadowExpectedSearchCount(check, sourceState),
				"rows":  []any{},
			}
			continue
		}
		payload, ok := adminMigrationShadowSourcePayload(check, sourceState)
		if !ok {
			t.Fatalf("missing source payload for check %+v", check)
		}
		response := adminMigrationShadowTargetFixturePayload(payload)
		for _, checksum := range check.DownloadChecksums {
			rawURL := "https://opencook.test/_blob/checksums/" + checksum + "?signature=secret"
			adminMigrationShadowReplaceDownloadURL(response, checksum, rawURL)
			if body := read.Bundle.Files[pathpkg.Join("blobs", "checksums", checksum)]; len(body) > 0 {
				downloads[rawURL] = body
			}
		}
		responses[check.Path] = response
	}
	for path, payload := range overrides {
		responses[path] = payload
	}
	return &fakeMigrationRehearsalClient{
		responses: responses,
		errPaths:  map[string]error{},
		downloads: downloads,
	}
}

// adminMigrationShadowTargetFixturePayload adds harmless target-only fields and
// reorders unordered arrays to prove the comparison normalizer is doing work.
func adminMigrationShadowTargetFixturePayload(payload any) any {
	value := adminMigrationShadowJSONValue(payload)
	adminMigrationShadowMutateFixtureValue("", value)
	return value
}

// adminMigrationShadowMutateFixtureValue recursively injects volatile target
// fields while keeping semantic source data unchanged.
func adminMigrationShadowMutateFixtureValue(key string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		typed["updated_at"] = "2026-04-30T00:00:00Z"
		typed["private_key"] = "secret target private key"
		for childKey, childValue := range typed {
			if childKey == "url" {
				if rawURL, ok := childValue.(string); ok {
					typed[childKey] = adminMigrationShadowTargetFixtureURL(rawURL)
				}
				continue
			}
			adminMigrationShadowMutateFixtureValue(childKey, childValue)
		}
	case []any:
		if adminMigrationShadowUnorderedArrayKey(key) {
			for left, right := 0, len(typed)-1; left < right; left, right = left+1, right-1 {
				typed[left], typed[right] = typed[right], typed[left]
			}
		}
		for _, item := range typed {
			adminMigrationShadowMutateFixtureValue(key, item)
		}
	}
}

// adminMigrationShadowTargetFixtureURL turns source checksum paths into
// absolute signed-looking target URLs so query-string normalization is covered.
func adminMigrationShadowTargetFixtureURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || !strings.Contains(parsed.Path, "/_blob/checksums/") {
		return raw
	}
	return "https://opencook.test" + parsed.EscapedPath() + "?signature=secret"
}

// adminMigrationShadowReplaceDownloadURL sets the target response URL for the
// checksum whose bytes the fake unsigned downloader should return.
func adminMigrationShadowReplaceDownloadURL(value any, checksum, rawURL string) bool {
	switch typed := value.(type) {
	case map[string]any:
		if rawChecksum, _ := typed["checksum"].(string); strings.EqualFold(rawChecksum, checksum) {
			typed["url"] = rawURL
			return true
		}
		for _, child := range typed {
			if adminMigrationShadowReplaceDownloadURL(child, checksum, rawURL) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if adminMigrationShadowReplaceDownloadURL(child, checksum, rawURL) {
				return true
			}
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

// adminMigrationSourceFixturePath resolves repo-level source fixtures from the
// cmd package working directory and fails early if the taxonomy fixture moved.
func adminMigrationSourceFixturePath(t *testing.T, parts ...string) string {
	t.Helper()
	segments := append([]string{"..", "..", "test", "compat", "fixtures"}, parts...)
	path := filepath.Join(segments...)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("source fixture %s unavailable: %v", path, err)
	}
	return path
}

// writeAdminMigrationNormalizedSourceFixture materializes the checked-in source
// taxonomy fixture through the normalizer so import preflight sees SHA metadata.
func writeAdminMigrationNormalizedSourceFixture(t *testing.T) string {
	t.Helper()
	outputPath := filepath.Join(t.TempDir(), "normalized-source")
	out := buildAdminMigrationSourceNormalize(
		adminMigrationSourceFixturePath(t, "chef-source-import", "v1"),
		&adminMigrationFlagValues{outputPath: outputPath},
	)
	if !out.OK {
		t.Fatalf("buildAdminMigrationSourceNormalize() output = %+v, want ok", out)
	}
	return outputPath
}

// mustReadAdminMigrationSourceImportProgress loads retry metadata so apply
// tests can prove blob-copy progress is durable across attempts.
func mustReadAdminMigrationSourceImportProgress(t *testing.T, path string) adminMigrationSourceImportProgress {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	var progress adminMigrationSourceImportProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		t.Fatalf("json.Unmarshal(progress) error = %v", err)
	}
	return progress
}

// mustReadAdminMigrationSourceSyncProgress loads cursor metadata so sync tests
// can prove successful applies are repeatable and retryable.
func mustReadAdminMigrationSourceSyncProgress(t *testing.T, path string) adminMigrationSourceSyncProgress {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	var progress adminMigrationSourceSyncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		t.Fatalf("json.Unmarshal(sync progress) error = %v", err)
	}
	return progress
}

// adminMigrationTestStringSliceContains keeps import-state assertions readable
// without depending on the order of normalized membership arrays.
func adminMigrationTestStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// requireAdminMigrationImportedSearchRef keeps the source-import search test
// explicit about every supported family that must be rebuilt from PostgreSQL.
func requireAdminMigrationImportedSearchRef(t *testing.T, refs []search.DocumentRef, want search.DocumentRef) {
	t.Helper()
	if !hasAdminReindexRef(refs, want) {
		t.Fatalf("imported search refs = %v, missing %v", refs, want)
	}
}

// adminMigrationSearchIDsFromRefs converts rebuilt search refs into provider
// IDs so migration tests can run search-check without a live OpenSearch service.
func adminMigrationSearchIDsFromRefs(refs []search.DocumentRef) []string {
	seen := map[string]struct{}{}
	for _, ref := range refs {
		seen[search.OpenSearchDocumentIDForRef(ref)] = struct{}{}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
// inventory tests do not depend on external chef-server-ctl tooling. Names
// ending in "/" are emitted as directory headers for folder-sensitive families.
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
		if strings.HasSuffix(name, "/") {
			if err := tarWriter.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Typeflag: tar.TypeDir}); err != nil {
				t.Fatalf("WriteHeader(%s) error = %v", name, err)
			}
			continue
		}
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
		{name: "source normalize missing path", args: []string{"admin", "migration", "source", "normalize", "--output", "/tmp/out"}, want: "source normalize PATH"},
		{name: "source normalize missing output", args: []string{"admin", "migration", "source", "normalize", "/tmp/source"}, want: "requires --output PATH"},
		{name: "source import missing subcommand", args: []string{"admin", "migration", "source", "import"}, want: "source import requires preflight"},
		{name: "source import preflight missing path", args: []string{"admin", "migration", "source", "import", "preflight", "--offline"}, want: "source import preflight PATH"},
		{name: "source import preflight missing offline", args: []string{"admin", "migration", "source", "import", "preflight", "/tmp/source"}, want: "requires --offline"},
		{name: "source import apply missing path", args: []string{"admin", "migration", "source", "import", "apply", "--offline", "--yes"}, want: "source import apply PATH"},
		{name: "source import apply missing offline", args: []string{"admin", "migration", "source", "import", "apply", "/tmp/source", "--yes"}, want: "requires --offline"},
		{name: "source import apply missing confirmation", args: []string{"admin", "migration", "source", "import", "apply", "/tmp/source", "--offline"}, want: "requires --dry-run or --yes"},
		{name: "source sync missing subcommand", args: []string{"admin", "migration", "source", "sync"}, want: "source sync requires preflight"},
		{name: "source sync preflight missing path", args: []string{"admin", "migration", "source", "sync", "preflight", "--offline"}, want: "source sync preflight PATH"},
		{name: "source sync preflight missing offline", args: []string{"admin", "migration", "source", "sync", "preflight", "/tmp/source"}, want: "requires --offline"},
		{name: "source sync apply missing path", args: []string{"admin", "migration", "source", "sync", "apply", "--offline", "--yes"}, want: "source sync apply PATH"},
		{name: "source sync apply missing offline", args: []string{"admin", "migration", "source", "sync", "apply", "/tmp/source", "--yes"}, want: "requires --offline"},
		{name: "source sync apply missing confirmation", args: []string{"admin", "migration", "source", "sync", "apply", "/tmp/source", "--offline"}, want: "requires --dry-run or --yes"},
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

// requireAdminMigrationFindingFamily narrows repeated finding codes to the
// source family that matters for parser taxonomy coverage.
func requireAdminMigrationFindingFamily(t *testing.T, source []any, code, family string) map[string]any {
	t.Helper()
	for _, item := range source {
		finding, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("finding = %#v, want object", item)
		}
		if finding["code"] == code && finding["family"] == family {
			return finding
		}
	}
	t.Fatalf("findings = %v, want code %s family %s", source, code, family)
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

// requireAdminMigrationSourcePayloadHash verifies normalized source payload
// SHA-256 metadata against the file bytes produced by source normalize.
func requireAdminMigrationSourcePayloadHash(t *testing.T, root string, payloads []adminMigrationSourceManifestPayload, path string) adminMigrationSourceManifestPayload {
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
		if payload.Count <= 0 {
			t.Fatalf("payload %s count = %d, want positive", path, payload.Count)
		}
		return payload
	}
	t.Fatalf("payloads = %#v, want %s", payloads, path)
	return adminMigrationSourceManifestPayload{}
}

// requireAdminMigrationSourceACLResource finds a normalized ACL resource and
// confirms all Chef ACL permission keys were emitted.
func requireAdminMigrationSourceACLResource(t *testing.T, acls []map[string]any, resource string) map[string]any {
	t.Helper()
	for _, acl := range acls {
		if acl["resource"] != resource {
			continue
		}
		for _, action := range []string{"create", "read", "update", "delete", "grant"} {
			if _, ok := acl[action].(map[string]any); !ok {
				t.Fatalf("acl %s action %s = %#v, want object", resource, action, acl[action])
			}
		}
		return acl
	}
	t.Fatalf("acls = %#v, want resource %s", acls, resource)
	return nil
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

// mustReadAdminMigrationSourceManifest loads a normalized source manifest for
// tests that need to assert generated payload paths and hashes.
func mustReadAdminMigrationSourceManifest(t *testing.T, root string) adminMigrationSourceManifest {
	t.Helper()
	var manifest adminMigrationSourceManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationSourceManifestPath), &manifest); err != nil {
		t.Fatalf("source manifest json.Unmarshal error = %v", err)
	}
	return manifest
}

// decodeAdminMigrationSourcePayload decodes a normalized payload file as the
// array-of-objects model produced by source normalize.
func decodeAdminMigrationSourcePayload(t *testing.T, root, path string) []map[string]any {
	t.Helper()
	var payload []map[string]any
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, path), &payload); err != nil {
		t.Fatalf("payload %s json.Unmarshal error = %v", path, err)
	}
	if len(payload) == 0 {
		t.Fatalf("payload %s empty, want at least one object", path)
	}
	return payload
}

// requireAdminMigrationSourcePayloadObject locates one normalized source object
// by a stable field value so fixture assertions do not depend on array order.
func requireAdminMigrationSourcePayloadObject(t *testing.T, payload []map[string]any, field, value string) map[string]any {
	t.Helper()
	for _, object := range payload {
		if object[field] == value {
			return object
		}
	}
	t.Fatalf("payload = %#v, want %s=%s", payload, field, value)
	return nil
}

// adminMigrationTestCookbookVersionPayload returns a Chef-shaped cookbook
// version source payload with one checksum-backed recipe file.
func adminMigrationTestCookbookVersionPayload(name, version, checksum string) map[string]any {
	return map[string]any{
		"name":          name + "-" + version,
		"cookbook_name": name,
		"version":       version,
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"metadata": map[string]any{
			"name":    name,
			"version": version,
		},
		"all_files": []map[string]any{{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		}},
	}
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
