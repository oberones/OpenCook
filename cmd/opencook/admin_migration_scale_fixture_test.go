package main

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/version"
)

func TestAdminMigrationProductionScaleFixtureDeterministic(t *testing.T) {
	first := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	second := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)

	firstJSON, err := json.Marshal(first)
	if err != nil {
		t.Fatalf("json.Marshal(first) error = %v", err)
	}
	secondJSON, err := json.Marshal(second)
	if err != nil {
		t.Fatalf("json.Marshal(second) error = %v", err)
	}
	if string(firstJSON) != string(secondJSON) {
		t.Fatalf("scale fixture is not deterministic\nfirst:  %s\nsecond: %s", string(firstJSON), string(secondJSON))
	}
	if first.Profile != adminMigrationScaleProfileSmall {
		t.Fatalf("profile = %q, want %q", first.Profile, adminMigrationScaleProfileSmall)
	}
	if first.DefaultOrganization != adminMigrationScaleFixtureDefaultOrg {
		t.Fatalf("default org = %q, want %q", first.DefaultOrganization, adminMigrationScaleFixtureDefaultOrg)
	}
	if _, ok := first.Bootstrap.Orgs[first.DefaultOrganization]; !ok {
		t.Fatalf("default org %q missing from fixture orgs", first.DefaultOrganization)
	}
	if len(first.Bootstrap.Orgs) < 2 {
		t.Fatalf("org count = %d, want multi-org fixture", len(first.Bootstrap.Orgs))
	}
}

func TestAdminMigrationScaleFixtureTitle(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  string
	}{
		{value: "ponyville-owner", want: "Ponyville Owner"},
		{value: "canterlot-user-12", want: "Canterlot User 12"},
		{value: "  cloudsdale--client  ", want: "Cloudsdale Client"},
		{value: "Manehattan", want: "Manehattan"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			if got := adminMigrationScaleFixtureTitle(tc.value); got != tc.want {
				t.Fatalf("adminMigrationScaleFixtureTitle(%q) = %q, want %q", tc.value, got, tc.want)
			}
		})
	}
}

func TestAdminMigrationProductionScaleFixtureCountsAndCoverage(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	spec, err := adminMigrationScaleFixtureSpec(adminMigrationScaleProfileSmall)
	if err != nil {
		t.Fatalf("adminMigrationScaleFixtureSpec() error = %v", err)
	}
	counts := adminMigrationScaleFixtureExpectedCounts(fixture)
	orgCount := len(spec.Organizations)
	usersPerOrg := 1 + spec.ExtraUsersPerOrg

	requireAdminMigrationScaleFixtureCount(t, counts, "", "users", 1+orgCount*usersPerOrg)
	requireAdminMigrationScaleFixtureCount(t, counts, "", "user_acls", 1+orgCount*usersPerOrg)
	requireAdminMigrationScaleFixtureCount(t, counts, "", "user_keys", 1+orgCount*usersPerOrg)
	requireAdminMigrationScaleFixtureCount(t, counts, "", "organizations", orgCount)
	requireAdminMigrationScaleFixtureCount(t, counts, "", "server_admin_memberships", 1+orgCount)

	for _, orgName := range spec.Organizations {
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "clients", 1+spec.ClientsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "client_keys", 1+spec.ClientsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "groups", 5)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "containers", len(adminMigrationRequiredDefaultContainers()))
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "nodes", spec.NodesPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "environments", 1+spec.EnvironmentsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "roles", spec.RolesPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "data_bags", spec.DataBagsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "data_bag_items", spec.DataBagsPerOrg*spec.DataBagItemsPerBag)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "policy_revisions", spec.PoliciesPerOrg*spec.PolicyRevisionsPerPolicy)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "policy_groups", spec.PolicyGroupsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "policy_assignments", spec.PolicyGroupsPerOrg*spec.PoliciesPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "sandboxes", spec.SandboxesPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "cookbook_versions", spec.CookbookVersionsPerOrg)
		requireAdminMigrationScaleFixtureCount(t, counts, orgName, "cookbook_artifacts", spec.CookbookArtifactsPerOrg)

		org := fixture.CoreObjects.Orgs[orgName]
		encrypted := org.DataBagItems["encrypted_secrets"]
		if len(encrypted) != spec.DataBagItemsPerBag {
			t.Fatalf("%s encrypted fixture items = %d, want %d", orgName, len(encrypted), spec.DataBagItemsPerBag)
		}
		for _, item := range encrypted {
			password, ok := item.RawData["password"].(map[string]any)
			if !ok || password["encrypted_data"] == "" || password["cipher"] == "" {
				t.Fatalf("%s encrypted item = %#v, want encrypted-looking payload", orgName, item.RawData)
			}
			break
		}
	}
}

func TestAdminMigrationProductionScaleFixtureBlobReferencesAreValidAndShared(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	blobByChecksum := map[string][]byte{}
	for _, blob := range fixture.BlobCopies {
		sum := md5.Sum(blob.Body)
		if got := hex.EncodeToString(sum[:]); got != blob.Checksum {
			t.Fatalf("blob checksum = %s, want md5(body) %s", blob.Checksum, got)
		}
		blobByChecksum[blob.Checksum] = blob.Body
	}
	if _, ok := blobByChecksum[fixture.SharedChecksum]; !ok {
		t.Fatalf("shared checksum %s missing from blob copies %v", fixture.SharedChecksum, adminMigrationScaleFixtureSortedChecksums(fixture))
	}

	cookbookInventory := adminMigrationCookbookInventoryFromExport(fixture.Cookbooks)
	refs := adminMigrationBlobReferencesFromState(fixture.CoreObjects, cookbookInventory, "")
	if len(refs) == 0 {
		t.Fatal("fixture produced no blob references")
	}
	for _, ref := range refs {
		if _, ok := blobByChecksum[ref.Checksum]; !ok {
			t.Fatalf("blob reference %+v missing from copied blobs %v", ref, adminMigrationScaleFixtureSortedChecksums(fixture))
		}
	}

	var sandboxShared, versionShared, artifactShared bool
	for _, orgName := range adminMigrationSortedMapKeys(fixture.CoreObjects.Orgs) {
		for _, sandbox := range fixture.CoreObjects.Orgs[orgName].Sandboxes {
			sandboxShared = sandboxShared || adminMigrationStringSliceContains(sandbox.Checksums, fixture.SharedChecksum)
		}
		for _, version := range fixture.Cookbooks.Orgs[orgName].Versions {
			for _, file := range version.AllFiles {
				versionShared = versionShared || file.Checksum == fixture.SharedChecksum
			}
		}
		for _, artifact := range fixture.Cookbooks.Orgs[orgName].Artifacts {
			for _, file := range artifact.AllFiles {
				artifactShared = artifactShared || file.Checksum == fixture.SharedChecksum
			}
		}
	}
	if !sandboxShared || !versionShared || !artifactShared {
		t.Fatalf("shared checksum coverage sandbox=%v version=%v artifact=%v, want all true", sandboxShared, versionShared, artifactShared)
	}
}

// TestAdminMigrationScaleBlobValidationFilesystemReportsRecoverySignals proves
// the production-scale fixture emits the blob evidence operators need for a
// filesystem-backed recovery drill.
func TestAdminMigrationScaleBlobValidationFilesystemReportsRecoverySignals(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	writeAdminMigrationScaleFixtureBlobs(t, blobRoot, fixture)
	_ = writeAdminMigrationTestBlob(t, blobRoot, "unreferenced production-scale orphan candidate")
	if err := os.WriteFile(filepath.Join(blobRoot, "not-a-chef-checksum"), []byte("ignored"), 0o644); err != nil {
		t.Fatalf("WriteFile(non-checksum orphan) error = %v", err)
	}

	store, err := blob.NewStore(config.Config{BlobBackend: "filesystem", BlobStorageURL: blobRoot})
	if err != nil {
		t.Fatalf("blob.NewStore(filesystem) error = %v", err)
	}
	checker, ok := store.(blob.Checker)
	if !ok {
		t.Fatalf("filesystem blob store does not implement blob.Checker")
	}
	cookbookInventory := adminMigrationCookbookInventoryFromExport(fixture.Cookbooks)
	refs := adminMigrationBlobReferencesFromState(fixture.CoreObjects, cookbookInventory, "")
	uniqueRefs := adminMigrationUniqueBlobReferences(refs)

	families, findings := adminMigrationValidateBlobReferences(context.Background(), store, checker, refs)
	if got := len(uniqueRefs); got != len(fixture.BlobCopies) {
		t.Fatalf("unique refs = %d, want copied blobs %d", got, len(fixture.BlobCopies))
	}
	requireAdminMigrationScaleBlobFamilyCount(t, families, "referenced_blobs", len(fixture.BlobCopies))
	requireAdminMigrationScaleBlobFamilyCount(t, families, "reachable_blobs", len(fixture.BlobCopies))
	requireAdminMigrationScaleBlobFamilyCount(t, families, "content_verified_blobs", len(fixture.BlobCopies))
	requireAdminMigrationScaleBlobFamilyCount(t, families, "missing_blobs", 0)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "checksum_mismatch_blobs", 0)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "provider_unavailable_checks", 0)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "candidate_orphan_blobs", 1)
	requireAdminMigrationFindingFromStructs(t, findings, "candidate_orphan_blobs")

	sharedRef, ok := adminMigrationScaleBlobReference(uniqueRefs, fixture.SharedChecksum)
	if !ok {
		t.Fatalf("shared checksum %s missing from unique refs %+v", fixture.SharedChecksum, uniqueRefs)
	}
	if sharedRef.Organization != "" || sharedRef.Family != "checksum_references" {
		t.Fatalf("shared checksum ref = %+v, want global checksum_references because sandbox/cookbook/artifact reuse it", sharedRef)
	}
}

// TestAdminMigrationScaleBlobValidationFilesystemDetectsMissingAndMismatch
// damages fixture blobs to pin missing and checksum-mismatch reporting.
func TestAdminMigrationScaleBlobValidationFilesystemDetectsMissingAndMismatch(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	writeAdminMigrationScaleFixtureBlobs(t, blobRoot, fixture)
	firstChecksum, secondChecksum := adminMigrationScaleFixtureTwoUniqueChecksums(t, fixture)
	if err := os.Remove(filepath.Join(blobRoot, firstChecksum)); err != nil {
		t.Fatalf("Remove(missing blob %s) error = %v", firstChecksum, err)
	}
	if err := os.WriteFile(filepath.Join(blobRoot, secondChecksum), []byte("wrong production-scale blob bytes"), 0o644); err != nil {
		t.Fatalf("WriteFile(mismatched blob %s) error = %v", secondChecksum, err)
	}

	store, err := blob.NewStore(config.Config{BlobBackend: "filesystem", BlobStorageURL: blobRoot})
	if err != nil {
		t.Fatalf("blob.NewStore(filesystem) error = %v", err)
	}
	checker := store.(blob.Checker)
	refs := adminMigrationBlobReferencesFromState(fixture.CoreObjects, adminMigrationCookbookInventoryFromExport(fixture.Cookbooks), "")
	families, findings := adminMigrationValidateBlobReferences(context.Background(), store, checker, refs)

	requireAdminMigrationScaleBlobFamilyCount(t, families, "referenced_blobs", len(fixture.BlobCopies))
	requireAdminMigrationScaleBlobFamilyCount(t, families, "reachable_blobs", len(fixture.BlobCopies)-1)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "content_verified_blobs", len(fixture.BlobCopies)-1)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "missing_blobs", 1)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "checksum_mismatch_blobs", 1)
	requireAdminMigrationScaleBlobFamilyCount(t, families, "candidate_orphan_blobs", 0)
	requireAdminMigrationFindingFromStructs(t, findings, "missing_blob")
	requireAdminMigrationFindingFromStructs(t, findings, "blob_checksum_mismatch")
}

// TestAdminMigrationScaleBlobValidationRedactsProviderFailures makes sure
// provider diagnostics stay useful without exposing URLs, signatures, or secrets.
func TestAdminMigrationScaleBlobValidationRedactsProviderFailures(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	leakyProviderErr := fmt.Errorf("provider https://access:secret@blob.example/checksums/%s?X-Amz-Signature=sig-secret&X-Amz-Credential=cred-secret returned body correct horse battery staple with secret-key", fixture.SharedChecksum)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "s3-compatible", Configured: true, Message: "S3-compatible test provider"},
			err:    leakyProviderErr,
			exists: map[string]bool{},
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization: fixture.DefaultOrganization,
			PostgresDSN:         "postgres://scale:supersecret@postgres.example/opencook",
			BlobBackend:         "s3",
			BlobStorageURL:      "s3://secret-bucket/checksums",
			BlobS3Endpoint:      "https://access:secret@blob.example",
			BlobS3AccessKeyID:   "access-secret",
			BlobS3SecretKey:     "secret-key",
			BlobS3SessionToken:  "session-secret",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore:  &fakeOfflineStore{bootstrap: fixture.Bootstrap, objects: fixture.CoreObjects},
			cookbookInventory: adminMigrationCookbookInventoryFromExport(fixture.Cookbooks),
			cookbookExport:    fixture.Cookbooks,
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "preflight", "--all-orgs", "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(scale blob redaction preflight) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "blob_provider_unavailable")
	for _, forbidden := range []string{"access:secret", "blob.example", "X-Amz-Signature", "X-Amz-Credential", "correct horse", "secret-key", "access-secret", "session-secret", "secret-bucket"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("provider failure leaked %q; stdout = %s stderr = %s", forbidden, stdout.String(), stderr.String())
		}
	}
}

func TestAdminMigrationProductionScaleFixtureSourceRowsAreUnique(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	state := adminMigrationScaleFixtureSourceState(fixture)
	scopes := adminMigrationScaleFixtureSourceScopes(fixture)
	records := adminMigrationSourceSyncRecords(state, scopes)
	counts := adminMigrationScaleFixtureExpectedCounts(fixture)

	for scope := range scopes {
		rows, ok := records[scope]
		if !ok || len(rows) == 0 {
			t.Fatalf("source scope %+v missing rows in records %+v", scope, records)
		}
		if scope.Family == "server_admin_memberships" {
			continue
		}
		if want, ok := counts[scope]; ok && len(rows) != want {
			t.Fatalf("source rows %+v = %d, want inventory count %d", scope, len(rows), want)
		}
		for id := range rows {
			if strings.TrimSpace(id) == "" {
				t.Fatalf("source scope %+v has blank row id in %v", scope, rows)
			}
		}
	}
}

func TestAdminMigrationProductionScaleFixtureProfilesGrow(t *testing.T) {
	defaultFixture := requireAdminMigrationScaleFixture(t, "")
	small := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	medium := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileMedium)
	large := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileLarge)

	if len(defaultFixture.CoreObjects.Orgs) != len(small.CoreObjects.Orgs) {
		t.Fatalf("default profile orgs = %d, want small profile orgs %d", len(defaultFixture.CoreObjects.Orgs), len(small.CoreObjects.Orgs))
	}
	smallNodes := adminMigrationScaleFixtureTotalOrgCount(small, "nodes")
	mediumNodes := adminMigrationScaleFixtureTotalOrgCount(medium, "nodes")
	largeNodes := adminMigrationScaleFixtureTotalOrgCount(large, "nodes")
	if !(smallNodes < mediumNodes && mediumNodes < largeNodes) {
		t.Fatalf("node counts small/medium/large = %d/%d/%d, want increasing", smallNodes, mediumNodes, largeNodes)
	}
	if _, err := adminMigrationProductionScaleFixture("unknown"); err == nil {
		t.Fatal("adminMigrationProductionScaleFixture(unknown) error = nil, want error")
	}
}

func TestAdminMigrationScaleFixtureCreateCommandWritesNormalizedSourceBundle(t *testing.T) {
	output := filepath.Join(t.TempDir(), "scale-source")
	cmd, stdout, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "scale-fixture", "create",
		"--profile", adminMigrationScaleProfileSmall,
		"--output", output,
		"--yes",
		"--with-timing",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(scale fixture create) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_scale_fixture_create" {
		t.Fatalf("command = %v, want migration_scale_fixture_create", out["command"])
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("scale fixture output missing duration_ms: %v", out)
	}
	config := requireAdminMigrationMap(t, out, "config")
	if config["scale_profile"] != adminMigrationScaleProfileSmall || config["source_type"] != "production_scale_fixture" {
		t.Fatalf("config = %v, want small production scale fixture", config)
	}
	report := requireAdminMigrationMap(t, out, "operator_report")
	if !strings.Contains(fmt.Sprint(report["summary"]), "migration_scale_fixture_create passed") {
		t.Fatalf("operator report summary = %v, want passed scale fixture", report["summary"])
	}
	reportInventory := requireAdminMigrationMap(t, report, "inventory")
	if reportInventory["total"] == float64(0) {
		t.Fatalf("operator report inventory = %v, want non-zero total", reportInventory)
	}
	requireAdminMigrationArrayContainsString(t, requireAdminMigrationArray(t, report, "next_steps"), "migration-scale-all")
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "scale_fixture", "ok")
	requireAdminMigrationDependency(t, deps, "normalized_source_output", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", 5)
	requireAdminMigrationInventoryFamily(t, families, adminMigrationScaleFixtureDefaultOrg, "nodes", 2)
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "wrote deterministic production-scale normalized source files")

	if _, err := os.Stat(filepath.Join(output, adminMigrationSourceManifestPath)); err != nil {
		t.Fatalf("scale source manifest missing: %v", err)
	}
	if read, err := adminMigrationReadSourceImportBundle(output); err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(scale output) error = %v", err)
	} else if read.Bundle.SourceType != "production_scale_fixture" {
		t.Fatalf("source type = %q, want production_scale_fixture", read.Bundle.SourceType)
	}
}

func TestAdminMigrationBackupCreateWithScaleFixtureReportsInspectCoverage(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	blobRoot := filepath.Join(t.TempDir(), "blobs")
	writeAdminMigrationScaleFixtureBlobs(t, blobRoot, fixture)
	outputPath := filepath.Join(t.TempDir(), "opencook-backup")

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = blob.NewStore
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization:     fixture.DefaultOrganization,
			PostgresDSN:             "postgres://scale:supersecret@postgres.example/opencook",
			BlobBackend:             "filesystem",
			BlobStorageURL:          blobRoot,
			BlobS3SecretKey:         "secret-key",
			BootstrapRequestorName:  adminMigrationScaleFixtureSuperuser,
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return &fakeMigrationInventoryStore{
			fakeOfflineStore:  &fakeOfflineStore{bootstrap: fixture.Bootstrap, objects: fixture.CoreObjects},
			cookbookInventory: adminMigrationCookbookInventoryFromExport(fixture.Cookbooks),
			cookbookExport:    fixture.Cookbooks,
		}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "backup", "create", "--output", outputPath, "--offline", "--yes", "--with-timing", "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale backup create) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("backup create output missing duration_ms: %v", out)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_blob_copy", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", len(fixture.Bootstrap.Orgs))
	requireAdminMigrationInventoryFamily(t, families, "", "copied_blobs", len(fixture.BlobCopies))

	manifest := mustReadAdminMigrationBackupManifest(t, outputPath)
	for _, path := range append(adminMigrationRequiredRestorePayloadPaths(), adminMigrationBackupRunbookPath) {
		requireAdminMigrationPayloadHash(t, outputPath, manifest.Payloads, path)
	}
	var blobManifest adminMigrationBackupBlobManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, outputPath, adminMigrationBackupBlobsPath), &blobManifest); err != nil {
		t.Fatalf("blob manifest json.Unmarshal error = %v", err)
	}
	if len(blobManifest.Copied) != len(fixture.BlobCopies) {
		t.Fatalf("copied blobs = %d, want fixture copied blobs %d", len(blobManifest.Copied), len(fixture.BlobCopies))
	}
	for _, copied := range blobManifest.Copied {
		requireAdminMigrationPayloadHash(t, outputPath, manifest.Payloads, copied.Path)
	}
	for _, forbidden := range []string{"supersecret", "secret-key", "BEGIN RSA PRIVATE KEY", "BEGIN PRIVATE KEY"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(readAdminMigrationBundleText(t, outputPath), forbidden) {
			t.Fatalf("scale backup leaked forbidden material %q", forbidden)
		}
	}

	firstInspect := runAdminMigrationScaleBackupInspect(t, outputPath)
	secondInspect := runAdminMigrationScaleBackupInspect(t, outputPath)
	requireAdminMigrationScaleBackupInspectCoverage(t, firstInspect, manifest, fixture)
	requireAdminMigrationScaleBackupInspectCoverage(t, secondInspect, manifest, fixture)
}

func TestAdminMigrationBackupInspectDetectsScaleBundleIntegrityFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*testing.T, string)
		wantCode   string
		wantStatus int
	}{
		{
			name: "missing listed payload",
			mutate: func(t *testing.T, root string) {
				if err := os.Remove(filepath.Join(root, adminMigrationBackupObjectsPath)); err != nil {
					t.Fatalf("Remove(core objects) error = %v", err)
				}
			},
			wantCode: "backup_payload_integrity_failed",
		},
		{
			name: "omitted copied blob payload",
			mutate: func(t *testing.T, root string) {
				blobManifest := readAdminMigrationScaleBackupBlobManifest(t, root)
				removeAdminMigrationScaleBackupManifestPayload(t, root, blobManifest.Copied[0].Path)
			},
			wantCode: "backup_blob_payload_missing",
		},
		{
			name: "truncated copied blob",
			mutate: func(t *testing.T, root string) {
				blobManifest := readAdminMigrationScaleBackupBlobManifest(t, root)
				if err := os.WriteFile(filepath.Join(root, blobManifest.Copied[0].Path), []byte("truncated"), 0o644); err != nil {
					t.Fatalf("WriteFile(truncated blob) error = %v", err)
				}
			},
			wantCode: "backup_blob_integrity_failed",
		},
		{
			name: "copied blob checksum mismatch",
			mutate: func(t *testing.T, root string) {
				blobManifest := readAdminMigrationScaleBackupBlobManifest(t, root)
				copied := blobManifest.Copied[0]
				body := []byte("valid sha metadata but wrong chef checksum\n")
				if err := os.WriteFile(filepath.Join(root, copied.Path), body, 0o644); err != nil {
					t.Fatalf("WriteFile(checksum mismatch blob) error = %v", err)
				}
				sum := sha256.Sum256(body)
				blobManifest.Copied[0].SHA256 = hex.EncodeToString(sum[:])
				blobManifest.Copied[0].Bytes = int64(len(body))
				writeAdminMigrationScaleBackupBlobManifest(t, root, blobManifest)
				updateAdminMigrationScaleBackupPayloadHash(t, root, copied.Path)
			},
			wantCode: "backup_blob_checksum_mismatch",
		},
		{
			name: "required restore payload omitted",
			mutate: func(t *testing.T, root string) {
				removeAdminMigrationScaleBackupManifestPayload(t, root, adminMigrationBackupCookbooksPath)
			},
			wantCode: "backup_required_payload_missing",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := writeAdminMigrationScaleBackupBundle(t, requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall))
			tc.mutate(t, root)
			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), []string{"admin", "migration", "backup", "inspect", root, "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(scale backup inspect %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_bundle", "error")
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantCode)
		})
	}
}

func TestAdminMigrationRestoreApplyValidatesScaleFixtureAfterRehydration(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	bundlePath := writeAdminMigrationScaleBackupBundle(t, fixture)
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
			DefaultOrganization:     fixture.DefaultOrganization,
			PostgresDSN:             "postgres://scale:supersecret@postgres.example/opencook",
			BlobBackend:             "memory",
			BootstrapRequestorName:  adminMigrationScaleFixtureSuperuser,
			BootstrapRequestorType:  "user",
			BootstrapRequestorKeyID: "default",
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "restore", "apply", bundlePath, "--offline", "--yes", "--with-timing", "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale restore apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_validation", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", len(fixture.Bootstrap.Orgs))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "nodes", len(fixture.CoreObjects.Orgs[fixture.DefaultOrganization].Nodes))
	requireAdminMigrationInventoryFamily(t, families, "", "content_verified_blobs", len(fixture.BlobCopies))
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("scale restore apply output missing duration_ms: %v", out)
	}
}

func TestAdminMigrationSourceImportPreflightWithScaleFixtureReportsProductionSignals(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "scale source blob backend"},
			exists: map[string]bool{},
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization: fixture.DefaultOrganization,
			PostgresDSN:         "postgres://scale:secret@postgres.example/opencook",
			BlobBackend:         "filesystem",
			BlobStorageURL:      t.TempDir(),
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--with-timing", "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source import preflight) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("scale source import preflight output missing duration_ms: %v", out)
	}
	deps := requireAdminMigrationArray(t, out, "dependencies")
	sourceBundle := requireAdminMigrationDependency(t, deps, "source_bundle", "ok")
	details := requireAdminMigrationMap(t, sourceBundle, "details")
	if details["sidecar_artifacts"] != "3" {
		t.Fatalf("source_bundle sidecar_artifacts = %v, want 3", details["sidecar_artifacts"])
	}
	requireAdminMigrationDependency(t, deps, "source_import_target", "ok")
	requireAdminMigrationDependency(t, deps, "blob", "ok")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "users", len(fixture.Bootstrap.Users))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "nodes", len(fixture.CoreObjects.Orgs[fixture.DefaultOrganization].Nodes))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "cookbook_versions", len(fixture.Cookbooks.Orgs[fixture.DefaultOrganization].Versions))
	requireAdminMigrationInventoryFamily(t, families, "", "cookbook_blob_references", len(fixture.BlobCopies))
	requireAdminMigrationInventoryFamily(t, families, "", "opensearch_source_artifacts", 1)
	requireAdminMigrationInventoryFamily(t, families, "", "referenced_blobs", len(fixture.BlobCopies))
	requireAdminMigrationInventoryFamily(t, families, "", "copied_blobs", len(fixture.BlobCopies))
	findings := requireAdminMigrationArray(t, out, "findings")
	requireAdminMigrationFindingFamily(t, findings, "source_search_rebuild_required", "opensearch")
	requireAdminMigrationFindingFamily(t, findings, "source_artifact_unsupported", "oc_id")
	mutations := requireAdminMigrationArray(t, out, "planned_mutations")
	requireAdminMigrationMutationMessage(t, mutations, "would create PostgreSQL-backed nodes records from normalized source payloads")
	requireAdminMigrationMutationMessage(t, mutations, "would copy checksum-addressed source blob bytes into the configured OpenCook blob backend before metadata import")
	requireAdminMigrationMutationMessage(t, mutations, "opencook admin reindex --all-orgs --complete")
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(blobPuts) != 0 {
		t.Fatalf("scale source preflight mutated bootstrap=%d objects=%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, len(blobPuts))
	}
}

func TestAdminMigrationSourceImportApplyScaleFixtureIsReplaySafeWithProgress(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
	progressPath := filepath.Join(t.TempDir(), "scale-source-import-progress.json")
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobExists := map[string]bool{}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "scale source blob backend"},
			exists: blobExists,
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization: fixture.DefaultOrganization,
			PostgresDSN:         "postgres://scale:secret@postgres.example/opencook",
			BlobBackend:         "filesystem",
			BlobStorageURL:      t.TempDir(),
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--with-timing", "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source import apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_write", "ok")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "copied_blob_writes", len(fixture.BlobCopies))
	if targetStore.bootstrapSaves != 1 || targetStore.objectSaves != 1 {
		t.Fatalf("scale import saves bootstrap=%d objects=%d, want 1/1", targetStore.bootstrapSaves, targetStore.objectSaves)
	}
	if len(blobPuts) != len(fixture.BlobCopies) {
		t.Fatalf("scale import blob puts = %d, want %d", len(blobPuts), len(fixture.BlobCopies))
	}
	progress := mustReadAdminMigrationSourceImportProgress(t, progressPath)
	if !progress.MetadataImported || len(progress.CopiedBlobs) != len(fixture.BlobCopies) {
		t.Fatalf("scale import progress = %#v, want metadata imported and all copied blobs", progress)
	}

	firstBootstrapSaves := targetStore.bootstrapSaves
	firstObjectSaves := targetStore.objectSaves
	blobPuts = map[string][]byte{}
	stdout.Reset()
	stderr.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source import replay) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	replayOut := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, replayOut, "dependencies"), "source_import_target", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, replayOut, "dependencies"), "source_import_write", "ok")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, replayOut, "inventory"), "families"), "", "progress_reused_blobs", len(fixture.BlobCopies))
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, replayOut, "planned_mutations"), "source import progress already marked metadata imported and target state still matches")
	if targetStore.bootstrapSaves != firstBootstrapSaves || targetStore.objectSaves != firstObjectSaves || len(blobPuts) != 0 {
		t.Fatalf("scale import replay mutated saves=%d/%d from %d/%d blobs=%d", targetStore.bootstrapSaves, targetStore.objectSaves, firstBootstrapSaves, firstObjectSaves, len(blobPuts))
	}
}

func TestAdminMigrationSourceSyncScaleFixtureRerunsWithCursorProgress(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
	progressPath := filepath.Join(t.TempDir(), "scale-source-sync-progress.json")
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(scale) error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead(scale) error = %v", err)
	}
	targetBootstrap := bootstrap.CloneBootstrapCoreState(sourceState.Bootstrap)
	targetUser := targetBootstrap.Users[adminMigrationScaleFixtureSuperuser]
	targetUser.DisplayName = "Stale Scale Superuser"
	targetBootstrap.Users[adminMigrationScaleFixtureSuperuser] = targetUser
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{bootstrap: targetBootstrap, objects: bootstrap.CloneCoreObjectState(sourceState.CoreObjects)},
		cookbookInventory: adminMigrationCookbookInventoryFromExport(sourceState.Cookbooks),
		cookbookExport:    adminMigrationCloneCookbookExport(sourceState.Cookbooks),
	}
	blobExists := map[string]bool{}
	blobPuts := map[string][]byte{}
	for _, blobCopy := range fixture.BlobCopies {
		blobExists[blobCopy.Checksum] = true
	}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "scale source blob backend"},
			exists: blobExists,
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization: fixture.DefaultOrganization,
			PostgresDSN:         "postgres://scale:secret@postgres.example/opencook",
			BlobBackend:         "filesystem",
			BlobStorageURL:      t.TempDir(),
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source sync apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if targetStore.bootstrap.Users[adminMigrationScaleFixtureSuperuser].DisplayName != sourceState.Bootstrap.Users[adminMigrationScaleFixtureSuperuser].DisplayName {
		t.Fatalf("scale sync user = %#v, want source display name", targetStore.bootstrap.Users[adminMigrationScaleFixtureSuperuser])
	}
	progress := mustReadAdminMigrationSourceSyncProgress(t, progressPath)
	cursor := adminMigrationSourceSyncCursor(read)
	if progress.SourceCursor != cursor || progress.LastStatus != "applied" || !adminMigrationTestStringSliceContains(progress.AppliedCursors, cursor) {
		t.Fatalf("scale sync progress = %#v, want cursor %s applied", progress, cursor)
	}
	firstBootstrapSaves := targetStore.bootstrapSaves
	firstObjectSaves := targetStore.objectSaves

	stdout.Reset()
	stderr.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "preflight", sourcePath, "--offline", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source sync preflight rerun) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	preflightOut := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, preflightOut, "planned_mutations"), "source sync found no PostgreSQL metadata changes for manifest-covered families")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, preflightOut, "inventory"), "families"), "", "users_unchanged", len(sourceState.Bootstrap.Users))

	stdout.Reset()
	stderr.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", progressPath, "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale source sync apply rerun) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	replayOut := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, replayOut, "planned_mutations"), "source sync found no PostgreSQL metadata changes for manifest-covered families")
	if targetStore.bootstrapSaves != firstBootstrapSaves || targetStore.objectSaves != firstObjectSaves {
		t.Fatalf("scale sync rerun saved metadata=%d/%d from %d/%d", targetStore.bootstrapSaves, targetStore.objectSaves, firstBootstrapSaves, firstObjectSaves)
	}
}

func TestAdminMigrationShadowCompareScaleCoverageReportsFamilyDiffsAndReadLikePosts(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
	fake := adminMigrationShadowClientForSourceCoverage(t, sourcePath, adminMigrationShadowCoverageScale, nil)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://opencook.test", RequestorName: adminMigrationScaleFixtureSuperuser, PrivateKeyPath: "/keys/pivotal.pem"}
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return fake, nil
	}

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "shadow", "compare",
		"--source", sourcePath,
		"--target-server-url", "http://opencook.test",
		"--coverage", "scale",
		"--with-timing",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(scale shadow compare) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("scale shadow compare output missing duration_ms: %v", out)
	}
	shadowDep := requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "shadow_read_compare", "ok")
	if details := requireAdminMigrationMap(t, shadowDep, "details"); details["coverage"] != adminMigrationShadowCoverageScale {
		t.Fatalf("shadow coverage detail = %v, want %q", details["coverage"], adminMigrationShadowCoverageScale)
	}
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "shadow_failed", 0)
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "shadow_nodes_checks", len(fixture.CoreObjects.Orgs[fixture.DefaultOrganization].Nodes))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "shadow_nodes_collection_checks", 1)
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "shadow_partial_search_checks", len(adminMigrationShadowSearchIndexes(fixture.Bootstrap.Orgs[fixture.DefaultOrganization], fixture.CoreObjects.Orgs[fixture.DefaultOrganization])))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "shadow_depsolver_checks", 1)
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "shadow_cookbook_versions_downloads", len(fixture.Cookbooks.Orgs[fixture.DefaultOrganization].Versions))
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "do not proxy writes, validator registration, cookbook upload, sandbox commit, or key/client mutations during shadow comparison")

	requireAdminMigrationFakeCall(t, fake.calls, http.MethodGet, "/users", nil)
	requireAdminMigrationFakeCall(t, fake.calls, http.MethodGet, "/organizations/"+fixture.DefaultOrganization+"/nodes", nil)
	requireAdminMigrationFakeCall(t, fake.calls, http.MethodGet, "/organizations/"+fixture.DefaultOrganization+"/search/node?q=*:*&start=0&rows=1", nil)
	requireAdminMigrationFakeCall(t, fake.calls, http.MethodGet, "/organizations/"+fixture.DefaultOrganization+"/search/node?q=*:*&start=1&rows=1", nil)
	requireAdminMigrationFakeCall(t, fake.calls, http.MethodPost, "/organizations/"+fixture.DefaultOrganization+"/search/node?q=*:*", map[string]any{"name": []any{"name"}})
	requireAdminMigrationFakeCall(t, fake.calls, http.MethodPost, "/organizations/"+fixture.DefaultOrganization+"/environments/_default/cookbook_versions", map[string]any{"run_list": []any{}})
	if len(fake.downloadCalls) <= 2 {
		t.Fatalf("scale shadow downloads = %d, want more than representative cookbook/artifact downloads", len(fake.downloadCalls))
	}
	for _, call := range fake.calls {
		switch call.method {
		case http.MethodGet:
			if call.payload != nil {
				t.Fatalf("scale shadow GET sent payload for %s: %#v", call.path, call.payload)
			}
		case http.MethodPost:
			if !strings.Contains(call.path, "/search/") && !strings.Contains(call.path, "/cookbook_versions") {
				t.Fatalf("scale shadow POST %s is not a read-like Chef route", call.path)
			}
			if call.payload == nil {
				t.Fatalf("scale shadow POST %s sent nil payload, want signed read-like request body", call.path)
			}
		default:
			t.Fatalf("scale shadow issued mutating method %s %s", call.method, call.path)
		}
	}
}

// TestAdminMigrationCutoverRehearsalScaleFixtureDownloadsCookbookAndArtifactBlobs
// verifies restored cookbook and artifact payloads still expose usable signed
// blob downloads after all cutover evidence gates pass.
func TestAdminMigrationCutoverRehearsalScaleFixtureDownloadsCookbookAndArtifactBlobs(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	bundlePath := writeAdminMigrationScaleBackupBundle(t, fixture)
	sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
	sourceRead, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(scale) error = %v", err)
	}
	allChecksums := adminMigrationScaleFixtureSortedChecksums(fixture)
	importProgressPath := adminMigrationSourceImportProgressFile(sourcePath, "")
	if err := adminMigrationWriteSourceImportProgress(sourcePath, importProgressPath, adminMigrationSourceImportProgress{
		MetadataImported: true,
		CopiedBlobs:      allChecksums,
		VerifiedBlobs:    allChecksums,
	}); err != nil {
		t.Fatalf("adminMigrationWriteSourceImportProgress(scale) error = %v", err)
	}
	syncProgressPath := adminMigrationSourceSyncProgressFile(sourcePath, "")
	cursor := adminMigrationSourceSyncCursor(sourceRead)
	if err := adminMigrationWriteSourceSyncProgress(sourcePath, syncProgressPath, adminMigrationSourceSyncProgress{
		SourceCursor:   cursor,
		LastStatus:     "applied",
		AppliedCursors: []string{cursor},
	}); err != nil {
		t.Fatalf("adminMigrationWriteSourceSyncProgress(scale) error = %v", err)
	}
	searchResultPath := filepath.Join(t.TempDir(), "scale-search-check.json")
	writeAdminMigrationSourceJSON(t, searchResultPath, map[string]any{
		"ok":      true,
		"command": "search_check",
		"counts":  map[string]any{"missing": 0, "stale": 0, "unsupported": 0, "failed": 0, "clean": 7},
	})
	shadowResultPath := filepath.Join(t.TempDir(), "scale-shadow-compare.json")
	writeAdminMigrationSourceJSON(t, shadowResultPath, map[string]any{
		"ok":      true,
		"command": "migration_shadow_compare",
		"inventory": map[string]any{
			"families": []map[string]any{{"family": "shadow_failed", "count": 0}},
		},
		"errors": []any{},
	})
	maintenanceResultPath := filepath.Join(t.TempDir(), "scale-maintenance-status.json")
	writeAdminMigrationSourceJSON(t, maintenanceResultPath, map[string]any{
		"ok":      true,
		"command": "maintenance_status",
		"active":  true,
		"expired": false,
		"backend": map[string]any{"name": "postgres", "shared": true},
		"state":   map[string]any{"mode": "cutover"},
	})
	fake, expectedDownloads := adminMigrationScaleRehearsalClientForBackupBundle(t, fixture, bundlePath)
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://env.example", RequestorName: "env-user", PrivateKeyPath: "/env/key.pem", ServerAPIVersion: "1"}
	}
	cmd.newAdmin = func(cfg admin.Config) (adminJSONClient, error) {
		if cfg.ServerURL != "https://admin:secret@opencook.test" || cfg.RequestorName != adminMigrationScaleFixtureSuperuser || cfg.PrivateKeyPath != "/keys/pivotal.pem" {
			t.Fatalf("admin config = %+v, want scale cutover overrides", cfg)
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
		"--maintenance-result", maintenanceResultPath,
		"--source-frozen",
		"--rollback-ready",
		"--server-url", "https://admin:secret@opencook.test",
		"--requestor-name", adminMigrationScaleFixtureSuperuser,
		"--private-key", "/keys/pivotal.pem",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(scale migration cutover rehearse) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob_integrity_evidence", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob_reachability", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "cutover_rehearsal", "ok")
	report := requireAdminMigrationMap(t, out, "operator_report")
	reportEvidence := requireAdminMigrationArray(t, report, "evidence")
	requireAdminMigrationOperatorEvidence(t, reportEvidence, "source_freeze_evidence", "ok")
	requireAdminMigrationOperatorEvidence(t, reportEvidence, "maintenance_evidence", "ok")
	requireAdminMigrationOperatorEvidence(t, reportEvidence, "rollback_readiness", "ok")
	requireAdminMigrationArrayContainsString(t, requireAdminMigrationArray(t, report, "guidance"), "source freeze evidence: ok")
	requireAdminMigrationArrayContainsString(t, requireAdminMigrationArray(t, report, "next_steps"), "keep source Chef available")
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_failed", 0)
	requireAdminMigrationInventoryFamily(t, families, "", "rehearsal_downloads", expectedDownloads)
	if len(fake.downloadCalls) != expectedDownloads {
		t.Fatalf("scale rehearsal downloads = %d, want %d; calls=%v", len(fake.downloadCalls), expectedDownloads, fake.downloadCalls)
	}
	requireAdminMigrationScaleFakeCallFamily(t, fake.calls, "cookbooks")
	requireAdminMigrationScaleFakeCallFamily(t, fake.calls, "cookbook_artifacts")
	for _, forbidden := range []string{"admin:secret", "/keys/pivotal.pem", "signature=secret", "X-Amz-Credential"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("scale cutover output leaked %q; stdout = %s stderr = %s", forbidden, stdout.String(), stderr.String())
		}
	}
}

func TestAdminMigrationSourceImportScaleFixtureRejectsInvalidAndMissingInputsWithoutMutation(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	t.Run("duplicate record before provider load", func(t *testing.T) {
		sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, false)
		appendAdminMigrationScaleSourcePayload(t, sourcePath, adminMigrationSourcePayloadKey{Family: "users"}, fixture.Bootstrap.Users[adminMigrationScaleFixtureSuperuser])
		cmd, stdout, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			t.Fatal("duplicate source payload must fail before provider configuration loads")
			return config.Config{}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(scale duplicate source preflight) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
		}
		requireAdminMigrationFinding(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "findings"), "source_duplicate_user")
	})
	t.Run("orphan reference before provider load", func(t *testing.T) {
		sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, false)
		appendAdminMigrationScaleSourcePayload(t, sourcePath, adminMigrationSourcePayloadKey{Organization: fixture.DefaultOrganization, Family: "group_memberships"}, map[string]string{
			"group": "admins",
			"type":  "user",
			"actor": "missing-scale-user",
		})
		cmd, stdout, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			t.Fatal("orphan source payload must fail before provider configuration loads")
			return config.Config{}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(scale orphan source preflight) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
		}
		requireAdminMigrationFinding(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "findings"), "source_orphan_group_membership")
	})
	t.Run("missing copied blob does not import metadata", func(t *testing.T) {
		sourcePath := writeAdminMigrationScaleSourceBundle(t, fixture, true)
		removeAdminMigrationScaleSourceCopiedBlob(t, sourcePath, fixture.SharedChecksum)
		targetStore := &fakeMigrationInventoryStore{
			fakeOfflineStore:  &fakeOfflineStore{bootstrap: bootstrap.BootstrapCoreState{}, objects: bootstrap.CoreObjectState{}},
			cookbookInventory: map[string]adminMigrationCookbookInventory{},
			cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
		}
		cmd, stdout, stderr := newTestCommand(t)
		cmd.newBlobStore = func(config.Config) (blob.Store, error) {
			return fakeMigrationBlobStore{
				status: blob.Status{Backend: "filesystem", Configured: true, Message: "scale source blob backend"},
				exists: map[string]bool{},
				puts:   map[string][]byte{},
			}, nil
		}
		cmd.loadOffline = func() (config.Config, error) {
			return config.Config{
				DefaultOrganization: fixture.DefaultOrganization,
				PostgresDSN:         "postgres://scale:secret@postgres.example/opencook",
				BlobBackend:         "filesystem",
				BlobStorageURL:      t.TempDir(),
			}, nil
		}
		cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
			return targetStore, nil, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--json"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(scale missing blob source apply) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
		}
		out := decodeAdminMigrationOutput(t, stdout.String())
		requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "blob", "ok")
		requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_blob_payload_missing")
		requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "missing_blobs", 1)
		if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 || len(targetStore.restoredCookbooks.Orgs) != 0 {
			t.Fatalf("missing source blob imported metadata bootstrap=%d objects=%d cookbooks=%#v", targetStore.bootstrapSaves, targetStore.objectSaves, targetStore.restoredCookbooks)
		}
	})
}

// requireAdminMigrationScaleFixture keeps fixture construction failures close to
// the test that requested a specific deterministic scale profile.
func requireAdminMigrationScaleFixture(t *testing.T, profile string) adminMigrationScaleFixture {
	t.Helper()
	fixture, err := adminMigrationProductionScaleFixture(profile)
	if err != nil {
		t.Fatalf("adminMigrationProductionScaleFixture(%q) error = %v", profile, err)
	}
	return fixture
}

// requireAdminMigrationFakeCall proves shadow comparison used only the intended
// read-like request shape for a path.
func requireAdminMigrationFakeCall(t *testing.T, calls []fakeAdminCall, method, path string, payload any) {
	t.Helper()
	for _, call := range calls {
		if call.method != method || call.path != path {
			continue
		}
		if !reflect.DeepEqual(call.payload, payload) {
			t.Fatalf("fake call %s %s payload = %#v, want %#v", method, path, call.payload, payload)
		}
		return
	}
	t.Fatalf("missing fake call %s %s in %+v", method, path, calls)
}

// requireAdminMigrationScaleFakeCallFamily checks that scale rehearsal touched
// at least one route segment for a restored family without coupling to org order.
func requireAdminMigrationScaleFakeCallFamily(t *testing.T, calls []fakeAdminCall, segment string) {
	t.Helper()
	needle := "/" + strings.Trim(segment, "/") + "/"
	for _, call := range calls {
		if strings.Contains(call.path, needle) || strings.HasSuffix(call.path, "/"+strings.Trim(segment, "/")) {
			return
		}
	}
	t.Fatalf("missing scale rehearsal call containing %q in %+v", needle, calls)
}

// requireAdminMigrationScaleBlobFamilyCount keeps direct blob-validation tests
// independent of family ordering while still checking the exact recovery signal.
func requireAdminMigrationScaleBlobFamilyCount(t *testing.T, families []adminMigrationInventoryFamily, family string, want int) {
	t.Helper()
	for _, item := range families {
		if item.Organization == "" && item.Family == family {
			if item.Count != want {
				t.Fatalf("blob family %s count = %d, want %d in %+v", family, item.Count, want, families)
			}
			return
		}
	}
	t.Fatalf("blob families = %+v, want %s count %d", families, family, want)
}

// requireAdminMigrationFindingFromStructs mirrors the JSON finding helper for
// internal validation paths that return typed findings before output encoding.
func requireAdminMigrationFindingFromStructs(t *testing.T, findings []adminMigrationFinding, code string) adminMigrationFinding {
	t.Helper()
	for _, finding := range findings {
		if finding.Code == code {
			return finding
		}
	}
	t.Fatalf("findings = %+v, want code %s", findings, code)
	return adminMigrationFinding{}
}

// adminMigrationScaleBlobReference locates one checksum in a generated
// reference set so tests can assert shared-checksum aggregation details.
func adminMigrationScaleBlobReference(refs []adminMigrationBlobReference, checksum string) (adminMigrationBlobReference, bool) {
	for _, ref := range refs {
		if ref.Checksum == checksum {
			return ref, true
		}
	}
	return adminMigrationBlobReference{}, false
}

// adminMigrationScaleFixtureBlobBodies indexes fixture blob bytes by checksum
// for fake signed-download clients used by scale cutover rehearsal tests.
func adminMigrationScaleFixtureBlobBodies(fixture adminMigrationScaleFixture) map[string][]byte {
	out := map[string][]byte{}
	for _, copy := range fixture.BlobCopies {
		out[copy.Checksum] = append([]byte(nil), copy.Body...)
	}
	return out
}

// adminMigrationScaleFixtureTwoUniqueChecksums picks two non-shared checksums
// so missing and mismatch drills do not collapse onto the shared-reference row.
func adminMigrationScaleFixtureTwoUniqueChecksums(t *testing.T, fixture adminMigrationScaleFixture) (string, string) {
	t.Helper()
	var selected []string
	for _, checksum := range adminMigrationScaleFixtureSortedChecksums(fixture) {
		if checksum == fixture.SharedChecksum {
			continue
		}
		selected = append(selected, checksum)
		if len(selected) == 2 {
			return selected[0], selected[1]
		}
	}
	t.Fatalf("scale fixture has %d non-shared checksums, want at least 2", len(selected))
	return "", ""
}

// adminMigrationScaleRehearsalClientForBackupBundle builds a fake live target
// that can serve every restored-state rehearsal read plus signed blob downloads.
func adminMigrationScaleRehearsalClientForBackupBundle(t *testing.T, fixture adminMigrationScaleFixture, bundlePath string) (*fakeMigrationRehearsalClient, int) {
	t.Helper()
	blobManifest, err := adminMigrationReadBackupBlobManifest(bundlePath)
	if err != nil {
		t.Fatalf("adminMigrationReadBackupBlobManifest(scale) error = %v", err)
	}
	blobBodies := adminMigrationScaleFixtureBlobBodies(fixture)
	checks := adminMigrationCutoverRehearsalChecks(fixture.Bootstrap, fixture.CoreObjects, fixture.Cookbooks, blobManifest)
	responses := map[string]any{}
	downloads := map[string][]byte{}
	expectedDownloads := 0
	var sawCookbook, sawArtifact bool
	for _, check := range checks {
		if len(check.DownloadChecksums) == 0 {
			continue
		}
		expectedDownloads++
		files := make([]any, 0, len(check.DownloadChecksums))
		for _, checksum := range check.DownloadChecksums {
			body, ok := blobBodies[checksum]
			if !ok {
				t.Fatalf("missing fixture blob body for checksum %s in check %+v", checksum, check)
			}
			rawURL := "https://opencook.test/_blob/checksums/" + checksum + "?signature=secret&X-Amz-Credential=credential"
			files = append(files, map[string]any{"checksum": checksum, "url": rawURL})
			downloads[rawURL] = body
		}
		responses[check.Path] = map[string]any{"all_files": files}
		sawCookbook = sawCookbook || check.Family == "cookbook_versions"
		sawArtifact = sawArtifact || check.Family == "cookbook_artifacts"
	}
	if !sawCookbook || !sawArtifact {
		t.Fatalf("scale rehearsal checks cookbook=%v artifact=%v, want both", sawCookbook, sawArtifact)
	}
	return &fakeMigrationRehearsalClient{
		responses: responses,
		errPaths:  map[string]error{},
		downloads: downloads,
	}, expectedDownloads
}

// requireAdminMigrationScaleFixtureCount checks generated inventory by scoped
// family key so tests are independent of inventory output ordering.
func requireAdminMigrationScaleFixtureCount(t *testing.T, counts map[adminMigrationSourcePayloadKey]int, organization, family string, want int) {
	t.Helper()
	key := adminMigrationSourcePayloadKey{Organization: organization, Family: family}
	if got, ok := counts[key]; !ok || got != want {
		t.Fatalf("fixture count %+v = %d/%v, want %d", key, got, ok, want)
	}
}

// adminMigrationScaleFixtureTotalOrgCount sums one org-scoped family across all
// organizations to compare scale profiles without hard-coding every count.
func adminMigrationScaleFixtureTotalOrgCount(fixture adminMigrationScaleFixture, family string) int {
	counts := adminMigrationScaleFixtureExpectedCounts(fixture)
	total := 0
	for key, count := range counts {
		if key.Organization != "" && key.Family == family {
			total += count
		}
	}
	return total
}

// adminMigrationStringSliceContains keeps shared-checksum assertions readable
// when checking generated sandbox checksum lists.
func adminMigrationStringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// writeAdminMigrationScaleFixtureBlobs materializes fixture blob bodies into the
// filesystem adapter layout used by backup-create integration tests.
func writeAdminMigrationScaleFixtureBlobs(t *testing.T, root string, fixture adminMigrationScaleFixture) {
	t.Helper()
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", root, err)
	}
	for _, blobCopy := range fixture.BlobCopies {
		if err := os.WriteFile(filepath.Join(root, blobCopy.Checksum), blobCopy.Body, 0o644); err != nil {
			t.Fatalf("WriteFile(blob %s) error = %v", blobCopy.Checksum, err)
		}
	}
}

// writeAdminMigrationScaleBackupBundle persists the generated fixture through
// the normal backup writer so inspect failure tests start from a valid bundle.
func writeAdminMigrationScaleBackupBundle(t *testing.T, fixture adminMigrationScaleFixture) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "opencook-backup")
	if _, err := adminMigrationWriteBackupBundle(root, adminMigrationBackupBundleInput{
		Build:       version.Info{Version: "scale-test", Commit: "scale-commit", BuiltAt: "scale-built-at"},
		CreatedAt:   mustParseAdminMigrationTime(t, "2026-04-29T12:00:00Z"),
		Config:      config.Config{PostgresDSN: "postgres://scale:secret@example/opencook", BlobS3SecretKey: "secret-key"},
		Bootstrap:   fixture.Bootstrap,
		CoreObjects: fixture.CoreObjects,
		Cookbooks:   fixture.Cookbooks,
		BlobCopies:  fixture.BlobCopies,
		Inventory:   fixture.Inventory,
	}); err != nil {
		t.Fatalf("adminMigrationWriteBackupBundle(scale) error = %v", err)
	}
	return root
}

// runAdminMigrationScaleBackupInspect executes inspect in the same provider-free
// CLI mode operators use, including timing evidence for Task 3.
func runAdminMigrationScaleBackupInspect(t *testing.T, root string) map[string]any {
	t.Helper()
	cmd, stdout, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "migration", "backup", "inspect", root, "--with-timing", "--json"})
	if code != exitOK {
		t.Fatalf("Run(scale backup inspect) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("backup inspect output missing duration_ms: %v", out)
	}
	return out
}

// requireAdminMigrationScaleBackupInspectCoverage pins the inspect summary so
// future backup hardening cannot accidentally drop hash or blob evidence.
func requireAdminMigrationScaleBackupInspectCoverage(t *testing.T, out map[string]any, manifest adminMigrationBackupManifest, fixture adminMigrationScaleFixture) {
	t.Helper()
	dependency := requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_bundle", "ok")
	details := requireAdminMigrationMap(t, dependency, "details")
	if details["payloads"] != fmt.Sprintf("%d", len(manifest.Payloads)) {
		t.Fatalf("payload details = %v, want %d", details["payloads"], len(manifest.Payloads))
	}
	for _, key := range []string{"hashed_payloads", "payload_bytes", "required_payloads", "referenced_blobs", "copied_blobs", "verified_copied_blobs"} {
		if strings.TrimSpace(fmt.Sprint(details[key])) == "" {
			t.Fatalf("backup inspect details missing %s: %v", key, details)
		}
	}
	if details["copied_blobs"] != fmt.Sprintf("%d", len(fixture.BlobCopies)) {
		t.Fatalf("copied_blobs detail = %v, want %d", details["copied_blobs"], len(fixture.BlobCopies))
	}
	if details["verified_copied_blobs"] != fmt.Sprintf("%d", len(fixture.BlobCopies)) {
		t.Fatalf("verified_copied_blobs detail = %v, want %d", details["verified_copied_blobs"], len(fixture.BlobCopies))
	}
	families := requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families")
	requireAdminMigrationInventoryFamily(t, families, "", "organizations", len(fixture.Bootstrap.Orgs))
	requireAdminMigrationInventoryFamily(t, families, fixture.DefaultOrganization, "nodes", len(fixture.CoreObjects.Orgs[fixture.DefaultOrganization].Nodes))
}

// readAdminMigrationScaleBackupBlobManifest decodes copied-blob metadata from a
// valid test bundle before a mutation intentionally damages it.
func readAdminMigrationScaleBackupBlobManifest(t *testing.T, root string) adminMigrationBackupBlobManifest {
	t.Helper()
	var manifest adminMigrationBackupBlobManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationBackupBlobsPath), &manifest); err != nil {
		t.Fatalf("blob manifest json.Unmarshal error = %v", err)
	}
	if len(manifest.Copied) == 0 {
		t.Fatal("scale backup blob manifest has no copied blobs")
	}
	return manifest
}

// writeAdminMigrationScaleBackupBlobManifest rewrites blobs/manifest.json and
// refreshes its parent manifest hash after tests alter copied blob metadata.
func writeAdminMigrationScaleBackupBlobManifest(t *testing.T, root string, manifest adminMigrationBackupBlobManifest) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent(blob manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, adminMigrationBackupBlobsPath), append(data, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(blob manifest) error = %v", err)
	}
	updateAdminMigrationScaleBackupPayloadHash(t, root, adminMigrationBackupBlobsPath)
}

// removeAdminMigrationScaleBackupManifestPayload deletes one manifest payload
// record without deleting bytes, modeling a malicious or accidental omission.
func removeAdminMigrationScaleBackupManifestPayload(t *testing.T, root, targetPath string) {
	t.Helper()
	manifest := mustReadAdminMigrationBackupManifest(t, root)
	filtered := make([]adminMigrationBackupPayload, 0, len(manifest.Payloads))
	for _, payload := range manifest.Payloads {
		if payload.Path == targetPath {
			continue
		}
		filtered = append(filtered, payload)
	}
	manifest.Payloads = filtered
	writeAdminMigrationScaleBackupManifest(t, root, manifest)
}

// updateAdminMigrationScaleBackupPayloadHash refreshes manifest integrity
// metadata for one payload after a test intentionally rewrites that file.
func updateAdminMigrationScaleBackupPayloadHash(t *testing.T, root, targetPath string) {
	t.Helper()
	manifest := mustReadAdminMigrationBackupManifest(t, root)
	for i := range manifest.Payloads {
		if manifest.Payloads[i].Path != targetPath {
			continue
		}
		data := readAdminMigrationTestFile(t, root, targetPath)
		sum := sha256.Sum256(data)
		manifest.Payloads[i].SHA256 = hex.EncodeToString(sum[:])
		manifest.Payloads[i].Bytes = int64(len(data))
		writeAdminMigrationScaleBackupManifest(t, root, manifest)
		return
	}
	t.Fatalf("manifest payload %s not found", targetPath)
}

// writeAdminMigrationScaleBackupManifest stores a modified manifest without
// changing payload files, matching how inspect sees on-disk backup tampering.
func writeAdminMigrationScaleBackupManifest(t *testing.T, root string, manifest adminMigrationBackupManifest) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent(manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, adminMigrationBackupManifestPath), append(data, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(manifest) error = %v", err)
	}
}

// writeAdminMigrationScaleSourceBundle renders the generated scale fixture into
// the normalized source-import contract so import/sync tests exercise real
// manifest hashes, sidecar artifacts, and copied checksum blob files.
func writeAdminMigrationScaleSourceBundle(t *testing.T, fixture adminMigrationScaleFixture, includeSidecars bool) string {
	t.Helper()
	files := map[string][]byte{}
	payloadValues := adminMigrationScaleSourcePayloadValues(t, fixture)
	blobChecksums := map[string]struct{}{}
	for _, blobCopy := range fixture.BlobCopies {
		blobChecksums[blobCopy.Checksum] = struct{}{}
		files[filepath.ToSlash(filepath.Join("blobs", "checksums", blobCopy.Checksum))] = append([]byte(nil), blobCopy.Body...)
	}
	searchCount := 0
	unsupportedCounts := map[string]int{}
	if includeSidecars {
		searchCount = 1
		unsupportedCounts["oc_id"] = 1
		files["derived/opensearch/chef/node/scale-fixture.json"] = []byte(`{"derived":true}` + "\n")
		files["unsupported/oc_id/actors.json"] = []byte(`{"unsupported":true}` + "\n")
	}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(payloadValues, files)
	if err != nil {
		t.Fatalf("adminMigrationMaterializeSourcePayloadFiles(scale) error = %v", err)
	}
	artifacts := adminMigrationSourceArtifactsFromSideChannels(blobChecksums, searchCount, unsupportedCounts)
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    "production_scale_fixture",
		Payloads:      payloads,
		Artifacts:     artifacts,
		Notes:         []string{"Generated by OpenCook production-scale migration validation tests."},
	}
	root := filepath.Join(t.TempDir(), "normalized-scale-source")
	if err := adminMigrationWriteNormalizedSourceBundle(root, adminMigrationSourceNormalizeBundle{Manifest: manifest, Files: files}, true); err != nil {
		t.Fatalf("adminMigrationWriteNormalizedSourceBundle(scale) error = %v", err)
	}
	if _, err := adminMigrationReadSourceImportBundle(root); err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(scale output) error = %v", err)
	}
	return root
}

// adminMigrationScaleSourcePayloadValues converts fixture state into source
// rows instead of backup rows, preserving relation-table families that source
// import and sync need for retry and drift accounting.
func adminMigrationScaleSourcePayloadValues(t *testing.T, fixture adminMigrationScaleFixture) map[adminMigrationSourcePayloadKey][]json.RawMessage {
	t.Helper()
	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	add := func(key adminMigrationSourcePayloadKey, value any) {
		payloadValues[key] = append(payloadValues[key], adminMigrationScaleSourceRaw(t, value))
	}

	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.Users) {
		add(adminMigrationSourcePayloadKey{Family: "users"}, fixture.Bootstrap.Users[username])
	}
	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserACLs) {
		add(adminMigrationSourcePayloadKey{Family: "user_acls"}, adminMigrationScaleSourceACLRecord("user:"+username, fixture.Bootstrap.UserACLs[username]))
	}
	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserKeys) {
		for _, keyName := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserKeys[username]) {
			add(adminMigrationSourcePayloadKey{Family: "user_keys"}, adminMigrationScaleSourceKeyRecord("username", username, fixture.Bootstrap.UserKeys[username][keyName]))
		}
	}
	add(adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, map[string]string{"type": "user", "actor": adminMigrationScaleFixtureSuperuser})

	for _, orgName := range adminMigrationSortedMapKeys(fixture.Bootstrap.Orgs) {
		bootstrapOrg := fixture.Bootstrap.Orgs[orgName]
		coreOrg := fixture.CoreObjects.Orgs[orgName]
		cookbookOrg := fixture.Cookbooks.Orgs[orgName]
		add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "organizations"}, bootstrapOrg.Organization)
		for _, clientName := range adminMigrationSortedMapKeys(bootstrapOrg.Clients) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "clients"}, bootstrapOrg.Clients[clientName])
		}
		for _, clientName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys) {
			for _, keyName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys[clientName]) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "client_keys"}, adminMigrationScaleSourceKeyRecord("client", clientName, bootstrapOrg.ClientKeys[clientName][keyName]))
			}
		}
		sourceGroups := adminMigrationScaleSourceGroups(bootstrapOrg)
		for _, groupName := range adminMigrationSortedMapKeys(sourceGroups) {
			group := sourceGroups[groupName]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "groups"}, group)
			for _, member := range adminMigrationSourceSyncGroupMembershipRecords(group) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "group_memberships"}, member)
			}
		}
		for _, containerName := range adminMigrationSortedMapKeys(bootstrapOrg.Containers) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "containers"}, bootstrapOrg.Containers[containerName])
		}
		for _, aclKey := range adminMigrationSortedMapKeys(bootstrapOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, adminMigrationScaleSourceACLRecord(adminMigrationScaleSourceACLResource(orgName, aclKey), bootstrapOrg.ACLs[aclKey]))
		}
		for _, envName := range adminMigrationSortedMapKeys(coreOrg.Environments) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "environments"}, coreOrg.Environments[envName])
		}
		for _, nodeName := range adminMigrationSortedMapKeys(coreOrg.Nodes) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "nodes"}, coreOrg.Nodes[nodeName])
		}
		for _, roleName := range adminMigrationSortedMapKeys(coreOrg.Roles) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "roles"}, coreOrg.Roles[roleName])
		}
		for _, bagName := range adminMigrationSortedMapKeys(coreOrg.DataBags) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bags"}, coreOrg.DataBags[bagName])
		}
		for _, bagName := range adminMigrationSortedMapKeys(coreOrg.DataBagItems) {
			for _, itemID := range adminMigrationSortedMapKeys(coreOrg.DataBagItems[bagName]) {
				item := coreOrg.DataBagItems[bagName][itemID]
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bag_items"}, map[string]any{"bag": bagName, "id": item.ID, "payload": item.RawData})
			}
		}
		for _, policyName := range adminMigrationSortedMapKeys(coreOrg.Policies) {
			for _, revisionID := range adminMigrationSortedMapKeys(coreOrg.Policies[policyName]) {
				revision := coreOrg.Policies[policyName][revisionID]
				payload := adminMigrationSourceImportCloneMap(revision.Payload)
				payload["name"] = revision.Name
				payload["revision_id"] = revision.RevisionID
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_revisions"}, payload)
			}
		}
		for _, groupName := range adminMigrationSortedMapKeys(coreOrg.PolicyGroups) {
			group := coreOrg.PolicyGroups[groupName]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_groups"}, map[string]any{"name": group.Name, "policies": group.Policies})
			for _, policyName := range adminMigrationSortedMapKeys(group.Policies) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_assignments"}, map[string]string{"group": groupName, "policy": policyName, "revision_id": group.Policies[policyName]})
			}
		}
		checksumRefs := map[string]map[string]string{}
		for _, sandboxID := range adminMigrationSortedMapKeys(coreOrg.Sandboxes) {
			sandbox := coreOrg.Sandboxes[sandboxID]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "sandboxes"}, map[string]any{"sandbox_id": sandbox.ID, "checksums": sandbox.Checksums, "completed": true})
			for _, checksum := range sandbox.Checksums {
				checksumRefs["sandboxes/"+checksum] = map[string]string{"family": "sandboxes", "checksum": checksum}
			}
		}
		for _, aclKey := range adminMigrationSortedMapKeys(coreOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, adminMigrationScaleSourceACLRecord(adminMigrationScaleSourceACLResource(orgName, aclKey), coreOrg.ACLs[aclKey]))
		}
		for _, version := range cookbookOrg.Versions {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_versions"}, version)
			for _, checksum := range adminMigrationCookbookFileChecksums(version.AllFiles) {
				checksumRefs["cookbook_versions/"+checksum] = map[string]string{"family": "cookbook_versions", "checksum": checksum}
			}
		}
		for _, artifact := range cookbookOrg.Artifacts {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_artifacts"}, artifact)
			for _, checksum := range adminMigrationCookbookFileChecksums(artifact.AllFiles) {
				checksumRefs["cookbook_artifacts/"+checksum] = map[string]string{"family": "cookbook_artifacts", "checksum": checksum}
			}
		}
		for _, refID := range adminMigrationSortedMapKeys(checksumRefs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}, checksumRefs[refID])
		}
	}
	return payloadValues
}

// adminMigrationScaleSourceRaw marshals source rows through JSON so tests catch
// the same object-shape requirements as files produced by the normalize CLI.
func adminMigrationScaleSourceRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal(scale source %T) error = %v", value, err)
	}
	return data
}

// adminMigrationScaleSourceKeyRecord exposes key records with the owner field
// names expected by source import's user_key and client_key payload families.
func adminMigrationScaleSourceKeyRecord(ownerField, owner string, record bootstrap.KeyRecord) map[string]string {
	return map[string]string{
		ownerField:        owner,
		"key_name":        record.Name,
		"public_key":      record.PublicKeyPEM,
		"expiration_date": record.ExpirationDate,
	}
}

// adminMigrationScaleSourceGroups fills nested-group placeholders that are
// legal in persisted fixture state but must be explicit rows in a normalized
// source import graph.
func adminMigrationScaleSourceGroups(org bootstrap.BootstrapCoreOrganizationState) map[string]bootstrap.Group {
	groups := make(map[string]bootstrap.Group, len(org.Groups))
	for name, group := range org.Groups {
		groups[name] = group
	}
	for _, group := range org.Groups {
		for _, nested := range group.Groups {
			if _, ok := groups[nested]; ok {
				continue
			}
			groups[nested] = bootstrap.Group{Name: nested, GroupName: nested, Organization: org.Organization.Name}
		}
	}
	return groups
}

// adminMigrationScaleSourceACLRecord stores ACL documents with an explicit
// type:name resource because the source family carries bootstrap and object ACLs
// in one org-scoped payload file.
func adminMigrationScaleSourceACLRecord(resource string, acl authz.ACL) map[string]any {
	return map[string]any{
		"resource": resource,
		"create":   adminMigrationSourceSyncCanonicalPermission(acl.Create),
		"read":     adminMigrationSourceSyncCanonicalPermission(acl.Read),
		"update":   adminMigrationSourceSyncCanonicalPermission(acl.Update),
		"delete":   adminMigrationSourceSyncCanonicalPermission(acl.Delete),
		"grant":    adminMigrationSourceSyncCanonicalPermission(acl.Grant),
	}
}

// adminMigrationScaleSourceACLResource rewrites OpenCook's internal ACL keys
// back into source-import resource identifiers.
func adminMigrationScaleSourceACLResource(orgName, aclKey string) string {
	if aclKey == adminMigrationOrganizationACLKey() {
		return "organization:" + orgName
	}
	return aclKey
}

// appendAdminMigrationScaleSourcePayload mutates a normalized scale bundle while
// refreshing manifest hash/count metadata, allowing semantic failure tests to
// pass integrity checks before source normalization rejects the content.
func appendAdminMigrationScaleSourcePayload(t *testing.T, root string, key adminMigrationSourcePayloadKey, value any) {
	t.Helper()
	payloadPath := adminMigrationSourcePayloadPath(key.Organization, key.Family)
	values, err := adminMigrationCanonicalSourcePayloadValues(readAdminMigrationTestFile(t, root, payloadPath))
	if err != nil {
		t.Fatalf("adminMigrationCanonicalSourcePayloadValues(%s) error = %v", payloadPath, err)
	}
	values = append(values, adminMigrationScaleSourceRaw(t, value))
	writeAdminMigrationScaleSourcePayload(t, root, payloadPath, values)
}

// writeAdminMigrationScaleSourcePayload rewrites one payload and its manifest
// entry so tests can isolate semantic validation from hash validation.
func writeAdminMigrationScaleSourcePayload(t *testing.T, root, payloadPath string, values []json.RawMessage) {
	t.Helper()
	data, err := adminMigrationMarshalSourcePayloadValues(values)
	if err != nil {
		t.Fatalf("adminMigrationMarshalSourcePayloadValues(%s) error = %v", payloadPath, err)
	}
	if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(payloadPath)), data, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", payloadPath, err)
	}
	manifest := readAdminMigrationScaleSourceManifest(t, root)
	for i := range manifest.Payloads {
		if manifest.Payloads[i].Path == payloadPath {
			manifest.Payloads[i].Count = len(values)
			manifest.Payloads[i].SHA256 = adminMigrationSHA256Hex(data)
			writeAdminMigrationScaleSourceManifest(t, root, manifest)
			return
		}
	}
	t.Fatalf("source manifest payload %s not found", payloadPath)
}

// removeAdminMigrationScaleSourceCopiedBlob removes local copied bytes while
// keeping metadata references intact, modeling a resumable provider-verification
// failure without creating an artifact-count mismatch.
func removeAdminMigrationScaleSourceCopiedBlob(t *testing.T, root, checksum string) {
	t.Helper()
	blobPath := filepath.ToSlash(filepath.Join("blobs", "checksums", checksum))
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(blobPath))); err != nil {
		t.Fatalf("Remove(%s) error = %v", blobPath, err)
	}
	manifest := readAdminMigrationScaleSourceManifest(t, root)
	for i := range manifest.Artifacts {
		if manifest.Artifacts[i].Family == "bookshelf" && manifest.Artifacts[i].Count > 0 {
			manifest.Artifacts[i].Count--
			writeAdminMigrationScaleSourceManifest(t, root, manifest)
			return
		}
	}
	t.Fatalf("source manifest bookshelf artifact not found")
}

// readAdminMigrationScaleSourceManifest decodes the normalized source manifest
// for tests that intentionally mutate payload or artifact metadata.
func readAdminMigrationScaleSourceManifest(t *testing.T, root string) adminMigrationSourceManifest {
	t.Helper()
	var manifest adminMigrationSourceManifest
	if err := json.Unmarshal(readAdminMigrationTestFile(t, root, adminMigrationSourceManifestPath), &manifest); err != nil {
		t.Fatalf("source manifest json.Unmarshal error = %v", err)
	}
	return manifest
}

// writeAdminMigrationScaleSourceManifest persists a modified normalized source
// manifest after a test rewrites source payloads or sidecar artifact counts.
func writeAdminMigrationScaleSourceManifest(t *testing.T, root string, manifest adminMigrationSourceManifest) {
	t.Helper()
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent(source manifest) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, adminMigrationSourceManifestPath), append(data, '\n'), 0o644); err != nil {
		t.Fatalf("WriteFile(source manifest) error = %v", err)
	}
}
