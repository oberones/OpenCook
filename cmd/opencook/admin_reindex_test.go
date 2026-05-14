package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestAdminReindexCommandScopesAndModes(t *testing.T) {
	for _, tc := range []struct {
		name              string
		args              []string
		wantDeleteQueries []adminReindexDeleteQuery
		wantDeletedIDs    []string
		wantRefs          []search.DocumentRef
		wantScanned       int
		wantDeleted       int
		wantUpserted      int
		wantMissing       int
		wantTiming        bool
	}{
		{
			name:              "full org complete",
			args:              []string{"admin", "reindex", "--org", "ponyville", "--complete", "--with-timing"},
			wantDeleteQueries: []adminReindexDeleteQuery{{org: "ponyville"}},
			wantRefs: []search.DocumentRef{
				{Organization: "ponyville", Index: "client", Name: "web01"},
				{Organization: "ponyville", Index: "environment", Name: "_default"},
				{Organization: "ponyville", Index: "node", Name: "twilight"},
				{Organization: "ponyville", Index: "ponies", Name: "alice"},
			},
			wantScanned:  4,
			wantDeleted:  4,
			wantUpserted: 4,
			wantTiming:   true,
		},
		{
			name:              "all orgs complete",
			args:              []string{"admin", "reindex", "--all-orgs"},
			wantDeleteQueries: []adminReindexDeleteQuery{{}},
			wantRefs: []search.DocumentRef{
				{Organization: "canterlot", Index: "environment", Name: "_default"},
				{Organization: "ponyville", Index: "node", Name: "twilight"},
			},
			wantScanned:  5,
			wantDeleted:  5,
			wantUpserted: 5,
		},
		{
			name:         "built-in index reindex",
			args:         []string{"admin", "reindex", "--org", "ponyville", "--index", "node", "--no-drop"},
			wantRefs:     []search.DocumentRef{{Organization: "ponyville", Index: "node", Name: "twilight"}},
			wantScanned:  1,
			wantUpserted: 1,
		},
		{
			name:         "data bag index reindex",
			args:         []string{"admin", "reindex", "--org", "ponyville", "--index", "ponies", "--no-drop"},
			wantRefs:     []search.DocumentRef{{Organization: "ponyville", Index: "ponies", Name: "alice"}},
			wantScanned:  1,
			wantUpserted: 1,
		},
		{
			name:           "named item complete",
			args:           []string{"admin", "reindex", "--org", "ponyville", "--index", "node", "--name", "twilight", "--name", "missing", "--complete"},
			wantDeletedIDs: []string{"ponyville/node/missing", "ponyville/node/twilight"},
			wantRefs:       []search.DocumentRef{{Organization: "ponyville", Index: "node", Name: "twilight"}},
			wantScanned:    1,
			wantDeleted:    2,
			wantUpserted:   1,
			wantMissing:    1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := adminReindexTestStore()
			target := &fakeAdminReindexTarget{}
			cmd, stdout, stderr := newAdminReindexTestCommand(t, store, target)

			if code := cmd.Run(context.Background(), tc.args); code != exitOK {
				t.Fatalf("Run(%v) exit = %d, want %d; stderr = %s", tc.args, code, exitOK, stderr.String())
			}
			if got := target.deleteQueries; !sameAdminReindexDeleteQueries(got, tc.wantDeleteQueries) {
				t.Fatalf("delete queries = %#v, want %#v", got, tc.wantDeleteQueries)
			}
			if got := target.deletedIDs; !sameAdminStrings(got, tc.wantDeletedIDs) {
				t.Fatalf("deleted IDs = %v, want %v", got, tc.wantDeletedIDs)
			}
			for _, ref := range tc.wantRefs {
				if !hasAdminReindexRef(target.upsertedRefs(), ref) {
					t.Fatalf("upserted refs = %v, missing %v", target.upsertedRefs(), ref)
				}
			}
			out := decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "scanned", tc.wantScanned)
			assertAdminReindexCount(t, out, "deleted", tc.wantDeleted)
			assertAdminReindexCount(t, out, "upserted", tc.wantUpserted)
			assertAdminReindexCount(t, out, "missing", tc.wantMissing)
			if _, ok := out["duration_ms"]; ok != tc.wantTiming {
				t.Fatalf("duration_ms present = %t, want %t in output %v", ok, tc.wantTiming, out)
			}
			requireAdminOutputWarningContains(t, out, "active maintenance mode confirmed")
			if len(tc.wantDeleteQueries) > 0 || len(tc.wantDeletedIDs) > 0 {
				requireAdminOutputWarningContains(t, out, "drop-and-reindex mutates derived OpenSearch documents")
			}
			if strings.Contains(stderr.String(), "raw provider") {
				t.Fatalf("stderr leaked provider internals: %q", stderr.String())
			}
		})
	}
}

func TestAdminOperationalSearchCommandsUseProviderCapabilityModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode adminCapabilityProviderMode
	}{
		{name: "direct delete by query", mode: adminCapabilityDirectDeleteByQuery},
		{name: "fallback delete", mode: adminCapabilityFallbackDelete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := newAdminCapabilityOpenSearchTransport(t, tc.mode)
			cmd, stdout, stderr := newAdminCapabilityOpenSearchCommand(t, adminReindexTestStore(), target)

			if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--complete"}); code != exitOK {
				t.Fatalf("Run(reindex %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out := decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "scanned", 4)
			assertAdminReindexCount(t, out, "upserted", 4)
			for _, id := range adminSupportedProviderIDs() {
				if !target.hasDocument(id) {
					t.Fatalf("%s provider docs missing %q after reindex: %v", tc.name, id, target.documentIDs())
				}
			}
			switch tc.mode {
			case adminCapabilityDirectDeleteByQuery:
				if target.directDeleteByQueries != 1 || target.fallbackDeleteSearches != 0 {
					t.Fatalf("direct mode delete paths = direct:%d fallback:%d, want direct only", target.directDeleteByQueries, target.fallbackDeleteSearches)
				}
			case adminCapabilityFallbackDelete:
				if target.directDeleteByQueries != 0 || target.fallbackDeleteSearches == 0 {
					t.Fatalf("fallback mode delete paths = direct:%d fallback:%d, want fallback search/delete path", target.directDeleteByQueries, target.fallbackDeleteSearches)
				}
			}

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", "ponyville"}); code != exitOK {
				t.Fatalf("Run(search check clean %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "clean", 1)

			target.forceDocument("ponyville/node/stale")
			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", "ponyville"}); code != exitPartial {
				t.Fatalf("Run(search check stale %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitPartial, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "stale", 1)

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", "ponyville", "--yes"}); code != exitOK {
				t.Fatalf("Run(search repair %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "deleted", 1)
			if target.hasDocument("ponyville/node/stale") {
				t.Fatalf("%s provider still has stale node after repair: %v", tc.name, target.documentIDs())
			}

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", "ponyville"}); code != exitOK {
				t.Fatalf("Run(search check after repair %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "clean", 1)
		})
	}
}

// TestAdminScaleOpenSearchReindexCheckAndRepair uses the production-scale
// migration fixture to prove OpenSearch rebuild, reindex, check, and repair
// stay correct against multi-org PostgreSQL-shaped state.
func TestAdminScaleOpenSearchReindexCheckAndRepair(t *testing.T) {
	fixture := requireAdminMigrationScaleFixture(t, adminMigrationScaleProfileSmall)
	scaleState := adminScaleSearchBootstrapService(t, fixture)
	allDocs := adminScaleSearchDocuments(t, scaleState, search.ReindexPlan{AllOrganizations: true})
	orgDocs := adminScaleSearchDocuments(t, scaleState, search.ReindexPlan{Organization: fixture.DefaultOrganization})
	nodeDocs := adminScaleSearchDocuments(t, scaleState, search.ReindexPlan{Organization: fixture.DefaultOrganization, Index: "node"})
	missingDocID := adminScaleFirstDocumentID(t, nodeDocs)
	staleSupportedID := fixture.DefaultOrganization + "/node/stale-scale-node"
	staleUnsupportedIDs := adminUnsupportedProviderIDs()
	staleCount := 1 + len(staleUnsupportedIDs)

	for _, tc := range []struct {
		name string
		mode adminCapabilityProviderMode
	}{
		{name: "direct delete by query", mode: adminCapabilityDirectDeleteByQuery},
		{name: "fallback delete with search-after", mode: adminCapabilityFallbackDelete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := adminScaleSearchStore(fixture)
			target := newAdminCapabilityOpenSearchTransport(t, tc.mode)
			if tc.mode == adminCapabilityFallbackDelete {
				adminSeedScaleFallbackPaginationDocuments(t, target, fixture.DefaultOrganization, 1105)
			}
			client, err := adminCapabilityOpenSearchClient(t, "http://opensearch.test", target)
			if err != nil {
				t.Fatalf("adminCapabilityOpenSearchClient() error = %v", err)
			}
			if err := search.RebuildOpenSearchIndex(context.Background(), client, scaleState); err != nil {
				t.Fatalf("RebuildOpenSearchIndex(scale) error = %v", err)
			}
			if len(target.docs) != len(allDocs) {
				t.Fatalf("startup rebuild docs = %d, want %d", len(target.docs), len(allDocs))
			}
			adminRequireNoUnsupportedOpenSearchDocuments(t, target.documentIDs())

			if tc.mode == adminCapabilityFallbackDelete && target.fallbackDeleteSearches < 2 {
				t.Fatalf("fallback startup rebuild searches = %d, want paginated delete search-after path", target.fallbackDeleteSearches)
			}
			target.resetCapabilityCounters()
			cmd, stdout, stderr := newAdminCapabilityOpenSearchCommand(t, store, target)

			if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", fixture.DefaultOrganization, "--complete", "--with-timing", "--json"}); code != exitOK {
				t.Fatalf("Run(scale complete reindex %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out := decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "scanned", len(orgDocs))
			assertAdminReindexCount(t, out, "deleted", len(orgDocs))
			assertAdminReindexCount(t, out, "upserted", len(orgDocs))
			if _, ok := out["duration_ms"]; !ok {
				t.Fatalf("scale complete reindex output missing duration_ms: %v", out)
			}
			requireAdminOutputWarningContains(t, out, "active maintenance mode confirmed")
			requireAdminOutputWarningContains(t, out, "drop-and-reindex mutates derived OpenSearch documents")
			adminRequireNoUnsupportedOpenSearchDocuments(t, target.documentIDs())
			switch tc.mode {
			case adminCapabilityDirectDeleteByQuery:
				if target.directDeleteByQueries != 1 || target.fallbackDeleteSearches != 0 {
					t.Fatalf("direct scale delete paths = direct:%d fallback:%d, want direct only", target.directDeleteByQueries, target.fallbackDeleteSearches)
				}
			case adminCapabilityFallbackDelete:
				if target.directDeleteByQueries != 0 || target.fallbackDeleteSearches == 0 {
					t.Fatalf("fallback scale delete paths = direct:%d fallback:%d, want fallback search/delete path", target.directDeleteByQueries, target.fallbackDeleteSearches)
				}
			}

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", fixture.DefaultOrganization, "--index", "node", "--no-drop", "--json"}); code != exitOK {
				t.Fatalf("Run(scale scoped reindex %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "scanned", len(nodeDocs))
			assertAdminReindexCount(t, out, "upserted", len(nodeDocs))
			requireAdminOutputWarningContains(t, out, "active maintenance mode confirmed")

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", fixture.DefaultOrganization, "--with-timing", "--json"}); code != exitOK {
				t.Fatalf("Run(scale clean search check %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "expected", len(orgDocs))
			assertAdminReindexCount(t, out, "observed", len(orgDocs))
			assertAdminReindexCount(t, out, "clean", 1)
			if _, ok := out["duration_ms"]; !ok {
				t.Fatalf("scale search check output missing duration_ms: %v", out)
			}

			delete(target.docs, missingDocID)
			target.forceDocument(staleSupportedID)
			for _, id := range staleUnsupportedIDs {
				target.forceDocument(id)
			}

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", fixture.DefaultOrganization, "--json"}); code != exitPartial {
				t.Fatalf("Run(scale drift search check %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitPartial, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "missing", 1)
			assertAdminReindexCount(t, out, "stale", staleCount)
			assertAdminReindexCount(t, out, "unsupported", len(adminUnsupportedProviderScopes()))
			requireAdminOutputStrings(t, out, "missing_documents", []string{missingDocID})
			requireNoAdminObjectCountsForIndexes(t, out, adminUnsupportedSearchIndexes()...)

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", fixture.DefaultOrganization, "--dry-run", "--json"}); code != exitPartial {
				t.Fatalf("Run(scale search repair dry-run %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitPartial, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "skipped", 1+staleCount)
			if _, ok := target.docs[missingDocID]; ok {
				t.Fatalf("dry-run restored missing doc %s", missingDocID)
			}

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", fixture.DefaultOrganization, "--yes", "--with-timing", "--json"}); code != exitOK {
				t.Fatalf("Run(scale search repair %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "upserted", 1)
			assertAdminReindexCount(t, out, "deleted", staleCount)
			assertAdminReindexCount(t, out, "unsupported", len(adminUnsupportedProviderScopes()))
			requireAdminOutputWarningContains(t, out, "active maintenance mode confirmed")
			requireAdminOutputWarningContains(t, out, "search repair mutates derived OpenSearch documents")
			if _, ok := target.docs[missingDocID]; !ok {
				t.Fatalf("repair did not restore missing doc %s", missingDocID)
			}
			if target.hasDocument(staleSupportedID) {
				t.Fatalf("repair left stale supported doc %s", staleSupportedID)
			}
			for _, id := range staleUnsupportedIDs {
				if target.hasDocument(id) {
					t.Fatalf("repair left unsupported stale doc %s", id)
				}
			}
			adminRequireNoUnsupportedOpenSearchDocuments(t, target.documentIDs())

			stdout.Reset()
			stderr.Reset()
			if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", fixture.DefaultOrganization, "--json"}); code != exitOK {
				t.Fatalf("Run(scale search check after repair %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitOK, stdout.String(), stderr.String())
			}
			out = decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "clean", 1)
		})
	}
}

func TestAdminReindexDryRunDoesNotRequireOpenSearchOrMutateProvider(t *testing.T) {
	store := adminReindexTestStore()
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://reindex-test"}, nil
	}
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://reindex-test" {
			t.Fatalf("dsn = %q, want postgres://reindex-test", dsn)
		}
		return store, nil, nil
	}
	cmd.newReindexTarget = func(string) (search.ReindexTarget, error) {
		t.Fatal("dry-run constructed OpenSearch target")
		return nil, nil
	}

	if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--index", "node", "--dry-run"}); code != exitOK {
		t.Fatalf("Run(dry-run) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	out := decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "scanned", 1)
	assertAdminReindexCount(t, out, "skipped", 2)
}

func TestAdminReindexActiveRunRequiresMaintenanceMode(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	maintenanceStore := maintenance.NewMemoryStore()
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			PostgresDSN:   "postgres://reindex-test",
			OpenSearchURL: "http://opensearch.test",
		}, nil
	}
	setTestMaintenanceStore(cmd, maintenanceStore, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		t.Fatal("reindex opened offline store before maintenance was active")
		return nil, nil, nil
	}
	cmd.newReindexTarget = func(string) (search.ReindexTarget, error) {
		t.Fatal("reindex opened OpenSearch target before maintenance was active")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--no-drop"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(reindex without maintenance) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "active maintenance mode is required") {
		t.Fatalf("stderr = %q, want maintenance requirement", stderr.String())
	}
	out := decodeAdminReindexOutput(t, stdout.String())
	requireAdminOutputErrorCode(t, out, "maintenance_required")
	assertAdminReindexCount(t, out, "failed", 1)
	check, err := maintenanceStore.Check(context.Background())
	if err != nil {
		t.Fatalf("maintenance Check() error = %v", err)
	}
	if check.Active {
		t.Fatalf("maintenance state active = true after rejected reindex, want no temporary gate left behind")
	}
}

func TestAdminReindexFailureLeavesActiveMaintenanceState(t *testing.T) {
	maintenanceStore := activeAdminWorkflowMaintenanceStore(t)
	target := &fakeAdminReindexTarget{pingErr: fmt.Errorf("%w: raw provider body from cluster", search.ErrUnavailable)}
	cmd, stdout, stderr := newAdminReindexTestCommand(t, adminReindexTestStore(), target)
	setTestMaintenanceStore(cmd, maintenanceStore, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})

	code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--no-drop"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(reindex provider failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	check, err := maintenanceStore.Check(context.Background())
	if err != nil {
		t.Fatalf("maintenance Check() error = %v", err)
	}
	if !check.Active {
		t.Fatalf("maintenance state active = false after failed reindex, want caller-managed window preserved")
	}
}

// TestAdminReindexEncryptedDataBagIndexRebuildsOpaqueDocuments proves the CLI
// loads PostgreSQL-backed encrypted-looking data bag state and rebuilds the
// provider document without requiring or exposing a data bag secret.
func TestAdminReindexEncryptedDataBagIndexRebuildsOpaqueDocuments(t *testing.T) {
	bagName := testfixtures.EncryptedDataBagName()
	target := &fakeAdminReindexTarget{}
	cmd, stdout, stderr := newAdminReindexTestCommand(t, adminEncryptedDataBagSearchStore(), target)

	if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--index", bagName, "--no-drop"}); code != exitOK {
		t.Fatalf("Run(encrypted reindex) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if len(target.deleteQueries) != 0 || len(target.deletedIDs) != 0 {
		t.Fatalf("encrypted reindex deletes = %#v/%#v, want none for --no-drop", target.deleteQueries, target.deletedIDs)
	}
	doc := requireAdminEncryptedDataBagDocument(t, target.upsertedDocuments())
	requireAdminEncryptedDataBagSearchFields(t, doc)
	out := decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "scanned", 1)
	assertAdminReindexCount(t, out, "upserted", 1)
	assertAdminEncryptedOperationalOutputDoesNotLeakSecret(t, stdout.String(), stderr.String())
}

// TestAdminReindexEncryptedDataBagProviderFailureIsRedacted keeps the
// provider-unavailable contract pinned for encrypted data bag scoped reindexing.
func TestAdminReindexEncryptedDataBagProviderFailureIsRedacted(t *testing.T) {
	bagName := testfixtures.EncryptedDataBagName()
	target := &fakeAdminReindexTarget{pingErr: fmt.Errorf("%w: raw provider body from internal cluster correct horse battery staple", search.ErrUnavailable)}
	cmd, stdout, stderr := newAdminReindexTestCommand(t, adminEncryptedDataBagSearchStore(), target)

	code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--index", bagName, "--no-drop"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(encrypted provider failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), search.ErrUnavailable.Error()) {
		t.Fatalf("stderr = %q, want redacted unavailable message", stderr.String())
	}
	assertAdminEncryptedOperationalOutputDoesNotLeakSecret(t, stdout.String(), stderr.String())
	out := decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "failed", 1)
}

func TestAdminReindexSkipsUnsupportedObjectFamilies(t *testing.T) {
	target := &fakeAdminReindexTarget{}
	cmd, stdout, stderr := newAdminReindexTestCommand(t, adminUnsupportedSearchStore(), target)

	if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--no-drop"}); code != exitOK {
		t.Fatalf("Run(unsupported family reindex) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	requireAdminReindexRefs(t, target.upsertedRefs(), adminSupportedSearchRefs())
	requireNoAdminReindexRefsForIndexes(t, target.upsertedRefs(), adminUnsupportedSearchIndexes()...)
	out := decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "scanned", 4)
	assertAdminReindexCount(t, out, "upserted", 4)
	assertAdminReindexCount(t, out, "missing", 0)
}

func TestAdminReindexRejectsUnsupportedIndexes(t *testing.T) {
	for _, index := range adminUnsupportedSearchIndexes() {
		t.Run(index, func(t *testing.T) {
			target := &fakeAdminReindexTarget{}
			cmd, stdout, stderr := newAdminReindexTestCommand(t, adminUnsupportedSearchStore(), target)

			if code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville", "--index", index, "--no-drop"}); code != exitNotFound {
				t.Fatalf("Run(reindex unsupported %s) exit = %d, want %d; stdout = %s stderr = %s", index, code, exitNotFound, stdout.String(), stderr.String())
			}
			if target.pings != 0 || target.ensureCalls != 0 || len(target.bulkBatches) != 0 || len(target.deletedIDs) != 0 || len(target.deleteQueries) != 0 {
				t.Fatalf("unsupported reindex mutated target: pings=%d ensure=%d bulks=%d deleted=%v queries=%v", target.pings, target.ensureCalls, len(target.bulkBatches), target.deletedIDs, target.deleteQueries)
			}
			if !strings.Contains(stderr.String(), search.ErrIndexNotFound.Error()) {
				t.Fatalf("stderr = %q, want ErrIndexNotFound", stderr.String())
			}
			out := decodeAdminReindexOutput(t, stdout.String())
			assertAdminReindexCount(t, out, "failed", 1)
		})
	}
}

func TestAdminReindexMissingOrgIndexAndProviderFailuresAreStable(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		target       *fakeAdminReindexTarget
		wantCode     int
		wantStderr   string
		forbidStderr string
	}{
		{
			name:       "missing organization",
			args:       []string{"admin", "reindex", "--org", "missing"},
			target:     &fakeAdminReindexTarget{},
			wantCode:   exitNotFound,
			wantStderr: search.ErrOrganizationNotFound.Error(),
		},
		{
			name:       "missing index",
			args:       []string{"admin", "reindex", "--org", "ponyville", "--index", "missing-index"},
			target:     &fakeAdminReindexTarget{},
			wantCode:   exitNotFound,
			wantStderr: search.ErrIndexNotFound.Error(),
		},
		{
			name:         "provider unavailable",
			args:         []string{"admin", "reindex", "--org", "ponyville", "--index", "node", "--no-drop"},
			target:       &fakeAdminReindexTarget{pingErr: fmt.Errorf("%w: raw provider body from cluster", search.ErrUnavailable)},
			wantCode:     exitDependencyUnavailable,
			wantStderr:   search.ErrUnavailable.Error(),
			forbidStderr: "raw provider body",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newAdminReindexTestCommand(t, adminReindexTestStore(), tc.target)

			if code := cmd.Run(context.Background(), tc.args); code != tc.wantCode {
				t.Fatalf("Run(%v) exit = %d, want %d; stdout = %s stderr = %s", tc.args, code, tc.wantCode, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.wantStderr) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.wantStderr)
			}
			if tc.forbidStderr != "" && strings.Contains(stderr.String(), tc.forbidStderr) {
				t.Fatalf("stderr leaked %q: %q", tc.forbidStderr, stderr.String())
			}
			out := decodeAdminReindexOutput(t, stdout.String())
			if ok, _ := out["ok"].(bool); ok {
				t.Fatalf("output ok = true, want false for %s", tc.name)
			}
			assertAdminReindexCount(t, out, "failed", 1)
		})
	}
}

func TestAdminReindexRequiresPostgresAndOpenSearchForActiveRuns(t *testing.T) {
	t.Run("postgres required", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			return config.Config{OpenSearchURL: "http://opensearch.test"}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(no postgres) exit = %d, want %d", code, exitDependencyUnavailable)
		}
		if !strings.Contains(stderr.String(), "PostgreSQL configuration") {
			t.Fatalf("stderr = %q, want PostgreSQL requirement", stderr.String())
		}
	})

	t.Run("opensearch required for active run", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			return config.Config{PostgresDSN: "postgres://reindex-test"}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "reindex", "--org", "ponyville"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(no opensearch) exit = %d, want %d", code, exitDependencyUnavailable)
		}
		if !strings.Contains(stderr.String(), "OpenSearch configuration") {
			t.Fatalf("stderr = %q, want OpenSearch requirement", stderr.String())
		}
	})
}

func TestAdminReindexUsageValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{name: "missing scope", args: []string{"admin", "reindex"}, want: "requires --org ORG or --all-orgs"},
		{name: "conflicting modes", args: []string{"admin", "reindex", "--org", "ponyville", "--drop", "--no-drop"}, want: "only one of --complete, --drop, or --no-drop"},
		{name: "all orgs with index", args: []string{"admin", "reindex", "--all-orgs", "--index", "node"}, want: "--index requires --org ORG"},
		{name: "name without index", args: []string{"admin", "reindex", "--org", "ponyville", "--name", "twilight"}, want: "--name requires --org ORG and --index INDEX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), tc.args)
			if code != exitUsage {
				t.Fatalf("Run(%v) exit = %d, want %d", tc.args, code, exitUsage)
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func newAdminReindexTestCommand(t *testing.T, store *fakeOfflineStore, target *fakeAdminReindexTarget) (*command, *strings.Builder, *strings.Builder) {
	t.Helper()
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	cmd := newCommand(stdout, stderr)
	cmd.load = func() (config.Config, error) {
		t.Fatal("unexpected server config load")
		return config.Config{}, nil
	}
	cmd.loadAdminConfig = func() admin.Config {
		t.Fatal("unexpected admin config load")
		return admin.Config{}
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			PostgresDSN:   "postgres://reindex-test",
			OpenSearchURL: "http://opensearch.test",
		}, nil
	}
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://reindex-test" {
			t.Fatalf("dsn = %q, want postgres://reindex-test", dsn)
		}
		return store, nil, nil
	}
	cmd.newReindexTarget = func(raw string) (search.ReindexTarget, error) {
		if raw != "http://opensearch.test" {
			t.Fatalf("opensearch URL = %q, want http://opensearch.test", raw)
		}
		if target == nil {
			return nil, errors.New("missing test target")
		}
		return target, nil
	}
	setTestMaintenanceStore(cmd, activeAdminWorkflowMaintenanceStore(t), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	return cmd, stdout, stderr
}

// activeAdminWorkflowMaintenanceStore gives mutating reindex/search command
// tests a pre-existing maintenance window, matching the production requirement.
func activeAdminWorkflowMaintenanceStore(t *testing.T) maintenance.Store {
	t.Helper()
	store := maintenance.NewMemoryStore()
	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Mode:   "repair",
		Reason: "test maintenance window",
		Actor:  "test",
	}); err != nil {
		t.Fatalf("Enable(test maintenance) error = %v", err)
	}
	return store
}

func adminReindexTestStore() *fakeOfflineStore {
	return &fakeOfflineStore{
		bootstrap: adminReindexBootstrapState(),
		objects:   adminReindexCoreObjectState(),
	}
}

// adminScaleSearchStore adapts the production-scale fixture to the offline
// PostgreSQL store shape used by admin reindex and search consistency commands.
func adminScaleSearchStore(fixture adminMigrationScaleFixture) *fakeOfflineStore {
	return &fakeOfflineStore{
		bootstrap: fixture.Bootstrap,
		objects:   fixture.CoreObjects,
	}
}

// adminScaleSearchBootstrapService mirrors command startup rehydration so
// expected-document counts use the same PostgreSQL-backed state as the CLI.
func adminScaleSearchBootstrapService(t *testing.T, fixture adminMigrationScaleFixture) *bootstrap.Service {
	t.Helper()
	return bootstrap.NewService(nil, bootstrap.Options{
		SuperuserName:             adminMigrationScaleFixtureSuperuser,
		InitialBootstrapCoreState: &fixture.Bootstrap,
		InitialCoreObjectState:    &fixture.CoreObjects,
	})
}

// adminScaleSearchDocuments filters the scale fixture through the production
// search document builder instead of duplicating expected per-family formulas.
func adminScaleSearchDocuments(t *testing.T, state *bootstrap.Service, plan search.ReindexPlan) []search.Document {
	t.Helper()
	docs, err := search.DocumentsFromBootstrapStateForPlan(state, plan)
	if err != nil {
		t.Fatalf("DocumentsFromBootstrapStateForPlan(%+v) error = %v", plan, err)
	}
	return docs
}

// adminScaleFirstDocumentID returns a deterministic document ID for drift
// injection in scale search check/repair tests.
func adminScaleFirstDocumentID(t *testing.T, docs []search.Document) string {
	t.Helper()
	if len(docs) == 0 {
		t.Fatal("scale fixture produced no searchable documents for drift injection")
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, search.OpenSearchDocumentID(doc))
	}
	sort.Strings(ids)
	return ids[0]
}

func adminReindexBootstrapState() bootstrap.BootstrapCoreState {
	state := adminOfflineTestState()
	ponyville := state.Orgs["ponyville"]
	ponyville.Clients = map[string]bootstrap.Client{
		"web01": {Name: "web01", ClientName: "web01", Organization: "ponyville"},
	}
	state.Orgs["ponyville"] = ponyville
	return state
}

func adminReindexCoreObjectState() bootstrap.CoreObjectState {
	creator := authn.Principal{Type: "user", Name: "pivotal"}
	return bootstrap.CoreObjectState{
		Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {
				Nodes: map[string]bootstrap.Node{
					"twilight": {
						Name:            "twilight",
						JSONClass:       "Chef::Node",
						ChefType:        "node",
						ChefEnvironment: "_default",
						Override:        map[string]any{},
						Normal:          map[string]any{"role": "librarian"},
						Default:         map[string]any{},
						Automatic:       map[string]any{},
						RunList:         []string{"recipe[base]"},
					},
				},
				DataBags: map[string]bootstrap.DataBag{
					"ponies": {Name: "ponies", JSONClass: "Chef::DataBag", ChefType: "data_bag"},
				},
				DataBagItems: map[string]map[string]bootstrap.DataBagItem{
					"ponies": {
						"alice": {ID: "alice", RawData: map[string]any{"id": "alice", "role": "operator"}},
					},
				},
				ACLs: map[string]authz.ACL{
					"node:twilight":        {Read: authz.Permission{Actors: []string{creator.Name}}},
					"data:ponies":          {Read: authz.Permission{Actors: []string{creator.Name}}},
					"environment:_default": {Read: authz.Permission{Actors: []string{creator.Name}}},
				},
			},
		},
	}
}

// adminEncryptedDataBagSearchStore adds the shared encrypted-looking data bag
// fixture to the fake offline store, matching what admin commands see after
// loading PostgreSQL-backed core object rows.
func adminEncryptedDataBagSearchStore() *fakeOfflineStore {
	store := adminReindexTestStore()
	org := store.objects.Orgs["ponyville"]
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	if org.DataBags == nil {
		org.DataBags = map[string]bootstrap.DataBag{}
	}
	org.DataBags[bagName] = bootstrap.DataBag{Name: bagName, JSONClass: "Chef::DataBag", ChefType: "data_bag"}
	if org.DataBagItems == nil {
		org.DataBagItems = map[string]map[string]bootstrap.DataBagItem{}
	}
	org.DataBagItems[bagName] = map[string]bootstrap.DataBagItem{
		itemID: {ID: itemID, RawData: testfixtures.EncryptedDataBagItem()},
	}
	if org.ACLs == nil {
		org.ACLs = map[string]authz.ACL{}
	}
	org.ACLs["data:"+bagName] = authz.ACL{Read: authz.Permission{Actors: []string{"pivotal"}}}
	store.objects.Orgs["ponyville"] = org
	return store
}

func adminUnsupportedSearchStore() *fakeOfflineStore {
	store := adminReindexTestStore()
	org := store.objects.Orgs["ponyville"]
	revision := bootstrap.PolicyRevision{
		Name:       "appserver",
		RevisionID: "1111111111111111111111111111111111111111",
		Payload:    adminUnsupportedPolicyPayload(),
	}
	org.Policies = map[string]map[string]bootstrap.PolicyRevision{
		"appserver": {
			revision.RevisionID: revision,
		},
	}
	org.PolicyGroups = map[string]bootstrap.PolicyGroup{
		"dev": {
			Name:     "dev",
			Policies: map[string]string{"appserver": revision.RevisionID},
		},
	}
	org.Sandboxes = map[string]bootstrap.Sandbox{
		"sandbox-one": {
			ID:           "sandbox-one",
			Organization: "ponyville",
			Checksums:    []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		},
	}
	store.objects.Orgs["ponyville"] = org
	return store
}

func adminUnsupportedPolicyPayload() map[string]any {
	return map[string]any{
		"name":        "appserver",
		"revision_id": "1111111111111111111111111111111111111111",
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "1.2.3",
			},
		},
	}
}

func adminSupportedSearchRefs() []search.DocumentRef {
	return []search.DocumentRef{
		{Organization: "ponyville", Index: "client", Name: "web01"},
		{Organization: "ponyville", Index: "environment", Name: "_default"},
		{Organization: "ponyville", Index: "node", Name: "twilight"},
		{Organization: "ponyville", Index: "ponies", Name: "alice"},
	}
}

func adminSupportedProviderIDs() []string {
	return []string{
		"ponyville/client/web01",
		"ponyville/environment/_default",
		"ponyville/node/twilight",
		"ponyville/ponies/alice",
	}
}

func adminUnsupportedProviderIDs() []string {
	return []string{
		"ponyville/checksums/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"ponyville/cookbook_artifacts/unindexedartifact",
		"ponyville/cookbooks/unindexed",
		"ponyville/policy/appserver",
		"ponyville/policy_groups/dev",
		"ponyville/sandbox/sandbox-one",
	}
}

func adminUnsupportedProviderScopes() []string {
	return []string{
		"ponyville/checksums",
		"ponyville/cookbook_artifacts",
		"ponyville/cookbooks",
		"ponyville/policy",
		"ponyville/policy_groups",
		"ponyville/sandbox",
	}
}

func adminUnsupportedSearchIndexes() []string {
	return []string{
		"cookbooks",
		"cookbook_artifacts",
		"policy",
		"policy_groups",
		"sandbox",
		"checksums",
	}
}

type fakeAdminReindexTarget struct {
	pings         int
	ensureCalls   int
	refreshes     int
	deleteQueries []adminReindexDeleteQuery
	deletedIDs    []string
	bulkBatches   [][]search.Document
	pingErr       error
}

type adminReindexDeleteQuery struct {
	org   string
	index string
}

func (t *fakeAdminReindexTarget) Ping(context.Context) error {
	t.pings++
	return t.pingErr
}

func (t *fakeAdminReindexTarget) EnsureChefIndex(context.Context) error {
	t.ensureCalls++
	return nil
}

func (t *fakeAdminReindexTarget) DeleteByQuery(_ context.Context, org, index string) error {
	t.deleteQueries = append(t.deleteQueries, adminReindexDeleteQuery{org: org, index: index})
	return nil
}

func (t *fakeAdminReindexTarget) DeleteDocument(_ context.Context, id string) error {
	t.deletedIDs = append(t.deletedIDs, id)
	return nil
}

func (t *fakeAdminReindexTarget) BulkUpsert(_ context.Context, docs []search.Document) error {
	batch := make([]search.Document, len(docs))
	copy(batch, docs)
	t.bulkBatches = append(t.bulkBatches, batch)
	return nil
}

func (t *fakeAdminReindexTarget) Refresh(context.Context) error {
	t.refreshes++
	return nil
}

func (t *fakeAdminReindexTarget) upsertedRefs() []search.DocumentRef {
	var refs []search.DocumentRef
	for _, batch := range t.bulkBatches {
		for _, doc := range batch {
			refs = append(refs, search.DocumentRef{
				Organization: doc.Resource.Organization,
				Index:        doc.Index,
				Name:         doc.Name,
			})
		}
	}
	return refs
}

// upsertedDocuments flattens fake admin reindex bulk batches so command tests
// can inspect the generated search document contents.
func (t *fakeAdminReindexTarget) upsertedDocuments() []search.Document {
	var docs []search.Document
	for _, batch := range t.bulkBatches {
		docs = append(docs, batch...)
	}
	return docs
}

type adminCapabilityProviderMode string

const (
	adminCapabilityDirectDeleteByQuery adminCapabilityProviderMode = "direct"
	adminCapabilityFallbackDelete      adminCapabilityProviderMode = "fallback"
)

type adminCapabilityOpenSearchTransport struct {
	t                      *testing.T
	mode                   adminCapabilityProviderMode
	docs                   map[string]map[string]any
	directDeleteByQueries  int
	fallbackDeleteSearches int
}

func newAdminCapabilityOpenSearchTransport(t *testing.T, mode adminCapabilityProviderMode) *adminCapabilityOpenSearchTransport {
	t.Helper()
	return &adminCapabilityOpenSearchTransport{
		t:    t,
		mode: mode,
		docs: map[string]map[string]any{},
	}
}

func newAdminCapabilityOpenSearchCommand(t *testing.T, store *fakeOfflineStore, transport *adminCapabilityOpenSearchTransport) (*command, *strings.Builder, *strings.Builder) {
	t.Helper()
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	cmd := newCommand(stdout, stderr)
	cmd.load = func() (config.Config, error) {
		t.Fatal("unexpected server config load")
		return config.Config{}, nil
	}
	cmd.loadAdminConfig = func() admin.Config {
		t.Fatal("unexpected admin config load")
		return admin.Config{}
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			PostgresDSN:   "postgres://capability-test",
			OpenSearchURL: "http://opensearch.test",
		}, nil
	}
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://capability-test" {
			t.Fatalf("dsn = %q, want postgres://capability-test", dsn)
		}
		return store, nil, nil
	}
	cmd.newReindexTarget = func(raw string) (search.ReindexTarget, error) {
		return adminCapabilityOpenSearchClient(t, raw, transport)
	}
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		return adminCapabilityOpenSearchClient(t, raw, transport)
	}
	setTestMaintenanceStore(cmd, activeAdminWorkflowMaintenanceStore(t), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	return cmd, stdout, stderr
}

func adminCapabilityOpenSearchClient(t *testing.T, raw string, transport *adminCapabilityOpenSearchTransport) (*search.OpenSearchClient, error) {
	t.Helper()
	if raw != "http://opensearch.test" {
		t.Fatalf("opensearch URL = %q, want http://opensearch.test", raw)
	}
	return search.NewOpenSearchClient(raw, search.WithOpenSearchTransport(transport))
}

func (t *adminCapabilityOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	t.t.Helper()

	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		if err != nil {
			t.t.Fatalf("ReadAll(OpenSearch request) error = %v", err)
		}
	}

	switch {
	case req.Method == http.MethodGet && req.URL.Path == "/":
		return t.jsonResponse(req, http.StatusOK, t.rootResponse()), nil
	case req.Method == http.MethodHead && req.URL.Path == "/chef":
		return t.response(req, http.StatusOK, ""), nil
	case req.Method == http.MethodGet && req.URL.Path == "/chef/_mapping":
		return t.jsonResponse(req, http.StatusOK, map[string]any{
			"chef": map[string]any{
				"mappings": map[string]any{
					"_meta":   map[string]any{"opencook_mapping_version": 1},
					"dynamic": true,
					"properties": map[string]any{
						"document_id":  map[string]any{"type": "keyword"},
						"compat_terms": map[string]any{"type": "keyword"},
					},
				},
			},
		}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_delete_by_query":
		return t.handleDeleteByQuery(req, body), nil
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_search":
		return t.handleSearch(req, body), nil
	case req.Method == http.MethodPost && req.URL.Path == "/_bulk":
		t.handleBulk(body)
		return t.jsonResponse(req, http.StatusOK, map[string]any{"errors": false}), nil
	case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.Path, "/chef/_doc/"):
		id := adminCapabilityDocumentIDFromPath(t.t, req.URL)
		delete(t.docs, id)
		return t.jsonResponse(req, http.StatusOK, map[string]any{"result": "deleted"}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_refresh":
		return t.jsonResponse(req, http.StatusOK, map[string]any{"_shards": map[string]any{"successful": 1}}), nil
	default:
		t.t.Fatalf("OpenSearch capability transport request = %s %s?%s body=%s", req.Method, req.URL.Path, req.URL.RawQuery, string(body))
		return nil, nil
	}
}

func (t *adminCapabilityOpenSearchTransport) rootResponse() map[string]any {
	if t.mode == adminCapabilityFallbackDelete {
		return map[string]any{
			"name": "fallback-node",
			"version": map[string]any{
				"distribution": "opensearch",
				"number":       "2.12.0",
			},
			"tagline": "The OpenSearch Project: https://opensearch.org/",
		}
	}
	return map[string]any{
		"name": "opensearch-node",
		"version": map[string]any{
			"distribution": "opensearch",
			"number":       "2.12.0",
		},
		"tagline": "The OpenSearch Project: https://opensearch.org/",
	}
}

func (t *adminCapabilityOpenSearchTransport) handleDeleteByQuery(req *http.Request, body []byte) *http.Response {
	if t.mode == adminCapabilityFallbackDelete {
		return t.jsonResponse(req, http.StatusMethodNotAllowed, map[string]any{"error": "delete-by-query disabled in test provider"})
	}
	t.directDeleteByQueries++
	org, index := adminCapabilityScopeFromBody(t.t, body)
	deleted := t.deleteScope(org, index)
	return t.jsonResponse(req, http.StatusOK, map[string]any{"deleted": deleted})
}

func (t *adminCapabilityOpenSearchTransport) handleSearch(req *http.Request, body []byte) *http.Response {
	org, index := adminCapabilityScopeFromBody(t.t, body)
	if adminCapabilityBodyUsesDeleteScope(t.t, body) || adminCapabilityBodyUsesFallbackDeleteSearch(t.t, body) {
		t.fallbackDeleteSearches++
	}

	ids := t.matchingIDs(org, index)
	sort.Strings(ids)
	ids = adminCapabilityApplySearchAfterAndSize(t.t, ids, body)

	hits := make([]any, 0, len(ids))
	for _, id := range ids {
		hits = append(hits, map[string]any{
			"_id":  id,
			"sort": []any{id},
		})
	}
	total := any(map[string]any{"value": len(ids)})
	if t.mode == adminCapabilityFallbackDelete {
		total = len(ids)
	}
	return t.jsonResponse(req, http.StatusOK, map[string]any{
		"hits": map[string]any{
			"total": total,
			"hits":  hits,
		},
	})
}

func (t *adminCapabilityOpenSearchTransport) handleBulk(body []byte) {
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines)%2 != 0 {
		t.t.Fatalf("bulk body has odd line count: %s", string(body))
	}
	for i := 0; i < len(lines); i += 2 {
		var action map[string]map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk action %q) error = %v", lines[i], err)
		}
		indexAction, ok := action["index"]
		if !ok {
			t.t.Fatalf("bulk action = %v, want index action", action)
		}
		id, ok := indexAction["_id"].(string)
		if !ok || id == "" {
			t.t.Fatalf("bulk action _id = %v, want string", indexAction["_id"])
		}
		var source map[string]any
		if err := json.Unmarshal([]byte(lines[i+1]), &source); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk source %q) error = %v", lines[i+1], err)
		}
		source["document_id"] = id
		t.docs[id] = source
	}
}

func (t *adminCapabilityOpenSearchTransport) deleteScope(org, index string) int {
	deleted := 0
	for _, id := range t.matchingIDs(org, index) {
		delete(t.docs, id)
		deleted++
	}
	return deleted
}

func (t *adminCapabilityOpenSearchTransport) matchingIDs(org, index string) []string {
	ids := make([]string, 0, len(t.docs))
	for id, source := range t.docs {
		if !adminCapabilityDocumentMatchesScope(id, source, org, index) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (t *adminCapabilityOpenSearchTransport) forceDocument(id string) {
	org, index, name, ok := adminCapabilitySplitDocumentID(id)
	if !ok {
		t.t.Fatalf("invalid forced document ID %q", id)
	}
	t.docs[id] = map[string]any{
		"document_id":   id,
		"organization":  org,
		"index":         index,
		"name":          name,
		"resource_type": index,
		"resource_name": name,
		"compat_terms": []any{
			"__org=" + org,
			"__index=" + index,
			"name=" + name,
			"__any=" + name,
		},
	}
}

func (t *adminCapabilityOpenSearchTransport) hasDocument(id string) bool {
	_, ok := t.docs[id]
	return ok
}

// resetCapabilityCounters lets one test reuse the same provider contents while
// measuring a later admin command's direct-vs-fallback delete behavior.
func (t *adminCapabilityOpenSearchTransport) resetCapabilityCounters() {
	t.directDeleteByQueries = 0
	t.fallbackDeleteSearches = 0
}

func (t *adminCapabilityOpenSearchTransport) documentIDs() []string {
	ids := make([]string, 0, len(t.docs))
	for id := range t.docs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (t *adminCapabilityOpenSearchTransport) jsonResponse(req *http.Request, status int, payload any) *http.Response {
	data, err := json.Marshal(payload)
	if err != nil {
		t.t.Fatalf("json.Marshal(OpenSearch response) error = %v", err)
	}
	return t.response(req, status, string(data))
}

func (t *adminCapabilityOpenSearchTransport) response(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}

func adminCapabilityDocumentIDFromPath(t *testing.T, u *url.URL) string {
	t.Helper()
	escaped := strings.TrimPrefix(u.EscapedPath(), "/chef/_doc/")
	id, err := url.PathUnescape(escaped)
	if err != nil {
		t.Fatalf("PathUnescape(%q) error = %v", escaped, err)
	}
	return id
}

func adminCapabilityScopeFromBody(t *testing.T, body []byte) (string, string) {
	t.Helper()
	var payload any
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("json.Unmarshal(OpenSearch request %s) error = %v", string(body), err)
		}
	}

	org := adminCapabilityFirstTerm(payload, "organization")
	index := adminCapabilityFirstTerm(payload, "index")
	for _, term := range adminCapabilityTerms(payload, "compat_terms") {
		switch {
		case strings.HasPrefix(term, "__org="):
			org = strings.TrimPrefix(term, "__org=")
		case strings.HasPrefix(term, "__index="):
			index = strings.TrimPrefix(term, "__index=")
		}
	}
	return org, index
}

func adminCapabilityBodyUsesDeleteScope(t *testing.T, body []byte) bool {
	t.Helper()
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(OpenSearch request %s) error = %v", string(body), err)
	}
	return adminCapabilityFirstTerm(payload, "organization") != "" || adminCapabilityFirstTerm(payload, "index") != ""
}

// adminCapabilityBodyUsesFallbackDeleteSearch detects the match-all and
// filter-only searches used by OpenSearch delete-by-query fallback pagination.
func adminCapabilityBodyUsesFallbackDeleteSearch(t *testing.T, body []byte) bool {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(OpenSearch request %s) error = %v", string(body), err)
	}
	query, _ := payload["query"].(map[string]any)
	if _, ok := query["match_all"]; ok {
		return true
	}
	boolQuery, _ := query["bool"].(map[string]any)
	if len(boolQuery) == 0 {
		return false
	}
	_, hasMust := boolQuery["must"]
	return !hasMust
}

func adminCapabilityApplySearchAfterAndSize(t *testing.T, ids []string, body []byte) []string {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(OpenSearch search request %s) error = %v", string(body), err)
	}
	if rawAfter, ok := payload["search_after"].([]any); ok && len(rawAfter) > 0 {
		after := fmt.Sprint(rawAfter[0])
		next := ids[:0]
		for _, id := range ids {
			if id > after {
				next = append(next, id)
			}
		}
		ids = next
	}
	if rawSize, ok := payload["size"].(float64); ok {
		size := int(rawSize)
		if size >= 0 && size < len(ids) {
			return ids[:size]
		}
	}
	return ids
}

func adminCapabilityFirstTerm(payload any, field string) string {
	terms := adminCapabilityTerms(payload, field)
	if len(terms) == 0 {
		return ""
	}
	return terms[0]
}

func adminCapabilityTerms(payload any, field string) []string {
	switch value := payload.(type) {
	case map[string]any:
		var out []string
		if term, ok := value["term"].(map[string]any); ok {
			if raw, ok := term[field]; ok {
				out = append(out, fmt.Sprint(raw))
			}
		}
		for _, child := range value {
			out = append(out, adminCapabilityTerms(child, field)...)
		}
		return out
	case []any:
		var out []string
		for _, child := range value {
			out = append(out, adminCapabilityTerms(child, field)...)
		}
		return out
	default:
		return nil
	}
}

func adminCapabilityDocumentMatchesScope(id string, source map[string]any, org, index string) bool {
	idOrg, idIndex, _, _ := adminCapabilitySplitDocumentID(id)
	sourceOrg := strings.TrimSpace(fmt.Sprint(source["organization"]))
	sourceIndex := strings.TrimSpace(fmt.Sprint(source["index"]))
	if sourceOrg == "" {
		sourceOrg = idOrg
	}
	if sourceIndex == "" {
		sourceIndex = idIndex
	}
	if org != "" && sourceOrg != org {
		return false
	}
	if index != "" && sourceIndex != index {
		return false
	}
	return true
}

func adminCapabilitySplitDocumentID(id string) (string, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(id), "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// adminSeedScaleFallbackPaginationDocuments forces enough stale provider IDs to
// require search_after pagination when delete-by-query fallback is used.
func adminSeedScaleFallbackPaginationDocuments(t *testing.T, target *adminCapabilityOpenSearchTransport, orgName string, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		target.forceDocument(fmt.Sprintf("%s/node/stale-scale-%04d", orgName, i))
	}
}

// adminRequireNoUnsupportedOpenSearchDocuments verifies cookbook, artifact,
// policy, policy-group, sandbox, and checksum rows remain non-searchable.
func adminRequireNoUnsupportedOpenSearchDocuments(t *testing.T, ids []string) {
	t.Helper()
	unsupported := make(map[string]struct{})
	for _, index := range adminUnsupportedSearchIndexes() {
		unsupported[index] = struct{}{}
	}
	for _, id := range ids {
		_, index, _, ok := adminCapabilitySplitDocumentID(id)
		if !ok {
			t.Fatalf("provider document ID %q is not an OpenCook search document ID", id)
		}
		if _, blocked := unsupported[index]; blocked {
			t.Fatalf("provider document IDs = %v, unexpectedly included non-searchable index %q", ids, index)
		}
	}
}

// requireAdminEncryptedDataBagDocument verifies the admin command rebuilt the
// shared encrypted-looking item as a data bag search document with raw_data intact.
func requireAdminEncryptedDataBagDocument(t *testing.T, docs []search.Document) search.Document {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	for _, doc := range docs {
		if doc.Resource.Organization != "ponyville" || doc.Index != bagName || doc.Name != itemID {
			continue
		}
		rawData, ok := doc.Object["raw_data"].(map[string]any)
		if !ok {
			t.Fatalf("admin encrypted document raw_data = %T(%v), want object", doc.Object["raw_data"], doc.Object["raw_data"])
		}
		if !payloadEqual(rawData, testfixtures.EncryptedDataBagItem()) {
			t.Fatalf("admin encrypted document raw_data = %#v, want %#v", rawData, testfixtures.EncryptedDataBagItem())
		}
		return doc
	}
	t.Fatalf("docs = %v, want ponyville/%s/%s", docs, bagName, itemID)
	return search.Document{}
}

// requireAdminEncryptedDataBagSearchFields checks the generated provider
// document indexes stored ciphertext envelope fields and clear metadata only.
func requireAdminEncryptedDataBagSearchFields(t *testing.T, doc search.Document) {
	t.Helper()

	password := testfixtures.EncryptedDataBagItem()["password"].(map[string]any)
	apiKey := testfixtures.EncryptedDataBagItem()["api_key"].(map[string]any)
	requireAdminSearchFieldContains(t, doc.Fields, "password_encrypted_data", password["encrypted_data"].(string))
	requireAdminSearchFieldContains(t, doc.Fields, "password_iv", password["iv"].(string))
	requireAdminSearchFieldContains(t, doc.Fields, "api_key_auth_tag", apiKey["auth_tag"].(string))
	requireAdminSearchFieldContains(t, doc.Fields, "environment", "production")
	if _, ok := doc.Fields["raw_data_password_encrypted_data"]; ok {
		t.Fatalf("admin encrypted document fields unexpectedly included raw_data-prefixed keys: %v", doc.Fields)
	}
}

// requireAdminSearchFieldContains provides a compact assertion for generated
// provider document fields without depending on OpenSearch implementation details.
func requireAdminSearchFieldContains(t *testing.T, fields map[string][]string, key, want string) {
	t.Helper()

	for _, got := range fields[key] {
		if got == want {
			return
		}
	}
	t.Fatalf("fields[%q] = %v, want to include %q", key, fields[key], want)
}

// assertAdminEncryptedOperationalOutputDoesNotLeakSecret guards command output
// and errors against plaintext or raw provider-body leakage.
func assertAdminEncryptedOperationalOutputDoesNotLeakSecret(t *testing.T, stdout, stderr string) {
	t.Helper()

	combined := stdout + "\n" + stderr
	for _, forbidden := range []string{"correct horse battery staple", "raw provider body", "data_bag_secret"} {
		if strings.Contains(combined, forbidden) {
			t.Fatalf("admin operational output leaked %q: stdout=%q stderr=%q", forbidden, stdout, stderr)
		}
	}
}

func decodeAdminReindexOutput(t *testing.T, raw string) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", raw, err)
	}
	return out
}

func requireAdminOutputWarningContains(t *testing.T, out map[string]any, want string) {
	t.Helper()

	raw, ok := out["warnings"].([]any)
	if !ok {
		t.Fatalf("warnings = %#v, want list containing %q", out["warnings"], want)
	}
	for _, item := range raw {
		warning, ok := item.(string)
		if ok && strings.Contains(warning, want) {
			return
		}
	}
	t.Fatalf("warnings = %#v, want entry containing %q", raw, want)
}

func requireAdminOutputErrorCode(t *testing.T, out map[string]any, want string) {
	t.Helper()

	raw, ok := out["errors"].([]any)
	if !ok {
		t.Fatalf("errors = %#v, want list containing code %q", out["errors"], want)
	}
	for _, item := range raw {
		errObj, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("errors item = %#v, want object", item)
		}
		if code, ok := errObj["code"].(string); ok && code == want {
			return
		}
	}
	t.Fatalf("errors = %#v, want code %q", raw, want)
}

func assertAdminReindexCount(t *testing.T, out map[string]any, key string, want int) {
	t.Helper()
	counts, ok := out["counts"].(map[string]any)
	if !ok {
		t.Fatalf("output counts = %#v, want object", out["counts"])
	}
	got, ok := counts[key].(float64)
	if !ok {
		t.Fatalf("counts[%s] = %#v, want number", key, counts[key])
	}
	if int(got) != want {
		t.Fatalf("counts[%s] = %d, want %d", key, int(got), want)
	}
}

func requireAdminReindexRefs(t *testing.T, got, want []search.DocumentRef) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("upserted refs = %v, want %v", got, want)
	}
	for _, ref := range want {
		if !hasAdminReindexRef(got, ref) {
			t.Fatalf("upserted refs = %v, missing %v", got, ref)
		}
	}
}

func requireNoAdminReindexRefsForIndexes(t *testing.T, refs []search.DocumentRef, indexes ...string) {
	t.Helper()

	unsupported := make(map[string]struct{}, len(indexes))
	for _, index := range indexes {
		unsupported[index] = struct{}{}
	}
	for _, ref := range refs {
		if _, blocked := unsupported[ref.Index]; blocked {
			t.Fatalf("refs = %v, unexpectedly included unsupported index %q", refs, ref.Index)
		}
	}
}

func hasAdminReindexRef(refs []search.DocumentRef, want search.DocumentRef) bool {
	for _, ref := range refs {
		if ref == want {
			return true
		}
	}
	return false
}

func sameAdminReindexDeleteQueries(got, want []adminReindexDeleteQuery) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sameAdminStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
