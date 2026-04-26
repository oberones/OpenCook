package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
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
			if strings.Contains(stderr.String(), "raw provider") {
				t.Fatalf("stderr leaked provider internals: %q", stderr.String())
			}
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
	return cmd, stdout, stderr
}

func adminReindexTestStore() *fakeOfflineStore {
	return &fakeOfflineStore{
		bootstrap: adminReindexBootstrapState(),
		objects:   adminReindexCoreObjectState(),
	}
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
