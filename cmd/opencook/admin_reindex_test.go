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
