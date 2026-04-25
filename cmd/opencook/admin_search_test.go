package main

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

func TestAdminSearchCheckAndRepairCommands(t *testing.T) {
	target := newFakeAdminSearchTarget(
		"ponyville/client/web01",
		"ponyville/environment/_default",
		"ponyville/node/stale",
	)
	cmd, stdout, stderr := newAdminSearchTestCommand(t, adminReindexTestStore(), target)

	if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", "ponyville", "--with-timing"}); code != exitPartial {
		t.Fatalf("Run(search check) exit = %d, want %d; stdout = %s stderr = %s", code, exitPartial, stdout.String(), stderr.String())
	}
	out := decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "missing", 2)
	assertAdminReindexCount(t, out, "stale", 1)
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("search check output missing duration_ms: %v", out)
	}
	if stderr.Len() != 0 {
		t.Fatalf("search check stderr = %q, want empty for drift-only result", stderr.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", "ponyville", "--dry-run"}); code != exitPartial {
		t.Fatalf("Run(search repair dry-run) exit = %d, want %d; stdout = %s stderr = %s", code, exitPartial, stdout.String(), stderr.String())
	}
	if len(target.deletedIDs) != 0 || len(target.bulkBatches) != 0 || target.ensureCalls != 0 {
		t.Fatalf("dry-run target mutations = deleted:%v bulks:%d ensure:%d, want none", target.deletedIDs, len(target.bulkBatches), target.ensureCalls)
	}
	out = decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "skipped", 3)

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", "ponyville", "--yes"}); code != exitOK {
		t.Fatalf("Run(search repair) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if target.ensureCalls != 1 || target.refreshes != 1 {
		t.Fatalf("repair ensure/refresh = %d/%d, want 1/1", target.ensureCalls, target.refreshes)
	}
	if !sameAdminStrings(target.deletedIDs, []string{"ponyville/node/stale"}) {
		t.Fatalf("deleted IDs = %v, want stale node", target.deletedIDs)
	}
	if len(target.bulkBatches) != 1 || len(target.bulkBatches[0]) != 2 {
		t.Fatalf("bulk batches = %#v, want two missing documents", target.bulkBatches)
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "search", "check", "--org", "ponyville"}); code != exitOK {
		t.Fatalf("Run(search check after repair) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	out = decodeAdminReindexOutput(t, stdout.String())
	assertAdminReindexCount(t, out, "clean", 1)
}

func TestAdminSearchCommandFailuresAreStableAndRedacted(t *testing.T) {
	for _, tc := range []struct {
		name         string
		args         []string
		target       *fakeAdminSearchTarget
		wantCode     int
		wantStderr   string
		forbidStderr string
	}{
		{
			name:       "missing organization",
			args:       []string{"admin", "search", "check", "--org", "missing"},
			target:     newFakeAdminSearchTarget(),
			wantCode:   exitNotFound,
			wantStderr: search.ErrOrganizationNotFound.Error(),
		},
		{
			name:       "missing index",
			args:       []string{"admin", "search", "check", "--org", "ponyville", "--index", "missing-index"},
			target:     newFakeAdminSearchTarget(),
			wantCode:   exitNotFound,
			wantStderr: search.ErrIndexNotFound.Error(),
		},
		{
			name:         "provider failure",
			args:         []string{"admin", "search", "check", "--org", "ponyville"},
			target:       &fakeAdminSearchTarget{ids: map[string]struct{}{}, searchErr: errors.New("raw provider body from cluster")},
			wantCode:     exitDependencyUnavailable,
			wantStderr:   "reindex failed",
			forbidStderr: "raw provider body",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newAdminSearchTestCommand(t, adminReindexTestStore(), tc.target)

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

func TestAdminSearchRequiresConfigAndRepairConfirmation(t *testing.T) {
	t.Run("postgres required", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			return config.Config{OpenSearchURL: "http://opensearch.test"}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "search", "check"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(no postgres) exit = %d, want %d", code, exitDependencyUnavailable)
		}
		if !strings.Contains(stderr.String(), "PostgreSQL configuration") {
			t.Fatalf("stderr = %q, want PostgreSQL requirement", stderr.String())
		}
	})

	t.Run("opensearch required", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		cmd.loadOffline = func() (config.Config, error) {
			return config.Config{PostgresDSN: "postgres://search-test"}, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "search", "check"})
		if code != exitDependencyUnavailable {
			t.Fatalf("Run(no opensearch) exit = %d, want %d", code, exitDependencyUnavailable)
		}
		if !strings.Contains(stderr.String(), "OpenSearch configuration") {
			t.Fatalf("stderr = %q, want OpenSearch requirement", stderr.String())
		}
	})

	t.Run("repair requires dry-run or yes", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		code := cmd.Run(context.Background(), []string{"admin", "search", "repair", "--org", "ponyville"})
		if code != exitUsage {
			t.Fatalf("Run(repair without confirmation) exit = %d, want %d", code, exitUsage)
		}
		if !strings.Contains(stderr.String(), "requires --dry-run or --yes") {
			t.Fatalf("stderr = %q, want confirmation message", stderr.String())
		}
	})
}

func newAdminSearchTestCommand(t *testing.T, store *fakeOfflineStore, target *fakeAdminSearchTarget) (*command, *strings.Builder, *strings.Builder) {
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
			PostgresDSN:   "postgres://search-test",
			OpenSearchURL: "http://opensearch.test",
		}, nil
	}
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://search-test" {
			t.Fatalf("dsn = %q, want postgres://search-test", dsn)
		}
		return store, nil, nil
	}
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		if raw != "http://opensearch.test" {
			t.Fatalf("opensearch URL = %q, want http://opensearch.test", raw)
		}
		return target, nil
	}
	return cmd, stdout, stderr
}

type fakeAdminSearchTarget struct {
	ids         map[string]struct{}
	pings       int
	ensureCalls int
	refreshes   int
	deletedIDs  []string
	bulkBatches [][]search.Document
	pingErr     error
	searchErr   error
}

func newFakeAdminSearchTarget(ids ...string) *fakeAdminSearchTarget {
	out := &fakeAdminSearchTarget{ids: map[string]struct{}{}}
	for _, id := range ids {
		out.ids[id] = struct{}{}
	}
	return out
}

func (t *fakeAdminSearchTarget) Ping(context.Context) error {
	t.pings++
	return t.pingErr
}

func (t *fakeAdminSearchTarget) EnsureChefIndex(context.Context) error {
	t.ensureCalls++
	return nil
}

func (t *fakeAdminSearchTarget) SearchIDs(_ context.Context, query search.Query) ([]string, error) {
	if t.searchErr != nil {
		return nil, t.searchErr
	}
	var ids []string
	for id := range t.ids {
		org, index, ok := splitAdminSearchID(id)
		if query.Organization != "" {
			if !ok || org != query.Organization {
				continue
			}
		}
		if query.Index != "" {
			if !ok || index != query.Index {
				continue
			}
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (t *fakeAdminSearchTarget) BulkUpsert(_ context.Context, docs []search.Document) error {
	batch := make([]search.Document, len(docs))
	copy(batch, docs)
	t.bulkBatches = append(t.bulkBatches, batch)
	for _, doc := range docs {
		t.ids[search.OpenSearchDocumentID(doc)] = struct{}{}
	}
	return nil
}

func (t *fakeAdminSearchTarget) DeleteDocument(_ context.Context, id string) error {
	t.deletedIDs = append(t.deletedIDs, id)
	delete(t.ids, id)
	return nil
}

func (t *fakeAdminSearchTarget) Refresh(context.Context) error {
	t.refreshes++
	return nil
}

func splitAdminSearchID(id string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(id), "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
