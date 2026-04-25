package search

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReindexServiceCompleteAllOrganizationsUsesStartupScope(t *testing.T) {
	state := newSearchRebuildState(t)
	target := &recordingReindexTarget{}
	now := newSteppingClock(
		mustParseReindexTime(t, "2026-04-25T12:00:00Z"),
		mustParseReindexTime(t, "2026-04-25T12:00:02Z"),
	)

	result, err := NewReindexService(state, target).WithNow(now).Run(context.Background(), ReindexPlan{
		Mode:             ReindexModeComplete,
		AllOrganizations: true,
	})
	if err != nil {
		t.Fatalf("Run(complete all) error = %v", err)
	}

	if target.pings != 1 || target.ensureCalls != 1 || target.refreshes != 1 {
		t.Fatalf("target setup calls = ping:%d ensure:%d refresh:%d, want 1/1/1", target.pings, target.ensureCalls, target.refreshes)
	}
	if len(target.deleteQueries) != 1 || target.deleteQueries[0] != (reindexDeleteQuery{}) {
		t.Fatalf("delete queries = %#v, want global match_all delete", target.deleteQueries)
	}
	if got := target.upsertedRefs(); !hasReindexRef(got, "ponyville", "client", "ponyville-validator") ||
		!hasReindexRef(got, "ponyville", "environment", "_default") ||
		!hasReindexRef(got, "ponyville", "node", "twilight") ||
		!hasReindexRef(got, "ponyville", "ponies", "alice") {
		t.Fatalf("upserted refs = %v, want startup search documents", got)
	}
	if result.Counts.Scanned != 4 || result.Counts.Upserted != 4 || result.Counts.Deleted != 4 {
		t.Fatalf("counts = %+v, want scanned/upserted/deleted 4", result.Counts)
	}
	if result.Duration != 2*time.Second {
		t.Fatalf("duration = %s, want 2s", result.Duration)
	}
}

func TestReindexServiceScopesToOrgIndexDataBagAndNames(t *testing.T) {
	state := newSearchRebuildState(t)

	t.Run("built-in index reindex", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeReindex,
			Organization: "ponyville",
			Index:        "node",
		})
		if err != nil {
			t.Fatalf("Run(node reindex) error = %v", err)
		}
		if len(target.deleteQueries) != 0 || len(target.deletedIDs) != 0 {
			t.Fatalf("delete calls = %#v/%#v, want none", target.deleteQueries, target.deletedIDs)
		}
		if got := target.upsertedRefs(); len(got) != 1 || !hasReindexRef(got, "ponyville", "node", "twilight") {
			t.Fatalf("upserted refs = %v, want node/twilight only", got)
		}
		if result.Counts.Scanned != 1 || result.Counts.Upserted != 1 {
			t.Fatalf("counts = %+v, want one scanned/upserted", result.Counts)
		}
	})

	t.Run("data bag index reindex", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeReindex,
			Organization: "ponyville",
			Index:        "ponies",
		})
		if err != nil {
			t.Fatalf("Run(data bag reindex) error = %v", err)
		}
		if got := target.upsertedRefs(); len(got) != 1 || !hasReindexRef(got, "ponyville", "ponies", "alice") {
			t.Fatalf("upserted refs = %v, want ponies/alice only", got)
		}
		if result.Counts.Scanned != 1 || result.Counts.Upserted != 1 {
			t.Fatalf("counts = %+v, want one scanned/upserted", result.Counts)
		}
	})

	t.Run("named complete deletes requested refs and upserts found refs", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeComplete,
			Organization: "ponyville",
			Index:        "node",
			Names:        []string{"missing", "twilight", "twilight"},
		})
		if err != nil {
			t.Fatalf("Run(named complete) error = %v", err)
		}
		if got, want := target.deletedIDs, []string{"ponyville/node/missing", "ponyville/node/twilight"}; !sameStrings(got, want) {
			t.Fatalf("deleted IDs = %v, want %v", got, want)
		}
		if got := target.upsertedRefs(); len(got) != 1 || !hasReindexRef(got, "ponyville", "node", "twilight") {
			t.Fatalf("upserted refs = %v, want node/twilight only", got)
		}
		if result.Counts.Scanned != 1 || result.Counts.Missing != 1 || result.Counts.Deleted != 2 || result.Counts.Upserted != 1 {
			t.Fatalf("counts = %+v, want scanned 1 missing 1 deleted 2 upserted 1", result.Counts)
		}
	})
}

func TestReindexServiceDropAndDryRunPlans(t *testing.T) {
	state := newSearchRebuildState(t)

	t.Run("drop index", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeDrop,
			Organization: "ponyville",
			Index:        "node",
		})
		if err != nil {
			t.Fatalf("Run(drop index) error = %v", err)
		}
		if got, want := target.deleteQueries, []reindexDeleteQuery{{org: "ponyville", index: "node"}}; !sameDeleteQueries(got, want) {
			t.Fatalf("delete queries = %#v, want %#v", got, want)
		}
		if len(target.bulkBatches) != 0 {
			t.Fatalf("bulk batches = %d, want none for drop", len(target.bulkBatches))
		}
		if result.Counts.Scanned != 1 || result.Counts.Deleted != 1 || result.Counts.Upserted != 0 {
			t.Fatalf("counts = %+v, want scanned/deleted 1", result.Counts)
		}
	})

	t.Run("dry run avoids provider calls", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeComplete,
			Organization: "ponyville",
			Index:        "node",
			DryRun:       true,
		})
		if err != nil {
			t.Fatalf("Run(dry run) error = %v", err)
		}
		if target.pings != 0 || target.ensureCalls != 0 || len(target.deleteQueries) != 0 || len(target.bulkBatches) != 0 || target.refreshes != 0 {
			t.Fatalf("target calls = %+v, want none for dry run", target)
		}
		if result.Counts.Scanned != 1 || result.Counts.Skipped != 2 || result.Counts.Upserted != 0 || result.Counts.Deleted != 0 {
			t.Fatalf("counts = %+v, want scanned 1 skipped 2", result.Counts)
		}
	})
}

func TestReindexServiceValidationAndProviderFailures(t *testing.T) {
	state := newSearchRebuildState(t)

	t.Run("missing organization", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeReindex,
			Organization: "missing",
		})
		if !errors.Is(err, ErrOrganizationNotFound) {
			t.Fatalf("Run(missing org) error = %v, want ErrOrganizationNotFound", err)
		}
		if target.pings != 0 {
			t.Fatalf("target pings = %d, want no provider calls", target.pings)
		}
		if result.Counts.Failed != 1 || result.Failures[0] != ErrOrganizationNotFound.Error() {
			t.Fatalf("result = %+v, want redacted org failure", result)
		}
	})

	t.Run("missing index", func(t *testing.T) {
		target := &recordingReindexTarget{}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeReindex,
			Organization: "ponyville",
			Index:        "missing-index",
		})
		if !errors.Is(err, ErrIndexNotFound) {
			t.Fatalf("Run(missing index) error = %v, want ErrIndexNotFound", err)
		}
		if target.pings != 0 {
			t.Fatalf("target pings = %d, want no provider calls", target.pings)
		}
		if result.Counts.Failed != 1 || result.Failures[0] != ErrIndexNotFound.Error() {
			t.Fatalf("result = %+v, want redacted index failure", result)
		}
	})

	t.Run("provider unavailable", func(t *testing.T) {
		target := &recordingReindexTarget{pingErr: errors.New("raw provider body: cluster down")}
		result, err := NewReindexService(state, target).Run(context.Background(), ReindexPlan{
			Mode:         ReindexModeReindex,
			Organization: "ponyville",
			Index:        "node",
		})
		if err == nil {
			t.Fatal("Run(provider failure) error = nil, want error")
		}
		if result.Counts.Failed != 1 || result.Failures[0] != "reindex failed" {
			t.Fatalf("result = %+v, want generic redacted provider failure", result)
		}
	})
}

type recordingReindexTarget struct {
	pings         int
	ensureCalls   int
	refreshes     int
	deleteQueries []reindexDeleteQuery
	deletedIDs    []string
	bulkBatches   [][]Document
	pingErr       error
	deleteErr     error
	upsertErr     error
}

type reindexDeleteQuery struct {
	org   string
	index string
}

func (t *recordingReindexTarget) Ping(context.Context) error {
	t.pings++
	return t.pingErr
}

func (t *recordingReindexTarget) EnsureChefIndex(context.Context) error {
	t.ensureCalls++
	return nil
}

func (t *recordingReindexTarget) DeleteByQuery(_ context.Context, org, index string) error {
	t.deleteQueries = append(t.deleteQueries, reindexDeleteQuery{org: org, index: index})
	return t.deleteErr
}

func (t *recordingReindexTarget) DeleteDocument(_ context.Context, id string) error {
	t.deletedIDs = append(t.deletedIDs, id)
	return t.deleteErr
}

func (t *recordingReindexTarget) BulkUpsert(_ context.Context, docs []Document) error {
	batch := make([]Document, len(docs))
	copy(batch, docs)
	t.bulkBatches = append(t.bulkBatches, batch)
	return t.upsertErr
}

func (t *recordingReindexTarget) Refresh(context.Context) error {
	t.refreshes++
	return nil
}

func (t *recordingReindexTarget) upsertedRefs() []DocumentRef {
	var refs []DocumentRef
	for _, batch := range t.bulkBatches {
		for _, doc := range batch {
			refs = append(refs, DocumentRef{
				Organization: doc.Resource.Organization,
				Index:        doc.Index,
				Name:         doc.Name,
			})
		}
	}
	return refs
}

func hasReindexRef(refs []DocumentRef, org, index, name string) bool {
	for _, ref := range refs {
		if ref.Organization == org && ref.Index == index && ref.Name == name {
			return true
		}
	}
	return false
}

func sameDeleteQueries(got, want []reindexDeleteQuery) bool {
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

func sameStrings(got, want []string) bool {
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

func newSteppingClock(times ...time.Time) func() time.Time {
	i := 0
	return func() time.Time {
		if i >= len(times) {
			return times[len(times)-1]
		}
		t := times[i]
		i++
		return t
	}
}

func mustParseReindexTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("time.Parse(%q) error = %v", raw, err)
	}
	return parsed
}
