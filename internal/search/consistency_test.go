package search

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestConsistencyServiceReportsDriftAndObjectCounts(t *testing.T) {
	state := newSearchRebuildState(t)
	target := newRecordingConsistencyTarget(
		"ponyville/client/ponyville-validator",
		"ponyville/environment/_default",
		"ponyville/node/stale",
		"ponyville/unsupported/stale",
	)

	result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
	})
	if err != nil {
		t.Fatalf("Run(check) error = %v", err)
	}

	if target.pings != 1 || target.ensureCalls != 0 || target.refreshes != 0 {
		t.Fatalf("target calls = ping:%d ensure:%d refresh:%d, want 1/0/0", target.pings, target.ensureCalls, target.refreshes)
	}
	if result.Counts.Expected != 4 || result.Counts.Observed != 4 || result.Counts.Missing != 2 || result.Counts.Stale != 2 || result.Counts.Unsupported != 1 || result.Counts.Clean != 0 {
		t.Fatalf("counts = %+v, want expected/observed/missing/stale/unsupported 4/4/2/2/1", result.Counts)
	}
	if !sameStrings(result.MissingDocuments, []string{"ponyville/node/twilight", "ponyville/ponies/alice"}) {
		t.Fatalf("missing = %v, want node and data bag item", result.MissingDocuments)
	}
	if !sameStrings(result.StaleDocuments, []string{"ponyville/node/stale", "ponyville/unsupported/stale"}) {
		t.Fatalf("stale = %v, want stale provider IDs", result.StaleDocuments)
	}
	if !sameStrings(result.UnsupportedScopes, []string{"ponyville/unsupported"}) {
		t.Fatalf("unsupported scopes = %v, want unsupported index", result.UnsupportedScopes)
	}
	if !hasConsistencyObjectCount(result.ObjectCounts, "ponyville", "node", 1, 1, 1, 1) {
		t.Fatalf("object counts = %+v, want node expected/observed/missing/stale 1", result.ObjectCounts)
	}
	if hasConsistencyObjectCount(result.ObjectCounts, "ponyville", "unsupported", 0, 1, 0, 1) {
		t.Fatalf("object counts = %+v, want unsupported provider scope reported only in unsupported_scopes", result.ObjectCounts)
	}
}

func TestConsistencyServiceRepairFixesDriftAndIsIdempotent(t *testing.T) {
	state := newSearchRebuildState(t)
	beforeDocs, err := DocumentsFromBootstrapState(state)
	if err != nil {
		t.Fatalf("DocumentsFromBootstrapState(before) error = %v", err)
	}
	beforeSearch, err := NewMemoryIndex(state, "").Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "*:*",
	})
	if err != nil {
		t.Fatalf("Memory search before repair error = %v", err)
	}

	target := newRecordingConsistencyTarget(
		"ponyville/client/ponyville-validator",
		"ponyville/environment/_default",
		"ponyville/node/stale",
	)
	result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
		Repair:       true,
	})
	if err != nil {
		t.Fatalf("Run(repair) error = %v", err)
	}
	if result.Counts.Missing != 2 || result.Counts.Stale != 1 || result.Counts.Upserted != 2 || result.Counts.Deleted != 1 {
		t.Fatalf("repair counts = %+v, want missing 2 stale 1 upserted 2 deleted 1", result.Counts)
	}
	if target.ensureCalls != 1 || target.refreshes != 1 {
		t.Fatalf("repair target ensure/refresh = %d/%d, want 1/1", target.ensureCalls, target.refreshes)
	}
	if !sameStrings(target.deletedIDs, []string{"ponyville/node/stale"}) {
		t.Fatalf("deleted IDs = %v, want stale node", target.deletedIDs)
	}

	afterDocs, err := DocumentsFromBootstrapState(state)
	if err != nil {
		t.Fatalf("DocumentsFromBootstrapState(after) error = %v", err)
	}
	if !reflect.DeepEqual(beforeDocs, afterDocs) {
		t.Fatal("repair mutated PostgreSQL-derived bootstrap state")
	}
	afterSearch, err := NewMemoryIndex(state, "").Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "*:*",
	})
	if err != nil {
		t.Fatalf("Memory search after repair error = %v", err)
	}
	if !reflect.DeepEqual(beforeSearch, afterSearch) {
		t.Fatal("repair changed Chef-facing search result shaping")
	}

	clean, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
		Repair:       true,
	})
	if err != nil {
		t.Fatalf("Run(second repair) error = %v", err)
	}
	if clean.Counts.Clean != 1 || clean.Counts.Missing != 0 || clean.Counts.Stale != 0 || clean.Counts.Upserted != 0 || clean.Counts.Deleted != 0 {
		t.Fatalf("second repair counts = %+v, want clean no-op", clean.Counts)
	}
}

// TestConsistencyServiceRepairsEncryptedDataBagDocuments pins search check and
// repair for encrypted-looking data bag items: missing provider documents are
// rebuilt from stored JSON, and stale provider IDs are removed without secrets.
func TestConsistencyServiceRepairsEncryptedDataBagDocuments(t *testing.T) {
	state := newEncryptedDataBagRebuildState(t)
	bagName := encryptedDataBagSearchIndexName()
	itemID := encryptedDataBagSearchItemName()
	target := newRecordingConsistencyTarget("ponyville/" + bagName + "/stale")

	check, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
		Index:        bagName,
	})
	if err != nil {
		t.Fatalf("Run(encrypted check) error = %v", err)
	}
	if check.Counts.Expected != 1 || check.Counts.Observed != 1 || check.Counts.Missing != 1 || check.Counts.Stale != 1 {
		t.Fatalf("check counts = %+v, want expected/observed/missing/stale 1", check.Counts)
	}
	if !sameStrings(check.MissingDocuments, []string{"ponyville/" + bagName + "/" + itemID}) {
		t.Fatalf("missing encrypted docs = %v, want encrypted item", check.MissingDocuments)
	}
	if !sameStrings(check.StaleDocuments, []string{"ponyville/" + bagName + "/stale"}) {
		t.Fatalf("stale encrypted docs = %v, want stale item", check.StaleDocuments)
	}

	repair, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
		Index:        bagName,
		Repair:       true,
	})
	if err != nil {
		t.Fatalf("Run(encrypted repair) error = %v", err)
	}
	if repair.Counts.Upserted != 1 || repair.Counts.Deleted != 1 || repair.Counts.Missing != 1 || repair.Counts.Stale != 1 {
		t.Fatalf("repair counts = %+v, want missing/stale/upserted/deleted 1", repair.Counts)
	}
	if target.ensureCalls != 1 || target.refreshes != 1 {
		t.Fatalf("repair ensure/refresh = %d/%d, want 1/1", target.ensureCalls, target.refreshes)
	}
	if !sameStrings(target.deletedIDs, []string{"ponyville/" + bagName + "/stale"}) {
		t.Fatalf("deleted IDs = %v, want stale encrypted item", target.deletedIDs)
	}
	doc := requireEncryptedDataBagDocument(t, target.upsertedDocuments())
	requireEncryptedDataBagSearchFields(t, doc)
	if !target.hasID("ponyville/"+bagName+"/"+itemID) || target.hasID("ponyville/"+bagName+"/stale") {
		t.Fatalf("provider IDs after repair = %v, want encrypted item present and stale removed", target.ids)
	}
}

func TestConsistencyServiceDryRunRepairDoesNotMutateProvider(t *testing.T) {
	state := newSearchRebuildState(t)
	target := newRecordingConsistencyTarget(
		"ponyville/client/ponyville-validator",
		"ponyville/environment/_default",
		"ponyville/node/stale",
	)

	result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
		Organization: "ponyville",
		Repair:       true,
		DryRun:       true,
	})
	if err != nil {
		t.Fatalf("Run(dry-run repair) error = %v", err)
	}
	if result.Counts.Skipped != 3 {
		t.Fatalf("skipped = %d, want 3 missing/stale operations", result.Counts.Skipped)
	}
	if target.ensureCalls != 0 || target.refreshes != 0 || len(target.deletedIDs) != 0 || len(target.bulkBatches) != 0 {
		t.Fatalf("target mutated during dry-run: %+v", target)
	}
	if !target.hasID("ponyville/node/stale") {
		t.Fatal("dry-run removed stale provider ID")
	}
}

func TestConsistencyServiceScopesValidationAndProviderFailures(t *testing.T) {
	state := newSearchRebuildState(t)

	t.Run("index scope", func(t *testing.T) {
		target := newRecordingConsistencyTarget("ponyville/node/twilight", "ponyville/client/ponyville-validator")
		result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
			Organization: "ponyville",
			Index:        "node",
		})
		if err != nil {
			t.Fatalf("Run(index check) error = %v", err)
		}
		if result.Counts.Expected != 1 || result.Counts.Observed != 1 || result.Counts.Clean != 1 {
			t.Fatalf("index counts = %+v, want clean node scope", result.Counts)
		}
	})

	t.Run("missing organization", func(t *testing.T) {
		target := newRecordingConsistencyTarget()
		result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{Organization: "missing"})
		if !errors.Is(err, ErrOrganizationNotFound) {
			t.Fatalf("Run(missing org) error = %v, want ErrOrganizationNotFound", err)
		}
		if target.pings != 0 {
			t.Fatalf("provider pings = %d, want no provider calls", target.pings)
		}
		if result.Counts.Failed != 1 || result.Failures[0] != ErrOrganizationNotFound.Error() {
			t.Fatalf("result = %+v, want redacted org failure", result)
		}
	})

	t.Run("missing index", func(t *testing.T) {
		target := newRecordingConsistencyTarget()
		result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{
			Organization: "ponyville",
			Index:        "missing-index",
		})
		if !errors.Is(err, ErrIndexNotFound) {
			t.Fatalf("Run(missing index) error = %v, want ErrIndexNotFound", err)
		}
		if target.pings != 0 {
			t.Fatalf("provider pings = %d, want no provider calls", target.pings)
		}
		if result.Counts.Failed != 1 || result.Failures[0] != ErrIndexNotFound.Error() {
			t.Fatalf("result = %+v, want redacted index failure", result)
		}
	})

	t.Run("provider unavailable redacted", func(t *testing.T) {
		target := newRecordingConsistencyTarget()
		target.searchErr = errors.New("raw provider body: shard unavailable")
		result, err := NewConsistencyService(state, target).Run(context.Background(), ConsistencyPlan{Organization: "ponyville"})
		if err == nil {
			t.Fatal("Run(provider failure) error = nil, want error")
		}
		if strings.Contains(err.Error(), "raw provider body") {
			t.Fatalf("returned error leaked raw provider body: %v", err)
		}
		if result.Counts.Failed != 1 || result.Failures[0] != ErrUnavailable.Error() {
			t.Fatalf("result = %+v, want redacted provider failure", result)
		}
	})

	t.Run("duration", func(t *testing.T) {
		target := newRecordingConsistencyTarget(
			"ponyville/client/ponyville-validator",
			"ponyville/environment/_default",
			"ponyville/node/twilight",
			"ponyville/ponies/alice",
		)
		now := newSteppingClock(
			mustParseReindexTime(t, "2026-04-25T12:00:00Z"),
			mustParseReindexTime(t, "2026-04-25T12:00:03Z"),
		)
		result, err := NewConsistencyService(state, target).WithNow(now).Run(context.Background(), ConsistencyPlan{Organization: "ponyville"})
		if err != nil {
			t.Fatalf("Run(duration) error = %v", err)
		}
		if result.Duration != 3*time.Second {
			t.Fatalf("duration = %s, want 3s", result.Duration)
		}
	})
}

type recordingConsistencyTarget struct {
	ids         map[string]struct{}
	pings       int
	ensureCalls int
	refreshes   int
	deletedIDs  []string
	bulkBatches [][]Document
	pingErr     error
	searchErr   error
	upsertErr   error
	deleteErr   error
}

func newRecordingConsistencyTarget(ids ...string) *recordingConsistencyTarget {
	out := &recordingConsistencyTarget{ids: map[string]struct{}{}}
	for _, id := range ids {
		out.ids[id] = struct{}{}
	}
	return out
}

func (t *recordingConsistencyTarget) Ping(context.Context) error {
	t.pings++
	return t.pingErr
}

func (t *recordingConsistencyTarget) EnsureChefIndex(context.Context) error {
	t.ensureCalls++
	return nil
}

func (t *recordingConsistencyTarget) SearchIDs(_ context.Context, query Query) ([]string, error) {
	if t.searchErr != nil {
		return nil, ErrUnavailable
	}
	var ids []string
	for id := range t.ids {
		ref, ok := parseOpenSearchDocumentID(id)
		if query.Organization != "" {
			if !ok || ref.Organization != query.Organization {
				continue
			}
		}
		if query.Index != "" {
			if !ok || ref.Index != query.Index {
				continue
			}
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (t *recordingConsistencyTarget) BulkUpsert(_ context.Context, docs []Document) error {
	if t.upsertErr != nil {
		return t.upsertErr
	}
	batch := make([]Document, len(docs))
	copy(batch, docs)
	t.bulkBatches = append(t.bulkBatches, batch)
	for _, doc := range docs {
		t.ids[OpenSearchDocumentID(doc)] = struct{}{}
	}
	return nil
}

func (t *recordingConsistencyTarget) DeleteDocument(_ context.Context, id string) error {
	if t.deleteErr != nil {
		return t.deleteErr
	}
	t.deletedIDs = append(t.deletedIDs, id)
	delete(t.ids, id)
	return nil
}

func (t *recordingConsistencyTarget) Refresh(context.Context) error {
	t.refreshes++
	return nil
}

// upsertedDocuments flattens consistency repair bulk writes so tests can inspect
// opaque document bodies in addition to repaired document IDs.
func (t *recordingConsistencyTarget) upsertedDocuments() []Document {
	var docs []Document
	for _, batch := range t.bulkBatches {
		docs = append(docs, batch...)
	}
	return docs
}

func (t *recordingConsistencyTarget) hasID(id string) bool {
	_, ok := t.ids[id]
	return ok
}

func hasConsistencyObjectCount(counts []ConsistencyObjectCount, org, index string, expected, observed, missing, stale int) bool {
	for _, count := range counts {
		if count.Organization == org && count.Index == index && count.Expected == expected && count.Observed == observed && count.Missing == missing && count.Stale == stale {
			return true
		}
	}
	return false
}
