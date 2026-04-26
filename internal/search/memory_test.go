package search

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestMemoryIndexIndexesRequiresConfiguredState(t *testing.T) {
	index := NewMemoryIndex(nil, "")

	_, err := index.Indexes(context.Background(), "ponyville")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Indexes() error = %v, want ErrUnavailable", err)
	}
}

func TestMemoryIndexIndexesReturnsNotFoundForUnknownOrganization(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	index := NewMemoryIndex(state, "")

	_, err := index.Indexes(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Indexes() error = %v, want ErrNotFound", err)
	}
}

func TestMemoryIndexIndexesIncludeClient(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	index := NewMemoryIndex(state, "")
	indexes, err := index.Indexes(context.Background(), "ponyville")
	if err != nil {
		t.Fatalf("Indexes() error = %v", err)
	}
	if len(indexes) != 4 {
		t.Fatalf("indexes len = %d, want 4 (%v)", len(indexes), indexes)
	}
	if indexes[0] != "client" {
		t.Fatalf("indexes[0] = %q, want %q", indexes[0], "client")
	}
}

func TestMemoryIndexIndexesIncludeDataBags(t *testing.T) {
	state := newSearchTestState(t)
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Payload: map[string]any{"name": "ponies"},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}

	index := NewMemoryIndex(state, "")
	indexes, err := index.Indexes(context.Background(), "ponyville")
	if err != nil {
		t.Fatalf("Indexes() error = %v", err)
	}

	found := false
	for _, name := range indexes {
		if name == "ponies" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("indexes = %v, want to include data bag index %q", indexes, "ponies")
	}
}

func TestMemoryIndexIndexesDeduplicateBuiltInNameCollisions(t *testing.T) {
	state := newSearchTestState(t)
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Payload: map[string]any{"name": "client"},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}

	index := NewMemoryIndex(state, "")
	indexes, err := index.Indexes(context.Background(), "ponyville")
	if err != nil {
		t.Fatalf("Indexes() error = %v", err)
	}

	clientCount := 0
	for _, name := range indexes {
		if name == "client" {
			clientCount++
		}
	}
	if clientCount != 1 {
		t.Fatalf("client index count = %d, want 1 (%v)", clientCount, indexes)
	}
}

func TestMemoryIndexSearchReturnsNotFoundForUnknownOrganization(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	index := NewMemoryIndex(state, "")

	_, err := index.Search(context.Background(), Query{
		Organization: "missing",
		Index:        "node",
		Q:            "*:*",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Search() error = %v, want ErrNotFound", err)
	}
}

func TestMemoryIndexSearchSupportsDataBagDocuments(t *testing.T) {
	state := newSearchTestState(t)
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Payload: map[string]any{"name": "ponies"},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
	if _, err := state.CreateDataBagItem("ponyville", "ponies", bootstrap.CreateDataBagItemInput{
		Payload: map[string]any{
			"id": "twilight",
			"ssh": map[string]any{
				"public_key": "ssh-rsa AAAA twilight",
			},
		},
	}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}

	index := NewMemoryIndex(state, "")
	result, err := index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "ponies",
		Q:            "ssh_public_key:*",
	})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(result.Documents) != 1 {
		t.Fatalf("documents len = %d, want 1 (%v)", len(result.Documents), result.Documents)
	}

	doc := result.Documents[0]
	if doc.Object["name"] != "data_bag_item_ponies_twilight" {
		t.Fatalf("doc.Object[name] = %v, want %q", doc.Object["name"], "data_bag_item_ponies_twilight")
	}
	if doc.Resource.Type != "data_bag" || doc.Resource.Name != "ponies" {
		t.Fatalf("doc.Resource = %+v, want data_bag/ponies", doc.Resource)
	}
	if doc.Partial["id"] != "twilight" {
		t.Fatalf("doc.Partial[id] = %v, want %q", doc.Partial["id"], "twilight")
	}
	if _, ok := doc.Fields["raw_data_ssh_public_key"]; ok {
		t.Fatalf("doc.Fields unexpectedly included raw_data-prefixed keys: %v", doc.Fields)
	}
}

func TestMemoryIndexSearchSupportsEscapedSlashAndAndNotTerms(t *testing.T) {
	state := newSearchTestState(t)
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Payload: map[string]any{"name": "x"},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
	for _, payload := range []map[string]any{
		{"id": "foo", "path": "foo/bar"},
		{"id": "foo-bar"},
	} {
		if _, err := state.CreateDataBagItem("ponyville", "x", bootstrap.CreateDataBagItemInput{Payload: payload}); err != nil {
			t.Fatalf("CreateDataBagItem(%v) error = %v", payload["id"], err)
		}
	}

	index := NewMemoryIndex(state, "")
	result, err := index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "x",
		Q:            "id:foo* AND NOT bar",
	})
	if err != nil {
		t.Fatalf("Search(and/not) error = %v", err)
	}
	if len(result.Documents) != 2 {
		t.Fatalf("and/not documents len = %d, want 2 (%v)", len(result.Documents), result.Documents)
	}

	result, err = index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "x",
		Q:            `path:foo\/*`,
	})
	if err != nil {
		t.Fatalf("Search(escaped slash) error = %v", err)
	}
	if len(result.Documents) != 1 {
		t.Fatalf("escaped slash documents len = %d, want 1 (%v)", len(result.Documents), result.Documents)
	}
	if result.Documents[0].Name != "foo" {
		t.Fatalf("escaped slash document name = %q, want %q", result.Documents[0].Name, "foo")
	}
}

func TestMemoryIndexSearchReturnsStableSortedLargeResultSet(t *testing.T) {
	state := newSearchTestState(t)
	creator := authn.Principal{Type: "user", Name: "pivotal"}
	for _, name := range []string{"zeta", "alpha", "kilo", "bravo", "hotel", "charlie", "india", "delta", "juliet", "echo", "foxtrot", "golf"} {
		if _, err := state.CreateNode("ponyville", bootstrap.CreateNodeInput{
			Creator: creator,
			Payload: map[string]any{
				"name":     name,
				"run_list": []any{"base"},
				"default": map[string]any{
					"sequence": "050",
				},
				"normal": map[string]any{
					"team": "fleet",
				},
			},
		}); err != nil {
			t.Fatalf("CreateNode(%s) error = %v", name, err)
		}
	}

	index := NewMemoryIndex(state, "")
	result, err := index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "(team:fleet OR recipe:missing) AND sequence:[001 TO 999]",
	})
	if err != nil {
		t.Fatalf("Search(large ordered query) error = %v", err)
	}

	got := make([]string, 0, len(result.Documents))
	for _, doc := range result.Documents {
		got = append(got, doc.Name)
	}
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "zeta"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("large ordered search names = %v, want %v", got, want)
	}
}

func TestMatchesAndExpressionRejectsEmptyExpressions(t *testing.T) {
	fields := map[string][]string{
		"name": {"twilight"},
	}

	if matchesAndExpression(fields, "") {
		t.Fatal("matchesAndExpression(\"\") = true, want false")
	}
	if matchesAndExpression(fields, "   ") {
		t.Fatal("matchesAndExpression(whitespace) = true, want false")
	}
}

func TestMatchesQueryDoesNotTreatEmptyOrClauseAsMatchAll(t *testing.T) {
	doc := Document{
		Fields: map[string][]string{
			"name": {"twilight"},
		},
	}

	if matchesQuery(doc, "name:rainbow OR ") {
		t.Fatal(`matchesQuery("name:rainbow OR ") = true, want false`)
	}
}

func newSearchTestState(t *testing.T) *bootstrap.Service {
	t.Helper()

	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	return state
}
