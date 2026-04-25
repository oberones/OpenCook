package search

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestOpenSearchIndexListsStateBackedIndexes(t *testing.T) {
	state := newSearchRebuildState(t)
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		t.Fatalf("Indexes() should not call OpenSearch")
		return 0, ""
	})))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	index := NewOpenSearchIndex(state, client, "http://opensearch.local")
	indexes, err := index.Indexes(context.Background(), "ponyville")
	if err != nil {
		t.Fatalf("Indexes() error = %v", err)
	}

	if got, want := strings.Join(indexes, ","), "client,environment,node,role,ponies"; got != want {
		t.Fatalf("Indexes() = %v, want %s", indexes, want)
	}
}

func TestOpenSearchIndexSearchHydratesCurrentStateFromReturnedIDs(t *testing.T) {
	state := newSearchRebuildState(t)
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		body := decodeJSONMap(t, recorded.Body)
		boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
		requireCompatPrefixClause(t, boolQuery["must"], "name=twi")

		return http.StatusOK, `{"hits":{"hits":[` +
			`{"_id":"ponyville/node/twilight"},` +
			`{"_id":"ponyville/node/stale"},` +
			`{"_id":"ponyville/role/twilight"},` +
			`{"_id":"canterlot/node/twilight"}` +
			`]}}`
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	index := NewOpenSearchIndex(state, client, "http://opensearch.local")
	result, err := index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:twi*",
	})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(result.Documents) != 1 {
		t.Fatalf("Search() docs len = %d, want 1 (%v)", len(result.Documents), result.Documents)
	}
	doc := result.Documents[0]
	if doc.Index != "node" || doc.Name != "twilight" {
		t.Fatalf("Search() doc = %s/%s, want node/twilight", doc.Index, doc.Name)
	}
	if doc.Object["name"] != "twilight" {
		t.Fatalf("hydrated doc object name = %v, want twilight", doc.Object["name"])
	}
}

func TestOpenSearchIndexHydratesDynamicDataBagDocuments(t *testing.T) {
	state := newSearchRebuildState(t)
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		return http.StatusOK, `{"hits":{"hits":[{"_id":"ponyville/ponies/alice"}]}}`
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	index := NewOpenSearchIndex(state, client, "http://opensearch.local")
	result, err := index.Search(context.Background(), Query{
		Organization: "ponyville",
		Index:        "ponies",
		Q:            "id:alice",
	})
	if err != nil {
		t.Fatalf("Search(data bag) error = %v", err)
	}

	if len(result.Documents) != 1 {
		t.Fatalf("Search(data bag) docs len = %d, want 1 (%v)", len(result.Documents), result.Documents)
	}
	doc := result.Documents[0]
	if doc.Index != "ponies" || doc.Name != "alice" {
		t.Fatalf("Search(data bag) doc = %s/%s, want ponies/alice", doc.Index, doc.Name)
	}
	if doc.Object["data_bag"] != "ponies" {
		t.Fatalf("data bag object data_bag = %v, want ponies", doc.Object["data_bag"])
	}
}
