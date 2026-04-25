package search

import "testing"

func TestQueryPlanPreservesMemoryMatcherBehavior(t *testing.T) {
	fields := map[string][]string{
		"name":   {"twilight"},
		"path":   {"foo/bar"},
		"recipe": {"base", "app::default"},
		"role":   {"web"},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "empty matches all", query: "", want: true},
		{name: "match all", query: "*:*", want: true},
		{name: "field term", query: "name:twilight", want: true},
		{name: "unqualified term", query: "twilight", want: true},
		{name: "prefix wildcard", query: "name:twi*", want: true},
		{name: "escaped slash prefix", query: `path:foo\/*`, want: true},
		{name: "or expression", query: "name:rainbow OR role:web", want: true},
		{name: "and not expression", query: "role:web AND NOT recipe:missing", want: true},
		{name: "dash negation", query: "role:web AND -recipe:base", want: false},
		{name: "missing field", query: "name:rainbow", want: false},
		{name: "empty or clause stays false", query: "name:rainbow OR ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CompileQuery(tt.query).MatchesFields(fields); got != tt.want {
				t.Fatalf("CompileQuery(%q).MatchesFields() = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestQueryPlanBuildsOpenSearchCompatibilityBody(t *testing.T) {
	body := CompileQuery(` path:foo\/* `).OpenSearchQueryBody()

	query, ok := body["query"].(map[string]any)
	if !ok {
		t.Fatalf("body[query] = %T(%v), want map", body["query"], body["query"])
	}
	boolQuery, ok := query["bool"].(map[string]any)
	if !ok {
		t.Fatalf("body query bool = %T(%v), want map", query["bool"], query["bool"])
	}
	must := boolQuery["must"].([]any)
	prefix := must[0].(map[string]any)["prefix"].(map[string]any)
	if prefix[openSearchCompatTermsField] != "path=foo/" {
		t.Fatalf("prefix query = %v, want path=foo/", prefix)
	}

	emptyBody := CompileQuery("").OpenSearchQueryBody()
	if _, ok := emptyBody["query"].(map[string]any)["match_all"].(map[string]any); !ok {
		t.Fatalf("empty query = %v, want match_all", emptyBody["query"])
	}
}
