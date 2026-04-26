package search

import (
	"errors"
	"strings"
	"testing"
)

// TestQueryPlanPreservesMemoryMatcherBehavior captures the exact matcher
// behavior that existed before this broader Lucene-compatibility bucket.
func TestQueryPlanPreservesMemoryMatcherBehavior(t *testing.T) {
	fields := map[string][]string{
		"name":       {"twilight"},
		"path":       {"foo/bar"},
		"recipe":     {"base", "app::default"},
		"role":       {"web"},
		"badge":      {"primary[blue]"},
		"public_key": {"ssh-rsa AAAA twilight"},
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
		{name: "field exists", query: "name:*", want: true},
		{name: "prefix wildcard", query: "name:twi*", want: true},
		{name: "escaped slash prefix", query: `path:foo\/*`, want: true},
		{name: "escaped bracket term", query: `badge:primary\[blue\]`, want: true},
		{name: "quoted phrase term", query: `public_key:"ssh-rsa AAAA twilight"`, want: true},
		{name: "escaped at sign miss stays literal", query: `recipe:app\@1.0.0`, want: false},
		{name: "or expression", query: "name:rainbow OR role:web", want: true},
		{name: "and expression", query: "role:web AND recipe:base", want: true},
		{name: "and not expression", query: "role:web AND NOT recipe:missing", want: true},
		{name: "grouped or with and expression", query: "(name:rainbow OR role:web) AND recipe:base", want: true},
		{name: "grouped or with missing and expression", query: "(name:rainbow OR role:web) AND recipe:missing", want: false},
		{name: "and with grouped or expression", query: "role:web AND (recipe:missing OR recipe:base)", want: true},
		{name: "only not expression", query: "NOT recipe:missing", want: true},
		{name: "not grouped expression", query: "NOT (recipe:missing OR name:rainbow)", want: true},
		{name: "dash negation", query: "role:web AND -recipe:base", want: false},
		{name: "missing field", query: "name:rainbow", want: false},
		{name: "unknown field", query: "missing:twilight", want: false},
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

// TestQueryPlanBaselineMatchesOpenSearchCompatibilityClause guards the current
// memory/OpenSearch parity before future tasks replace the split-based parser.
func TestQueryPlanBaselineMatchesOpenSearchCompatibilityClause(t *testing.T) {
	doc := Document{
		Index: "node",
		Name:  "twilight",
		Fields: map[string][]string{
			"name":                    {"twilight"},
			"chef_environment":        {"production"},
			"path":                    {"foo/bar"},
			"badge":                   {"primary[blue]"},
			"recipe":                  {"base", "app::default"},
			"role":                    {"web"},
			"policy_name":             {"delivery"},
			"ssh_public_key":          {"ssh-rsa AAAA twilight"},
			"password_encrypted_data": {"ciphertext"},
		},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "empty query", query: "", want: true},
		{name: "whitespace query", query: "   ", want: true},
		{name: "match all", query: "*:*", want: true},
		{name: "field term", query: "name:twilight", want: true},
		{name: "unqualified term", query: "twilight", want: true},
		{name: "field exists", query: "policy_name:*", want: true},
		{name: "missing field exists", query: "missing:*", want: false},
		{name: "prefix wildcard", query: "name:twi*", want: true},
		{name: "escaped slash prefix", query: `path:foo\/*`, want: true},
		{name: "escaped bracket term", query: `badge:primary\[blue\]`, want: true},
		{name: "quoted phrase term", query: `ssh_public_key:"ssh-rsa AAAA twilight"`, want: true},
		{name: "encrypted envelope field term", query: "password_encrypted_data:ciphertext", want: true},
		{name: "or expression", query: "name:rainbow OR role:web", want: true},
		{name: "and expression", query: "role:web AND recipe:base", want: true},
		{name: "and not expression", query: "role:web AND NOT recipe:missing", want: true},
		{name: "and has precedence over or", query: "name:rainbow OR role:web AND recipe:base", want: true},
		{name: "and precedence can still fail", query: "name:rainbow OR role:web AND recipe:missing", want: false},
		{name: "grouped or with and expression", query: "(name:rainbow OR role:web) AND recipe:base", want: true},
		{name: "grouped or with missing and expression", query: "(name:rainbow OR role:web) AND recipe:missing", want: false},
		{name: "and with grouped or expression", query: "role:web AND (recipe:missing OR policy_name:delivery)", want: true},
		{name: "only not expression", query: "NOT recipe:missing", want: true},
		{name: "not grouped expression", query: "NOT (recipe:missing OR name:rainbow)", want: true},
		{name: "not grouped matching expression", query: "NOT (recipe:base OR name:rainbow)", want: false},
		{name: "dash negation", query: "role:web AND -recipe:base", want: false},
		{name: "dash grouped negation", query: "-(recipe:base OR name:rainbow)", want: false},
		{name: "missing field", query: "name:rainbow", want: false},
		{name: "empty or clause stays false", query: "name:rainbow OR ", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if got := plan.Matches(doc); got != tt.want {
				t.Fatalf("CompileQuery(%q).Matches() = %v, want %v", tt.query, got, tt.want)
			}
			if got := openSearchCompatibilityClauseMatches(doc, plan.OpenSearchQueryClause()); got != tt.want {
				t.Fatalf("OpenSearchQueryClause(%q) match = %v, want %v; clause = %#v", tt.query, got, tt.want, plan.OpenSearchQueryClause())
			}
		})
	}
}

// TestQueryPlanRejectsMalformedBooleanOperators keeps typoed boolean queries
// from silently broadening result sets in either memory or OpenSearch mode.
func TestQueryPlanRejectsMalformedBooleanOperators(t *testing.T) {
	fields := map[string][]string{
		"name":   {"twilight"},
		"role":   {"web"},
		"recipe": {"base"},
	}
	tests := []string{
		"OR name:twilight",
		"AND name:twilight",
		"name:twilight OR",
		"name:twilight AND",
		"name:twilight OR OR role:web",
		"name:twilight AND AND role:web",
		"name:twilight OR AND role:web",
		"name:twilight AND OR role:web",
		"NOT",
		"name:twilight AND NOT",
		"name:twilight OR NOT",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			plan := CompileQuery(query)
			if !errors.Is(plan.Err(), ErrInvalidQuery) {
				t.Fatalf("CompileQuery(%q).Err() = %v, want ErrInvalidQuery", query, plan.Err())
			}
			if plan.MatchesFields(fields) {
				t.Fatalf("CompileQuery(%q).MatchesFields() = true, want false", query)
			}
			if got := openSearchCompatibilityClauseMatches(Document{Fields: fields}, plan.OpenSearchQueryClause()); got {
				t.Fatalf("OpenSearchQueryClause(%q) match = true, want false", query)
			}
		})
	}
}

// TestQueryPlanSupportsGroupedBooleanPrecedence pins the Task 3 boolean
// contract explicitly: parenthesized groups bind first, unary negation applies
// to the following term/group, AND binds tighter than OR, and memory/OpenSearch
// planning keep the same candidate semantics.
func TestQueryPlanSupportsGroupedBooleanPrecedence(t *testing.T) {
	doc := Document{
		Index: "node",
		Name:  "twilight",
		Fields: map[string][]string{
			"name":        {"twilight"},
			"role":        {"web"},
			"recipe":      {"base", "app::default"},
			"policy_name": {"delivery"},
		},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "and binds tighter than or", query: "name:twilight OR role:db AND recipe:missing", want: true},
		{name: "and precedence succeeds", query: "name:rainbow OR role:web AND recipe:base", want: true},
		{name: "and precedence fails", query: "name:rainbow OR role:web AND recipe:missing", want: false},
		{name: "grouped or combined with required term", query: "(name:rainbow OR role:web) AND recipe:base", want: true},
		{name: "grouped or can fail required term", query: "(name:rainbow OR role:web) AND recipe:missing", want: false},
		{name: "required term with grouped disjunction", query: "role:web AND (recipe:missing OR policy_name:delivery)", want: true},
		{name: "not grouped expression misses", query: "role:web AND NOT (recipe:missing OR name:rainbow)", want: true},
		{name: "not grouped expression blocks", query: "role:web AND NOT (recipe:base OR name:rainbow)", want: false},
		{name: "dash negates grouped expression", query: "-(recipe:base OR name:rainbow)", want: false},
		{name: "dash negated group combines with required group", query: "(name:rainbow OR role:web) AND -recipe:missing", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if err := plan.Err(); err != nil {
				t.Fatalf("CompileQuery(%q).Err() = %v, want nil", tt.query, err)
			}
			if got := plan.Matches(doc); got != tt.want {
				t.Fatalf("CompileQuery(%q).Matches() = %v, want %v", tt.query, got, tt.want)
			}
			if got := openSearchCompatibilityClauseMatches(doc, plan.OpenSearchQueryClause()); got != tt.want {
				t.Fatalf("OpenSearchQueryClause(%q) match = %v, want %v; clause = %#v", tt.query, got, tt.want, plan.OpenSearchQueryClause())
			}
		})
	}
}

// TestQueryPlanSupportsEscapingQuotingAndWordBreakBehavior captures the Task 4
// Chef word-break subset from pedant: escaped special characters are literals,
// quoted phrases are exact strings, wildcarded phrases remain phrase-sensitive,
// and partial word searches do not split on punctuation.
func TestQueryPlanSupportsEscapingQuotingAndWordBreakBehavior(t *testing.T) {
	doc := Document{
		Index: "node",
		Name:  "search_supernode",
		Fields: map[string][]string{
			"name":                    {"search_supernode"},
			"attr_colon":              {"hello:world"},
			"attr_bang":               {"hello!world"},
			"attr_paren":              {"hello(world"},
			"attr_quote":              {`hello"world`},
			"attr_phrase":             {"hello world"},
			"key[abc":                 {"bracket-key"},
			"path":                    {"foo/bar"},
			"recipe":                  {"app::default"},
			"policy_name":             {"delivery-group"},
			"password_encrypted_data": {"cipher/text+opaque"},
		},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "escaped colon exact value", query: `attr_colon:hello\:world`, want: true},
		{name: "escaped bang exact value", query: `attr_bang:hello\!world`, want: true},
		{name: "escaped paren exact value", query: `attr_paren:hello\(world`, want: true},
		{name: "escaped quote exact value", query: `attr_quote:hello\"world`, want: true},
		{name: "escaped bracket field name", query: `key\[abc:bracket-key`, want: true},
		{name: "escaped slash prefix", query: `path:foo\/*`, want: true},
		{name: "recipe value escaped colons", query: `recipe:app\:\:default`, want: true},
		{name: "encrypted-looking slash plus value", query: `password_encrypted_data:cipher\/text+opaque`, want: true},
		{name: "quoted phrase exact value", query: `attr_phrase:"hello world"`, want: true},
		{name: "quoted phrase no punctuation word break", query: `attr_bang:"hello world"`, want: false},
		{name: "partial first word does not match", query: "attr_bang:hello", want: false},
		{name: "partial second word does not match", query: "attr_bang:world", want: false},
		{name: "wildcard around special char matches", query: `attr_bang:*\!*`, want: true},
		{name: "wildcard before special char with no trailing match misses", query: `attr_bang:*\!`, want: false},
		{name: "wildcard before special char with trailing match succeeds", query: `attr_bang:*\!world`, want: true},
		{name: "wildcarded phrase remains phrase-sensitive", query: `attr_bang:*"hello world"*`, want: false},
		{name: "wildcarded phrase exact space succeeds", query: `attr_phrase:*"hello world"*`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if err := plan.Err(); err != nil {
				t.Fatalf("CompileQuery(%q).Err() = %v, want nil", tt.query, err)
			}
			if got := plan.Matches(doc); got != tt.want {
				t.Fatalf("CompileQuery(%q).Matches() = %v, want %v", tt.query, got, tt.want)
			}
			if got := openSearchCompatibilityClauseMatches(doc, plan.OpenSearchQueryClause()); got != tt.want {
				t.Fatalf("OpenSearchQueryClause(%q) match = %v, want %v; clause = %#v", tt.query, got, tt.want, plan.OpenSearchQueryClause())
			}
		})
	}

	invalid := []string{
		`attr_phrase:"hello world`,
		`attr_phrase:hello\`,
	}
	for _, query := range invalid {
		t.Run("invalid "+query, func(t *testing.T) {
			if err := CompileQuery(query).Err(); !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("CompileQuery(%q).Err() = %v, want ErrInvalidQuery", query, err)
			}
		})
	}
}

// TestQueryPlanSupportsWildcardAndExistenceSemantics pins Task 5 wildcard
// behavior across field values, field names, leaf aliases, nested fields,
// run-list projections, policy fields, ordinary data bags, and encrypted-looking
// data bag envelopes.
func TestQueryPlanSupportsWildcardAndExistenceSemantics(t *testing.T) {
	doc := Document{
		Index: "node",
		Name:  "twilight",
		Fields: map[string][]string{
			"name":                    {"twilight"},
			"top_mid_bottom":          {"nested-target"},
			"bottom":                  {"nested-target"},
			"run_list":                {"recipe[app::default]", "role[webserver]"},
			"recipe":                  {"app::default"},
			"role":                    {"webserver"},
			"policy_name":             {"delivery-app"},
			"policy_group":            {"prod-blue"},
			"badge":                   {"primary[blue]"},
			"password_encrypted_data": {"cipher-text-opaque"},
			"encrypted_data":          {"cipher-text-opaque"},
		},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "match all", query: "*:*", want: true},
		{name: "field existence", query: "name:*", want: true},
		{name: "missing field existence", query: "missing:*", want: false},
		{name: "any-field exact value", query: `*:primary\[blue\]`, want: true},
		{name: "any-field missing value", query: "*:not-there", want: false},
		{name: "leading wildcard value", query: "name:*light", want: true},
		{name: "infix wildcard value", query: "name:twi*ght", want: true},
		{name: "single-character wildcard value", query: "name:twi?ight", want: true},
		{name: "trailing wildcard value", query: "policy_name:delivery-*", want: true},
		{name: "nested path exact", query: "top_mid_bottom:nested-target", want: true},
		{name: "leaf alias exact", query: "bottom:nested-target", want: true},
		{name: "wildcard field name", query: "top_*_bottom:nested-target", want: true},
		{name: "wildcard field name miss", query: "missing_*:nested-target", want: false},
		{name: "wildcard field name with value prefix", query: "top_*:nested-*", want: true},
		{name: "run list infix wildcard", query: `run_list:*app\:\:default*`, want: true},
		{name: "recipe leading wildcard", query: "recipe:*default", want: true},
		{name: "role trailing wildcard", query: "role:web*", want: true},
		{name: "policy group infix wildcard", query: "policy_group:prod*blue", want: true},
		{name: "ordinary data bag field wildcard", query: `ba*:primary\[blue\]`, want: true},
		{name: "encrypted envelope field wildcard", query: "*_encrypted_data:*opaque", want: true},
		{name: "encrypted leaf alias wildcard", query: "encrypted_data:cipher*opaque", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if err := plan.Err(); err != nil {
				t.Fatalf("CompileQuery(%q).Err() = %v, want nil", tt.query, err)
			}
			if got := plan.Matches(doc); got != tt.want {
				t.Fatalf("CompileQuery(%q).Matches() = %v, want %v", tt.query, got, tt.want)
			}
			if got := openSearchCompatibilityClauseMatches(doc, plan.OpenSearchQueryClause()); got != tt.want {
				t.Fatalf("OpenSearchQueryClause(%q) match = %v, want %v; clause = %#v", tt.query, got, tt.want, plan.OpenSearchQueryClause())
			}
		})
	}
}

// TestQueryPlanSupportsRangeQueryBehavior pins Task 6's decision: Chef field
// ranges are accepted and evaluated as lexicographic keyword ranges over the
// expanded search fields, matching the compat_terms OpenSearch clause.
func TestQueryPlanSupportsRangeQueryBehavior(t *testing.T) {
	doc := Document{
		Index: "node",
		Name:  "twilight",
		Fields: map[string][]string{
			"name":       {"twilight"},
			"build":      {"010"},
			"channel":    {"stable"},
			"score":      {"42"},
			"created_at": {"2026-04-02"},
		},
	}

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "inclusive exact", query: "build:[010 TO 010]", want: true},
		{name: "exclusive exact excludes both bounds", query: "build:{010 TO 010}", want: false},
		{name: "inclusive lower upper", query: "build:[001 TO 099]", want: true},
		{name: "exclusive range excludes lower candidate", query: "build:{010 TO 099}", want: false},
		{name: "exclusive range excludes upper candidate", query: "build:{001 TO 010}", want: false},
		{name: "open lower", query: "build:[* TO 010]", want: true},
		{name: "open upper", query: "build:[010 TO *]", want: true},
		{name: "all values range behaves as field exists", query: "build:[* TO *]", want: true},
		{name: "missing all values range", query: "missing:[* TO *]", want: false},
		{name: "string range", query: "channel:[alpha TO stable]", want: true},
		{name: "numeric-looking range uses keyword ordering", query: "score:[40 TO 50]", want: true},
		{name: "date-like range", query: "created_at:[2026-01-01 TO 2026-12-31]", want: true},
		{name: "range miss", query: "build:[011 TO 099]", want: false},
		{name: "range combined with boolean", query: "name:twilight AND build:[001 TO 099]", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if err := plan.Err(); err != nil {
				t.Fatalf("CompileQuery(%q).Err() = %v, want nil", tt.query, err)
			}
			if got := plan.Matches(doc); got != tt.want {
				t.Fatalf("CompileQuery(%q).Matches() = %v, want %v", tt.query, got, tt.want)
			}
			if got := openSearchCompatibilityClauseMatches(doc, plan.OpenSearchQueryClause()); got != tt.want {
				t.Fatalf("OpenSearchQueryClause(%q) match = %v, want %v; clause = %#v", tt.query, got, tt.want, plan.OpenSearchQueryClause())
			}
		})
	}

	invalid := []string{
		"build:[001 TO]",
		"build:[001 099]",
		"build:[001 TO 099}",
		"build:[]",
		"build:[00* TO 099]",
		"*: [001 TO 099]",
	}
	for _, query := range invalid {
		t.Run("invalid "+query, func(t *testing.T) {
			if err := CompileQuery(query).Err(); !errors.Is(err, ErrInvalidQuery) {
				t.Fatalf("CompileQuery(%q).Err() = %v, want ErrInvalidQuery", query, err)
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

	rangeBody := CompileQuery("build:[001 TO 099]").OpenSearchQueryBody()
	rangeQuery := rangeBody["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)[0].(map[string]any)["range"].(map[string]any)[openSearchCompatTermsField].(map[string]any)
	if rangeQuery["gte"] != "build=001" || rangeQuery["lte"] != "build=099" {
		t.Fatalf("range query = %v, want build inclusive bounds", rangeQuery)
	}
}

// TestTokenizeQueryClassifiesCurrentAndPendingSyntax documents the lexer
// boundary now that grouping is enabled while quoted phrases remain pending.
func TestTokenizeQueryClassifiesCurrentAndPendingSyntax(t *testing.T) {
	tokens, err := tokenizeQuery(`(name:twilight OR role:web) AND NOT path:foo\/* OR badge:primary\[blue\]`)
	if err != nil {
		t.Fatalf("tokenizeQuery() error = %v", err)
	}
	want := []queryToken{
		{kind: queryTokenLParen, value: "("},
		{kind: queryTokenTerm, value: "name:twilight"},
		{kind: queryTokenOr, value: "OR"},
		{kind: queryTokenTerm, value: "role:web"},
		{kind: queryTokenRParen, value: ")"},
		{kind: queryTokenAnd, value: "AND"},
		{kind: queryTokenNot, value: "NOT"},
		{kind: queryTokenTerm, value: `path:foo\/*`},
		{kind: queryTokenOr, value: "OR"},
		{kind: queryTokenTerm, value: `badge:primary\[blue\]`},
	}
	if len(tokens) != len(want) {
		t.Fatalf("tokens len = %d, want %d: %#v", len(tokens), len(want), tokens)
	}
	for i := range want {
		if tokens[i] != want[i] {
			t.Fatalf("tokens[%d] = %#v, want %#v", i, tokens[i], want[i])
		}
	}

	tokens, err = tokenizeQuery(`name:"twilight sparkle" AND path:foo\/*`)
	if err != nil {
		t.Fatalf("tokenize quoted phrase error = %v", err)
	}
	quotedWant := []queryToken{
		{kind: queryTokenTerm, value: `name:"twilight sparkle"`},
		{kind: queryTokenAnd, value: "AND"},
		{kind: queryTokenTerm, value: `path:foo\/*`},
	}
	if len(tokens) != len(quotedWant) {
		t.Fatalf("quoted tokens len = %d, want %d: %#v", len(tokens), len(quotedWant), tokens)
	}
	for i := range quotedWant {
		if tokens[i] != quotedWant[i] {
			t.Fatalf("quoted tokens[%d] = %#v, want %#v", i, tokens[i], quotedWant[i])
		}
	}

	if tokens, err := tokenizeQuery(`name:"twilight sparkle`); !errors.Is(err, ErrInvalidQuery) || tokens[0].kind != queryTokenQuoted {
		t.Fatalf("unterminated quote tokens=%#v err=%v, want quoted ErrInvalidQuery", tokens, err)
	}

	tokens, err = tokenizeQuery(`build:[001 TO 099] AND name:twilight`)
	if err != nil {
		t.Fatalf("tokenize range error = %v", err)
	}
	rangeWant := []queryToken{
		{kind: queryTokenTerm, value: `build:[001 TO 099]`},
		{kind: queryTokenAnd, value: "AND"},
		{kind: queryTokenTerm, value: "name:twilight"},
	}
	if len(tokens) != len(rangeWant) {
		t.Fatalf("range tokens len = %d, want %d: %#v", len(tokens), len(rangeWant), tokens)
	}
	for i := range rangeWant {
		if tokens[i] != rangeWant[i] {
			t.Fatalf("range tokens[%d] = %#v, want %#v", i, tokens[i], rangeWant[i])
		}
	}
}

// TestCompileQueryBuildsCurrentSubsetAST checks the concrete AST forms for the
// supported subset so future parser work can safely widen behavior.
func TestCompileQueryBuildsCurrentSubsetAST(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		wantRoot  any
		wantError bool
	}{
		{name: "empty", query: "", wantRoot: matchAllNode{}},
		{name: "match all", query: "*:*", wantRoot: matchAllNode{}},
		{name: "term", query: "name:twilight", wantRoot: andNode{}},
		{name: "or", query: "name:twilight OR role:web", wantRoot: orNode{}},
		{name: "grouping", query: "(name:twilight OR role:web) AND recipe:base", wantRoot: andNode{}},
		{name: "quoted phrase", query: `name:"twilight sparkle"`, wantRoot: andNode{}},
		{name: "range", query: `build:[001 TO 099]`, wantRoot: andNode{}},
		{name: "unbalanced grouping rejected", query: "(name:twilight", wantRoot: matchNoneNode{}, wantError: true},
		{name: "unterminated quote rejected", query: `name:"twilight sparkle`, wantRoot: matchNoneNode{}, wantError: true},
		{name: "malformed range rejected", query: `build:[001 099]`, wantRoot: matchNoneNode{}, wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := CompileQuery(tt.query)
			if tt.wantError {
				if !errors.Is(plan.Err(), ErrInvalidQuery) {
					t.Fatalf("CompileQuery(%q).Err() = %v, want ErrInvalidQuery", tt.query, plan.Err())
				}
			} else if plan.Err() != nil {
				t.Fatalf("CompileQuery(%q).Err() = %v, want nil", tt.query, plan.Err())
			}
			switch tt.wantRoot.(type) {
			case matchAllNode:
				if _, ok := plan.root.(matchAllNode); !ok {
					t.Fatalf("CompileQuery(%q).root = %T, want matchAllNode", tt.query, plan.root)
				}
			case matchNoneNode:
				if _, ok := plan.root.(matchNoneNode); !ok {
					t.Fatalf("CompileQuery(%q).root = %T, want matchNoneNode", tt.query, plan.root)
				}
			case andNode:
				if _, ok := plan.root.(andNode); !ok {
					t.Fatalf("CompileQuery(%q).root = %T, want andNode", tt.query, plan.root)
				}
			case orNode:
				if _, ok := plan.root.(orNode); !ok {
					t.Fatalf("CompileQuery(%q).root = %T, want orNode", tt.query, plan.root)
				}
			}
		})
	}
}

// openSearchCompatibilityClauseMatches evaluates the small compatibility query
// shape OpenCook emits today, giving this baseline test a provider-independent
// way to prove the memory matcher and OpenSearch clause still agree.
func openSearchCompatibilityClauseMatches(doc Document, clause map[string]any) bool {
	terms := openSearchCompatibilityTerms(doc)
	return openSearchCompatibilityClauseMatchesTerms(terms, clause)
}

// openSearchCompatibilityClauseMatchesTerms mirrors only the currently emitted
// OpenSearch clause shapes: match_all, match_none, term, prefix, wildcard, range, and bool.
func openSearchCompatibilityClauseMatchesTerms(terms []string, clause map[string]any) bool {
	if _, ok := clause["match_all"]; ok {
		return true
	}
	if _, ok := clause["match_none"]; ok {
		return false
	}
	if term, ok := clause["term"].(map[string]any); ok {
		return openSearchCompatibilityHasTerm(terms, term[openSearchCompatTermsField])
	}
	if prefix, ok := clause["prefix"].(map[string]any); ok {
		return openSearchCompatibilityHasPrefix(terms, prefix[openSearchCompatTermsField])
	}
	if wildcard, ok := clause["wildcard"].(map[string]any); ok {
		return openSearchCompatibilityHasWildcard(terms, wildcard[openSearchCompatTermsField])
	}
	if rangeClause, ok := clause["range"].(map[string]any); ok {
		return openSearchCompatibilityHasRange(terms, rangeClause[openSearchCompatTermsField])
	}

	boolQuery, ok := clause["bool"].(map[string]any)
	if !ok {
		return false
	}
	if should, ok := boolQuery["should"].([]any); ok {
		for _, raw := range should {
			if nested, ok := raw.(map[string]any); ok && openSearchCompatibilityClauseMatchesTerms(terms, nested) {
				return true
			}
		}
		return false
	}
	if must, ok := boolQuery["must"].([]any); ok {
		for _, raw := range must {
			nested, ok := raw.(map[string]any)
			if !ok || !openSearchCompatibilityClauseMatchesTerms(terms, nested) {
				return false
			}
		}
	}
	if mustNot, ok := boolQuery["must_not"].([]any); ok {
		for _, raw := range mustNot {
			nested, ok := raw.(map[string]any)
			if ok && openSearchCompatibilityClauseMatchesTerms(terms, nested) {
				return false
			}
		}
	}
	return true
}

// openSearchCompatibilityHasTerm checks exact keyword-token membership in the
// synthetic compat_terms field used by OpenCook's active OpenSearch path.
func openSearchCompatibilityHasTerm(terms []string, raw any) bool {
	token, ok := raw.(string)
	if !ok {
		return false
	}
	for _, candidate := range terms {
		if candidate == token {
			return true
		}
	}
	return false
}

// openSearchCompatibilityHasPrefix checks prefix-token membership, including
// the empty-prefix form used for field existence checks like name:*.
func openSearchCompatibilityHasPrefix(terms []string, raw any) bool {
	prefix, ok := raw.(string)
	if !ok {
		return false
	}
	for _, candidate := range terms {
		if strings.HasPrefix(candidate, prefix) {
			return true
		}
	}
	return false
}

// openSearchCompatibilityHasWildcard checks wildcard-token membership for the
// compat_terms wildcard clauses emitted by phrase and word-break queries.
func openSearchCompatibilityHasWildcard(terms []string, raw any) bool {
	pattern, ok := raw.(string)
	if !ok {
		return false
	}
	for _, candidate := range terms {
		if wildcardPatternMatches(pattern, candidate) {
			return true
		}
	}
	return false
}

// openSearchCompatibilityHasRange checks keyword range bounds over compat_terms
// so parser tests can prove memory and provider clauses agree.
func openSearchCompatibilityHasRange(terms []string, raw any) bool {
	bounds, ok := raw.(map[string]any)
	if !ok {
		return false
	}
	for _, candidate := range terms {
		if rangeTokenMatches(candidate, bounds) {
			return true
		}
	}
	return false
}

func rangeTokenMatches(candidate string, bounds map[string]any) bool {
	for op, rawBound := range bounds {
		bound, ok := rawBound.(string)
		if !ok {
			return false
		}
		cmp := strings.Compare(candidate, bound)
		switch op {
		case "gt":
			if cmp <= 0 {
				return false
			}
		case "gte":
			if cmp < 0 {
				return false
			}
		case "lt":
			if cmp >= 0 {
				return false
			}
		case "lte":
			if cmp > 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}
