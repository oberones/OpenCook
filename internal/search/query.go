package search

import (
	"fmt"
	"strings"
)

const (
	openSearchCompatTermsField = "compat_terms"
	openSearchCompatAnyField   = "__any"
	openSearchCompatOrgField   = "__org"
	openSearchCompatIndexField = "__index"
)

type QueryPlan struct {
	root queryNode
	err  error
}

// CompileQuery parses the Chef search query string into the shared internal
// AST used by both memory search and OpenSearch request planning.
func CompileQuery(raw string) QueryPlan {
	query := strings.TrimSpace(raw)
	root, err := parseQuery(query)
	if err != nil {
		return QueryPlan{root: matchNoneNode{}, err: err}
	}
	return QueryPlan{root: root}
}

// Err returns a stable parser error that callers can map to Chef-facing search
// error shapes without exposing parser/provider internals.
func (p QueryPlan) Err() error {
	return p.err
}

// Matches evaluates the compiled query against an expanded search document.
func (p QueryPlan) Matches(doc Document) bool {
	if p.err != nil {
		return false
	}
	if p.root == nil {
		return true
	}
	return p.root.matches(doc.Fields)
}

// MatchesFields is a test/helper shortcut for evaluating already-expanded
// document fields without constructing a complete search document.
func (p QueryPlan) MatchesFields(fields map[string][]string) bool {
	return p.Matches(Document{Fields: fields})
}

// OpenSearchQueryBody returns the query-only request fragment for tests and
// request builders that need to inspect the generated provider clause.
func (p QueryPlan) OpenSearchQueryBody() map[string]any {
	return map[string]any{
		"query": p.OpenSearchQueryClause(),
	}
}

// OpenSearchQueryClause compiles the AST into the current compat_terms-based
// OpenSearch clause, keeping provider behavior aligned with memory search.
func (p QueryPlan) OpenSearchQueryClause() map[string]any {
	if p.err != nil {
		return openSearchMatchNoneClause()
	}
	if p.root == nil {
		return openSearchMatchAllClause()
	}
	return p.root.openSearchClause()
}

type queryNode interface {
	matches(map[string][]string) bool
	openSearchClause() map[string]any
}

type matchAllNode struct{}

// matches on matchAllNode intentionally accepts every expanded document.
func (matchAllNode) matches(map[string][]string) bool {
	return true
}

// openSearchClause on matchAllNode emits the provider match-all clause.
func (matchAllNode) openSearchClause() map[string]any {
	return openSearchMatchAllClause()
}

type matchNoneNode struct{}

// matches on matchNoneNode intentionally rejects every expanded document.
func (matchNoneNode) matches(map[string][]string) bool {
	return false
}

// openSearchClause on matchNoneNode emits the provider match-none clause.
func (matchNoneNode) openSearchClause() map[string]any {
	return openSearchMatchNoneClause()
}

type orNode struct {
	children []queryNode
}

// matches on orNode accepts a document when any child expression matches.
func (n orNode) matches(fields map[string][]string) bool {
	for _, child := range n.children {
		if child.matches(fields) {
			return true
		}
	}
	return false
}

// openSearchClause on orNode compiles children into should clauses, preserving
// the current OR semantics over compat_terms.
func (n orNode) openSearchClause() map[string]any {
	should := make([]any, 0, len(n.children))
	for _, child := range n.children {
		should = append(should, child.openSearchClause())
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               should,
			"minimum_should_match": 1,
		},
	}
}

type andNode struct {
	children []queryNode
	negated  []queryNode
}

// matches on andNode requires all positive children and rejects documents that
// match any negated child.
func (n andNode) matches(fields map[string][]string) bool {
	for _, child := range n.children {
		if !child.matches(fields) {
			return false
		}
	}
	for _, child := range n.negated {
		if child.matches(fields) {
			return false
		}
	}
	return len(n.children) > 0 || len(n.negated) > 0
}

// openSearchClause on andNode compiles positive terms as must clauses and
// negated terms as must_not clauses.
func (n andNode) openSearchClause() map[string]any {
	must := make([]any, 0, len(n.children))
	for _, child := range n.children {
		must = append(must, child.openSearchClause())
	}
	if len(must) == 0 && len(n.negated) > 0 {
		must = append(must, openSearchMatchAllClause())
	}

	mustNot := make([]any, 0, len(n.negated))
	for _, child := range n.negated {
		mustNot = append(mustNot, child.openSearchClause())
	}
	if len(must) == 0 && len(mustNot) == 0 {
		return openSearchMatchNoneClause()
	}

	boolQuery := make(map[string]any, 2)
	if len(must) > 0 {
		boolQuery["must"] = must
	}
	if len(mustNot) > 0 {
		boolQuery["must_not"] = mustNot
	}
	return map[string]any{"bool": boolQuery}
}

type termNode struct {
	field        string
	value        string
	any          bool
	prefix       bool
	pattern      bool
	fieldPattern bool
}

type rangeNode struct {
	field        string
	start        string
	end          string
	includeStart bool
	includeEnd   bool
	openStart    bool
	openEnd      bool
}

// matches on termNode evaluates exact, prefix, any-field, and field-existence
// terms against the flattened Chef search field map.
func (n termNode) matches(fields map[string][]string) bool {
	if n.any {
		return matchesAnyFieldValue(fields, n.value, n.prefix, n.pattern)
	}
	if n.fieldPattern {
		return matchesFieldPatternValue(fields, n.field, n.value, n.prefix, n.pattern)
	}
	candidates := fields[n.field]
	if len(candidates) == 0 {
		return false
	}
	return matchesCandidates(candidates, n.value, n.prefix, n.pattern)
}

// openSearchClause on termNode emits the equivalent compat_terms exact or
// prefix query used by the active OpenSearch path.
func (n termNode) openSearchClause() map[string]any {
	field := n.field
	if n.any {
		field = openSearchCompatAnyField
	}
	if n.pattern || n.fieldPattern {
		value := n.value
		if n.prefix {
			value += "*"
		}
		return openSearchCompatWildcardClause(field, value)
	}
	if n.prefix {
		return openSearchCompatPrefixClause(field, n.value)
	}
	return openSearchCompatTermClause(field, n.value)
}

// matches on rangeNode evaluates Chef field ranges as lexicographic keyword
// ranges over expanded search values, matching the compat_terms provider path.
func (n rangeNode) matches(fields map[string][]string) bool {
	candidates := fields[n.field]
	if len(candidates) == 0 {
		return false
	}
	if n.openStart && n.openEnd {
		return true
	}
	for _, candidate := range candidates {
		if rangeValueMatches(candidate, n.start, n.end, n.includeStart, n.includeEnd, n.openStart, n.openEnd) {
			return true
		}
	}
	return false
}

// openSearchClause on rangeNode emits a keyword range against compat_terms so
// active OpenSearch and memory search share the same lexicographic semantics.
func (n rangeNode) openSearchClause() map[string]any {
	if n.openStart && n.openEnd {
		return openSearchCompatPrefixClause(n.field, "")
	}
	return openSearchCompatRangeClause(n.field, n.start, n.end, n.includeStart, n.includeEnd, n.openStart, n.openEnd)
}

// parseQuery turns the supported Chef/Lucene boolean subset into an AST with
// pinned precedence: parentheses, unary NOT/- operators, AND, then OR.
func parseQuery(query string) (queryNode, error) {
	if query == "" || query == "*:*" {
		return matchAllNode{}, nil
	}
	tokens, err := tokenizeQuery(query)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return matchAllNode{}, nil
	}

	parser := queryParser{tokens: tokens}
	node, err := parser.parseOr()
	if err != nil {
		return nil, err
	}
	if parser.peek().kind != queryTokenEOF {
		return nil, fmt.Errorf("%w: unexpected token %q", ErrInvalidQuery, parser.peek().value)
	}
	return node, nil
}

// parseAndExpression preserves the older test seam for AND-only fragments,
// including empty-fragment falsehood, while sharing the full parser for real
// expressions.
func parseAndExpression(expression string) (queryNode, error) {
	if strings.TrimSpace(expression) == "" {
		return matchNoneNode{}, nil
	}
	return parseQuery(expression)
}

// parseTerm normalizes one supported fielded or unqualified term into a leaf
// AST node, including phrase, escaping, wildcard, and existence semantics.
func parseTerm(raw string) (queryNode, error) {
	field := openSearchCompatAnyField
	value := raw
	anyField := true
	fieldPattern := false
	if rawField, rawValue, ok := splitFieldTerm(raw); ok {
		component, err := normalizeQueryComponent(strings.TrimSpace(rawField), false)
		if err != nil {
			return nil, err
		}
		field = component.text
		fieldPattern = component.wildcard
		value = rawValue
		anyField = false
	}
	if isRangeLike(value) {
		if anyField || fieldPattern {
			return nil, fmt.Errorf("%w: range queries require a concrete field", ErrInvalidQuery)
		}
		bounds, err := parseRangeValue(value)
		if err != nil {
			return nil, err
		}
		return rangeNode{
			field:        field,
			start:        bounds.start,
			end:          bounds.end,
			includeStart: bounds.includeStart,
			includeEnd:   bounds.includeEnd,
			openStart:    bounds.openStart,
			openEnd:      bounds.openEnd,
		}, nil
	}

	valueComponent, err := normalizeQueryComponent(strings.TrimSpace(value), true)
	if err != nil {
		return nil, err
	}
	value = valueComponent.text
	if field == "" || value == "" {
		return matchNoneNode{}, nil
	}
	if field == "*" {
		if value == "*" && valueComponent.wildcard {
			return matchAllNode{}, nil
		}
		field = openSearchCompatAnyField
		anyField = true
		fieldPattern = false
	}

	prefix := false
	pattern := valueComponent.wildcard
	if pattern && value == "*" {
		prefix = true
		value = ""
		pattern = false
	} else if pattern && isSimpleTrailingWildcard(value) {
		prefix = true
		value = strings.TrimSuffix(valueComponent.text, "*")
		pattern = false
	}
	return termNode{field: field, value: value, any: anyField, prefix: prefix, pattern: pattern, fieldPattern: fieldPattern}, nil
}

type queryTokenKind string

const (
	queryTokenTerm   queryTokenKind = "term"
	queryTokenAnd    queryTokenKind = "and"
	queryTokenOr     queryTokenKind = "or"
	queryTokenNot    queryTokenKind = "not"
	queryTokenLParen queryTokenKind = "lparen"
	queryTokenRParen queryTokenKind = "rparen"
	queryTokenQuoted queryTokenKind = "quoted"
	queryTokenEOF    queryTokenKind = "eof"
)

type queryToken struct {
	kind  queryTokenKind
	value string
}

// tokenizeQuery splits a Chef search query into parser tokens while respecting
// escaped characters and quoted phrases.
func tokenizeQuery(query string) ([]queryToken, error) {
	tokens := make([]queryToken, 0)
	for i := 0; i < len(query); {
		switch query[i] {
		case ' ', '\t', '\n', '\r':
			i++
			continue
		case '(':
			tokens = append(tokens, queryToken{kind: queryTokenLParen, value: "("})
			i++
			continue
		case ')':
			tokens = append(tokens, queryToken{kind: queryTokenRParen, value: ")"})
			i++
			continue
		}

		start := i
		inQuote := false
		inRange := false
		rangeClose := byte(0)
		for i < len(query) {
			switch query[i] {
			case '\\':
				if i+1 < len(query) {
					i += 2
					continue
				}
				tokens = append(tokens, queryToken{kind: queryTokenTerm, value: query[start:i]})
				return tokens, fmt.Errorf("%w: trailing escape", ErrInvalidQuery)
			case '"':
				inQuote = !inQuote
			case '[', '{':
				if !inQuote && !inRange {
					inRange = true
					if query[i] == '[' {
						rangeClose = ']'
					} else {
						rangeClose = '}'
					}
				}
			case ']', '}':
				if inRange {
					if query[i] != rangeClose {
						tokens = append(tokens, queryToken{kind: queryTokenTerm, value: query[start : i+1]})
						return tokens, fmt.Errorf("%w: mismatched range delimiter", ErrInvalidQuery)
					}
					inRange = false
					rangeClose = 0
				}
			case ' ', '\t', '\n', '\r', '(', ')':
				if inQuote || inRange {
					i++
					continue
				}
				goto emitToken
			}
			i++
		}
		if inQuote {
			tokens = append(tokens, queryToken{kind: queryTokenQuoted, value: query[start:i]})
			return tokens, fmt.Errorf("%w: unterminated quote", ErrInvalidQuery)
		}
		if inRange {
			tokens = append(tokens, queryToken{kind: queryTokenTerm, value: query[start:i]})
			return tokens, fmt.Errorf("%w: unterminated range", ErrInvalidQuery)
		}

	emitToken:
		part := query[start:i]
		if part == "" {
			continue
		}
		switch part {
		case "AND":
			tokens = append(tokens, queryToken{kind: queryTokenAnd, value: part})
		case "OR":
			tokens = append(tokens, queryToken{kind: queryTokenOr, value: part})
		case "NOT":
			tokens = append(tokens, queryToken{kind: queryTokenNot, value: part})
		default:
			if part == "-" {
				tokens = append(tokens, queryToken{kind: queryTokenNot, value: part})
				continue
			}
			if strings.HasPrefix(part, "-") && len(part) > 1 {
				tokens = append(tokens, queryToken{kind: queryTokenNot, value: "-"})
				part = strings.TrimPrefix(part, "-")
			}
			tokens = append(tokens, queryToken{kind: queryTokenTerm, value: part})
		}
	}
	return tokens, nil
}

type queryParser struct {
	tokens []queryToken
	pos    int
}

// parseOr parses disjunctions after higher-precedence AND expressions have
// been collected.
func (p *queryParser) parseOr() (queryNode, error) {
	children := make([]queryNode, 0, 2)
	first, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	children = append(children, first)

	for p.accept(queryTokenOr) {
		child, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		children = append(children, child)
	}
	if len(children) == 1 {
		return children[0], nil
	}
	return orNode{children: children}, nil
}

// parseAnd parses required and negated terms until an OR, right parenthesis,
// or end-of-query boundary is reached.
func (p *queryParser) parseAnd() (queryNode, error) {
	positive := make([]queryNode, 0)
	negated := make([]queryNode, 0)
	processed := false

	for {
		switch p.peek().kind {
		case queryTokenAnd:
			p.next()
			continue
		case queryTokenOr, queryTokenRParen, queryTokenEOF:
			if !processed {
				return matchNoneNode{}, nil
			}
			return andNode{children: positive, negated: negated}, nil
		}

		isNegated, child, consumed, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if !consumed {
			if !processed {
				return matchNoneNode{}, nil
			}
			return andNode{children: positive, negated: negated}, nil
		}

		processed = true
		if isNegated {
			negated = append(negated, child)
		} else {
			positive = append(positive, child)
		}
		if !p.accept(queryTokenAnd) {
			return andNode{children: positive, negated: negated}, nil
		}
	}
}

// parseUnary handles leading NOT and unary '-' operators before parsing the
// primary term or parenthesized subexpression they modify.
func (p *queryParser) parseUnary() (bool, queryNode, bool, error) {
	negated := false
	for p.peek().kind == queryTokenNot {
		p.next()
		negated = !negated
	}

	child, consumed, err := p.parsePrimary()
	return negated, child, consumed, err
}

// parsePrimary parses one term or parenthesized subexpression.
func (p *queryParser) parsePrimary() (queryNode, bool, error) {
	switch p.peek().kind {
	case queryTokenTerm:
		token := p.next()
		node, err := parseTerm(token.value)
		return node, true, err
	case queryTokenLParen:
		p.next()
		node, err := p.parseOr()
		if err != nil {
			return nil, true, err
		}
		if !p.accept(queryTokenRParen) {
			return nil, true, fmt.Errorf("%w: missing closing parenthesis", ErrInvalidQuery)
		}
		return node, true, nil
	default:
		return matchNoneNode{}, false, nil
	}
}

// accept consumes the next token when it has the requested kind.
func (p *queryParser) accept(kind queryTokenKind) bool {
	if p.peek().kind != kind {
		return false
	}
	p.next()
	return true
}

// next consumes and returns one token, returning EOF after the token stream is
// exhausted.
func (p *queryParser) next() queryToken {
	token := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return token
}

// peek returns the current token without consuming it.
func (p *queryParser) peek() queryToken {
	if p.pos >= len(p.tokens) {
		return queryToken{kind: queryTokenEOF}
	}
	return p.tokens[p.pos]
}

func openSearchCompatTermClause(field, value string) map[string]any {
	return map[string]any{
		"term": map[string]any{
			openSearchCompatTermsField: openSearchCompatToken(field, value),
		},
	}
}

func openSearchCompatPrefixClause(field, prefix string) map[string]any {
	return map[string]any{
		"prefix": map[string]any{
			openSearchCompatTermsField: openSearchCompatToken(field, prefix),
		},
	}
}

// openSearchCompatRangeClause emits inclusive or exclusive keyword bounds for
// one concrete expanded field encoded in the compat_terms field.
func openSearchCompatRangeClause(field, start, end string, includeStart, includeEnd, openStart, openEnd bool) map[string]any {
	bounds := make(map[string]any, 2)
	if openStart {
		bounds["gte"] = openSearchCompatToken(field, "")
	} else if includeStart {
		bounds["gte"] = openSearchCompatToken(field, start)
	} else {
		bounds["gt"] = openSearchCompatToken(field, start)
	}
	if openEnd {
		bounds["lte"] = openSearchCompatToken(field, "\ufff0")
	} else if includeEnd {
		bounds["lte"] = openSearchCompatToken(field, end)
	} else {
		bounds["lt"] = openSearchCompatToken(field, end)
	}
	return map[string]any{
		"range": map[string]any{
			openSearchCompatTermsField: bounds,
		},
	}
}

// openSearchCompatWildcardClause emits a compat_terms wildcard query for
// leading/infix value wildcards and wildcard field-name patterns.
func openSearchCompatWildcardClause(fieldPattern, valuePattern string) map[string]any {
	return map[string]any{
		"wildcard": map[string]any{
			openSearchCompatTermsField: openSearchCompatToken(fieldPattern, valuePattern),
		},
	}
}

func openSearchCompatToken(field, value string) string {
	return field + "=" + value
}

func openSearchMatchAllClause() map[string]any {
	return map[string]any{"match_all": map[string]any{}}
}

func openSearchMatchNoneClause() map[string]any {
	return map[string]any{"match_none": map[string]any{}}
}

// matchesQuery preserves the older test seam while routing evaluation through
// the shared compiled query plan.
func matchesQuery(doc Document, query string) bool {
	return CompileQuery(query).Matches(doc)
}

// matchesAndExpression preserves the older test seam for an AND-only fragment
// while using the same parser nodes as full query evaluation.
func matchesAndExpression(fields map[string][]string, expression string) bool {
	node, err := parseAndExpression(expression)
	if err != nil {
		return false
	}
	return node.matches(fields)
}

// matchesTerm preserves the older test seam for one term while sharing the new
// term parser and matcher.
func matchesTerm(fields map[string][]string, term string) bool {
	node, err := parseTerm(term)
	if err != nil {
		return false
	}
	return node.matches(fields)
}

// matchesCandidates checks a field's indexed values for exact, prefix, or
// wildcard-pattern matches, mirroring the compat_terms OpenSearch query shape.
func matchesCandidates(candidates []string, value string, prefix bool, pattern bool) bool {
	if prefix && value == "" {
		return len(candidates) > 0
	}
	if value == "" {
		return false
	}
	for _, candidate := range candidates {
		switch {
		case pattern:
			if wildcardPatternMatches(value, candidate) {
				return true
			}
		case prefix:
			if strings.HasPrefix(candidate, value) {
				return true
			}
		case candidate == value:
			return true
		}
	}
	return false
}

// matchesAnyField preserves the older test seam for unqualified terms.
func matchesAnyField(fields map[string][]string, value string) bool {
	component, err := normalizeQueryComponent(strings.TrimSpace(value), true)
	if err != nil {
		return false
	}
	prefix := component.wildcard && isSimpleTrailingWildcard(component.text)
	pattern := component.wildcard && !prefix
	value = component.text
	if prefix {
		value = strings.TrimSuffix(value, "*")
	}
	return matchesAnyFieldValue(fields, value, prefix, pattern)
}

// matchesAnyFieldValue scans every expanded field for an exact, prefix, or
// wildcard-pattern match.
func matchesAnyFieldValue(fields map[string][]string, value string, prefix bool, pattern bool) bool {
	for _, candidates := range fields {
		if matchesCandidates(candidates, value, prefix, pattern) {
			return true
		}
	}

	return false
}

// matchesFieldPatternValue scans fields whose names match a wildcard field
// pattern, then applies the value matcher to those fields' candidate values.
func matchesFieldPatternValue(fields map[string][]string, fieldPattern, value string, prefix bool, pattern bool) bool {
	for field, candidates := range fields {
		if !wildcardPatternMatches(fieldPattern, field) {
			continue
		}
		if matchesCandidates(candidates, value, prefix, pattern) {
			return true
		}
	}
	return false
}

// rangeValueMatches applies inclusive/exclusive range endpoints to one
// candidate value using Chef-compatible keyword ordering.
func rangeValueMatches(candidate, start, end string, includeStart, includeEnd, openStart, openEnd bool) bool {
	if !openStart {
		cmp := strings.Compare(candidate, start)
		if cmp < 0 || (cmp == 0 && !includeStart) {
			return false
		}
	}
	if !openEnd {
		cmp := strings.Compare(candidate, end)
		if cmp > 0 || (cmp == 0 && !includeEnd) {
			return false
		}
	}
	return true
}

// wildcardPatternMatches evaluates the Lucene-style `*` and `?` wildcard
// subset used by Chef search word-break queries against keyword tokens.
func wildcardPatternMatches(pattern, candidate string) bool {
	matched := make([][]bool, len(pattern)+1)
	seen := make([][]bool, len(pattern)+1)
	for i := range matched {
		matched[i] = make([]bool, len(candidate)+1)
		seen[i] = make([]bool, len(candidate)+1)
	}
	var match func(int, int) bool
	match = func(pi, ci int) bool {
		if seen[pi][ci] {
			return matched[pi][ci]
		}
		seen[pi][ci] = true
		switch {
		case pi == len(pattern):
			matched[pi][ci] = ci == len(candidate)
		case pattern[pi] == '*':
			matched[pi][ci] = match(pi+1, ci) || (ci < len(candidate) && match(pi, ci+1))
		case pattern[pi] == '?':
			matched[pi][ci] = ci < len(candidate) && match(pi+1, ci+1)
		default:
			matched[pi][ci] = ci < len(candidate) && pattern[pi] == candidate[ci] && match(pi+1, ci+1)
		}
		return matched[pi][ci]
	}
	return match(0, 0)
}

// splitFieldTerm finds the first unescaped, unquoted colon so escaped colons in
// values remain literal instead of accidentally creating fielded queries.
func splitFieldTerm(raw string) (string, string, bool) {
	inQuote := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			if i+1 < len(raw) {
				i++
			}
		case '"':
			inQuote = !inQuote
		case ':':
			if !inQuote {
				return raw[:i], raw[i+1:], true
			}
		}
	}
	return "", raw, false
}

type rangeBounds struct {
	start        string
	end          string
	includeStart bool
	includeEnd   bool
	openStart    bool
	openEnd      bool
}

// parseRangeValue parses the Chef field range forms `[a TO b]` and `{a TO b}`,
// including `*` open bounds, while rejecting malformed provider-dependent input.
func parseRangeValue(raw string) (rangeBounds, error) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 5 {
		return rangeBounds{}, fmt.Errorf("%w: malformed range", ErrInvalidQuery)
	}
	open := raw[0]
	close := raw[len(raw)-1]
	if (open != '[' && open != '{') || (open == '[' && close != ']') || (open == '{' && close != '}') {
		return rangeBounds{}, fmt.Errorf("%w: malformed range delimiters", ErrInvalidQuery)
	}
	inner := strings.TrimSpace(raw[1 : len(raw)-1])
	startRaw, endRaw, ok := strings.Cut(inner, " TO ")
	if !ok {
		return rangeBounds{}, fmt.Errorf("%w: malformed range separator", ErrInvalidQuery)
	}
	startComponent, err := normalizeQueryComponent(strings.TrimSpace(startRaw), false)
	if err != nil {
		return rangeBounds{}, err
	}
	endComponent, err := normalizeQueryComponent(strings.TrimSpace(endRaw), false)
	if err != nil {
		return rangeBounds{}, err
	}
	if startComponent.text == "" || endComponent.text == "" {
		return rangeBounds{}, fmt.Errorf("%w: empty range bound", ErrInvalidQuery)
	}
	if (startComponent.wildcard && startComponent.text != "*") || (endComponent.wildcard && endComponent.text != "*") {
		return rangeBounds{}, fmt.Errorf("%w: wildcard range bounds are unsupported", ErrInvalidQuery)
	}
	return rangeBounds{
		start:        startComponent.text,
		end:          endComponent.text,
		includeStart: open == '[',
		includeEnd:   close == ']',
		openStart:    startComponent.text == "*",
		openEnd:      endComponent.text == "*",
	}, nil
}

// isRangeLike identifies values that should be parsed as Chef field ranges
// before generic wildcard/phrase normalization runs.
func isRangeLike(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	return value[0] == '[' || value[0] == '{'
}

type queryComponent struct {
	text     string
	wildcard bool
}

// normalizeQueryComponent removes Chef/Lucene escaping and phrase quotes while
// preserving whether unescaped wildcard characters should act as wildcards.
func normalizeQueryComponent(raw string, allowQuotes bool) (queryComponent, error) {
	var out strings.Builder
	inQuote := false
	wildcard := false
	for i := 0; i < len(raw); i++ {
		switch raw[i] {
		case '\\':
			if i+1 >= len(raw) {
				return queryComponent{}, fmt.Errorf("%w: trailing escape", ErrInvalidQuery)
			}
			i++
			out.WriteByte(raw[i])
		case '"':
			if !allowQuotes {
				return queryComponent{}, fmt.Errorf("%w: quoted field names are not supported", ErrInvalidQuery)
			}
			inQuote = !inQuote
		case '*', '?':
			wildcard = true
			out.WriteByte(raw[i])
		default:
			out.WriteByte(raw[i])
		}
	}
	if inQuote {
		return queryComponent{}, fmt.Errorf("%w: unterminated quote", ErrInvalidQuery)
	}
	return queryComponent{text: out.String(), wildcard: wildcard}, nil
}

// isSimpleTrailingWildcard preserves the existing efficient prefix-query shape
// for `field:value*` while letting broader wildcard forms use wildcard queries.
func isSimpleTrailingWildcard(value string) bool {
	if !strings.HasSuffix(value, "*") {
		return false
	}
	for i := 0; i < len(value)-1; i++ {
		if value[i] == '*' || value[i] == '?' {
			return false
		}
	}
	return true
}
