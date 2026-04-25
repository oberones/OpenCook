package search

import "strings"

const (
	openSearchCompatTermsField = "compat_terms"
	openSearchCompatAnyField   = "__any"
	openSearchCompatOrgField   = "__org"
	openSearchCompatIndexField = "__index"
)

type QueryPlan struct {
	raw string
}

func CompileQuery(raw string) QueryPlan {
	return QueryPlan{raw: strings.TrimSpace(raw)}
}

func (p QueryPlan) Matches(doc Document) bool {
	return matchesQuery(doc, p.raw)
}

func (p QueryPlan) MatchesFields(fields map[string][]string) bool {
	return p.Matches(Document{Fields: fields})
}

func (p QueryPlan) OpenSearchQueryBody() map[string]any {
	return map[string]any{
		"query": p.OpenSearchQueryClause(),
	}
}

func (p QueryPlan) OpenSearchQueryString() string {
	if p.raw == "" {
		return "*:*"
	}
	return p.raw
}

func (p QueryPlan) OpenSearchQueryClause() map[string]any {
	query := strings.TrimSpace(p.raw)
	if query == "" || query == "*:*" {
		return openSearchMatchAllClause()
	}

	orParts := strings.Split(query, " OR ")
	if len(orParts) == 1 {
		return openSearchAndExpressionClause(orParts[0])
	}

	should := make([]any, 0, len(orParts))
	for _, part := range orParts {
		should = append(should, openSearchAndExpressionClause(part))
	}
	return map[string]any{
		"bool": map[string]any{
			"should":               should,
			"minimum_should_match": 1,
		},
	}
}

func openSearchAndExpressionClause(expression string) map[string]any {
	if strings.TrimSpace(expression) == "" {
		return openSearchMatchNoneClause()
	}

	must := make([]any, 0)
	mustNot := make([]any, 0)
	processed := false
	for _, rawTerm := range strings.Split(expression, " AND ") {
		rawTerm = strings.TrimSpace(rawTerm)
		if rawTerm == "" {
			continue
		}

		negated := false
		switch {
		case strings.HasPrefix(rawTerm, "NOT "):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "NOT "))
		case strings.HasPrefix(rawTerm, "-"):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "-"))
		}
		if rawTerm == "" {
			continue
		}

		processed = true
		clause := openSearchTermClause(rawTerm)
		if negated {
			mustNot = append(mustNot, clause)
			continue
		}
		must = append(must, clause)
	}
	if !processed {
		return openSearchMatchNoneClause()
	}
	if len(must) == 0 && len(mustNot) == 0 {
		return openSearchMatchNoneClause()
	}
	if len(must) == 0 {
		must = append(must, openSearchMatchAllClause())
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

func openSearchTermClause(term string) map[string]any {
	parts := strings.SplitN(term, ":", 2)
	field := openSearchCompatAnyField
	value := term
	if len(parts) == 2 {
		field = unescapeQueryToken(strings.TrimSpace(parts[0]))
		value = parts[1]
	}

	field = strings.TrimSpace(field)
	value = unescapeQueryToken(strings.TrimSpace(value))
	if field == "" || value == "" {
		return openSearchMatchNoneClause()
	}
	if value == "*" {
		return openSearchCompatPrefixClause(field, "")
	}

	wildcard := strings.HasSuffix(value, "*")
	if wildcard {
		value = strings.TrimSuffix(value, "*")
	}
	if wildcard {
		return openSearchCompatPrefixClause(field, value)
	}
	return openSearchCompatTermClause(field, value)
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

func openSearchCompatToken(field, value string) string {
	return field + "=" + value
}

func openSearchMatchAllClause() map[string]any {
	return map[string]any{"match_all": map[string]any{}}
}

func openSearchMatchNoneClause() map[string]any {
	return map[string]any{"match_none": map[string]any{}}
}

func matchesQuery(doc Document, query string) bool {
	query = strings.TrimSpace(query)
	if query == "" || query == "*:*" {
		return true
	}

	for _, clause := range strings.Split(query, " OR ") {
		if matchesAndExpression(doc.Fields, clause) {
			return true
		}
	}
	return false
}

func matchesAndExpression(fields map[string][]string, expression string) bool {
	if strings.TrimSpace(expression) == "" {
		return false
	}

	terms := strings.Split(expression, " AND ")
	positiveSeen := false
	processedTerm := false
	for _, rawTerm := range terms {
		rawTerm = strings.TrimSpace(rawTerm)
		if rawTerm == "" {
			continue
		}

		negated := false
		switch {
		case strings.HasPrefix(rawTerm, "NOT "):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "NOT "))
		case strings.HasPrefix(rawTerm, "-"):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "-"))
		}
		if rawTerm == "" {
			continue
		}

		processedTerm = true
		matched := matchesTerm(fields, rawTerm)
		if negated {
			if matched {
				return false
			}
			continue
		}

		positiveSeen = true
		if !matched {
			return false
		}
	}

	return positiveSeen || processedTerm
}

func matchesTerm(fields map[string][]string, term string) bool {
	parts := strings.SplitN(term, ":", 2)
	if len(parts) != 2 {
		return matchesAnyField(fields, unescapeQueryToken(strings.TrimSpace(term)))
	}

	field := unescapeQueryToken(strings.TrimSpace(parts[0]))
	value := unescapeQueryToken(strings.TrimSpace(parts[1]))
	candidates := fields[field]
	if len(candidates) == 0 {
		return false
	}
	if value == "*" {
		return true
	}

	wildcard := strings.HasSuffix(value, "*")
	if wildcard {
		value = strings.TrimSuffix(value, "*")
	}

	for _, candidate := range candidates {
		if wildcard {
			if strings.HasPrefix(candidate, value) {
				return true
			}
			continue
		}
		if candidate == value {
			return true
		}
	}
	return false
}

func matchesAnyField(fields map[string][]string, value string) bool {
	if value == "" {
		return false
	}

	wildcard := strings.HasSuffix(value, "*")
	if wildcard {
		value = strings.TrimSuffix(value, "*")
	}

	for _, candidates := range fields {
		for _, candidate := range candidates {
			if wildcard {
				if strings.HasPrefix(candidate, value) {
					return true
				}
				continue
			}
			if candidate == value {
				return true
			}
		}
	}

	return false
}

func unescapeQueryToken(value string) string {
	replacer := strings.NewReplacer(`\:`, ":", `\[`, "[", `\]`, "]", `\@`, "@", `\/`, "/")
	return replacer.Replace(value)
}
