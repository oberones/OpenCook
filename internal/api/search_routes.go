package api

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
)

func (s *server) handleSearchIndexes(w http.ResponseWriter, r *http.Request) {
	org, basePath, ok := s.resolveSearchRoute(w, r)
	if !ok {
		return
	}

	if r.Method != http.MethodGet || !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	requestor, ok := requestorFromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authn_context_missing",
			Message: "authenticated requestor missing from context",
		})
		return
	}

	indexes, err := s.deps.Search.Indexes(r.Context(), org)
	if err != nil {
		if !s.writeSearchError(w, err, "search index listing", "") {
			return
		}
		return
	}

	response := make(map[string]string)
	for _, indexName := range indexes {
		resource, found := searchIndexResource(indexName, org, s.deps.Bootstrap)
		if !found {
			continue
		}
		allowed, err := s.authorizeRequestor(r.Context(), requestor, authz.ActionRead, resource)
		if err != nil {
			s.logf("search index authz failure for %s: %v", indexName, err)
			writeJSON(w, http.StatusInternalServerError, apiError{
				Error:   "authz_failed",
				Message: "internal authorization error",
			})
			return
		}
		if !allowed {
			continue
		}
		response[indexName] = basePath + "/" + indexName
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *server) handleSearchQuery(w http.ResponseWriter, r *http.Request) {
	org, basePath, ok := s.resolveSearchRoute(w, r)
	if !ok {
		return
	}

	indexName := strings.TrimSpace(r.PathValue("index"))
	indexPath := basePath + "/" + indexName
	if !matchesCollectionPath(r.URL.Path, indexPath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	resource, found := searchIndexResource(indexName, org, s.deps.Bootstrap)
	if !found {
		writeSearchIndexNotFound(w, indexName)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, resource) {
		return
	}

	requestor, ok := requestorFromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authn_context_missing",
			Message: "authenticated requestor missing from context",
		})
		return
	}

	start, rows, ok := parseSearchPaging(w, r)
	if !ok {
		return
	}

	result, err := s.deps.Search.Search(r.Context(), search.Query{
		Organization: org,
		Index:        indexName,
		Q:            r.URL.Query().Get("q"),
	})
	if err != nil {
		if !s.writeSearchError(w, err, "search query "+indexName, indexName) {
			return
		}
		return
	}

	filtered, err := s.filterSearchDocuments(r.Context(), requestor, result.Documents)
	if err != nil {
		s.logf("search authz filter failure for %s: %v", indexName, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authz_failed",
			Message: "internal authorization error",
		})
		return
	}

	page := paginateSearchDocuments(filtered, start, rows)
	switch r.Method {
	case http.MethodGet:
		rowsPayload := make([]map[string]any, 0, len(page))
		for _, doc := range page {
			rowsPayload = append(rowsPayload, doc.Object)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"start": start,
			"total": len(filtered),
			"rows":  rowsPayload,
		})
	case http.MethodPost:
		selectors, ok := decodePartialSearchBody(w, r)
		if !ok {
			return
		}

		rowsPayload := make([]map[string]any, 0, len(page))
		for _, doc := range page {
			rowsPayload = append(rowsPayload, map[string]any{
				"url":  searchDocumentURL(doc, org, basePath),
				"data": applyPartialSearchSelectors(doc.Partial, selectors),
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"start": start,
			"total": len(filtered),
			"rows":  rowsPayload,
		})
	default:
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed for search route",
		})
	}
}

func (s *server) resolveSearchRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		if s.deps.Bootstrap != nil {
			if _, exists := s.deps.Bootstrap.GetOrganization(org); !exists {
				writeJSON(w, http.StatusNotFound, apiError{
					Error:   "not_found",
					Message: "organization not found",
				})
				return "", "", false
			}
		}
		return org, "/organizations/" + org + "/search", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/search", true
}

func searchIndexResource(indexName, org string, state *bootstrap.Service) (authz.Resource, bool) {
	switch indexName {
	case "client":
		return authz.Resource{Type: "container", Name: "clients", Organization: org}, true
	case "environment":
		return authz.Resource{Type: "container", Name: "environments", Organization: org}, true
	case "node":
		return authz.Resource{Type: "container", Name: "nodes", Organization: org}, true
	case "role":
		return authz.Resource{Type: "container", Name: "roles", Organization: org}, true
	default:
		if state != nil {
			if _, orgExists, bagExists := state.GetDataBag(org, indexName); orgExists && bagExists {
				return authz.Resource{Type: "data_bag", Name: indexName, Organization: org}, true
			}
		}
		return authz.Resource{}, false
	}
}

func parseSearchPaging(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	start, err := parseNonNegativeInt(r.URL.Query().Get("start"), 0)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_search_query",
			Message: "start must be a non-negative integer",
		})
		return 0, 0, false
	}
	rows, err := parseNonNegativeInt(r.URL.Query().Get("rows"), 0)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_search_query",
			Message: "rows must be a non-negative integer",
		})
		return 0, 0, false
	}
	return start, rows, true
}

func parseNonNegativeInt(raw string, fallback int) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, errors.New("invalid non-negative integer")
	}
	return value, nil
}

func (s *server) filterSearchDocuments(ctx context.Context, requestor authn.Principal, documents []search.Document) ([]search.Document, error) {
	filtered := make([]search.Document, 0, len(documents))
	for _, doc := range documents {
		allowed, err := s.authorizeRequestor(ctx, requestor, authz.ActionRead, doc.Resource)
		if err != nil {
			return nil, err
		}
		if !allowed {
			continue
		}
		filtered = append(filtered, doc)
	}
	return filtered, nil
}

func paginateSearchDocuments(documents []search.Document, start, rows int) []search.Document {
	if start >= len(documents) {
		return []search.Document{}
	}
	end := len(documents)
	if rows > 0 && start+rows < end {
		end = start + rows
	}
	return documents[start:end]
}

func decodePartialSearchBody(w http.ResponseWriter, r *http.Request) (map[string][]string, bool) {
	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return nil, false
	}

	selectors := make(map[string][]string, len(payload))
	for alias, value := range payload {
		switch typed := value.(type) {
		case []any:
			path := make([]string, 0, len(typed))
			for _, item := range typed {
				text, ok := item.(string)
				if !ok {
					writeJSON(w, http.StatusBadRequest, apiError{
						Error:   "invalid_partial_search",
						Message: "partial search paths must be arrays of strings",
					})
					return nil, false
				}
				path = append(path, text)
			}
			selectors[alias] = path
		default:
			writeJSON(w, http.StatusBadRequest, apiError{
				Error:   "invalid_partial_search",
				Message: "partial search request body must be an object of string arrays",
			})
			return nil, false
		}
	}
	return selectors, true
}

func (s *server) writeSearchError(w http.ResponseWriter, err error, operation, indexName string) bool {
	if err == nil {
		return true
	}

	switch {
	case errors.Is(err, search.ErrIndexNotFound):
		writeSearchIndexNotFound(w, indexName)
	case errors.Is(err, search.ErrUnavailable):
		writeJSON(w, http.StatusServiceUnavailable, apiError{
			Error:   "search_unavailable",
			Message: "search backend unavailable",
		})
	case errors.Is(err, search.ErrOrganizationNotFound), errors.Is(err, search.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
	default:
		s.logf("%s failure: %v", operation, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "search_failed",
			Message: "internal search compatibility error",
		})
	}
	return false
}

func writeSearchIndexNotFound(w http.ResponseWriter, indexName string) {
	writeDataBagMessages(w, http.StatusNotFound, "I don't know how to search for "+indexName+" data objects.")
}

func applyPartialSearchSelectors(document map[string]any, selectors map[string][]string) map[string]any {
	data := make(map[string]any, len(selectors))
	for alias, path := range selectors {
		data[alias] = partialSearchValue(document, path)
	}
	return data
}

func partialSearchValue(document map[string]any, path []string) any {
	if len(path) == 0 {
		return nil
	}

	var current any = document
	for _, segment := range path {
		node, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current, ok = node[segment]
		if !ok {
			return nil
		}
	}
	return current
}

func searchDocumentURL(doc search.Document, org, basePath string) string {
	switch doc.Index {
	case "client":
		if basePath == "/search" {
			return "/clients/" + doc.Name
		}
		return "/organizations/" + org + "/clients/" + doc.Name
	case "environment":
		if basePath == "/search" {
			return "/environments/" + doc.Name
		}
		return "/organizations/" + org + "/environments/" + doc.Name
	case "node":
		if basePath == "/search" {
			return "/nodes/" + doc.Name
		}
		return "/organizations/" + org + "/nodes/" + doc.Name
	case "role":
		if basePath == "/search" {
			return "/roles/" + doc.Name
		}
		return "/organizations/" + org + "/roles/" + doc.Name
	default:
		if basePath == "/search" {
			return "/data/" + doc.Index + "/" + doc.Name
		}
		return "/organizations/" + org + "/data/" + doc.Index + "/" + doc.Name
	}
}
