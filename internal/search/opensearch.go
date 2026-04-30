package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

const (
	openSearchChefIndex      = "chef"
	openSearchSearchPageSize = 1000
	openSearchMappingVersion = 1
	openSearchMappingMetaKey = "opencook_mapping_version"
)

type openSearchIndexDescriptor struct {
	Name           string
	MappingVersion int
}

var openSearchChefIndexDescriptor = openSearchIndexDescriptor{
	Name:           openSearchChefIndex,
	MappingVersion: openSearchMappingVersion,
}

var errOpenSearchDeleteByQueryUnsupported = errors.New("opensearch delete-by-query unsupported")

type OpenSearchTransport interface {
	Do(*http.Request) (*http.Response, error)
}

type OpenSearchClient struct {
	endpoint          *url.URL
	transport         OpenSearchTransport
	providerInfoMu    sync.RWMutex
	providerInfo      OpenSearchProviderInfo
	providerInfoKnown bool
}

type OpenSearchOption func(*openSearchOptions)

type openSearchOptions struct {
	transport OpenSearchTransport
}

func WithOpenSearchTransport(transport OpenSearchTransport) OpenSearchOption {
	return func(opts *openSearchOptions) {
		opts.transport = transport
	}
}

func ValidateOpenSearchEndpoint(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	_, err := parseOpenSearchEndpoint(raw)
	return err
}

func NewOpenSearchClient(raw string, opts ...OpenSearchOption) (*OpenSearchClient, error) {
	endpoint, err := parseOpenSearchEndpoint(raw)
	if err != nil {
		return nil, err
	}

	options := openSearchOptions{transport: http.DefaultClient}
	for _, opt := range opts {
		opt(&options)
	}
	if options.transport == nil {
		options.transport = http.DefaultClient
	}

	return &OpenSearchClient{
		endpoint:  endpoint,
		transport: options.transport,
	}, nil
}

func (c *OpenSearchClient) Status() Status {
	info, ok := c.ProviderInfo()
	return OpenSearchProviderStatus(info, ok)
}

// ProviderInfo returns the last successful provider discovery snapshot.
// Callers get a copy so later discovery refreshes cannot mutate their view.
func (c *OpenSearchClient) ProviderInfo() (OpenSearchProviderInfo, bool) {
	if c == nil {
		return OpenSearchProviderInfo{}, false
	}
	c.providerInfoMu.RLock()
	defer c.providerInfoMu.RUnlock()
	return c.providerInfo, c.providerInfoKnown
}

func OpenSearchActiveStatus() Status {
	return OpenSearchProviderStatus(OpenSearchProviderInfo{}, false)
}

// OpenSearchProviderStatus preserves the stable status payload shape while
// making discovered provider identity and capability mode visible in the
// existing human-readable message.
func OpenSearchProviderStatus(info OpenSearchProviderInfo, known bool) Status {
	return Status{
		Backend:    "opensearch",
		Configured: true,
		Message:    openSearchProviderStatusMessage(info, known),
	}
}

func OpenSearchUnavailableStatus() Status {
	return Status{
		Backend:    "opensearch",
		Configured: true,
		Message:    "OpenSearch is configured but unavailable; search routes cannot reach the provider",
	}
}

func (c *OpenSearchClient) Ping(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodGet, "/", nil, nil, "")
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return classifyOpenSearchResponse(resp, http.StatusOK)
}

func (c *OpenSearchClient) EnsureChefIndex(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodHead, "/"+openSearchChefIndex, nil, nil, "")
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	closeResponseBody(resp)

	switch resp.StatusCode {
	case http.StatusOK:
		return c.ensureChefIndexMapping(ctx)
	case http.StatusNotFound:
	default:
		return classifyOpenSearchResponse(resp, http.StatusOK)
	}

	return c.createChefIndex(ctx)
}

func (c *OpenSearchClient) createChefIndex(ctx context.Context) error {
	resp, err := c.doJSONResponse(ctx, http.MethodPut, "/"+openSearchChefIndexDescriptor.Name, nil, openSearchChefIndexDescriptor.CreateBody())
	if err != nil {
		return err
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		closeResponseBody(resp)
		return nil
	case http.StatusBadRequest, http.StatusConflict:
		closeResponseBody(resp)
		return c.ensureChefIndexMapping(ctx)
	default:
		defer closeResponseBody(resp)
		return classifyOpenSearchResponse(resp, http.StatusOK, http.StatusCreated)
	}
}

func (d openSearchIndexDescriptor) CreateBody() map[string]any {
	return map[string]any{
		"settings": map[string]any{
			"index": map[string]any{
				"number_of_shards":     1,
				"auto_expand_replicas": "0-1",
			},
		},
		"mappings": d.Mapping(),
	}
}

func (c *OpenSearchClient) ensureChefIndexMapping(ctx context.Context) error {
	mapping, err := c.getChefIndexMapping(ctx)
	if err != nil {
		return err
	}
	if openSearchChefMappingCurrent(mapping) {
		return nil
	}
	return c.doJSON(ctx, http.MethodPut, "/"+openSearchChefIndexDescriptor.Name+"/_mapping", nil, openSearchChefIndexDescriptor.Mapping(), http.StatusOK)
}

func (c *OpenSearchClient) getChefIndexMapping(ctx context.Context) (map[string]any, error) {
	req, err := c.newRequest(ctx, http.MethodGet, "/"+openSearchChefIndexDescriptor.Name+"/_mapping", nil, nil, "")
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchResponse(resp, http.StatusOK); err != nil {
		return nil, err
	}

	var payload map[string]any
	if err := decodeOpenSearchJSON(resp.Body, &payload, "mapping response"); err != nil {
		return nil, err
	}
	mapping, ok := openSearchChefMappingFromResponse(payload)
	if !ok {
		return nil, fmt.Errorf("%w: malformed mapping response", ErrUnavailable)
	}
	return mapping, nil
}

func (c *OpenSearchClient) BulkUpsert(ctx context.Context, docs []Document) error {
	if len(docs) == 0 {
		return nil
	}

	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	for _, doc := range docs {
		action := map[string]any{
			"index": map[string]any{
				"_index": openSearchChefIndex,
				"_id":    OpenSearchDocumentID(doc),
			},
		}
		if err := encoder.Encode(action); err != nil {
			return fmt.Errorf("%w: encode bulk action", ErrRejected)
		}
		if err := encoder.Encode(openSearchDocumentSource(doc)); err != nil {
			return fmt.Errorf("%w: encode bulk document", ErrRejected)
		}
	}

	req, err := c.newRequest(ctx, http.MethodPost, "/_bulk", nil, &body, "application/x-ndjson")
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchResponse(resp, http.StatusOK, http.StatusCreated); err != nil {
		return err
	}

	var payload openSearchBulkResponse
	if err := decodeOpenSearchJSON(resp.Body, &payload, "bulk response"); err != nil {
		return err
	}
	return classifyOpenSearchBulkResponse(payload)
}

type openSearchBulkResponse struct {
	Errors *bool                           `json:"errors"`
	Items  []map[string]openSearchBulkItem `json:"items"`
}

type openSearchBulkItem struct {
	Status int `json:"status"`
}

func classifyOpenSearchBulkResponse(payload openSearchBulkResponse) error {
	if payload.Errors == nil {
		return fmt.Errorf("%w: malformed bulk response", ErrUnavailable)
	}
	if !*payload.Errors {
		return nil
	}
	if len(payload.Items) == 0 {
		return fmt.Errorf("%w: malformed bulk response", ErrUnavailable)
	}

	var rejected error
	for _, item := range payload.Items {
		status, ok := openSearchBulkItemStatus(item)
		if !ok {
			return fmt.Errorf("%w: malformed bulk item response", ErrUnavailable)
		}
		if err := classifyOpenSearchStatus(status, http.StatusOK, http.StatusCreated); err != nil {
			if errors.Is(err, ErrUnavailable) {
				return fmt.Errorf("%w: bulk item status %d", ErrUnavailable, status)
			}
			if rejected == nil {
				rejected = fmt.Errorf("%w: bulk item status %d", ErrRejected, status)
			}
		}
	}
	return rejected
}

// openSearchBulkItemStatus accepts the action-keyed item envelope used by
// OpenSearch bulk responses without coupling parsing to only one action name.
func openSearchBulkItemStatus(item map[string]openSearchBulkItem) (int, bool) {
	for _, result := range item {
		if result.Status == 0 {
			return 0, false
		}
		return result.Status, true
	}
	return 0, false
}

func (c *OpenSearchClient) SearchIDs(ctx context.Context, query Query) ([]string, error) {
	return c.searchIDs(ctx, query, openSearchSearchPageSize)
}

func (c *OpenSearchClient) searchIDs(ctx context.Context, query Query, pageSize int) ([]string, error) {
	if pageSize <= 0 {
		pageSize = openSearchSearchPageSize
	}

	var ids []string
	var searchAfter []any
	for {
		page, err := c.searchIDsPage(ctx, query, pageSize, searchAfter)
		if err != nil {
			return nil, err
		}
		ids = append(ids, page.IDs...)
		if page.HitCount < pageSize {
			return ids, nil
		}
		if len(page.NextSearchAfter) == 0 {
			return nil, fmt.Errorf("%w: search_after sort value missing", ErrUnavailable)
		}
		searchAfter = page.NextSearchAfter
	}
}

type openSearchSearchPage struct {
	IDs             []string
	HitCount        int
	NextSearchAfter []any
}

// searchIDsPage requests one provider page after compiling the shared query
// plan, so parser errors are caught before a backend request is attempted.
func (c *OpenSearchClient) searchIDsPage(ctx context.Context, query Query, pageSize int, searchAfter []any) (openSearchSearchPage, error) {
	body, err := openSearchSearchBody(query, pageSize, searchAfter)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	req, err := c.newJSONRequest(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_search", nil, body)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchResponse(resp, http.StatusOK); err != nil {
		return openSearchSearchPage{}, err
	}

	return decodeOpenSearchSearchPage(resp.Body)
}

func decodeOpenSearchSearchPage(body io.Reader) (openSearchSearchPage, error) {
	var payload struct {
		Hits struct {
			Hits []struct {
				ID   string `json:"_id"`
				Sort []any  `json:"sort"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := decodeOpenSearchJSON(body, &payload, "search response"); err != nil {
		return openSearchSearchPage{}, err
	}

	ids := make([]string, 0, len(payload.Hits.Hits))
	var nextSearchAfter []any
	for _, hit := range payload.Hits.Hits {
		nextSearchAfter = hit.Sort
		if strings.TrimSpace(hit.ID) == "" {
			continue
		}
		ids = append(ids, hit.ID)
	}
	return openSearchSearchPage{
		IDs:             ids,
		HitCount:        len(payload.Hits.Hits),
		NextSearchAfter: nextSearchAfter,
	}, nil
}

func (c *OpenSearchClient) Refresh(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_refresh", nil, nil, http.StatusOK, http.StatusAccepted)
}

func (c *OpenSearchClient) DeleteDocument(ctx context.Context, id string) error {
	path := "/" + openSearchChefIndex + "/_doc/" + url.PathEscape(id)
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, http.StatusOK, http.StatusAccepted, http.StatusNotFound)
}

func (c *OpenSearchClient) DeleteByQuery(ctx context.Context, org, index string) error {
	fallback, err := c.deleteByQueryFallbackRequired(ctx)
	if err != nil {
		return err
	}
	if fallback {
		return c.deleteByQueryFallback(ctx, org, index)
	}

	query := url.Values{"refresh": []string{"true"}}
	body := openSearchDeleteByQueryBody(org, index)
	err = c.deleteByQueryDirect(ctx, query, body)
	if errors.Is(err, errOpenSearchDeleteByQueryUnsupported) {
		return c.deleteByQueryFallback(ctx, org, index)
	}
	return err
}

func (c *OpenSearchClient) deleteByQueryDirect(ctx context.Context, query url.Values, body map[string]any) error {
	resp, err := c.doJSONResponse(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_delete_by_query", query, body)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return errOpenSearchDeleteByQueryUnsupported
	default:
		return classifyOpenSearchResponse(resp, http.StatusOK, http.StatusAccepted)
	}
}

func (c *OpenSearchClient) deleteByQueryFallbackRequired(ctx context.Context) (bool, error) {
	info, ok := c.ProviderInfo()
	if !ok {
		var err error
		info, err = c.DiscoverProvider(ctx)
		if err != nil {
			return false, err
		}
	}
	return info.Capabilities.DeleteByQueryFallbackRequired || !info.Capabilities.DeleteByQuery, nil
}

func (c *OpenSearchClient) deleteByQueryFallback(ctx context.Context, org, index string) error {
	if info, ok := c.ProviderInfo(); ok && !info.Capabilities.SearchAfterPagination {
		return fmt.Errorf("%w: provider %s delete-by-query fallback requires search_after pagination", ErrInvalidConfiguration, openSearchProviderIdentity(info))
	}
	return c.deleteByQueryFallbackWithPageSize(ctx, org, index, openSearchSearchPageSize)
}

func (c *OpenSearchClient) deleteByQueryFallbackWithPageSize(ctx context.Context, org, index string, pageSize int) error {
	if pageSize <= 0 {
		pageSize = openSearchSearchPageSize
	}

	deleted := false
	var searchAfter []any
	for {
		page, err := c.searchScopedDocumentIDsPage(ctx, org, index, pageSize, searchAfter)
		if err != nil {
			return err
		}
		for _, id := range page.IDs {
			if err := c.DeleteDocument(ctx, id); err != nil {
				return err
			}
			deleted = true
		}
		if page.HitCount < pageSize {
			break
		}
		if len(page.NextSearchAfter) == 0 {
			return fmt.Errorf("%w: search_after sort value missing", ErrUnavailable)
		}
		searchAfter = page.NextSearchAfter
	}
	if !deleted {
		return nil
	}
	return c.Refresh(ctx)
}

func (c *OpenSearchClient) searchScopedDocumentIDsPage(ctx context.Context, org, index string, pageSize int, searchAfter []any) (openSearchSearchPage, error) {
	body := openSearchDeleteScopeSearchBody(org, index, pageSize, searchAfter)
	req, err := c.newJSONRequest(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_search", nil, body)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchResponse(resp, http.StatusOK); err != nil {
		return openSearchSearchPage{}, err
	}
	page, err := decodeOpenSearchSearchPage(resp.Body)
	if err != nil {
		return openSearchSearchPage{}, err
	}
	return page, nil
}

func OpenSearchDocumentID(doc Document) string {
	return OpenSearchDocumentIDForRef(DocumentRef{
		Organization: doc.Resource.Organization,
		Index:        doc.Index,
		Name:         doc.Name,
	})
}

func openSearchDeleteByQueryBody(org, index string) map[string]any {
	filters := openSearchTermFilters(org, index)
	if len(filters) == 0 {
		return map[string]any{
			"query": map[string]any{
				"match_all": map[string]any{},
			},
		}
	}
	return map[string]any{
		"query": map[string]any{
			"bool": map[string]any{
				"filter": filters,
			},
		},
	}
}

func openSearchDeleteScopeSearchBody(org, index string, pageSize int, searchAfter []any) map[string]any {
	if pageSize <= 0 {
		pageSize = openSearchSearchPageSize
	}
	body := map[string]any{
		"_source": false,
		"size":    pageSize,
		"query":   openSearchDeleteScopeQuery(org, index),
		"sort": []any{
			map[string]any{
				"document_id": map[string]any{
					"order": "asc",
				},
			},
		},
	}
	if len(searchAfter) > 0 {
		body["search_after"] = searchAfter
	}
	return body
}

func openSearchDeleteScopeQuery(org, index string) map[string]any {
	filters := openSearchTermFilters(org, index)
	if len(filters) == 0 {
		return map[string]any{
			"match_all": map[string]any{},
		}
	}
	return map[string]any{
		"bool": map[string]any{
			"filter": filters,
		},
	}
}

func openSearchDocumentSource(doc Document) map[string]any {
	source := make(map[string]any, len(doc.Fields)+5)
	source["document_id"] = OpenSearchDocumentID(doc)
	source["organization"] = doc.Resource.Organization
	source["index"] = doc.Index
	source["name"] = doc.Name
	source["resource_type"] = doc.Resource.Type
	source["resource_name"] = doc.Resource.Name
	source[openSearchCompatTermsField] = openSearchCompatibilityTerms(doc)
	for key, values := range doc.Fields {
		out := make([]string, len(values))
		copy(out, values)
		source[key] = out
	}
	return source
}

func openSearchChefIndexMapping() map[string]any {
	return openSearchChefIndexDescriptor.Mapping()
}

func (d openSearchIndexDescriptor) Mapping() map[string]any {
	return map[string]any{
		"_meta": map[string]any{
			openSearchMappingMetaKey: d.MappingVersion,
		},
		"dynamic": true,
		"properties": map[string]any{
			"document_id": map[string]any{
				"type": "keyword",
			},
			openSearchCompatTermsField: map[string]any{
				"type": "keyword",
			},
		},
	}
}

func openSearchChefMappingFromResponse(payload map[string]any) (map[string]any, bool) {
	if mappings, ok := payload["mappings"].(map[string]any); ok {
		return mappings, true
	}
	index, ok := payload[openSearchChefIndexDescriptor.Name].(map[string]any)
	if !ok {
		return nil, false
	}
	mappings, ok := index["mappings"].(map[string]any)
	return mappings, ok
}

func openSearchChefMappingCurrent(mapping map[string]any) bool {
	if !openSearchMappingHasKeywordProperty(mapping, "document_id") {
		return false
	}
	if !openSearchMappingHasKeywordProperty(mapping, openSearchCompatTermsField) {
		return false
	}
	meta, ok := mapping["_meta"].(map[string]any)
	if !ok {
		return false
	}
	return openSearchMappingVersionValue(meta[openSearchMappingMetaKey]) >= openSearchChefIndexDescriptor.MappingVersion
}

func openSearchMappingHasKeywordProperty(mapping map[string]any, field string) bool {
	properties, ok := mapping["properties"].(map[string]any)
	if !ok {
		return false
	}
	property, ok := properties[field].(map[string]any)
	if !ok {
		return false
	}
	fieldType, ok := property["type"].(string)
	return ok && fieldType == "keyword"
}

func openSearchMappingVersionValue(raw any) int {
	switch value := raw.(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, err := value.Int64()
		if err != nil {
			return 0
		}
		return int(parsed)
	default:
		return 0
	}
}

func openSearchCompatibilityTerms(doc Document) []string {
	terms := make([]string, 0, len(doc.Fields)*2+2)
	terms = append(terms,
		openSearchCompatToken(openSearchCompatOrgField, doc.Resource.Organization),
		openSearchCompatToken(openSearchCompatIndexField, doc.Index),
	)
	for field, values := range doc.Fields {
		for _, value := range values {
			terms = append(terms,
				openSearchCompatToken(field, value),
				openSearchCompatToken(openSearchCompatAnyField, value),
			)
		}
	}
	return terms
}

// openSearchSearchBody compiles the shared query plan into the active
// OpenSearch request body while preserving org/index compat_terms filters.
func openSearchSearchBody(query Query, pageSize int, searchAfter []any) (map[string]any, error) {
	plan := CompileQuery(query.Q)
	if err := plan.Err(); err != nil {
		return nil, err
	}
	body := map[string]any{
		"_source": false,
		"size":    pageSize,
		"query": map[string]any{
			"bool": map[string]any{
				"filter": openSearchCompatibilityFilters(query.Organization, query.Index),
				"must":   plan.OpenSearchQueryClause(),
			},
		},
		"sort": []any{
			map[string]any{
				"document_id": map[string]any{
					"order": "asc",
				},
			},
		},
	}
	if len(searchAfter) > 0 {
		body["search_after"] = searchAfter
	}
	return body, nil
}

func openSearchCompatibilityFilters(org, index string) []any {
	filters := make([]any, 0, 2)
	if strings.TrimSpace(org) != "" {
		filters = append(filters, openSearchCompatTermClause(openSearchCompatOrgField, org))
	}
	if strings.TrimSpace(index) != "" {
		filters = append(filters, openSearchCompatTermClause(openSearchCompatIndexField, index))
	}
	return filters
}

func openSearchTermFilters(org, index string) []any {
	filters := make([]any, 0, 2)
	if strings.TrimSpace(org) != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"organization": org},
		})
	}
	if strings.TrimSpace(index) != "" {
		filters = append(filters, map[string]any{
			"term": map[string]any{"index": index},
		})
	}
	return filters
}

func (c *OpenSearchClient) doJSON(ctx context.Context, method, path string, query url.Values, payload any, accepted ...int) error {
	resp, err := c.doJSONResponse(ctx, method, path, query, payload)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return classifyOpenSearchResponse(resp, accepted...)
}

func (c *OpenSearchClient) doJSONResponse(ctx context.Context, method, path string, query url.Values, payload any) (*http.Response, error) {
	req, err := c.newJSONRequest(ctx, method, path, query, payload)
	if err != nil {
		return nil, err
	}
	return c.do(req)
}

func (c *OpenSearchClient) newJSONRequest(ctx context.Context, method, path string, query url.Values, payload any) (*http.Request, error) {
	var body io.Reader
	if payload != nil {
		var encoded bytes.Buffer
		if err := json.NewEncoder(&encoded).Encode(payload); err != nil {
			return nil, fmt.Errorf("%w: encode request", ErrRejected)
		}
		body = &encoded
	}
	return c.newRequest(ctx, method, path, query, body, "application/json")
}

func (c *OpenSearchClient) newRequest(ctx context.Context, method, path string, query url.Values, body io.Reader, contentType string) (*http.Request, error) {
	endpoint := c.resolve(path, query)
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, fmt.Errorf("%w: build request", ErrInvalidConfiguration)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("User-Agent", "opencook")
	return req, nil
}

func (c *OpenSearchClient) resolve(path string, query url.Values) *url.URL {
	out := *c.endpoint
	basePath := strings.TrimRight(out.EscapedPath(), "/")
	path = "/" + strings.TrimLeft(path, "/")
	escapedPath := ""
	if path == "/" {
		escapedPath = basePath
		if escapedPath == "" {
			escapedPath = "/"
		}
	} else {
		escapedPath = basePath + path
	}
	if unescapedPath, err := url.PathUnescape(escapedPath); err == nil {
		out.Path = unescapedPath
		if escapedPath != unescapedPath {
			out.RawPath = escapedPath
		} else {
			out.RawPath = ""
		}
	} else {
		out.Path = escapedPath
		out.RawPath = ""
	}
	out.RawQuery = query.Encode()
	return &out
}

func (c *OpenSearchClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.transport.Do(req)
	if err != nil {
		return nil, classifyOpenSearchTransportError(err)
	}
	if resp == nil {
		return nil, fmt.Errorf("%w: empty response", ErrUnavailable)
	}
	return resp, nil
}

func parseOpenSearchEndpoint(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("%w: OPENCOOK_OPENSEARCH_URL is empty", ErrInvalidConfiguration)
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: parse OPENCOOK_OPENSEARCH_URL", ErrInvalidConfiguration)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: OPENCOOK_OPENSEARCH_URL must use http or https", ErrInvalidConfiguration)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: OPENCOOK_OPENSEARCH_URL must include a host", ErrInvalidConfiguration)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: OPENCOOK_OPENSEARCH_URL must not include query strings or fragments", ErrInvalidConfiguration)
	}

	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = ""
	return parsed, nil
}

func classifyOpenSearchTransportError(err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: request canceled", ErrUnavailable)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: request deadline exceeded", ErrUnavailable)
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: request timed out", ErrUnavailable)
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return fmt.Errorf("%w: DNS lookup failed", ErrUnavailable)
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return fmt.Errorf("%w: connection failed", ErrUnavailable)
	}
	return fmt.Errorf("%w: transport request failed", ErrUnavailable)
}

func classifyOpenSearchResponse(resp *http.Response, accepted ...int) error {
	if resp == nil {
		return fmt.Errorf("%w: empty response", ErrUnavailable)
	}
	return classifyOpenSearchStatus(resp.StatusCode, accepted...)
}

func classifyOpenSearchStatus(status int, accepted ...int) error {
	for _, code := range accepted {
		if status == code {
			return nil
		}
	}
	switch {
	case status == http.StatusTooManyRequests:
		return fmt.Errorf("%w: provider throttled status %d", ErrUnavailable, status)
	case status >= 500:
		return fmt.Errorf("%w: provider outage status %d", ErrUnavailable, status)
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return fmt.Errorf("%w: provider authentication/configuration status %d", ErrRejected, status)
	case status == http.StatusConflict:
		return fmt.Errorf("%w: provider conflict status %d", ErrRejected, status)
	case status == http.StatusBadRequest || status == http.StatusNotFound || status == http.StatusMethodNotAllowed || status == http.StatusNotImplemented:
		return fmt.Errorf("%w: provider rejected malformed or unsupported request status %d", ErrRejected, status)
	case status >= 400:
		return fmt.Errorf("%w: provider rejected request status %d", ErrRejected, status)
	default:
		return fmt.Errorf("%w: unexpected status %d", ErrUnavailable, status)
	}
}

func decodeOpenSearchJSON(body io.Reader, target any, label string) error {
	if err := json.NewDecoder(body).Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: empty %s", ErrUnavailable, label)
		}
		return fmt.Errorf("%w: malformed %s", ErrUnavailable, label)
	}
	return nil
}

func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
