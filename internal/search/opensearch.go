package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const openSearchChefIndex = "chef"

type OpenSearchTransport interface {
	Do(*http.Request) (*http.Response, error)
}

type OpenSearchClient struct {
	endpoint  *url.URL
	transport OpenSearchTransport
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
	return OpenSearchActiveStatus()
}

func OpenSearchActiveStatus() Status {
	return Status{
		Backend:    "opensearch",
		Configured: true,
		Message:    "OpenSearch-backed search provider active",
	}
}

func OpenSearchUnavailableStatus() Status {
	return Status{
		Backend:    "opensearch",
		Configured: true,
		Message:    "OpenSearch is configured but unavailable",
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
	return classifyOpenSearchStatus(resp.StatusCode, http.StatusOK)
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
		return classifyOpenSearchStatus(resp.StatusCode, http.StatusOK)
	}

	body := map[string]any{
		"settings": map[string]any{
			"index": map[string]any{
				"number_of_shards":     1,
				"auto_expand_replicas": "0-1",
			},
		},
		"mappings": openSearchChefIndexMapping(),
	}
	return c.doJSON(ctx, http.MethodPut, "/"+openSearchChefIndex, nil, body, http.StatusOK, http.StatusCreated)
}

func (c *OpenSearchClient) ensureChefIndexMapping(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPut, "/"+openSearchChefIndex+"/_mapping", nil, openSearchChefIndexMapping(), http.StatusOK)
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
	if err := classifyOpenSearchStatus(resp.StatusCode, http.StatusOK, http.StatusCreated); err != nil {
		return err
	}

	var payload struct {
		Errors bool `json:"errors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: decode bulk response", ErrUnavailable)
	}
	if payload.Errors {
		return fmt.Errorf("%w: bulk item failure", ErrRejected)
	}
	return nil
}

func (c *OpenSearchClient) SearchIDs(ctx context.Context, query Query, start, rows int) ([]string, error) {
	body := openSearchSearchBody(query, start, rows)
	req, err := c.newJSONRequest(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_search", nil, body)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp)
	if err := classifyOpenSearchStatus(resp.StatusCode, http.StatusOK); err != nil {
		return nil, err
	}

	var payload struct {
		Hits struct {
			Hits []struct {
				ID string `json:"_id"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("%w: decode search response", ErrUnavailable)
	}

	ids := make([]string, 0, len(payload.Hits.Hits))
	for _, hit := range payload.Hits.Hits {
		if strings.TrimSpace(hit.ID) == "" {
			continue
		}
		ids = append(ids, hit.ID)
	}
	return ids, nil
}

func (c *OpenSearchClient) Refresh(ctx context.Context) error {
	return c.doJSON(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_refresh", nil, nil, http.StatusOK)
}

func (c *OpenSearchClient) DeleteDocument(ctx context.Context, id string) error {
	path := "/" + openSearchChefIndex + "/_doc/" + url.PathEscape(id)
	return c.doJSON(ctx, http.MethodDelete, path, nil, nil, http.StatusOK, http.StatusAccepted, http.StatusNotFound)
}

func (c *OpenSearchClient) DeleteByQuery(ctx context.Context, org, index string) error {
	query := url.Values{"refresh": []string{"true"}}
	body := openSearchDeleteByQueryBody(org, index)
	return c.doJSON(ctx, http.MethodPost, "/"+openSearchChefIndex+"/_delete_by_query", query, body, http.StatusOK, http.StatusAccepted)
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

func openSearchDocumentSource(doc Document) map[string]any {
	source := make(map[string]any, len(doc.Fields)+5)
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
	return map[string]any{
		"dynamic": true,
		"properties": map[string]any{
			openSearchCompatTermsField: map[string]any{
				"type": "keyword",
			},
		},
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

func openSearchSearchBody(query Query, start, rows int) map[string]any {
	return map[string]any{
		"_source": false,
		"from":    start,
		"size":    rows,
		"query": map[string]any{
			"bool": map[string]any{
				"filter": openSearchCompatibilityFilters(query.Organization, query.Index),
				"must":   CompileQuery(query.Q).OpenSearchQueryClause(),
			},
		},
		"sort": []any{
			map[string]any{
				"_id": map[string]any{
					"order": "asc",
				},
			},
		},
	}
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
	req, err := c.newJSONRequest(ctx, method, path, query, payload)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer closeResponseBody(resp)
	return classifyOpenSearchStatus(resp.StatusCode, accepted...)
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
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: request timed out", ErrUnavailable)
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return fmt.Errorf("%w: request timed out", ErrUnavailable)
	}
	return fmt.Errorf("%w: request failed", ErrUnavailable)
}

func classifyOpenSearchStatus(status int, accepted ...int) error {
	for _, code := range accepted {
		if status == code {
			return nil
		}
	}
	switch {
	case status == http.StatusTooManyRequests || status >= 500:
		return fmt.Errorf("%w: status %d", ErrUnavailable, status)
	case status >= 400:
		return fmt.Errorf("%w: status %d", ErrRejected, status)
	default:
		return fmt.Errorf("%w: unexpected status %d", ErrUnavailable, status)
	}
}

func closeResponseBody(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
