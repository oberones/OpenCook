package search

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestOpenSearchEndpointValidation(t *testing.T) {
	if err := ValidateOpenSearchEndpoint(""); err != nil {
		t.Fatalf("ValidateOpenSearchEndpoint(empty) error = %v, want nil", err)
	}

	for _, raw := range []string{
		"",
		"ftp://opensearch.example",
		"http://",
		"http://opensearch.example?pretty=true",
		"http://opensearch.example/#frag",
	} {
		_, err := NewOpenSearchClient(raw)
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("NewOpenSearchClient(%q) error = %v, want ErrInvalidConfiguration", raw, err)
		}
	}
}

func TestOpenSearchClientPingAndEnsureChefIndexRequestShapes(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/prefix":
			return http.StatusOK, `{"version":{"distribution":"opensearch"}}`
		case r.Method == http.MethodHead && r.URL.Path == "/prefix/chef":
			return http.StatusNotFound, ""
		case r.Method == http.MethodPut && r.URL.Path == "/prefix/chef":
			return http.StatusCreated, `{"acknowledged":true}`
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
			return 0, ""
		}
	})

	client, err := NewOpenSearchClient("http://opensearch.local/prefix", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := client.EnsureChefIndex(context.Background()); err != nil {
		t.Fatalf("EnsureChefIndex() error = %v", err)
	}

	requests := transport.Requests()
	requireRecordedRequest(t, requests, 0, http.MethodGet, "/prefix", "")
	requireRecordedRequest(t, requests, 1, http.MethodHead, "/prefix/chef", "")
	requireRecordedRequest(t, requests, 2, http.MethodPut, "/prefix/chef", "")
	if got := requests[2].Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("PUT Content-Type = %q, want application/json", got)
	}

	body := decodeJSONMap(t, requests[2].Body)
	if _, ok := body["settings"].(map[string]any); !ok {
		t.Fatalf("PUT body settings = %T, want object (%v)", body["settings"], body)
	}
	if mappings, ok := body["mappings"].(map[string]any); !ok || mappings["dynamic"] != true {
		t.Fatalf("PUT body mappings = %v, want dynamic true", body["mappings"])
	}
	properties := body["mappings"].(map[string]any)["properties"].(map[string]any)
	documentID := properties["document_id"].(map[string]any)
	if documentID["type"] != "keyword" {
		t.Fatalf("document_id mapping = %v, want keyword", documentID)
	}
	compatTerms := properties[openSearchCompatTermsField].(map[string]any)
	if compatTerms["type"] != "keyword" {
		t.Fatalf("compat terms mapping = %v, want keyword", compatTerms)
	}
}

func TestOpenSearchClientBulkUpsertRequestShape(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/_bulk" {
			t.Fatalf("request = %s %s, want POST /_bulk", r.Method, r.URL.Path)
		}
		return http.StatusOK, `{"errors":false}`
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	doc := NewDocumentBuilder().Node("ponyville", bootstrap.Node{
		Name:            "twilight",
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		RunList:         []string{"base", "role[web]"},
		PolicyName:      "delivery",
		PolicyGroup:     "prod",
	})

	if err := client.BulkUpsert(context.Background(), []Document{doc}); err != nil {
		t.Fatalf("BulkUpsert() error = %v", err)
	}

	recorded := transport.Requests()[0]
	if got := recorded.Header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("Content-Type = %q, want application/x-ndjson", got)
	}

	lines := strings.Split(strings.TrimSpace(recorded.Body), "\n")
	if len(lines) != 2 {
		t.Fatalf("bulk lines len = %d, want 2 (%q)", len(lines), recorded.Body)
	}
	action := decodeJSONMap(t, lines[0])
	indexAction := action["index"].(map[string]any)
	if indexAction["_index"] != openSearchChefIndex {
		t.Fatalf("bulk _index = %v, want %q", indexAction["_index"], openSearchChefIndex)
	}
	if indexAction["_id"] != "ponyville/node/twilight" {
		t.Fatalf("bulk _id = %v, want ponyville/node/twilight", indexAction["_id"])
	}

	source := decodeJSONMap(t, lines[1])
	if source["organization"] != "ponyville" || source["index"] != "node" {
		t.Fatalf("bulk source identity = %v, want ponyville/node/twilight", source)
	}
	if source["document_id"] != "ponyville/node/twilight" {
		t.Fatalf("bulk source document_id = %v, want ponyville/node/twilight", source["document_id"])
	}
	requireJSONListContains(t, source["name"], "twilight")
	requireJSONListContains(t, source["recipe"], "base")
	requireJSONListContains(t, source["role"], "web")
	requireJSONListContains(t, source["policy_name"], "delivery")
	requireJSONListContains(t, source["policy_group"], "prod")
	requireJSONListContains(t, source[openSearchCompatTermsField], "name=twilight")
	requireJSONListContains(t, source[openSearchCompatTermsField], "__any=twilight")
	requireJSONListContains(t, source[openSearchCompatTermsField], "__org=ponyville")
	requireJSONListContains(t, source[openSearchCompatTermsField], "__index=node")
}

func TestOpenSearchClientSearchIDsRequestShape(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		return http.StatusOK, `{"hits":{"hits":[{"_id":"ponyville/node/twilight"},{"_id":"ponyville/node/rarity"}]}}`
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	ids, err := client.SearchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:twi*",
	})
	if err != nil {
		t.Fatalf("SearchIDs() error = %v", err)
	}
	if got, want := strings.Join(ids, ","), "ponyville/node/twilight,ponyville/node/rarity"; got != want {
		t.Fatalf("SearchIDs() = %v, want %s", ids, want)
	}

	recorded := transport.Requests()[0]
	body := decodeJSONMap(t, recorded.Body)
	if _, ok := body["from"]; ok {
		t.Fatalf("search body included from = %v, want search_after pagination", body["from"])
	}
	if _, ok := body["search_after"]; ok {
		t.Fatalf("first search body included search_after = %v", body["search_after"])
	}
	if body["_source"] != false || body["size"] != float64(openSearchSearchPageSize) {
		t.Fatalf("search body paging/source = %v, want _source false size %d", body, openSearchSearchPageSize)
	}
	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__org=ponyville")
	requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__index=node")
	requireCompatPrefixClause(t, boolQuery["must"], "name=twi")
	sortSpec := body["sort"].([]any)[0].(map[string]any)["document_id"].(map[string]any)
	if sortSpec["order"] != "asc" {
		t.Fatalf("sort = %v, want document_id asc", body["sort"])
	}
}

func TestOpenSearchClientSearchIDsUsesSearchAfterPagination(t *testing.T) {
	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		body := decodeJSONMap(t, recorded.Body)
		if body["size"] != float64(2) {
			t.Fatalf("request %d size = %v, want 2", requestCount, body["size"])
		}
		switch requestCount {
		case 1:
			if _, ok := body["search_after"]; ok {
				t.Fatalf("first search request search_after = %v, want omitted", body["search_after"])
			}
			return http.StatusOK, `{"hits":{"hits":[` +
				`{"_id":"ponyville/node/applejack","sort":["ponyville/node/applejack"]},` +
				`{"_id":"ponyville/node/fluttershy","sort":["ponyville/node/fluttershy"]}` +
				`]}}`
		case 2:
			searchAfter := body["search_after"].([]any)
			if len(searchAfter) != 1 || searchAfter[0] != "ponyville/node/fluttershy" {
				t.Fatalf("second search_after = %v, want fluttershy document id", searchAfter)
			}
			return http.StatusOK, `{"hits":{"hits":[` +
				`{"_id":"ponyville/node/twilight","sort":["ponyville/node/twilight"]}` +
				`]}}`
		default:
			t.Fatalf("unexpected search request %d", requestCount)
			return http.StatusInternalServerError, ""
		}
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	ids, err := client.searchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:*",
	}, 2)
	if err != nil {
		t.Fatalf("searchIDs() error = %v", err)
	}
	if got, want := strings.Join(ids, ","), "ponyville/node/applejack,ponyville/node/fluttershy,ponyville/node/twilight"; got != want {
		t.Fatalf("searchIDs() = %s, want %s", got, want)
	}
	if requestCount != 2 {
		t.Fatalf("requestCount = %d, want 2", requestCount)
	}
}

func TestOpenSearchClientRefreshAndDeleteRequestShapes(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/chef/_refresh":
		case r.Method == http.MethodDelete && r.URL.EscapedPath() == "/chef/_doc/ponyville%2Fnode%2Ftwilight":
		case r.Method == http.MethodPost && r.URL.Path == "/chef/_delete_by_query" && r.URL.RawQuery == "refresh=true":
		default:
			t.Fatalf("unexpected request %s %s raw=%s escaped=%s", r.Method, r.URL.Path, r.URL.RawQuery, r.URL.EscapedPath())
		}
		return http.StatusOK, `{"acknowledged":true}`
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	if err := client.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if err := client.DeleteDocument(context.Background(), "ponyville/node/twilight"); err != nil {
		t.Fatalf("DeleteDocument() error = %v", err)
	}
	if err := client.DeleteByQuery(context.Background(), "ponyville", "node"); err != nil {
		t.Fatalf("DeleteByQuery() error = %v", err)
	}

	requests := transport.Requests()
	requireRecordedRequest(t, requests, 0, http.MethodPost, "/chef/_refresh", "")
	requireRecordedRequest(t, requests, 1, http.MethodDelete, "/chef/_doc/ponyville/node/twilight", "")
	requireRecordedRequest(t, requests, 2, http.MethodPost, "/chef/_delete_by_query", "refresh=true")
	body := decodeJSONMap(t, requests[2].Body)
	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	requireTermFilter(t, boolQuery["filter"], "organization", "ponyville")
	requireTermFilter(t, boolQuery["filter"], "index", "node")
}

func TestOpenSearchClientErrorClassificationDoesNotLeakProviderBodies(t *testing.T) {
	rejectedTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusBadRequest, "raw provider body with internal endpoint details"
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(rejectedTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	err = client.Ping(context.Background())
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("Ping() error = %v, want ErrRejected", err)
	}
	if strings.Contains(err.Error(), "internal endpoint details") {
		t.Fatalf("Ping() error leaked provider body: %q", err.Error())
	}

	unavailableTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusServiceUnavailable, ""
	})
	unavailableClient, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(unavailableTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(unavailable) error = %v", err)
	}
	if err := unavailableClient.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping(503) error = %v, want ErrUnavailable", err)
	}

	timeoutClient, err := NewOpenSearchClient("http://opensearch.example", WithOpenSearchTransport(timeoutTransport{}))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(timeout) error = %v", err)
	}
	if err := timeoutClient.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping(timeout) error = %v, want ErrUnavailable", err)
	}

	bulkFailureTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusOK, `{"errors":true}`
	})
	bulkFailureClient, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(bulkFailureTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(bulk failure) error = %v", err)
	}
	if err := bulkFailureClient.BulkUpsert(context.Background(), []Document{{Index: "node", Name: "twilight"}}); !errors.Is(err, ErrRejected) {
		t.Fatalf("BulkUpsert(errors=true) error = %v, want ErrRejected", err)
	}
}

func TestOpenSearchStatusVariants(t *testing.T) {
	memory := NewMemoryIndex(nil, "http://opensearch.example").Status()
	if memory.Backend != "memory-compat" || !strings.Contains(memory.Message, "memory search fallback") {
		t.Fatalf("memory status = %+v, want memory fallback wording", memory)
	}

	active := OpenSearchActiveStatus()
	if active.Backend != "opensearch" || !active.Configured || !strings.Contains(active.Message, "active") {
		t.Fatalf("active status = %+v, want active OpenSearch status", active)
	}

	unavailable := OpenSearchUnavailableStatus()
	if unavailable.Backend != "opensearch" || !unavailable.Configured || !strings.Contains(unavailable.Message, "unavailable") {
		t.Fatalf("unavailable status = %+v, want configured unavailable status", unavailable)
	}
}

type recordedOpenSearchRequest struct {
	Method string
	Path   string
	Query  string
	Header http.Header
	Body   string
}

type recordingOpenSearchTransport struct {
	t        *testing.T
	handler  func(*http.Request, recordedOpenSearchRequest) (int, string)
	requests []recordedOpenSearchRequest
}

type timeoutTransport struct{}

func newRecordingOpenSearchTransport(t *testing.T, handler func(*http.Request, recordedOpenSearchRequest) (int, string)) *recordingOpenSearchTransport {
	t.Helper()

	return &recordingOpenSearchTransport{
		t:       t,
		handler: handler,
	}
}

func (t *recordingOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	recorded := recordOpenSearchRequest(t.t, req)
	t.requests = append(t.requests, recorded)
	status, body := t.handler(req, recorded)
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func (t *recordingOpenSearchTransport) Requests() []recordedOpenSearchRequest {
	out := make([]recordedOpenSearchRequest, len(t.requests))
	copy(out, t.requests)
	return out
}

func (timeoutTransport) Do(*http.Request) (*http.Response, error) {
	return nil, context.DeadlineExceeded
}

func recordOpenSearchRequest(t *testing.T, r *http.Request) recordedOpenSearchRequest {
	t.Helper()

	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll(request body) error = %v", err)
		}
	}
	return recordedOpenSearchRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Query:  r.URL.RawQuery,
		Header: r.Header.Clone(),
		Body:   string(body),
	}
}

func requireRecordedRequest(t *testing.T, requests []recordedOpenSearchRequest, idx int, method, path, query string) {
	t.Helper()

	if len(requests) <= idx {
		t.Fatalf("requests len = %d, want index %d", len(requests), idx)
	}
	req := requests[idx]
	if req.Method != method || req.Path != path || req.Query != query {
		t.Fatalf("request[%d] = %s %s?%s, want %s %s?%s", idx, req.Method, req.Path, req.Query, method, path, query)
	}
}

func decodeJSONMap(t *testing.T, raw string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("json.Unmarshal(%q) error = %v", raw, err)
	}
	return out
}

func requireJSONListContains(t *testing.T, raw any, want string) {
	t.Helper()

	for _, item := range raw.([]any) {
		if item == want {
			return
		}
	}
	t.Fatalf("list = %v, want to contain %q", raw, want)
}

func requireTermFilter(t *testing.T, raw any, field, want string) {
	t.Helper()

	for _, filter := range raw.([]any) {
		term := filter.(map[string]any)["term"].(map[string]any)
		if term[field] == want {
			return
		}
	}
	t.Fatalf("filters = %v, want term %s=%s", raw, field, want)
}

func requireCompatPrefixClause(t *testing.T, raw any, want string) {
	t.Helper()

	boolQuery, ok := raw.(map[string]any)["bool"].(map[string]any)
	if !ok {
		t.Fatalf("query clause = %T %v, want bool", raw, raw)
	}
	for _, clause := range boolQuery["must"].([]any) {
		prefix, ok := clause.(map[string]any)["prefix"].(map[string]any)
		if ok && prefix[openSearchCompatTermsField] == want {
			return
		}
	}
	t.Fatalf("query clause = %v, want prefix %q", raw, want)
}
