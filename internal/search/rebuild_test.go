package search

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestDocumentsFromBootstrapStateIncludesStartupSearchSurfaces(t *testing.T) {
	state := newSearchRebuildState(t)

	docs, err := DocumentsFromBootstrapState(state)
	if err != nil {
		t.Fatalf("DocumentsFromBootstrapState() error = %v", err)
	}

	requireDocumentRef(t, docs, "client", "ponyville-validator")
	requireDocumentRef(t, docs, "environment", "_default")
	requireDocumentRef(t, docs, "node", "twilight")
	requireDocumentRef(t, docs, "ponies", "alice")
}

func TestRebuildOpenSearchIndexDeletesStaleDocumentsAndUpsertsCurrentState(t *testing.T) {
	state := newSearchRebuildState(t)

	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/" {
				t.Fatalf("request 1 = %s %s, want GET /", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"version":{"distribution":"opensearch"}}`
		case 2:
			if r.Method != http.MethodHead || r.URL.Path != "/chef" {
				t.Fatalf("request 2 = %s %s, want HEAD /chef", r.Method, r.URL.Path)
			}
			return http.StatusOK, ""
		case 3:
			if r.Method != http.MethodPut || r.URL.Path != "/chef/_mapping" {
				t.Fatalf("request 3 = %s %s, want PUT /chef/_mapping", r.Method, r.URL.Path)
			}
			body := decodeJSONMap(t, recorded.Body)
			properties := body["properties"].(map[string]any)
			documentID := properties["document_id"].(map[string]any)
			if documentID["type"] != "keyword" {
				t.Fatalf("mapping body = %v, want keyword document_id", body)
			}
			compatTerms := properties[openSearchCompatTermsField].(map[string]any)
			if compatTerms["type"] != "keyword" {
				t.Fatalf("mapping body = %v, want keyword compat_terms", body)
			}
			return http.StatusOK, `{"acknowledged":true}`
		case 4:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_delete_by_query" || r.URL.RawQuery != "refresh=true" {
				t.Fatalf("request 4 = %s %s?%s, want POST /chef/_delete_by_query?refresh=true", r.Method, r.URL.Path, r.URL.RawQuery)
			}
			body := decodeJSONMap(t, recorded.Body)
			query := body["query"].(map[string]any)
			if _, ok := query["match_all"].(map[string]any); !ok {
				t.Fatalf("delete-by-query body = %v, want match_all stale-document cleanup", body)
			}
			return http.StatusOK, `{"deleted": 99}`
		case 5:
			if r.Method != http.MethodPost || r.URL.Path != "/_bulk" {
				t.Fatalf("request 5 = %s %s, want POST /_bulk", r.Method, r.URL.Path)
			}
			for _, want := range []string{
				`"_id":"ponyville/client/ponyville-validator"`,
				`"_id":"ponyville/environment/_default"`,
				`"_id":"ponyville/node/twilight"`,
				`"_id":"ponyville/ponies/alice"`,
			} {
				if !strings.Contains(recorded.Body, want) {
					t.Fatalf("bulk body missing %s: %s", want, recorded.Body)
				}
			}
			if strings.Contains(recorded.Body, "stale-node") {
				t.Fatalf("bulk body unexpectedly included stale document: %s", recorded.Body)
			}
			return http.StatusOK, `{"errors":false}`
		case 6:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_refresh" {
				t.Fatalf("request 6 = %s %s, want POST /chef/_refresh", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"_shards":{"successful":1}}`
		default:
			t.Fatalf("unexpected extra request %d: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	if err := RebuildOpenSearchIndex(context.Background(), client, state); err != nil {
		t.Fatalf("RebuildOpenSearchIndex() error = %v", err)
	}
	if got := len(transport.Requests()); got != 6 {
		t.Fatalf("request count = %d, want 6", got)
	}
}

func TestRebuildOpenSearchIndexUnavailableDoesNotLeakProviderBody(t *testing.T) {
	state := newSearchRebuildState(t)
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodGet || r.URL.Path != "/" {
			t.Fatalf("request = %s %s, want GET / before rebuild work", r.Method, r.URL.Path)
		}
		return http.StatusServiceUnavailable, "raw provider body from internal cluster"
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	err = RebuildOpenSearchIndex(context.Background(), client, state)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("RebuildOpenSearchIndex() error = %v, want ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), "internal cluster") {
		t.Fatalf("RebuildOpenSearchIndex() leaked provider body: %q", err.Error())
	}
	if got := len(transport.Requests()); got != 1 {
		t.Fatalf("request count = %d, want only ping before failing", got)
	}
}

func newSearchRebuildState(t *testing.T) *bootstrap.Service {
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
	creator := authn.Principal{Type: "user", Name: "pivotal"}
	if _, err := state.CreateNode("ponyville", bootstrap.CreateNodeInput{
		Creator: creator,
		Payload: map[string]any{
			"name":     "twilight",
			"run_list": []any{"base"},
		},
	}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Creator: creator,
		Payload: map[string]any{"name": "ponies"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
	if _, err := state.CreateDataBagItem("ponyville", "ponies", bootstrap.CreateDataBagItemInput{
		Payload: map[string]any{"id": "alice", "role": "operator"},
	}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
	return state
}

// newEncryptedDataBagRebuildState layers the shared encrypted-looking fixture
// onto the normal rebuild state so reindex/repair tests exercise the same
// PostgreSQL-derived search document builder without introducing crypto logic.
func newEncryptedDataBagRebuildState(t *testing.T) *bootstrap.Service {
	t.Helper()

	state := newSearchRebuildState(t)
	creator := authn.Principal{Type: "user", Name: "pivotal"}
	bagName := testfixtures.EncryptedDataBagName()
	if _, err := state.CreateDataBag("ponyville", bootstrap.CreateDataBagInput{
		Creator: creator,
		Payload: map[string]any{"name": bagName},
	}); err != nil {
		t.Fatalf("CreateDataBag(%s) error = %v", bagName, err)
	}
	if _, err := state.CreateDataBagItem("ponyville", bagName, bootstrap.CreateDataBagItemInput{
		Payload: testfixtures.EncryptedDataBagItem(),
	}); err != nil {
		t.Fatalf("CreateDataBagItem(%s/%s) error = %v", bagName, testfixtures.EncryptedDataBagItemID(), err)
	}
	return state
}

// requireEncryptedDataBagDocument finds the shared encrypted data bag document
// and verifies the full row still carries the original opaque JSON under
// raw_data instead of a decoded or reshaped representation.
func requireEncryptedDataBagDocument(t *testing.T, docs []Document) Document {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	for _, doc := range docs {
		if doc.Resource.Organization != "ponyville" || doc.Index != bagName || doc.Name != itemID {
			continue
		}
		rawData, ok := doc.Object["raw_data"].(map[string]any)
		if !ok {
			t.Fatalf("encrypted document raw_data = %T(%v), want object", doc.Object["raw_data"], doc.Object["raw_data"])
		}
		if !reflect.DeepEqual(rawData, testfixtures.EncryptedDataBagItem()) {
			t.Fatalf("encrypted document raw_data = %#v, want %#v", rawData, testfixtures.EncryptedDataBagItem())
		}
		return doc
	}
	t.Fatalf("docs = %v, want ponyville/%s/%s", docs, bagName, itemID)
	return Document{}
}

// requireEncryptedDataBagSearchFields proves encrypted envelopes are indexed as
// stored JSON fields and that raw_data-prefixed field names remain absent.
func requireEncryptedDataBagSearchFields(t *testing.T, doc Document) {
	t.Helper()

	password := testfixtures.EncryptedDataBagItem()["password"].(map[string]any)
	apiKey := testfixtures.EncryptedDataBagItem()["api_key"].(map[string]any)
	requireFieldContains(t, doc.Fields, "password_encrypted_data", password["encrypted_data"].(string))
	requireFieldContains(t, doc.Fields, "password_iv", password["iv"].(string))
	requireFieldContains(t, doc.Fields, "api_key_auth_tag", apiKey["auth_tag"].(string))
	requireFieldContains(t, doc.Fields, "environment", "production")
	if _, ok := doc.Fields["raw_data_password_encrypted_data"]; ok {
		t.Fatalf("encrypted document fields unexpectedly included raw_data-prefixed keys: %v", doc.Fields)
	}
}

// encryptedDataBagSearchIndexName centralizes the fixture data bag name for
// operational search tests that should not duplicate Chef-facing constants.
func encryptedDataBagSearchIndexName() string {
	return testfixtures.EncryptedDataBagName()
}

// encryptedDataBagSearchItemName centralizes the fixture item ID for reindex
// and repair assertions that work with provider document IDs.
func encryptedDataBagSearchItemName() string {
	return testfixtures.EncryptedDataBagItemID()
}

func requireDocumentRef(t *testing.T, docs []Document, index, name string) {
	t.Helper()

	for _, doc := range docs {
		if doc.Resource.Organization == "ponyville" && doc.Index == index && doc.Name == name {
			return
		}
	}
	t.Fatalf("docs = %v, want ponyville/%s/%s", docs, index, name)
}
