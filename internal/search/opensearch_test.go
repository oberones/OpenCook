package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
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

func TestOpenSearchClientDiscoverProviderParsesOpenSearchCapabilities(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/prefix":
			return http.StatusOK, `{
				"name":"search-node-1",
				"cluster_name":"opencook-functional",
				"version":{
					"distribution":"opensearch",
					"number":"2.12.0",
					"build_type":"tar",
					"build_hash":"abc123"
				},
				"tagline":"The OpenSearch Project: https://opensearch.org/"
			}`
		case r.Method == http.MethodHead && r.URL.Path == "/prefix/chef":
			return http.StatusNotFound, ""
		default:
			t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
			return 0, ""
		}
	})
	client, err := NewOpenSearchClient("http://opensearch.local/prefix", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	info, err := client.DiscoverProvider(context.Background())
	if err != nil {
		t.Fatalf("DiscoverProvider() error = %v", err)
	}
	if info.Distribution != "opensearch" || info.Version != "2.12.0" {
		t.Fatalf("provider identity = %+v, want opensearch 2.12.0", info)
	}
	if !info.VersionParsed || info.Major != 2 || info.Minor != 12 || info.Patch != 0 {
		t.Fatalf("provider version parse = parsed %v %d.%d.%d, want true 2.12.0", info.VersionParsed, info.Major, info.Minor, info.Patch)
	}
	if info.NodeName != "search-node-1" || info.ClusterName != "opencook-functional" || info.BuildType != "tar" || info.BuildHash != "abc123" {
		t.Fatalf("provider metadata = %+v, want node/cluster/build details", info)
	}
	requireOpenSearchCapabilities(t, info.Capabilities, OpenSearchCapabilities{
		IndexExistsChecks:        true,
		CreateIndex:              true,
		PutMapping:               true,
		BulkIndexing:             true,
		SearchIDs:                true,
		SearchAfterPagination:    true,
		Refresh:                  true,
		DeleteDocument:           true,
		DeleteByQuery:            true,
		TotalHitsObjectResponses: true,
	})
	cached, ok := client.ProviderInfo()
	if !ok {
		t.Fatal("ProviderInfo() ok = false, want cached discovery snapshot")
	}
	if !reflect.DeepEqual(cached, info) {
		t.Fatalf("ProviderInfo() = %+v, want %+v", cached, info)
	}

	requests := transport.Requests()
	requireRecordedRequest(t, requests, 0, http.MethodGet, "/prefix", "")
	requireRecordedRequest(t, requests, 1, http.MethodHead, "/prefix/chef", "")
}

func TestOpenSearchClientDiscoverProviderInfersVersionedCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		root       string
		want       OpenSearchProviderInfo
		wantCaps   OpenSearchCapabilities
		headStatus int
	}{
		{
			name: "elasticsearch six has legacy total hits",
			root: `{
				"version":{
					"number":"6.8.23",
					"build_flavor":"oss",
					"build_type":"docker",
					"build_hash":"es6"
				},
				"tagline":"You Know, for Search"
			}`,
			want: OpenSearchProviderInfo{
				Distribution:  "elasticsearch",
				Version:       "6.8.23",
				VersionParsed: true,
				Major:         6,
				Minor:         8,
				Patch:         23,
				Tagline:       "You Know, for Search",
				BuildFlavor:   "oss",
				BuildType:     "docker",
				BuildHash:     "es6",
			},
			wantCaps: OpenSearchCapabilities{
				IndexExistsChecks:     true,
				CreateIndex:           true,
				PutMapping:            true,
				BulkIndexing:          true,
				SearchIDs:             true,
				SearchAfterPagination: true,
				Refresh:               true,
				DeleteDocument:        true,
				DeleteByQuery:         true,
			},
			headStatus: http.StatusOK,
		},
		{
			name: "unknown future provider keeps required capabilities enabled",
			root: `{
				"version":{
					"distribution":"galaxysearch",
					"number":"99.1.2-SNAPSHOT",
					"build_hash":"future"
				},
				"tagline":"compatible search"
			}`,
			want: OpenSearchProviderInfo{
				Distribution:  "galaxysearch",
				Version:       "99.1.2-SNAPSHOT",
				VersionParsed: true,
				Major:         99,
				Minor:         1,
				Patch:         2,
				Tagline:       "compatible search",
				BuildHash:     "future",
			},
			wantCaps: OpenSearchCapabilities{
				IndexExistsChecks:        true,
				CreateIndex:              true,
				PutMapping:               true,
				BulkIndexing:             true,
				SearchIDs:                true,
				SearchAfterPagination:    true,
				Refresh:                  true,
				DeleteDocument:           true,
				DeleteByQuery:            true,
				TotalHitsObjectResponses: true,
			},
			headStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/":
					return http.StatusOK, tt.root
				case r.Method == http.MethodHead && r.URL.Path == "/chef":
					return tt.headStatus, ""
				default:
					t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
					return 0, ""
				}
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}
			info, err := client.DiscoverProvider(context.Background())
			if err != nil {
				t.Fatalf("DiscoverProvider() error = %v", err)
			}
			want := tt.want
			want.Capabilities = tt.wantCaps
			if !reflect.DeepEqual(info, want) {
				t.Fatalf("DiscoverProvider() = %+v, want %+v", info, want)
			}
		})
	}
}

func TestOpenSearchClientDiscoverProviderFailuresAreClassifiedAndRedacted(t *testing.T) {
	tests := []struct {
		name    string
		handler func(*http.Request, recordedOpenSearchRequest) (int, string)
		wantErr error
	}{
		{
			name: "root unavailable",
			handler: func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodGet || r.URL.Path != "/" {
					t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
				}
				return http.StatusServiceUnavailable, "raw provider body from internal cluster correct horse battery staple"
			},
			wantErr: ErrUnavailable,
		},
		{
			name: "malformed root response",
			handler: func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodGet || r.URL.Path != "/" {
					t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
				}
				return http.StatusOK, `{"cluster_name":"correct horse battery staple"`
			},
			wantErr: ErrUnavailable,
		},
		{
			name: "index capability rejected",
			handler: func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/":
					return http.StatusOK, `{"version":{"distribution":"opensearch","number":"2.12.0"}}`
				case r.Method == http.MethodHead && r.URL.Path == "/chef":
					return http.StatusUnauthorized, "raw provider body from internal cluster correct horse battery staple"
				default:
					t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
					return 0, ""
				}
			},
			wantErr: ErrRejected,
		},
		{
			name: "legacy elasticsearch without search after is rejected",
			handler: func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == "/":
					return http.StatusOK, `{"version":{"number":"2.4.6","build_hash":"es2"},"tagline":"You Know, for Search"}`
				case r.Method == http.MethodHead && r.URL.Path == "/chef":
					return http.StatusOK, ""
				default:
					t.Fatalf("unexpected discovery request %s %s", r.Method, r.URL.String())
					return 0, ""
				}
			},
			wantErr: ErrInvalidConfiguration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(newRecordingOpenSearchTransport(t, tt.handler)))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}
			_, err = client.DiscoverProvider(context.Background())
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("DiscoverProvider() error = %v, want %v", err, tt.wantErr)
			}
			if strings.Contains(err.Error(), "correct horse battery staple") || strings.Contains(err.Error(), "internal cluster") {
				t.Fatalf("DiscoverProvider() error leaked provider detail: %q", err.Error())
			}
			if _, ok := client.ProviderInfo(); ok {
				t.Fatal("ProviderInfo() ok = true after failed discovery, want false")
			}
		})
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
	requireOpenSearchChefMappingDescriptor(t, body["mappings"].(map[string]any))
}

func TestOpenSearchClientEnsureChefIndexVersionedMappingLifecycle(t *testing.T) {
	tests := []struct {
		name    string
		handler func(*testing.T, *int, *http.Request, recordedOpenSearchRequest) (int, string)
		wantErr error
		want    []recordedOpenSearchRequest
	}{
		{
			name: "existing compatible mapping skips update",
			handler: func(t *testing.T, requestCount *int, r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				(*requestCount)++
				switch *requestCount {
				case 1:
					if r.Method != http.MethodHead || r.URL.Path != "/chef" {
						t.Fatalf("request 1 = %s %s, want HEAD /chef", r.Method, r.URL.Path)
					}
					return http.StatusOK, ""
				case 2:
					if r.Method != http.MethodGet || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 2 = %s %s, want GET /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusOK, openSearchMappingResponse(t, openSearchChefIndexDescriptor.Mapping())
				default:
					t.Fatalf("unexpected request %d: %s %s", *requestCount, r.Method, r.URL.String())
					return http.StatusInternalServerError, ""
				}
			},
			want: []recordedOpenSearchRequest{
				{Method: http.MethodHead, Path: "/chef"},
				{Method: http.MethodGet, Path: "/chef/_mapping"},
			},
		},
		{
			name: "older compatible mapping updates metadata",
			handler: func(t *testing.T, requestCount *int, r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
				(*requestCount)++
				switch *requestCount {
				case 1:
					if r.Method != http.MethodHead || r.URL.Path != "/chef" {
						t.Fatalf("request 1 = %s %s, want HEAD /chef", r.Method, r.URL.Path)
					}
					return http.StatusOK, ""
				case 2:
					if r.Method != http.MethodGet || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 2 = %s %s, want GET /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusOK, openSearchMappingResponse(t, openSearchLegacyChefMapping())
				case 3:
					if r.Method != http.MethodPut || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 3 = %s %s, want PUT /chef/_mapping", r.Method, r.URL.Path)
					}
					requireOpenSearchChefMappingDescriptor(t, decodeJSONMap(t, recorded.Body))
					return http.StatusOK, `{"acknowledged":true}`
				default:
					t.Fatalf("unexpected request %d: %s %s", *requestCount, r.Method, r.URL.String())
					return http.StatusInternalServerError, ""
				}
			},
			want: []recordedOpenSearchRequest{
				{Method: http.MethodHead, Path: "/chef"},
				{Method: http.MethodGet, Path: "/chef/_mapping"},
				{Method: http.MethodPut, Path: "/chef/_mapping"},
			},
		},
		{
			name: "mapping conflict is rejected and redacted",
			handler: func(t *testing.T, requestCount *int, r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				(*requestCount)++
				switch *requestCount {
				case 1:
					return http.StatusOK, ""
				case 2:
					if r.Method != http.MethodGet || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 2 = %s %s, want GET /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusOK, openSearchMappingResponse(t, map[string]any{
						"dynamic": true,
						"properties": map[string]any{
							"document_id": map[string]any{"type": "text"},
							openSearchCompatTermsField: map[string]any{
								"type": "keyword",
							},
						},
					})
				case 3:
					if r.Method != http.MethodPut || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 3 = %s %s, want PUT /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusConflict, "raw provider body with internal cluster correct horse battery staple"
				default:
					t.Fatalf("unexpected request %d: %s %s", *requestCount, r.Method, r.URL.String())
					return http.StatusInternalServerError, ""
				}
			},
			wantErr: ErrRejected,
			want: []recordedOpenSearchRequest{
				{Method: http.MethodHead, Path: "/chef"},
				{Method: http.MethodGet, Path: "/chef/_mapping"},
				{Method: http.MethodPut, Path: "/chef/_mapping"},
			},
		},
		{
			name: "malformed mapping response is unavailable",
			handler: func(t *testing.T, requestCount *int, r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				(*requestCount)++
				switch *requestCount {
				case 1:
					return http.StatusOK, ""
				case 2:
					if r.Method != http.MethodGet || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 2 = %s %s, want GET /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusOK, `{"chef":{"mappings":"not-an-object"}}`
				default:
					t.Fatalf("unexpected request %d: %s %s", *requestCount, r.Method, r.URL.String())
					return http.StatusInternalServerError, ""
				}
			},
			wantErr: ErrUnavailable,
			want: []recordedOpenSearchRequest{
				{Method: http.MethodHead, Path: "/chef"},
				{Method: http.MethodGet, Path: "/chef/_mapping"},
			},
		},
		{
			name: "create race validates existing compatible mapping",
			handler: func(t *testing.T, requestCount *int, r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
				(*requestCount)++
				switch *requestCount {
				case 1:
					if r.Method != http.MethodHead || r.URL.Path != "/chef" {
						t.Fatalf("request 1 = %s %s, want HEAD /chef", r.Method, r.URL.Path)
					}
					return http.StatusNotFound, ""
				case 2:
					if r.Method != http.MethodPut || r.URL.Path != "/chef" {
						t.Fatalf("request 2 = %s %s, want PUT /chef", r.Method, r.URL.Path)
					}
					body := decodeJSONMap(t, recorded.Body)
					requireOpenSearchChefMappingDescriptor(t, body["mappings"].(map[string]any))
					return http.StatusConflict, `{"error":{"type":"resource_already_exists_exception"}}`
				case 3:
					if r.Method != http.MethodGet || r.URL.Path != "/chef/_mapping" {
						t.Fatalf("request 3 = %s %s, want GET /chef/_mapping", r.Method, r.URL.Path)
					}
					return http.StatusOK, openSearchMappingResponse(t, openSearchChefIndexDescriptor.Mapping())
				default:
					t.Fatalf("unexpected request %d: %s %s", *requestCount, r.Method, r.URL.String())
					return http.StatusInternalServerError, ""
				}
			},
			want: []recordedOpenSearchRequest{
				{Method: http.MethodHead, Path: "/chef"},
				{Method: http.MethodPut, Path: "/chef"},
				{Method: http.MethodGet, Path: "/chef/_mapping"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestCount := 0
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
				return tt.handler(t, &requestCount, r, recorded)
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}
			err = client.EnsureChefIndex(context.Background())
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("EnsureChefIndex() error = %v, want %v", err, tt.wantErr)
				}
				if strings.Contains(err.Error(), "correct horse battery staple") || strings.Contains(err.Error(), "internal cluster") {
					t.Fatalf("EnsureChefIndex() error leaked provider body: %q", err.Error())
				}
			} else if err != nil {
				t.Fatalf("EnsureChefIndex() error = %v", err)
			}
			requests := transport.Requests()
			if len(requests) != len(tt.want) {
				t.Fatalf("requests len = %d, want %d (%v)", len(requests), len(tt.want), requests)
			}
			for i, want := range tt.want {
				requireRecordedRequest(t, requests, i, want.Method, want.Path, want.Query)
			}
		})
	}
}

func TestOpenSearchClientProviderContractRequestSequence(t *testing.T) {
	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/prefix" {
				t.Fatalf("ping request = %s %s, want GET /prefix", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"version":{"distribution":"opensearch","number":"2.12.0"}}`
		case 2:
			if r.Method != http.MethodHead || r.URL.Path != "/prefix/chef" {
				t.Fatalf("index exists request = %s %s, want HEAD /prefix/chef", r.Method, r.URL.Path)
			}
			return http.StatusOK, ""
		case 3:
			if r.Method != http.MethodGet || r.URL.Path != "/prefix/chef/_mapping" {
				t.Fatalf("mapping check request = %s %s, want GET /prefix/chef/_mapping", r.Method, r.URL.Path)
			}
			return http.StatusOK, openSearchMappingResponse(t, openSearchLegacyChefMapping())
		case 4:
			if r.Method != http.MethodPut || r.URL.Path != "/prefix/chef/_mapping" {
				t.Fatalf("mapping request = %s %s, want PUT /prefix/chef/_mapping", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"acknowledged":true}`
		case 5:
			if r.Method != http.MethodPost || r.URL.Path != "/prefix/_bulk" {
				t.Fatalf("bulk request = %s %s, want POST /prefix/_bulk", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"errors":false}`
		case 6:
			if r.Method != http.MethodPost || r.URL.Path != "/prefix/chef/_search" {
				t.Fatalf("search request = %s %s, want POST /prefix/chef/_search", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"hits":{"hits":[{"_id":"ponyville/node/twilight","_source":{"name":["provider-spoof"]}}]}}`
		case 7:
			if r.Method != http.MethodPost || r.URL.Path != "/prefix/chef/_refresh" {
				t.Fatalf("refresh request = %s %s, want POST /prefix/chef/_refresh", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"_shards":{"successful":1}}`
		case 8:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/prefix/chef/_doc/ponyville%2Fnode%2Ftwilight" {
				t.Fatalf("delete document request = %s %s escaped=%s, want DELETE /prefix/chef/_doc/ponyville%%2Fnode%%2Ftwilight", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusOK, `{"result":"deleted"}`
		case 9:
			if r.Method != http.MethodGet || r.URL.Path != "/prefix" {
				t.Fatalf("delete-by-query discovery request = %s %s, want GET /prefix", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"version":{"distribution":"opensearch","number":"2.12.0"}}`
		case 10:
			if r.Method != http.MethodHead || r.URL.Path != "/prefix/chef" {
				t.Fatalf("delete-by-query discovery index request = %s %s, want HEAD /prefix/chef", r.Method, r.URL.Path)
			}
			return http.StatusOK, ""
		case 11:
			if r.Method != http.MethodPost || r.URL.Path != "/prefix/chef/_delete_by_query" || r.URL.RawQuery != "refresh=true" {
				t.Fatalf("delete-by-query request = %s %s?%s, want POST /prefix/chef/_delete_by_query?refresh=true", r.Method, r.URL.Path, r.URL.RawQuery)
			}
			return http.StatusOK, `{"deleted":1}`
		default:
			t.Fatalf("unexpected provider request %d: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})

	client, err := NewOpenSearchClient("http://opensearch.local/prefix", WithOpenSearchTransport(transport))
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
	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping() error = %v", err)
	}
	if err := client.EnsureChefIndex(context.Background()); err != nil {
		t.Fatalf("EnsureChefIndex() error = %v", err)
	}
	if err := client.BulkUpsert(context.Background(), []Document{doc}); err != nil {
		t.Fatalf("BulkUpsert() error = %v", err)
	}
	ids, err := client.SearchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:twi*",
	})
	if err != nil {
		t.Fatalf("SearchIDs() error = %v", err)
	}
	if got, want := strings.Join(ids, ","), "ponyville/node/twilight"; got != want {
		t.Fatalf("SearchIDs() = %s, want %s", got, want)
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
	if len(requests) != 11 {
		t.Fatalf("requests len = %d, want 11", len(requests))
	}
	requireRecordedRequest(t, requests, 0, http.MethodGet, "/prefix", "")
	requireRecordedRequest(t, requests, 1, http.MethodHead, "/prefix/chef", "")
	requireRecordedRequest(t, requests, 2, http.MethodGet, "/prefix/chef/_mapping", "")
	requireRecordedRequest(t, requests, 3, http.MethodPut, "/prefix/chef/_mapping", "")
	requireRecordedRequest(t, requests, 4, http.MethodPost, "/prefix/_bulk", "")
	requireRecordedRequest(t, requests, 5, http.MethodPost, "/prefix/chef/_search", "")
	requireRecordedRequest(t, requests, 6, http.MethodPost, "/prefix/chef/_refresh", "")
	requireRecordedRequest(t, requests, 7, http.MethodDelete, "/prefix/chef/_doc/ponyville/node/twilight", "")
	requireRecordedRequest(t, requests, 8, http.MethodGet, "/prefix", "")
	requireRecordedRequest(t, requests, 9, http.MethodHead, "/prefix/chef", "")
	requireRecordedRequest(t, requests, 10, http.MethodPost, "/prefix/chef/_delete_by_query", "refresh=true")

	requireOpenSearchChefMappingDescriptor(t, decodeJSONMap(t, requests[3].Body))

	if got := requests[4].Header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("bulk Content-Type = %q, want application/x-ndjson", got)
	}
	lines := strings.Split(strings.TrimSpace(requests[4].Body), "\n")
	if len(lines) != 2 {
		t.Fatalf("bulk lines = %d, want 2 (%q)", len(lines), requests[4].Body)
	}
	bulkAction := decodeJSONMap(t, lines[0])["index"].(map[string]any)
	if bulkAction["_index"] != openSearchChefIndex || bulkAction["_id"] != "ponyville/node/twilight" {
		t.Fatalf("bulk action = %v, want chef index and ponyville/node/twilight id", bulkAction)
	}
	bulkSource := decodeJSONMap(t, lines[1])
	if bulkSource["document_id"] != "ponyville/node/twilight" || bulkSource["organization"] != "ponyville" || bulkSource["index"] != "node" {
		t.Fatalf("bulk source identity = %v, want ponyville/node/twilight", bulkSource)
	}
	requireJSONListContains(t, bulkSource[openSearchCompatTermsField], "__org=ponyville")
	requireJSONListContains(t, bulkSource[openSearchCompatTermsField], "__index=node")

	searchBody := decodeJSONMap(t, requests[5].Body)
	if searchBody["_source"] != false {
		t.Fatalf("search _source = %v, want false", searchBody["_source"])
	}
	boolQuery := searchBody["query"].(map[string]any)["bool"].(map[string]any)
	requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__org=ponyville")
	requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__index=node")
	requireCompatPrefixClause(t, boolQuery["must"], "name=twi")

	deleteBody := decodeJSONMap(t, requests[10].Body)
	deleteBool := deleteBody["query"].(map[string]any)["bool"].(map[string]any)
	requireTermFilter(t, deleteBool["filter"], "organization", "ponyville")
	requireTermFilter(t, deleteBool["filter"], "index", "node")
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
	if !strings.HasSuffix(recorded.Body, "\n") {
		t.Fatalf("bulk body = %q, want trailing newline for NDJSON compatibility", recorded.Body)
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

func TestOpenSearchClientBulkUpsertEmptyIsNoop(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		t.Fatal("provider request was made for empty bulk upsert")
		return http.StatusInternalServerError, ""
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	if err := client.BulkUpsert(context.Background(), nil); err != nil {
		t.Fatalf("BulkUpsert(nil) error = %v", err)
	}
	if err := client.BulkUpsert(context.Background(), []Document{}); err != nil {
		t.Fatalf("BulkUpsert(empty) error = %v", err)
	}
	if requests := transport.Requests(); len(requests) != 0 {
		t.Fatalf("provider requests = %d, want 0", len(requests))
	}
}

func TestOpenSearchClientBulkUpsertItemFailuresAreClassifiedAndRedacted(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantErr    error
	}{
		{
			name:       "mixed success and retryable item failure",
			statusCode: http.StatusOK,
			body: `{
				"errors": true,
				"items": [
					{"index":{"_id":"ponyville/node/twilight","status":201}},
					{"index":{"_id":"ponyville/node/rarity","status":429,"error":{"reason":"correct horse battery staple internal cluster overloaded"}}}
				]
			}`,
			wantErr: ErrUnavailable,
		},
		{
			name:       "mixed success and rejected item failure",
			statusCode: http.StatusOK,
			body: `{
				"errors": true,
				"items": [
					{"index":{"_id":"ponyville/node/twilight","status":200}},
					{"index":{"_id":"ponyville/node/rarity","status":400,"error":{"reason":"correct horse battery staple mapping internals"}}}
				]
			}`,
			wantErr: ErrRejected,
		},
		{
			name:       "bulk endpoint retryable status",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"error":"correct horse battery staple internal cluster"}`,
			wantErr:    ErrUnavailable,
		},
		{
			name:       "bulk endpoint rejected status",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"correct horse battery staple mapping internals"}`,
			wantErr:    ErrRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodPost || r.URL.Path != "/_bulk" {
					t.Fatalf("request = %s %s, want POST /_bulk", r.Method, r.URL.Path)
				}
				return tt.statusCode, tt.body
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}

			err = client.BulkUpsert(context.Background(), []Document{{Index: "node", Name: "twilight"}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("BulkUpsert() error = %v, want %v", err, tt.wantErr)
			}
			for _, leaked := range []string{"correct horse battery staple", "internal cluster", "mapping internals"} {
				if strings.Contains(err.Error(), leaked) {
					t.Fatalf("BulkUpsert() error leaked %q: %q", leaked, err.Error())
				}
			}
		})
	}
}

func TestOpenSearchClientBulkUpsertMalformedResponsesAreUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{"errors": true`},
		{name: "missing errors field", body: `{}`},
		{name: "errors true without items", body: `{"errors":true}`},
		{name: "errors true with missing item status", body: `{"errors":true,"items":[{"index":{"_id":"ponyville/node/twilight"}}]}`},
		{name: "errors true with empty item envelope", body: `{"errors":true,"items":[{}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodPost || r.URL.Path != "/_bulk" {
					t.Fatalf("request = %s %s, want POST /_bulk", r.Method, r.URL.Path)
				}
				return http.StatusOK, tt.body
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}

			err = client.BulkUpsert(context.Background(), []Document{{Index: "node", Name: "twilight"}})
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("BulkUpsert() error = %v, want ErrUnavailable", err)
			}
		})
	}
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

func TestOpenSearchClientSearchIDsProviderResponseVariants(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{
			name: "integer total and unexpected source",
			body: `{"hits":{"total":2,"hits":[` +
				`{"_id":"ponyville/node/twilight","_source":{"name":["provider-spoof"],"secret":"must-not-leak"}},` +
				`{"_id":"ponyville/node/rarity"}` +
				`]}}`,
			want: []string{"ponyville/node/twilight", "ponyville/node/rarity"},
		},
		{
			name: "object total",
			body: `{"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"ponyville/node/fluttershy"}]}}`,
			want: []string{"ponyville/node/fluttershy"},
		},
		{
			name: "missing hits array",
			body: `{"hits":{"total":0}}`,
		},
		{
			name: "empty hits array",
			body: `{"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`,
		},
		{
			name: "missing hit id is ignored",
			body: `{"hits":{"total":{"value":2,"relation":"eq"},"hits":[` +
				`{"sort":["ponyville/node/provider-only"],"_source":{"name":["provider-only"]}},` +
				`{"_id":"ponyville/node/twilight"}` +
				`]}}`,
			want: []string{"ponyville/node/twilight"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
					t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
				}
				return http.StatusOK, tt.body
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}

			got, err := client.SearchIDs(context.Background(), Query{
				Organization: "ponyville",
				Index:        "node",
				Q:            "name:*",
			})
			if err != nil {
				t.Fatalf("SearchIDs() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("SearchIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpenSearchSearchBodyUsesSharedASTRequestShapes(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  map[string]any
	}{
		{
			name:  "boolean",
			query: "role:web AND NOT recipe:missing",
			want: map[string]any{"bool": map[string]any{
				"must":     []any{openSearchCompatTermClause("role", "web")},
				"must_not": []any{openSearchCompatTermClause("recipe", "missing")},
			}},
		},
		{
			name:  "grouped",
			query: "(team:friendship OR recipe:missing) AND name:twilight",
			want: map[string]any{"bool": map[string]any{
				"must": []any{
					map[string]any{"bool": map[string]any{
						"should": []any{
							openSearchCompatRequiredClause(openSearchCompatTermClause("team", "friendship")),
							openSearchCompatRequiredClause(openSearchCompatTermClause("recipe", "missing")),
						},
						"minimum_should_match": 1,
					}},
					openSearchCompatTermClause("name", "twilight"),
				},
			}},
		},
		{
			name:  "quoted phrase",
			query: `note:"hello world"`,
			want:  openSearchCompatRequiredClause(openSearchCompatTermClause("note", "hello world")),
		},
		{
			name:  "existence",
			query: "name:*",
			want:  openSearchCompatRequiredClause(openSearchCompatPrefixClause("name", "")),
		},
		{
			name:  "wildcard value",
			query: "name:*light",
			want:  openSearchCompatRequiredClause(openSearchCompatWildcardClause("name", "*light")),
		},
		{
			name:  "wildcard field and value",
			query: "te*:friend*",
			want:  openSearchCompatRequiredClause(openSearchCompatWildcardClause("te*", "friend*")),
		},
		{
			name:  "range",
			query: "build:[001 TO 099]",
			want:  openSearchCompatRequiredClause(openSearchCompatRangeClause("build", "001", "099", true, true, false, false)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, err := openSearchSearchBody(Query{
				Organization: "ponyville",
				Index:        "node",
				Q:            tt.query,
			}, 25, []any{"ponyville/node/applejack"})
			if err != nil {
				t.Fatalf("openSearchSearchBody(%q) error = %v", tt.query, err)
			}
			if body["_source"] != false || body["size"] != 25 {
				t.Fatalf("body paging/source = %v, want _source false size 25", body)
			}
			searchAfter := body["search_after"].([]any)
			if len(searchAfter) != 1 || searchAfter[0] != "ponyville/node/applejack" {
				t.Fatalf("search_after = %v, want previous document id", searchAfter)
			}
			boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
			requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__org=ponyville")
			requireTermFilter(t, boolQuery["filter"], openSearchCompatTermsField, "__index=node")
			if got := boolQuery["must"].(map[string]any); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("must clause for %q = %#v, want %#v", tt.query, got, tt.want)
			}
			rawBody, err := json.Marshal(body)
			if err != nil {
				t.Fatalf("json.Marshal(body) error = %v", err)
			}
			if strings.Contains(string(rawBody), "query_string") {
				t.Fatalf("body used provider query_string instead of shared AST clause: %s", rawBody)
			}
		})
	}
}

// openSearchCompatRequiredClause mirrors the bool.must wrapper emitted by
// andNode so OpenSearch request-shape tests compare against the real AST output.
func openSearchCompatRequiredClause(children ...map[string]any) map[string]any {
	must := make([]any, 0, len(children))
	for _, child := range children {
		must = append(must, child)
	}
	return map[string]any{"bool": map[string]any{"must": must}}
}

func TestOpenSearchClientSearchIDsInvalidQueryDoesNotContactProvider(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		t.Fatal("provider request was made for invalid query")
		return http.StatusInternalServerError, ""
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	_, err = client.SearchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "build:[001 099]",
	})
	if !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("SearchIDs(invalid query) error = %v, want ErrInvalidQuery", err)
	}
	if requests := transport.Requests(); len(requests) != 0 {
		t.Fatalf("provider requests = %d, want 0", len(requests))
	}
}

func TestOpenSearchClientSearchIDsRejectedProviderDoesNotLeakBodies(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusBadRequest, "raw provider body with query parse internals"
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	_, err = client.SearchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:twilight",
	})
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("SearchIDs(provider rejected) error = %v, want ErrRejected", err)
	}
	if strings.Contains(err.Error(), "query parse internals") {
		t.Fatalf("SearchIDs(provider rejected) leaked provider body: %q", err.Error())
	}
}

func TestOpenSearchClientSearchIDsMalformedProviderResponsesAreUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{"hits":{"hits":[`},
		{name: "hits is wrong type", body: `{"hits":"correct horse battery staple"}`},
		{name: "hit array is wrong type", body: `{"hits":{"hits":"correct horse battery staple"}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
				return http.StatusOK, tt.body
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}

			_, err = client.SearchIDs(context.Background(), Query{
				Organization: "ponyville",
				Index:        "node",
				Q:            "name:twilight",
			})
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("SearchIDs(malformed response) error = %v, want ErrUnavailable", err)
			}
			if strings.Contains(err.Error(), "correct horse battery staple") {
				t.Fatalf("SearchIDs(malformed response) leaked provider body: %q", err.Error())
			}
		})
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

func TestOpenSearchClientSearchIDsRequiresSortOnNonFinalPage(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		body := decodeJSONMap(t, recorded.Body)
		if body["size"] != float64(2) {
			t.Fatalf("search size = %v, want 2", body["size"])
		}
		return http.StatusOK, `{"hits":{"total":{"value":3,"relation":"eq"},"hits":[` +
			`{"sort":["ponyville/node/provider-only"],"_source":{"name":["provider-only"]}},` +
			`{"_id":"ponyville/node/twilight"}` +
			`]}}`
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	_, err = client.searchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:*",
	}, 2)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("searchIDs(missing non-final sort) error = %v, want ErrUnavailable", err)
	}
	if !strings.Contains(err.Error(), "search_after sort value missing") {
		t.Fatalf("searchIDs(missing non-final sort) error = %q, want search_after context", err.Error())
	}
}

func TestOpenSearchClientSearchIDsPaginatesPastDefaultPageSizeWithoutLowCap(t *testing.T) {
	ids := make([]string, 1005)
	for i := range ids {
		ids[i] = fmt.Sprintf("ponyville/node/node-%04d", i)
	}

	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
			t.Fatalf("request = %s %s, want POST /chef/_search", r.Method, r.URL.Path)
		}
		body := decodeJSONMap(t, recorded.Body)
		if body["size"] != float64(openSearchSearchPageSize) {
			t.Fatalf("request %d size = %v, want default page size %d", requestCount, body["size"], openSearchSearchPageSize)
		}
		switch requestCount {
		case 1:
			if _, ok := body["search_after"]; ok {
				t.Fatalf("first search request search_after = %v, want omitted", body["search_after"])
			}
			return http.StatusOK, openSearchHitsResponse(t, ids[:openSearchSearchPageSize], 250000)
		case 2:
			searchAfter := body["search_after"].([]any)
			if len(searchAfter) != 1 || searchAfter[0] != ids[openSearchSearchPageSize-1] {
				t.Fatalf("second search_after = %v, want last first-page document id", searchAfter)
			}
			return http.StatusOK, openSearchHitsResponse(t, ids[openSearchSearchPageSize:], 250000)
		default:
			t.Fatalf("unexpected search request %d", requestCount)
			return http.StatusInternalServerError, ""
		}
	})

	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	got, err := client.SearchIDs(context.Background(), Query{
		Organization: "ponyville",
		Index:        "node",
		Q:            "name:node-*",
	})
	if err != nil {
		t.Fatalf("SearchIDs(large result) error = %v", err)
	}
	if len(got) != len(ids) || got[0] != ids[0] || got[len(got)-1] != ids[len(ids)-1] {
		t.Fatalf("SearchIDs(large result) = len %d first/last %q/%q, want len %d first/last %q/%q", len(got), got[0], got[len(got)-1], len(ids), ids[0], ids[len(ids)-1])
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
		case r.Method == http.MethodGet && r.URL.Path == "/":
			return http.StatusOK, `{"version":{"distribution":"opensearch","number":"2.12.0"}}`
		case r.Method == http.MethodHead && r.URL.Path == "/chef":
			return http.StatusOK, ""
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
	requireRecordedRequest(t, requests, 2, http.MethodGet, "/", "")
	requireRecordedRequest(t, requests, 3, http.MethodHead, "/chef", "")
	requireRecordedRequest(t, requests, 4, http.MethodPost, "/chef/_delete_by_query", "refresh=true")
	body := decodeJSONMap(t, requests[4].Body)
	boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
	requireTermFilter(t, boolQuery["filter"], "organization", "ponyville")
	requireTermFilter(t, boolQuery["filter"], "index", "node")
}

func TestOpenSearchClientDeleteByQueryFallsBackWhenCachedCapabilitiesRequireIt(t *testing.T) {
	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
				t.Fatalf("request 1 = %s %s, want fallback POST /chef/_search", r.Method, r.URL.Path)
			}
			body := decodeJSONMap(t, recorded.Body)
			if body["_source"] != false || body["size"] != float64(openSearchSearchPageSize) {
				t.Fatalf("fallback search body = %v, want _source false and default page size", body)
			}
			boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
			requireTermFilter(t, boolQuery["filter"], "organization", "ponyville")
			requireTermFilter(t, boolQuery["filter"], "index", "node")
			return http.StatusOK, `{"hits":{"hits":[` +
				`{"_id":"ponyville/node/applejack","sort":["ponyville/node/applejack"]},` +
				`{"_id":"ponyville/node/twilight","sort":["ponyville/node/twilight"]}` +
				`]}}`
		case 2:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/chef/_doc/ponyville%2Fnode%2Fapplejack" {
				t.Fatalf("request 2 = %s %s escaped=%s, want delete applejack", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusOK, `{"result":"deleted"}`
		case 3:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/chef/_doc/ponyville%2Fnode%2Ftwilight" {
				t.Fatalf("request 3 = %s %s escaped=%s, want delete twilight", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusOK, `{"result":"deleted"}`
		case 4:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_refresh" {
				t.Fatalf("request 4 = %s %s, want POST /chef/_refresh", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"refreshed":true}`
		default:
			t.Fatalf("unexpected request %d: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	client.setProviderInfo(OpenSearchProviderInfo{
		Distribution: "opensearch",
		Version:      "fallback-mode",
		Capabilities: OpenSearchCapabilities{
			SearchAfterPagination:         true,
			DeleteDocument:                true,
			DeleteByQueryFallbackRequired: true,
			Refresh:                       true,
		},
	})

	if err := client.DeleteByQuery(context.Background(), "ponyville", "node"); err != nil {
		t.Fatalf("DeleteByQuery(fallback) error = %v", err)
	}
	if requestCount != 4 {
		t.Fatalf("requestCount = %d, want 4", requestCount)
	}
}

func TestOpenSearchClientDeleteByQueryRejectsFallbackWithoutSearchAfter(t *testing.T) {
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		t.Fatalf("unexpected provider request after unsupported cached capabilities: %s %s", r.Method, r.URL.String())
		return 0, ""
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	client.setProviderInfo(OpenSearchProviderInfo{
		Distribution: "elasticsearch",
		Version:      "2.4.6",
		Capabilities: OpenSearchCapabilities{
			DeleteDocument: true,
		},
	})

	err = client.DeleteByQuery(context.Background(), "ponyville", "node")
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("DeleteByQuery(unsupported fallback) error = %v, want ErrInvalidConfiguration", err)
	}
	if !strings.Contains(err.Error(), "search_after pagination") {
		t.Fatalf("DeleteByQuery(unsupported fallback) error = %q, want search_after context", err.Error())
	}
}

func TestOpenSearchClientDeleteByQueryUnsupportedResponseFallsBack(t *testing.T) {
	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodGet || r.URL.Path != "/" {
				t.Fatalf("request 1 = %s %s, want discovery GET /", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"version":{"distribution":"opensearch","number":"2.12.0"}}`
		case 2:
			if r.Method != http.MethodHead || r.URL.Path != "/chef" {
				t.Fatalf("request 2 = %s %s, want discovery HEAD /chef", r.Method, r.URL.Path)
			}
			return http.StatusOK, ""
		case 3:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_delete_by_query" {
				t.Fatalf("request 3 = %s %s, want direct delete-by-query attempt", r.Method, r.URL.Path)
			}
			return http.StatusMethodNotAllowed, "correct horse battery staple direct delete-by-query unsupported"
		case 4:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
				t.Fatalf("request 4 = %s %s, want fallback search", r.Method, r.URL.Path)
			}
			body := decodeJSONMap(t, recorded.Body)
			boolQuery := body["query"].(map[string]any)["bool"].(map[string]any)
			requireTermFilter(t, boolQuery["filter"], "organization", "ponyville")
			return http.StatusOK, `{"hits":{"hits":[{"_id":"ponyville/node/twilight","sort":["ponyville/node/twilight"]}]}}`
		case 5:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/chef/_doc/ponyville%2Fnode%2Ftwilight" {
				t.Fatalf("request 5 = %s %s escaped=%s, want document delete", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusOK, `{"result":"deleted"}`
		case 6:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_refresh" {
				t.Fatalf("request 6 = %s %s, want fallback refresh", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"refreshed":true}`
		default:
			t.Fatalf("unexpected request %d: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	if err := client.DeleteByQuery(context.Background(), "ponyville", ""); err != nil {
		t.Fatalf("DeleteByQuery(unsupported direct fallback) error = %v", err)
	}
}

func TestOpenSearchClientDeleteByQueryFallbackPaginatesAndDeletesDeterministically(t *testing.T) {
	requestCount := 0
	deleted := make([]string, 0)
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, recorded recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
				t.Fatalf("request 1 = %s %s, want first fallback search", r.Method, r.URL.Path)
			}
			body := decodeJSONMap(t, recorded.Body)
			if _, ok := body["query"].(map[string]any)["match_all"].(map[string]any); !ok {
				t.Fatalf("fallback all-org search query = %v, want match_all", body["query"])
			}
			if _, ok := body["search_after"]; ok {
				t.Fatalf("first fallback search_after = %v, want omitted", body["search_after"])
			}
			return http.StatusOK, `{"hits":{"hits":[` +
				`{"_id":"ponyville/node/applejack","sort":["ponyville/node/applejack"]},` +
				`{"_id":"ponyville/node/fluttershy","sort":["ponyville/node/fluttershy"]}` +
				`]}}`
		case 2, 3, 5:
			escaped := strings.TrimPrefix(r.URL.EscapedPath(), "/chef/_doc/")
			id, err := url.PathUnescape(escaped)
			if r.Method != http.MethodDelete || err != nil {
				t.Fatalf("request %d = %s %s escaped=%s, want document delete", requestCount, r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			deleted = append(deleted, id)
			return http.StatusOK, `{"result":"deleted"}`
		case 4:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
				t.Fatalf("request 4 = %s %s, want second fallback search", r.Method, r.URL.Path)
			}
			body := decodeJSONMap(t, recorded.Body)
			searchAfter := body["search_after"].([]any)
			if len(searchAfter) != 1 || searchAfter[0] != "ponyville/node/fluttershy" {
				t.Fatalf("second fallback search_after = %v, want fluttershy", searchAfter)
			}
			return http.StatusOK, `{"hits":{"hits":[{"_id":"ponyville/node/twilight","sort":["ponyville/node/twilight"]}]}}`
		case 6:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_refresh" {
				t.Fatalf("request 6 = %s %s, want final refresh", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"refreshed":true}`
		default:
			t.Fatalf("unexpected request %d: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	if err := client.deleteByQueryFallbackWithPageSize(context.Background(), "", "", 2); err != nil {
		t.Fatalf("deleteByQueryFallbackWithPageSize() error = %v", err)
	}
	wantDeleted := []string{"ponyville/node/applejack", "ponyville/node/fluttershy", "ponyville/node/twilight"}
	if !sameStrings(deleted, wantDeleted) {
		t.Fatalf("deleted IDs = %v, want %v", deleted, wantDeleted)
	}
}

func TestOpenSearchClientDeleteByQueryFallbackPartialDeleteFailureIsRedacted(t *testing.T) {
	requestCount := 0
	transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
		requestCount++
		switch requestCount {
		case 1:
			if r.Method != http.MethodPost || r.URL.Path != "/chef/_search" {
				t.Fatalf("request 1 = %s %s, want fallback search", r.Method, r.URL.Path)
			}
			return http.StatusOK, `{"hits":{"hits":[` +
				`{"_id":"ponyville/node/applejack","sort":["ponyville/node/applejack"]},` +
				`{"_id":"ponyville/node/twilight","sort":["ponyville/node/twilight"]}` +
				`]}}`
		case 2:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/chef/_doc/ponyville%2Fnode%2Fapplejack" {
				t.Fatalf("request 2 = %s %s escaped=%s, want applejack delete", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusOK, `{"result":"deleted"}`
		case 3:
			if r.Method != http.MethodDelete || r.URL.EscapedPath() != "/chef/_doc/ponyville%2Fnode%2Ftwilight" {
				t.Fatalf("request 3 = %s %s escaped=%s, want twilight delete", r.Method, r.URL.Path, r.URL.EscapedPath())
			}
			return http.StatusServiceUnavailable, "correct horse battery staple internal cluster"
		default:
			t.Fatalf("unexpected request %d after partial delete failure: %s %s", requestCount, r.Method, r.URL.String())
			return http.StatusInternalServerError, ""
		}
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}

	err = client.deleteByQueryFallbackWithPageSize(context.Background(), "ponyville", "node", 2)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("deleteByQueryFallbackWithPageSize(partial failure) error = %v, want ErrUnavailable", err)
	}
	for _, leaked := range []string{"correct horse battery staple", "internal cluster"} {
		if strings.Contains(err.Error(), leaked) {
			t.Fatalf("partial fallback delete error leaked %q: %q", leaked, err.Error())
		}
	}
	if requestCount != 3 {
		t.Fatalf("requestCount = %d, want stop before refresh after failed delete", requestCount)
	}
}

func TestOpenSearchClientRefreshStatusesAreClassifiedAndRedacted(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		wantErr    error
	}{
		{name: "ok", statusCode: http.StatusOK},
		{name: "accepted", statusCode: http.StatusAccepted},
		{name: "unavailable", statusCode: http.StatusServiceUnavailable, wantErr: ErrUnavailable},
		{name: "rejected", statusCode: http.StatusBadRequest, wantErr: ErrRejected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := newRecordingOpenSearchTransport(t, func(r *http.Request, _ recordedOpenSearchRequest) (int, string) {
				if r.Method != http.MethodPost || r.URL.Path != "/chef/_refresh" {
					t.Fatalf("request = %s %s, want POST /chef/_refresh", r.Method, r.URL.Path)
				}
				return tt.statusCode, "correct horse battery staple internal cluster"
			})
			client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(transport))
			if err != nil {
				t.Fatalf("NewOpenSearchClient() error = %v", err)
			}

			err = client.Refresh(context.Background())
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("Refresh() error = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Refresh() error = %v, want %v", err, tt.wantErr)
			}
			for _, leaked := range []string{"correct horse battery staple", "internal cluster"} {
				if strings.Contains(err.Error(), leaked) {
					t.Fatalf("Refresh() error leaked %q: %q", leaked, err.Error())
				}
			}
		})
	}
}

func TestOpenSearchFailureClassificationMatrix(t *testing.T) {
	statuses := []struct {
		name       string
		statusCode int
		wantErr    error
		wantText   string
	}{
		{name: "malformed request", statusCode: http.StatusBadRequest, wantErr: ErrRejected, wantText: "malformed or unsupported"},
		{name: "unsupported api", statusCode: http.StatusNotFound, wantErr: ErrRejected, wantText: "malformed or unsupported"},
		{name: "authentication", statusCode: http.StatusUnauthorized, wantErr: ErrRejected, wantText: "authentication/configuration"},
		{name: "authorization", statusCode: http.StatusForbidden, wantErr: ErrRejected, wantText: "authentication/configuration"},
		{name: "mapping conflict", statusCode: http.StatusConflict, wantErr: ErrRejected, wantText: "conflict"},
		{name: "throttling", statusCode: http.StatusTooManyRequests, wantErr: ErrUnavailable, wantText: "throttled"},
		{name: "provider outage", statusCode: http.StatusBadGateway, wantErr: ErrUnavailable, wantText: "outage"},
	}
	for _, tt := range statuses {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyOpenSearchStatus(tt.statusCode, http.StatusOK)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("classifyOpenSearchStatus(%d) error = %v, want %v", tt.statusCode, err, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("classifyOpenSearchStatus(%d) error = %q, want text %q", tt.statusCode, err.Error(), tt.wantText)
			}
			requireOpenSearchErrorRedacted(t, err)
		})
	}

	transports := []struct {
		name     string
		err      error
		wantText string
	}{
		{name: "context cancellation", err: context.Canceled, wantText: "canceled"},
		{name: "context deadline", err: context.DeadlineExceeded, wantText: "deadline"},
		{name: "DNS failure", err: &net.DNSError{Err: "lookup secret-cluster.internal", Name: "secret-cluster.internal"}, wantText: "DNS lookup failed"},
		{name: "connect failure", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: permission denied to secret-cluster.internal")}, wantText: "connection failed"},
	}
	for _, tt := range transports {
		t.Run(tt.name, func(t *testing.T) {
			err := classifyOpenSearchTransportError(tt.err)
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("classifyOpenSearchTransportError() error = %v, want ErrUnavailable", err)
			}
			if !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("classifyOpenSearchTransportError() error = %q, want text %q", err.Error(), tt.wantText)
			}
			requireOpenSearchErrorRedacted(t, err)
		})
	}
}

func TestOpenSearchRequiredJSONBodyClassificationIsRedacted(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{
			name: "empty body",
			raw:  "",
		},
		{
			name: "malformed body",
			raw:  `{"cluster_name":"secret-cluster","token":"AKIAIOSFODNN7EXAMPLE","password":"correct horse battery staple"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var payload map[string]any
			err := decodeOpenSearchJSON(strings.NewReader(tt.raw), &payload, "test response")
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("decodeOpenSearchJSON() error = %v, want ErrUnavailable", err)
			}
			requireOpenSearchErrorRedacted(t, err)
		})
	}
}

func TestOpenSearchClientErrorClassificationDoesNotLeakProviderBodies(t *testing.T) {
	rejectedTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusBadRequest, "raw provider body with internal endpoint details secret-cluster AKIAIOSFODNN7EXAMPLE"
	})
	client, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(rejectedTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	err = client.Ping(context.Background())
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("Ping() error = %v, want ErrRejected", err)
	}
	requireOpenSearchErrorRedacted(t, err)

	unavailableTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusServiceUnavailable, "raw provider body from secret-cluster correct horse battery staple"
	})
	unavailableClient, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(unavailableTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(unavailable) error = %v", err)
	}
	if err := unavailableClient.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping(503) error = %v, want ErrUnavailable", err)
	} else {
		requireOpenSearchErrorRedacted(t, err)
	}

	timeoutClient, err := NewOpenSearchClient("http://opensearch.example", WithOpenSearchTransport(timeoutTransport{}))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(timeout) error = %v", err)
	}
	if err := timeoutClient.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping(timeout) error = %v, want ErrUnavailable", err)
	} else {
		requireOpenSearchErrorRedacted(t, err)
	}

	bulkFailureTransport := newRecordingOpenSearchTransport(t, func(*http.Request, recordedOpenSearchRequest) (int, string) {
		return http.StatusOK, `{"errors":true,"items":[{"index":{"status":400,"error":{"reason":"raw provider body with internal endpoint details secret-cluster AKIAIOSFODNN7EXAMPLE"}}}]}`
	})
	bulkFailureClient, err := NewOpenSearchClient("http://opensearch.local", WithOpenSearchTransport(bulkFailureTransport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient(bulk failure) error = %v", err)
	}
	if err := bulkFailureClient.BulkUpsert(context.Background(), []Document{{Index: "node", Name: "twilight"}}); !errors.Is(err, ErrRejected) {
		t.Fatalf("BulkUpsert(errors=true) error = %v, want ErrRejected", err)
	} else {
		requireOpenSearchErrorRedacted(t, err)
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

	known := OpenSearchProviderStatus(OpenSearchProviderInfo{
		Distribution: "opensearch",
		Version:      "2.12.0",
		Capabilities: OpenSearchCapabilities{
			SearchAfterPagination:    true,
			DeleteByQuery:            true,
			TotalHitsObjectResponses: true,
		},
	}, true)
	for _, want := range []string{"opensearch 2.12.0", "search-after pagination", "delete-by-query", "object total hits"} {
		if !strings.Contains(known.Message, want) {
			t.Fatalf("known provider status message = %q, want %q", known.Message, want)
		}
	}

	unknown := OpenSearchProviderStatus(OpenSearchProviderInfo{
		Distribution: "galaxysearch",
		Version:      "99.1.2",
		Capabilities: OpenSearchCapabilities{
			SearchAfterPagination:    true,
			DeleteByQuery:            true,
			TotalHitsObjectResponses: true,
		},
	}, true)
	if !strings.Contains(unknown.Message, "OpenSearch-compatible") || !strings.Contains(unknown.Message, "galaxysearch 99.1.2") {
		t.Fatalf("unknown provider status message = %q, want compatible provider wording", unknown.Message)
	}

	degraded := OpenSearchProviderStatus(OpenSearchProviderInfo{
		Distribution: "opensearch",
		Version:      "fallback-mode",
		Capabilities: OpenSearchCapabilities{
			SearchAfterPagination:         true,
			DeleteByQueryFallbackRequired: true,
		},
	}, true)
	if !strings.Contains(degraded.Message, "delete-by-query fallback required") || !strings.Contains(degraded.Message, "legacy total hits") {
		t.Fatalf("degraded provider status message = %q, want fallback and legacy wording", degraded.Message)
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

func requireOpenSearchErrorRedacted(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("error = nil, want redacted provider error")
	}
	for _, forbidden := range []string{
		"correct horse battery staple",
		"raw provider body",
		"internal endpoint details",
		"internal cluster",
		"secret-cluster",
		"AKIAIOSFODNN7EXAMPLE",
	} {
		if strings.Contains(err.Error(), forbidden) {
			t.Fatalf("OpenSearch error leaked %q: %q", forbidden, err.Error())
		}
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

func openSearchHitsResponse(t *testing.T, ids []string, total int) string {
	t.Helper()

	hits := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		hits = append(hits, map[string]any{
			"_id":  id,
			"sort": []any{id},
		})
	}
	raw, err := json.Marshal(map[string]any{
		"hits": map[string]any{
			"total": map[string]any{
				"value":    total,
				"relation": "gte",
			},
			"hits": hits,
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal(OpenSearch hits response) error = %v", err)
	}
	return string(raw)
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

func openSearchMappingResponse(t *testing.T, mapping map[string]any) string {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		openSearchChefIndex: map[string]any{
			"mappings": mapping,
		},
	})
	if err != nil {
		t.Fatalf("json.Marshal(mapping response) error = %v", err)
	}
	return string(raw)
}

func openSearchLegacyChefMapping() map[string]any {
	return map[string]any{
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

func requireOpenSearchChefMappingDescriptor(t *testing.T, mapping map[string]any) {
	t.Helper()

	if mapping["dynamic"] != true {
		t.Fatalf("mapping dynamic = %v, want true", mapping["dynamic"])
	}
	meta, ok := mapping["_meta"].(map[string]any)
	if !ok {
		t.Fatalf("mapping _meta = %T(%v), want object", mapping["_meta"], mapping["_meta"])
	}
	if got := openSearchMappingVersionValue(meta[openSearchMappingMetaKey]); got != openSearchMappingVersion {
		t.Fatalf("mapping version = %v, want %d", meta[openSearchMappingMetaKey], openSearchMappingVersion)
	}
	properties := mapping["properties"].(map[string]any)
	if properties["document_id"].(map[string]any)["type"] != "keyword" {
		t.Fatalf("document_id mapping = %v, want keyword", properties["document_id"])
	}
	if properties[openSearchCompatTermsField].(map[string]any)["type"] != "keyword" {
		t.Fatalf("compat terms mapping = %v, want keyword", properties[openSearchCompatTermsField])
	}
}

func requireOpenSearchCapabilities(t *testing.T, got, want OpenSearchCapabilities) {
	t.Helper()

	if got != want {
		t.Fatalf("capabilities = %+v, want %+v", got, want)
	}
}
