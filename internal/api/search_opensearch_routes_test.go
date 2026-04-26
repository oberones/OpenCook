package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestActivePostgresOpenSearchRoutesHydratePersistedSearchResults(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	seedActivePostgresOpenSearchState(t, persisted)

	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "client", "*:*"): {
			"ponyville/client/ponyville-validator",
		},
		apiOpenSearchQueryKey("ponyville", "environment", "name:_default OR name:production"): {
			"ponyville/environment/_default",
			"ponyville/environment/production",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:twi*"): {
			"ponyville/node/twilight",
			"ponyville/node/stale",
		},
		apiOpenSearchQueryKey("ponyville", "node", "rainbow"): {
			"ponyville/node/rainbow",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:*"): {
			"ponyville/node/rainbow",
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "role", "name:* AND NOT name:db"): {
			"ponyville/role/web",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `path:foo\/*`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `badge:primary\[blue\]`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", "ssh_public_key:*"): {
			"ponyville/ponies/alice",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, nil)

	assertActiveOpenSearchIndexURLs(t, restarted.router, "/search", map[string]string{
		"client":      "/search/client",
		"environment": "/search/environment",
		"node":        "/search/node",
		"role":        "/search/role",
		"ponies":      "/search/ponies",
	})
	assertActiveOpenSearchIndexURLs(t, restarted.router, "/organizations/ponyville/search", map[string]string{
		"client":      "/organizations/ponyville/search/client",
		"environment": "/organizations/ponyville/search/environment",
		"node":        "/organizations/ponyville/search/node",
		"role":        "/organizations/ponyville/search/role",
		"ponies":      "/organizations/ponyville/search/ponies",
	})

	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/client", "*:*"), "/search/client", []string{"ponyville-validator"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/environment", "name:_default OR name:production"), "/organizations/ponyville/search/environment", []string{"_default", "production"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "name:twi*"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/node", "rainbow"), "/organizations/ponyville/search/node", []string{"rainbow"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "name:* AND NOT name:db"), "/search/role", []string{"web"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/ponies", `path:foo\/*`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/ponies", `badge:primary\[blue\]`), "/organizations/ponyville/search/ponies", []string{"data_bag_item_ponies_alice"})

	pagedRec := performActiveOpenSearchRequest(t, restarted.router, http.MethodGet, searchPath("/search/node", "name:*")+"&rows=1&start=1", "/search/node", nil)
	pagedPayload := decodeSearchPayload(t, pagedRec)
	if pagedPayload["total"] != float64(2) {
		t.Fatalf("paged total = %v, want 2", pagedPayload["total"])
	}
	pagedRows := pagedPayload["rows"].([]any)
	if len(pagedRows) != 1 || pagedRows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("paged rows = %v, want second sorted row twilight", pagedRows)
	}

	partialBody := []byte(`{"private_key":["ssh","private_key"],"public_key":["ssh","public_key"]}`)
	partialRec := performActiveOpenSearchRequest(t, restarted.router, http.MethodPost, searchPath("/organizations/ponyville/search/ponies", "ssh_public_key:*"), "/organizations/ponyville/search/ponies", partialBody)
	partialPayload := decodeSearchPayload(t, partialRec)
	partialRows := partialPayload["rows"].([]any)
	if len(partialRows) != 1 {
		t.Fatalf("partial rows len = %d, want 1 (%v)", len(partialRows), partialRows)
	}
	partialRow := partialRows[0].(map[string]any)
	if partialRow["url"] != "/organizations/ponyville/data/ponies/alice" {
		t.Fatalf("partial row url = %v, want /organizations/ponyville/data/ponies/alice", partialRow["url"])
	}
	partialData := partialRow["data"].(map[string]any)
	if partialData["private_key"] != "---RSA Private Key--- Alice" || partialData["public_key"] != "---RSA Public Key--- Alice" {
		t.Fatalf("partial row data = %v, want hydrated data bag secrets", partialData)
	}
}

func TestActivePostgresOpenSearchFiltersDeniedIDsAfterHydration(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	seedActivePostgresOpenSearchState(t, persisted)

	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "node", "name:*"): {
			"ponyville/node/rainbow",
			"ponyville/node/twilight",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:rainbow": {},
			},
		}
	})

	rec := performActiveOpenSearchRequest(t, restarted.router, http.MethodGet, searchPath("/search/node", "name:*"), "/search/node", nil)
	payload := decodeSearchPayload(t, rec)
	if payload["total"] != float64(1) {
		t.Fatalf("filtered total = %v, want 1", payload["total"])
	}
	rows := payload["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("filtered rows = %v, want only twilight", rows)
	}
}

func TestActivePostgresOpenSearchMutationEventsDriveSearchResults(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)

	fixture.createOrganizationWithValidator("ponyville")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:ponyville-validator"), "/search/client", []string{"ponyville-validator"})

	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/clients", mustMarshalDataBagJSON(t, map[string]any{
		"name":       "twilight",
		"public_key": publicKeyPEM,
	}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:twilight"), "/search/client", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/clients/twilight", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:twilight"), "/search/client", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/environments", []byte(`{"name":"qa","json_class":"Chef::Environment","chef_type":"environment","description":"old-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:old-env-term"), "/search/environment", []string{"qa"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/environments/qa", []byte(`{"name":"qa","json_class":"Chef::Environment","chef_type":"environment","description":"new-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:old-env-term"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:new-env-term"), "/search/environment", []string{"qa"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/environments/qa", []byte(`{"name":"staging","json_class":"Chef::Environment","chef_type":"environment","description":"renamed-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "name:qa"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "name:staging"), "/search/environment", []string{"staging"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/environments/staging", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:renamed-env-term"), "/search/environment", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "twilight", "normal": map[string]any{"team": "weather"}}), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "wrong-name", "normal": map[string]any{"team": "bad"}}), http.StatusBadRequest)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:bad"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{})
	transport.ForceDocument("ponyville/node/twilight", map[string]any{
		"organization": "ponyville",
		"index":        "node",
		"name":         []any{"twilight"},
		"team":         []any{"weather"},
	})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/roles", []byte(`{"name":"web","description":"old-role-term","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{},"run_list":["base"],"env_run_lists":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:old-role-term"), "/search/role", []string{"web"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/roles/web", []byte(`{"name":"web","description":"new-role-term"}`), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:old-role-term"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:new-role-term"), "/search/role", []string{"web"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/roles/web", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:new-role-term"), "/search/role", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{"id": "alice", "color": "blue"}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:blue"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/data/ponies/alice", mustMarshalDataBagJSON(t, map[string]any{"id": "alice", "color": "green"}), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:blue"), "/search/ponies", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/data/ponies/alice", mustMarshalDataBagJSON(t, map[string]any{"id": "wrong", "color": "red"}), http.StatusBadRequest)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:red"), "/search/ponies", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/data/ponies/alice", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{})
}

func TestActivePostgresOpenSearchUnavailableRoutesUseStableErrors(t *testing.T) {
	fixture := newActivePostgresBootstrapFixtureWithSearch(t, pgtest.NewState(pgtest.Seed{}), func(state *bootstrap.Service) search.Index {
		return search.NewOpenSearchIndex(state, nil, "http://opensearch.example")
	})
	fixture.createOrganizationWithValidator("ponyville")

	listReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/search", nil)
	listRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable /search status = %d, want %d, body = %s", listRec.Code, http.StatusServiceUnavailable, listRec.Body.String())
	}
	assertAPIError(t, listRec, "search_unavailable")
	if strings.Contains(listRec.Body.String(), "opensearch.example") {
		t.Fatalf("unavailable /search leaked provider details: %s", listRec.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/_status", nil)
	statusRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("/_status status = %d, want %d, body = %s", statusRec.Code, http.StatusOK, statusRec.Body.String())
	}
	var statusPayload map[string]any
	if err := json.Unmarshal(statusRec.Body.Bytes(), &statusPayload); err != nil {
		t.Fatalf("json.Unmarshal(/_status) error = %v", err)
	}
	openSearchStatus := statusPayload["dependencies"].(map[string]any)["opensearch"].(map[string]any)
	if openSearchStatus["backend"] != "opensearch" || openSearchStatus["configured"] != true || openSearchStatus["message"] != "OpenSearch is configured but unavailable" {
		t.Fatalf("opensearch status = %v, want configured unavailable wording", openSearchStatus)
	}
}

func TestActivePostgresOpenSearchQueryUnavailableUsesStableError(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, unavailableAPIOpenSearchTransport{}, nil)
	fixture.createOrganizationWithValidator("ponyville")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)

	req := httptest.NewRequest(http.MethodGet, searchPath("/search/node", "team:friendship"), nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable search query status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	assertAPIError(t, rec, "search_unavailable")
	for _, leaked := range []string{"opensearch.example", "provider body", "internal cluster"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("unavailable search query leaked %q in body: %s", leaked, rec.Body.String())
		}
	}
}

// TestActivePostgresOpenSearchEncryptedDataBagQueryUnavailableUsesStableError
// keeps Chef-facing provider-unavailable behavior redacted for encrypted-looking
// data bag search routes as well as built-in indexes.
func TestActivePostgresOpenSearchEncryptedDataBagQueryUnavailableUsesStableError(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, unavailableAPIOpenSearchTransport{}, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	bagPath := "/organizations/ponyville/data/" + bagName
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)

	rawPath := searchPath("/search/"+bagName, "password_encrypted_data:"+encryptedDataBagFixtureField(t, "password", "encrypted_data"))
	req := httptest.NewRequest(http.MethodGet, rawPath, nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/search/"+bagName, nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("encrypted unavailable search status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	assertAPIError(t, rec, "search_unavailable")
	for _, leaked := range []string{"opensearch.example", "provider body", "internal cluster", "correct horse battery staple"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("encrypted unavailable search leaked %q in body: %s", leaked, rec.Body.String())
		}
	}
}

func TestActivePostgresOpenSearchIndexingFailuresDoNotRollbackObjectPersistence(t *testing.T) {
	indexer := &failingAPIDocumentIndexer{}
	fixture := newActivePostgresBootstrapFixtureWithSearchIndexerAndAuthorizer(t, pgtest.NewState(pgtest.Seed{}), nil, indexer, nil)
	fixture.createOrganizationWithValidator("ponyville")
	if indexer.upserts == 0 {
		t.Fatal("organization creation did not attempt a validator-client index upsert")
	}

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)
	if indexer.upserts < 2 {
		t.Fatalf("node creation upserts = %d, want at least validator plus node attempts", indexer.upserts)
	}

	getReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/nodes/twilight", nil)
	getRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("node persisted after failed index upsert status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	before := indexer.snapshot()
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "wrong-name", "normal": map[string]any{"team": "bad"}}), http.StatusBadRequest)
	indexer.requireSnapshot(t, before)

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK)
	if indexer.deletes == 0 {
		t.Fatal("node delete did not attempt an index delete")
	}

	missingReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/nodes/twilight", nil)
	missingRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("node deleted despite failed index delete status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

// TestActivePostgresOpenSearchEncryptedDataBagInvalidWritesDoNotMutateDocuments
// proves invalid encrypted-looking item writes do not emit stale OpenSearch
// document changes when provider-backed indexing is active.
func TestActivePostgresOpenSearchEncryptedDataBagInvalidWritesDoNotMutateDocuments(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/organizations/ponyville/data/" + bagName
	itemPath := bagPath + "/" + itemID
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:"+itemID), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})

	beforeInvalidWrites := transport.SnapshotDocuments()
	mismatchedUpdate := encryptedEnvelopeVariantPayload(t, itemID, func(envelope map[string]any) {
		envelope["encrypted_data"] = "opensearch-should-not-see-this"
	})
	mismatchedUpdate["id"] = "wrong-item"
	mismatchedUpdate["environment"] = "blocked"
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, mismatchedUpdate), http.StatusBadRequest)

	missingID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	delete(missingID, "id")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, missingID), http.StatusBadRequest)

	transport.RequireDocuments(t, beforeInvalidWrites)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:"+itemID), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "environment:blocked"), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:wrong-item"), "/search/"+bagName, []string{})
}

// TestActivePostgresOpenSearchEncryptedDataBagSearchAndPartialSearch pins the
// active PostgreSQL plus OpenSearch-backed path to the same opaque encrypted
// data bag search contract as the in-memory search index.
func TestActivePostgresOpenSearchEncryptedDataBagSearchAndPartialSearch(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/organizations/ponyville/data/" + bagName
	passwordCiphertext := encryptedDataBagFixtureField(t, "password", "encrypted_data")
	passwordIV := encryptedDataBagFixtureField(t, "password", "iv")
	apiAuthTag := encryptedDataBagFixtureField(t, "api_key", "auth_tag")

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)

	fullRec := performActiveOpenSearchRequest(t, fixture.router, http.MethodGet, searchPath("/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, nil)
	assertEncryptedDataBagSearchFullRow(t, decodeSearchPayload(t, fullRec), bagName, itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, fullRec)

	partialRec := performActiveOpenSearchRequest(t, fixture.router, http.MethodPost, searchPath("/organizations/ponyville/search/"+bagName, "environment:production AND api_key_auth_tag:"+apiAuthTag), "/organizations/ponyville/search/"+bagName, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, partialRec), "/organizations/ponyville/data/"+bagName+"/"+itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, partialRec)

	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "password_iv:"+passwordIV), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "environment:production"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "raw_data_password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "correct-horse-battery-staple"), "/search/"+bagName, []string{})

	denied := newActivePostgresOpenSearchFixture(t, fixture.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return &denySearchDocumentAfterIndexGateAuthorizer{
			base:   authz.NewACLAuthorizer(state),
			target: "data_bag:" + bagName,
		}
	})
	assertActiveOpenSearchFullRows(t, denied.router, searchPath("/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{})
}

func newActivePostgresOpenSearchFixture(t *testing.T, pgState *pgtest.State, transport search.OpenSearchTransport, authorizerFactory func(*bootstrap.Service) authz.Authorizer) *activePostgresBootstrapFixture {
	t.Helper()

	return newActivePostgresBootstrapFixtureWithSearchAndAuthorizer(t, pgState, func(state *bootstrap.Service) search.Index {
		client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
		if err != nil {
			t.Fatalf("NewOpenSearchClient() error = %v", err)
		}
		return search.NewOpenSearchIndex(state, client, "http://opensearch.example")
	}, authorizerFactory)
}

func newActivePostgresOpenSearchIndexingFixture(t *testing.T, pgState *pgtest.State, client *search.OpenSearchClient, authorizerFactory func(*bootstrap.Service) authz.Authorizer) *activePostgresBootstrapFixture {
	t.Helper()

	return newActivePostgresBootstrapFixtureWithSearchIndexerAndAuthorizer(t, pgState, func(state *bootstrap.Service) search.Index {
		return search.NewOpenSearchIndex(state, client, "http://opensearch.example")
	}, client, authorizerFactory)
}

func seedActivePostgresOpenSearchState(t *testing.T, fixture *activePostgresBootstrapFixture) {
	t.Helper()

	fixture.createOrganizationWithValidator("ponyville")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/environments", []byte(`{"name":"production","json_class":"Chef::Environment","chef_type":"environment","description":"prod-search-target","cookbook_versions":{},"default_attributes":{"region":"equus"},"override_attributes":{}}`), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{
		"path": "foo/bar",
	}, map[string]any{
		"team": "friendship",
	}, []string{"web"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalSearchNodePayload(t, "rainbow", map[string]any{}, map[string]any{
		"team": "weather",
	}, []string{"base"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/roles", []byte(`{"name":"web","description":"role-search-target","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{},"run_list":["base"],"env_run_lists":{}}`), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":    "alice",
		"path":  "foo/bar",
		"badge": "primary[blue]",
		"ssh": map[string]any{
			"public_key":  "---RSA Public Key--- Alice",
			"private_key": "---RSA Private Key--- Alice",
		},
	}), http.StatusCreated)
}

func sendActivePostgresPivotalJSON(t *testing.T, router http.Handler, method, path string, body []byte, want int) {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", method, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, want, rec.Body.String())
	}
}

func assertActiveOpenSearchIndexURLs(t *testing.T, router http.Handler, path string, want map[string]string) {
	t.Helper()

	rec := performActiveOpenSearchRequest(t, router, http.MethodGet, path, path, nil)
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v, body = %s", path, err, rec.Body.String())
	}
	if len(payload) != len(want) {
		t.Fatalf("%s index count = %d, want %d (%v)", path, len(payload), len(want), payload)
	}
	for key, expectedURL := range want {
		if payload[key] != expectedURL {
			t.Fatalf("%s index %q = %q, want %q", path, key, payload[key], expectedURL)
		}
	}
}

func assertActiveOpenSearchFullRows(t *testing.T, router http.Handler, rawPath, signPath string, wantNames []string) {
	t.Helper()

	rec := performActiveOpenSearchRequest(t, router, http.MethodGet, rawPath, signPath, nil)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(len(wantNames)) || len(rows) != len(wantNames) {
		t.Fatalf("%s payload = %v, want %d rows", rawPath, payload, len(wantNames))
	}
	for i, want := range wantNames {
		name := rows[i].(map[string]any)["name"]
		if name != want {
			t.Fatalf("%s row[%d] name = %v, want %q", rawPath, i, name, want)
		}
	}
}

func performActiveOpenSearchRequest(t *testing.T, router http.Handler, method, rawPath, signPath string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, rawPath, reader)
	applySignedHeaders(t, req, "pivotal", "", method, signPath, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec
}

func searchPath(path, query string) string {
	return path + "?q=" + url.QueryEscape(query)
}

func apiOpenSearchQueryKey(org, index, query string) string {
	return org + "\x00" + index + "\x00" + query
}

type apiOpenSearchSearchTransport struct {
	t   *testing.T
	ids map[string][]string
}

func newAPIOpenSearchSearchTransport(t *testing.T, ids map[string][]string) *apiOpenSearchSearchTransport {
	t.Helper()

	return &apiOpenSearchSearchTransport{
		t:   t,
		ids: ids,
	}
}

func (t *apiOpenSearchSearchTransport) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.URL.Path != "/chef/_search" {
		t.t.Fatalf("OpenSearch request = %s %s, want POST /chef/_search", req.Method, req.URL.Path)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(OpenSearch request) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.t.Fatalf("json.Unmarshal(OpenSearch request) error = %v, body = %s", err, string(body))
	}

	org, index, query := apiOpenSearchSearchIdentity(t.t, payload)
	key := apiOpenSearchQueryKey(org, index, query)
	ids, ok := t.ids[key]
	if !ok {
		t.t.Fatalf("no fake OpenSearch IDs for org=%q index=%q query=%q body=%s", org, index, query, string(body))
	}

	hits := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		hits = append(hits, map[string]string{"_id": id})
	}
	raw, err := json.Marshal(map[string]any{
		"hits": map[string]any{
			"hits": hits,
		},
	})
	if err != nil {
		t.t.Fatalf("json.Marshal(OpenSearch response) error = %v", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Request:    req,
	}, nil
}

func apiOpenSearchSearchIdentity(t *testing.T, payload map[string]any) (string, string, string) {
	t.Helper()

	boolQuery := payload["query"].(map[string]any)["bool"].(map[string]any)
	org := apiOpenSearchCompatFilterValue(t, boolQuery["filter"], "__org")
	index := apiOpenSearchCompatFilterValue(t, boolQuery["filter"], "__index")
	query := apiOpenSearchCompatQueryString(t, boolQuery["must"])
	return org, index, query
}

func apiOpenSearchCompatFilterValue(t *testing.T, raw any, field string) string {
	t.Helper()

	for _, filter := range raw.([]any) {
		term := filter.(map[string]any)["term"].(map[string]any)
		token, ok := term["compat_terms"].(string)
		if !ok {
			continue
		}
		if value, ok := strings.CutPrefix(token, field+"="); ok {
			return value
		}
	}
	t.Fatalf("OpenSearch filter missing %q: %v", field, raw)
	return ""
}

func apiOpenSearchCompatQueryString(t *testing.T, raw any) string {
	t.Helper()

	clause, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("OpenSearch query clause = %T %v, want object", raw, raw)
	}
	if _, ok := clause["match_all"].(map[string]any); ok {
		return "*:*"
	}
	if _, ok := clause["match_none"].(map[string]any); ok {
		return ""
	}
	if term, ok := clause["term"].(map[string]any); ok {
		return apiOpenSearchCompatTokenQuery(t, term["compat_terms"].(string), false)
	}
	if prefix, ok := clause["prefix"].(map[string]any); ok {
		return apiOpenSearchCompatTokenQuery(t, prefix["compat_terms"].(string), true)
	}

	boolQuery, ok := clause["bool"].(map[string]any)
	if !ok {
		t.Fatalf("OpenSearch query clause = %v, want bool/term/prefix/match", clause)
	}
	if should, ok := boolQuery["should"].([]any); ok {
		parts := make([]string, 0, len(should))
		for _, item := range should {
			parts = append(parts, apiOpenSearchCompatQueryString(t, item))
		}
		return strings.Join(parts, " OR ")
	}

	parts := make([]string, 0)
	if must, ok := boolQuery["must"].([]any); ok {
		for _, item := range must {
			part := apiOpenSearchCompatQueryString(t, item)
			if part != "*:*" && part != "" {
				parts = append(parts, part)
			}
		}
	}
	if mustNot, ok := boolQuery["must_not"].([]any); ok {
		for _, item := range mustNot {
			part := apiOpenSearchCompatQueryString(t, item)
			if part != "" {
				parts = append(parts, "NOT "+part)
			}
		}
	}
	return strings.Join(parts, " AND ")
}

func apiOpenSearchCompatTokenQuery(t *testing.T, token string, wildcard bool) string {
	t.Helper()

	field, value, ok := strings.Cut(token, "=")
	if !ok {
		t.Fatalf("OpenSearch compat token = %q, want field=value", token)
	}
	value = apiOpenSearchEscapeQueryValue(value)
	if wildcard {
		value += "*"
	}
	if field == "__any" {
		return value
	}
	return field + ":" + value
}

func apiOpenSearchEscapeQueryValue(value string) string {
	replacer := strings.NewReplacer(`:`, `\:`, `[`, `\[`, `]`, `\]`, `@`, `\@`, `/`, `\/`)
	return replacer.Replace(value)
}

type unavailableAPIOpenSearchTransport struct{}

func (unavailableAPIOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("raw provider body from internal cluster opensearch.example")),
		Request:    req,
	}, nil
}

type failingAPIDocumentIndexer struct {
	upserts int
	deletes int
}

type failingAPIDocumentIndexerSnapshot struct {
	upserts int
	deletes int
}

func (i *failingAPIDocumentIndexer) UpsertDocuments(context.Context, []search.Document) error {
	i.upserts++
	return search.ErrUnavailable
}

func (i *failingAPIDocumentIndexer) DeleteDocuments(context.Context, []search.DocumentRef) error {
	i.deletes++
	return search.ErrUnavailable
}

func (i *failingAPIDocumentIndexer) snapshot() failingAPIDocumentIndexerSnapshot {
	return failingAPIDocumentIndexerSnapshot{
		upserts: i.upserts,
		deletes: i.deletes,
	}
}

func (i *failingAPIDocumentIndexer) requireSnapshot(t *testing.T, want failingAPIDocumentIndexerSnapshot) {
	t.Helper()

	if got := i.snapshot(); got != want {
		t.Fatalf("indexer snapshot = %+v, want %+v", got, want)
	}
}

type statefulAPIOpenSearchTransport struct {
	t    *testing.T
	mu   sync.Mutex
	docs map[string]map[string]any
}

func newStatefulAPIOpenSearchTransport(t *testing.T) *statefulAPIOpenSearchTransport {
	t.Helper()

	return &statefulAPIOpenSearchTransport{
		t:    t,
		docs: make(map[string]map[string]any),
	}
}

// SnapshotDocuments returns a deep copy of fake OpenSearch documents so tests
// can verify failed Chef-facing mutations did not emit provider-side changes.
func (tr *statefulAPIOpenSearchTransport) SnapshotDocuments() map[string]map[string]any {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	out := make(map[string]map[string]any, len(tr.docs))
	for id, doc := range tr.docs {
		out[id] = cloneOpenSearchSource(doc)
	}
	return out
}

// RequireDocuments compares the fake OpenSearch document set against a prior
// snapshot and fails with the complete diff context when unexpected drift occurs.
func (tr *statefulAPIOpenSearchTransport) RequireDocuments(t *testing.T, want map[string]map[string]any) {
	t.Helper()

	got := tr.SnapshotDocuments()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OpenSearch documents = %#v, want %#v", got, want)
	}
}

func (t *statefulAPIOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/_bulk":
		return t.handleBulk(req)
	case req.Method == http.MethodPut && req.URL.Path == "/chef/_mapping":
		return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"acknowledged": true}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_refresh":
		return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"refreshed": true}), nil
	case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.EscapedPath(), "/chef/_doc/"):
		return t.handleDelete(req)
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_search":
		return t.handleSearch(req)
	default:
		t.t.Fatalf("OpenSearch request = %s %s, want bulk/refresh/delete/search", req.Method, req.URL.String())
		return nil, nil
	}
}

func (t *statefulAPIOpenSearchTransport) ForceDocument(id string, source map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.docs[id] = cloneOpenSearchSource(source)
}

func (t *statefulAPIOpenSearchTransport) handleBulk(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(bulk request) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines)%2 != 0 {
		t.t.Fatalf("bulk request has odd line count %d: %s", len(lines), string(body))
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < len(lines); i += 2 {
		action := map[string]any{}
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk action) error = %v, line = %s", err, lines[i])
		}
		indexAction := action["index"].(map[string]any)
		id := indexAction["_id"].(string)
		source := map[string]any{}
		if err := json.Unmarshal([]byte(lines[i+1]), &source); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk source) error = %v, line = %s", err, lines[i+1])
		}
		t.docs[id] = source
	}
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"errors": false}), nil
}

func (t *statefulAPIOpenSearchTransport) handleDelete(req *http.Request) (*http.Response, error) {
	escaped := strings.TrimPrefix(req.URL.EscapedPath(), "/chef/_doc/")
	id, err := url.PathUnescape(escaped)
	if err != nil {
		t.t.Fatalf("PathUnescape(%q) error = %v", escaped, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.docs, id)
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"result": "deleted"}), nil
}

func (t *statefulAPIOpenSearchTransport) handleSearch(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(search request) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.t.Fatalf("json.Unmarshal(search request) error = %v, body = %s", err, string(body))
	}
	org, index, query := apiOpenSearchSearchIdentity(t.t, payload)
	plan := search.CompileQuery(query)

	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0)
	for id, source := range t.docs {
		if openSearchSourceString(source, "organization") != org || openSearchSourceString(source, "index") != index {
			continue
		}
		if !plan.MatchesFields(openSearchSourceFields(source)) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	hits := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		hits = append(hits, map[string]string{"_id": id})
	}
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{
		"hits": map[string]any{"hits": hits},
	}), nil
}

func apiOpenSearchJSONResponse(req *http.Request, status int, payload any) *http.Response {
	raw, err := json.Marshal(payload)
	if err != nil {
		raw = []byte(`{"errors":true}`)
	}
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Request:    req,
	}
}

func openSearchSourceString(source map[string]any, key string) string {
	value, _ := source[key].(string)
	return value
}

func openSearchSourceFields(source map[string]any) map[string][]string {
	fields := make(map[string][]string, len(source))
	for key, raw := range source {
		switch value := raw.(type) {
		case []any:
			for _, item := range value {
				fields[key] = append(fields[key], fmt.Sprint(item))
			}
		case []string:
			fields[key] = append(fields[key], value...)
		case string:
			fields[key] = append(fields[key], value)
		case bool, float64:
			fields[key] = append(fields[key], fmt.Sprint(value))
		}
	}
	return fields
}

func cloneOpenSearchSource(source map[string]any) map[string]any {
	raw, err := json.Marshal(source)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}
