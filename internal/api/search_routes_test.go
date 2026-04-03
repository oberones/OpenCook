package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

func TestSearchIndexesEndpointListsImplementedIndexes(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/search", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/search", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("search index status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search indexes) error = %v", err)
	}

	expected := map[string]string{
		"client":      "/search/client",
		"environment": "/search/environment",
		"node":        "/search/node",
		"role":        "/search/role",
	}
	if len(payload) != len(expected) {
		t.Fatalf("search indexes len = %d, want %d (%v)", len(payload), len(expected), payload)
	}
	for key, want := range expected {
		if payload[key] != want {
			t.Fatalf("search index %q = %q, want %q", key, payload[key], want)
		}
	}
}

func TestSearchIndexesEndpointIncludesDataBagIndexes(t *testing.T) {
	router := newTestRouter(t)

	createReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create data bag status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	req := httptest.NewRequest(http.MethodGet, "/search", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/search", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("search index status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search indexes with data bag) error = %v", err)
	}
	if payload["ponies"] != "/search/ponies" {
		t.Fatalf("search index %q = %q, want %q", "ponies", payload["ponies"], "/search/ponies")
	}
}

func TestSearchIndexesEndpointReturnsNotFoundForUnknownOrganization(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/organizations/missing/search", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/missing/search", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	req.SetPathValue("org", "missing")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("search indexes missing org status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search indexes missing org) error = %v", err)
	}
	if payload["message"] != "organization not found" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization not found")
	}
}

func TestSearchNodeEndpointSupportsFullPartialAndPagination(t *testing.T) {
	router := newTestRouter(t)

	twilightBody := mustMarshalSearchNodePayload(t, "twilight", map[string]any{
		"top": map[string]any{
			"mid": map[string]any{
				"bottom": "found_it_default",
			},
		},
		"is": map[string]any{
			"default": true,
		},
	}, map[string]any{
		"top": map[string]any{
			"mid": map[string]any{
				"bottom": "found_it_normal",
			},
		},
		"is": map[string]any{
			"normal": true,
		},
	}, []string{"web"})
	createTwilightReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(twilightBody))
	applySignedHeaders(t, createTwilightReq, "silent-bob", "", http.MethodPost, "/nodes", twilightBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createTwilightRec := httptest.NewRecorder()
	router.ServeHTTP(createTwilightRec, createTwilightReq)
	if createTwilightRec.Code != http.StatusCreated {
		t.Fatalf("create twilight status = %d, want %d, body = %s", createTwilightRec.Code, http.StatusCreated, createTwilightRec.Body.String())
	}

	rainbowBody := mustMarshalSearchNodePayload(t, "rainbow", map[string]any{}, map[string]any{}, []string{"base"})
	createRainbowReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(rainbowBody))
	applySignedHeaders(t, createRainbowReq, "silent-bob", "", http.MethodPost, "/nodes", rainbowBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRainbowRec := httptest.NewRecorder()
	router.ServeHTTP(createRainbowRec, createRainbowReq)
	if createRainbowRec.Code != http.StatusCreated {
		t.Fatalf("create rainbow status = %d, want %d, body = %s", createRainbowRec.Code, http.StatusCreated, createRainbowRec.Body.String())
	}

	searchReq := httptest.NewRequest(http.MethodGet, "/search/node?q=name:twi*", nil)
	applySignedHeaders(t, searchReq, "silent-bob", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	searchRec := httptest.NewRecorder()
	router.ServeHTTP(searchRec, searchReq)

	if searchRec.Code != http.StatusOK {
		t.Fatalf("search node status = %d, want %d, body = %s", searchRec.Code, http.StatusOK, searchRec.Body.String())
	}

	var searchPayload map[string]any
	if err := json.Unmarshal(searchRec.Body.Bytes(), &searchPayload); err != nil {
		t.Fatalf("json.Unmarshal(search node) error = %v", err)
	}
	rows, ok := searchPayload["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("search rows = %T %v, want one result", searchPayload["rows"], searchPayload["rows"])
	}
	row, ok := rows[0].(map[string]any)
	if !ok {
		t.Fatalf("search row = %T, want map[string]any", rows[0])
	}
	if row["name"] != "twilight" {
		t.Fatalf("search row name = %v, want %q", row["name"], "twilight")
	}
	runList := stringSliceFromAny(t, row["run_list"])
	if len(runList) != 1 || runList[0] != "recipe[web]" {
		t.Fatalf("search run_list = %v, want [recipe[web]]", runList)
	}

	partialBody := []byte(`{"goal":["top","mid","bottom"],"we_found_default":["is","default"],"we_found_normal":["is","normal"]}`)
	partialReq := httptest.NewRequest(http.MethodPost, "/search/node?q=name:twilight", bytes.NewReader(partialBody))
	applySignedHeaders(t, partialReq, "silent-bob", "", http.MethodPost, "/search/node", partialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	partialRec := httptest.NewRecorder()
	router.ServeHTTP(partialRec, partialReq)

	if partialRec.Code != http.StatusOK {
		t.Fatalf("partial search node status = %d, want %d, body = %s", partialRec.Code, http.StatusOK, partialRec.Body.String())
	}

	var partialPayload map[string]any
	if err := json.Unmarshal(partialRec.Body.Bytes(), &partialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial node) error = %v", err)
	}
	partialRows, ok := partialPayload["rows"].([]any)
	if !ok || len(partialRows) != 1 {
		t.Fatalf("partial rows = %T %v, want one result", partialPayload["rows"], partialPayload["rows"])
	}
	partialRow := partialRows[0].(map[string]any)
	if partialRow["url"] != "/nodes/twilight" {
		t.Fatalf("partial row url = %v, want %q", partialRow["url"], "/nodes/twilight")
	}
	data, ok := partialRow["data"].(map[string]any)
	if !ok {
		t.Fatalf("partial row data = %T, want map[string]any", partialRow["data"])
	}
	if data["goal"] != "found_it_normal" {
		t.Fatalf("partial search goal = %v, want %q", data["goal"], "found_it_normal")
	}
	if data["we_found_default"] != true {
		t.Fatalf("partial search we_found_default = %v, want true", data["we_found_default"])
	}
	if data["we_found_normal"] != true {
		t.Fatalf("partial search we_found_normal = %v, want true", data["we_found_normal"])
	}

	pagedReq := httptest.NewRequest(http.MethodGet, "/search/node?q=name:*&rows=1&start=1", nil)
	applySignedHeaders(t, pagedReq, "silent-bob", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	pagedRec := httptest.NewRecorder()
	router.ServeHTTP(pagedRec, pagedReq)

	if pagedRec.Code != http.StatusOK {
		t.Fatalf("paged search node status = %d, want %d, body = %s", pagedRec.Code, http.StatusOK, pagedRec.Body.String())
	}

	var pagedPayload map[string]any
	if err := json.Unmarshal(pagedRec.Body.Bytes(), &pagedPayload); err != nil {
		t.Fatalf("json.Unmarshal(paged node) error = %v", err)
	}
	if pagedPayload["total"] != float64(2) {
		t.Fatalf("paged total = %v, want 2", pagedPayload["total"])
	}
	pagedRows, ok := pagedPayload["rows"].([]any)
	if !ok || len(pagedRows) != 1 {
		t.Fatalf("paged rows = %T %v, want one result", pagedPayload["rows"], pagedPayload["rows"])
	}
}

func TestSearchNodeEndpointSupportsPolicyFieldQueriesWithoutPolicyForeignKeys(t *testing.T) {
	router := newTestRouter(t)

	body := mustMarshalSearchNodePayload(t, "pinkie", map[string]any{}, map[string]any{}, []string{"base"})
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(node payload) error = %v", err)
	}
	payload["policy_name"] = "appserver"
	payload["policy_group"] = "dev"
	body = mustMarshalDataBagJSON(t, payload)

	createReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/nodes", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create policy-aware node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	searchReq := httptest.NewRequest(http.MethodGet, "/search/node?q=policy_name:appserver%20AND%20policy_group:dev", nil)
	applySignedHeaders(t, searchReq, "silent-bob", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	searchRec := httptest.NewRecorder()
	router.ServeHTTP(searchRec, searchReq)
	if searchRec.Code != http.StatusOK {
		t.Fatalf("search node by policy fields status = %d, want %d, body = %s", searchRec.Code, http.StatusOK, searchRec.Body.String())
	}

	var searchPayload map[string]any
	if err := json.Unmarshal(searchRec.Body.Bytes(), &searchPayload); err != nil {
		t.Fatalf("json.Unmarshal(policy search payload) error = %v", err)
	}
	rows := searchPayload["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("rows len = %d, want 1 (%v)", len(rows), rows)
	}
	row := rows[0].(map[string]any)
	if row["policy_name"] != "appserver" || row["policy_group"] != "dev" {
		t.Fatalf("policy fields = %v/%v, want appserver/dev", row["policy_name"], row["policy_group"])
	}

	partialBody := []byte(`{"policy_name":["policy_name"],"policy_group":["policy_group"]}`)
	partialReq := httptest.NewRequest(http.MethodPost, "/search/node?q=name:pinkie", bytes.NewReader(partialBody))
	applySignedHeaders(t, partialReq, "silent-bob", "", http.MethodPost, "/search/node", partialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	partialRec := httptest.NewRecorder()
	router.ServeHTTP(partialRec, partialReq)
	if partialRec.Code != http.StatusOK {
		t.Fatalf("partial node search by policy fields status = %d, want %d, body = %s", partialRec.Code, http.StatusOK, partialRec.Body.String())
	}

	var partialPayload map[string]any
	if err := json.Unmarshal(partialRec.Body.Bytes(), &partialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial policy search payload) error = %v", err)
	}
	partialRows := partialPayload["rows"].([]any)
	if len(partialRows) != 1 {
		t.Fatalf("partial rows len = %d, want 1 (%v)", len(partialRows), partialRows)
	}
	data := partialRows[0].(map[string]any)["data"].(map[string]any)
	if data["policy_name"] != "appserver" || data["policy_group"] != "dev" {
		t.Fatalf("partial policy fields = %v/%v, want appserver/dev", data["policy_name"], data["policy_group"])
	}
}

func TestSearchClientEndpointSupportsFullAndPartialSearch(t *testing.T) {
	router := newTestRouter(t)

	searchReq := httptest.NewRequest(http.MethodGet, "/search/client?q=name:org-validator", nil)
	applySignedHeaders(t, searchReq, "silent-bob", "", http.MethodGet, "/search/client", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	searchRec := httptest.NewRecorder()
	router.ServeHTTP(searchRec, searchReq)

	if searchRec.Code != http.StatusOK {
		t.Fatalf("search client status = %d, want %d, body = %s", searchRec.Code, http.StatusOK, searchRec.Body.String())
	}

	var searchPayload map[string]any
	if err := json.Unmarshal(searchRec.Body.Bytes(), &searchPayload); err != nil {
		t.Fatalf("json.Unmarshal(search client) error = %v", err)
	}
	rows := searchPayload["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("search client rows len = %d, want 1 (%v)", len(rows), rows)
	}
	row := rows[0].(map[string]any)
	if row["name"] != "org-validator" {
		t.Fatalf("client row name = %v, want %q", row["name"], "org-validator")
	}
	if row["clientname"] != "org-validator" {
		t.Fatalf("client row clientname = %v, want %q", row["clientname"], "org-validator")
	}
	if row["json_class"] != "Chef::ApiClient" {
		t.Fatalf("client row json_class = %v, want %q", row["json_class"], "Chef::ApiClient")
	}
	if row["chef_type"] != "client" {
		t.Fatalf("client row chef_type = %v, want %q", row["chef_type"], "client")
	}
	if row["orgname"] != "ponyville" {
		t.Fatalf("client row orgname = %v, want %q", row["orgname"], "ponyville")
	}
	if row["validator"] != false {
		t.Fatalf("client row validator = %v, want false", row["validator"])
	}

	partialBody := []byte(`{"validator":["validator"],"org":["orgname"]}`)
	partialReq := httptest.NewRequest(http.MethodPost, "/search/client?q=name:org-validator", bytes.NewReader(partialBody))
	applySignedHeaders(t, partialReq, "silent-bob", "", http.MethodPost, "/search/client", partialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	partialRec := httptest.NewRecorder()
	router.ServeHTTP(partialRec, partialReq)

	if partialRec.Code != http.StatusOK {
		t.Fatalf("partial search client status = %d, want %d, body = %s", partialRec.Code, http.StatusOK, partialRec.Body.String())
	}

	var partialPayload map[string]any
	if err := json.Unmarshal(partialRec.Body.Bytes(), &partialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial client) error = %v", err)
	}
	partialRows := partialPayload["rows"].([]any)
	if len(partialRows) != 1 {
		t.Fatalf("partial client rows len = %d, want 1 (%v)", len(partialRows), partialRows)
	}
	partialRow := partialRows[0].(map[string]any)
	if partialRow["url"] != "/clients/org-validator" {
		t.Fatalf("partial client url = %v, want %q", partialRow["url"], "/clients/org-validator")
	}
	data := partialRow["data"].(map[string]any)
	if data["validator"] != false {
		t.Fatalf("partial client validator = %v, want false", data["validator"])
	}
	if data["org"] != "ponyville" {
		t.Fatalf("partial client org = %v, want %q", data["org"], "ponyville")
	}
}

func TestSearchQueryEndpointRejectsExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/search/node/extra?q=name:*", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/search/node/extra", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	req.SetPathValue("index", "node")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("search extra segment status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func TestSearchQueryEndpointReturnsNotFoundForUnknownOrganization(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/organizations/missing/search/node?q=name:*", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/missing/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	req.SetPathValue("org", "missing")
	req.SetPathValue("index", "node")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("search query missing org status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search query missing org) error = %v", err)
	}
	if payload["message"] != "organization not found" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization not found")
	}
}

func TestSearchDataBagEndpointSupportsFullAndPartialSearch(t *testing.T) {
	router := newTestRouter(t)

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create data bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}

	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id": "alice",
		"ssh": map[string]any{
			"public_key":  "---RSA Public Key--- Alice",
			"private_key": "---RSA Private Key--- Alice",
		},
	}))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create data bag item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	searchReq := httptest.NewRequest(http.MethodGet, "/search/ponies?q=id:alice", nil)
	applySignedHeaders(t, searchReq, "silent-bob", "", http.MethodGet, "/search/ponies", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	searchReq.SetPathValue("index", "ponies")
	searchRec := httptest.NewRecorder()
	router.ServeHTTP(searchRec, searchReq)

	if searchRec.Code != http.StatusOK {
		t.Fatalf("search data bag status = %d, want %d, body = %s", searchRec.Code, http.StatusOK, searchRec.Body.String())
	}

	var searchPayload map[string]any
	if err := json.Unmarshal(searchRec.Body.Bytes(), &searchPayload); err != nil {
		t.Fatalf("json.Unmarshal(search data bag) error = %v", err)
	}
	rows := searchPayload["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("search data bag rows len = %d, want 1 (%v)", len(rows), rows)
	}
	row := rows[0].(map[string]any)
	if row["name"] != "data_bag_item_ponies_alice" {
		t.Fatalf("search row name = %v, want %q", row["name"], "data_bag_item_ponies_alice")
	}
	if row["data_bag"] != "ponies" {
		t.Fatalf("search row data_bag = %v, want %q", row["data_bag"], "ponies")
	}
	rawData := row["raw_data"].(map[string]any)
	if rawData["id"] != "alice" {
		t.Fatalf("search row raw_data[id] = %v, want %q", rawData["id"], "alice")
	}

	partialBody := []byte(`{"private_key":["ssh","private_key"],"public_key":["ssh","public_key"]}`)
	partialReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/search/ponies?q=ssh_public_key:*", bytes.NewReader(partialBody))
	applySignedHeaders(t, partialReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/search/ponies", partialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	partialReq.SetPathValue("org", "ponyville")
	partialReq.SetPathValue("index", "ponies")
	partialRec := httptest.NewRecorder()
	router.ServeHTTP(partialRec, partialReq)

	if partialRec.Code != http.StatusOK {
		t.Fatalf("partial search data bag status = %d, want %d, body = %s", partialRec.Code, http.StatusOK, partialRec.Body.String())
	}

	var partialPayload map[string]any
	if err := json.Unmarshal(partialRec.Body.Bytes(), &partialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial data bag) error = %v", err)
	}
	partialRows := partialPayload["rows"].([]any)
	if len(partialRows) != 1 {
		t.Fatalf("partial data bag rows len = %d, want 1 (%v)", len(partialRows), partialRows)
	}
	partialRow := partialRows[0].(map[string]any)
	if partialRow["url"] != "/organizations/ponyville/data/ponies/alice" {
		t.Fatalf("partial data bag url = %v, want %q", partialRow["url"], "/organizations/ponyville/data/ponies/alice")
	}
	data := partialRow["data"].(map[string]any)
	if data["private_key"] != "---RSA Private Key--- Alice" {
		t.Fatalf("partial data bag private_key = %v, want expected private key", data["private_key"])
	}
	if data["public_key"] != "---RSA Public Key--- Alice" {
		t.Fatalf("partial data bag public_key = %v, want expected public key", data["public_key"])
	}

	missReq := httptest.NewRequest(http.MethodGet, "/search/ponies?q=raw_data_ssh_public_key:*", nil)
	applySignedHeaders(t, missReq, "silent-bob", "", http.MethodGet, "/search/ponies", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	missReq.SetPathValue("index", "ponies")
	missRec := httptest.NewRecorder()
	router.ServeHTTP(missRec, missReq)

	if missRec.Code != http.StatusOK {
		t.Fatalf("search raw_data-prefixed miss status = %d, want %d, body = %s", missRec.Code, http.StatusOK, missRec.Body.String())
	}

	var missPayload map[string]any
	if err := json.Unmarshal(missRec.Body.Bytes(), &missPayload); err != nil {
		t.Fatalf("json.Unmarshal(raw_data-prefixed miss) error = %v", err)
	}
	if missPayload["total"] != float64(0) {
		t.Fatalf("raw_data-prefixed miss total = %v, want 0", missPayload["total"])
	}
}

func TestSearchDataBagEndpointReturnsChefStyleNotFound(t *testing.T) {
	router := newTestRouter(t)

	req := httptest.NewRequest(http.MethodGet, "/search/no_bag?q=id:*", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/search/no_bag", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	req.SetPathValue("index", "no_bag")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("search missing data bag status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	var payload map[string][]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search missing data bag) error = %v", err)
	}
	if len(payload["error"]) != 1 || payload["error"][0] != "I don't know how to search for no_bag data objects." {
		t.Fatalf("search missing data bag payload = %v, want Chef-style message", payload)
	}
}

func TestSearchDataBagEndpointSupportsSpecialCharacterQueries(t *testing.T) {
	router := newTestRouter(t)

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "x"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create data bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}

	for _, payload := range []map[string]any{
		{"id": "foo", "path": "foo/bar"},
		{"id": "foo-bar"},
	} {
		req := newSignedJSONRequest(t, http.MethodPost, "/data/x", mustMarshalDataBagJSON(t, payload))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create data bag item %v status = %d, want %d, body = %s", payload["id"], rec.Code, http.StatusCreated, rec.Body.String())
		}
	}

	andReq := httptest.NewRequest(http.MethodGet, "/search/x?q=id:foo*%20AND%20NOT%20bar", nil)
	applySignedHeaders(t, andReq, "silent-bob", "", http.MethodGet, "/search/x", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	andReq.SetPathValue("index", "x")
	andRec := httptest.NewRecorder()
	router.ServeHTTP(andRec, andReq)

	if andRec.Code != http.StatusOK {
		t.Fatalf("search and/not status = %d, want %d, body = %s", andRec.Code, http.StatusOK, andRec.Body.String())
	}

	var andPayload map[string]any
	if err := json.Unmarshal(andRec.Body.Bytes(), &andPayload); err != nil {
		t.Fatalf("json.Unmarshal(search and/not) error = %v", err)
	}
	if andPayload["total"] != float64(2) {
		t.Fatalf("and/not total = %v, want 2", andPayload["total"])
	}

	pathReq := httptest.NewRequest(http.MethodGet, `/search/x?q=path:foo\/*`, nil)
	applySignedHeaders(t, pathReq, "silent-bob", "", http.MethodGet, "/search/x", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	pathReq.SetPathValue("index", "x")
	pathRec := httptest.NewRecorder()
	router.ServeHTTP(pathRec, pathReq)

	if pathRec.Code != http.StatusOK {
		t.Fatalf("search escaped slash status = %d, want %d, body = %s", pathRec.Code, http.StatusOK, pathRec.Body.String())
	}

	var pathPayload map[string]any
	if err := json.Unmarshal(pathRec.Body.Bytes(), &pathPayload); err != nil {
		t.Fatalf("json.Unmarshal(search escaped slash) error = %v", err)
	}
	pathRows := pathPayload["rows"].([]any)
	if len(pathRows) != 1 {
		t.Fatalf("escaped slash rows len = %d, want 1 (%v)", len(pathRows), pathRows)
	}
	pathRow := pathRows[0].(map[string]any)
	if pathRow["name"] != "data_bag_item_x_foo" {
		t.Fatalf("escaped slash row name = %v, want %q", pathRow["name"], "data_bag_item_x_foo")
	}
}

func TestSearchRoleAndEnvironmentEndpointsSupportFullAndPartialSearch(t *testing.T) {
	router := newTestRouter(t)

	environmentBody := []byte(`{"name":"production","json_class":"Chef::Environment","chef_type":"environment","description":"env-search-target","cookbook_versions":{},"default_attributes":{"top":{"middle":{"bottom":"found_it"}}},"override_attributes":{}}`)
	environmentReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(environmentBody))
	applySignedHeaders(t, environmentReq, "silent-bob", "", http.MethodPost, "/environments", environmentBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	environmentRec := httptest.NewRecorder()
	router.ServeHTTP(environmentRec, environmentReq)
	if environmentRec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", environmentRec.Code, http.StatusCreated, environmentRec.Body.String())
	}

	roleBody := []byte(`{"name":"web","description":"role-search-target","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{"top":{"mid":{"bottom":"found_it"}}},"run_list":["base"],"env_run_lists":{}}`)
	roleReq := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(roleBody))
	applySignedHeaders(t, roleReq, "silent-bob", "", http.MethodPost, "/roles", roleBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	roleRec := httptest.NewRecorder()
	router.ServeHTTP(roleRec, roleReq)
	if roleRec.Code != http.StatusCreated {
		t.Fatalf("create role status = %d, want %d, body = %s", roleRec.Code, http.StatusCreated, roleRec.Body.String())
	}

	roleSearchReq := httptest.NewRequest(http.MethodGet, "/search/role?q=name:web", nil)
	applySignedHeaders(t, roleSearchReq, "silent-bob", "", http.MethodGet, "/search/role", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	roleSearchRec := httptest.NewRecorder()
	router.ServeHTTP(roleSearchRec, roleSearchReq)
	if roleSearchRec.Code != http.StatusOK {
		t.Fatalf("search role status = %d, want %d, body = %s", roleSearchRec.Code, http.StatusOK, roleSearchRec.Body.String())
	}

	var roleSearchPayload map[string]any
	if err := json.Unmarshal(roleSearchRec.Body.Bytes(), &roleSearchPayload); err != nil {
		t.Fatalf("json.Unmarshal(search role) error = %v", err)
	}
	roleRows := roleSearchPayload["rows"].([]any)
	roleRow := roleRows[0].(map[string]any)
	if roleRow["description"] != "role-search-target" {
		t.Fatalf("role description = %v, want %q", roleRow["description"], "role-search-target")
	}
	roleRunList := stringSliceFromAny(t, roleRow["run_list"])
	if len(roleRunList) != 1 || roleRunList[0] != "recipe[base]" {
		t.Fatalf("role run_list = %v, want [recipe[base]]", roleRunList)
	}

	rolePartialBody := []byte(`{"goal":["override_attributes","top","mid","bottom"]}`)
	rolePartialReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/search/role?q=description:role-search-target", bytes.NewReader(rolePartialBody))
	applySignedHeaders(t, rolePartialReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/search/role", rolePartialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	rolePartialRec := httptest.NewRecorder()
	router.ServeHTTP(rolePartialRec, rolePartialReq)
	if rolePartialRec.Code != http.StatusOK {
		t.Fatalf("partial search role status = %d, want %d, body = %s", rolePartialRec.Code, http.StatusOK, rolePartialRec.Body.String())
	}

	var rolePartialPayload map[string]any
	if err := json.Unmarshal(rolePartialRec.Body.Bytes(), &rolePartialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial role) error = %v", err)
	}
	rolePartialRow := rolePartialPayload["rows"].([]any)[0].(map[string]any)
	if rolePartialRow["url"] != "/organizations/ponyville/roles/web" {
		t.Fatalf("partial role url = %v, want %q", rolePartialRow["url"], "/organizations/ponyville/roles/web")
	}
	rolePartialData := rolePartialRow["data"].(map[string]any)
	if rolePartialData["goal"] != "found_it" {
		t.Fatalf("partial role goal = %v, want %q", rolePartialData["goal"], "found_it")
	}

	environmentPartialBody := []byte(`{"goal":["default_attributes","top","middle","bottom"]}`)
	environmentPartialReq := httptest.NewRequest(http.MethodPost, "/search/environment?q=description:env-search-target", bytes.NewReader(environmentPartialBody))
	applySignedHeaders(t, environmentPartialReq, "silent-bob", "", http.MethodPost, "/search/environment", environmentPartialBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	environmentPartialRec := httptest.NewRecorder()
	router.ServeHTTP(environmentPartialRec, environmentPartialReq)
	if environmentPartialRec.Code != http.StatusOK {
		t.Fatalf("partial search environment status = %d, want %d, body = %s", environmentPartialRec.Code, http.StatusOK, environmentPartialRec.Body.String())
	}

	var environmentPartialPayload map[string]any
	if err := json.Unmarshal(environmentPartialRec.Body.Bytes(), &environmentPartialPayload); err != nil {
		t.Fatalf("json.Unmarshal(partial environment) error = %v", err)
	}
	environmentPartialRow := environmentPartialPayload["rows"].([]any)[0].(map[string]any)
	environmentPartialData := environmentPartialRow["data"].(map[string]any)
	if environmentPartialData["goal"] != "found_it" {
		t.Fatalf("partial environment goal = %v, want %q", environmentPartialData["goal"], "found_it")
	}
}

func TestSearchEndpointFiltersDeniedResults(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"role:secret": {},
			},
		}
	})

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "visible",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []string{},
			"env_run_lists":       map[string]any{},
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "secret",
			"description":         "hidden",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []string{},
			"env_run_lists":       map[string]any{},
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	}); err != nil {
		t.Fatalf("CreateRole(secret) error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/search/role?q=name:*", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/search/role", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("search role status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(filtered search) error = %v", err)
	}
	rows := payload["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("filtered search rows len = %d, want 1 (%v)", len(rows), rows)
	}
	row := rows[0].(map[string]any)
	if row["name"] != "web" {
		t.Fatalf("filtered search row name = %v, want %q", row["name"], "web")
	}
}

type denyingSearchAuthorizer struct {
	base     authz.Authorizer
	denyRead map[string]struct{}
}

func (a denyingSearchAuthorizer) Name() string {
	return "denying-search-test"
}

func (a denyingSearchAuthorizer) Authorize(ctx context.Context, subject authz.Subject, action authz.Action, resource authz.Resource) (authz.Decision, error) {
	if action == authz.ActionRead {
		if _, denied := a.denyRead[resource.Type+":"+resource.Name]; denied {
			return authz.Decision{Allowed: false, Reason: "denied for test"}, nil
		}
	}
	if a.base == nil {
		return authz.Decision{Allowed: true, Reason: "no base authorizer"}, nil
	}
	return a.base.Authorize(ctx, subject, action, resource)
}

func newSearchTestRouterWithAuthorizer(t *testing.T, authorizerFactory func(*bootstrap.Service) authz.Authorizer) (http.Handler, *bootstrap.Service) {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
	store := authn.NewMemoryKeyStore()
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	})
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "pivotal",
		},
		PublicKey: &privateKey.PublicKey,
	})

	state := bootstrap.NewService(store, bootstrap.Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "silent-bob"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
	}
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(pivotal) error = %v", err)
	}
	if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	var authorizer authz.Authorizer = authz.NewACLAuthorizer(state)
	if authorizerFactory != nil {
		authorizer = authorizerFactory(state)
	}

	skew := 15 * time.Minute
	router := NewRouter(Dependencies{
		Logger: log.New(ioDiscard{}, "", 0),
		Config: config.Config{
			ServiceName:      "opencook",
			Environment:      "test",
			AuthSkew:         skew,
			MaxAuthBodyBytes: config.DefaultMaxAuthBodyBytes,
		},
		Version: version.Current(),
		Compat:  compat.NewDefaultRegistry(),
		Authn: authn.NewChefVerifier(store, authn.Options{
			AllowedClockSkew: &skew,
			Now: func() time.Time {
				return mustParseTime(t, "2026-04-02T15:04:35Z")
			},
		}),
		Authz:     authorizer,
		Bootstrap: state,
		Blob:      blob.NewNoopStore(""),
		Search:    search.NewMemoryIndex(state, ""),
		Postgres:  pg.New(""),
	})
	return router, state
}

func mustMarshalSearchNodePayload(t *testing.T, name string, defaults, normal map[string]any, runList []string) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":             name,
		"json_class":       "Chef::Node",
		"chef_type":        "node",
		"chef_environment": "_default",
		"override":         map[string]any{},
		"normal":           normal,
		"default":          defaults,
		"automatic":        map[string]any{},
		"run_list":         runList,
	})
	if err != nil {
		t.Fatalf("json.Marshal(search node payload) error = %v", err)
	}
	return body
}
