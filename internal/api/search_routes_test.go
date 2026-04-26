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
	"github.com/oberones/OpenCook/internal/testfixtures"
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

func TestSearchIndexesEndpointPinsOrgScopedAliasURLs(t *testing.T) {
	router := newTestRouter(t)

	createReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create org data bag status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	req := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/search", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("org search index status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(org search indexes) error = %v", err)
	}

	expected := map[string]string{
		"client":      "/organizations/ponyville/search/client",
		"environment": "/organizations/ponyville/search/environment",
		"node":        "/organizations/ponyville/search/node",
		"role":        "/organizations/ponyville/search/role",
		"ponies":      "/organizations/ponyville/search/ponies",
	}
	if len(payload) != len(expected) {
		t.Fatalf("org search indexes len = %d, want %d (%v)", len(payload), len(expected), payload)
	}
	for key, want := range expected {
		if payload[key] != want {
			t.Fatalf("org search index %q = %q, want %q", key, payload[key], want)
		}
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

func TestSearchQueryEndpointPinsDefaultPagingOrderingAndOrgAlias(t *testing.T) {
	router := newTestRouter(t)
	createSearchNode(t, router, "twilight", map[string]any{}, map[string]any{}, []string{"web"})
	createSearchNode(t, router, "rainbow", map[string]any{}, map[string]any{}, []string{"base"})

	req := newSignedSearchRequest(t, http.MethodGet, "/search/node?q=name:*", "/search/node", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default-paged search status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeSearchPayload(t, rec)
	if payload["start"] != float64(0) {
		t.Fatalf("default-paged start = %v, want 0", payload["start"])
	}
	if payload["total"] != float64(2) {
		t.Fatalf("default-paged total = %v, want 2", payload["total"])
	}
	rows := payload["rows"].([]any)
	if len(rows) != 2 {
		t.Fatalf("default-paged rows len = %d, want 2 (%v)", len(rows), rows)
	}
	if rows[0].(map[string]any)["name"] != "rainbow" || rows[1].(map[string]any)["name"] != "twilight" {
		t.Fatalf("default-paged row order = %v, want rainbow then twilight", rows)
	}

	orgReq := newSignedSearchRequest(t, http.MethodGet, "/organizations/ponyville/search/node?q=name:twi*", "/organizations/ponyville/search/node", nil)
	orgRec := httptest.NewRecorder()
	router.ServeHTTP(orgRec, orgReq)
	if orgRec.Code != http.StatusOK {
		t.Fatalf("org-scoped search status = %d, want %d, body = %s", orgRec.Code, http.StatusOK, orgRec.Body.String())
	}

	orgPayload := decodeSearchPayload(t, orgRec)
	orgRows := orgPayload["rows"].([]any)
	if orgPayload["start"] != float64(0) || orgPayload["total"] != float64(1) || len(orgRows) != 1 {
		t.Fatalf("org-scoped search payload = %v, want start 0 total 1 one row", orgPayload)
	}
	if orgRows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("org-scoped search row = %v, want twilight", orgRows[0])
	}
}

func TestSearchQueryEndpointSupportsGroupedBooleanPrecedence(t *testing.T) {
	router := newTestRouter(t)
	createSearchNode(t, router, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"web"})
	createSearchNode(t, router, "rainbow", map[string]any{}, map[string]any{"team": "weather"}, []string{"base"})

	assertSearchNames(t, router, searchPath("/search/node", "(team:friendship OR recipe:missing) AND recipe:web"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "name:rainbow OR recipe:web AND team:friendship"), "/search/node", []string{"rainbow", "twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "(name:rainbow OR recipe:web) AND team:friendship"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "recipe:web AND NOT (team:weather OR name:rainbow)"), "/search/node", []string{"twilight"})
}

func TestSearchQueryEndpointSupportsEscapedQuotedAndWordBreakTerms(t *testing.T) {
	router := newTestRouter(t)
	createSearchNode(t, router, "search_supernode", map[string]any{
		"attr_colon":  "hello:world",
		"attr_bang":   "hello!world",
		"attr_phrase": "hello world",
		"path":        "foo/bar",
	}, map[string]any{}, []string{"app::default"})

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create word-break data bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}
	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":          "alice",
		"badge":       "primary[blue]",
		"note":        "hello world",
		"punctuation": "hello!world",
		"path":        "foo/bar",
	}))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create word-break data bag item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	assertSearchNames(t, router, searchPath("/search/node", `attr_colon:hello\:world`), "/search/node", []string{"search_supernode"})
	assertSearchNames(t, router, searchPath("/search/node", `attr_bang:hello\!world`), "/search/node", []string{"search_supernode"})
	assertSearchNames(t, router, searchPath("/search/node", `attr_bang:hello`), "/search/node", []string{})
	assertSearchNames(t, router, searchPath("/search/node", `attr_bang:world`), "/search/node", []string{})
	assertSearchNames(t, router, searchPath("/search/node", `attr_phrase:"hello world"`), "/search/node", []string{"search_supernode"})
	assertSearchNames(t, router, searchPath("/search/node", `attr_bang:*"hello world"*`), "/search/node", []string{})
	assertSearchNames(t, router, searchPath("/search/node", `path:foo\/*`), "/search/node", []string{"search_supernode"})
	assertSearchNames(t, router, searchPath("/search/node", `recipe:app\:\:default`), "/search/node", []string{"search_supernode"})
	assertSearchNames(t, router, searchPath("/search/ponies", `badge:primary\[blue\]`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/ponies", `note:"hello world"`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/ponies", `punctuation:*\!*`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/ponies", `punctuation:*"hello world"*`), "/search/ponies", []string{})
}

func TestSearchQueryEndpointSupportsWildcardAndExistenceSemantics(t *testing.T) {
	router := newTestRouter(t)

	nodeBody := mustMarshalSearchNodePayload(t, "twilight", map[string]any{
		"top": map[string]any{
			"mid": map[string]any{
				"bottom": "nested-target",
			},
		},
	}, map[string]any{}, []string{"app::default", "role[webserver]"})
	var nodePayload map[string]any
	if err := json.Unmarshal(nodeBody, &nodePayload); err != nil {
		t.Fatalf("json.Unmarshal(wildcard node payload) error = %v", err)
	}
	nodePayload["policy_name"] = "delivery-app"
	nodePayload["policy_group"] = "prod-blue"
	nodeBody = mustMarshalDataBagJSON(t, nodePayload)
	createNodeReq := newSignedJSONRequest(t, http.MethodPost, "/nodes", nodeBody)
	createNodeRec := httptest.NewRecorder()
	router.ServeHTTP(createNodeRec, createNodeReq)
	if createNodeRec.Code != http.StatusCreated {
		t.Fatalf("create wildcard node status = %d, want %d, body = %s", createNodeRec.Code, http.StatusCreated, createNodeRec.Body.String())
	}

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create wildcard data bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}
	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":    "alice",
		"badge": "primary[blue]",
		"path":  "foo/bar",
	}))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create wildcard data bag item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	secretBag := "secrets"
	createSecretBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": secretBag}))
	createSecretBagRec := httptest.NewRecorder()
	router.ServeHTTP(createSecretBagRec, createSecretBagReq)
	if createSecretBagRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted wildcard data bag status = %d, want %d, body = %s", createSecretBagRec.Code, http.StatusCreated, createSecretBagRec.Body.String())
	}
	createSecretItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/"+secretBag, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	createSecretItemRec := httptest.NewRecorder()
	router.ServeHTTP(createSecretItemRec, createSecretItemReq)
	if createSecretItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted wildcard item status = %d, want %d, body = %s", createSecretItemRec.Code, http.StatusCreated, createSecretItemRec.Body.String())
	}

	assertSearchNames(t, router, searchPath("/search/node", "*:*"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "name:*"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "missing:*"), "/search/node", []string{})
	assertSearchNames(t, router, searchPath("/search/node", "name:*light"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "name:twi*ght"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "name:twi?ight"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "top_mid_bottom:nested-target"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "bottom:nested-target"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "top_*_bottom:nested-target"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", `run_list:*app\:\:default*`), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "recipe:*default"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "role:web*"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "policy_name:*app"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "policy_group:prod*blue"), "/search/node", []string{"twilight"})
	assertSearchNames(t, router, searchPath("/search/ponies", `*:primary\[blue\]`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/ponies", "ba*:primary*"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/ponies", `path:*\/bar`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertSearchNames(t, router, searchPath("/search/"+secretBag, "*_encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + testfixtures.EncryptedDataBagItemID()})
	assertSearchNames(t, router, searchPath("/search/"+secretBag, "encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + testfixtures.EncryptedDataBagItemID()})
}

func TestSearchQueryEndpointSupportsRangeSemantics(t *testing.T) {
	router := newTestRouter(t)

	createRangedNode := func(name, build, channel string) {
		t.Helper()
		createSearchNode(t, router, name, map[string]any{
			"build":   build,
			"channel": channel,
		}, map[string]any{}, []string{"base"})
	}
	createRangedNode("applejack", "001", "alpha")
	createRangedNode("rainbow", "010", "stable")
	createRangedNode("twilight", "100", "zeta")

	assertSearchNames(t, router, searchPath("/search/node", "build:[001 TO 099]"), "/search/node", []string{"applejack", "rainbow"})
	assertSearchNames(t, router, searchPath("/search/node", "build:{001 TO 099}"), "/search/node", []string{"rainbow"})
	assertSearchNames(t, router, searchPath("/search/node", "build:{010 TO 010}"), "/search/node", []string{})
	assertSearchNames(t, router, searchPath("/search/node", "build:[* TO 010]"), "/search/node", []string{"applejack", "rainbow"})
	assertSearchNames(t, router, searchPath("/search/node", "build:[010 TO *]"), "/search/node", []string{"rainbow", "twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "build:[* TO *]"), "/search/node", []string{"applejack", "rainbow", "twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "channel:[beta TO zzz]"), "/search/node", []string{"rainbow", "twilight"})
	assertSearchNames(t, router, searchPath("/search/node", "name:rainbow AND build:[001 TO 099]"), "/search/node", []string{"rainbow"})

	invalidReq := newSignedSearchRequest(t, http.MethodGet, searchPath("/search/node", "build:[001 099]"), "/search/node", nil)
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("malformed range status = %d, want %d, body = %s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}
	assertAPIError(t, invalidRec, "invalid_search_query")
}

func TestSearchRoutesSupportWidenedQueryFormsAcrossSurfaces(t *testing.T) {
	router := newTestRouter(t)

	postJSON := func(path string, body []byte) {
		t.Helper()

		req := newSignedJSONRequest(t, http.MethodPost, path, body)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("POST %s status = %d, want %d, body = %s", path, rec.Code, http.StatusCreated, rec.Body.String())
		}
	}

	postJSON("/environments", []byte(`{"name":"production","json_class":"Chef::Environment","chef_type":"environment","description":"production search target","cookbook_versions":{},"default_attributes":{"region":"equus"},"override_attributes":{}}`))
	postJSON("/roles", []byte(`{"name":"web","description":"visible role search target","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{"tier":"frontend"},"run_list":["base"],"env_run_lists":{}}`))
	createSearchNode(t, router, "twilight", map[string]any{
		"path":  "foo/bar",
		"build": "010",
	}, map[string]any{
		"team": "friendship",
	}, []string{"web"})
	createDataBagForTest(t, router, "/data", "ponies")
	postJSON("/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":    "alice",
		"badge": "primary[blue]",
		"note":  "hello world",
	}))

	secretBag := testfixtures.EncryptedDataBagName()
	secretItemName := "data_bag_item_" + secretBag + "_" + testfixtures.EncryptedDataBagItemID()
	createDataBagForTest(t, router, "/data", secretBag)
	postJSON("/data/"+secretBag, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))

	for _, tc := range []struct {
		name     string
		rawPath  string
		signPath string
		want     []string
	}{
		{name: "client default alias", rawPath: searchPath("/search/client", "name:org-*"), signPath: "/search/client", want: []string{"org-validator"}},
		{name: "client org alias", rawPath: searchPath("/organizations/ponyville/search/client", "clientname:org-*"), signPath: "/organizations/ponyville/search/client", want: []string{"org-validator"}},
		{name: "environment default alias", rawPath: searchPath("/search/environment", `description:"production search target"`), signPath: "/search/environment", want: []string{"production"}},
		{name: "environment org alias", rawPath: searchPath("/organizations/ponyville/search/environment", "description:*search*"), signPath: "/organizations/ponyville/search/environment", want: []string{"production"}},
		{name: "node default alias", rawPath: searchPath("/search/node", "(team:friendship OR recipe:missing) AND build:[001 TO 099]"), signPath: "/search/node", want: []string{"twilight"}},
		{name: "node org alias", rawPath: searchPath("/organizations/ponyville/search/node", `path:foo\/*`), signPath: "/organizations/ponyville/search/node", want: []string{"twilight"}},
		{name: "role default alias", rawPath: searchPath("/search/role", "description:*role*"), signPath: "/search/role", want: []string{"web"}},
		{name: "role org alias", rawPath: searchPath("/organizations/ponyville/search/role", "(name:web OR name:db) AND NOT name:db"), signPath: "/organizations/ponyville/search/role", want: []string{"web"}},
		{name: "data bag default alias", rawPath: searchPath("/search/ponies", "ba*:primary*"), signPath: "/search/ponies", want: []string{"data_bag_item_ponies_alice"}},
		{name: "data bag org alias", rawPath: searchPath("/organizations/ponyville/search/ponies", `note:"hello world"`), signPath: "/organizations/ponyville/search/ponies", want: []string{"data_bag_item_ponies_alice"}},
		{name: "encrypted data bag default alias", rawPath: searchPath("/search/"+secretBag, "*_encrypted_data:*"), signPath: "/search/" + secretBag, want: []string{secretItemName}},
		{name: "encrypted data bag org alias", rawPath: searchPath("/organizations/ponyville/search/"+secretBag, "environment:production AND *_encrypted_data:*"), signPath: "/organizations/ponyville/search/" + secretBag, want: []string{secretItemName}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertSearchNames(t, router, tc.rawPath, tc.signPath, tc.want)
		})
	}

	assertSearchPartialData(t, router, http.MethodPost, searchPath("/search/client", "name:org-*"), "/search/client", []byte(`{"org":["orgname"]}`), "/clients/org-validator", map[string]any{"org": "ponyville"})
	assertSearchPartialData(t, router, http.MethodPost, searchPath("/organizations/ponyville/search/environment", "description:*search*"), "/organizations/ponyville/search/environment", []byte(`{"desc":["description"]}`), "/organizations/ponyville/environments/production", map[string]any{"desc": "production search target"})
	assertSearchPartialData(t, router, http.MethodPost, searchPath("/search/node", "(team:friendship OR recipe:missing) AND build:[001 TO 099]"), "/search/node", []byte(`{"team":["team"],"build":["build"]}`), "/nodes/twilight", map[string]any{"team": "friendship", "build": "010"})
	assertSearchPartialData(t, router, http.MethodPost, searchPath("/organizations/ponyville/search/role", "description:*role*"), "/organizations/ponyville/search/role", []byte(`{"desc":["description"]}`), "/organizations/ponyville/roles/web", map[string]any{"desc": "visible role search target"})
	assertSearchPartialData(t, router, http.MethodPost, searchPath("/search/ponies", "ba*:primary*"), "/search/ponies", []byte(`{"badge":["badge"],"note":["note"]}`), "/data/ponies/alice", map[string]any{"badge": "primary[blue]", "note": "hello world"})

	encryptedPartialRec := serveSignedSearchRequestAs(t, router, "silent-bob", http.MethodPost, searchPath("/organizations/ponyville/search/"+secretBag, "*_encrypted_data:*"), "/organizations/ponyville/search/"+secretBag, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, encryptedPartialRec), "/organizations/ponyville/data/"+secretBag+"/"+testfixtures.EncryptedDataBagItemID())
}

func TestSearchRoutesPinPagingTotalsAndOrderingForWidenedQueries(t *testing.T) {
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:charlie": {},
				"node:hotel":   {},
			},
		}
	})

	for _, name := range []string{"zeta", "alpha", "kilo", "bravo", "hotel", "charlie", "india", "delta", "juliet", "echo", "foxtrot", "golf"} {
		createSearchNode(t, router, name, map[string]any{
			"sequence": "050",
		}, map[string]any{
			"team": "fleet",
		}, []string{"base"})
	}

	query := "(team:fleet OR recipe:missing) AND sequence:[001 TO 999]"
	expected := []string{"alpha", "bravo", "delta", "echo", "foxtrot", "golf", "india", "juliet", "kilo", "zeta"}

	assertSearchPageNames(t, router, searchPath("/search/node", query), "/search/node", 0, 10, expected)
	assertSearchPageNames(t, router, searchPath("/search/node", query)+"&rows=0", "/search/node", 0, 10, expected)
	assertSearchPageNames(t, router, searchPath("/search/node", query)+"&rows=100000", "/search/node", 0, 10, expected)
	assertSearchPageNames(t, router, searchPath("/search/node", query)+"&start=2&rows=3", "/search/node", 2, 10, []string{"delta", "echo", "foxtrot"})
	assertSearchPageNames(t, router, searchPath("/search/node", query)+"&start=999&rows=25", "/search/node", 999, 10, []string{})
}

func TestSearchVisibleStateDoesNotChangeAfterInvalidNodeMutation(t *testing.T) {
	router := newTestRouter(t)
	createSearchNode(t, router, "twilight", map[string]any{}, map[string]any{}, []string{"base"})

	assertSearchTotal(t, router, "/search/node?q=recipe:base", "/search/node", 1)
	assertSearchTotal(t, router, "/search/node?q=role:web", "/search/node", 0)

	body := mustMarshalSearchNodePayload(t, "rainbow", map[string]any{}, map[string]any{}, []string{"role[web]"})
	req := newSignedJSONRequest(t, http.MethodPut, "/nodes/twilight", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid node update status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertSearchTotal(t, router, "/search/node?q=recipe:base", "/search/node", 1)
	assertSearchTotal(t, router, "/search/node?q=role:web", "/search/node", 0)
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

func TestSearchRoutesPinCurrentMethodAndPagingErrors(t *testing.T) {
	router := newTestRouter(t)

	indexReq := newSignedJSONRequest(t, http.MethodPost, "/search", []byte(`{}`))
	indexRec := httptest.NewRecorder()
	router.ServeHTTP(indexRec, indexReq)
	if indexRec.Code != http.StatusNotFound {
		t.Fatalf("POST /search status = %d, want %d, body = %s", indexRec.Code, http.StatusNotFound, indexRec.Body.String())
	}
	assertAPIError(t, indexRec, "not_found")

	queryReq := newSignedSearchRequest(t, http.MethodPut, "/search/node?q=*:*", "/search/node", nil)
	queryRec := httptest.NewRecorder()
	router.ServeHTTP(queryRec, queryReq)
	if queryRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT /search/node status = %d, want %d, body = %s", queryRec.Code, http.StatusMethodNotAllowed, queryRec.Body.String())
	}
	assertAPIError(t, queryRec, "method_not_allowed")

	startReq := newSignedSearchRequest(t, http.MethodGet, "/search/node?q=*:*&start=-1", "/search/node", nil)
	startRec := httptest.NewRecorder()
	router.ServeHTTP(startRec, startReq)
	if startRec.Code != http.StatusBadRequest {
		t.Fatalf("negative start status = %d, want %d, body = %s", startRec.Code, http.StatusBadRequest, startRec.Body.String())
	}
	assertAPIError(t, startRec, "invalid_search_query")

	rowsReq := newSignedSearchRequest(t, http.MethodGet, "/search/node?q=*:*&rows=nope", "/search/node", nil)
	rowsRec := httptest.NewRecorder()
	router.ServeHTTP(rowsRec, rowsReq)
	if rowsRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid rows status = %d, want %d, body = %s", rowsRec.Code, http.StatusBadRequest, rowsRec.Body.String())
	}
	assertAPIError(t, rowsRec, "invalid_search_query")

	invalidQueryReq := newSignedSearchRequest(t, http.MethodGet, searchPath("/search/node", "(name:twilight"), "/search/node", nil)
	invalidQueryRec := httptest.NewRecorder()
	router.ServeHTTP(invalidQueryRec, invalidQueryReq)
	if invalidQueryRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid query status = %d, want %d, body = %s", invalidQueryRec.Code, http.StatusBadRequest, invalidQueryRec.Body.String())
	}
	assertAPIError(t, invalidQueryRec, "invalid_search_query")
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

// TestSearchDataBagEndpointTreatsEncryptedItemsAsOpaqueSearchDocuments pins the
// memory-backed search contract for Chef encrypted-data-bag-looking payloads:
// the server indexes and returns the stored JSON without decrypting or requiring
// any data bag secret.
func TestSearchDataBagEndpointTreatsEncryptedItemsAsOpaqueSearchDocuments(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	passwordCiphertext := encryptedDataBagFixtureField(t, "password", "encrypted_data")
	passwordIV := encryptedDataBagFixtureField(t, "password", "iv")
	apiAuthTag := encryptedDataBagFixtureField(t, "api_key", "auth_tag")

	createDataBagForTest(t, router, "/data", bagName)
	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/"+bagName, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted data bag item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	fullRec := serveSignedSearchRequestAs(t, router, "silent-bob", http.MethodGet, searchPath("/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, nil)
	assertEncryptedDataBagSearchFullRow(t, decodeSearchPayload(t, fullRec), bagName, itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, fullRec)

	partialRec := serveSignedSearchRequestAs(t, router, "silent-bob", http.MethodPost, searchPath("/organizations/ponyville/search/"+bagName, "environment:production AND api_key_auth_tag:"+apiAuthTag), "/organizations/ponyville/search/"+bagName, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, partialRec), "/organizations/ponyville/data/"+bagName+"/"+itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, partialRec)

	assertSearchTotal(t, router, searchPath("/search/"+bagName, "password_iv:"+passwordIV), "/search/"+bagName, 1)
	assertSearchTotal(t, router, searchPath("/search/"+bagName, "encrypted_data:"+passwordCiphertext), "/search/"+bagName, 1)
	assertSearchTotal(t, router, searchPath("/search/"+bagName, "environment:production"), "/search/"+bagName, 1)
	assertSearchTotal(t, router, searchPath("/search/"+bagName, "raw_data_password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, 0)
	assertSearchTotal(t, router, searchPath("/search/"+bagName, "correct-horse-battery-staple"), "/search/"+bagName, 0)
}

// TestSearchDataBagEndpointFiltersDeniedEncryptedItems proves encrypted-looking
// data bag documents still go through the normal ACL filter after query matching.
func TestSearchDataBagEndpointFiltersDeniedEncryptedItems(t *testing.T) {
	bagName := testfixtures.EncryptedDataBagName()
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		return &denySearchDocumentAfterIndexGateAuthorizer{
			base:   authz.NewACLAuthorizer(state),
			target: "data_bag:" + bagName,
		}
	})

	createDataBagForTest(t, router, "/data", bagName)
	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/"+bagName, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted data bag item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	query := "password_encrypted_data:" + encryptedDataBagFixtureField(t, "password", "encrypted_data")
	assertSearchTotal(t, router, searchPath("/search/"+bagName, query), "/search/"+bagName, 0)
}

func TestSearchDataBagEndpointReturnsChefStyleNotFound(t *testing.T) {
	router := newTestRouter(t)

	for _, route := range []struct {
		name     string
		rawPath  string
		signPath string
	}{
		{name: "default", rawPath: "/search/no_bag?q=id:*", signPath: "/search/no_bag"},
		{name: "org_scoped", rawPath: "/organizations/ponyville/search/no_bag?q=id:*", signPath: "/organizations/ponyville/search/no_bag"},
	} {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedSearchRequest(t, http.MethodGet, route.rawPath, route.signPath, nil)
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
		})
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
	if payload["start"] != float64(0) || payload["total"] != float64(1) {
		t.Fatalf("filtered search start/total = %v/%v, want 0/1", payload["start"], payload["total"])
	}
	row := rows[0].(map[string]any)
	if row["name"] != "web" {
		t.Fatalf("filtered search row name = %v, want %q", row["name"], "web")
	}
	assertSearchNames(t, router, searchPath("/search/role", "(name:web OR name:secret) AND name:*"), "/search/role", []string{"web"})
}

func TestSearchEndpointPaginatesBroadWildcardAfterACLFiltering(t *testing.T) {
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:rainbow": {},
			},
		}
	})
	createSearchNode(t, router, "rainbow", map[string]any{}, map[string]any{}, []string{"base"})
	createSearchNode(t, router, "rarity", map[string]any{}, map[string]any{}, []string{"base"})
	createSearchNode(t, router, "twilight", map[string]any{}, map[string]any{}, []string{"base"})

	req := newSignedSearchRequest(t, http.MethodGet, searchPath("/search/node", "name:*")+"&rows=1&start=1", "/search/node", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("wildcard ACL-paged search status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(2) || payload["start"] != float64(1) || len(rows) != 1 {
		t.Fatalf("wildcard ACL-paged payload = %v, want start 1 total 2 one row", payload)
	}
	if rows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("wildcard ACL-paged row = %v, want twilight", rows[0])
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

// denySearchDocumentAfterIndexGateAuthorizer allows the search index gate but
// denies the later hydrated-document read. Data bag search uses the parent bag
// as both resources, so the call count lets tests prove post-query filtering.
type denySearchDocumentAfterIndexGateAuthorizer struct {
	base   authz.Authorizer
	target string
	reads  int
}

// Name identifies the test-only authorizer in failures without affecting route
// behavior under the normal ACL authorizer.
func (a *denySearchDocumentAfterIndexGateAuthorizer) Name() string {
	return "deny-search-document-after-index-gate-test"
}

// Authorize delegates normal checks, but denies the second read of the target
// resource so tests can distinguish route authorization from result filtering.
func (a *denySearchDocumentAfterIndexGateAuthorizer) Authorize(ctx context.Context, subject authz.Subject, action authz.Action, resource authz.Resource) (authz.Decision, error) {
	if action == authz.ActionRead && resource.Type+":"+resource.Name == a.target {
		a.reads++
		if a.reads > 1 {
			return authz.Decision{Allowed: false, Reason: "denied for document filter test"}, nil
		}
	}
	if a.base == nil {
		return authz.Decision{Allowed: true, Reason: "no base authorizer"}, nil
	}
	return a.base.Authorize(ctx, subject, action, resource)
}

func newSearchTestRouterWithAuthorizer(t *testing.T, authorizerFactory func(*bootstrap.Service) authz.Authorizer) (http.Handler, *bootstrap.Service) {
	t.Helper()

	return newSearchTestRouterWithConfigAndAuthorizer(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, authorizerFactory)
}

func newSearchTestRouterWithConfigAndAuthorizer(t *testing.T, cfg config.Config, authorizerFactory func(*bootstrap.Service) authz.Authorizer) (http.Handler, *bootstrap.Service) {
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

	if cfg.ServiceName == "" {
		cfg.ServiceName = "opencook"
	}
	if cfg.Environment == "" {
		cfg.Environment = "test"
	}
	if cfg.AuthSkew == 0 {
		cfg.AuthSkew = 15 * time.Minute
	}
	if cfg.MaxAuthBodyBytes == 0 {
		cfg.MaxAuthBodyBytes = config.DefaultMaxAuthBodyBytes
	}

	skew := cfg.AuthSkew
	now := func() time.Time {
		return mustParseTime(t, "2026-04-02T15:04:35Z")
	}
	router := NewRouter(Dependencies{
		Logger:  log.New(ioDiscard{}, "", 0),
		Config:  cfg,
		Version: version.Current(),
		Compat:  compat.NewDefaultRegistry(),
		Now:     now,
		Authn: authn.NewChefVerifier(store, authn.Options{
			AllowedClockSkew: &skew,
			Now:              now,
		}),
		Authz:            authorizer,
		Bootstrap:        state,
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         pg.New(""),
	})
	return router, state
}

// newSignedSearchRequest signs a search request as the default org member used
// by most memory-backed route tests.
func newSignedSearchRequest(t *testing.T, method, rawPath, signPath string, body []byte) *http.Request {
	t.Helper()

	return newSignedSearchRequestAs(t, "silent-bob", method, rawPath, signPath, body)
}

// newSignedSearchRequestAs lets search tests exercise the same signed route
// surface with alternate actors when they need ACL-specific assertions.
func newSignedSearchRequestAs(t *testing.T, userID, method, rawPath, signPath string, body []byte) *http.Request {
	t.Helper()

	req := httptest.NewRequest(method, rawPath, bytes.NewReader(body))
	applySignedHeaders(t, req, userID, "", method, signPath, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	return req
}

// serveSignedSearchRequestAs performs a signed search request and fails fast on
// non-200 responses so search-shape assertions can stay focused on payloads.
func serveSignedSearchRequestAs(t *testing.T, router http.Handler, userID, method, rawPath, signPath string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := newSignedSearchRequestAs(t, userID, method, rawPath, signPath, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec
}

// encryptedDataBagFixtureField extracts a nested encrypted envelope value from
// the shared fixture so search tests do not duplicate ciphertext constants.
func encryptedDataBagFixtureField(t *testing.T, envelopeName, fieldName string) string {
	t.Helper()

	envelope, ok := testfixtures.EncryptedDataBagItem()[envelopeName].(map[string]any)
	if !ok {
		t.Fatalf("fixture field %q = %T, want envelope object", envelopeName, testfixtures.EncryptedDataBagItem()[envelopeName])
	}
	value, ok := envelope[fieldName].(string)
	if !ok {
		t.Fatalf("fixture field %s.%s = %T, want string", envelopeName, fieldName, envelope[fieldName])
	}
	return value
}

// encryptedDataBagPartialSearchBody selects encrypted envelope members plus a
// clear metadata field, matching how Chef partial search returns stored values.
func encryptedDataBagPartialSearchBody(t *testing.T) []byte {
	t.Helper()

	body, err := json.Marshal(map[string][]string{
		"password_ciphertext": {"password", "encrypted_data"},
		"password_iv":         {"password", "iv"},
		"api_auth_tag":        {"api_key", "auth_tag"},
		"environment":         {"environment"},
	})
	if err != nil {
		t.Fatalf("json.Marshal(encrypted partial search body) error = %v", err)
	}
	return body
}

// assertEncryptedDataBagSearchFullRow checks the Chef-style full search row and
// confirms encrypted-looking JSON is nested under raw_data exactly as stored.
func assertEncryptedDataBagSearchFullRow(t *testing.T, payload map[string]any, bagName, itemID string) {
	t.Helper()

	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("encrypted full search payload = %v, want one row", payload)
	}
	row := rows[0].(map[string]any)
	wantName := "data_bag_item_" + bagName + "_" + itemID
	if row["name"] != wantName {
		t.Fatalf("encrypted search row name = %v, want %q", row["name"], wantName)
	}
	if row["data_bag"] != bagName {
		t.Fatalf("encrypted search row data_bag = %v, want %q", row["data_bag"], bagName)
	}
	rawData, ok := row["raw_data"].(map[string]any)
	if !ok {
		t.Fatalf("encrypted search row raw_data = %T(%v), want object", row["raw_data"], row["raw_data"])
	}
	assertRawDataBagItemPayload(t, rawData, testfixtures.EncryptedDataBagItem())
}

// assertEncryptedDataBagPartialSearchRow verifies partial search returns only
// the requested stored envelope fields and clear metadata values.
func assertEncryptedDataBagPartialSearchRow(t *testing.T, payload map[string]any, wantURL string) {
	t.Helper()

	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("encrypted partial search payload = %v, want one row", payload)
	}
	row := rows[0].(map[string]any)
	if row["url"] != wantURL {
		t.Fatalf("encrypted partial search url = %v, want %q", row["url"], wantURL)
	}
	data := row["data"].(map[string]any)
	if data["password_ciphertext"] != encryptedDataBagFixtureField(t, "password", "encrypted_data") {
		t.Fatalf("partial password_ciphertext = %v, want stored ciphertext", data["password_ciphertext"])
	}
	if data["password_iv"] != encryptedDataBagFixtureField(t, "password", "iv") {
		t.Fatalf("partial password_iv = %v, want stored iv", data["password_iv"])
	}
	if data["api_auth_tag"] != encryptedDataBagFixtureField(t, "api_key", "auth_tag") {
		t.Fatalf("partial api_auth_tag = %v, want stored auth tag", data["api_auth_tag"])
	}
	if data["environment"] != "production" {
		t.Fatalf("partial environment = %v, want production", data["environment"])
	}
}

// assertSearchResponseOmitsDecryptedPlaintext guards against accidentally
// decoding or inventing plaintext while handling encrypted-looking envelopes.
func assertSearchResponseOmitsDecryptedPlaintext(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	for _, forbidden := range [][]byte{
		[]byte("example-password-ciphertext"),
		[]byte("correct horse battery staple"),
		[]byte("data_bag_secret"),
	} {
		if bytes.Contains(rec.Body.Bytes(), forbidden) {
			t.Fatalf("search response unexpectedly exposed plaintext marker %q: %s", string(forbidden), rec.Body.String())
		}
	}
}

func createSearchNode(t *testing.T, router http.Handler, name string, defaults, normal map[string]any, runList []string) {
	t.Helper()

	body := mustMarshalSearchNodePayload(t, name, defaults, normal, runList)
	req := newSignedJSONRequest(t, http.MethodPost, "/nodes", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create search node %q status = %d, want %d, body = %s", name, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func decodeSearchPayload(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search payload) error = %v, body = %s", err, rec.Body.String())
	}
	return payload
}

func assertSearchTotal(t *testing.T, router http.Handler, rawPath, signPath string, want int) {
	t.Helper()

	req := newSignedSearchRequest(t, http.MethodGet, rawPath, signPath, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search %s status = %d, want %d, body = %s", rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeSearchPayload(t, rec)
	if payload["total"] != float64(want) {
		t.Fatalf("search %s total = %v, want %d", rawPath, payload["total"], want)
	}
}

// assertSearchPartialData verifies POST partial search route behavior while
// callers vary the query language and default-org/org-scoped aliases.
func assertSearchPartialData(t *testing.T, router http.Handler, method, rawPath, signPath string, body []byte, wantURL string, wantData map[string]any) {
	t.Helper()

	rec := serveSignedSearchRequestAs(t, router, "silent-bob", method, rawPath, signPath, body)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("partial search %s payload = %v, want one row", rawPath, payload)
	}
	row := rows[0].(map[string]any)
	if row["url"] != wantURL {
		t.Fatalf("partial search %s url = %v, want %q", rawPath, row["url"], wantURL)
	}
	data := row["data"].(map[string]any)
	for key, want := range wantData {
		if data[key] != want {
			t.Fatalf("partial search %s data[%q] = %v, want %v (data=%v)", rawPath, key, data[key], want, data)
		}
	}
}

// assertSearchPageNames pins paging against filtered route results, not raw
// provider candidates, while preserving the current deterministic row order.
func assertSearchPageNames(t *testing.T, router http.Handler, rawPath, signPath string, wantStart, wantTotal int, wantNames []string) {
	t.Helper()

	req := newSignedSearchRequest(t, http.MethodGet, rawPath, signPath, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search page %s status = %d, want %d, body = %s", rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeSearchPayload(t, rec)
	if payload["start"] != float64(wantStart) || payload["total"] != float64(wantTotal) {
		t.Fatalf("search page %s start/total = %v/%v, want %d/%d", rawPath, payload["start"], payload["total"], wantStart, wantTotal)
	}
	rows := payload["rows"].([]any)
	if len(rows) != len(wantNames) {
		t.Fatalf("search page %s rows len = %d, want %d (%v)", rawPath, len(rows), len(wantNames), rows)
	}
	for i, wantName := range wantNames {
		if got := rows[i].(map[string]any)["name"]; got != wantName {
			t.Fatalf("search page %s row[%d] name = %v, want %q", rawPath, i, got, wantName)
		}
	}
}

// assertSearchNames verifies a full search route returns the expected hydrated
// row names in order after provider matching and route-level ACL filtering.
func assertSearchNames(t *testing.T, router http.Handler, rawPath, signPath string, want []string) {
	t.Helper()

	req := newSignedSearchRequest(t, http.MethodGet, rawPath, signPath, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search %s status = %d, want %d, body = %s", rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(len(want)) || len(rows) != len(want) {
		t.Fatalf("search %s payload = %v, want %d rows", rawPath, payload, len(want))
	}
	for i, wantName := range want {
		got := rows[i].(map[string]any)["name"]
		if got != wantName {
			t.Fatalf("search %s row[%d] name = %v, want %q", rawPath, i, got, wantName)
		}
	}
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
