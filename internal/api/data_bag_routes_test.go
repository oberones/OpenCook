package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDataEndpointCreatesAndListsDataBags(t *testing.T) {
	router := newTestRouter(t)

	listReq := newSignedJSONRequest(t, http.MethodGet, "/data", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("initial list status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var emptyPayload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &emptyPayload); err != nil {
		t.Fatalf("json.Unmarshal(initial list) error = %v", err)
	}
	if len(emptyPayload) != 0 {
		t.Fatalf("initial list = %v, want empty object", emptyPayload)
	}

	createBody := mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"})
	createReq := newSignedJSONRequest(t, http.MethodPost, "/data", createBody)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create) error = %v", err)
	}
	if createPayload["uri"] != "/data/ponies" {
		t.Fatalf("create uri = %v, want %q", createPayload["uri"], "/data/ponies")
	}

	listReq = newSignedJSONRequest(t, http.MethodGet, "/data", nil)
	listRec = httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list) error = %v", err)
	}
	if listPayload["ponies"] != "/data/ponies" {
		t.Fatalf("list payload = %v, want ponies => /data/ponies", listPayload)
	}
}

func TestDataBagItemLifecycleMatchesCompatibilityShape(t *testing.T) {
	router := newTestRouter(t)

	createBagBody := mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"})
	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", createBagBody)
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}

	getBagReq := newSignedJSONRequest(t, http.MethodGet, "/data/ponies", nil)
	getBagRec := httptest.NewRecorder()
	router.ServeHTTP(getBagRec, getBagReq)
	if getBagRec.Code != http.StatusOK {
		t.Fatalf("get empty bag status = %d, want %d, body = %s", getBagRec.Code, http.StatusOK, getBagRec.Body.String())
	}

	var emptyBagPayload map[string]string
	if err := json.Unmarshal(getBagRec.Body.Bytes(), &emptyBagPayload); err != nil {
		t.Fatalf("json.Unmarshal(empty bag) error = %v", err)
	}
	if len(emptyBagPayload) != 0 {
		t.Fatalf("empty bag payload = %v, want empty object", emptyBagPayload)
	}

	createItemBody := mustMarshalDataBagJSON(t, map[string]any{
		"id":  "twilight",
		"foo": "bar",
		"nested": map[string]any{
			"friend": "spike",
		},
	})
	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", createItemBody)
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)

	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	var createItemPayload map[string]any
	if err := json.Unmarshal(createItemRec.Body.Bytes(), &createItemPayload); err != nil {
		t.Fatalf("json.Unmarshal(create item) error = %v", err)
	}
	if createItemPayload["id"] != "twilight" {
		t.Fatalf("create item id = %v, want %q", createItemPayload["id"], "twilight")
	}
	if createItemPayload["chef_type"] != "data_bag_item" {
		t.Fatalf("create item chef_type = %v, want %q", createItemPayload["chef_type"], "data_bag_item")
	}
	if createItemPayload["data_bag"] != "ponies" {
		t.Fatalf("create item data_bag = %v, want %q", createItemPayload["data_bag"], "ponies")
	}
	if createItemPayload["foo"] != "bar" {
		t.Fatalf("create item foo = %v, want %q", createItemPayload["foo"], "bar")
	}

	getItemReq := newSignedJSONRequest(t, http.MethodGet, "/data/ponies/twilight", nil)
	getItemRec := httptest.NewRecorder()
	router.ServeHTTP(getItemRec, getItemReq)

	if getItemRec.Code != http.StatusOK {
		t.Fatalf("get item status = %d, want %d, body = %s", getItemRec.Code, http.StatusOK, getItemRec.Body.String())
	}

	var getItemPayload map[string]any
	if err := json.Unmarshal(getItemRec.Body.Bytes(), &getItemPayload); err != nil {
		t.Fatalf("json.Unmarshal(get item) error = %v", err)
	}
	if getItemPayload["id"] != "twilight" {
		t.Fatalf("get item id = %v, want %q", getItemPayload["id"], "twilight")
	}
	if _, ok := getItemPayload["chef_type"]; ok {
		t.Fatalf("get item unexpectedly included chef_type: %v", getItemPayload)
	}
	if _, ok := getItemPayload["data_bag"]; ok {
		t.Fatalf("get item unexpectedly included data_bag: %v", getItemPayload)
	}

	updateItemBody := mustMarshalDataBagJSON(t, map[string]any{
		"foo": "updated",
		"extra": map[string]any{
			"answer": float64(42),
		},
	})
	updateItemReq := newSignedJSONRequest(t, http.MethodPut, "/data/ponies/twilight", updateItemBody)
	updateItemRec := httptest.NewRecorder()
	router.ServeHTTP(updateItemRec, updateItemReq)

	if updateItemRec.Code != http.StatusOK {
		t.Fatalf("update item status = %d, want %d, body = %s", updateItemRec.Code, http.StatusOK, updateItemRec.Body.String())
	}

	var updateItemPayload map[string]any
	if err := json.Unmarshal(updateItemRec.Body.Bytes(), &updateItemPayload); err != nil {
		t.Fatalf("json.Unmarshal(update item) error = %v", err)
	}
	if updateItemPayload["id"] != "twilight" {
		t.Fatalf("update item id = %v, want %q", updateItemPayload["id"], "twilight")
	}
	if updateItemPayload["chef_type"] != "data_bag_item" {
		t.Fatalf("update item chef_type = %v, want %q", updateItemPayload["chef_type"], "data_bag_item")
	}

	deleteItemReq := newSignedJSONRequest(t, http.MethodDelete, "/data/ponies/twilight", nil)
	deleteItemRec := httptest.NewRecorder()
	router.ServeHTTP(deleteItemRec, deleteItemReq)

	if deleteItemRec.Code != http.StatusOK {
		t.Fatalf("delete item status = %d, want %d, body = %s", deleteItemRec.Code, http.StatusOK, deleteItemRec.Body.String())
	}

	var deleteItemPayload map[string]any
	if err := json.Unmarshal(deleteItemRec.Body.Bytes(), &deleteItemPayload); err != nil {
		t.Fatalf("json.Unmarshal(delete item) error = %v", err)
	}
	if deleteItemPayload["name"] != "data_bag_item_ponies_twilight" {
		t.Fatalf("delete item name = %v, want %q", deleteItemPayload["name"], "data_bag_item_ponies_twilight")
	}
	if deleteItemPayload["data_bag"] != "ponies" {
		t.Fatalf("delete item data_bag = %v, want %q", deleteItemPayload["data_bag"], "ponies")
	}
	rawData, ok := deleteItemPayload["raw_data"].(map[string]any)
	if !ok {
		t.Fatalf("delete item raw_data = %T, want map[string]any", deleteItemPayload["raw_data"])
	}
	if rawData["id"] != "twilight" {
		t.Fatalf("delete item raw_data[id] = %v, want %q", rawData["id"], "twilight")
	}
	if rawData["foo"] != "updated" {
		t.Fatalf("delete item raw_data[foo] = %v, want %q", rawData["foo"], "updated")
	}

	missingReq := newSignedJSONRequest(t, http.MethodGet, "/data/ponies/twilight", nil)
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)

	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing item status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}

	var missingPayload map[string][]string
	if err := json.Unmarshal(missingRec.Body.Bytes(), &missingPayload); err != nil {
		t.Fatalf("json.Unmarshal(missing item) error = %v", err)
	}
	if len(missingPayload["error"]) != 1 || missingPayload["error"][0] != "Cannot load data bag item twilight for data bag ponies" {
		t.Fatalf("missing item payload = %v, want specific Chef-style message", missingPayload)
	}
}

func TestDataBagDeleteCascadesItems(t *testing.T) {
	router := newTestRouter(t)

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}

	var createBagPayload map[string]any
	if err := json.Unmarshal(createBagRec.Body.Bytes(), &createBagPayload); err != nil {
		t.Fatalf("json.Unmarshal(create bag) error = %v", err)
	}
	if createBagPayload["uri"] != "/organizations/ponyville/data/ponies" {
		t.Fatalf("create bag uri = %v, want %q", createBagPayload["uri"], "/organizations/ponyville/data/ponies")
	}

	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":  "twilight",
		"foo": "bar",
	}))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	deleteBagReq := newSignedJSONRequest(t, http.MethodDelete, "/organizations/ponyville/data/ponies", nil)
	deleteBagRec := httptest.NewRecorder()
	router.ServeHTTP(deleteBagRec, deleteBagReq)

	if deleteBagRec.Code != http.StatusOK {
		t.Fatalf("delete bag status = %d, want %d, body = %s", deleteBagRec.Code, http.StatusOK, deleteBagRec.Body.String())
	}

	var deleteBagPayload map[string]any
	if err := json.Unmarshal(deleteBagRec.Body.Bytes(), &deleteBagPayload); err != nil {
		t.Fatalf("json.Unmarshal(delete bag) error = %v", err)
	}
	if deleteBagPayload["name"] != "ponies" {
		t.Fatalf("delete bag name = %v, want %q", deleteBagPayload["name"], "ponies")
	}

	getBagReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/data/ponies", nil)
	getBagRec := httptest.NewRecorder()
	router.ServeHTTP(getBagRec, getBagReq)

	if getBagRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted bag status = %d, want %d, body = %s", getBagRec.Code, http.StatusNotFound, getBagRec.Body.String())
	}

	getItemReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/data/ponies/twilight", nil)
	getItemRec := httptest.NewRecorder()
	router.ServeHTTP(getItemRec, getItemReq)

	if getItemRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted bag item status = %d, want %d, body = %s", getItemRec.Code, http.StatusNotFound, getItemRec.Body.String())
	}
}

func TestDataBagRoutesReturnAllowHeadersAndConflictMessages(t *testing.T) {
	router := newTestRouter(t)

	rootReq := newSignedJSONRequest(t, http.MethodPut, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	rootRec := httptest.NewRecorder()
	router.ServeHTTP(rootRec, rootReq)
	if rootRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("root method status = %d, want %d, body = %s", rootRec.Code, http.StatusMethodNotAllowed, rootRec.Body.String())
	}
	if rootRec.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("root Allow = %q, want %q", rootRec.Header().Get("Allow"), "GET, POST")
	}

	createBagReq := newSignedJSONRequest(t, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create bag status = %d, want %d, body = %s", createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}

	bagReq := newSignedJSONRequest(t, http.MethodPut, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}))
	bagRec := httptest.NewRecorder()
	router.ServeHTTP(bagRec, bagReq)
	if bagRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("bag method status = %d, want %d, body = %s", bagRec.Code, http.StatusMethodNotAllowed, bagRec.Body.String())
	}
	if bagRec.Header().Get("Allow") != "GET, POST, DELETE" {
		t.Fatalf("bag Allow = %q, want %q", bagRec.Header().Get("Allow"), "GET, POST, DELETE")
	}

	itemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies/twilight", mustMarshalDataBagJSON(t, map[string]any{"id": "twilight"}))
	itemRec := httptest.NewRecorder()
	router.ServeHTTP(itemRec, itemReq)
	if itemRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("item method status = %d, want %d, body = %s", itemRec.Code, http.StatusMethodNotAllowed, itemRec.Body.String())
	}
	if itemRec.Header().Get("Allow") != "GET, PUT, DELETE" {
		t.Fatalf("item Allow = %q, want %q", itemRec.Header().Get("Allow"), "GET, PUT, DELETE")
	}

	createItemReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{"id": "twilight"}))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	conflictReq := newSignedJSONRequest(t, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{"id": "twilight"}))
	conflictRec := httptest.NewRecorder()
	router.ServeHTTP(conflictRec, conflictReq)
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, want %d, body = %s", conflictRec.Code, http.StatusConflict, conflictRec.Body.String())
	}

	var conflictPayload map[string][]string
	if err := json.Unmarshal(conflictRec.Body.Bytes(), &conflictPayload); err != nil {
		t.Fatalf("json.Unmarshal(conflict) error = %v", err)
	}
	if len(conflictPayload["error"]) != 1 || conflictPayload["error"][0] != "Data Bag Item 'twilight' already exists in Data Bag 'ponies'." {
		t.Fatalf("conflict payload = %v, want item-specific message", conflictPayload)
	}
}

func newSignedJSONRequest(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	applySignedHeaders(t, req, "silent-bob", "", method, path, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	return req
}

func mustMarshalDataBagJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return body
}
