package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/testfixtures"
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

// TestEncryptedDataBagItemDefaultOrgLifecyclePreservesOpaquePayload pins the
// default-org CRUD contract for client-side encrypted-looking JSON envelopes.
func TestEncryptedDataBagItemDefaultOrgLifecyclePreservesOpaquePayload(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	createDataBagForTest(t, router, "/data", bagName)

	createFixture := testfixtures.EncryptedDataBagItem()
	createItemReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, createFixture))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}
	createItemPayload := decodeDataBagPayload(t, createItemRec)
	assertDataBagItemWrapper(t, createItemPayload, bagName, itemID)
	assertDataBagItemContainsPayload(t, createItemPayload, createFixture)

	getItemReq := newSignedJSONRequest(t, http.MethodGet, itemPath, nil)
	getItemRec := httptest.NewRecorder()
	router.ServeHTTP(getItemRec, getItemReq)
	if getItemRec.Code != http.StatusOK {
		t.Fatalf("get encrypted item status = %d, want %d, body = %s", getItemRec.Code, http.StatusOK, getItemRec.Body.String())
	}
	getItemPayload := decodeDataBagPayload(t, getItemRec)
	assertRawDataBagItemPayload(t, getItemPayload, createFixture)

	updateFixture := testfixtures.UpdatedEncryptedDataBagItem()
	updateItemReq := newSignedJSONRequest(t, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, updateFixture))
	updateItemRec := httptest.NewRecorder()
	router.ServeHTTP(updateItemRec, updateItemReq)
	if updateItemRec.Code != http.StatusOK {
		t.Fatalf("update encrypted item status = %d, want %d, body = %s", updateItemRec.Code, http.StatusOK, updateItemRec.Body.String())
	}
	wantUpdated := testfixtures.CloneDataBagPayload(updateFixture)
	wantUpdated["id"] = itemID
	updateItemPayload := decodeDataBagPayload(t, updateItemRec)
	assertDataBagItemWrapper(t, updateItemPayload, bagName, itemID)
	assertDataBagItemContainsPayload(t, updateItemPayload, wantUpdated)

	getUpdatedReq := newSignedJSONRequest(t, http.MethodGet, itemPath, nil)
	getUpdatedRec := httptest.NewRecorder()
	router.ServeHTTP(getUpdatedRec, getUpdatedReq)
	if getUpdatedRec.Code != http.StatusOK {
		t.Fatalf("get updated encrypted item status = %d, want %d, body = %s", getUpdatedRec.Code, http.StatusOK, getUpdatedRec.Body.String())
	}
	getUpdatedPayload := decodeDataBagPayload(t, getUpdatedRec)
	assertRawDataBagItemPayload(t, getUpdatedPayload, wantUpdated)

	deleteItemReq := newSignedJSONRequest(t, http.MethodDelete, itemPath, nil)
	deleteItemRec := httptest.NewRecorder()
	router.ServeHTTP(deleteItemRec, deleteItemReq)
	if deleteItemRec.Code != http.StatusOK {
		t.Fatalf("delete encrypted item status = %d, want %d, body = %s", deleteItemRec.Code, http.StatusOK, deleteItemRec.Body.String())
	}
	deleteItemPayload := decodeDataBagPayload(t, deleteItemRec)
	assertDeletedDataBagItemPayload(t, deleteItemPayload, bagName, itemID, wantUpdated)

	missingReq := newSignedJSONRequest(t, http.MethodGet, itemPath, nil)
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted encrypted item status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

// TestEncryptedDataBagItemExplicitOrgLifecycleAndBagDeleteCascade pins the same
// encrypted-looking payload behavior on explicit org routes and bag deletion.
func TestEncryptedDataBagItemExplicitOrgLifecycleAndBagDeleteCascade(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	bagPath := "/organizations/ponyville/data/" + bagName

	createDataBagForTest(t, router, "/organizations/ponyville/data", bagName)

	nestedFixture := testfixtures.NestedEncryptedDataBagItem()
	nestedID, ok := nestedFixture["id"].(string)
	if !ok {
		t.Fatalf("nested encrypted fixture id = %T, want string", nestedFixture["id"])
	}
	nestedPath := bagPath + "/" + nestedID

	createNestedReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, nestedFixture))
	createNestedRec := httptest.NewRecorder()
	router.ServeHTTP(createNestedRec, createNestedReq)
	if createNestedRec.Code != http.StatusCreated {
		t.Fatalf("create explicit encrypted item status = %d, want %d, body = %s", createNestedRec.Code, http.StatusCreated, createNestedRec.Body.String())
	}
	createNestedPayload := decodeDataBagPayload(t, createNestedRec)
	assertDataBagItemWrapper(t, createNestedPayload, bagName, nestedID)
	assertDataBagItemContainsPayload(t, createNestedPayload, nestedFixture)

	getNestedReq := newSignedJSONRequest(t, http.MethodGet, nestedPath, nil)
	getNestedRec := httptest.NewRecorder()
	router.ServeHTTP(getNestedRec, getNestedReq)
	if getNestedRec.Code != http.StatusOK {
		t.Fatalf("get explicit encrypted item status = %d, want %d, body = %s", getNestedRec.Code, http.StatusOK, getNestedRec.Body.String())
	}
	getNestedPayload := decodeDataBagPayload(t, getNestedRec)
	assertRawDataBagItemPayload(t, getNestedPayload, nestedFixture)

	updateNested := testfixtures.CloneDataBagPayload(nestedFixture)
	updateNested["metadata"] = map[string]any{
		"enabled": false,
		"weight":  float64(7),
		"note":    "rotated without decrypting",
	}
	updateNestedReq := newSignedJSONRequest(t, http.MethodPut, nestedPath, mustMarshalDataBagJSON(t, updateNested))
	updateNestedRec := httptest.NewRecorder()
	router.ServeHTTP(updateNestedRec, updateNestedReq)
	if updateNestedRec.Code != http.StatusOK {
		t.Fatalf("update explicit encrypted item status = %d, want %d, body = %s", updateNestedRec.Code, http.StatusOK, updateNestedRec.Body.String())
	}
	updateNestedPayload := decodeDataBagPayload(t, updateNestedRec)
	assertDataBagItemWrapper(t, updateNestedPayload, bagName, nestedID)
	assertDataBagItemContainsPayload(t, updateNestedPayload, updateNested)

	deleteNestedReq := newSignedJSONRequest(t, http.MethodDelete, nestedPath, nil)
	deleteNestedRec := httptest.NewRecorder()
	router.ServeHTTP(deleteNestedRec, deleteNestedReq)
	if deleteNestedRec.Code != http.StatusOK {
		t.Fatalf("delete explicit encrypted item status = %d, want %d, body = %s", deleteNestedRec.Code, http.StatusOK, deleteNestedRec.Body.String())
	}
	deleteNestedPayload := decodeDataBagPayload(t, deleteNestedRec)
	assertDeletedDataBagItemPayload(t, deleteNestedPayload, bagName, nestedID, updateNested)

	cascadeFixture := testfixtures.EncryptedDataBagItem()
	cascadeID := testfixtures.EncryptedDataBagItemID()
	createCascadeReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, cascadeFixture))
	createCascadeRec := httptest.NewRecorder()
	router.ServeHTTP(createCascadeRec, createCascadeReq)
	if createCascadeRec.Code != http.StatusCreated {
		t.Fatalf("create cascade encrypted item status = %d, want %d, body = %s", createCascadeRec.Code, http.StatusCreated, createCascadeRec.Body.String())
	}

	deleteBagReq := newSignedJSONRequest(t, http.MethodDelete, bagPath, nil)
	deleteBagRec := httptest.NewRecorder()
	router.ServeHTTP(deleteBagRec, deleteBagReq)
	if deleteBagRec.Code != http.StatusOK {
		t.Fatalf("delete encrypted bag status = %d, want %d, body = %s", deleteBagRec.Code, http.StatusOK, deleteBagRec.Body.String())
	}

	getBagReq := newSignedJSONRequest(t, http.MethodGet, bagPath, nil)
	getBagRec := httptest.NewRecorder()
	router.ServeHTTP(getBagRec, getBagReq)
	if getBagRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted encrypted bag status = %d, want %d, body = %s", getBagRec.Code, http.StatusNotFound, getBagRec.Body.String())
	}

	getCascadeReq := newSignedJSONRequest(t, http.MethodGet, bagPath+"/"+cascadeID, nil)
	getCascadeRec := httptest.NewRecorder()
	router.ServeHTTP(getCascadeRec, getCascadeReq)
	if getCascadeRec.Code != http.StatusNotFound {
		t.Fatalf("get cascade-deleted encrypted item status = %d, want %d, body = %s", getCascadeRec.Code, http.StatusNotFound, getCascadeRec.Body.String())
	}
}

// TestEncryptedDataBagItemIDCompatibilityAndNoMutation pins id validation for
// encrypted-looking items without adding encrypted-field schema validation.
func TestEncryptedDataBagItemIDCompatibilityAndNoMutation(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	createDataBagForTest(t, router, "/data", bagName)

	missingID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	delete(missingID, "id")
	missingIDReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, missingID))
	missingIDRec := httptest.NewRecorder()
	router.ServeHTTP(missingIDRec, missingIDReq)
	assertDataBagError(t, missingIDRec, http.StatusBadRequest, "Field 'id' missing")

	missingAfterFailedCreateReq := newSignedJSONRequest(t, http.MethodGet, itemPath, nil)
	missingAfterFailedCreateRec := httptest.NewRecorder()
	router.ServeHTTP(missingAfterFailedCreateRec, missingAfterFailedCreateReq)
	if missingAfterFailedCreateRec.Code != http.StatusNotFound {
		t.Fatalf("get after failed encrypted create status = %d, want %d, body = %s", missingAfterFailedCreateRec.Code, http.StatusNotFound, missingAfterFailedCreateRec.Body.String())
	}

	createFixture := testfixtures.EncryptedDataBagItem()
	createItemReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, createFixture))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted item before mismatch status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	mismatchedID := testfixtures.CloneDataBagPayload(testfixtures.UpdatedEncryptedDataBagItem())
	mismatchedID["id"] = "wrong-item"
	mismatchedReq := newSignedJSONRequest(t, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, mismatchedID))
	mismatchedRec := httptest.NewRecorder()
	router.ServeHTTP(mismatchedRec, mismatchedReq)
	assertDataBagError(t, mismatchedRec, http.StatusBadRequest, "DataBagItem name mismatch.")

	getAfterMismatchReq := newSignedJSONRequest(t, http.MethodGet, itemPath, nil)
	getAfterMismatchRec := httptest.NewRecorder()
	router.ServeHTTP(getAfterMismatchRec, getAfterMismatchReq)
	if getAfterMismatchRec.Code != http.StatusOK {
		t.Fatalf("get after encrypted mismatch status = %d, want %d, body = %s", getAfterMismatchRec.Code, http.StatusOK, getAfterMismatchRec.Body.String())
	}
	getAfterMismatchPayload := decodeDataBagPayload(t, getAfterMismatchRec)
	assertRawDataBagItemPayload(t, getAfterMismatchPayload, createFixture)
}

// TestEncryptedDataBagItemValidationErrorsKeepChefShapes pins malformed write
// handling for encrypted-looking items while proving failed writes leave the
// existing raw item and search-visible state untouched.
func TestEncryptedDataBagItemValidationErrorsKeepChefShapes(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID
	createFixture := testfixtures.EncryptedDataBagItem()

	createDataBagForTest(t, router, "/data", bagName)
	createItemReq := newSignedJSONRequest(t, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, createFixture))
	createItemRec := httptest.NewRecorder()
	router.ServeHTTP(createItemRec, createItemReq)
	if createItemRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted item status = %d, want %d, body = %s", createItemRec.Code, http.StatusCreated, createItemRec.Body.String())
	}

	malformedRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, []byte(`{"id":"malformed"`))
	assertDataBagAPIError(t, malformedRec, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")

	trailingRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, []byte(`{"id":"trailing"} {"id":"extra"}`))
	assertDataBagAPIError(t, trailingRec, http.StatusBadRequest, "invalid_json", "request body must contain exactly one JSON document")

	emptyRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, []byte{})
	assertDataBagAPIError(t, emptyRec, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")

	missingID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	delete(missingID, "id")
	missingIDRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, missingID))
	assertDataBagError(t, missingIDRec, http.StatusBadRequest, "Field 'id' missing")

	invalidID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	invalidID["id"] = "invalid/id"
	invalidIDRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, invalidID))
	assertDataBagError(t, invalidIDRec, http.StatusBadRequest, "Field 'id' invalid")

	mismatchedID := testfixtures.CloneDataBagPayload(testfixtures.UpdatedEncryptedDataBagItem())
	mismatchedID["id"] = "wrong-item"
	mismatchRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, mismatchedID))
	assertDataBagError(t, mismatchRec, http.StatusBadRequest, "DataBagItem name mismatch.")

	missingBagRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, "/data/missing_encrypted_bag", mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	assertDataBagError(t, missingBagRec, http.StatusNotFound, "No data bag 'missing_encrypted_bag' could be found. Please create this data bag before adding items to it.")

	missingOrgRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, "/organizations/missing/data/"+bagName, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	assertDataBagAPIError(t, missingOrgRec, http.StatusNotFound, "not_found", "organization not found")

	methodRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, itemPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	assertDataBagAPIError(t, methodRec, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for data bag item route")
	if methodRec.Header().Get("Allow") != "GET, PUT, DELETE" {
		t.Fatalf("item method Allow = %q, want %q", methodRec.Header().Get("Allow"), "GET, PUT, DELETE")
	}

	assertEncryptedDataBagItemRaw(t, router, itemPath, createFixture)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "id:"+itemID), "/search/"+bagName, 1)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "id:malformed"), "/search/"+bagName, 0)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "environment:staging"), "/search/"+bagName, 0)
}

// TestEncryptedDataBagEnvelopeVariantsRemainOpaque proves OpenCook does not
// validate encrypted envelope internals that Chef clients own and decrypt.
func TestEncryptedDataBagEnvelopeVariantsRemainOpaque(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	bagPath := "/data/" + bagName
	createDataBagForTest(t, router, "/data", bagName)
	createBaseRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	if createBaseRec.Code != http.StatusCreated {
		t.Fatalf("create baseline encrypted item status = %d, want %d, body = %s", createBaseRec.Code, http.StatusCreated, createBaseRec.Body.String())
	}

	variants := []struct {
		name   string
		id     string
		mutate func(map[string]any)
	}{
		{
			name: "missing iv",
			id:   "missing_iv",
			mutate: func(envelope map[string]any) {
				delete(envelope, "iv")
			},
		},
		{
			name: "unknown version",
			id:   "unknown_version",
			mutate: func(envelope map[string]any) {
				envelope["version"] = float64(999)
			},
		},
		{
			name: "unknown cipher",
			id:   "unknown_cipher",
			mutate: func(envelope map[string]any) {
				envelope["cipher"] = "chef-client-future-cipher"
			},
		},
		{
			name: "extra encrypted envelope fields",
			id:   "extra_fields",
			mutate: func(envelope map[string]any) {
				envelope["aad"] = "opaque-authenticated-data"
				envelope["future_field"] = map[string]any{"kept": true}
			},
		},
		{
			name: "non string encrypted data",
			id:   "non_string_encrypted_data",
			mutate: func(envelope map[string]any) {
				envelope["encrypted_data"] = map[string]any{"opaque": true}
			},
		},
	}

	for _, tt := range variants {
		t.Run(tt.name, func(t *testing.T) {
			payload := encryptedEnvelopeVariantPayload(t, tt.id, tt.mutate)
			createRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, payload))
			if createRec.Code != http.StatusCreated {
				t.Fatalf("create encrypted variant status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
			}
			assertEncryptedDataBagItemRaw(t, router, bagPath+"/"+tt.id, payload)
		})
	}

	updatePayload := encryptedEnvelopeVariantPayload(t, testfixtures.EncryptedDataBagItemID(), func(envelope map[string]any) {
		envelope["cipher"] = "update-path-future-cipher"
		envelope["iv"] = []any{"non", "string", "iv"}
	})
	updateRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPut, bagPath+"/"+testfixtures.EncryptedDataBagItemID(), mustMarshalDataBagJSON(t, updatePayload))
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update encrypted variant status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	assertEncryptedDataBagItemRaw(t, router, bagPath+"/"+testfixtures.EncryptedDataBagItemID(), updatePayload)
}

// TestEncryptedDataBagItemAuthUsesParentDataBagACLs pins item authorization to
// the parent data bag ACL and proves denied writes do not mutate item or search
// state.
func TestEncryptedDataBagItemAuthUsesParentDataBagACLs(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	createDataBagForTest(t, router, "/data", bagName)
	createRec := performDataBagRequestAs(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()))
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create encrypted item status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	normalUpdate := testfixtures.CloneDataBagPayload(testfixtures.UpdatedEncryptedDataBagItem())
	normalUpdate["id"] = itemID
	normalRec := performDataBagRequestAs(t, router, "normal-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, normalUpdate))
	if normalRec.Code != http.StatusOK {
		t.Fatalf("normal-user encrypted update status = %d, want %d, body = %s", normalRec.Code, http.StatusOK, normalRec.Body.String())
	}

	blockedUpdate := encryptedEnvelopeVariantPayload(t, itemID, func(envelope map[string]any) {
		envelope["encrypted_data"] = "blocked-update-ciphertext"
	})
	blockedUpdate["environment"] = "blocked"
	outsideUpdateRec := performDataBagRequestAs(t, router, "outside-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, blockedUpdate))
	if outsideUpdateRec.Code != http.StatusForbidden {
		t.Fatalf("outside-user encrypted update status = %d, want %d, body = %s", outsideUpdateRec.Code, http.StatusForbidden, outsideUpdateRec.Body.String())
	}
	invalidUpdateRec := performDataBagRequestAs(t, router, "invalid-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, blockedUpdate))
	if invalidUpdateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user encrypted update status = %d, want %d, body = %s", invalidUpdateRec.Code, http.StatusUnauthorized, invalidUpdateRec.Body.String())
	}

	blockedCreate := encryptedEnvelopeVariantPayload(t, "outside_created", func(envelope map[string]any) {
		envelope["encrypted_data"] = "outside-create-ciphertext"
	})
	outsideCreateRec := performDataBagRequestAs(t, router, "outside-user", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, blockedCreate))
	if outsideCreateRec.Code != http.StatusForbidden {
		t.Fatalf("outside-user encrypted create status = %d, want %d, body = %s", outsideCreateRec.Code, http.StatusForbidden, outsideCreateRec.Body.String())
	}
	invalidCreateRec := performDataBagRequestAs(t, router, "invalid-user", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, blockedCreate))
	if invalidCreateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user encrypted create status = %d, want %d, body = %s", invalidCreateRec.Code, http.StatusUnauthorized, invalidCreateRec.Body.String())
	}

	outsideDeleteRec := performDataBagRequestAs(t, router, "outside-user", http.MethodDelete, itemPath, nil)
	if outsideDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("outside-user encrypted delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
	}
	invalidDeleteRec := performDataBagRequestAs(t, router, "invalid-user", http.MethodDelete, itemPath, nil)
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user encrypted delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	assertEncryptedDataBagItemRaw(t, router, itemPath, normalUpdate)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "environment:staging"), "/search/"+bagName, 1)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "environment:blocked"), "/search/"+bagName, 0)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "id:outside_created"), "/search/"+bagName, 0)
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

// performDataBagRequestAs sends a signed request as a specific test actor so
// validation and ACL tests can exercise the same Chef-auth path as real clients.
func performDataBagRequestAs(t *testing.T, router http.Handler, userID, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	req := newSignedJSONRequestAs(t, userID, method, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// createDataBagForTest creates a data bag through the same signed route surface
// the caller is exercising so default-org and explicit-org aliases stay honest.
func createDataBagForTest(t *testing.T, router http.Handler, collectionPath, bagName string) {
	t.Helper()

	createBagReq := newSignedJSONRequest(t, http.MethodPost, collectionPath, mustMarshalDataBagJSON(t, map[string]any{"name": bagName}))
	createBagRec := httptest.NewRecorder()
	router.ServeHTTP(createBagRec, createBagReq)
	if createBagRec.Code != http.StatusCreated {
		t.Fatalf("create data bag %q status = %d, want %d, body = %s", bagName, createBagRec.Code, http.StatusCreated, createBagRec.Body.String())
	}
}

// assertEncryptedDataBagItemRaw fetches an item through the route layer and
// compares the raw stored JSON without allowing create/update wrapper fields.
func assertEncryptedDataBagItemRaw(t *testing.T, router http.Handler, path string, want map[string]any) {
	t.Helper()

	rec := performDataBagRequestAs(t, router, "silent-bob", http.MethodGet, path, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("get encrypted data bag item status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	assertRawDataBagItemPayload(t, decodeDataBagPayload(t, rec), want)
}

// decodeDataBagPayload decodes a route response into a JSON object and keeps
// response assertion code focused on compatibility fields instead of plumbing.
func decodeDataBagPayload(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(data bag payload) error = %v", err)
	}
	return payload
}

// assertDataBagSearchTotal checks only result count, which is enough for
// no-mutation tests that need to prove failed writes did not create stale rows.
func assertDataBagSearchTotal(t *testing.T, router http.Handler, rawPath, signPath string, want float64) {
	t.Helper()

	req := newSignedSearchRequest(t, http.MethodGet, rawPath, signPath, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("search %s status = %d, want %d, body = %s", rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(search %s) error = %v", rawPath, err)
	}
	if payload["total"] != want {
		t.Fatalf("search %s total = %v, want %v", rawPath, payload["total"], want)
	}
}

// assertDataBagItemWrapper checks the Chef-style fields OpenCook adds to
// create/update responses while leaving the caller to assert opaque item fields.
func assertDataBagItemWrapper(t *testing.T, payload map[string]any, bagName, itemID string) {
	t.Helper()

	if payload["id"] != itemID {
		t.Fatalf("data bag item id = %v, want %q", payload["id"], itemID)
	}
	if payload["chef_type"] != "data_bag_item" {
		t.Fatalf("data bag item chef_type = %v, want %q", payload["chef_type"], "data_bag_item")
	}
	if payload["data_bag"] != bagName {
		t.Fatalf("data bag item data_bag = %v, want %q", payload["data_bag"], bagName)
	}
}

// assertDataBagItemContainsPayload compares wrapped create/update responses as
// exactly the fixture payload plus the Chef metadata fields added by the route.
func assertDataBagItemContainsPayload(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()

	if len(got) != len(want)+2 {
		t.Fatalf("data bag item field count = %d, want %d raw fields plus chef_type/data_bag: %#v", len(got), len(want), got)
	}
	for key := range got {
		if key == "chef_type" || key == "data_bag" {
			continue
		}
		if _, ok := want[key]; !ok {
			t.Fatalf("data bag item included unexpected field %q: %#v", key, got)
		}
	}
	for key, wantValue := range want {
		if !reflect.DeepEqual(got[key], wantValue) {
			t.Fatalf("data bag item field %q = %#v, want %#v", key, got[key], wantValue)
		}
	}
}

// assertRawDataBagItemPayload pins direct item reads to the stored opaque JSON
// object and rejects accidental create/update response wrapper leakage.
func assertRawDataBagItemPayload(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()

	if _, ok := got["chef_type"]; ok {
		t.Fatalf("raw data bag item unexpectedly included chef_type: %v", got)
	}
	if _, ok := got["data_bag"]; ok {
		t.Fatalf("raw data bag item unexpectedly included data_bag: %v", got)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("raw data bag item = %#v, want %#v", got, want)
	}
}

// assertDeletedDataBagItemPayload checks delete responses preserve the old raw
// data under raw_data while returning the Chef-style deletion envelope.
func assertDeletedDataBagItemPayload(t *testing.T, payload map[string]any, bagName, itemID string, wantRaw map[string]any) {
	t.Helper()

	if payload["name"] != "data_bag_item_"+bagName+"_"+itemID {
		t.Fatalf("deleted data bag item name = %v, want %q", payload["name"], "data_bag_item_"+bagName+"_"+itemID)
	}
	if payload["json_class"] != "Chef::DataBagItem" {
		t.Fatalf("deleted data bag item json_class = %v, want %q", payload["json_class"], "Chef::DataBagItem")
	}
	if payload["chef_type"] != "data_bag_item" {
		t.Fatalf("deleted data bag item chef_type = %v, want %q", payload["chef_type"], "data_bag_item")
	}
	if payload["data_bag"] != bagName {
		t.Fatalf("deleted data bag item data_bag = %v, want %q", payload["data_bag"], bagName)
	}
	rawData, ok := payload["raw_data"].(map[string]any)
	if !ok {
		t.Fatalf("deleted data bag item raw_data = %T, want map[string]any", payload["raw_data"])
	}
	assertRawDataBagItemPayload(t, rawData, wantRaw)
}

// assertDataBagAPIError verifies the structured apiError shape used by generic
// request parsing, routing, and method errors around data bag endpoints.
func assertDataBagAPIError(t *testing.T, rec *httptest.ResponseRecorder, status int, wantError, wantMessage string) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("data bag API error status = %d, want %d, body = %s", rec.Code, status, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(data bag API error) error = %v", err)
	}
	if payload["error"] != wantError || payload["message"] != wantMessage {
		t.Fatalf("data bag API error payload = %v, want error=%q message=%q", payload, wantError, wantMessage)
	}
}

// encryptedEnvelopeVariantPayload returns an encrypted-looking item whose
// password envelope has been deliberately mutated to prove server opacity.
func encryptedEnvelopeVariantPayload(t *testing.T, id string, mutate func(map[string]any)) map[string]any {
	t.Helper()

	payload := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	payload["id"] = id
	password, ok := payload["password"].(map[string]any)
	if !ok {
		t.Fatalf("encrypted fixture password = %T, want map[string]any", payload["password"])
	}
	mutate(password)
	return payload
}

// assertDataBagError keeps validation tests precise about Chef-style error
// arrays while avoiding repeated JSON decoding boilerplate.
func assertDataBagError(t *testing.T, rec *httptest.ResponseRecorder, status int, wantMessage string) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("data bag error status = %d, want %d, body = %s", rec.Code, status, rec.Body.String())
	}
	var payload map[string][]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(data bag error) error = %v", err)
	}
	if len(payload["error"]) != 1 || payload["error"][0] != wantMessage {
		t.Fatalf("data bag error payload = %v, want %q", payload, wantMessage)
	}
}

// mustMarshalDataBagJSON serializes test request payloads and fails the caller
// immediately if a fixture stops being valid JSON-compatible data.
func mustMarshalDataBagJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return body
}
