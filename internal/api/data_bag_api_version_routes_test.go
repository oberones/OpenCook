package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestAPIVersionDataBagCRUDPayloadSemantics(t *testing.T) {
	router := newTestRouter(t)

	for _, serverAPIVersion := range []string{"0", "1", "2"} {
		t.Run("v"+serverAPIVersion, func(t *testing.T) {
			bagName := "versioned_bag_" + serverAPIVersion
			itemID := "item_" + serverAPIVersion

			createBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), serverAPIVersion)
			if createBag.Code != http.StatusCreated {
				t.Fatalf("create data bag status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
			}
			createBagPayload := mustDecodeObject(t, createBag)
			if len(createBagPayload) != 1 || createBagPayload["uri"] != "/data/"+bagName {
				t.Fatalf("create data bag payload = %v, want only default-org URI", createBagPayload)
			}

			listBags := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, "/organizations/ponyville/data", nil, serverAPIVersion)
			if listBags.Code != http.StatusOK {
				t.Fatalf("explicit-org data bag list status = %d, want %d, body = %s", listBags.Code, http.StatusOK, listBags.Body.String())
			}
			if got := mustDecodeStringMap(t, listBags)[bagName]; got != "/organizations/ponyville/data/"+bagName {
				t.Fatalf("explicit-org data bag URI = %q, want explicit-org URI", got)
			}

			emptyBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, "/data/"+bagName, nil, serverAPIVersion)
			assertEmptyObjectResponse(t, emptyBag)

			createRaw := map[string]any{
				"id":       itemID,
				"category": "friendship-" + serverAPIVersion,
				"metadata": map[string]any{
					"enabled": true,
					"note":    "created-" + serverAPIVersion,
				},
				"nested": map[string]any{
					"owner": "platform",
				},
			}
			createItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/organizations/ponyville/data/"+bagName, mustMarshalDataBagJSON(t, createRaw), serverAPIVersion)
			if createItem.Code != http.StatusCreated {
				t.Fatalf("create data bag item status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
			}
			createItemPayload := decodeDataBagPayload(t, createItem)
			assertDataBagItemWrapper(t, createItemPayload, bagName, itemID)
			assertDataBagItemContainsPayload(t, createItemPayload, createRaw)

			items := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, "/organizations/ponyville/data/"+bagName, nil, serverAPIVersion)
			if items.Code != http.StatusOK {
				t.Fatalf("explicit-org data bag item list status = %d, want %d, body = %s", items.Code, http.StatusOK, items.Body.String())
			}
			if got := mustDecodeStringMap(t, items)[itemID]; got != "/organizations/ponyville/data/"+bagName+"/"+itemID {
				t.Fatalf("explicit-org data bag item URI = %q, want explicit-org item URI", got)
			}

			rawGet := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, "/data/"+bagName+"/"+itemID, nil, serverAPIVersion)
			if rawGet.Code != http.StatusOK {
				t.Fatalf("get raw data bag item status = %d, want %d, body = %s", rawGet.Code, http.StatusOK, rawGet.Body.String())
			}
			assertRawDataBagItemPayload(t, decodeDataBagPayload(t, rawGet), createRaw)

			updateRaw := map[string]any{
				"category": "updated-" + serverAPIVersion,
				"metadata": map[string]any{
					"enabled": false,
					"note":    "updated-" + serverAPIVersion,
				},
			}
			wantUpdated := testfixtures.CloneDataBagPayload(updateRaw)
			wantUpdated["id"] = itemID
			updateItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPut, "/data/"+bagName+"/"+itemID, mustMarshalDataBagJSON(t, updateRaw), serverAPIVersion)
			if updateItem.Code != http.StatusOK {
				t.Fatalf("update data bag item status = %d, want %d, body = %s", updateItem.Code, http.StatusOK, updateItem.Body.String())
			}
			updatePayload := decodeDataBagPayload(t, updateItem)
			assertDataBagItemWrapper(t, updatePayload, bagName, itemID)
			assertDataBagItemContainsPayload(t, updatePayload, wantUpdated)

			assertSearchPartialData(t, router, http.MethodPost, searchPath("/organizations/ponyville/search/"+bagName, "category:updated-"+serverAPIVersion), "/organizations/ponyville/search/"+bagName, []byte(`{"category":["category"],"note":["metadata","note"]}`), "/organizations/ponyville/data/"+bagName+"/"+itemID, map[string]any{
				"category": "updated-" + serverAPIVersion,
				"note":     "updated-" + serverAPIVersion,
			})

			deleteItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodDelete, "/organizations/ponyville/data/"+bagName+"/"+itemID, nil, serverAPIVersion)
			if deleteItem.Code != http.StatusOK {
				t.Fatalf("delete data bag item status = %d, want %d, body = %s", deleteItem.Code, http.StatusOK, deleteItem.Body.String())
			}
			assertDeletedDataBagItemPayload(t, decodeDataBagPayload(t, deleteItem), bagName, itemID, wantUpdated)
			assertObjectMissingWithVersion(t, router, "/data/"+bagName+"/"+itemID, serverAPIVersion)

			deleteBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodDelete, "/data/"+bagName, nil, serverAPIVersion)
			if deleteBag.Code != http.StatusOK {
				t.Fatalf("delete data bag status = %d, want %d, body = %s", deleteBag.Code, http.StatusOK, deleteBag.Body.String())
			}
			assertDeletedDataBagPayload(t, mustDecodeObject(t, deleteBag), bagName)
			assertObjectMissingWithVersion(t, router, "/organizations/ponyville/data/"+bagName, serverAPIVersion)
		})
	}
}

// TestAPIVersionDataBagValidationFailuresKeepExistingState freezes malformed
// data bag and item writes while proving rejected bodies do not leak into reads
// or search projections.
func TestAPIVersionDataBagValidationFailuresKeepExistingState(t *testing.T) {
	router := newTestRouter(t)
	bagName := "validation_bag"
	itemID := "visible"
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID
	current := map[string]any{
		"id":       itemID,
		"category": "visible",
		"nested": map[string]any{
			"owner":   "platform",
			"enabled": true,
		},
	}

	createBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("create validation data bag status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	createItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, current), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("create validation data bag item status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}

	duplicateBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	assertDataBagError(t, duplicateBag, http.StatusConflict, "Data bag already exists")

	missingName := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{}), "2")
	assertDataBagError(t, missingName, http.StatusBadRequest, "Field 'name' missing")

	invalidName := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "bad/name"}), "2")
	assertDataBagError(t, invalidName, http.StatusBadRequest, "Field 'name' invalid")

	badJSONBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", []byte(`{"name":"broken"`), "2")
	assertDataBagAPIError(t, badJSONBag, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")

	trailingJSONBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", []byte(`{"name":"trailing_bag"} {"name":"extra"}`), "2")
	assertDataBagAPIError(t, trailingJSONBag, http.StatusBadRequest, "invalid_json", "request body must contain exactly one JSON document")

	listBags := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, "/data", nil, "2")
	if listBags.Code != http.StatusOK {
		t.Fatalf("list data bags after failed creates status = %d, want %d, body = %s", listBags.Code, http.StatusOK, listBags.Body.String())
	}
	if got := mustDecodeStringMap(t, listBags); len(got) != 1 || got[bagName] != bagPath {
		t.Fatalf("data bag list after failed creates = %v, want only %s => %s", got, bagName, bagPath)
	}
	assertObjectMissingWithVersion(t, router, "/data/trailing_bag", "2")

	missingBagItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data/missing_bag", mustMarshalDataBagJSON(t, map[string]any{"id": "ghost", "category": "missing-bag"}), "2")
	assertDataBagError(t, missingBagItem, http.StatusNotFound, "No data bag 'missing_bag' could be found. Please create this data bag before adding items to it.")

	duplicateItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, current), "2")
	assertDataBagError(t, duplicateItem, http.StatusConflict, "Data Bag Item 'visible' already exists in Data Bag 'validation_bag'.")

	missingID := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"category": "missing-id"}), "2")
	assertDataBagError(t, missingID, http.StatusBadRequest, "Field 'id' missing")

	invalidID := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"id": "bad/item", "category": "invalid"}), "2")
	assertDataBagError(t, invalidID, http.StatusBadRequest, "Field 'id' invalid")

	badJSONItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, []byte(`{"id":"broken"`), "2")
	assertDataBagAPIError(t, badJSONItem, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")

	trailingJSONItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPut, itemPath, []byte(`{"category":"trailing"} {"id":"extra"}`), "2")
	assertDataBagAPIError(t, trailingJSONItem, http.StatusBadRequest, "invalid_json", "request body must contain exactly one JSON document")

	mismatchedUpdate := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, map[string]any{"id": "other", "category": "mismatch"}), "2")
	assertDataBagError(t, mismatchedUpdate, http.StatusBadRequest, "DataBagItem name mismatch.")

	invalidUpdateID := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, map[string]any{"id": []any{"visible"}, "category": "invalid-update"}), "2")
	assertDataBagError(t, invalidUpdateID, http.StatusBadRequest, "Field 'id' invalid")

	assertRawDataBagItemWithVersion(t, router, itemPath, "2", current)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, 1)
	for _, query := range []string{"category:missing-bag", "category:mismatch", "category:invalid", "category:invalid-update", "id:ghost", "id:other"} {
		assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, query), "/search/"+bagName, 0)
	}
}

func TestAPIVersionEncryptedDataBagItemOpacityAndACLFiltering(t *testing.T) {
	router := newTestRouter(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	createBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("create encrypted data bag status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	createItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("create encrypted data bag item status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}
	createPayload := decodeDataBagPayload(t, createItem)
	assertDataBagItemWrapper(t, createPayload, bagName, itemID)
	assertDataBagItemContainsPayload(t, createPayload, testfixtures.EncryptedDataBagItem())

	rawGet := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodGet, itemPath, nil, "2")
	if rawGet.Code != http.StatusOK {
		t.Fatalf("get encrypted item status = %d, want %d, body = %s", rawGet.Code, http.StatusOK, rawGet.Body.String())
	}
	assertRawDataBagItemPayload(t, decodeDataBagPayload(t, rawGet), testfixtures.EncryptedDataBagItem())

	partialRec := serveSignedSearchRequestAs(t, router, "silent-bob", http.MethodPost, searchPath("/organizations/ponyville/search/"+bagName, "*_encrypted_data:*"), "/organizations/ponyville/search/"+bagName, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, partialRec), "/organizations/ponyville/data/"+bagName+"/"+itemID)

	outsideSearch := newSignedSearchRequestAs(t, "outside-user", http.MethodGet, searchPath("/search/"+bagName, "*_encrypted_data:*"), "/search/"+bagName, nil)
	outsideSearchRec := httptest.NewRecorder()
	router.ServeHTTP(outsideSearchRec, outsideSearch)
	if outsideSearchRec.Code != http.StatusForbidden {
		t.Fatalf("outside encrypted data bag search status = %d, want %d, body = %s", outsideSearchRec.Code, http.StatusForbidden, outsideSearchRec.Body.String())
	}

	outsideGet := serveSignedAPIVersionRequest(t, router, "outside-user", http.MethodGet, itemPath, nil, "2")
	if outsideGet.Code != http.StatusForbidden {
		t.Fatalf("outside encrypted item read status = %d, want %d, body = %s", outsideGet.Code, http.StatusForbidden, outsideGet.Body.String())
	}

	filteredRouter, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		return &denySearchDocumentAfterIndexGateAuthorizer{
			base:   authz.NewACLAuthorizer(state),
			target: "data_bag:" + bagName,
		}
	})
	filteredBag := serveSignedAPIVersionRequest(t, filteredRouter, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if filteredBag.Code != http.StatusCreated {
		t.Fatalf("filtered data bag create status = %d, want %d, body = %s", filteredBag.Code, http.StatusCreated, filteredBag.Body.String())
	}
	filteredItem := serveSignedAPIVersionRequest(t, filteredRouter, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), "2")
	if filteredItem.Code != http.StatusCreated {
		t.Fatalf("filtered encrypted item create status = %d, want %d, body = %s", filteredItem.Code, http.StatusCreated, filteredItem.Body.String())
	}
	assertSearchTotal(t, filteredRouter, searchPath("/search/"+bagName, "*_encrypted_data:*"), "/search/"+bagName, 0)
}

func TestAPIVersionDataBagRejectedWritesDoNotMutateMemorySearch(t *testing.T) {
	router := newTestRouter(t)
	bagName := "versioned_reject_bag"
	itemID := "visible"
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	createBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("create rejection data bag status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	current := map[string]any{"id": itemID, "category": "visible"}
	createItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, current), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("create rejection data bag item status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, 1)

	blockedBag := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "blocked_bag"}), "3")
	assertInvalidServerAPIVersionResponse(t, blockedBag, "3")
	assertObjectMissingWithVersion(t, router, "/data/blocked_bag", "2")

	blockedItem := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"id": "blocked", "category": "blocked"}), "3")
	assertInvalidServerAPIVersionResponse(t, blockedItem, "3")
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:blocked"), "/search/"+bagName, 0)

	badUpdate := map[string]any{"id": itemID, "category": "bad-version"}
	blockedUpdate := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, badUpdate), "3")
	assertInvalidServerAPIVersionResponse(t, blockedUpdate, "3")
	assertRawDataBagItemWithVersion(t, router, itemPath, "2", current)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:bad-version"), "/search/"+bagName, 0)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, 1)

	blockedDelete := serveSignedAPIVersionRequest(t, router, "silent-bob", http.MethodDelete, itemPath, nil, "3")
	assertInvalidServerAPIVersionResponse(t, blockedDelete, "3")
	assertRawDataBagItemWithVersion(t, router, itemPath, "2", current)

	outsideUpdate := map[string]any{"id": itemID, "category": "outside"}
	outsideUpdateRec := serveSignedAPIVersionRequest(t, router, "outside-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, outsideUpdate), "2")
	if outsideUpdateRec.Code != http.StatusForbidden {
		t.Fatalf("outside data bag item update status = %d, want %d, body = %s", outsideUpdateRec.Code, http.StatusForbidden, outsideUpdateRec.Body.String())
	}
	outsideCreateRec := serveSignedAPIVersionRequest(t, router, "outside-user", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"id": "outside", "category": "outside"}), "2")
	if outsideCreateRec.Code != http.StatusForbidden {
		t.Fatalf("outside data bag item create status = %d, want %d, body = %s", outsideCreateRec.Code, http.StatusForbidden, outsideCreateRec.Body.String())
	}
	outsideDeleteRec := serveSignedAPIVersionRequest(t, router, "outside-user", http.MethodDelete, itemPath, nil, "2")
	if outsideDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("outside data bag item delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
	}
	assertRawDataBagItemWithVersion(t, router, itemPath, "2", current)
	assertDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "category:outside"), "/search/"+bagName, 0)
}

func TestActivePostgresDataBagAPIVersionPayloadsRehydrateAndMutate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("ponyville")
	bagName := "persisted_bag"
	itemID := "alice"
	bagPath := "/organizations/ponyville/data/" + bagName
	itemPath := "/data/" + bagName + "/" + itemID
	orgItemPath := bagPath + "/" + itemID

	createBag := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("active Postgres data bag create status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	createRaw := map[string]any{
		"id":       itemID,
		"category": "persisted",
		"ssh": map[string]any{
			"public_key":  "---RSA Public Key--- Alice",
			"private_key": "---RSA Private Key--- Alice",
		},
	}
	createItem := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, createRaw), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("active Postgres data bag item create status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}

	restarted := fixture.restart()
	bags := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/data", nil, "2")
	if bags.Code != http.StatusOK {
		t.Fatalf("rehydrated data bag list status = %d, want %d, body = %s", bags.Code, http.StatusOK, bags.Body.String())
	}
	if got := mustDecodeStringMap(t, bags)[bagName]; got != "/data/"+bagName {
		t.Fatalf("rehydrated data bag URI = %q, want default-org URI", got)
	}
	items := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, bagPath, nil, "2")
	if items.Code != http.StatusOK {
		t.Fatalf("rehydrated item list status = %d, want %d, body = %s", items.Code, http.StatusOK, items.Body.String())
	}
	if got := mustDecodeStringMap(t, items)[itemID]; got != orgItemPath {
		t.Fatalf("rehydrated explicit item URI = %q, want %q", got, orgItemPath)
	}
	assertRawDataBagItemWithVersion(t, restarted.router, itemPath, "2", createRaw)

	partialBody := []byte(`{"private_key":["ssh","private_key"],"public_key":["ssh","public_key"]}`)
	partialPayload := mustServeActivePostgresSearchRequest(t, restarted.router, http.MethodPost, "/organizations/ponyville/search/"+bagName+"?q=ssh_public_key:*", partialBody, http.StatusOK)
	assertOnePartialSearchRow(t, partialPayload, orgItemPath, map[string]any{
		"private_key": "---RSA Private Key--- Alice",
		"public_key":  "---RSA Public Key--- Alice",
	})

	updateRaw := map[string]any{"category": "rehydrated-update"}
	wantUpdated := testfixtures.CloneDataBagPayload(updateRaw)
	wantUpdated["id"] = itemID
	updateItem := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, updateRaw), "2")
	if updateItem.Code != http.StatusOK {
		t.Fatalf("active Postgres data bag item update status = %d, want %d, body = %s", updateItem.Code, http.StatusOK, updateItem.Body.String())
	}
	assertDataBagItemContainsPayload(t, decodeDataBagPayload(t, updateItem), wantUpdated)

	updated := restarted.restart()
	assertRawDataBagItemWithVersion(t, updated.router, orgItemPath, "2", wantUpdated)
	deleteItem := serveSignedAPIVersionRequest(t, updated.router, "pivotal", http.MethodDelete, orgItemPath, nil, "2")
	if deleteItem.Code != http.StatusOK {
		t.Fatalf("active Postgres data bag item delete status = %d, want %d, body = %s", deleteItem.Code, http.StatusOK, deleteItem.Body.String())
	}
	assertDeletedDataBagItemPayload(t, decodeDataBagPayload(t, deleteItem), bagName, itemID, wantUpdated)

	deleted := updated.restart()
	assertObjectMissingWithVersion(t, deleted.router, itemPath, "2")
	deleteBag := serveSignedAPIVersionRequest(t, deleted.router, "pivotal", http.MethodDelete, bagPath, nil, "2")
	if deleteBag.Code != http.StatusOK {
		t.Fatalf("active Postgres data bag delete status = %d, want %d, body = %s", deleteBag.Code, http.StatusOK, deleteBag.Body.String())
	}
	empty := deleted.restart()
	assertObjectMissingWithVersion(t, empty.router, "/data/"+bagName, "2")
}

func TestActivePostgresOpenSearchDataBagAPIVersionFieldsAndRejectedWritesNoMutation(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "outside-user",
		DisplayName: "Outside User",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(outside-user) error = %v", err)
	}

	bagName := "search_bag"
	itemID := "visible"
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID
	createBag := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed data bag create status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	current := map[string]any{
		"id":       itemID,
		"category": "visible",
		"details":  map[string]any{"kind": "api-version"},
	}
	createItem := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, current), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed data bag item create status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}

	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "category:visible AND kind:api-version"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchPartialData(t, fixture.router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, []byte(`{"category":["category"],"kind":["details","kind"]}`), itemPath, map[string]any{
		"category": "visible",
		"kind":     "api-version",
	})

	snapshot := transport.SnapshotDocuments()
	blockedBag := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "blocked_bag"}), "3")
	assertInvalidServerAPIVersionResponse(t, blockedBag, "3")
	transport.RequireDocuments(t, snapshot)
	assertObjectMissingWithVersion(t, fixture.router, "/data/blocked_bag", "2")

	blockedItem := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"id": "blocked", "category": "blocked"}), "3")
	assertInvalidServerAPIVersionResponse(t, blockedItem, "3")
	transport.RequireDocuments(t, snapshot)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "category:blocked"), "/search/"+bagName, []string{})

	badUpdate := map[string]any{"id": itemID, "category": "bad-version"}
	updateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, badUpdate), "3")
	assertInvalidServerAPIVersionResponse(t, updateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertRawDataBagItemWithVersion(t, fixture.router, itemPath, "2", current)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "category:bad-version"), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})

	deleteBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodDelete, itemPath, nil, "3")
	assertInvalidServerAPIVersionResponse(t, deleteBlocked, "3")
	transport.RequireDocuments(t, snapshot)

	outsideCreate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, map[string]any{"id": "outside", "category": "outside"}), "2")
	if outsideCreate.Code != http.StatusForbidden {
		t.Fatalf("outside OpenSearch data bag item create status = %d, want %d, body = %s", outsideCreate.Code, http.StatusForbidden, outsideCreate.Body.String())
	}
	outsideUpdate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, map[string]any{"id": itemID, "category": "outside"}), "2")
	if outsideUpdate.Code != http.StatusForbidden {
		t.Fatalf("outside OpenSearch data bag item update status = %d, want %d, body = %s", outsideUpdate.Code, http.StatusForbidden, outsideUpdate.Body.String())
	}
	outsideDelete := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, itemPath, nil, "2")
	if outsideDelete.Code != http.StatusForbidden {
		t.Fatalf("outside OpenSearch data bag item delete status = %d, want %d, body = %s", outsideDelete.Code, http.StatusForbidden, outsideDelete.Body.String())
	}
	outsideDeleteBag := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, bagPath, nil, "2")
	if outsideDeleteBag.Code != http.StatusForbidden {
		t.Fatalf("outside OpenSearch data bag delete status = %d, want %d, body = %s", outsideDeleteBag.Code, http.StatusForbidden, outsideDeleteBag.Body.String())
	}
	transport.RequireDocuments(t, snapshot)
	assertRawDataBagItemWithVersion(t, fixture.router, itemPath, "2", current)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "category:outside"), "/search/"+bagName, []string{})

	restarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRawDataBagItemWithVersion(t, restarted.router, "/organizations/ponyville/data/"+bagName+"/"+itemID, "2", current)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/"+bagName, "category:visible AND kind:api-version"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/organizations/ponyville/search/"+bagName, "category:visible"), "/organizations/ponyville/search/"+bagName, []byte(`{"category":["category"],"kind":["details","kind"]}`), "/organizations/ponyville/data/"+bagName+"/"+itemID, map[string]any{
		"category": "visible",
		"kind":     "api-version",
	})

	updatedRaw := map[string]any{
		"category": "updated",
		"details":  map[string]any{"kind": "after-restart"},
	}
	wantUpdated := testfixtures.CloneDataBagPayload(updatedRaw)
	wantUpdated["id"] = itemID
	updateItem := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, updatedRaw), "2")
	if updateItem.Code != http.StatusOK {
		t.Fatalf("OpenSearch-backed data bag item update after restart status = %d, want %d, body = %s", updateItem.Code, http.StatusOK, updateItem.Body.String())
	}
	assertDataBagItemContainsPayload(t, decodeDataBagPayload(t, updateItem), wantUpdated)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/"+bagName, "category:visible"), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/"+bagName, "category:updated AND kind:after-restart"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})

	afterUpdateRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRawDataBagItemWithVersion(t, afterUpdateRestart.router, "/organizations/ponyville/data/"+bagName+"/"+itemID, "2", wantUpdated)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/"+bagName, "category:updated AND kind:after-restart"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})

	deleteItem := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/organizations/ponyville/data/"+bagName+"/"+itemID, nil, "2")
	if deleteItem.Code != http.StatusOK {
		t.Fatalf("OpenSearch-backed data bag item delete status = %d, want %d, body = %s", deleteItem.Code, http.StatusOK, deleteItem.Body.String())
	}
	assertDeletedDataBagItemPayload(t, decodeDataBagPayload(t, deleteItem), bagName, itemID, wantUpdated)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/"+bagName, "category:updated"), "/search/"+bagName, []string{})

	afterDeleteRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertObjectMissingWithVersion(t, afterDeleteRestart.router, itemPath, "2")
	assertActiveOpenSearchFullRows(t, afterDeleteRestart.router, searchPath("/search/"+bagName, "category:updated"), "/search/"+bagName, []string{})

	secretBag := testfixtures.EncryptedDataBagName()
	secretItemID := testfixtures.EncryptedDataBagItemID()
	secretBagPath := "/organizations/ponyville/data/" + secretBag
	secretItemPath := secretBagPath + "/" + secretItemID
	createSecretBag := serveSignedAPIVersionRequest(t, afterDeleteRestart.router, "pivotal", http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": secretBag}), "2")
	if createSecretBag.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed encrypted data bag create status = %d, want %d, body = %s", createSecretBag.Code, http.StatusCreated, createSecretBag.Body.String())
	}
	createSecretItem := serveSignedAPIVersionRequest(t, afterDeleteRestart.router, "pivotal", http.MethodPost, secretBagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), "2")
	if createSecretItem.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed encrypted data bag item create status = %d, want %d, body = %s", createSecretItem.Code, http.StatusCreated, createSecretItem.Body.String())
	}
	assertActiveOpenSearchFullRows(t, afterDeleteRestart.router, searchPath("/search/"+secretBag, "environment:production AND *_encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + secretItemID})
	assertActiveOpenSearchPartialData(t, afterDeleteRestart.router, searchPath("/organizations/ponyville/search/"+secretBag, "*_encrypted_data:*"), "/organizations/ponyville/search/"+secretBag, encryptedDataBagPartialSearchBody(t), secretItemPath, encryptedDataBagPartialExpectation(t, testfixtures.EncryptedDataBagItem()))

	secretRestarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRawDataBagItemWithVersion(t, secretRestarted.router, secretItemPath, "2", testfixtures.EncryptedDataBagItem())
	assertActiveOpenSearchFullRows(t, secretRestarted.router, searchPath("/search/"+secretBag, "environment:production AND *_encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + secretItemID})

	updatedSecret := testfixtures.UpdatedEncryptedDataBagItem()
	wantUpdatedSecret := testfixtures.CloneDataBagPayload(updatedSecret)
	wantUpdatedSecret["id"] = secretItemID
	updateSecret := serveSignedAPIVersionRequest(t, secretRestarted.router, "pivotal", http.MethodPut, secretItemPath, mustMarshalDataBagJSON(t, updatedSecret), "2")
	if updateSecret.Code != http.StatusOK {
		t.Fatalf("OpenSearch-backed encrypted data bag item update status = %d, want %d, body = %s", updateSecret.Code, http.StatusOK, updateSecret.Body.String())
	}
	assertDataBagItemContainsPayload(t, decodeDataBagPayload(t, updateSecret), wantUpdatedSecret)
	assertActiveOpenSearchFullRows(t, secretRestarted.router, searchPath("/search/"+secretBag, "environment:production"), "/search/"+secretBag, []string{})
	assertActiveOpenSearchFullRows(t, secretRestarted.router, searchPath("/search/"+secretBag, "environment:staging AND *_encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + secretItemID})
	assertActiveOpenSearchPartialData(t, secretRestarted.router, searchPath("/organizations/ponyville/search/"+secretBag, "*_encrypted_data:*"), "/organizations/ponyville/search/"+secretBag, encryptedDataBagPartialSearchBody(t), secretItemPath, encryptedDataBagPartialExpectation(t, wantUpdatedSecret))

	afterSecretUpdateRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRawDataBagItemWithVersion(t, afterSecretUpdateRestart.router, secretItemPath, "2", wantUpdatedSecret)
	assertActiveOpenSearchFullRows(t, afterSecretUpdateRestart.router, searchPath("/search/"+secretBag, "environment:staging AND *_encrypted_data:*"), "/search/"+secretBag, []string{"data_bag_item_" + secretBag + "_" + secretItemID})

	deleteSecret := serveSignedAPIVersionRequest(t, afterSecretUpdateRestart.router, "pivotal", http.MethodDelete, secretItemPath, nil, "2")
	if deleteSecret.Code != http.StatusOK {
		t.Fatalf("OpenSearch-backed encrypted data bag item delete status = %d, want %d, body = %s", deleteSecret.Code, http.StatusOK, deleteSecret.Body.String())
	}
	assertDeletedDataBagItemPayload(t, decodeDataBagPayload(t, deleteSecret), secretBag, secretItemID, wantUpdatedSecret)
	assertActiveOpenSearchFullRows(t, afterSecretUpdateRestart.router, searchPath("/search/"+secretBag, "*_encrypted_data:*"), "/search/"+secretBag, []string{})

	afterSecretDeleteRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertObjectMissingWithVersion(t, afterSecretDeleteRestart.router, secretItemPath, "2")
	assertActiveOpenSearchFullRows(t, afterSecretDeleteRestart.router, searchPath("/search/"+secretBag, "*_encrypted_data:*"), "/search/"+secretBag, []string{})
}

// encryptedDataBagPartialExpectation derives the partial-search assertion body
// from a fixture payload so updated opaque envelope values stay easy to verify.
func encryptedDataBagPartialExpectation(t *testing.T, payload map[string]any) map[string]any {
	t.Helper()

	password, ok := payload["password"].(map[string]any)
	if !ok {
		t.Fatalf("encrypted data bag payload password = %T, want object", payload["password"])
	}
	apiKey, ok := payload["api_key"].(map[string]any)
	if !ok {
		t.Fatalf("encrypted data bag payload api_key = %T, want object", payload["api_key"])
	}
	return map[string]any{
		"password_ciphertext": password["encrypted_data"],
		"password_iv":         password["iv"],
		"api_auth_tag":        apiKey["auth_tag"],
		"environment":         payload["environment"],
	}
}

func assertRawDataBagItemWithVersion(t *testing.T, router http.Handler, path, serverAPIVersion string, want map[string]any) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	if rec.Code != http.StatusOK {
		t.Fatalf("get raw data bag item %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
	assertRawDataBagItemPayload(t, decodeDataBagPayload(t, rec), want)
}

func assertDeletedDataBagPayload(t *testing.T, payload map[string]any, bagName string) {
	t.Helper()

	if len(payload) != 3 {
		t.Fatalf("deleted data bag payload field count = %d, want 3: %v", len(payload), payload)
	}
	if payload["name"] != bagName {
		t.Fatalf("deleted data bag name = %v, want %q", payload["name"], bagName)
	}
	if payload["json_class"] != "Chef::DataBag" {
		t.Fatalf("deleted data bag json_class = %v, want Chef::DataBag", payload["json_class"])
	}
	if payload["chef_type"] != "data_bag" {
		t.Fatalf("deleted data bag chef_type = %v, want data_bag", payload["chef_type"])
	}
}

func assertOnePartialSearchRow(t *testing.T, payload map[string]any, wantURL string, wantData map[string]any) {
	t.Helper()

	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("partial search payload = %v, want one row", payload)
	}
	row := rows[0].(map[string]any)
	if row["url"] != wantURL {
		t.Fatalf("partial search url = %v, want %q", row["url"], wantURL)
	}
	data := row["data"].(map[string]any)
	for key, want := range wantData {
		if data[key] != want {
			t.Fatalf("partial search data[%s] = %v, want %v (data=%v)", key, data[key], want, data)
		}
	}
}
