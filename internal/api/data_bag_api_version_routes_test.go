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

	restarted := fixture.restart()
	assertRawDataBagItemWithVersion(t, restarted.router, "/organizations/ponyville/data/"+bagName+"/"+itemID, "2", current)
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
