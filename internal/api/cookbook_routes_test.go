package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/config"
)

func TestCookbookArtifactEndpointsCreateReadDownloadAndDelete(t *testing.T) {
	router := newTestRouter(t)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello from opencook'"))
	identifier := "1111111111111111111111111111111111111111"
	createBody := mustMarshalSandboxJSON(t, cookbookArtifactPayload("demo", identifier, "1.2.3", checksum, map[string]string{
		"apt": ">= 1.0.0",
	}))

	createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/demo/"+identifier, createBody)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create artifact status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	listReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list artifacts status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list artifacts) error = %v", err)
	}
	demo := listPayload["demo"].(map[string]any)
	versions := demo["versions"].([]any)
	if len(versions) != 1 {
		t.Fatalf("versions len = %d, want 1 (%v)", len(versions), versions)
	}
	if versions[0].(map[string]any)["identifier"] != identifier {
		t.Fatalf("identifier = %v, want %q", versions[0].(map[string]any)["identifier"], identifier)
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/"+identifier, nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get artifact status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get artifact) error = %v", err)
	}
	if getPayload["identifier"] != identifier {
		t.Fatalf("identifier = %v, want %q", getPayload["identifier"], identifier)
	}
	recipes := getPayload["recipes"].([]any)
	if len(recipes) != 1 {
		t.Fatalf("recipes len = %d, want 1 (%v)", len(recipes), recipes)
	}
	recipe := recipes[0].(map[string]any)
	if recipe["checksum"] != checksum {
		t.Fatalf("recipe checksum = %v, want %q", recipe["checksum"], checksum)
	}

	downloadReq := httptest.NewRequest(http.MethodGet, recipe["url"].(string), nil)
	downloadRec := httptest.NewRecorder()
	router.ServeHTTP(downloadRec, downloadReq)
	if downloadRec.Code != http.StatusOK {
		t.Fatalf("download checksum status = %d, want %d, body = %s", downloadRec.Code, http.StatusOK, downloadRec.Body.String())
	}
	if downloadRec.Body.String() != "puts 'hello from opencook'" {
		t.Fatalf("download body = %q, want recipe contents", downloadRec.Body.String())
	}

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/demo/"+identifier, nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete artifact status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	missingReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/"+identifier, nil)
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing artifact status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

func TestCookbookArtifactEndpointsRejectMissingUploadedChecksum(t *testing.T) {
	router := newTestRouter(t)

	body := mustMarshalSandboxJSON(t, cookbookArtifactPayload("demo", "1111111111111111111111111111111111111111", "1.2.3", "8288b67da0793b5abec709d6226e6b73", nil))
	req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	errors := payload["error"].([]any)
	if len(errors) != 1 || errors[0] != "Manifest has a checksum that hasn't been uploaded." {
		t.Fatalf("errors = %v, want missing checksum validation", errors)
	}
}

func TestCookbookArtifactRoutesReturn503WhenS3BlobCheckerUnavailable(t *testing.T) {
	store, control := newCookbookRouteTestS3BlobStore(t)
	router := newTestRouterWithBlob(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, store)

	t.Run("create", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'artifact create unavailable'"))
		control.setExistsUnavailable(true)
		defer control.setExistsUnavailable(false)

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/unavailable-create/1111111111111111111111111111111111111111",
			mustMarshalSandboxJSON(t, cookbookArtifactPayload("unavailable-create", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("artifact create status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
		}
		assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	})

	t.Run("repeated put", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'artifact update unavailable'"))
		createCookbookArtifact(t, router, "unavailable-update", "2222222222222222222222222222222222222222", "1.2.3", checksum, nil)

		control.setExistsUnavailable(true)
		defer control.setExistsUnavailable(false)

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/unavailable-update/2222222222222222222222222222222222222222",
			mustMarshalSandboxJSON(t, cookbookArtifactPayload("unavailable-update", "2222222222222222222222222222222222222222", "1.2.3", checksum, nil)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("artifact repeated put status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
		}
		assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	})
}

func TestCookbookArtifactCollectionEndpointsMatchPedantShapes(t *testing.T) {
	router := newTestRouter(t)

	emptyReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/", nil)
	emptyRec := httptest.NewRecorder()
	router.ServeHTTP(emptyRec, emptyReq)
	if emptyRec.Code != http.StatusOK {
		t.Fatalf("empty collection status = %d, want %d, body = %s", emptyRec.Code, http.StatusOK, emptyRec.Body.String())
	}

	var emptyPayload map[string]any
	if err := json.Unmarshal(emptyRec.Body.Bytes(), &emptyPayload); err != nil {
		t.Fatalf("json.Unmarshal(empty collection) error = %v", err)
	}
	if len(emptyPayload) != 0 {
		t.Fatalf("empty collection payload = %v, want {}", emptyPayload)
	}

	createCookbookArtifact(t, router, "cookbook_name", "1111111111111111111111111111111111111111", "1.0.0", "", nil)
	createCookbookArtifact(t, router, "cookbook_name", "2222222222222222222222222222222222222222", "1.0.0", "", nil)
	createCookbookArtifact(t, router, "cookbook_name2", "3333333333333333333333333333333333333333", "1.0.0", "", nil)

	listReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list collection status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(collection) error = %v", err)
	}

	cookbookNameEntry, ok := listPayload["cookbook_name"].(map[string]any)
	if !ok {
		t.Fatalf("collection cookbook_name entry = %T, want map[string]any (%v)", listPayload["cookbook_name"], listPayload)
	}
	if cookbookNameEntry["url"] != "/cookbook_artifacts/cookbook_name" {
		t.Fatalf("cookbook_name url = %v, want %q", cookbookNameEntry["url"], "/cookbook_artifacts/cookbook_name")
	}
	cookbookNameVersions := cookbookVersionListForName(t, listPayload, "cookbook_name")
	if len(cookbookNameVersions) != 2 {
		t.Fatalf("cookbook_name versions len = %d, want 2 (%v)", len(cookbookNameVersions), cookbookNameVersions)
	}
	firstVersion := cookbookNameVersions[0].(map[string]any)
	secondVersion := cookbookNameVersions[1].(map[string]any)
	if firstVersion["identifier"] != "1111111111111111111111111111111111111111" || secondVersion["identifier"] != "2222222222222222222222222222222222222222" {
		t.Fatalf("cookbook_name identifiers = %v, want ordered identifiers", cookbookNameVersions)
	}
	if firstVersion["url"] != "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111" {
		t.Fatalf("first artifact url = %v, want %q", firstVersion["url"], "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111")
	}
	if secondVersion["url"] != "/cookbook_artifacts/cookbook_name/2222222222222222222222222222222222222222" {
		t.Fatalf("second artifact url = %v, want %q", secondVersion["url"], "/cookbook_artifacts/cookbook_name/2222222222222222222222222222222222222222")
	}

	cookbookName2Entry, ok := listPayload["cookbook_name2"].(map[string]any)
	if !ok {
		t.Fatalf("collection cookbook_name2 entry = %T, want map[string]any (%v)", listPayload["cookbook_name2"], listPayload)
	}
	if cookbookName2Entry["url"] != "/cookbook_artifacts/cookbook_name2" {
		t.Fatalf("cookbook_name2 url = %v, want %q", cookbookName2Entry["url"], "/cookbook_artifacts/cookbook_name2")
	}
	cookbookName2Versions := cookbookVersionListForName(t, listPayload, "cookbook_name2")
	if len(cookbookName2Versions) != 1 {
		t.Fatalf("cookbook_name2 versions len = %d, want 1 (%v)", len(cookbookName2Versions), cookbookName2Versions)
	}

	namedReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/cookbook_name", nil)
	namedRec := httptest.NewRecorder()
	router.ServeHTTP(namedRec, namedReq)
	if namedRec.Code != http.StatusOK {
		t.Fatalf("named collection status = %d, want %d, body = %s", namedRec.Code, http.StatusOK, namedRec.Body.String())
	}

	var namedPayload map[string]any
	if err := json.Unmarshal(namedRec.Body.Bytes(), &namedPayload); err != nil {
		t.Fatalf("json.Unmarshal(named collection) error = %v", err)
	}
	if len(namedPayload) != 1 {
		t.Fatalf("named payload len = %d, want 1 (%v)", len(namedPayload), namedPayload)
	}
	namedVersions := cookbookVersionListForName(t, namedPayload, "cookbook_name")
	if len(namedVersions) != 2 {
		t.Fatalf("named cookbook_name versions len = %d, want 2 (%v)", len(namedVersions), namedVersions)
	}
}

func TestCookbookArtifactDownloadReturns503WhenS3BlobGetterUnavailable(t *testing.T) {
	store, control := newCookbookRouteTestS3BlobStore(t)
	router := newTestRouterWithBlob(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, store)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'artifact download unavailable'"))
	createCookbookArtifact(t, router, "artifact-download-unavailable", "3333333333333333333333333333333333333333", "1.2.3", checksum, nil)
	downloadURL := cookbookArtifactFileURL(t, router, "/cookbook_artifacts/artifact-download-unavailable/3333333333333333333333333333333333333333")

	control.setGetUnavailable(true)
	defer control.setGetUnavailable(false)

	downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	downloadRec := httptest.NewRecorder()
	router.ServeHTTP(downloadRec, downloadReq)

	if downloadRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("artifact download status = %d, want %d, body = %s", downloadRec.Code, http.StatusServiceUnavailable, downloadRec.Body.String())
	}
	assertCookbookAPIError(t, downloadRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
}

func TestCookbookArtifactEndpointsSupportAPIV2AllFilesReadShape(t *testing.T) {
	router := newTestRouter(t)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'artifact v2'"))
	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	getReq.Header.Set("X-Ops-Server-API-Version", "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("artifact v2 get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(v2 artifact get) error = %v", err)
	}
	if _, ok := payload["recipes"]; ok {
		t.Fatalf("v2 artifact response unexpectedly included recipes: %v", payload)
	}
	allFiles, ok := payload["all_files"].([]any)
	if !ok || len(allFiles) != 1 {
		t.Fatalf("v2 all_files = %T/%v, want single entry", payload["all_files"], payload["all_files"])
	}
	file, ok := allFiles[0].(map[string]any)
	if !ok {
		t.Fatalf("v2 all_files entry = %T, want map[string]any", allFiles[0])
	}
	if file["name"] != "recipes/default.rb" || file["path"] != "recipes/default.rb" {
		t.Fatalf("v2 file entry = %v, want recipes/default.rb path and name", file)
	}
	if file["checksum"] != checksum {
		t.Fatalf("v2 file checksum = %v, want %q", file["checksum"], checksum)
	}
	if file["specificity"] != "default" {
		t.Fatalf("v2 file specificity = %v, want %q", file["specificity"], "default")
	}
	if _, ok := file["url"].(string); !ok {
		t.Fatalf("v2 file url = %T, want string (%v)", file["url"], file)
	}

	metadata, ok := payload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("v2 metadata = %T, want map[string]any", payload["metadata"])
	}
	providing, ok := metadata["providing"].(map[string]any)
	if !ok || providing["demo::default"] != ">= 0.0.0" {
		t.Fatalf("v2 metadata.providing = %v, want default recipe entry", metadata["providing"])
	}
	recipes, ok := metadata["recipes"].(map[string]any)
	if !ok || recipes["demo::default"] != "" {
		t.Fatalf("v2 metadata.recipes = %v, want default recipe entry", metadata["recipes"])
	}
}

func TestCookbookVersionRoutesReturn503WhenS3BlobCheckerUnavailable(t *testing.T) {
	store, control := newCookbookRouteTestS3BlobStore(t)
	router := newTestRouterWithBlob(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, store)

	t.Run("create", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'cookbook create unavailable'"))
		control.setExistsUnavailable(true)
		defer control.setExistsUnavailable(false)

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/unavailable-create/1.2.3",
			mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-create", "1.2.3", checksum, nil)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("cookbook create status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
		}
		assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	})

	t.Run("update", func(t *testing.T) {
		originalChecksum := uploadCookbookChecksum(t, router, []byte("puts 'cookbook existing body'"))
		replacementChecksum := uploadCookbookChecksum(t, router, []byte("puts 'cookbook replacement body'"))
		createCookbookVersion(t, router, "unavailable-update", "1.2.3", originalChecksum, nil)

		control.setExistsUnavailable(true)
		defer control.setExistsUnavailable(false)

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/unavailable-update/1.2.3",
			mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-update", "1.2.3", replacementChecksum, nil)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("cookbook update status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
		}
		assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	})
}

func TestCookbookDownloadReturns503WhenS3BlobGetterUnavailable(t *testing.T) {
	store, control := newCookbookRouteTestS3BlobStore(t)
	router := newTestRouterWithBlob(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, store)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'cookbook download unavailable'"))
	createCookbookVersion(t, router, "download-unavailable", "1.2.3", checksum, nil)
	downloadURL := cookbookFileURL(t, router, "/cookbooks/download-unavailable/1.2.3")

	control.setGetUnavailable(true)
	defer control.setGetUnavailable(false)

	downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	downloadRec := httptest.NewRecorder()
	router.ServeHTTP(downloadRec, downloadReq)

	if downloadRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("cookbook download status = %d, want %d, body = %s", downloadRec.Code, http.StatusServiceUnavailable, downloadRec.Body.String())
	}
	assertCookbookAPIError(t, downloadRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
}

func TestOrganizationCookbookArtifactBlobLinkedParity(t *testing.T) {
	orgArtifactPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbook_artifacts/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	t.Run("missing_uploaded_checksum", func(t *testing.T) {
		router := newTestRouter(t)

		body := mustMarshalSandboxJSON(t, cookbookArtifactPayload("demo", "1111111111111111111111111111111111111111", "1.2.3", "8288b67da0793b5abec709d6226e6b73", nil))
		req := newSignedJSONRequest(t, http.MethodPut, orgArtifactPath("demo", "1111111111111111111111111111111111111111"), body)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("org-scoped missing checksum status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(org-scoped missing checksum) error = %v", err)
		}
		errors := payload["error"].([]any)
		if len(errors) != 1 || errors[0] != "Manifest has a checksum that hasn't been uploaded." {
			t.Fatalf("errors = %v, want missing checksum validation", errors)
		}
	})

	t.Run("normal_user_read_and_signed_recipe_download", func(t *testing.T) {
		router := newTestRouter(t)

		checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello org artifact normal user'"))
		createOrgCookbookArtifact(t, router, "ponyville", "demo", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)

		getReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, orgArtifactPath("demo", "1111111111111111111111111111111111111111"), nil)
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(org-scoped normal user artifact get) error = %v", err)
		}
		rawRecipes, ok := payload["recipes"].([]any)
		if !ok || len(rawRecipes) != 1 {
			t.Fatalf("recipes = %T/%v, want single recipe entry", payload["recipes"], payload["recipes"])
		}
		recipe, ok := rawRecipes[0].(map[string]any)
		if !ok {
			t.Fatalf("recipe entry = %T, want map[string]any", rawRecipes[0])
		}
		downloadURL, ok := recipe["url"].(string)
		if !ok || downloadURL == "" {
			t.Fatalf("recipe url = %T/%v, want non-empty string", recipe["url"], recipe["url"])
		}

		downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
		downloadRec := httptest.NewRecorder()
		router.ServeHTTP(downloadRec, downloadReq)
		if downloadRec.Code != http.StatusOK {
			t.Fatalf("org-scoped recipe download status = %d, want %d, body = %s", downloadRec.Code, http.StatusOK, downloadRec.Body.String())
		}
		if downloadRec.Body.String() != "puts 'hello org artifact normal user'" {
			t.Fatalf("org-scoped recipe download body = %q, want recipe contents", downloadRec.Body.String())
		}
	})

	t.Run("s3_blob_checker_unavailable", func(t *testing.T) {
		store, control := newCookbookRouteTestS3BlobStore(t)
		router := newTestRouterWithBlob(t, config.Config{
			ServiceName: "opencook",
			Environment: "test",
			AuthSkew:    15 * time.Minute,
		}, store)

		t.Run("create", func(t *testing.T) {
			checksum := uploadCookbookChecksum(t, router, []byte("puts 'org artifact create unavailable'"))
			control.setExistsUnavailable(true)
			defer control.setExistsUnavailable(false)

			req := newSignedJSONRequest(t, http.MethodPut, orgArtifactPath("unavailable-create", "1111111111111111111111111111111111111111"),
				mustMarshalSandboxJSON(t, cookbookArtifactPayload("unavailable-create", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("org-scoped artifact create status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
			assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
		})

		t.Run("repeated_put", func(t *testing.T) {
			checksum := uploadCookbookChecksum(t, router, []byte("puts 'org artifact update unavailable'"))
			createOrgCookbookArtifact(t, router, "ponyville", "unavailable-update", "2222222222222222222222222222222222222222", "1.2.3", checksum, nil)

			control.setExistsUnavailable(true)
			defer control.setExistsUnavailable(false)

			req := newSignedJSONRequest(t, http.MethodPut, orgArtifactPath("unavailable-update", "2222222222222222222222222222222222222222"),
				mustMarshalSandboxJSON(t, cookbookArtifactPayload("unavailable-update", "2222222222222222222222222222222222222222", "1.2.3", checksum, nil)))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("org-scoped artifact repeated put status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
			assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
		})
	})

	t.Run("s3_blob_getter_unavailable", func(t *testing.T) {
		store, control := newCookbookRouteTestS3BlobStore(t)
		router := newTestRouterWithBlob(t, config.Config{
			ServiceName: "opencook",
			Environment: "test",
			AuthSkew:    15 * time.Minute,
		}, store)

		checksum := uploadCookbookChecksum(t, router, []byte("puts 'org artifact download unavailable'"))
		createOrgCookbookArtifact(t, router, "ponyville", "artifact-download-unavailable", "3333333333333333333333333333333333333333", "1.2.3", checksum, nil)
		downloadURL := cookbookArtifactFileURL(t, router, orgArtifactPath("artifact-download-unavailable", "3333333333333333333333333333333333333333"))

		control.setGetUnavailable(true)
		defer control.setGetUnavailable(false)

		downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
		downloadRec := httptest.NewRecorder()
		router.ServeHTTP(downloadRec, downloadReq)

		if downloadRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("org-scoped artifact download status = %d, want %d, body = %s", downloadRec.Code, http.StatusServiceUnavailable, downloadRec.Body.String())
		}
		assertCookbookAPIError(t, downloadRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
	})

	t.Run("delete_cleanup_preserves_shared_checksums", func(t *testing.T) {
		router := newTestRouter(t)
		orgCookbookPath := func(name string, extra ...string) string {
			path := "/organizations/ponyville/cookbooks/" + name
			for _, segment := range extra {
				path += "/" + segment
			}
			return path
		}

		sharedChecksum := uploadCookbookChecksum(t, router, []byte("puts 'shared body'"))
		uniqueChecksum := uploadCookbookChecksum(t, router, []byte("puts 'unique body'"))

		createOrgCookbookVersion(t, router, "ponyville", "shared-demo", "1.0.0", sharedChecksum, nil)
		createOrgCookbookArtifact(t, router, "ponyville", "shared-artifact", "1111111111111111111111111111111111111111", "1.0.0", sharedChecksum, nil)
		createOrgCookbookArtifact(t, router, "ponyville", "unique-artifact", "2222222222222222222222222222222222222222", "1.0.0", uniqueChecksum, nil)

		sharedURL := cookbookFileURL(t, router, orgCookbookPath("shared-demo", "1.0.0"))
		uniqueURL := cookbookArtifactFileURL(t, router, orgArtifactPath("unique-artifact", "2222222222222222222222222222222222222222"))

		deleteUniqueReq := newSignedJSONRequest(t, http.MethodDelete, orgArtifactPath("unique-artifact", "2222222222222222222222222222222222222222"), nil)
		deleteUniqueRec := httptest.NewRecorder()
		router.ServeHTTP(deleteUniqueRec, deleteUniqueReq)
		if deleteUniqueRec.Code != http.StatusOK {
			t.Fatalf("org-scoped delete unique artifact status = %d, want %d, body = %s", deleteUniqueRec.Code, http.StatusOK, deleteUniqueRec.Body.String())
		}
		assertBlobDownloadStatus(t, router, uniqueURL, http.StatusNotFound)

		deleteSharedArtifactReq := newSignedJSONRequest(t, http.MethodDelete, orgArtifactPath("shared-artifact", "1111111111111111111111111111111111111111"), nil)
		deleteSharedArtifactRec := httptest.NewRecorder()
		router.ServeHTTP(deleteSharedArtifactRec, deleteSharedArtifactReq)
		if deleteSharedArtifactRec.Code != http.StatusOK {
			t.Fatalf("org-scoped delete shared artifact status = %d, want %d, body = %s", deleteSharedArtifactRec.Code, http.StatusOK, deleteSharedArtifactRec.Body.String())
		}
		assertBlobDownloadStatus(t, router, sharedURL, http.StatusOK)

		deleteCookbookReq := newSignedJSONRequest(t, http.MethodDelete, orgCookbookPath("shared-demo", "1.0.0"), nil)
		deleteCookbookRec := httptest.NewRecorder()
		router.ServeHTTP(deleteCookbookRec, deleteCookbookReq)
		if deleteCookbookRec.Code != http.StatusOK {
			t.Fatalf("org-scoped delete shared cookbook status = %d, want %d, body = %s", deleteCookbookRec.Code, http.StatusOK, deleteCookbookRec.Body.String())
		}
		assertBlobDownloadStatus(t, router, sharedURL, http.StatusNotFound)
	})
}

func TestOrganizationCookbookVersionBlobLinkedParity(t *testing.T) {
	orgCookbookPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbooks/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	t.Run("normal_user_read_and_signed_recipe_download", func(t *testing.T) {
		router := newTestRouter(t)
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello org normal user'"))
		createOrgCookbookVersion(t, router, "ponyville", "demo", "1.2.3", checksum, nil)

		getReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, orgCookbookPath("demo", "1.2.3"), nil)
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(org-scoped normal user cookbook get) error = %v", err)
		}
		rawRecipes, ok := payload["recipes"].([]any)
		if !ok || len(rawRecipes) != 1 {
			t.Fatalf("recipes = %T/%v, want single recipe entry", payload["recipes"], payload["recipes"])
		}
		recipe, ok := rawRecipes[0].(map[string]any)
		if !ok {
			t.Fatalf("recipe entry = %T, want map[string]any", rawRecipes[0])
		}
		downloadURL, ok := recipe["url"].(string)
		if !ok || downloadURL == "" {
			t.Fatalf("recipe url = %T/%v, want non-empty string", recipe["url"], recipe["url"])
		}

		downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
		downloadRec := httptest.NewRecorder()
		router.ServeHTTP(downloadRec, downloadReq)
		if downloadRec.Code != http.StatusOK {
			t.Fatalf("org-scoped recipe download status = %d, want %d, body = %s", downloadRec.Code, http.StatusOK, downloadRec.Body.String())
		}
		if downloadRec.Body.String() != "puts 'hello org normal user'" {
			t.Fatalf("org-scoped recipe download body = %q, want recipe contents", downloadRec.Body.String())
		}
	})

	t.Run("s3_blob_checker_unavailable", func(t *testing.T) {
		store, control := newCookbookRouteTestS3BlobStore(t)
		router := newTestRouterWithBlob(t, config.Config{
			ServiceName: "opencook",
			Environment: "test",
			AuthSkew:    15 * time.Minute,
		}, store)

		t.Run("create", func(t *testing.T) {
			checksum := uploadCookbookChecksum(t, router, []byte("puts 'org cookbook create unavailable'"))
			control.setExistsUnavailable(true)
			defer control.setExistsUnavailable(false)

			req := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("unavailable-create", "1.2.3"),
				mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-create", "1.2.3", checksum, nil)))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("org-scoped cookbook create status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
			assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
		})

		t.Run("update", func(t *testing.T) {
			originalChecksum := uploadCookbookChecksum(t, router, []byte("puts 'org cookbook existing body'"))
			replacementChecksum := uploadCookbookChecksum(t, router, []byte("puts 'org cookbook replacement body'"))
			createOrgCookbookVersion(t, router, "ponyville", "unavailable-update", "1.2.3", originalChecksum, nil)

			control.setExistsUnavailable(true)
			defer control.setExistsUnavailable(false)

			req := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("unavailable-update", "1.2.3"),
				mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-update", "1.2.3", replacementChecksum, nil)))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("org-scoped cookbook update status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
			}
			assertCookbookAPIError(t, rec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
		})
	})

	t.Run("s3_blob_getter_unavailable", func(t *testing.T) {
		store, control := newCookbookRouteTestS3BlobStore(t)
		router := newTestRouterWithBlob(t, config.Config{
			ServiceName: "opencook",
			Environment: "test",
			AuthSkew:    15 * time.Minute,
		}, store)

		checksum := uploadCookbookChecksum(t, router, []byte("puts 'org cookbook download unavailable'"))
		createOrgCookbookVersion(t, router, "ponyville", "download-unavailable", "1.2.3", checksum, nil)
		downloadURL := cookbookFileURL(t, router, orgCookbookPath("download-unavailable", "1.2.3"))

		control.setGetUnavailable(true)
		defer control.setGetUnavailable(false)

		downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
		downloadRec := httptest.NewRecorder()
		router.ServeHTTP(downloadRec, downloadReq)

		if downloadRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("org-scoped cookbook download status = %d, want %d, body = %s", downloadRec.Code, http.StatusServiceUnavailable, downloadRec.Body.String())
		}
		assertCookbookAPIError(t, downloadRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
	})

	t.Run("released_checksums_cleanup", func(t *testing.T) {
		router := newTestRouter(t)
		oldChecksum := uploadCookbookChecksum(t, router, []byte("puts 'old org body'"))
		newChecksum := uploadCookbookChecksum(t, router, []byte("puts 'new org body'"))

		createOrgCookbookVersion(t, router, "ponyville", "cleanup-demo", "1.2.3", oldChecksum, nil)

		oldURL := cookbookFileURL(t, router, orgCookbookPath("cleanup-demo", "1.2.3"))

		updateReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("cleanup-demo", "1.2.3"), mustMarshalSandboxJSON(t, cookbookVersionPayload("cleanup-demo", "1.2.3", newChecksum, nil)))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("org-scoped update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		assertBlobDownloadStatus(t, router, oldURL, http.StatusNotFound)

		newURL := cookbookFileURL(t, router, orgCookbookPath("cleanup-demo", "1.2.3"))
		assertBlobDownloadStatus(t, router, newURL, http.StatusOK)

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, orgCookbookPath("cleanup-demo", "1.2.3"), nil)
		deleteRec := httptest.NewRecorder()
		router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("org-scoped delete cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}

		assertBlobDownloadStatus(t, router, newURL, http.StatusNotFound)
	})
}

func TestCookbookArtifactCreateValidationAndVersionParity(t *testing.T) {
	router := newTestRouter(t)

	createCases := []struct {
		name       string
		path       string
		payload    map[string]any
		wantStatus int
		wantErrors []string
		verifyPath string
	}{
		{
			name: "create accepts version larger than four bytes",
			path: "/cookbook_artifacts/large-version/1111111111111111111111111111111111111111",
			payload: cookbookArtifactPayload(
				"large-version",
				"1111111111111111111111111111111111111111",
				"1.2.2147483669",
				"",
				nil,
			),
			wantStatus: http.StatusCreated,
			verifyPath: "/cookbook_artifacts/large-version/1111111111111111111111111111111111111111",
		},
		{
			name: "create accepts prerelease version",
			path: "/cookbook_artifacts/prerelease/2222222222222222222222222222222222222222",
			payload: cookbookArtifactPayload(
				"prerelease",
				"2222222222222222222222222222222222222222",
				"1.2.3.beta.5",
				"",
				nil,
			),
			wantStatus: http.StatusCreated,
			verifyPath: "/cookbook_artifacts/prerelease/2222222222222222222222222222222222222222",
		},
		{
			name: "create rejects invalid identifier in url",
			path: "/cookbook_artifacts/cookbook_name/foo@bar",
			payload: cookbookArtifactPayload(
				"cookbook_name",
				"foo@bar",
				"1.2.3",
				"",
				nil,
			),
			wantStatus: http.StatusBadRequest,
			wantErrors: []string{"Field 'identifier' invalid"},
			verifyPath: "/cookbook_artifacts/cookbook_name/foo@bar",
		},
		{
			name: "create rejects invalid cookbook name in url",
			path: "/cookbook_artifacts/first@second/1111111111111111111111111111111111111111",
			payload: cookbookArtifactPayload(
				"first@second",
				"1111111111111111111111111111111111111111",
				"1.2.3",
				"",
				nil,
			),
			wantStatus: http.StatusBadRequest,
			wantErrors: []string{"Field 'name' invalid"},
			verifyPath: "/cookbook_artifacts/first@second/1111111111111111111111111111111111111111",
		},
		{
			name: "create rejects identifier mismatch",
			path: "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111",
			payload: cookbookArtifactPayload(
				"cookbook_name",
				"ffffffffffffffffffffffffffffffffffffffff",
				"1.2.3",
				"",
				nil,
			),
			wantStatus: http.StatusBadRequest,
			wantErrors: []string{"Field 'identifier' invalid : 1111111111111111111111111111111111111111 does not match ffffffffffffffffffffffffffffffffffffffff"},
			verifyPath: "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111",
		},
		{
			name: "create rejects cookbook name mismatch",
			path: "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111",
			payload: cookbookArtifactPayload(
				"foobar",
				"1111111111111111111111111111111111111111",
				"1.2.3",
				"",
				nil,
			),
			wantStatus: http.StatusBadRequest,
			wantErrors: []string{"Field 'name' invalid : cookbook_name does not match foobar"},
			verifyPath: "/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111",
		},
	}

	for _, tc := range createCases {
		t.Run(tc.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPut, tc.path, mustMarshalSandboxJSON(t, tc.payload))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s status = %d, want %d, body = %s", tc.path, rec.Code, tc.wantStatus, rec.Body.String())
			}

			if tc.wantStatus == http.StatusCreated {
				req := newSignedJSONRequest(t, http.MethodGet, tc.verifyPath, nil)
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("GET %s status = %d, want %d, body = %s", tc.verifyPath, rec.Code, http.StatusOK, rec.Body.String())
				}

				var payload map[string]any
				if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
					t.Fatalf("json.Unmarshal(%s) error = %v", tc.verifyPath, err)
				}
				if payload["version"] != tc.payload["version"] {
					t.Fatalf("%s version = %v, want %v", tc.verifyPath, payload["version"], tc.payload["version"])
				}
				return
			}

			assertCookbookErrorList(t, rec.Body.Bytes(), tc.wantErrors)
			assertCookbookArtifactMissing(t, router, tc.verifyPath)
		})
	}
}

func TestCookbookArtifactUpdateConflictAndNoMutationParity(t *testing.T) {
	router := newTestRouter(t)

	createCookbookArtifact(t, router, "cookbook-to-be-modified", "1111111111111111111111111111111111111111", "1.2.3", "", nil)

	updatedPayload := cookbookArtifactPayload("cookbook-to-be-modified", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
	updatedPayload["metadata"].(map[string]any)["description"] = "hi there"

	adminReq := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, updatedPayload))
	adminRec := httptest.NewRecorder()
	router.ServeHTTP(adminRec, adminReq)
	if adminRec.Code != http.StatusConflict {
		t.Fatalf("admin update status = %d, want %d, body = %s", adminRec.Code, http.StatusConflict, adminRec.Body.String())
	}
	assertCookbookArtifactStringError(t, adminRec.Body.Bytes(), "Cookbook artifact already exists")
	assertCookbookArtifactDescription(t, router, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", "compatibility cookbook")

	outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, updatedPayload))
	outsideRec := httptest.NewRecorder()
	router.ServeHTTP(outsideRec, outsideReq)
	if outsideRec.Code != http.StatusForbidden {
		t.Fatalf("outside user update status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
	}
	assertCookbookArtifactDescription(t, router, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", "compatibility cookbook")

	invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, updatedPayload))
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user update status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
	}
	assertCookbookArtifactDescription(t, router, "/cookbook_artifacts/cookbook-to-be-modified/1111111111111111111111111111111111111111", "compatibility cookbook")
}

func TestCookbookArtifactCreateAllowsMetadataDefaultOverrides(t *testing.T) {
	router := newTestRouter(t)

	payload := cookbookArtifactPayload("override-demo", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "1.2.3", "", nil)
	metadata := payload["metadata"].(map[string]any)
	metadata["description"] = "my cookbook"
	metadata["long_description"] = "this is a great cookbook"
	metadata["maintainer"] = "This is my name"
	metadata["maintainer_email"] = "cookbook_author@example.com"
	metadata["license"] = "MPL"

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/override-demo/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", mustMarshalSandboxJSON(t, payload))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create override artifact status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/override-demo/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get override artifact status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(override artifact) error = %v", err)
	}
	getMetadata, ok := getPayload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("override metadata = %T, want map[string]any", getPayload["metadata"])
	}
	for key, want := range map[string]string{
		"description":      "my cookbook",
		"long_description": "this is a great cookbook",
		"maintainer":       "This is my name",
		"maintainer_email": "cookbook_author@example.com",
		"license":          "MPL",
	} {
		if got := getMetadata[key]; got != want {
			t.Fatalf("metadata[%q] = %v, want %q", key, got, want)
		}
	}
}

func TestCookbookArtifactCreateAllowsMultipleIdentifiersForSameCookbook(t *testing.T) {
	router := newTestRouter(t)

	firstPayload := cookbookArtifactPayload("multiple_versions", "1111111111111111111111111111111111111111", "1.0.0", "", nil)
	firstPayload["metadata"].(map[string]any)["description"] = "first artifact"
	firstReq := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/multiple_versions/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, firstPayload))
	firstRec := httptest.NewRecorder()
	router.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusCreated {
		t.Fatalf("create first artifact status = %d, want %d, body = %s", firstRec.Code, http.StatusCreated, firstRec.Body.String())
	}

	secondPayload := cookbookArtifactPayload("multiple_versions", "2222222222222222222222222222222222222222", "1.0.0", "", nil)
	secondPayload["metadata"].(map[string]any)["description"] = "second artifact"
	secondReq := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/multiple_versions/2222222222222222222222222222222222222222", mustMarshalSandboxJSON(t, secondPayload))
	secondRec := httptest.NewRecorder()
	router.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusCreated {
		t.Fatalf("create second artifact status = %d, want %d, body = %s", secondRec.Code, http.StatusCreated, secondRec.Body.String())
	}

	for path, wantDescription := range map[string]string{
		"/cookbook_artifacts/multiple_versions/1111111111111111111111111111111111111111": "first artifact",
		"/cookbook_artifacts/multiple_versions/2222222222222222222222222222222222222222": "second artifact",
	} {
		req := newSignedJSONRequest(t, http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
		}
		metadata, ok := payload["metadata"].(map[string]any)
		if !ok {
			t.Fatalf("%s metadata = %T, want map[string]any", path, payload["metadata"])
		}
		if got := metadata["description"]; got != wantDescription {
			t.Fatalf("%s metadata.description = %v, want %q", path, got, wantDescription)
		}
	}
}

func TestCookbookArtifactCreateValidationHTTPParity(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name       string
		path       string
		payload    map[string]any
		wantErrors []string
		verifyPath string
	}{
		{
			name: "empty metadata",
			path: "/cookbook_artifacts/invalid-meta/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("invalid-meta", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				payload["metadata"] = map[string]any{}
				return payload
			}(),
			wantErrors: []string{"Field 'metadata.version' missing"},
			verifyPath: "/cookbook_artifacts/invalid-meta/1111111111111111111111111111111111111111",
		},
		{
			name: "recipes as string",
			path: "/cookbook_artifacts/segment-string/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("segment-string", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				delete(payload, "all_files")
				payload["recipes"] = "foo"
				return payload
			}(),
			wantErrors: []string{"Field 'recipes' invalid"},
			verifyPath: "/cookbook_artifacts/segment-string/1111111111111111111111111111111111111111",
		},
		{
			name: "recipes element missing required fields",
			path: "/cookbook_artifacts/segment-empty/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("segment-empty", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				delete(payload, "all_files")
				payload["recipes"] = []any{map[string]any{}}
				return payload
			}(),
			wantErrors: []string{"Field 'recipes' invalid"},
			verifyPath: "/cookbook_artifacts/segment-empty/1111111111111111111111111111111111111111",
		},
		{
			name: "dependencies as string",
			path: "/cookbook_artifacts/dependency-string/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("dependency-string", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				payload["metadata"].(map[string]any)["dependencies"] = "foo"
				return payload
			}(),
			wantErrors: []string{"Field 'metadata.dependencies' invalid"},
			verifyPath: "/cookbook_artifacts/dependency-string/1111111111111111111111111111111111111111",
		},
		{
			name: "dependencies malformed constraint",
			path: "/cookbook_artifacts/dependency-constraint/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("dependency-constraint", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				payload["metadata"].(map[string]any)["dependencies"] = map[string]any{"apt": "s395dss@#"}
				return payload
			}(),
			wantErrors: []string{"Invalid value 's395dss@#' for metadata.dependencies"},
			verifyPath: "/cookbook_artifacts/dependency-constraint/1111111111111111111111111111111111111111",
		},
		{
			name: "platforms nested object",
			path: "/cookbook_artifacts/platforms-object/1111111111111111111111111111111111111111",
			payload: func() map[string]any {
				payload := cookbookArtifactPayload("platforms-object", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
				payload["metadata"].(map[string]any)["platforms"] = map[string]any{"ubuntu": map[string]any{}}
				return payload
			}(),
			wantErrors: []string{"Invalid value '{[]}' for metadata.platforms"},
			verifyPath: "/cookbook_artifacts/platforms-object/1111111111111111111111111111111111111111",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPut, tc.path, mustMarshalSandboxJSON(t, tc.payload))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s status = %d, want %d, body = %s", tc.path, rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertCookbookErrorList(t, rec.Body.Bytes(), tc.wantErrors)
			assertCookbookArtifactMissing(t, router, tc.verifyPath)
		})
	}
}

func TestCookbookArtifactEndpointAllowsNormalUserCreateAndRejectsUnauthorizedCreate(t *testing.T) {
	router := newTestRouter(t)

	const normalIdentifier = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const outsideIdentifier = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const invalidIdentifier = "cccccccccccccccccccccccccccccccccccccccc"

	normalPayload := cookbookArtifactPayload("created-by-user", normalIdentifier, "1.2.3", "", nil)
	normalPayload["metadata"].(map[string]any)["description"] = "created by normal-user"
	normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, "/cookbook_artifacts/created-by-user/"+normalIdentifier, mustMarshalSandboxJSON(t, normalPayload))
	normalRec := httptest.NewRecorder()
	router.ServeHTTP(normalRec, normalReq)
	if normalRec.Code != http.StatusCreated {
		t.Fatalf("normal user create status = %d, want %d, body = %s", normalRec.Code, http.StatusCreated, normalRec.Body.String())
	}
	assertCookbookArtifactDescription(t, router, "/cookbook_artifacts/created-by-user/"+normalIdentifier, "created by normal-user")

	outsidePayload := cookbookArtifactPayload("blocked-by-outside", outsideIdentifier, "1.2.3", "", nil)
	outsidePayload["metadata"].(map[string]any)["description"] = "outside user attempted create"
	outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, "/cookbook_artifacts/blocked-by-outside/"+outsideIdentifier, mustMarshalSandboxJSON(t, outsidePayload))
	outsideRec := httptest.NewRecorder()
	router.ServeHTTP(outsideRec, outsideReq)
	if outsideRec.Code != http.StatusForbidden {
		t.Fatalf("outside user create status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
	}
	assertCookbookArtifactMissing(t, router, "/cookbook_artifacts/blocked-by-outside/"+outsideIdentifier)

	invalidPayload := cookbookArtifactPayload("blocked-by-invalid", invalidIdentifier, "1.2.3", "", nil)
	invalidPayload["metadata"].(map[string]any)["description"] = "invalid user attempted create"
	invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, "/cookbook_artifacts/blocked-by-invalid/"+invalidIdentifier, mustMarshalSandboxJSON(t, invalidPayload))
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user create status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
	}
	assertCookbookArtifactMissing(t, router, "/cookbook_artifacts/blocked-by-invalid/"+invalidIdentifier)
}

func TestCookbookArtifactEndpointsDoNotDeleteExistingIdentifierOnWrongDelete(t *testing.T) {
	router := newTestRouter(t)

	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", "", nil)

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/demo/ffffffffffffffffffffffffffffffffffffffff", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNotFound {
		t.Fatalf("delete wrong identifier status = %d, want %d, body = %s", deleteRec.Code, http.StatusNotFound, deleteRec.Body.String())
	}
	assertCookbookErrorList(t, deleteRec.Body.Bytes(), []string{"not_found"})

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get existing artifact after failed delete status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestCookbookArtifactEndpointAuthorizationMatchesChefShapes(t *testing.T) {
	router := newTestRouter(t)

	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", "", nil)

	outsideGetReq := newSignedJSONRequestAs(t, "outside-user", http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	outsideGetRec := httptest.NewRecorder()
	router.ServeHTTP(outsideGetRec, outsideGetReq)
	if outsideGetRec.Code != http.StatusForbidden {
		t.Fatalf("outside user get status = %d, want %d, body = %s", outsideGetRec.Code, http.StatusForbidden, outsideGetRec.Body.String())
	}

	invalidDeleteReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodDelete, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	invalidDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(invalidDeleteRec, invalidDeleteReq)
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get existing artifact after auth failures status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestCookbookArtifactEndpointAllowsNormalUserReadAndSignedRecipeDownload(t *testing.T) {
	router := newTestRouter(t)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello artifact normal user'"))
	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)

	getReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("normal user get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(normal user cookbook artifact get) error = %v", err)
	}
	rawRecipes, ok := payload["recipes"].([]any)
	if !ok || len(rawRecipes) != 1 {
		t.Fatalf("recipes = %T/%v, want single recipe entry", payload["recipes"], payload["recipes"])
	}
	recipe, ok := rawRecipes[0].(map[string]any)
	if !ok {
		t.Fatalf("recipe entry = %T, want map[string]any", rawRecipes[0])
	}
	downloadURL, ok := recipe["url"].(string)
	if !ok || downloadURL == "" {
		t.Fatalf("recipe url = %T/%v, want non-empty string", recipe["url"], recipe["url"])
	}

	downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	downloadRec := httptest.NewRecorder()
	router.ServeHTTP(downloadRec, downloadReq)
	if downloadRec.Code != http.StatusOK {
		t.Fatalf("recipe download status = %d, want %d, body = %s", downloadRec.Code, http.StatusOK, downloadRec.Body.String())
	}
	if downloadRec.Body.String() != "puts 'hello artifact normal user'" {
		t.Fatalf("recipe download body = %q, want recipe contents", downloadRec.Body.String())
	}
}

func TestCookbookArtifactEndpointAllowsNormalUserDeleteAndRejectsUnauthorizedDelete(t *testing.T) {
	router := newTestRouter(t)

	createCookbookArtifact(t, router, "normal-delete", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
	normalDeleteReq := newSignedJSONRequestAs(t, "normal-user", http.MethodDelete, "/cookbook_artifacts/normal-delete/1111111111111111111111111111111111111111", nil)
	normalDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(normalDeleteRec, normalDeleteReq)
	if normalDeleteRec.Code != http.StatusOK {
		t.Fatalf("normal user delete status = %d, want %d, body = %s", normalDeleteRec.Code, http.StatusOK, normalDeleteRec.Body.String())
	}
	assertCookbookArtifactMissing(t, router, "/cookbook_artifacts/normal-delete/1111111111111111111111111111111111111111")

	createCookbookArtifact(t, router, "blocked-delete", "2222222222222222222222222222222222222222", "1.2.3", "", nil)

	outsideDeleteReq := newSignedJSONRequestAs(t, "outside-user", http.MethodDelete, "/cookbook_artifacts/blocked-delete/2222222222222222222222222222222222222222", nil)
	outsideDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(outsideDeleteRec, outsideDeleteReq)
	if outsideDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("outside user delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
	}

	invalidDeleteReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodDelete, "/cookbook_artifacts/blocked-delete/2222222222222222222222222222222222222222", nil)
	invalidDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(invalidDeleteRec, invalidDeleteReq)
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/blocked-delete/2222222222222222222222222222222222222222", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get existing artifact after unauthorized deletes status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestCookbookArtifactRoutesAcceptTrailingSlashes(t *testing.T) {
	router := newTestRouter(t)

	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
	createOrgCookbookArtifact(t, router, "ponyville", "demo", "2222222222222222222222222222222222222222", "1.2.3", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default-org collection", path: "/cookbook_artifacts/"},
		{name: "default-org named collection", path: "/cookbook_artifacts/demo/"},
		{name: "default-org named artifact", path: "/cookbook_artifacts/demo/1111111111111111111111111111111111111111/"},
		{name: "org-scoped collection", path: "/organizations/ponyville/cookbook_artifacts/"},
		{name: "org-scoped named collection", path: "/organizations/ponyville/cookbook_artifacts/demo/"},
		{name: "org-scoped named artifact", path: "/organizations/ponyville/cookbook_artifacts/demo/2222222222222222222222222222222222222222/"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodGet, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("%s status = %d, want %d, body = %s", route.path, rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestCookbookArtifactRoutesReturnMethodNotAllowedWithAllowHeader(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name        string
		path        string
		wantAllow   string
		wantMessage string
	}{
		{
			name:        "default-org collection",
			path:        "/cookbook_artifacts",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook artifacts route",
		},
		{
			name:        "default-org named collection",
			path:        "/cookbook_artifacts/demo",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook artifact route",
		},
		{
			name:        "default-org named artifact",
			path:        "/cookbook_artifacts/demo/1111111111111111111111111111111111111111",
			wantAllow:   strings.Join([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, ", "),
			wantMessage: "method not allowed for cookbook artifact version route",
		},
		{
			name:        "org-scoped collection",
			path:        "/organizations/ponyville/cookbook_artifacts",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook artifacts route",
		},
		{
			name:        "org-scoped named collection",
			path:        "/organizations/ponyville/cookbook_artifacts/demo",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook artifact route",
		},
		{
			name:        "org-scoped named artifact",
			path:        "/organizations/ponyville/cookbook_artifacts/demo/1111111111111111111111111111111111111111",
			wantAllow:   strings.Join([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, ", "),
			wantMessage: "method not allowed for cookbook artifact version route",
		},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s status = %d, want %d, body = %s", route.path, rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
			}
			if rec.Header().Get("Allow") != route.wantAllow {
				t.Fatalf("%s Allow = %q, want %q", route.path, rec.Header().Get("Allow"), route.wantAllow)
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "method_not_allowed" {
				t.Fatalf("%s error = %v, want %q", route.path, payload["error"], "method_not_allowed")
			}
			if payload["message"] != route.wantMessage {
				t.Fatalf("%s message = %v, want %q", route.path, payload["message"], route.wantMessage)
			}
		})
	}
}

func TestCookbookArtifactRoutesReturnNotFoundForExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	routes := []string{
		"/cookbook_artifacts/demo/1111111111111111111111111111111111111111/extra",
		"/organizations/ponyville/cookbook_artifacts/demo/1111111111111111111111111111111111111111/extra",
	}

	for _, path := range routes {
		t.Run(path, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
			}
			assertCookbookErrorList(t, rec.Body.Bytes(), []string{"not_found"})
		})
	}
}

func TestOrganizationCookbookArtifactCollectionAndNamedCollectionParity(t *testing.T) {
	router := newTestRouter(t)

	createOrgCookbookArtifact(t, router, "ponyville", "cookbook_name", "1111111111111111111111111111111111111111", "1.0.0", "", nil)
	createOrgCookbookArtifact(t, router, "ponyville", "cookbook_name", "2222222222222222222222222222222222222222", "1.0.0", "", nil)
	createOrgCookbookArtifact(t, router, "ponyville", "cookbook_name2", "3333333333333333333333333333333333333333", "1.0.0", "", nil)

	listReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/cookbook_artifacts", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("org-scoped list collection status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(org collection) error = %v", err)
	}

	cookbookNameEntry, ok := listPayload["cookbook_name"].(map[string]any)
	if !ok {
		t.Fatalf("org collection cookbook_name entry = %T, want map[string]any (%v)", listPayload["cookbook_name"], listPayload)
	}
	if cookbookNameEntry["url"] != "/organizations/ponyville/cookbook_artifacts/cookbook_name" {
		t.Fatalf("org collection cookbook_name url = %v, want %q", cookbookNameEntry["url"], "/organizations/ponyville/cookbook_artifacts/cookbook_name")
	}
	cookbookNameVersions := cookbookVersionListForName(t, listPayload, "cookbook_name")
	if len(cookbookNameVersions) != 2 {
		t.Fatalf("org collection cookbook_name versions len = %d, want 2 (%v)", len(cookbookNameVersions), cookbookNameVersions)
	}
	firstVersion := cookbookNameVersions[0].(map[string]any)
	secondVersion := cookbookNameVersions[1].(map[string]any)
	if firstVersion["url"] != "/organizations/ponyville/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111" {
		t.Fatalf("org collection first artifact url = %v, want %q", firstVersion["url"], "/organizations/ponyville/cookbook_artifacts/cookbook_name/1111111111111111111111111111111111111111")
	}
	if secondVersion["url"] != "/organizations/ponyville/cookbook_artifacts/cookbook_name/2222222222222222222222222222222222222222" {
		t.Fatalf("org collection second artifact url = %v, want %q", secondVersion["url"], "/organizations/ponyville/cookbook_artifacts/cookbook_name/2222222222222222222222222222222222222222")
	}

	namedReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/cookbook_artifacts/cookbook_name", nil)
	namedRec := httptest.NewRecorder()
	router.ServeHTTP(namedRec, namedReq)
	if namedRec.Code != http.StatusOK {
		t.Fatalf("org-scoped named collection status = %d, want %d, body = %s", namedRec.Code, http.StatusOK, namedRec.Body.String())
	}

	var namedPayload map[string]any
	if err := json.Unmarshal(namedRec.Body.Bytes(), &namedPayload); err != nil {
		t.Fatalf("json.Unmarshal(org named collection) error = %v", err)
	}
	if len(namedPayload) != 1 {
		t.Fatalf("org named payload len = %d, want 1 (%v)", len(namedPayload), namedPayload)
	}
	namedVersions := cookbookVersionListForName(t, namedPayload, "cookbook_name")
	if len(namedVersions) != 2 {
		t.Fatalf("org named cookbook_name versions len = %d, want 2 (%v)", len(namedVersions), namedVersions)
	}
}

func TestOrganizationCookbookArtifactValidationAndNoMutationParity(t *testing.T) {
	router := newTestRouter(t)

	orgArtifactPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbook_artifacts/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	t.Run("create_validation_and_version_parity", func(t *testing.T) {
		createCases := []struct {
			name       string
			path       string
			payload    map[string]any
			wantStatus int
			wantErrors []string
			verifyPath string
		}{
			{
				name: "create accepts version larger than four bytes",
				path: orgArtifactPath("large-version", "1111111111111111111111111111111111111111"),
				payload: cookbookArtifactPayload(
					"large-version",
					"1111111111111111111111111111111111111111",
					"1.2.2147483669",
					"",
					nil,
				),
				wantStatus: http.StatusCreated,
				verifyPath: orgArtifactPath("large-version", "1111111111111111111111111111111111111111"),
			},
			{
				name: "create accepts prerelease version",
				path: orgArtifactPath("prerelease", "2222222222222222222222222222222222222222"),
				payload: cookbookArtifactPayload(
					"prerelease",
					"2222222222222222222222222222222222222222",
					"1.2.3.beta.5",
					"",
					nil,
				),
				wantStatus: http.StatusCreated,
				verifyPath: orgArtifactPath("prerelease", "2222222222222222222222222222222222222222"),
			},
			{
				name: "create rejects invalid identifier in url",
				path: orgArtifactPath("cookbook_name", "foo@bar"),
				payload: cookbookArtifactPayload(
					"cookbook_name",
					"foo@bar",
					"1.2.3",
					"",
					nil,
				),
				wantStatus: http.StatusBadRequest,
				wantErrors: []string{"Field 'identifier' invalid"},
				verifyPath: orgArtifactPath("cookbook_name", "foo@bar"),
			},
			{
				name: "create rejects invalid cookbook name in url",
				path: orgArtifactPath("first@second", "1111111111111111111111111111111111111111"),
				payload: cookbookArtifactPayload(
					"first@second",
					"1111111111111111111111111111111111111111",
					"1.2.3",
					"",
					nil,
				),
				wantStatus: http.StatusBadRequest,
				wantErrors: []string{"Field 'name' invalid"},
				verifyPath: orgArtifactPath("first@second", "1111111111111111111111111111111111111111"),
			},
			{
				name: "create rejects identifier mismatch",
				path: orgArtifactPath("cookbook_name", "1111111111111111111111111111111111111111"),
				payload: cookbookArtifactPayload(
					"cookbook_name",
					"ffffffffffffffffffffffffffffffffffffffff",
					"1.2.3",
					"",
					nil,
				),
				wantStatus: http.StatusBadRequest,
				wantErrors: []string{"Field 'identifier' invalid : 1111111111111111111111111111111111111111 does not match ffffffffffffffffffffffffffffffffffffffff"},
				verifyPath: orgArtifactPath("cookbook_name", "1111111111111111111111111111111111111111"),
			},
			{
				name: "create rejects cookbook name mismatch",
				path: orgArtifactPath("cookbook_name", "1111111111111111111111111111111111111111"),
				payload: cookbookArtifactPayload(
					"foobar",
					"1111111111111111111111111111111111111111",
					"1.2.3",
					"",
					nil,
				),
				wantStatus: http.StatusBadRequest,
				wantErrors: []string{"Field 'name' invalid : cookbook_name does not match foobar"},
				verifyPath: orgArtifactPath("cookbook_name", "1111111111111111111111111111111111111111"),
			},
		}

		for _, tc := range createCases {
			t.Run(tc.name, func(t *testing.T) {
				req := newSignedJSONRequest(t, http.MethodPut, tc.path, mustMarshalSandboxJSON(t, tc.payload))
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != tc.wantStatus {
					t.Fatalf("%s status = %d, want %d, body = %s", tc.path, rec.Code, tc.wantStatus, rec.Body.String())
				}

				if tc.wantStatus == http.StatusCreated {
					req := newSignedJSONRequest(t, http.MethodGet, tc.verifyPath, nil)
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Fatalf("GET %s status = %d, want %d, body = %s", tc.verifyPath, rec.Code, http.StatusOK, rec.Body.String())
					}

					var payload map[string]any
					if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
						t.Fatalf("json.Unmarshal(%s) error = %v", tc.verifyPath, err)
					}
					if payload["version"] != tc.payload["version"] {
						t.Fatalf("%s version = %v, want %v", tc.verifyPath, payload["version"], tc.payload["version"])
					}
					return
				}

				assertCookbookErrorList(t, rec.Body.Bytes(), tc.wantErrors)
				assertCookbookArtifactMissing(t, router, tc.verifyPath)
			})
		}
	})

	t.Run("repeated_put_conflict_and_unauthorized_updates_do_not_mutate_existing_artifact", func(t *testing.T) {
		createOrgCookbookArtifact(t, router, "ponyville", "cookbook-to-be-modified", "1111111111111111111111111111111111111111", "1.2.3", "", nil)

		updatedPayload := cookbookArtifactPayload("cookbook-to-be-modified", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
		updatedPayload["metadata"].(map[string]any)["description"] = "hi there"

		adminReq := newSignedJSONRequest(t, http.MethodPut, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), mustMarshalSandboxJSON(t, updatedPayload))
		adminRec := httptest.NewRecorder()
		router.ServeHTTP(adminRec, adminReq)
		if adminRec.Code != http.StatusConflict {
			t.Fatalf("org-scoped repeated put status = %d, want %d, body = %s", adminRec.Code, http.StatusConflict, adminRec.Body.String())
		}
		assertCookbookArtifactStringError(t, adminRec.Body.Bytes(), "Cookbook artifact already exists")
		assertCookbookArtifactDescription(t, router, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), "compatibility cookbook")

		outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), mustMarshalSandboxJSON(t, updatedPayload))
		outsideRec := httptest.NewRecorder()
		router.ServeHTTP(outsideRec, outsideReq)
		if outsideRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user update status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
		}
		assertCookbookArtifactDescription(t, router, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), "compatibility cookbook")

		invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), mustMarshalSandboxJSON(t, updatedPayload))
		invalidRec := httptest.NewRecorder()
		router.ServeHTTP(invalidRec, invalidReq)
		if invalidRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user update status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
		}
		assertCookbookArtifactDescription(t, router, orgArtifactPath("cookbook-to-be-modified", "1111111111111111111111111111111111111111"), "compatibility cookbook")
	})

	t.Run("validation_http_parity", func(t *testing.T) {
		tests := []struct {
			name       string
			path       string
			payload    map[string]any
			wantErrors []string
			verifyPath string
		}{
			{
				name: "empty metadata",
				path: orgArtifactPath("invalid-meta", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("invalid-meta", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					payload["metadata"] = map[string]any{}
					return payload
				}(),
				wantErrors: []string{"Field 'metadata.version' missing"},
				verifyPath: orgArtifactPath("invalid-meta", "1111111111111111111111111111111111111111"),
			},
			{
				name: "recipes as string",
				path: orgArtifactPath("segment-string", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("segment-string", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					delete(payload, "all_files")
					payload["recipes"] = "foo"
					return payload
				}(),
				wantErrors: []string{"Field 'recipes' invalid"},
				verifyPath: orgArtifactPath("segment-string", "1111111111111111111111111111111111111111"),
			},
			{
				name: "recipes element missing required fields",
				path: orgArtifactPath("segment-empty", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("segment-empty", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					delete(payload, "all_files")
					payload["recipes"] = []any{map[string]any{}}
					return payload
				}(),
				wantErrors: []string{"Field 'recipes' invalid"},
				verifyPath: orgArtifactPath("segment-empty", "1111111111111111111111111111111111111111"),
			},
			{
				name: "dependencies as string",
				path: orgArtifactPath("dependency-string", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("dependency-string", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					payload["metadata"].(map[string]any)["dependencies"] = "foo"
					return payload
				}(),
				wantErrors: []string{"Field 'metadata.dependencies' invalid"},
				verifyPath: orgArtifactPath("dependency-string", "1111111111111111111111111111111111111111"),
			},
			{
				name: "dependencies malformed constraint",
				path: orgArtifactPath("dependency-constraint", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("dependency-constraint", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					payload["metadata"].(map[string]any)["dependencies"] = map[string]any{"apt": "s395dss@#"}
					return payload
				}(),
				wantErrors: []string{"Invalid value 's395dss@#' for metadata.dependencies"},
				verifyPath: orgArtifactPath("dependency-constraint", "1111111111111111111111111111111111111111"),
			},
			{
				name: "platforms nested object",
				path: orgArtifactPath("platforms-object", "1111111111111111111111111111111111111111"),
				payload: func() map[string]any {
					payload := cookbookArtifactPayload("platforms-object", "1111111111111111111111111111111111111111", "1.2.3", "", nil)
					payload["metadata"].(map[string]any)["platforms"] = map[string]any{"ubuntu": map[string]any{}}
					return payload
				}(),
				wantErrors: []string{"Invalid value '{[]}' for metadata.platforms"},
				verifyPath: orgArtifactPath("platforms-object", "1111111111111111111111111111111111111111"),
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				req := newSignedJSONRequest(t, http.MethodPut, tc.path, mustMarshalSandboxJSON(t, tc.payload))
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("%s status = %d, want %d, body = %s", tc.path, rec.Code, http.StatusBadRequest, rec.Body.String())
				}

				assertCookbookErrorList(t, rec.Body.Bytes(), tc.wantErrors)
				assertCookbookArtifactMissing(t, router, tc.verifyPath)
			})
		}
	})
}

func TestOrganizationCookbookArtifactAuthParity(t *testing.T) {
	router := newTestRouter(t)

	orgArtifactPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbook_artifacts/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	t.Run("create_and_read_auth", func(t *testing.T) {
		const normalIdentifier = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		const outsideIdentifier = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
		const invalidIdentifier = "cccccccccccccccccccccccccccccccccccccccc"

		normalPayload := cookbookArtifactPayload("created-by-user", normalIdentifier, "1.2.3", "", nil)
		normalPayload["metadata"].(map[string]any)["description"] = "created by normal-user"
		normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, orgArtifactPath("created-by-user", normalIdentifier), mustMarshalSandboxJSON(t, normalPayload))
		normalRec := httptest.NewRecorder()
		router.ServeHTTP(normalRec, normalReq)
		if normalRec.Code != http.StatusCreated {
			t.Fatalf("org-scoped normal user create status = %d, want %d, body = %s", normalRec.Code, http.StatusCreated, normalRec.Body.String())
		}
		assertCookbookArtifactDescription(t, router, orgArtifactPath("created-by-user", normalIdentifier), "created by normal-user")

		outsidePayload := cookbookArtifactPayload("blocked-by-outside", outsideIdentifier, "1.2.3", "", nil)
		outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, orgArtifactPath("blocked-by-outside", outsideIdentifier), mustMarshalSandboxJSON(t, outsidePayload))
		outsideRec := httptest.NewRecorder()
		router.ServeHTTP(outsideRec, outsideReq)
		if outsideRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user create status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
		}
		assertCookbookArtifactMissing(t, router, orgArtifactPath("blocked-by-outside", outsideIdentifier))

		invalidPayload := cookbookArtifactPayload("blocked-by-invalid", invalidIdentifier, "1.2.3", "", nil)
		invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, orgArtifactPath("blocked-by-invalid", invalidIdentifier), mustMarshalSandboxJSON(t, invalidPayload))
		invalidRec := httptest.NewRecorder()
		router.ServeHTTP(invalidRec, invalidReq)
		if invalidRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user create status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
		}
		assertCookbookArtifactMissing(t, router, orgArtifactPath("blocked-by-invalid", invalidIdentifier))

		outsideGetReq := newSignedJSONRequestAs(t, "outside-user", http.MethodGet, orgArtifactPath("created-by-user", normalIdentifier), nil)
		outsideGetRec := httptest.NewRecorder()
		router.ServeHTTP(outsideGetRec, outsideGetReq)
		if outsideGetRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user get status = %d, want %d, body = %s", outsideGetRec.Code, http.StatusForbidden, outsideGetRec.Body.String())
		}

		getReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, orgArtifactPath("created-by-user", normalIdentifier), nil)
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}
	})

	t.Run("wrong_delete_and_delete_auth", func(t *testing.T) {
		createOrgCookbookArtifact(t, router, "ponyville", "demo", "1111111111111111111111111111111111111111", "1.2.3", "", nil)

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, orgArtifactPath("demo", "ffffffffffffffffffffffffffffffffffffffff"), nil)
		deleteRec := httptest.NewRecorder()
		router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusNotFound {
			t.Fatalf("org-scoped delete wrong identifier status = %d, want %d, body = %s", deleteRec.Code, http.StatusNotFound, deleteRec.Body.String())
		}
		assertCookbookErrorList(t, deleteRec.Body.Bytes(), []string{"not_found"})
		assertCookbookArtifactDescription(t, router, orgArtifactPath("demo", "1111111111111111111111111111111111111111"), "compatibility cookbook")

		invalidDeleteReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodDelete, orgArtifactPath("demo", "1111111111111111111111111111111111111111"), nil)
		invalidDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(invalidDeleteRec, invalidDeleteReq)
		if invalidDeleteRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
		}
		assertCookbookArtifactDescription(t, router, orgArtifactPath("demo", "1111111111111111111111111111111111111111"), "compatibility cookbook")

		createOrgCookbookArtifact(t, router, "ponyville", "normal-delete", "2222222222222222222222222222222222222222", "1.2.3", "", nil)
		normalDeleteReq := newSignedJSONRequestAs(t, "normal-user", http.MethodDelete, orgArtifactPath("normal-delete", "2222222222222222222222222222222222222222"), nil)
		normalDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(normalDeleteRec, normalDeleteReq)
		if normalDeleteRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user delete status = %d, want %d, body = %s", normalDeleteRec.Code, http.StatusOK, normalDeleteRec.Body.String())
		}
		assertCookbookArtifactMissing(t, router, orgArtifactPath("normal-delete", "2222222222222222222222222222222222222222"))

		createOrgCookbookArtifact(t, router, "ponyville", "blocked-delete", "3333333333333333333333333333333333333333", "1.2.3", "", nil)
		outsideDeleteReq := newSignedJSONRequestAs(t, "outside-user", http.MethodDelete, orgArtifactPath("blocked-delete", "3333333333333333333333333333333333333333"), nil)
		outsideDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(outsideDeleteRec, outsideDeleteReq)
		if outsideDeleteRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
		}
		assertCookbookArtifactDescription(t, router, orgArtifactPath("blocked-delete", "3333333333333333333333333333333333333333"), "compatibility cookbook")
	})
}

func TestCookbookCollectionAndNamedFilterAuthorizationParity(t *testing.T) {
	router := newTestRouter(t)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'collection auth parity'"))
	createCookbookVersion(t, router, "demo", "1.2.3", checksum, nil)
	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)

	routes := []string{
		"/cookbooks",
		"/cookbooks/_latest",
		"/cookbooks/_recipes",
		"/cookbooks/demo",
		"/cookbooks/demo/_latest",
		"/cookbook_artifacts",
		"/cookbook_artifacts/demo",
		"/organizations/ponyville/cookbooks",
		"/organizations/ponyville/cookbooks/_latest",
		"/organizations/ponyville/cookbooks/_recipes",
		"/organizations/ponyville/cookbooks/demo",
		"/organizations/ponyville/cookbooks/demo/_latest",
		"/organizations/ponyville/cookbook_artifacts",
		"/organizations/ponyville/cookbook_artifacts/demo",
	}

	for _, path := range routes {
		t.Run(path, func(t *testing.T) {
			normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, path, nil)
			normalRec := httptest.NewRecorder()
			router.ServeHTTP(normalRec, normalReq)
			if normalRec.Code != http.StatusOK {
				t.Fatalf("normal user GET %s status = %d, want %d, body = %s", path, normalRec.Code, http.StatusOK, normalRec.Body.String())
			}

			outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodGet, path, nil)
			outsideRec := httptest.NewRecorder()
			router.ServeHTTP(outsideRec, outsideReq)
			if outsideRec.Code != http.StatusForbidden {
				t.Fatalf("outside user GET %s status = %d, want %d, body = %s", path, outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
			}

			invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodGet, path, nil)
			invalidRec := httptest.NewRecorder()
			router.ServeHTTP(invalidRec, invalidReq)
			if invalidRec.Code != http.StatusUnauthorized {
				t.Fatalf("invalid user GET %s status = %d, want %d, body = %s", path, invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
			}
		})
	}
}

func TestOrganizationCookbookVersionMutationAuthorizationParity(t *testing.T) {
	router := newTestRouter(t)

	orgCookbookPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbooks/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	t.Run("create", func(t *testing.T) {
		normalPayload := cookbookVersionPayload("created-by-user", "1.2.3", "", nil)
		normalPayload["metadata"].(map[string]any)["description"] = "created by normal-user"
		normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, orgCookbookPath("created-by-user", "1.2.3"), mustMarshalSandboxJSON(t, normalPayload))
		normalRec := httptest.NewRecorder()
		router.ServeHTTP(normalRec, normalReq)
		if normalRec.Code != http.StatusCreated {
			t.Fatalf("org-scoped normal user create status = %d, want %d, body = %s", normalRec.Code, http.StatusCreated, normalRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("created-by-user", "1.2.3"), "created by normal-user")

		outsidePayload := cookbookVersionPayload("blocked-by-outside", "1.2.3", "", nil)
		outsidePayload["metadata"].(map[string]any)["description"] = "outside user attempted create"
		outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, orgCookbookPath("blocked-by-outside", "1.2.3"), mustMarshalSandboxJSON(t, outsidePayload))
		outsideRec := httptest.NewRecorder()
		router.ServeHTTP(outsideRec, outsideReq)
		if outsideRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user create status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
		}
		assertCookbookMissing(t, router, orgCookbookPath("blocked-by-outside", "1.2.3"))

		invalidPayload := cookbookVersionPayload("blocked-by-invalid", "1.2.3", "", nil)
		invalidPayload["metadata"].(map[string]any)["description"] = "invalid user attempted create"
		invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, orgCookbookPath("blocked-by-invalid", "1.2.3"), mustMarshalSandboxJSON(t, invalidPayload))
		invalidRec := httptest.NewRecorder()
		router.ServeHTTP(invalidRec, invalidReq)
		if invalidRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user create status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
		}
		assertCookbookMissing(t, router, orgCookbookPath("blocked-by-invalid", "1.2.3"))
	})

	t.Run("update", func(t *testing.T) {
		createOrgCookbookVersion(t, router, "ponyville", "demo", "1.2.3", "", nil)

		normalPayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
		normalPayload["metadata"].(map[string]any)["description"] = "updated by normal-user"
		normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, normalPayload))
		normalRec := httptest.NewRecorder()
		router.ServeHTTP(normalRec, normalReq)
		if normalRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user update status = %d, want %d, body = %s", normalRec.Code, http.StatusOK, normalRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("demo", "1.2.3"), "updated by normal-user")

		outsidePayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
		outsidePayload["metadata"].(map[string]any)["description"] = "outside user update"
		outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, outsidePayload))
		outsideRec := httptest.NewRecorder()
		router.ServeHTTP(outsideRec, outsideReq)
		if outsideRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user update status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("demo", "1.2.3"), "updated by normal-user")

		invalidPayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
		invalidPayload["metadata"].(map[string]any)["description"] = "invalid user update"
		invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, invalidPayload))
		invalidRec := httptest.NewRecorder()
		router.ServeHTTP(invalidRec, invalidReq)
		if invalidRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user update status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("demo", "1.2.3"), "updated by normal-user")
	})

	t.Run("delete", func(t *testing.T) {
		createOrgCookbookVersion(t, router, "ponyville", "normal-delete", "1.2.3", "", nil)
		normalDeleteReq := newSignedJSONRequestAs(t, "normal-user", http.MethodDelete, orgCookbookPath("normal-delete", "1.2.3"), nil)
		normalDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(normalDeleteRec, normalDeleteReq)
		if normalDeleteRec.Code != http.StatusOK {
			t.Fatalf("org-scoped normal user delete status = %d, want %d, body = %s", normalDeleteRec.Code, http.StatusOK, normalDeleteRec.Body.String())
		}
		assertCookbookMissing(t, router, orgCookbookPath("normal-delete", "1.2.3"))

		createOrgCookbookVersion(t, router, "ponyville", "blocked-delete", "1.2.3", "", nil)
		outsideDeleteReq := newSignedJSONRequestAs(t, "outside-user", http.MethodDelete, orgCookbookPath("blocked-delete", "1.2.3"), nil)
		outsideDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(outsideDeleteRec, outsideDeleteReq)
		if outsideDeleteRec.Code != http.StatusForbidden {
			t.Fatalf("org-scoped outside user delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("blocked-delete", "1.2.3"), "compatibility cookbook")

		invalidDeleteReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodDelete, orgCookbookPath("blocked-delete", "1.2.3"), nil)
		invalidDeleteRec := httptest.NewRecorder()
		router.ServeHTTP(invalidDeleteRec, invalidDeleteReq)
		if invalidDeleteRec.Code != http.StatusUnauthorized {
			t.Fatalf("org-scoped invalid user delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
		}
		assertCookbookDescription(t, router, orgCookbookPath("blocked-delete", "1.2.3"), "compatibility cookbook")
	})
}

func TestOrganizationCookbookArtifactSupportsAPIV2AllFilesReadShape(t *testing.T) {
	router := newTestRouter(t)

	checksum := uploadCookbookChecksum(t, router, []byte("puts 'org artifact v2'"))
	createOrgCookbookArtifact(t, router, "ponyville", "demo", "1111111111111111111111111111111111111111", "1.2.3", checksum, nil)

	getReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	getReq.Header.Set("X-Ops-Server-API-Version", "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("org-scoped artifact v2 get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(org v2 artifact get) error = %v", err)
	}
	if _, ok := payload["recipes"]; ok {
		t.Fatalf("org-scoped v2 artifact response unexpectedly included recipes: %v", payload)
	}
	allFiles, ok := payload["all_files"].([]any)
	if !ok || len(allFiles) != 1 {
		t.Fatalf("org-scoped v2 all_files = %T/%v, want single entry", payload["all_files"], payload["all_files"])
	}
	file, ok := allFiles[0].(map[string]any)
	if !ok {
		t.Fatalf("org-scoped v2 all_files entry = %T, want map[string]any", allFiles[0])
	}
	if file["name"] != "recipes/default.rb" || file["path"] != "recipes/default.rb" {
		t.Fatalf("org-scoped v2 file entry = %v, want recipes/default.rb path and name", file)
	}
	if file["checksum"] != checksum {
		t.Fatalf("org-scoped v2 file checksum = %v, want %q", file["checksum"], checksum)
	}
	if file["specificity"] != "default" {
		t.Fatalf("org-scoped v2 file specificity = %v, want %q", file["specificity"], "default")
	}
	if _, ok := file["url"].(string); !ok {
		t.Fatalf("org-scoped v2 file url = %T, want string (%v)", file["url"], file)
	}
}

func TestCookbookEndpointsListLatestRecipesUniverseAndV2VersionView(t *testing.T) {
	router := newTestRouter(t)

	checksumV1 := uploadCookbookChecksum(t, router, []byte("puts 'v1'"))
	checksumV2 := uploadCookbookChecksum(t, router, []byte("puts 'v2'"))

	createCookbookVersion(t, router, "demo", "1.0.0", checksumV1, map[string]string{
		"apt": ">= 1.0.0",
	})
	createCookbookVersion(t, router, "demo", "1.2.0", checksumV2, map[string]string{
		"apt": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "other", "0.1.0", "", nil)

	listReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks", nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list cookbooks status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list cookbooks) error = %v", err)
	}
	demo := listPayload["demo"].(map[string]any)
	demoVersions := demo["versions"].([]any)
	if len(demoVersions) != 1 || demoVersions[0].(map[string]any)["version"] != "1.2.0" {
		t.Fatalf("default cookbook versions = %v, want latest only", demoVersions)
	}

	allReq := httptest.NewRequest(http.MethodGet, "/cookbooks?num_versions=all", nil)
	applySignedHeaders(t, allReq, "silent-bob", "", http.MethodGet, "/cookbooks", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	allRec := httptest.NewRecorder()
	router.ServeHTTP(allRec, allReq)
	if allRec.Code != http.StatusOK {
		t.Fatalf("list cookbooks all status = %d, want %d, body = %s", allRec.Code, http.StatusOK, allRec.Body.String())
	}
	if err := json.Unmarshal(allRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list cookbooks all) error = %v", err)
	}
	demoVersions = listPayload["demo"].(map[string]any)["versions"].([]any)
	if len(demoVersions) != 2 {
		t.Fatalf("all cookbook versions len = %d, want 2 (%v)", len(demoVersions), demoVersions)
	}

	latestReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/_latest", nil)
	latestRec := httptest.NewRecorder()
	router.ServeHTTP(latestRec, latestReq)
	if latestRec.Code != http.StatusOK {
		t.Fatalf("latest cookbooks status = %d, want %d, body = %s", latestRec.Code, http.StatusOK, latestRec.Body.String())
	}
	var latestPayload map[string]any
	if err := json.Unmarshal(latestRec.Body.Bytes(), &latestPayload); err != nil {
		t.Fatalf("json.Unmarshal(latest cookbooks) error = %v", err)
	}
	if latestPayload["demo"] != "/cookbooks/demo/1.2.0" {
		t.Fatalf("latest demo url = %v, want %q", latestPayload["demo"], "/cookbooks/demo/1.2.0")
	}

	recipesReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/_recipes", nil)
	recipesRec := httptest.NewRecorder()
	router.ServeHTTP(recipesRec, recipesReq)
	if recipesRec.Code != http.StatusOK {
		t.Fatalf("recipe list status = %d, want %d, body = %s", recipesRec.Code, http.StatusOK, recipesRec.Body.String())
	}
	var recipesPayload []string
	if err := json.Unmarshal(recipesRec.Body.Bytes(), &recipesPayload); err != nil {
		t.Fatalf("json.Unmarshal(recipe list) error = %v", err)
	}
	if len(recipesPayload) == 0 || recipesPayload[0] != "demo" {
		t.Fatalf("recipes payload = %v, want latest recipe listing", recipesPayload)
	}

	namedReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo", nil)
	namedRec := httptest.NewRecorder()
	router.ServeHTTP(namedRec, namedReq)
	if namedRec.Code != http.StatusOK {
		t.Fatalf("named cookbook status = %d, want %d, body = %s", namedRec.Code, http.StatusOK, namedRec.Body.String())
	}
	var namedPayload map[string]any
	if err := json.Unmarshal(namedRec.Body.Bytes(), &namedPayload); err != nil {
		t.Fatalf("json.Unmarshal(named cookbook) error = %v", err)
	}
	namedDemo := namedPayload["demo"].(map[string]any)
	namedVersions := namedDemo["versions"].([]any)
	if len(namedVersions) != 2 {
		t.Fatalf("named cookbook versions len = %d, want 2 (%v)", len(namedVersions), namedVersions)
	}
	if namedVersions[0].(map[string]any)["version"] != "1.2.0" || namedVersions[1].(map[string]any)["version"] != "1.0.0" {
		t.Fatalf("named cookbook versions = %v, want descending versions", namedVersions)
	}

	versionReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/_latest", nil)
	versionReq.Header.Set("X-Ops-Server-API-Version", "2")
	versionRec := httptest.NewRecorder()
	router.ServeHTTP(versionRec, versionReq)
	if versionRec.Code != http.StatusOK {
		t.Fatalf("latest version status = %d, want %d, body = %s", versionRec.Code, http.StatusOK, versionRec.Body.String())
	}
	var versionPayload map[string]any
	if err := json.Unmarshal(versionRec.Body.Bytes(), &versionPayload); err != nil {
		t.Fatalf("json.Unmarshal(version view) error = %v", err)
	}
	if versionPayload["version"] != "1.2.0" {
		t.Fatalf("version = %v, want %q", versionPayload["version"], "1.2.0")
	}
	if versionPayload["cookbook_name"] != "demo" {
		t.Fatalf("cookbook_name = %v, want %q", versionPayload["cookbook_name"], "demo")
	}
	if versionPayload["json_class"] != "Chef::CookbookVersion" {
		t.Fatalf("json_class = %v, want %q", versionPayload["json_class"], "Chef::CookbookVersion")
	}
	if _, ok := versionPayload["all_files"]; !ok {
		t.Fatalf("v2 cookbook version payload missing all_files: %v", versionPayload)
	}

	universeReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/universe", nil)
	universeRec := httptest.NewRecorder()
	router.ServeHTTP(universeRec, universeReq)
	if universeRec.Code != http.StatusOK {
		t.Fatalf("universe status = %d, want %d, body = %s", universeRec.Code, http.StatusOK, universeRec.Body.String())
	}
	var universePayload map[string]any
	if err := json.Unmarshal(universeRec.Body.Bytes(), &universePayload); err != nil {
		t.Fatalf("json.Unmarshal(universe) error = %v", err)
	}
	demoUniverse := universePayload["demo"].(map[string]any)
	v12 := demoUniverse["1.2.0"].(map[string]any)
	if v12["location_path"] != "/organizations/ponyville/cookbooks/demo/1.2.0" {
		t.Fatalf("location_path = %v, want org-scoped cookbook path", v12["location_path"])
	}
	deps := v12["dependencies"].(map[string]any)
	if deps["apt"] != ">= 2.0.0" {
		t.Fatalf("dependencies.apt = %v, want %q", deps["apt"], ">= 2.0.0")
	}
}

func TestCookbookRoutesAcceptTrailingSlashes(t *testing.T) {
	router := newTestRouter(t)

	checksumV1 := uploadCookbookChecksum(t, router, []byte("puts 'route semantics v1'"))
	checksumV2 := uploadCookbookChecksum(t, router, []byte("puts 'route semantics v2'"))

	createCookbookVersion(t, router, "demo", "1.0.0", checksumV1, nil)
	createCookbookVersion(t, router, "demo", "1.2.0", checksumV2, nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default-org collection", path: "/cookbooks/"},
		{name: "default-org latest collection", path: "/cookbooks/_latest/"},
		{name: "default-org recipes collection", path: "/cookbooks/_recipes/"},
		{name: "default-org named cookbook", path: "/cookbooks/demo/"},
		{name: "default-org named version", path: "/cookbooks/demo/1.2.0/"},
		{name: "default-org named latest version", path: "/cookbooks/demo/_latest/"},
		{name: "default-org universe", path: "/universe/"},
		{name: "org-scoped collection", path: "/organizations/ponyville/cookbooks/"},
		{name: "org-scoped latest collection", path: "/organizations/ponyville/cookbooks/_latest/"},
		{name: "org-scoped recipes collection", path: "/organizations/ponyville/cookbooks/_recipes/"},
		{name: "org-scoped named cookbook", path: "/organizations/ponyville/cookbooks/demo/"},
		{name: "org-scoped named version", path: "/organizations/ponyville/cookbooks/demo/1.2.0/"},
		{name: "org-scoped named latest version", path: "/organizations/ponyville/cookbooks/demo/_latest/"},
		{name: "org-scoped universe", path: "/organizations/ponyville/universe/"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodGet, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("%s status = %d, want %d, body = %s", route.path, rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestCookbookRoutesReturnMethodNotAllowedWithAllowHeader(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name        string
		path        string
		wantAllow   string
		wantMessage string
	}{
		{
			name:        "default-org collection",
			path:        "/cookbooks",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "default-org latest collection",
			path:        "/cookbooks/_latest",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "default-org recipes collection",
			path:        "/cookbooks/_recipes",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "default-org named cookbook",
			path:        "/cookbooks/demo",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook route",
		},
		{
			name:        "default-org named version",
			path:        "/cookbooks/demo/1.2.3",
			wantAllow:   strings.Join([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, ", "),
			wantMessage: "method not allowed for cookbook version route",
		},
		{
			name:        "default-org universe",
			path:        "/universe",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for universe route",
		},
		{
			name:        "org-scoped collection",
			path:        "/organizations/ponyville/cookbooks",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "org-scoped latest collection",
			path:        "/organizations/ponyville/cookbooks/_latest",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "org-scoped recipes collection",
			path:        "/organizations/ponyville/cookbooks/_recipes",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbooks route",
		},
		{
			name:        "org-scoped named cookbook",
			path:        "/organizations/ponyville/cookbooks/demo",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for cookbook route",
		},
		{
			name:        "org-scoped named version",
			path:        "/organizations/ponyville/cookbooks/demo/1.2.3",
			wantAllow:   strings.Join([]string{http.MethodGet, http.MethodPut, http.MethodDelete}, ", "),
			wantMessage: "method not allowed for cookbook version route",
		},
		{
			name:        "org-scoped universe",
			path:        "/organizations/ponyville/universe",
			wantAllow:   http.MethodGet,
			wantMessage: "method not allowed for universe route",
		},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s status = %d, want %d, body = %s", route.path, rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
			}
			if rec.Header().Get("Allow") != route.wantAllow {
				t.Fatalf("%s Allow = %q, want %q", route.path, rec.Header().Get("Allow"), route.wantAllow)
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "method_not_allowed" {
				t.Fatalf("%s error = %v, want %q", route.path, payload["error"], "method_not_allowed")
			}
			if payload["message"] != route.wantMessage {
				t.Fatalf("%s message = %v, want %q", route.path, payload["message"], route.wantMessage)
			}
		})
	}
}

func TestCookbookRoutesReturnNotFoundForExtraPath(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default-org latest collection extra path", path: "/cookbooks/_latest/extra"},
		{name: "default-org recipes collection extra path", path: "/cookbooks/_recipes/extra"},
		{name: "default-org named version extra path", path: "/cookbooks/demo/1.2.3/extra"},
		{name: "default-org named latest version extra path", path: "/cookbooks/demo/_latest/extra"},
		{name: "default-org universe extra path", path: "/universe/extra"},
		{name: "org-scoped latest collection extra path", path: "/organizations/ponyville/cookbooks/_latest/extra"},
		{name: "org-scoped recipes collection extra path", path: "/organizations/ponyville/cookbooks/_recipes/extra"},
		{name: "org-scoped named version extra path", path: "/organizations/ponyville/cookbooks/demo/1.2.3/extra"},
		{name: "org-scoped named latest version extra path", path: "/organizations/ponyville/cookbooks/demo/_latest/extra"},
		{name: "org-scoped universe extra path", path: "/organizations/ponyville/universe/extra"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodGet, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s status = %d, want %d, body = %s", route.path, rec.Code, http.StatusNotFound, rec.Body.String())
			}
		})
	}
}

func TestCookbookRecipesCollectionUsesLatestManifestAndDefaultNaming(t *testing.T) {
	router := newTestRouter(t)

	oldDefaultChecksum := uploadCookbookChecksum(t, router, []byte("puts 'old default'"))
	oldLegacyChecksum := uploadCookbookChecksum(t, router, []byte("puts 'legacy recipe'"))
	newDefaultChecksum := uploadCookbookChecksum(t, router, []byte("puts 'new default'"))
	newUsersChecksum := uploadCookbookChecksum(t, router, []byte("puts 'users recipe'"))

	oldPayload := cookbookVersionPayload("demo", "1.0.0", "", nil)
	oldPayload["metadata"].(map[string]any)["recipes"] = map[string]any{
		"demo::default": "",
		"demo::legacy":  "",
	}
	oldPayload["all_files"] = []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    oldDefaultChecksum,
			"specificity": "default",
		},
		map[string]any{
			"name":        "recipes/legacy.rb",
			"path":        "recipes/legacy.rb",
			"checksum":    oldLegacyChecksum,
			"specificity": "default",
		},
	}
	oldCreateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.0.0", mustMarshalSandboxJSON(t, oldPayload))
	oldCreateRec := httptest.NewRecorder()
	router.ServeHTTP(oldCreateRec, oldCreateReq)
	if oldCreateRec.Code != http.StatusCreated {
		t.Fatalf("old cookbook create status = %d, want %d, body = %s", oldCreateRec.Code, http.StatusCreated, oldCreateRec.Body.String())
	}

	newPayload := cookbookVersionPayload("demo", "2.0.0", "", nil)
	newPayload["metadata"].(map[string]any)["recipes"] = map[string]any{
		"demo::default": "",
		"demo::ghost":   "",
		"demo::users":   "",
	}
	newPayload["all_files"] = []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    newDefaultChecksum,
			"specificity": "default",
		},
		map[string]any{
			"name":        "recipes/users.rb",
			"path":        "recipes/users.rb",
			"checksum":    newUsersChecksum,
			"specificity": "default",
		},
	}
	newCreateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/2.0.0", mustMarshalSandboxJSON(t, newPayload))
	newCreateRec := httptest.NewRecorder()
	router.ServeHTTP(newCreateRec, newCreateReq)
	if newCreateRec.Code != http.StatusCreated {
		t.Fatalf("new cookbook create status = %d, want %d, body = %s", newCreateRec.Code, http.StatusCreated, newCreateRec.Body.String())
	}

	recipesReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/_recipes", nil)
	recipesRec := httptest.NewRecorder()
	router.ServeHTTP(recipesRec, recipesReq)
	if recipesRec.Code != http.StatusOK {
		t.Fatalf("recipe list status = %d, want %d, body = %s", recipesRec.Code, http.StatusOK, recipesRec.Body.String())
	}

	var recipesPayload []string
	if err := json.Unmarshal(recipesRec.Body.Bytes(), &recipesPayload); err != nil {
		t.Fatalf("json.Unmarshal(recipe list) error = %v", err)
	}
	want := []string{"demo", "demo::users"}
	if len(recipesPayload) != len(want) {
		t.Fatalf("recipes payload len = %d, want %d (%v)", len(recipesPayload), len(want), recipesPayload)
	}
	for idx := range want {
		if recipesPayload[idx] != want[idx] {
			t.Fatalf("recipes payload[%d] = %q, want %q (%v)", idx, recipesPayload[idx], want[idx], recipesPayload)
		}
	}
}

func TestCookbookVersionEndpointsCreateUpdateAndDelete(t *testing.T) {
	router := newTestRouter(t)
	checksumV1 := uploadCookbookChecksum(t, router, []byte("puts 'hello v1'"))
	checksumV2 := uploadCookbookChecksum(t, router, []byte("puts 'hello v2'"))

	createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "1.2.3", checksumV1, map[string]string{
		"apt": ">= 1.0.0",
	})))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create cookbook status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create cookbook) error = %v", err)
	}
	if createPayload["name"] != "demo-1.2.3" {
		t.Fatalf("create name = %v, want %q", createPayload["name"], "demo-1.2.3")
	}
	if createPayload["cookbook_name"] != "demo" {
		t.Fatalf("create cookbook_name = %v, want %q", createPayload["cookbook_name"], "demo")
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get cookbook status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get cookbook) error = %v", err)
	}
	recipes := getPayload["recipes"].([]any)
	if len(recipes) != 1 {
		t.Fatalf("recipes len = %d, want 1 (%v)", len(recipes), recipes)
	}
	if _, ok := recipes[0].(map[string]any)["url"]; !ok {
		t.Fatalf("recipe entry missing url: %v", recipes[0])
	}
	for _, segment := range []string{"attributes", "definitions", "files", "libraries", "providers", "resources", "root_files", "templates"} {
		raw, ok := getPayload[segment]
		if !ok {
			t.Fatalf("legacy cookbook payload missing %q: %v", segment, getPayload)
		}
		entries, ok := raw.([]any)
		if !ok {
			t.Fatalf("legacy cookbook payload %q = %T, want []any", segment, raw)
		}
		if len(entries) != 0 {
			t.Fatalf("legacy cookbook payload %q len = %d, want 0 (%v)", segment, len(entries), entries)
		}
	}

	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "1.2.3", checksumV2, map[string]string{
		"apt": ">= 2.0.0",
	})))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	updatedGetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	updatedGetReq.Header.Set("X-Ops-Server-API-Version", "2")
	updatedGetRec := httptest.NewRecorder()
	router.ServeHTTP(updatedGetRec, updatedGetReq)
	if updatedGetRec.Code != http.StatusOK {
		t.Fatalf("updated get cookbook status = %d, want %d, body = %s", updatedGetRec.Code, http.StatusOK, updatedGetRec.Body.String())
	}
	var updatedPayload map[string]any
	if err := json.Unmarshal(updatedGetRec.Body.Bytes(), &updatedPayload); err != nil {
		t.Fatalf("json.Unmarshal(updated get cookbook) error = %v", err)
	}
	allFiles := updatedPayload["all_files"].([]any)
	if len(allFiles) != 1 {
		t.Fatalf("all_files len = %d, want 1 (%v)", len(allFiles), allFiles)
	}
	deps := updatedPayload["metadata"].(map[string]any)["dependencies"].(map[string]any)
	if deps["apt"] != ">= 2.0.0" {
		t.Fatalf("updated dependencies.apt = %v, want %q", deps["apt"], ">= 2.0.0")
	}

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/demo/1.2.3", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	missingReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing cookbook status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

func TestCookbookVersionCreateAndUpdateValidationMatchChefSemantics(t *testing.T) {
	router := newTestRouter(t)

	createVersionMismatchReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "0.0.1", "", nil)))
	createVersionMismatchRec := httptest.NewRecorder()
	router.ServeHTTP(createVersionMismatchRec, createVersionMismatchReq)
	if createVersionMismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("create version mismatch status = %d, want %d, body = %s", createVersionMismatchRec.Code, http.StatusBadRequest, createVersionMismatchRec.Body.String())
	}
	assertCookbookErrorList(t, createVersionMismatchRec.Body.Bytes(), []string{"Field 'name' invalid"})

	createNameMismatchReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, cookbookVersionPayload("other", "1.2.3", "", nil)))
	createNameMismatchRec := httptest.NewRecorder()
	router.ServeHTTP(createNameMismatchRec, createNameMismatchReq)
	if createNameMismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("create cookbook_name mismatch status = %d, want %d, body = %s", createNameMismatchRec.Code, http.StatusBadRequest, createNameMismatchRec.Body.String())
	}
	assertCookbookErrorList(t, createNameMismatchRec.Body.Bytes(), []string{"Field 'name' invalid"})

	createCookbookVersion(t, router, "demo", "1.2.3", "", nil)
	updatePayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
	updatePayload["cookbook_name"] = "other"
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("update cookbook_name mismatch status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	assertCookbookErrorList(t, updateRec.Body.Bytes(), []string{"Field 'cookbook_name' invalid"})
}

func TestCookbookVersionTopLevelFieldValidationAndNoMutationParity(t *testing.T) {
	router := newTestRouter(t)

	createCookbookVersion(t, router, "top-level-demo", "1.2.3", "", nil)

	tests := []struct {
		name         string
		path         string
		payload      map[string]any
		wantStatus   int
		wantErrors   []string
		verifyPath   string
		verifyExists bool
		wantDesc     string
	}{
		{
			name: "create rejects invalid json_class",
			path: "/cookbooks/create-json-class/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("create-json-class", "1.2.3", "", nil)
				payload["json_class"] = "Chef::Role"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'json_class' invalid"},
			verifyPath:   "/cookbooks/create-json-class/1.2.3",
			verifyExists: false,
		},
		{
			name: "create rejects invalid chef_type",
			path: "/cookbooks/create-chef-type/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("create-chef-type", "1.2.3", "", nil)
				payload["chef_type"] = "not_a_cookbook_version"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'chef_type' invalid"},
			verifyPath:   "/cookbooks/create-chef-type/1.2.3",
			verifyExists: false,
		},
		{
			name: "create rejects invalid version",
			path: "/cookbooks/create-version/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("create-version", "1.2.3", "", nil)
				payload["version"] = "0.0"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'version' invalid"},
			verifyPath:   "/cookbooks/create-version/1.2.3",
			verifyExists: false,
		},
		{
			name: "create rejects invalid request key",
			path: "/cookbooks/create-invalid-key/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("create-invalid-key", "1.2.3", "", nil)
				payload["bogus"] = "nope"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid key bogus in request body"},
			verifyPath:   "/cookbooks/create-invalid-key/1.2.3",
			verifyExists: false,
		},
		{
			name: "update rejects invalid json_class",
			path: "/cookbooks/top-level-demo/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
				payload["json_class"] = "Chef::NonCookbook"
				payload["metadata"].(map[string]any)["description"] = "bad json_class"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'json_class' invalid"},
			verifyPath:   "/cookbooks/top-level-demo/1.2.3",
			verifyExists: true,
			wantDesc:     "compatibility cookbook",
		},
		{
			name: "update rejects invalid chef_type",
			path: "/cookbooks/top-level-demo/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
				payload["chef_type"] = "not_cookbook"
				payload["metadata"].(map[string]any)["description"] = "bad chef_type"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'chef_type' invalid"},
			verifyPath:   "/cookbooks/top-level-demo/1.2.3",
			verifyExists: true,
			wantDesc:     "compatibility cookbook",
		},
		{
			name: "update rejects invalid version",
			path: "/cookbooks/top-level-demo/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
				payload["version"] = "0.0"
				payload["metadata"].(map[string]any)["description"] = "bad version"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Field 'version' invalid"},
			verifyPath:   "/cookbooks/top-level-demo/1.2.3",
			verifyExists: true,
			wantDesc:     "compatibility cookbook",
		},
		{
			name: "update rejects invalid request key",
			path: "/cookbooks/top-level-demo/1.2.3",
			payload: func() map[string]any {
				payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
				payload["bogus"] = []string{"still bad"}
				payload["metadata"].(map[string]any)["description"] = "bad bogus key"
				return payload
			}(),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid key bogus in request body"},
			verifyPath:   "/cookbooks/top-level-demo/1.2.3",
			verifyExists: true,
			wantDesc:     "compatibility cookbook",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPut, tt.path, mustMarshalSandboxJSON(t, tt.payload))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("%s status = %d, want %d, body = %s", tt.path, rec.Code, tt.wantStatus, rec.Body.String())
			}
			assertCookbookErrorList(t, rec.Body.Bytes(), tt.wantErrors)

			if !tt.verifyExists {
				assertCookbookMissing(t, router, tt.verifyPath)
				return
			}
			assertCookbookDescription(t, router, tt.verifyPath, tt.wantDesc)
		})
	}
}

func TestCookbookVersionRouteValidationRejectsMalformedNameAndVersions(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name         string
		method       string
		path         string
		body         []byte
		wantStatus   int
		wantErrors   []string
		verifyNoDemo bool
	}{
		{
			name:         "invalid cookbook name on create",
			method:       http.MethodPut,
			path:         "/cookbooks/first@second/1.2.3",
			body:         mustMarshalSandboxJSON(t, cookbookVersionPayload("first@second", "1.2.3", "", nil)),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid cookbook name 'first@second' using regex: 'Malformed cookbook name. Must only contain A-Z, a-z, 0-9, _, . or -'."},
			verifyNoDemo: false,
		},
		{
			name:         "negative cookbook version on create",
			method:       http.MethodPut,
			path:         "/cookbooks/demo/1.2.-42",
			body:         mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "1.2.-42", "", nil)),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid cookbook version '1.2.-42'."},
			verifyNoDemo: true,
		},
		{
			name:         "alphabetic cookbook version on create",
			method:       http.MethodPut,
			path:         "/cookbooks/demo/abc",
			body:         mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "abc", "", nil)),
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid cookbook version 'abc'."},
			verifyNoDemo: true,
		},
		{
			name:         "negative cookbook version on read",
			method:       http.MethodGet,
			path:         "/cookbooks/demo/1.2.-42",
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid cookbook version '1.2.-42'."},
			verifyNoDemo: false,
		},
		{
			name:         "negative cookbook version on delete",
			method:       http.MethodDelete,
			path:         "/cookbooks/demo/1.2.-42",
			wantStatus:   http.StatusBadRequest,
			wantErrors:   []string{"Invalid cookbook version '1.2.-42'."},
			verifyNoDemo: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, tt.method, tt.path, tt.body)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tt.wantStatus {
				t.Fatalf("%s status = %d, want %d, body = %s", tt.path, rec.Code, tt.wantStatus, rec.Body.String())
			}
			assertCookbookErrorList(t, rec.Body.Bytes(), tt.wantErrors)
			if tt.verifyNoDemo {
				assertCookbookMissing(t, router, "/cookbooks/demo")
			}
		})
	}
}

func TestCookbookVersionRouteValidationHandlesLargeVersionComponents(t *testing.T) {
	router := newTestRouter(t)

	createCookbookVersion(t, router, "large-version-demo", "1.2.2147483669", "", nil)

	overflowPayload := cookbookVersionPayload("overflow-demo", "1.2.9223372036854775849", "", nil)
	overflowReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/overflow-demo/1.2.9223372036854775849", mustMarshalSandboxJSON(t, overflowPayload))
	overflowRec := httptest.NewRecorder()
	router.ServeHTTP(overflowRec, overflowReq)
	if overflowRec.Code != http.StatusBadRequest {
		t.Fatalf("overflow version create status = %d, want %d, body = %s", overflowRec.Code, http.StatusBadRequest, overflowRec.Body.String())
	}
	assertCookbookErrorList(t, overflowRec.Body.Bytes(), []string{"Invalid cookbook version '1.2.9223372036854775849'."})

	assertCookbookMissing(t, router, "/cookbooks/overflow-demo")
}

func TestOrganizationCookbookVersionValidationAndNoMutationParity(t *testing.T) {
	router := newTestRouter(t)

	orgCookbookPath := func(name string, extra ...string) string {
		path := "/organizations/ponyville/cookbooks/" + name
		for _, segment := range extra {
			path += "/" + segment
		}
		return path
	}

	createOrgCookbookVersion := func(t *testing.T, name, version, checksum string, dependencies map[string]string) {
		t.Helper()

		req := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath(name, version), mustMarshalSandboxJSON(t, cookbookVersionPayload(name, version, checksum, dependencies)))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create org-scoped cookbook %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
		}
	}

	t.Run("route_and_payload_mismatches", func(t *testing.T) {
		createVersionMismatchReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, cookbookVersionPayload("demo", "0.0.1", "", nil)))
		createVersionMismatchRec := httptest.NewRecorder()
		router.ServeHTTP(createVersionMismatchRec, createVersionMismatchReq)
		if createVersionMismatchRec.Code != http.StatusBadRequest {
			t.Fatalf("org-scoped create version mismatch status = %d, want %d, body = %s", createVersionMismatchRec.Code, http.StatusBadRequest, createVersionMismatchRec.Body.String())
		}
		assertCookbookErrorList(t, createVersionMismatchRec.Body.Bytes(), []string{"Field 'name' invalid"})
		assertCookbookMissing(t, router, orgCookbookPath("demo", "1.2.3"))

		createNameMismatchReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, cookbookVersionPayload("other", "1.2.3", "", nil)))
		createNameMismatchRec := httptest.NewRecorder()
		router.ServeHTTP(createNameMismatchRec, createNameMismatchReq)
		if createNameMismatchRec.Code != http.StatusBadRequest {
			t.Fatalf("org-scoped create cookbook_name mismatch status = %d, want %d, body = %s", createNameMismatchRec.Code, http.StatusBadRequest, createNameMismatchRec.Body.String())
		}
		assertCookbookErrorList(t, createNameMismatchRec.Body.Bytes(), []string{"Field 'name' invalid"})
		assertCookbookMissing(t, router, orgCookbookPath("demo", "1.2.3"))

		createOrgCookbookVersion(t, "demo", "1.2.3", "", nil)
		updatePayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
		updatePayload["cookbook_name"] = "other"
		updateReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("demo", "1.2.3"), mustMarshalSandboxJSON(t, updatePayload))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusBadRequest {
			t.Fatalf("org-scoped update cookbook_name mismatch status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
		}
		assertCookbookErrorList(t, updateRec.Body.Bytes(), []string{"Field 'cookbook_name' invalid"})
		assertCookbookDescription(t, router, orgCookbookPath("demo", "1.2.3"), "compatibility cookbook")
	})

	t.Run("top_level_field_validation", func(t *testing.T) {
		createOrgCookbookVersion(t, "top-level-demo", "1.2.3", "", nil)

		tests := []struct {
			name         string
			path         string
			payload      map[string]any
			wantErrors   []string
			verifyPath   string
			verifyExists bool
			wantDesc     string
		}{
			{
				name: "create rejects invalid json_class",
				path: orgCookbookPath("create-json-class", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("create-json-class", "1.2.3", "", nil)
					payload["json_class"] = "Chef::Role"
					return payload
				}(),
				wantErrors:   []string{"Field 'json_class' invalid"},
				verifyPath:   orgCookbookPath("create-json-class", "1.2.3"),
				verifyExists: false,
			},
			{
				name: "create rejects invalid chef_type",
				path: orgCookbookPath("create-chef-type", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("create-chef-type", "1.2.3", "", nil)
					payload["chef_type"] = "not_a_cookbook_version"
					return payload
				}(),
				wantErrors:   []string{"Field 'chef_type' invalid"},
				verifyPath:   orgCookbookPath("create-chef-type", "1.2.3"),
				verifyExists: false,
			},
			{
				name: "create rejects invalid version",
				path: orgCookbookPath("create-version", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("create-version", "1.2.3", "", nil)
					payload["version"] = "0.0"
					return payload
				}(),
				wantErrors:   []string{"Field 'version' invalid"},
				verifyPath:   orgCookbookPath("create-version", "1.2.3"),
				verifyExists: false,
			},
			{
				name: "create rejects invalid request key",
				path: orgCookbookPath("create-invalid-key", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("create-invalid-key", "1.2.3", "", nil)
					payload["bogus"] = "nope"
					return payload
				}(),
				wantErrors:   []string{"Invalid key bogus in request body"},
				verifyPath:   orgCookbookPath("create-invalid-key", "1.2.3"),
				verifyExists: false,
			},
			{
				name: "update rejects invalid json_class",
				path: orgCookbookPath("top-level-demo", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
					payload["json_class"] = "Chef::NonCookbook"
					payload["metadata"].(map[string]any)["description"] = "bad json_class"
					return payload
				}(),
				wantErrors:   []string{"Field 'json_class' invalid"},
				verifyPath:   orgCookbookPath("top-level-demo", "1.2.3"),
				verifyExists: true,
				wantDesc:     "compatibility cookbook",
			},
			{
				name: "update rejects invalid chef_type",
				path: orgCookbookPath("top-level-demo", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
					payload["chef_type"] = "not_cookbook"
					payload["metadata"].(map[string]any)["description"] = "bad chef_type"
					return payload
				}(),
				wantErrors:   []string{"Field 'chef_type' invalid"},
				verifyPath:   orgCookbookPath("top-level-demo", "1.2.3"),
				verifyExists: true,
				wantDesc:     "compatibility cookbook",
			},
			{
				name: "update rejects invalid version",
				path: orgCookbookPath("top-level-demo", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
					payload["version"] = "0.0"
					payload["metadata"].(map[string]any)["description"] = "bad version"
					return payload
				}(),
				wantErrors:   []string{"Field 'version' invalid"},
				verifyPath:   orgCookbookPath("top-level-demo", "1.2.3"),
				verifyExists: true,
				wantDesc:     "compatibility cookbook",
			},
			{
				name: "update rejects invalid request key",
				path: orgCookbookPath("top-level-demo", "1.2.3"),
				payload: func() map[string]any {
					payload := cookbookVersionPayload("top-level-demo", "1.2.3", "", nil)
					payload["bogus"] = []string{"still bad"}
					payload["metadata"].(map[string]any)["description"] = "bad bogus key"
					return payload
				}(),
				wantErrors:   []string{"Invalid key bogus in request body"},
				verifyPath:   orgCookbookPath("top-level-demo", "1.2.3"),
				verifyExists: true,
				wantDesc:     "compatibility cookbook",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req := newSignedJSONRequest(t, http.MethodPut, tt.path, mustMarshalSandboxJSON(t, tt.payload))
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("%s status = %d, want %d, body = %s", tt.path, rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertCookbookErrorList(t, rec.Body.Bytes(), tt.wantErrors)

				if !tt.verifyExists {
					assertCookbookMissing(t, router, tt.verifyPath)
					return
				}
				assertCookbookDescription(t, router, tt.verifyPath, tt.wantDesc)
			})
		}
	})

	t.Run("malformed_name_and_version_routes", func(t *testing.T) {
		tests := []struct {
			name       string
			method     string
			path       string
			body       []byte
			wantErrors []string
			verifyPath string
		}{
			{
				name:       "invalid cookbook name on create",
				method:     http.MethodPut,
				path:       orgCookbookPath("first@second", "1.2.3"),
				body:       mustMarshalSandboxJSON(t, cookbookVersionPayload("first@second", "1.2.3", "", nil)),
				wantErrors: []string{"Invalid cookbook name 'first@second' using regex: 'Malformed cookbook name. Must only contain A-Z, a-z, 0-9, _, . or -'."},
			},
			{
				name:       "negative cookbook version on create",
				method:     http.MethodPut,
				path:       orgCookbookPath("invalid-demo", "1.2.-42"),
				body:       mustMarshalSandboxJSON(t, cookbookVersionPayload("invalid-demo", "1.2.-42", "", nil)),
				wantErrors: []string{"Invalid cookbook version '1.2.-42'."},
				verifyPath: orgCookbookPath("invalid-demo"),
			},
			{
				name:       "alphabetic cookbook version on create",
				method:     http.MethodPut,
				path:       orgCookbookPath("invalid-demo", "abc"),
				body:       mustMarshalSandboxJSON(t, cookbookVersionPayload("invalid-demo", "abc", "", nil)),
				wantErrors: []string{"Invalid cookbook version 'abc'."},
				verifyPath: orgCookbookPath("invalid-demo"),
			},
			{
				name:       "negative cookbook version on read",
				method:     http.MethodGet,
				path:       orgCookbookPath("demo", "1.2.-42"),
				wantErrors: []string{"Invalid cookbook version '1.2.-42'."},
			},
			{
				name:       "negative cookbook version on delete",
				method:     http.MethodDelete,
				path:       orgCookbookPath("demo", "1.2.-42"),
				wantErrors: []string{"Invalid cookbook version '1.2.-42'."},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				req := newSignedJSONRequest(t, tt.method, tt.path, tt.body)
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("%s status = %d, want %d, body = %s", tt.path, rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertCookbookErrorList(t, rec.Body.Bytes(), tt.wantErrors)
				if tt.verifyPath != "" {
					assertCookbookMissing(t, router, tt.verifyPath)
				}
			})
		}
	})

	t.Run("invalid_metadata_shapes_keep_existing_version_intact", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'org metadata exactness'"))
		createOrgCookbookVersion(t, "meta-demo", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})

		cases := []struct {
			name    string
			mutate  func(map[string]any)
			message string
		}{
			{
				name: "platforms_nested_object",
				mutate: func(payload map[string]any) {
					payload["metadata"].(map[string]any)["platforms"] = map[string]any{"ubuntu": map[string]any{}}
				},
				message: "Invalid value '{[]}' for metadata.platforms",
			},
			{
				name: "dependencies_array",
				mutate: func(payload map[string]any) {
					payload["metadata"].(map[string]any)["dependencies"] = []any{"foo"}
				},
				message: "Field 'metadata.dependencies' invalid",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				payload := cookbookVersionPayload("meta-demo", "1.2.3", checksum, map[string]string{
					"apt": ">= 2.0.0",
				})
				payload["metadata"].(map[string]any)["description"] = "this should not persist"
				tc.mutate(payload)

				req := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("meta-demo", "1.2.3"), mustMarshalSandboxJSON(t, payload))
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("update status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertCookbookErrorList(t, rec.Body.Bytes(), []string{tc.message})
				assertCookbookDescription(t, router, orgCookbookPath("meta-demo", "1.2.3"), "compatibility cookbook")
			})
		}
	})

	t.Run("checksum_failures_do_not_mutate_existing_version", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello v1'"))
		createOrgCookbookVersion(t, "checksum-demo", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})

		updatePayload := cookbookVersionPayload("checksum-demo", "1.2.3", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", map[string]string{
			"apt": ">= 2.0.0",
		})
		updateReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("checksum-demo", "1.2.3"), mustMarshalSandboxJSON(t, updatePayload))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusBadRequest {
			t.Fatalf("update checksum status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
		}

		var updateErrorPayload map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateErrorPayload); err != nil {
			t.Fatalf("json.Unmarshal(update error) error = %v", err)
		}
		updateErrors := updateErrorPayload["error"].([]any)
		if len(updateErrors) != 1 || updateErrors[0] != "Manifest has checksum aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa but it hasn't yet been uploaded" {
			t.Fatalf("update errors = %v, want checksum-specific message", updateErrors)
		}
		getReq := newSignedJSONRequest(t, http.MethodGet, orgCookbookPath("checksum-demo", "1.2.3"), nil)
		getReq.Header.Set("X-Ops-Server-API-Version", "2")
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get cookbook status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}
		var getPayload map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
			t.Fatalf("json.Unmarshal(get after checksum failure) error = %v", err)
		}
		if got := getPayload["metadata"].(map[string]any)["dependencies"].(map[string]any)["apt"]; got != ">= 1.0.0" {
			t.Fatalf("dependencies.apt = %v, want original value after failed update", got)
		}

		firstChecksum := uploadCookbookChecksum(t, router, []byte("puts 'first body'"))
		secondChecksum := uploadCookbookChecksum(t, router, []byte("puts 'second body'"))
		thirdChecksum := uploadCookbookChecksum(t, router, []byte("puts 'third body'"))
		createPayload := cookbookVersionPayload("invalid-checksum-demo", "1.2.3", "", nil)
		createPayload["all_files"] = []any{
			cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
			cookbookFilePayload("files/default/second", "files/default/second", secondChecksum),
			cookbookFilePayload("files/default/third", "files/default/third", thirdChecksum),
		}
		createReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("invalid-checksum-demo", "1.2.3"), mustMarshalSandboxJSON(t, createPayload))
		createReq.Header.Set("X-Ops-Server-API-Version", "2")
		createRec := httptest.NewRecorder()
		router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusCreated {
			t.Fatalf("create invalid-checksum-demo status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
		}

		updateFilesPayload := cookbookVersionPayload("invalid-checksum-demo", "1.2.3", "", nil)
		updateFilesPayload["all_files"] = []any{
			cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
			cookbookFilePayload("files/default/missing", "files/default/missing", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		}
		updateFilesReq := newSignedJSONRequest(t, http.MethodPut, orgCookbookPath("invalid-checksum-demo", "1.2.3"), mustMarshalSandboxJSON(t, updateFilesPayload))
		updateFilesReq.Header.Set("X-Ops-Server-API-Version", "2")
		updateFilesRec := httptest.NewRecorder()
		router.ServeHTTP(updateFilesRec, updateFilesReq)
		if updateFilesRec.Code != http.StatusBadRequest {
			t.Fatalf("invalid checksum file update status = %d, want %d, body = %s", updateFilesRec.Code, http.StatusBadRequest, updateFilesRec.Body.String())
		}
		assertCookbookErrorList(t, updateFilesRec.Body.Bytes(), []string{"Manifest has checksum aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa but it hasn't yet been uploaded"})
		assertCookbookAllFilePaths(t, router, orgCookbookPath("invalid-checksum-demo", "1.2.3"), []string{
			"files/default/first",
			"files/default/second",
			"files/default/third",
		})
	})
}

func TestCookbookCollectionNumVersionsEdgeCases(t *testing.T) {
	router := newTestRouter(t)

	createCookbookVersion(t, router, "demo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "demo", "1.2.0", "", nil)
	createCookbookVersion(t, router, "other", "0.1.0", "", nil)

	zeroReq := httptest.NewRequest(http.MethodGet, "/cookbooks?num_versions=0", nil)
	applySignedHeaders(t, zeroReq, "silent-bob", "", http.MethodGet, "/cookbooks", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	zeroRec := httptest.NewRecorder()
	router.ServeHTTP(zeroRec, zeroReq)
	if zeroRec.Code != http.StatusOK {
		t.Fatalf("num_versions=0 status = %d, want %d, body = %s", zeroRec.Code, http.StatusOK, zeroRec.Body.String())
	}
	var zeroPayload map[string]any
	if err := json.Unmarshal(zeroRec.Body.Bytes(), &zeroPayload); err != nil {
		t.Fatalf("json.Unmarshal(num_versions=0) error = %v", err)
	}
	for _, cookbook := range []string{"demo", "other"} {
		versions := cookbookVersionListForName(t, zeroPayload, cookbook)
		if len(versions) != 0 {
			t.Fatalf("%s versions len = %d, want 0 (%v)", cookbook, len(versions), versions)
		}
	}

	for _, rawURL := range []string{"/cookbooks?num_versions=", "/cookbooks?num_versions=-1", "/cookbooks?num_versions=foo"} {
		req := httptest.NewRequest(http.MethodGet, rawURL, nil)
		applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/cookbooks", nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, "2026-04-02T15:04:05Z")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want %d, body = %s", rawURL, rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertCookbookErrorList(t, rec.Body.Bytes(), []string{"You have requested an invalid number of versions (x >= 0 || 'all')"})
	}
}

func TestCookbookLatestVersionNotFoundUsesChefErrorShape(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/missing/_latest", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing latest status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
	assertCookbookErrorList(t, rec.Body.Bytes(), []string{"Cannot find a cookbook named missing with version _latest"})
}

func TestCookbookVersionEndpointsHonorFrozenForce(t *testing.T) {
	router := newTestRouter(t)
	checksumV1 := uploadCookbookChecksum(t, router, []byte("puts 'hello v1'"))
	checksumV2 := uploadCookbookChecksum(t, router, []byte("puts 'hello v2'"))

	createCookbookVersion(t, router, "demo", "1.2.3", checksumV1, map[string]string{
		"apt": ">= 1.0.0",
	})

	freezePayload := cookbookVersionPayload("demo", "1.2.3", checksumV1, map[string]string{
		"apt": ">= 1.0.0",
	})
	freezePayload["frozen?"] = true
	freezeReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, freezePayload))
	freezeRec := httptest.NewRecorder()
	router.ServeHTTP(freezeRec, freezeReq)
	if freezeRec.Code != http.StatusOK {
		t.Fatalf("freeze cookbook status = %d, want %d, body = %s", freezeRec.Code, http.StatusOK, freezeRec.Body.String())
	}

	updatePayload := cookbookVersionPayload("demo", "1.2.3", checksumV2, map[string]string{
		"apt": ">= 2.0.0",
	})
	updatePayload["frozen?"] = false
	updatePayload["metadata"].(map[string]any)["description"] = "this is different"

	conflictReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	conflictRec := httptest.NewRecorder()
	router.ServeHTTP(conflictRec, conflictReq)
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("conflict cookbook status = %d, want %d, body = %s", conflictRec.Code, http.StatusConflict, conflictRec.Body.String())
	}
	var conflictPayload map[string]any
	if err := json.Unmarshal(conflictRec.Body.Bytes(), &conflictPayload); err != nil {
		t.Fatalf("json.Unmarshal(conflict) error = %v", err)
	}
	conflictErrors := conflictPayload["error"].([]any)
	if len(conflictErrors) != 1 || conflictErrors[0] != "The cookbook demo at version 1.2.3 is frozen. Use the 'force' option to override." {
		t.Fatalf("conflict errors = %v, want frozen message", conflictErrors)
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getReq.Header.Set("X-Ops-Server-API-Version", "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get frozen cookbook status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(frozen get) error = %v", err)
	}
	if getPayload["frozen?"] != true {
		t.Fatalf("frozen get payload = %v, want frozen true", getPayload["frozen?"])
	}
	if got := getPayload["metadata"].(map[string]any)["description"]; got == "this is different" {
		t.Fatalf("metadata.description = %v, want original value after conflict", got)
	}

	forceBody := mustMarshalSandboxJSON(t, updatePayload)
	forceReq := httptest.NewRequest(http.MethodPut, "/cookbooks/demo/1.2.3?force=true", bytes.NewReader(forceBody))
	applySignedHeaders(t, forceReq, "silent-bob", "", http.MethodPut, "/cookbooks/demo/1.2.3", forceBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	forceRec := httptest.NewRecorder()
	router.ServeHTTP(forceRec, forceReq)
	if forceRec.Code != http.StatusOK {
		t.Fatalf("force cookbook status = %d, want %d, body = %s", forceRec.Code, http.StatusOK, forceRec.Body.String())
	}
	var forcePayload map[string]any
	if err := json.Unmarshal(forceRec.Body.Bytes(), &forcePayload); err != nil {
		t.Fatalf("json.Unmarshal(force) error = %v", err)
	}
	if forcePayload["frozen?"] != true {
		t.Fatalf("force payload frozen = %v, want true", forcePayload["frozen?"])
	}

	forcedGetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	forcedGetReq.Header.Set("X-Ops-Server-API-Version", "2")
	forcedGetRec := httptest.NewRecorder()
	router.ServeHTTP(forcedGetRec, forcedGetReq)
	if forcedGetRec.Code != http.StatusOK {
		t.Fatalf("forced get cookbook status = %d, want %d, body = %s", forcedGetRec.Code, http.StatusOK, forcedGetRec.Body.String())
	}
	var forcedGetPayload map[string]any
	if err := json.Unmarshal(forcedGetRec.Body.Bytes(), &forcedGetPayload); err != nil {
		t.Fatalf("json.Unmarshal(forced get) error = %v", err)
	}
	if forcedGetPayload["frozen?"] != true {
		t.Fatalf("forced get payload frozen = %v, want true", forcedGetPayload["frozen?"])
	}
	if got := forcedGetPayload["metadata"].(map[string]any)["description"]; got != "this is different" {
		t.Fatalf("metadata.description = %v, want forced update", got)
	}

	forceFalseBody := mustMarshalSandboxJSON(t, updatePayload)
	forceFalseReq := httptest.NewRequest(http.MethodPut, "/cookbooks/demo/1.2.3?force=false", bytes.NewReader(forceFalseBody))
	applySignedHeaders(t, forceFalseReq, "silent-bob", "", http.MethodPut, "/cookbooks/demo/1.2.3", forceFalseBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	forceFalseRec := httptest.NewRecorder()
	router.ServeHTTP(forceFalseRec, forceFalseReq)
	if forceFalseRec.Code != http.StatusConflict {
		t.Fatalf("force=false cookbook status = %d, want %d, body = %s", forceFalseRec.Code, http.StatusConflict, forceFalseRec.Body.String())
	}
	assertCookbookErrorList(t, forceFalseRec.Body.Bytes(), []string{"The cookbook demo at version 1.2.3 is frozen. Use the 'force' option to override."})

	afterForceFalseGetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	afterForceFalseGetReq.Header.Set("X-Ops-Server-API-Version", "2")
	afterForceFalseGetRec := httptest.NewRecorder()
	router.ServeHTTP(afterForceFalseGetRec, afterForceFalseGetReq)
	if afterForceFalseGetRec.Code != http.StatusOK {
		t.Fatalf("get after force=false status = %d, want %d, body = %s", afterForceFalseGetRec.Code, http.StatusOK, afterForceFalseGetRec.Body.String())
	}
	var afterForceFalsePayload map[string]any
	if err := json.Unmarshal(afterForceFalseGetRec.Body.Bytes(), &afterForceFalsePayload); err != nil {
		t.Fatalf("json.Unmarshal(get after force=false) error = %v", err)
	}
	if got := afterForceFalsePayload["metadata"].(map[string]any)["description"]; got != "this is different" {
		t.Fatalf("metadata.description after force=false = %v, want forced update to remain intact", got)
	}
	if afterForceFalsePayload["frozen?"] != true {
		t.Fatalf("frozen after force=false = %v, want true", afterForceFalsePayload["frozen?"])
	}
}

func TestCookbookVersionWriteResponsePreservesOptionalFieldOmissions(t *testing.T) {
	router := newTestRouter(t)

	t.Run("v0", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'exact write response v0'"))

		createCookbookVersion(t, router, "demo-v0", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})

		updatePayload := cookbookVersionPayload("demo-v0", "1.2.3", checksum, map[string]string{
			"apt": ">= 2.0.0",
		})
		delete(updatePayload, "all_files")
		delete(updatePayload, "version")
		delete(updatePayload, "json_class")
		delete(updatePayload, "chef_type")

		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo-v0/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		var updateResponse map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
			t.Fatalf("json.Unmarshal(update response) error = %v", err)
		}
		if !reflect.DeepEqual(updateResponse, updatePayload) {
			t.Fatalf("update response = %#v, want %#v", updateResponse, updatePayload)
		}
		if _, ok := updateResponse["version"]; ok {
			t.Fatalf("update response unexpectedly included version: %v", updateResponse)
		}
		if _, ok := updateResponse["json_class"]; ok {
			t.Fatalf("update response unexpectedly included json_class: %v", updateResponse)
		}
		if _, ok := updateResponse["chef_type"]; ok {
			t.Fatalf("update response unexpectedly included chef_type: %v", updateResponse)
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo-v0/1.2.3", nil)
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}
		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if getResponse["version"] != "1.2.3" {
			t.Fatalf("get response version = %v, want %q", getResponse["version"], "1.2.3")
		}
		if getResponse["json_class"] != "Chef::CookbookVersion" {
			t.Fatalf("get response json_class = %v, want %q", getResponse["json_class"], "Chef::CookbookVersion")
		}
		if getResponse["chef_type"] != "cookbook_version" {
			t.Fatalf("get response chef_type = %v, want %q", getResponse["chef_type"], "cookbook_version")
		}
	})

	t.Run("v2", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'exact write response v2'"))

		createPayload := cookbookVersionPayload("demo-v2", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})
		delete(createPayload, "recipes")
		createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo-v2/1.2.3", mustMarshalSandboxJSON(t, createPayload))
		createReq.Header.Set("X-Ops-Server-API-Version", "2")
		createRec := httptest.NewRecorder()
		router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
		}

		updatePayload := cookbookVersionPayload("demo-v2", "1.2.3", checksum, map[string]string{
			"apt": ">= 2.0.0",
		})
		delete(updatePayload, "recipes")
		delete(updatePayload, "version")
		delete(updatePayload, "json_class")
		delete(updatePayload, "chef_type")

		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo-v2/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
		updateReq.Header.Set("X-Ops-Server-API-Version", "2")
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		var updateResponse map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
			t.Fatalf("json.Unmarshal(update response) error = %v", err)
		}
		if !reflect.DeepEqual(updateResponse, updatePayload) {
			t.Fatalf("update response = %#v, want %#v", updateResponse, updatePayload)
		}
		if _, ok := updateResponse["version"]; ok {
			t.Fatalf("update response unexpectedly included version: %v", updateResponse)
		}
		if _, ok := updateResponse["json_class"]; ok {
			t.Fatalf("update response unexpectedly included json_class: %v", updateResponse)
		}
		if _, ok := updateResponse["chef_type"]; ok {
			t.Fatalf("update response unexpectedly included chef_type: %v", updateResponse)
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo-v2/1.2.3", nil)
		getReq.Header.Set("X-Ops-Server-API-Version", "2")
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}
		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if getResponse["version"] != "1.2.3" {
			t.Fatalf("get response version = %v, want %q", getResponse["version"], "1.2.3")
		}
		if getResponse["json_class"] != "Chef::CookbookVersion" {
			t.Fatalf("get response json_class = %v, want %q", getResponse["json_class"], "Chef::CookbookVersion")
		}
		if getResponse["chef_type"] != "cookbook_version" {
			t.Fatalf("get response chef_type = %v, want %q", getResponse["chef_type"], "cookbook_version")
		}
	})
}

func TestCookbookVersionWriteResponsesPreserveFileCollectionShape(t *testing.T) {
	router := newTestRouter(t)

	t.Run("v0_delete_all_files_omits_segment", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'legacy files'"))

		createPayload := cookbookVersionPayload("legacy", "1.2.3", "", nil)
		delete(createPayload, "all_files")
		createPayload["files"] = []any{
			cookbookFilePayload("config", "files/default/config", checksum),
		}
		createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/legacy/1.2.3", mustMarshalSandboxJSON(t, createPayload))
		createRec := httptest.NewRecorder()
		router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
		}

		updatePayload := cookbookVersionPayload("legacy", "1.2.3", "", nil)
		delete(updatePayload, "all_files")
		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/legacy/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		var updateResponse map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
			t.Fatalf("json.Unmarshal(update response) error = %v", err)
		}
		if !reflect.DeepEqual(updateResponse, updatePayload) {
			t.Fatalf("update response = %#v, want %#v", updateResponse, updatePayload)
		}
		if _, ok := updateResponse["files"]; ok {
			t.Fatalf("update response unexpectedly included files: %v", updateResponse)
		}
	})

	t.Run("v0_explicit_empty_files_are_retained", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'legacy files explicit empty'"))

		createPayload := cookbookVersionPayload("legacy-empty", "1.2.3", "", nil)
		delete(createPayload, "all_files")
		createPayload["files"] = []any{
			cookbookFilePayload("config", "files/default/config", checksum),
		}
		createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/legacy-empty/1.2.3", mustMarshalSandboxJSON(t, createPayload))
		createRec := httptest.NewRecorder()
		router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
		}

		updatePayload := cookbookVersionPayload("legacy-empty", "1.2.3", "", nil)
		delete(updatePayload, "all_files")
		updatePayload["files"] = []any{}
		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/legacy-empty/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		var updateResponse map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
			t.Fatalf("json.Unmarshal(update response) error = %v", err)
		}
		if !reflect.DeepEqual(updateResponse, updatePayload) {
			t.Fatalf("update response = %#v, want %#v", updateResponse, updatePayload)
		}
	})

	t.Run("v2_delete_all_files_omits_all_files", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'all files'"))

		createPayload := cookbookVersionPayload("modern", "1.2.3", checksum, nil)
		delete(createPayload, "recipes")
		createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/modern/1.2.3", mustMarshalSandboxJSON(t, createPayload))
		createReq.Header.Set("X-Ops-Server-API-Version", "2")
		createRec := httptest.NewRecorder()
		router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
		}

		updatePayload := cookbookVersionPayload("modern", "1.2.3", "", nil)
		delete(updatePayload, "recipes")
		delete(updatePayload, "all_files")
		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/modern/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
		updateReq.Header.Set("X-Ops-Server-API-Version", "2")
		updateRec := httptest.NewRecorder()
		router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
		}

		var updateResponse map[string]any
		if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
			t.Fatalf("json.Unmarshal(update response) error = %v", err)
		}
		if !reflect.DeepEqual(updateResponse, updatePayload) {
			t.Fatalf("update response = %#v, want %#v", updateResponse, updatePayload)
		}
		if _, ok := updateResponse["all_files"]; ok {
			t.Fatalf("update response unexpectedly included all_files: %v", updateResponse)
		}
	})
}

func TestCookbookVersionCreateResponsePreservesOptionalFieldOmissions(t *testing.T) {
	router := newTestRouter(t)

	t.Run("v0", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'create exactness v0'"))

		payload := cookbookVersionPayload("create-v0", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})
		delete(payload, "all_files")
		delete(payload, "version")
		delete(payload, "json_class")
		delete(payload, "chef_type")

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/create-v0/1.2.3", mustMarshalSandboxJSON(t, payload))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
		}

		var createResponse map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &createResponse); err != nil {
			t.Fatalf("json.Unmarshal(create response) error = %v", err)
		}
		if !reflect.DeepEqual(createResponse, payload) {
			t.Fatalf("create response = %#v, want %#v", createResponse, payload)
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/create-v0/1.2.3", nil)
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if getResponse["version"] != "1.2.3" {
			t.Fatalf("get version = %v, want %q", getResponse["version"], "1.2.3")
		}
		if getResponse["json_class"] != "Chef::CookbookVersion" {
			t.Fatalf("get json_class = %v, want %q", getResponse["json_class"], "Chef::CookbookVersion")
		}
		if getResponse["chef_type"] != "cookbook_version" {
			t.Fatalf("get chef_type = %v, want %q", getResponse["chef_type"], "cookbook_version")
		}
	})

	t.Run("v2", func(t *testing.T) {
		checksum := uploadCookbookChecksum(t, router, []byte("puts 'create exactness v2'"))

		payload := cookbookVersionPayload("create-v2", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})
		delete(payload, "recipes")
		delete(payload, "version")
		delete(payload, "json_class")
		delete(payload, "chef_type")

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/create-v2/1.2.3", mustMarshalSandboxJSON(t, payload))
		req.Header.Set("X-Ops-Server-API-Version", "2")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
		}

		var createResponse map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &createResponse); err != nil {
			t.Fatalf("json.Unmarshal(create response) error = %v", err)
		}
		if !reflect.DeepEqual(createResponse, payload) {
			t.Fatalf("create response = %#v, want %#v", createResponse, payload)
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/create-v2/1.2.3", nil)
		getReq.Header.Set("X-Ops-Server-API-Version", "2")
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if getResponse["version"] != "1.2.3" {
			t.Fatalf("get version = %v, want %q", getResponse["version"], "1.2.3")
		}
		if getResponse["json_class"] != "Chef::CookbookVersion" {
			t.Fatalf("get json_class = %v, want %q", getResponse["json_class"], "Chef::CookbookVersion")
		}
		if getResponse["chef_type"] != "cookbook_version" {
			t.Fatalf("get chef_type = %v, want %q", getResponse["chef_type"], "cookbook_version")
		}
	})
}

func TestCookbookVersionMetadataMutationHTTPParity(t *testing.T) {
	router := newTestRouter(t)
	checksum := uploadCookbookChecksum(t, router, []byte("puts 'metadata exactness'"))

	createCookbookVersion(t, router, "meta-demo", "1.2.3", checksum, map[string]string{
		"apt": ">= 1.0.0",
	})

	t.Run("metadata_name_delete_is_exact_on_write_but_canonical_on_read", func(t *testing.T) {
		payload := cookbookVersionPayload("meta-demo", "1.2.3", checksum, map[string]string{
			"apt": ">= 1.0.0",
		})
		delete(payload, "all_files")
		delete(payload["metadata"].(map[string]any), "name")

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/meta-demo/1.2.3", mustMarshalSandboxJSON(t, payload))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var putResponse map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &putResponse); err != nil {
			t.Fatalf("json.Unmarshal(put response) error = %v", err)
		}
		if !reflect.DeepEqual(putResponse, payload) {
			t.Fatalf("put response = %#v, want %#v", putResponse, payload)
		}
		if _, ok := putResponse["metadata"].(map[string]any)["name"]; ok {
			t.Fatalf("put response unexpectedly included metadata.name: %v", putResponse["metadata"])
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/meta-demo/1.2.3", nil)
		getReq.Header.Set("X-Ops-Server-API-Version", "2")
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if got := getResponse["metadata"].(map[string]any)["name"]; got != "meta-demo" {
			t.Fatalf("get metadata.name = %v, want %q", got, "meta-demo")
		}
	})

	t.Run("metadata_providing_is_exact_on_write_but_filtered_on_read", func(t *testing.T) {
		providing := map[string]any{
			"cats::sleep":                "0.0.1",
			"here(:kitty, :time_to_eat)": "0.0.1",
		}
		payload := cookbookVersionPayload("meta-demo", "1.2.3", checksum, nil)
		delete(payload, "all_files")
		payload["metadata"].(map[string]any)["providing"] = providing

		req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/meta-demo/1.2.3", mustMarshalSandboxJSON(t, payload))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("update status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var putResponse map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &putResponse); err != nil {
			t.Fatalf("json.Unmarshal(put response) error = %v", err)
		}
		if !reflect.DeepEqual(putResponse, payload) {
			t.Fatalf("put response = %#v, want %#v", putResponse, payload)
		}

		getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/meta-demo/1.2.3", nil)
		getReq.Header.Set("X-Ops-Server-API-Version", "2")
		getRec := httptest.NewRecorder()
		router.ServeHTTP(getRec, getReq)
		if getRec.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
		}

		var getResponse map[string]any
		if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
			t.Fatalf("json.Unmarshal(get response) error = %v", err)
		}
		if _, ok := getResponse["metadata"].(map[string]any)["providing"]; ok {
			t.Fatalf("get response unexpectedly included metadata.providing: %v", getResponse["metadata"])
		}
	})

	t.Run("invalid_metadata_shapes_keep_existing_version_intact", func(t *testing.T) {
		cases := []struct {
			name    string
			mutate  func(map[string]any)
			message string
		}{
			{
				name: "platforms_nested_object",
				mutate: func(payload map[string]any) {
					payload["metadata"].(map[string]any)["platforms"] = map[string]any{"ubuntu": map[string]any{}}
				},
				message: "Invalid value '{[]}' for metadata.platforms",
			},
			{
				name: "dependencies_array",
				mutate: func(payload map[string]any) {
					payload["metadata"].(map[string]any)["dependencies"] = []any{"foo"}
				},
				message: "Field 'metadata.dependencies' invalid",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				payload := cookbookVersionPayload("meta-demo", "1.2.3", checksum, map[string]string{
					"apt": ">= 2.0.0",
				})
				payload["metadata"].(map[string]any)["description"] = "this should not persist"
				tc.mutate(payload)

				req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/meta-demo/1.2.3", mustMarshalSandboxJSON(t, payload))
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if rec.Code != http.StatusBadRequest {
					t.Fatalf("update status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
				}
				assertCookbookErrorList(t, rec.Body.Bytes(), []string{tc.message})

				getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/meta-demo/1.2.3", nil)
				getReq.Header.Set("X-Ops-Server-API-Version", "2")
				getRec := httptest.NewRecorder()
				router.ServeHTTP(getRec, getReq)
				if getRec.Code != http.StatusOK {
					t.Fatalf("get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
				}

				var getResponse map[string]any
				if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
					t.Fatalf("json.Unmarshal(get response) error = %v", err)
				}
				if got := getResponse["metadata"].(map[string]any)["description"]; got == "this should not persist" {
					t.Fatalf("metadata.description = %v, want original persisted value after validation failure", got)
				}
			})
		}
	})
}

func TestCookbookVersionEndpointsCleanUpReleasedChecksums(t *testing.T) {
	router := newTestRouter(t)
	oldChecksum := uploadCookbookChecksum(t, router, []byte("puts 'old body'"))
	newChecksum := uploadCookbookChecksum(t, router, []byte("puts 'new body'"))

	createCookbookVersion(t, router, "cleanup-demo", "1.2.3", oldChecksum, nil)

	oldURL := cookbookFileURL(t, router, "/cookbooks/cleanup-demo/1.2.3")

	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/cleanup-demo/1.2.3", mustMarshalSandboxJSON(t, cookbookVersionPayload("cleanup-demo", "1.2.3", newChecksum, nil)))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	assertBlobDownloadStatus(t, router, oldURL, http.StatusNotFound)

	newURL := cookbookFileURL(t, router, "/cookbooks/cleanup-demo/1.2.3")
	assertBlobDownloadStatus(t, router, newURL, http.StatusOK)

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/cleanup-demo/1.2.3", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	assertBlobDownloadStatus(t, router, newURL, http.StatusNotFound)
}

func TestCookbookVersionEndpointsPreserveSharedChecksumsAcrossVersions(t *testing.T) {
	router := newTestRouter(t)
	sharedChecksum := uploadCookbookChecksum(t, router, []byte("puts 'shared body'"))
	uniqueV1Checksum := uploadCookbookChecksum(t, router, []byte("puts 'version one body'"))
	uniqueV2Checksum := uploadCookbookChecksum(t, router, []byte("puts 'version two body'"))
	replacementChecksum := uploadCookbookChecksum(t, router, []byte("puts 'replacement body'"))

	createCookbookVersionWithFiles(t, router, "demo", "1.2.3", []map[string]any{
		cookbookFilePayload("files/default/shared", "files/default/shared", sharedChecksum),
		cookbookFilePayload("files/default/one", "files/default/one", uniqueV1Checksum),
	})
	createCookbookVersionWithFiles(t, router, "demo", "1.2.4", []map[string]any{
		cookbookFilePayload("files/default/shared", "files/default/shared", sharedChecksum),
		cookbookFilePayload("files/default/two", "files/default/two", uniqueV2Checksum),
	})

	sharedURL := cookbookBlobURLByPath(t, router, "/cookbooks/demo/1.2.3", "files/default/shared")
	uniqueV2URL := cookbookBlobURLByPath(t, router, "/cookbooks/demo/1.2.4", "files/default/two")

	updatePayload := cookbookVersionPayload("demo", "1.2.4", "", nil)
	updatePayload["all_files"] = []any{
		cookbookFilePayload("files/default/replacement", "files/default/replacement", replacementChecksum),
	}
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.4", mustMarshalSandboxJSON(t, updatePayload))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	assertBlobDownloadStatus(t, router, uniqueV2URL, http.StatusNotFound)
	assertBlobDownloadStatus(t, router, sharedURL, http.StatusOK)
}

func TestCookbookVersionEndpointsDeleteAllFilesOnUpdate(t *testing.T) {
	router := newTestRouter(t)
	firstChecksum := uploadCookbookChecksum(t, router, []byte("puts 'first body'"))
	secondChecksum := uploadCookbookChecksum(t, router, []byte("puts 'second body'"))

	createCookbookVersionWithFiles(t, router, "delete-all-demo", "1.2.3", []map[string]any{
		cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
		cookbookFilePayload("files/default/second", "files/default/second", secondChecksum),
	})

	firstURL := cookbookBlobURLByPath(t, router, "/cookbooks/delete-all-demo/1.2.3", "files/default/first")
	secondURL := cookbookBlobURLByPath(t, router, "/cookbooks/delete-all-demo/1.2.3", "files/default/second")

	updatePayload := cookbookVersionPayload("delete-all-demo", "1.2.3", "", nil)
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/delete-all-demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateReq.Header.Set("X-Ops-Server-API-Version", "2")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("delete all files update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	assertCookbookAllFilePaths(t, router, "/cookbooks/delete-all-demo/1.2.3", nil)
	assertBlobDownloadStatus(t, router, firstURL, http.StatusNotFound)
	assertBlobDownloadStatus(t, router, secondURL, http.StatusNotFound)
}

func TestCookbookVersionEndpointsDeleteSomeFilesOnUpdate(t *testing.T) {
	router := newTestRouter(t)
	firstChecksum := uploadCookbookChecksum(t, router, []byte("puts 'first body'"))
	secondChecksum := uploadCookbookChecksum(t, router, []byte("puts 'second body'"))
	thirdChecksum := uploadCookbookChecksum(t, router, []byte("puts 'third body'"))

	createCookbookVersionWithFiles(t, router, "delete-some-demo", "1.2.3", []map[string]any{
		cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
		cookbookFilePayload("files/default/second", "files/default/second", secondChecksum),
		cookbookFilePayload("files/default/third", "files/default/third", thirdChecksum),
	})

	firstURL := cookbookBlobURLByPath(t, router, "/cookbooks/delete-some-demo/1.2.3", "files/default/first")
	secondURL := cookbookBlobURLByPath(t, router, "/cookbooks/delete-some-demo/1.2.3", "files/default/second")
	thirdURL := cookbookBlobURLByPath(t, router, "/cookbooks/delete-some-demo/1.2.3", "files/default/third")

	updatePayload := cookbookVersionPayload("delete-some-demo", "1.2.3", "", nil)
	updatePayload["all_files"] = []any{
		cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
		cookbookFilePayload("files/default/second", "files/default/second", secondChecksum),
	}
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/delete-some-demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateReq.Header.Set("X-Ops-Server-API-Version", "2")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("delete some files update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	assertCookbookAllFilePaths(t, router, "/cookbooks/delete-some-demo/1.2.3", []string{
		"files/default/first",
		"files/default/second",
	})
	assertBlobDownloadStatus(t, router, firstURL, http.StatusOK)
	assertBlobDownloadStatus(t, router, secondURL, http.StatusOK)
	assertBlobDownloadStatus(t, router, thirdURL, http.StatusNotFound)
}

func TestCookbookVersionEndpointsRejectInvalidChecksumFileUpdateWithoutMutation(t *testing.T) {
	router := newTestRouter(t)
	firstChecksum := uploadCookbookChecksum(t, router, []byte("puts 'first body'"))
	secondChecksum := uploadCookbookChecksum(t, router, []byte("puts 'second body'"))
	thirdChecksum := uploadCookbookChecksum(t, router, []byte("puts 'third body'"))

	createCookbookVersionWithFiles(t, router, "invalid-checksum-demo", "1.2.3", []map[string]any{
		cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
		cookbookFilePayload("files/default/second", "files/default/second", secondChecksum),
		cookbookFilePayload("files/default/third", "files/default/third", thirdChecksum),
	})

	firstURL := cookbookBlobURLByPath(t, router, "/cookbooks/invalid-checksum-demo/1.2.3", "files/default/first")
	secondURL := cookbookBlobURLByPath(t, router, "/cookbooks/invalid-checksum-demo/1.2.3", "files/default/second")
	thirdURL := cookbookBlobURLByPath(t, router, "/cookbooks/invalid-checksum-demo/1.2.3", "files/default/third")

	updatePayload := cookbookVersionPayload("invalid-checksum-demo", "1.2.3", "", nil)
	updatePayload["all_files"] = []any{
		cookbookFilePayload("files/default/first", "files/default/first", firstChecksum),
		cookbookFilePayload("files/default/missing", "files/default/missing", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
	}
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/invalid-checksum-demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateReq.Header.Set("X-Ops-Server-API-Version", "2")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid checksum update status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}
	assertCookbookErrorList(t, updateRec.Body.Bytes(), []string{"Manifest has checksum aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa but it hasn't yet been uploaded"})

	assertCookbookAllFilePaths(t, router, "/cookbooks/invalid-checksum-demo/1.2.3", []string{
		"files/default/first",
		"files/default/second",
		"files/default/third",
	})
	assertBlobDownloadStatus(t, router, firstURL, http.StatusOK)
	assertBlobDownloadStatus(t, router, secondURL, http.StatusOK)
	assertBlobDownloadStatus(t, router, thirdURL, http.StatusOK)
}

func TestCookbookArtifactDeleteCleanupPreservesSharedChecksums(t *testing.T) {
	router := newTestRouter(t)
	sharedChecksum := uploadCookbookChecksum(t, router, []byte("puts 'shared body'"))
	uniqueChecksum := uploadCookbookChecksum(t, router, []byte("puts 'unique body'"))

	createCookbookVersion(t, router, "shared-demo", "1.0.0", sharedChecksum, nil)
	createCookbookArtifact(t, router, "shared-artifact", "1111111111111111111111111111111111111111", "1.0.0", sharedChecksum, nil)
	createCookbookArtifact(t, router, "unique-artifact", "2222222222222222222222222222222222222222", "1.0.0", uniqueChecksum, nil)

	sharedURL := cookbookFileURL(t, router, "/cookbooks/shared-demo/1.0.0")
	uniqueURL := cookbookArtifactFileURL(t, router, "/cookbook_artifacts/unique-artifact/2222222222222222222222222222222222222222")

	deleteUniqueReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/unique-artifact/2222222222222222222222222222222222222222", nil)
	deleteUniqueRec := httptest.NewRecorder()
	router.ServeHTTP(deleteUniqueRec, deleteUniqueReq)
	if deleteUniqueRec.Code != http.StatusOK {
		t.Fatalf("delete unique artifact status = %d, want %d, body = %s", deleteUniqueRec.Code, http.StatusOK, deleteUniqueRec.Body.String())
	}
	assertBlobDownloadStatus(t, router, uniqueURL, http.StatusNotFound)

	deleteSharedArtifactReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/shared-artifact/1111111111111111111111111111111111111111", nil)
	deleteSharedArtifactRec := httptest.NewRecorder()
	router.ServeHTTP(deleteSharedArtifactRec, deleteSharedArtifactReq)
	if deleteSharedArtifactRec.Code != http.StatusOK {
		t.Fatalf("delete shared artifact status = %d, want %d, body = %s", deleteSharedArtifactRec.Code, http.StatusOK, deleteSharedArtifactRec.Body.String())
	}
	assertBlobDownloadStatus(t, router, sharedURL, http.StatusOK)

	deleteCookbookReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/shared-demo/1.0.0", nil)
	deleteCookbookRec := httptest.NewRecorder()
	router.ServeHTTP(deleteCookbookRec, deleteCookbookReq)
	if deleteCookbookRec.Code != http.StatusOK {
		t.Fatalf("delete shared cookbook status = %d, want %d, body = %s", deleteCookbookRec.Code, http.StatusOK, deleteCookbookRec.Body.String())
	}
	assertBlobDownloadStatus(t, router, sharedURL, http.StatusNotFound)
}

func TestCookbookVersionEndpointsDoNotDeleteExistingVersionOnWrongDelete(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "demo", "1.2.3", "", nil)

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/demo/9.9.9", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusNotFound {
		t.Fatalf("delete wrong version status = %d, want %d, body = %s", deleteRec.Code, http.StatusNotFound, deleteRec.Body.String())
	}
	assertCookbookErrorList(t, deleteRec.Body.Bytes(), []string{"Cannot find a cookbook named demo with version 9.9.9"})

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get existing version after failed delete status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestCookbookVersionEndpointAuthorizationMatchesChefShapes(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "demo", "1.2.3", "", nil)

	outsideGetReq := httptest.NewRequest(http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	applySignedHeaders(t, outsideGetReq, "outside-user", "", http.MethodGet, "/cookbooks/demo/1.2.3", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	outsideGetRec := httptest.NewRecorder()
	router.ServeHTTP(outsideGetRec, outsideGetReq)
	if outsideGetRec.Code != http.StatusForbidden {
		t.Fatalf("outside user get status = %d, want %d, body = %s", outsideGetRec.Code, http.StatusForbidden, outsideGetRec.Body.String())
	}

	invalidDeleteReq := httptest.NewRequest(http.MethodDelete, "/cookbooks/demo/1.2.3", nil)
	applySignedHeaders(t, invalidDeleteReq, "invalid-user", "", http.MethodDelete, "/cookbooks/demo/1.2.3", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	invalidDeleteRec := httptest.NewRecorder()
	router.ServeHTTP(invalidDeleteRec, invalidDeleteReq)
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get existing version after auth failures status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestCookbookVersionEndpointAllowsNormalUserReadAndSignedRecipeDownload(t *testing.T) {
	router := newTestRouter(t)
	checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello normal user'"))
	createCookbookVersion(t, router, "demo", "1.2.3", checksum, nil)

	getReq := newSignedJSONRequestAs(t, "normal-user", http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("normal user get status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(normal user cookbook get) error = %v", err)
	}
	rawRecipes, ok := payload["recipes"].([]any)
	if !ok || len(rawRecipes) != 1 {
		t.Fatalf("recipes = %T/%v, want single recipe entry", payload["recipes"], payload["recipes"])
	}
	recipe, ok := rawRecipes[0].(map[string]any)
	if !ok {
		t.Fatalf("recipe entry = %T, want map[string]any", rawRecipes[0])
	}
	downloadURL, ok := recipe["url"].(string)
	if !ok || downloadURL == "" {
		t.Fatalf("recipe url = %T/%v, want non-empty string", recipe["url"], recipe["url"])
	}

	downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	downloadRec := httptest.NewRecorder()
	router.ServeHTTP(downloadRec, downloadReq)
	if downloadRec.Code != http.StatusOK {
		t.Fatalf("recipe download status = %d, want %d, body = %s", downloadRec.Code, http.StatusOK, downloadRec.Body.String())
	}
	if downloadRec.Body.String() != "puts 'hello normal user'" {
		t.Fatalf("recipe download body = %q, want recipe contents", downloadRec.Body.String())
	}
}

func TestCookbookVersionEndpointAllowsNormalUserDelete(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "demo", "1.2.3", "", nil)

	deleteReq := newSignedJSONRequestAs(t, "normal-user", http.MethodDelete, "/cookbooks/demo/1.2.3", nil)
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("normal user delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("get after normal user delete status = %d, want %d, body = %s", getRec.Code, http.StatusNotFound, getRec.Body.String())
	}
}

func TestCookbookVersionEndpointAllowsNormalUserUpdateAndRejectsUnauthorizedMutation(t *testing.T) {
	router := newTestRouter(t)

	createCookbookVersion(t, router, "demo", "1.2.3", "", nil)

	normalPayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
	normalMetadata := normalPayload["metadata"].(map[string]any)
	normalMetadata["description"] = "updated by normal-user"
	normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, normalPayload))
	normalRec := httptest.NewRecorder()
	router.ServeHTTP(normalRec, normalReq)
	if normalRec.Code != http.StatusOK {
		t.Fatalf("normal user update status = %d, want %d, body = %s", normalRec.Code, http.StatusOK, normalRec.Body.String())
	}
	assertCookbookDescription(t, router, "/cookbooks/demo/1.2.3", "updated by normal-user")

	outsidePayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
	outsideMetadata := outsidePayload["metadata"].(map[string]any)
	outsideMetadata["description"] = "outside user attempted update"
	outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, outsidePayload))
	outsideRec := httptest.NewRecorder()
	router.ServeHTTP(outsideRec, outsideReq)
	if outsideRec.Code != http.StatusForbidden {
		t.Fatalf("outside user update status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
	}
	assertCookbookDescription(t, router, "/cookbooks/demo/1.2.3", "updated by normal-user")

	invalidPayload := cookbookVersionPayload("demo", "1.2.3", "", nil)
	invalidMetadata := invalidPayload["metadata"].(map[string]any)
	invalidMetadata["description"] = "invalid user attempted update"
	invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, invalidPayload))
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user update status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
	}
	assertCookbookDescription(t, router, "/cookbooks/demo/1.2.3", "updated by normal-user")
}

func TestCookbookVersionEndpointAllowsNormalUserCreateAndRejectsUnauthorizedCreate(t *testing.T) {
	router := newTestRouter(t)

	normalPayload := cookbookVersionPayload("created-by-user", "1.2.3", "", nil)
	normalMetadata := normalPayload["metadata"].(map[string]any)
	normalMetadata["description"] = "created by normal-user"
	normalPayload["metadata"] = normalMetadata
	normalReq := newSignedJSONRequestAs(t, "normal-user", http.MethodPut, "/cookbooks/created-by-user/1.2.3", mustMarshalSandboxJSON(t, normalPayload))
	normalRec := httptest.NewRecorder()
	router.ServeHTTP(normalRec, normalReq)
	if normalRec.Code != http.StatusCreated {
		t.Fatalf("normal user create status = %d, want %d, body = %s", normalRec.Code, http.StatusCreated, normalRec.Body.String())
	}
	assertCookbookDescription(t, router, "/cookbooks/created-by-user/1.2.3", "created by normal-user")

	outsidePayload := cookbookVersionPayload("blocked-by-outside", "1.2.3", "", nil)
	outsideMetadata := outsidePayload["metadata"].(map[string]any)
	outsideMetadata["description"] = "outside user attempted create"
	outsidePayload["metadata"] = outsideMetadata
	outsideReq := newSignedJSONRequestAs(t, "outside-user", http.MethodPut, "/cookbooks/blocked-by-outside/1.2.3", mustMarshalSandboxJSON(t, outsidePayload))
	outsideRec := httptest.NewRecorder()
	router.ServeHTTP(outsideRec, outsideReq)
	if outsideRec.Code != http.StatusForbidden {
		t.Fatalf("outside user create status = %d, want %d, body = %s", outsideRec.Code, http.StatusForbidden, outsideRec.Body.String())
	}
	assertCookbookMissing(t, router, "/cookbooks/blocked-by-outside/1.2.3")

	invalidPayload := cookbookVersionPayload("blocked-by-invalid", "1.2.3", "", nil)
	invalidMetadata := invalidPayload["metadata"].(map[string]any)
	invalidMetadata["description"] = "invalid user attempted create"
	invalidPayload["metadata"] = invalidMetadata
	invalidReq := newSignedJSONRequestAs(t, "invalid-user", http.MethodPut, "/cookbooks/blocked-by-invalid/1.2.3", mustMarshalSandboxJSON(t, invalidPayload))
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid user create status = %d, want %d, body = %s", invalidRec.Code, http.StatusUnauthorized, invalidRec.Body.String())
	}
	assertCookbookMissing(t, router, "/cookbooks/blocked-by-invalid/1.2.3")
}

func TestCookbookVersionEndpointsUseChecksumSpecificUpdateError(t *testing.T) {
	router := newTestRouter(t)
	checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello v1'"))

	createCookbookVersion(t, router, "demo", "1.2.3", checksum, map[string]string{
		"apt": ">= 1.0.0",
	})

	updatePayload := cookbookVersionPayload("demo", "1.2.3", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", map[string]string{
		"apt": ">= 2.0.0",
	})
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusBadRequest, updateRec.Body.String())
	}

	var updateErrorPayload map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updateErrorPayload); err != nil {
		t.Fatalf("json.Unmarshal(update error) error = %v", err)
	}
	updateErrors := updateErrorPayload["error"].([]any)
	if len(updateErrors) != 1 || updateErrors[0] != "Manifest has checksum aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa but it hasn't yet been uploaded" {
		t.Fatalf("update errors = %v, want checksum-specific message", updateErrors)
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getReq.Header.Set("X-Ops-Server-API-Version", "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get cookbook status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get after checksum failure) error = %v", err)
	}
	if got := getPayload["metadata"].(map[string]any)["dependencies"].(map[string]any)["apt"]; got != ">= 1.0.0" {
		t.Fatalf("dependencies.apt = %v, want original value after failed update", got)
	}
}

func TestCookbookVersionReadInflatesDefaultsAndFiltersExtraMetadata(t *testing.T) {
	router := newTestRouter(t)
	checksum := uploadCookbookChecksum(t, router, []byte("puts 'hello v1'"))

	createCookbookVersion(t, router, "demo", "1.2.3", checksum, map[string]string{
		"apt": ">= 1.0.0",
	})

	updatePayload := cookbookVersionPayload("demo", "1.2.3", checksum, nil)
	metadata := updatePayload["metadata"].(map[string]any)
	metadata["name"] = "renamed-app"
	delete(metadata, "description")
	delete(metadata, "long_description")
	delete(metadata, "maintainer")
	delete(metadata, "maintainer_email")
	delete(metadata, "license")
	delete(metadata, "dependencies")
	delete(metadata, "attributes")
	delete(metadata, "recipes")
	metadata["platforms"] = map[string]any{"ubuntu": ">= 20.04"}
	metadata["providing"] = "demo::default"

	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	var updateResponse map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResponse); err != nil {
		t.Fatalf("json.Unmarshal(update response) error = %v", err)
	}
	updateMetadata := updateResponse["metadata"].(map[string]any)
	if updateMetadata["name"] != "renamed-app" {
		t.Fatalf("update metadata.name = %v, want %q", updateMetadata["name"], "renamed-app")
	}
	if _, ok := updateMetadata["description"]; ok {
		t.Fatalf("update metadata.description unexpectedly present: %v", updateMetadata)
	}
	if _, ok := updateMetadata["platforms"]; !ok {
		t.Fatalf("update metadata.platforms missing from exact write response: %v", updateMetadata)
	}
	if updateMetadata["providing"] != "demo::default" {
		t.Fatalf("update metadata.providing = %v, want %q", updateMetadata["providing"], "demo::default")
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
	getReq.Header.Set("X-Ops-Server-API-Version", "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get cookbook status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getResponse map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getResponse); err != nil {
		t.Fatalf("json.Unmarshal(get response) error = %v", err)
	}
	getMetadata := getResponse["metadata"].(map[string]any)
	if getMetadata["name"] != "demo" {
		t.Fatalf("get metadata.name = %v, want %q", getMetadata["name"], "demo")
	}
	if getMetadata["description"] != defaultCookbookDescription {
		t.Fatalf("get metadata.description = %v, want default description", getMetadata["description"])
	}
	if getMetadata["long_description"] != defaultCookbookLongDescription {
		t.Fatalf("get metadata.long_description = %v, want empty string", getMetadata["long_description"])
	}
	if getMetadata["maintainer"] != defaultCookbookMaintainer {
		t.Fatalf("get metadata.maintainer = %v, want default maintainer", getMetadata["maintainer"])
	}
	if getMetadata["maintainer_email"] != defaultCookbookMaintainerEmail {
		t.Fatalf("get metadata.maintainer_email = %v, want default maintainer email", getMetadata["maintainer_email"])
	}
	if getMetadata["license"] != defaultCookbookLicense {
		t.Fatalf("get metadata.license = %v, want default license", getMetadata["license"])
	}
	if getMetadata["version"] != "1.2.3" {
		t.Fatalf("get metadata.version = %v, want %q", getMetadata["version"], "1.2.3")
	}
	if dependencies, ok := getMetadata["dependencies"].(map[string]any); !ok || len(dependencies) != 0 {
		t.Fatalf("get metadata.dependencies = %v, want empty map", getMetadata["dependencies"])
	}
	if attributes, ok := getMetadata["attributes"].(map[string]any); !ok || len(attributes) != 0 {
		t.Fatalf("get metadata.attributes = %v, want empty map", getMetadata["attributes"])
	}
	if recipes, ok := getMetadata["recipes"].(map[string]any); !ok || len(recipes) != 0 {
		t.Fatalf("get metadata.recipes = %v, want empty map", getMetadata["recipes"])
	}
	if _, ok := getMetadata["platforms"]; ok {
		t.Fatalf("get metadata.platforms unexpectedly present: %v", getMetadata)
	}
	if _, ok := getMetadata["providing"]; ok {
		t.Fatalf("get metadata.providing unexpectedly present: %v", getMetadata)
	}
}

func TestCookbookArtifactEndpointsSupportRootLevelAllFiles(t *testing.T) {
	router := newTestRouter(t)
	checksum := uploadCookbookChecksum(t, router, []byte("name 'demo'"))

	createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, map[string]any{
		"name":       "demo",
		"identifier": "1111111111111111111111111111111111111111",
		"version":    "1.2.3",
		"chef_type":  "cookbook_version",
		"metadata": map[string]any{
			"version":      "1.2.3",
			"name":         "demo",
			"dependencies": map[string]any{},
			"recipes":      map[string]any{},
		},
		"all_files": []any{
			map[string]any{
				"name":        "metadata.rb",
				"path":        "metadata.rb",
				"checksum":    checksum,
				"specificity": "default",
			},
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create artifact status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	legacyGetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	legacyGetRec := httptest.NewRecorder()
	router.ServeHTTP(legacyGetRec, legacyGetReq)
	if legacyGetRec.Code != http.StatusOK {
		t.Fatalf("legacy get artifact status = %d, want %d, body = %s", legacyGetRec.Code, http.StatusOK, legacyGetRec.Body.String())
	}
	var legacyPayload map[string]any
	if err := json.Unmarshal(legacyGetRec.Body.Bytes(), &legacyPayload); err != nil {
		t.Fatalf("json.Unmarshal(legacy get artifact) error = %v", err)
	}
	rootFiles := legacyPayload["root_files"].([]any)
	if len(rootFiles) != 1 {
		t.Fatalf("root_files len = %d, want 1 (%v)", len(rootFiles), rootFiles)
	}
	if got := rootFiles[0].(map[string]any)["path"]; got != "metadata.rb" {
		t.Fatalf("root_files[0].path = %v, want %q", got, "metadata.rb")
	}

	v2GetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
	v2GetReq.Header.Set("X-Ops-Server-API-Version", "2")
	v2GetRec := httptest.NewRecorder()
	router.ServeHTTP(v2GetRec, v2GetReq)
	if v2GetRec.Code != http.StatusOK {
		t.Fatalf("v2 get artifact status = %d, want %d, body = %s", v2GetRec.Code, http.StatusOK, v2GetRec.Body.String())
	}
	var v2Payload map[string]any
	if err := json.Unmarshal(v2GetRec.Body.Bytes(), &v2Payload); err != nil {
		t.Fatalf("json.Unmarshal(v2 get artifact) error = %v", err)
	}
	allFiles := v2Payload["all_files"].([]any)
	if len(allFiles) != 1 {
		t.Fatalf("all_files len = %d, want 1 (%v)", len(allFiles), allFiles)
	}
	file := allFiles[0].(map[string]any)
	if file["name"] != "root_files/metadata.rb" {
		t.Fatalf("all_files[0].name = %v, want %q", file["name"], "root_files/metadata.rb")
	}
	if file["path"] != "metadata.rb" {
		t.Fatalf("all_files[0].path = %v, want %q", file["path"], "metadata.rb")
	}
}

func TestCookbookVersionEndpointsConvertBetweenV0AndV2(t *testing.T) {
	router := newTestRouter(t)
	recipeChecksum := uploadCookbookChecksum(t, router, []byte("puts 'recipe contents'"))
	rootChecksum := uploadCookbookChecksum(t, router, []byte("change log"))
	templateChecksum := uploadCookbookChecksum(t, router, []byte("template body"))

	v0Payload := map[string]any{
		"name":          "vconv-1.2.3",
		"cookbook_name": "vconv",
		"version":       "1.2.3",
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"frozen?":       false,
		"metadata": map[string]any{
			"version":          "1.2.3",
			"name":             "vconv",
			"maintainer":       defaultCookbookMaintainer,
			"maintainer_email": defaultCookbookMaintainerEmail,
			"description":      defaultCookbookDescription,
			"long_description": defaultCookbookLongDescription,
			"license":          defaultCookbookLicense,
			"dependencies":     map[string]any{},
			"attributes":       map[string]any{},
			"recipes":          map[string]any{},
		},
		"recipes": []any{
			map[string]any{
				"name":        "default.rb",
				"path":        "recipes/default.rb",
				"checksum":    recipeChecksum,
				"specificity": "default",
			},
		},
		"root_files": []any{
			map[string]any{
				"name":        "CHANGELOG",
				"path":        "CHANGELOG",
				"checksum":    rootChecksum,
				"specificity": "default",
			},
		},
		"templates": []any{
			map[string]any{
				"name":        "config.erb",
				"path":        "templates/default/config.erb",
				"checksum":    templateChecksum,
				"specificity": "default",
			},
		},
	}

	v0CreateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/vconv/1.2.3", mustMarshalSandboxJSON(t, v0Payload))
	v0CreateRec := httptest.NewRecorder()
	router.ServeHTTP(v0CreateRec, v0CreateReq)
	if v0CreateRec.Code != http.StatusCreated {
		t.Fatalf("v0 create status = %d, want %d, body = %s", v0CreateRec.Code, http.StatusCreated, v0CreateRec.Body.String())
	}

	var v0CreateResponse map[string]any
	if err := json.Unmarshal(v0CreateRec.Body.Bytes(), &v0CreateResponse); err != nil {
		t.Fatalf("json.Unmarshal(v0 create) error = %v", err)
	}
	if _, ok := v0CreateResponse["all_files"]; ok {
		t.Fatalf("v0 create response unexpectedly included all_files: %v", v0CreateResponse)
	}

	v2GetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/vconv/1.2.3", nil)
	v2GetReq.Header.Set("X-Ops-Server-API-Version", "2")
	v2GetRec := httptest.NewRecorder()
	router.ServeHTTP(v2GetRec, v2GetReq)
	if v2GetRec.Code != http.StatusOK {
		t.Fatalf("v2 get status = %d, want %d, body = %s", v2GetRec.Code, http.StatusOK, v2GetRec.Body.String())
	}

	var v2GetResponse map[string]any
	if err := json.Unmarshal(v2GetRec.Body.Bytes(), &v2GetResponse); err != nil {
		t.Fatalf("json.Unmarshal(v2 get) error = %v", err)
	}
	allFiles, ok := v2GetResponse["all_files"].([]any)
	if !ok || len(allFiles) != 3 {
		t.Fatalf("v2 all_files = %v, want 3 entries", v2GetResponse["all_files"])
	}
	if _, ok := v2GetResponse["recipes"]; ok {
		t.Fatalf("v2 get response unexpectedly included recipes: %v", v2GetResponse)
	}
	gotNames := map[string]map[string]any{}
	for _, raw := range allFiles {
		file := raw.(map[string]any)
		gotNames[file["name"].(string)] = file
	}
	if _, ok := gotNames["recipes/default.rb"]; !ok {
		t.Fatalf("v2 all_files missing recipe entry: %v", gotNames)
	}
	if template := gotNames["templates/config.erb"]; template == nil {
		t.Fatalf("v2 all_files missing template entry: %v", gotNames)
	} else if template["path"] != "templates/default/config.erb" {
		t.Fatalf("v2 template path = %v, want %q", template["path"], "templates/default/config.erb")
	}
	if root := gotNames["root_files/CHANGELOG"]; root == nil {
		t.Fatalf("v2 all_files missing root_files entry: %v", gotNames)
	} else if root["path"] != "CHANGELOG" {
		t.Fatalf("v2 root file path = %v, want %q", root["path"], "CHANGELOG")
	}

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/vconv/1.2.3", nil)
	deleteReq.Header.Set("X-Ops-Server-API-Version", "2")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	recipeChecksum = uploadCookbookChecksum(t, router, []byte("puts 'recipe contents'"))
	rootChecksum = uploadCookbookChecksum(t, router, []byte("change log"))
	templateChecksum = uploadCookbookChecksum(t, router, []byte("template body"))

	v2Payload := map[string]any{
		"name":          "vconv-2.0.0",
		"cookbook_name": "vconv",
		"version":       "2.0.0",
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"frozen?":       false,
		"metadata": map[string]any{
			"version":          "2.0.0",
			"name":             "vconv",
			"maintainer":       defaultCookbookMaintainer,
			"maintainer_email": defaultCookbookMaintainerEmail,
			"description":      defaultCookbookDescription,
			"long_description": defaultCookbookLongDescription,
			"license":          defaultCookbookLicense,
			"dependencies":     map[string]any{},
			"attributes":       map[string]any{},
			"recipes":          map[string]any{},
		},
		"all_files": []any{
			map[string]any{
				"name":        "recipes/default.rb",
				"path":        "recipes/default.rb",
				"checksum":    recipeChecksum,
				"specificity": "default",
			},
			map[string]any{
				"name":        "root_files/CHANGELOG",
				"path":        "CHANGELOG",
				"checksum":    rootChecksum,
				"specificity": "default",
			},
			map[string]any{
				"name":        "templates/default/config.erb",
				"path":        "templates/default/config.erb",
				"checksum":    templateChecksum,
				"specificity": "default",
			},
		},
	}

	v2CreateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/vconv/2.0.0", mustMarshalSandboxJSON(t, v2Payload))
	v2CreateReq.Header.Set("X-Ops-Server-API-Version", "2")
	v2CreateRec := httptest.NewRecorder()
	router.ServeHTTP(v2CreateRec, v2CreateReq)
	if v2CreateRec.Code != http.StatusCreated {
		t.Fatalf("v2 create status = %d, want %d, body = %s", v2CreateRec.Code, http.StatusCreated, v2CreateRec.Body.String())
	}

	var v2CreateResponse map[string]any
	if err := json.Unmarshal(v2CreateRec.Body.Bytes(), &v2CreateResponse); err != nil {
		t.Fatalf("json.Unmarshal(v2 create) error = %v", err)
	}
	v2CreatedFiles := v2CreateResponse["all_files"].([]any)
	if len(v2CreatedFiles) != 3 {
		t.Fatalf("v2 create all_files len = %d, want 3 (%v)", len(v2CreatedFiles), v2CreatedFiles)
	}
	foundRootName := false
	foundTemplateName := false
	for _, raw := range v2CreatedFiles {
		file := raw.(map[string]any)
		if file["name"] == "root_files/CHANGELOG" && file["path"] == "CHANGELOG" {
			foundRootName = true
		}
		if file["name"] == "templates/config.erb" && file["path"] == "templates/default/config.erb" {
			foundTemplateName = true
		}
	}
	if !foundRootName {
		t.Fatalf("v2 create response missing root_files/CHANGELOG: %v", v2CreatedFiles)
	}
	if !foundTemplateName {
		t.Fatalf("v2 create response missing templates/config.erb: %v", v2CreatedFiles)
	}

	v0GetReq := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/vconv/2.0.0", nil)
	v0GetRec := httptest.NewRecorder()
	router.ServeHTTP(v0GetRec, v0GetReq)
	if v0GetRec.Code != http.StatusOK {
		t.Fatalf("v0 get status = %d, want %d, body = %s", v0GetRec.Code, http.StatusOK, v0GetRec.Body.String())
	}

	var v0GetResponse map[string]any
	if err := json.Unmarshal(v0GetRec.Body.Bytes(), &v0GetResponse); err != nil {
		t.Fatalf("json.Unmarshal(v0 get) error = %v", err)
	}
	if _, ok := v0GetResponse["all_files"]; ok {
		t.Fatalf("v0 get response unexpectedly included all_files: %v", v0GetResponse)
	}
	recipes, ok := v0GetResponse["recipes"].([]any)
	if !ok || len(recipes) != 1 {
		t.Fatalf("v0 recipes = %v, want 1 entry", v0GetResponse["recipes"])
	}
	recipe := recipes[0].(map[string]any)
	if recipe["name"] != "default.rb" || recipe["path"] != "recipes/default.rb" {
		t.Fatalf("v0 recipe = %v, want default.rb/recipes/default.rb", recipe)
	}
	rootFiles, ok := v0GetResponse["root_files"].([]any)
	if !ok || len(rootFiles) != 1 {
		t.Fatalf("v0 root_files = %v, want 1 entry", v0GetResponse["root_files"])
	}
	root := rootFiles[0].(map[string]any)
	if root["name"] != "CHANGELOG" || root["path"] != "CHANGELOG" {
		t.Fatalf("v0 root file = %v, want CHANGELOG/CHANGELOG", root)
	}
	templates, ok := v0GetResponse["templates"].([]any)
	if !ok || len(templates) != 1 {
		t.Fatalf("v0 templates = %v, want 1 entry", v0GetResponse["templates"])
	}
	template := templates[0].(map[string]any)
	if template["name"] != "config.erb" || template["path"] != "templates/default/config.erb" {
		t.Fatalf("v0 template = %v, want config.erb/templates/default/config.erb", template)
	}
}

func uploadCookbookChecksum(t *testing.T, router http.Handler, content []byte) string {
	t.Helper()

	checksum := checksumHex(content)
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)
	sandboxID := createPayload["sandbox_id"].(string)

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/x-binary")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}

	commitReq := newSignedJSONRequest(t, http.MethodPut, "/sandboxes/"+sandboxID, mustMarshalSandboxJSON(t, map[string]any{
		"is_completed": true,
	}))
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusOK {
		t.Fatalf("commit sandbox status = %d, want %d, body = %s", commitRec.Code, http.StatusOK, commitRec.Body.String())
	}

	return checksum
}

func cookbookFileURL(t *testing.T, router http.Handler, path string) string {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	req.Header.Set("X-Ops-Server-API-Version", "2")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	allFiles := payload["all_files"].([]any)
	if len(allFiles) == 0 {
		t.Fatalf("%s all_files = %v, want at least one entry", path, payload["all_files"])
	}
	return allFiles[0].(map[string]any)["url"].(string)
}

func cookbookArtifactFileURL(t *testing.T, router http.Handler, path string) string {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	recipes := payload["recipes"].([]any)
	if len(recipes) == 0 {
		t.Fatalf("%s recipes = %v, want at least one entry", path, payload["recipes"])
	}
	return recipes[0].(map[string]any)["url"].(string)
}

func cookbookBlobURLByPath(t *testing.T, router http.Handler, path, wantPath string) string {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	req.Header.Set("X-Ops-Server-API-Version", "2")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	allFiles, ok := payload["all_files"].([]any)
	if !ok {
		t.Fatalf("%s all_files = %T, want []any (%v)", path, payload["all_files"], payload)
	}
	for _, raw := range allFiles {
		file, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("%s all_files entry = %T, want map[string]any", path, raw)
		}
		if file["path"] == wantPath {
			url, ok := file["url"].(string)
			if !ok {
				t.Fatalf("%s file url = %T, want string (%v)", path, file["url"], file)
			}
			return url
		}
	}
	t.Fatalf("%s missing file path %q in %v", path, wantPath, allFiles)
	return ""
}

func assertBlobDownloadStatus(t *testing.T, router http.Handler, downloadURL string, want int) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("GET %s status = %d, want %d, body = %s", downloadURL, rec.Code, want, rec.Body.String())
	}
}

func assertCookbookDescription(t *testing.T, router http.Handler, path, want string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	rawMetadata, ok := payload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("%s metadata = %T, want map[string]any (%v)", path, payload["metadata"], payload)
	}
	if rawMetadata["description"] != want {
		t.Fatalf("%s metadata.description = %v, want %q", path, rawMetadata["description"], want)
	}
}

func assertCookbookAllFilePaths(t *testing.T, router http.Handler, path string, want []string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	req.Header.Set("X-Ops-Server-API-Version", "2")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	rawAllFiles, ok := payload["all_files"].([]any)
	if !ok {
		t.Fatalf("%s all_files = %T, want []any (%v)", path, payload["all_files"], payload)
	}
	got := make([]string, 0, len(rawAllFiles))
	for _, raw := range rawAllFiles {
		file, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("%s all_files entry = %T, want map[string]any", path, raw)
		}
		pathValue, ok := file["path"].(string)
		if !ok {
			t.Fatalf("%s all_files path = %T, want string (%v)", path, file["path"], file)
		}
		got = append(got, pathValue)
	}
	sort.Strings(got)
	wantCopy := append([]string(nil), want...)
	if wantCopy == nil {
		wantCopy = []string{}
	}
	sort.Strings(wantCopy)
	if !reflect.DeepEqual(got, wantCopy) {
		t.Fatalf("%s all_files paths = %v, want %v", path, got, wantCopy)
	}
}

func assertCookbookMissing(t *testing.T, router http.Handler, path string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func assertCookbookArtifactMissing(t *testing.T, router http.Handler, path string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func assertCookbookArtifactDescription(t *testing.T, router http.Handler, path, want string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	rawMetadata, ok := payload["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("%s metadata = %T, want map[string]any (%v)", path, payload["metadata"], payload)
	}
	description, ok := rawMetadata["description"].(string)
	if !ok {
		t.Fatalf("%s metadata.description = %T, want string (%v)", path, rawMetadata["description"], rawMetadata)
	}
	if description != want {
		t.Fatalf("%s metadata.description = %q, want %q", path, description, want)
	}
}

func assertCookbookArtifactStringError(t *testing.T, body []byte, want string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(error payload) error = %v", err)
	}
	got, ok := payload["error"].(string)
	if !ok {
		t.Fatalf("payload error = %T, want string (%v)", payload["error"], payload)
	}
	if got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func assertCookbookAPIError(t *testing.T, body []byte, wantError, wantMessage string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(api error payload) error = %v", err)
	}
	if got := payload["error"]; got != wantError {
		t.Fatalf("error = %v, want %q", got, wantError)
	}
	if got := payload["message"]; got != wantMessage {
		t.Fatalf("message = %v, want %q", got, wantMessage)
	}
}

func assertCookbookErrorList(t *testing.T, body []byte, want []string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal(error payload) error = %v", err)
	}
	rawErrors, ok := payload["error"].([]any)
	if !ok {
		t.Fatalf("payload error = %T, want []any (%v)", payload["error"], payload)
	}
	if len(rawErrors) != len(want) {
		t.Fatalf("error len = %d, want %d (%v)", len(rawErrors), len(want), rawErrors)
	}
	for i := range want {
		if rawErrors[i] != want[i] {
			t.Fatalf("error[%d] = %v, want %q (%v)", i, rawErrors[i], want[i], rawErrors)
		}
	}
}

func cookbookVersionListForName(t *testing.T, payload map[string]any, name string) []any {
	t.Helper()

	rawCookbook, ok := payload[name]
	if !ok {
		t.Fatalf("payload missing cookbook %q: %v", name, payload)
	}
	cookbookEntry, ok := rawCookbook.(map[string]any)
	if !ok {
		t.Fatalf("payload[%q] = %T, want map[string]any (%v)", name, rawCookbook, payload)
	}
	rawVersions, ok := cookbookEntry["versions"]
	if !ok {
		t.Fatalf("payload[%q] missing versions: %v", name, cookbookEntry)
	}
	versions, ok := rawVersions.([]any)
	if !ok {
		t.Fatalf("payload[%q].versions = %T, want []any (%v)", name, rawVersions, cookbookEntry)
	}
	return versions
}

type cookbookRouteTestS3BlobControl struct {
	mu                sync.Mutex
	objects           map[string][]byte
	existsUnavailable bool
	getUnavailable    bool
}

func (c *cookbookRouteTestS3BlobControl) setExistsUnavailable(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.existsUnavailable = v
}

func (c *cookbookRouteTestS3BlobControl) setGetUnavailable(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getUnavailable = v
}

func newCookbookRouteTestS3BlobStore(t *testing.T) (*blob.S3CompatibleStore, *cookbookRouteTestS3BlobControl) {
	t.Helper()

	control := &cookbookRouteTestS3BlobControl{
		objects: map[string][]byte{},
	}

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		control.mu.Lock()
		defer control.mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll() error = %v", err)
				return testRoundTripResponse(r, http.StatusInternalServerError, ""), nil
			}
			control.objects[r.URL.Path] = body
			return testRoundTripResponse(r, http.StatusOK, ""), nil
		case http.MethodHead:
			if control.existsUnavailable {
				return testRoundTripResponse(r, http.StatusForbidden, ""), nil
			}
			if _, ok := control.objects[r.URL.Path]; !ok {
				return testRoundTripResponse(r, http.StatusNotFound, ""), nil
			}
			return testRoundTripResponse(r, http.StatusOK, ""), nil
		case http.MethodGet:
			if control.getUnavailable {
				return testRoundTripResponse(r, http.StatusForbidden, ""), nil
			}
			body, ok := control.objects[r.URL.Path]
			if !ok {
				return testRoundTripResponse(r, http.StatusNotFound, ""), nil
			}
			return testRoundTripResponse(r, http.StatusOK, string(body)), nil
		case http.MethodDelete:
			if _, ok := control.objects[r.URL.Path]; !ok {
				return testRoundTripResponse(r, http.StatusNotFound, ""), nil
			}
			delete(control.objects, r.URL.Path)
			return testRoundTripResponse(r, http.StatusNoContent, ""), nil
		default:
			return testRoundTripResponse(r, http.StatusMethodNotAllowed, ""), nil
		}
	})}

	store, err := blob.NewS3CompatibleStore(blob.S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		SessionToken:   "session-token",
		HTTPClient:     client,
		MaxRetries:     0,
		Now: func() time.Time {
			return time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	return store, control
}

func createCookbookVersionWithFiles(t *testing.T, router http.Handler, name, version string, files []map[string]any) {
	t.Helper()

	payload := cookbookVersionPayload(name, version, "", nil)
	allFiles := make([]any, 0, len(files))
	for _, file := range files {
		allFiles = append(allFiles, file)
	}
	payload["all_files"] = allFiles

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/"+name+"/"+version, mustMarshalSandboxJSON(t, payload))
	req.Header.Set("X-Ops-Server-API-Version", "2")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook %s/%s with files status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func cookbookFilePayload(name, path, checksum string) map[string]any {
	return map[string]any{
		"name":        name,
		"path":        path,
		"checksum":    checksum,
		"specificity": "default",
	}
}

func createCookbookArtifact(t *testing.T, router http.Handler, name, identifier, version, checksum string, dependencies map[string]string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/"+name+"/"+identifier, mustMarshalSandboxJSON(t, cookbookArtifactPayload(name, identifier, version, checksum, dependencies)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook artifact %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func createOrgCookbookArtifact(t *testing.T, router http.Handler, org, name, identifier, version, checksum string, dependencies map[string]string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodPut, "/organizations/"+org+"/cookbook_artifacts/"+name+"/"+identifier, mustMarshalSandboxJSON(t, cookbookArtifactPayload(name, identifier, version, checksum, dependencies)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org-scoped cookbook artifact %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func createOrgCookbookVersion(t *testing.T, router http.Handler, org, name, version, checksum string, dependencies map[string]string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodPut, "/organizations/"+org+"/cookbooks/"+name+"/"+version, mustMarshalSandboxJSON(t, cookbookVersionPayload(name, version, checksum, dependencies)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create org-scoped cookbook %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func createCookbookVersion(t *testing.T, router http.Handler, name, version, checksum string, dependencies map[string]string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/"+name+"/"+version, mustMarshalSandboxJSON(t, cookbookVersionPayload(name, version, checksum, dependencies)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func cookbookArtifactPayload(name, identifier, version, checksum string, dependencies map[string]string) map[string]any {
	metadataDeps := map[string]any{}
	for depName, constraint := range dependencies {
		metadataDeps[depName] = constraint
	}

	metadataRecipes := map[string]any{}
	recipes := []any{}
	allFiles := []any{}
	if checksum != "" {
		metadataRecipes[name+"::default"] = ""
		recipe := map[string]any{
			"name":        "default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		}
		recipes = append(recipes, recipe)
		allFiles = append(allFiles, map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		})
	}

	return map[string]any{
		"name":       name,
		"identifier": identifier,
		"version":    version,
		"chef_type":  "cookbook_version",
		"frozen?":    false,
		"metadata": map[string]any{
			"version":          version,
			"name":             name,
			"maintainer":       "OpenCook",
			"maintainer_email": "opencook@example.com",
			"description":      "compatibility cookbook",
			"long_description": "compatibility cookbook",
			"license":          defaultCookbookLicense,
			"dependencies":     metadataDeps,
			"attributes":       map[string]any{},
			"recipes":          metadataRecipes,
			"providing":        map[string]any{name + "::default": ">= 0.0.0"},
		},
		"recipes":   recipes,
		"all_files": allFiles,
	}
}

func cookbookVersionPayload(name, version, checksum string, dependencies map[string]string) map[string]any {
	metadataDeps := map[string]any{}
	for depName, constraint := range dependencies {
		metadataDeps[depName] = constraint
	}

	metadataRecipes := map[string]any{}
	recipes := []any{}
	allFiles := []any{}
	if checksum != "" {
		metadataRecipes[name+"::default"] = ""
		legacyRecipe := map[string]any{
			"name":        "default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		}
		recipes = append(recipes, legacyRecipe)
		allFiles = append(allFiles, map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		})
	}

	return map[string]any{
		"name":          name + "-" + version,
		"cookbook_name": name,
		"version":       version,
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"frozen?":       false,
		"metadata": map[string]any{
			"version":          version,
			"name":             name,
			"maintainer":       "OpenCook",
			"maintainer_email": "opencook@example.com",
			"description":      "compatibility cookbook",
			"long_description": "compatibility cookbook",
			"license":          defaultCookbookLicense,
			"dependencies":     metadataDeps,
			"attributes":       map[string]any{},
			"recipes":          metadataRecipes,
		},
		"recipes":   recipes,
		"all_files": allFiles,
	}
}
