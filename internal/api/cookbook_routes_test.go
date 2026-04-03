package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
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
	if len(recipesPayload) == 0 || recipesPayload[0] != "demo::default" {
		t.Fatalf("recipes payload = %v, want latest recipe listing", recipesPayload)
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
	if file["name"] != "metadata.rb" {
		t.Fatalf("all_files[0].name = %v, want %q", file["name"], "metadata.rb")
	}
	if file["path"] != "metadata.rb" {
		t.Fatalf("all_files[0].path = %v, want %q", file["path"], "metadata.rb")
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

func createCookbookArtifact(t *testing.T, router http.Handler, name, identifier, version, checksum string, dependencies map[string]string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbook_artifacts/"+name+"/"+identifier, mustMarshalSandboxJSON(t, cookbookArtifactPayload(name, identifier, version, checksum, dependencies)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook artifact %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
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
			"license":          "Apache-2.0",
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
			"license":          "Apache-2.0",
			"dependencies":     metadataDeps,
			"attributes":       map[string]any{},
			"recipes":          metadataRecipes,
		},
		"recipes":   recipes,
		"all_files": allFiles,
	}
}
