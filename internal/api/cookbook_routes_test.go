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
