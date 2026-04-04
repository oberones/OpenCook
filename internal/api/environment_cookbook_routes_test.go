package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnvironmentCookbookRoutesFilterVersionsAndDefaultNamedRouteToAll(t *testing.T) {
	router := newTestRouter(t)

	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersionWithRecipes(t, router, "demo", "1.0.0", "default")
	createCookbookVersionWithRecipes(t, router, "demo", "2.0.0", "default", "users")
	createCookbookVersionWithRecipes(t, router, "demo", "3.0.0", "default", "admins")
	createCookbookVersionWithRecipes(t, router, "other", "0.5.0", "default")

	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"demo": "< 2.5.0",
	})

	listReq := httptest.NewRequest(http.MethodGet, "/environments/production/cookbooks?num_versions=2", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/environments/production/cookbooks", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("environment cookbooks status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(environment cookbooks) error = %v", err)
	}
	assertCookbookVersionList(t, listPayload, "demo", "2.0.0", "1.0.0")
	assertCookbookVersionList(t, listPayload, "other", "0.5.0")

	namedReq := newSignedJSONRequest(t, http.MethodGet, "/environments/production/cookbooks/demo", nil)
	namedRec := httptest.NewRecorder()
	router.ServeHTTP(namedRec, namedReq)
	if namedRec.Code != http.StatusOK {
		t.Fatalf("named environment cookbook status = %d, want %d, body = %s", namedRec.Code, http.StatusOK, namedRec.Body.String())
	}

	var namedPayload map[string]any
	if err := json.Unmarshal(namedRec.Body.Bytes(), &namedPayload); err != nil {
		t.Fatalf("json.Unmarshal(named environment cookbook) error = %v", err)
	}
	assertCookbookVersionList(t, namedPayload, "demo", "2.0.0", "1.0.0")

	missingReq := newSignedJSONRequest(t, http.MethodGet, "/environments/production/cookbooks/missing", nil)
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("missing named environment cookbook status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

func TestEnvironmentRecipesRouteUsesLatestAllowedCookbookVersions(t *testing.T) {
	router := newTestRouter(t)

	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersionWithRecipes(t, router, "demo", "1.0.0", "default", "legacy")
	createCookbookVersionWithRecipes(t, router, "demo", "2.0.0", "default", "users")
	createCookbookVersionWithRecipes(t, router, "demo", "3.0.0", "default", "admins")
	createCookbookVersionWithRecipes(t, router, "other", "1.2.0", "default", "baz")

	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"demo": "< 2.5.0",
	})

	req := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/environments/production/recipes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("environment recipes status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload []string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(environment recipes) error = %v", err)
	}

	want := []string{"demo", "demo::users", "other", "other::baz"}
	if len(payload) != len(want) {
		t.Fatalf("len(payload) = %d, want %d (%v)", len(payload), len(want), payload)
	}
	for idx := range want {
		if payload[idx] != want[idx] {
			t.Fatalf("payload[%d] = %q, want %q (%v)", idx, payload[idx], want[idx], payload)
		}
	}
}

func TestEnvironmentCookbookRoutesRejectInvalidNumVersions(t *testing.T) {
	router := newTestRouter(t)

	createEnvironmentForCookbookTests(t, router, "production")

	req := httptest.NewRequest(http.MethodGet, "/environments/production/cookbooks?num_versions=skittles", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/environments/production/cookbooks", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid num_versions status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

func createEnvironmentForCookbookTests(t *testing.T, router http.Handler, name string) {
	t.Helper()

	body := mustMarshalEnvironmentPayload(t, name)
	req := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(body))
	applySignedHeaders(t, req, "silent-bob", "", http.MethodPost, "/environments", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func updateEnvironmentCookbookConstraints(t *testing.T, router http.Handler, name string, constraints map[string]string) {
	t.Helper()

	bodyMap := map[string]any{
		"name":                name,
		"json_class":          "Chef::Environment",
		"chef_type":           "environment",
		"description":         "",
		"cookbook_versions":   constraints,
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
	}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		t.Fatalf("json.Marshal(environment update) error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPut, "/environments/"+name, bytes.NewReader(body))
	applySignedHeaders(t, req, "silent-bob", "", http.MethodPut, "/environments/"+name, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("update environment status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func createCookbookVersionWithRecipes(t *testing.T, router http.Handler, name, version string, recipeNames ...string) {
	t.Helper()

	payload := cookbookVersionPayload(name, version, "", nil)
	metadataRecipes := map[string]any{}
	allFiles := make([]any, 0, len(recipeNames))
	for _, recipeName := range recipeNames {
		checksum := uploadCookbookChecksum(t, router, []byte("puts '"+name+"-"+version+"-"+recipeName+"'"))
		metadataRecipes[name+"::"+recipeName] = ""
		allFiles = append(allFiles, map[string]any{
			"name":        "recipes/" + recipeName + ".rb",
			"path":        "recipes/" + recipeName + ".rb",
			"checksum":    checksum,
			"specificity": "default",
		})
	}
	payload["metadata"].(map[string]any)["recipes"] = metadataRecipes
	payload["all_files"] = allFiles

	req := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/"+name+"/"+version, mustMarshalSandboxJSON(t, payload))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func assertCookbookVersionList(t *testing.T, payload map[string]any, cookbook string, wantVersions ...string) {
	t.Helper()

	entry, ok := payload[cookbook].(map[string]any)
	if !ok {
		t.Fatalf("payload[%q] missing or wrong type: %v", cookbook, payload)
	}
	versions, ok := entry["versions"].([]any)
	if !ok {
		t.Fatalf("payload[%q].versions = %T, want []any (%v)", cookbook, entry["versions"], entry["versions"])
	}
	if len(versions) != len(wantVersions) {
		t.Fatalf("payload[%q].versions len = %d, want %d (%v)", cookbook, len(versions), len(wantVersions), versions)
	}
	for idx, version := range wantVersions {
		if versions[idx].(map[string]any)["version"] != version {
			t.Fatalf("payload[%q].versions[%d] = %v, want %q (%v)", cookbook, idx, versions[idx], version, versions)
		}
	}
}
