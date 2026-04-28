package api

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/blob"
)

func TestCookbookAPIVersionReadWriteDownloadMatrix(t *testing.T) {
	router := newTestRouter(t)

	oldChecksum := uploadCookbookChecksum(t, router, []byte("puts 'api matrix old'"))
	latestChecksum := uploadCookbookChecksum(t, router, []byte("puts 'api matrix latest'"))
	updatedChecksum := uploadCookbookChecksum(t, router, []byte("puts 'api matrix updated'"))
	orgChecksum := uploadCookbookChecksum(t, router, []byte("puts 'org api matrix'"))
	artifactChecksum := uploadCookbookChecksum(t, router, []byte("puts 'artifact api matrix'"))
	orgArtifactChecksum := uploadCookbookChecksum(t, router, []byte("puts 'org artifact api matrix'"))

	oldCreate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/cookbooks/api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("api-matrix", "1.0.0", oldChecksum, map[string]string{"base": ">= 0.1.0"})), "0")
	assertCookbookAPIVersionStatus(t, oldCreate, http.StatusCreated, "create legacy cookbook")
	assertCookbookLegacyWriteShape(t, mustDecodeObject(t, oldCreate), "recipes/default.rb", oldChecksum)

	latestCreate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/cookbooks/api-matrix/2.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("api-matrix", "2.0.0", latestChecksum, map[string]string{"base": ">= 2.0.0"})), "2")
	assertCookbookAPIVersionStatus(t, latestCreate, http.StatusCreated, "create v2 cookbook")
	assertCookbookV2WriteShape(t, mustDecodeObject(t, latestCreate), "recipes/default.rb", latestChecksum)

	updatePayload := cookbookVersionPayload("api-matrix", "2.0.0", updatedChecksum, map[string]string{"base": ">= 2.1.0"})
	updatePayload["metadata"].(map[string]any)["description"] = "api matrix updated description"
	latestUpdate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/cookbooks/api-matrix/2.0.0",
		mustMarshalSandboxJSON(t, updatePayload), "2")
	assertCookbookAPIVersionStatus(t, latestUpdate, http.StatusOK, "update v2 cookbook")
	assertCookbookV2WriteShape(t, mustDecodeObject(t, latestUpdate), "recipes/default.rb", updatedChecksum)

	orgCreate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/organizations/ponyville/cookbooks/org-api-matrix/3.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("org-api-matrix", "3.0.0", orgChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, orgCreate, http.StatusCreated, "create org v2 cookbook")

	assertCookbookVersionCollectionForAPIVersion(t, router, "/cookbooks", "2", "/cookbooks", "api-matrix", "2.0.0")
	assertCookbookVersionCollectionForAPIVersion(t, router, "/cookbooks/api-matrix", "0", "/cookbooks", "api-matrix", "2.0.0", "1.0.0")
	assertCookbookVersionCollectionForAPIVersion(t, router, "/organizations/ponyville/cookbooks/org-api-matrix", "2", "/organizations/ponyville/cookbooks", "org-api-matrix", "3.0.0")

	legacyRead := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/cookbooks/api-matrix/2.0.0", nil, "0")
	assertCookbookAPIVersionStatus(t, legacyRead, http.StatusOK, "legacy cookbook read")
	assertCookbookLegacyReadShape(t, mustDecodeObject(t, legacyRead), "recipes/default.rb", updatedChecksum, "ponyville")

	v2Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/cookbooks/api-matrix/2.0.0", nil, "2")
	assertCookbookAPIVersionStatus(t, v2Read, http.StatusOK, "v2 cookbook read")
	v2ReadPayload := mustDecodeObject(t, v2Read)
	assertCookbookV2ReadShape(t, v2ReadPayload, "recipes/default.rb", updatedChecksum, "ponyville")
	downloadURL := cookbookFileURLByPathFromPayload(t, v2ReadPayload, "recipes/default.rb")
	assertCookbookDownloadURLShape(t, downloadURL, updatedChecksum, "ponyville")
	assertCookbookDownloadBody(t, router, downloadURL, "puts 'api matrix updated'")

	latestView := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/cookbooks/api-matrix/_latest", nil, "2")
	assertCookbookAPIVersionStatus(t, latestView, http.StatusOK, "named latest cookbook read")
	if got := mustDecodeObject(t, latestView)["version"]; got != "2.0.0" {
		t.Fatalf("named latest version = %v, want %q", got, "2.0.0")
	}

	assertCookbookLatestMapForAPIVersion(t, router, "/cookbooks/_latest", "2", "api-matrix", "/cookbooks/api-matrix/2.0.0")
	assertCookbookLatestMapForAPIVersion(t, router, "/organizations/ponyville/cookbooks/_latest", "0", "org-api-matrix", "/organizations/ponyville/cookbooks/org-api-matrix/3.0.0")
	assertCookbookRecipesContainForAPIVersion(t, router, "/cookbooks/_recipes", "2", "api-matrix", "org-api-matrix")
	assertUniverseContainsForAPIVersion(t, router, "/universe", "2", "api-matrix", "2.0.0", "/cookbooks/api-matrix/2.0.0")
	assertUniverseContainsForAPIVersion(t, router, "/organizations/ponyville/universe", "0", "org-api-matrix", "3.0.0", "/organizations/ponyville/cookbooks/org-api-matrix/3.0.0")

	orgRead := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/cookbooks/org-api-matrix/3.0.0", nil, "2")
	assertCookbookAPIVersionStatus(t, orgRead, http.StatusOK, "org v2 cookbook read")
	orgPayload := mustDecodeObject(t, orgRead)
	assertCookbookV2ReadShape(t, orgPayload, "recipes/default.rb", orgChecksum, "ponyville")
	orgDownloadURL := cookbookFileURLByPathFromPayload(t, orgPayload, "recipes/default.rb")
	assertCookbookDownloadURLShape(t, orgDownloadURL, orgChecksum, "ponyville")
	assertCookbookDownloadBody(t, router, orgDownloadURL, "puts 'org api matrix'")

	artifactIdentifier := "1111111111111111111111111111111111111111"
	artifactCreate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/cookbook_artifacts/artifact-api-matrix/"+artifactIdentifier,
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("artifact-api-matrix", artifactIdentifier, "2.0.0", artifactChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, artifactCreate, http.StatusCreated, "create v2 artifact")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, artifactCreate), "recipes/default.rb", artifactChecksum, "ponyville")

	orgArtifactIdentifier := "2222222222222222222222222222222222222222"
	orgArtifactCreate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/organizations/ponyville/cookbook_artifacts/org-artifact-api-matrix/"+orgArtifactIdentifier,
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("org-artifact-api-matrix", orgArtifactIdentifier, "3.0.0", orgArtifactChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, orgArtifactCreate, http.StatusCreated, "create org v2 artifact")

	assertCookbookArtifactCollectionForAPIVersion(t, router, "/cookbook_artifacts", "2", "/cookbook_artifacts", "artifact-api-matrix", artifactIdentifier)
	assertCookbookArtifactCollectionForAPIVersion(t, router, "/cookbook_artifacts/artifact-api-matrix", "0", "/cookbook_artifacts", "artifact-api-matrix", artifactIdentifier)
	assertCookbookArtifactCollectionForAPIVersion(t, router, "/organizations/ponyville/cookbook_artifacts/org-artifact-api-matrix", "2", "/organizations/ponyville/cookbook_artifacts", "org-artifact-api-matrix", orgArtifactIdentifier)

	artifactLegacyRead := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/cookbook_artifacts/artifact-api-matrix/"+artifactIdentifier, nil, "0")
	assertCookbookAPIVersionStatus(t, artifactLegacyRead, http.StatusOK, "legacy artifact read")
	assertCookbookLegacyReadShape(t, mustDecodeObject(t, artifactLegacyRead), "recipes/default.rb", artifactChecksum, "ponyville")

	artifactV2Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/cookbook_artifacts/artifact-api-matrix/"+artifactIdentifier, nil, "2")
	assertCookbookAPIVersionStatus(t, artifactV2Read, http.StatusOK, "v2 artifact read")
	artifactPayload := mustDecodeObject(t, artifactV2Read)
	assertCookbookV2ReadShape(t, artifactPayload, "recipes/default.rb", artifactChecksum, "ponyville")
	artifactDownloadURL := cookbookFileURLByPathFromPayload(t, artifactPayload, "recipes/default.rb")
	assertCookbookDownloadURLShape(t, artifactDownloadURL, artifactChecksum, "ponyville")
	assertCookbookDownloadBody(t, router, artifactDownloadURL, "puts 'artifact api matrix'")

	cookbookDelete := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/cookbooks/api-matrix/2.0.0", nil, "2")
	assertCookbookAPIVersionStatus(t, cookbookDelete, http.StatusOK, "delete v2 cookbook")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, cookbookDelete), "recipes/default.rb", updatedChecksum, "ponyville")
	assertCookbookMissing(t, router, "/cookbooks/api-matrix/2.0.0")

	artifactDelete := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/cookbook_artifacts/artifact-api-matrix/"+artifactIdentifier, nil, "2")
	assertCookbookAPIVersionStatus(t, artifactDelete, http.StatusOK, "delete v2 artifact")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, artifactDelete), "recipes/default.rb", artifactChecksum, "ponyville")
	assertCookbookArtifactMissing(t, router, "/cookbook_artifacts/artifact-api-matrix/"+artifactIdentifier)
}

func TestCookbookAPIVersionPostgresProviderRehydratesFileShapeConversions(t *testing.T) {
	fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

	legacyRecipeChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'legacy recipe'"))
	legacyRootChecksum := uploadCookbookChecksum(t, fixture.router, []byte("legacy changelog"))
	legacyCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/cookbooks/rehydrate-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, legacySegmentCookbookPayload("rehydrate-api-matrix", "1.0.0", legacyRecipeChecksum, legacyRootChecksum)), "0")
	assertCookbookAPIVersionStatus(t, legacyCreate, http.StatusCreated, "create legacy postgres cookbook")

	artifactChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'rehydrated artifact'"))
	artifactIdentifier := "3333333333333333333333333333333333333333"
	artifactCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/organizations/ponyville/cookbook_artifacts/rehydrate-artifact-api-matrix/"+artifactIdentifier,
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("rehydrate-artifact-api-matrix", artifactIdentifier, "1.0.0", artifactChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, artifactCreate, http.StatusCreated, "create v2 postgres artifact")

	restarted := fixture.restart()
	v2Read := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/cookbooks/rehydrate-api-matrix/1.0.0", nil, "2")
	assertCookbookAPIVersionStatus(t, v2Read, http.StatusOK, "rehydrated v2 cookbook read")
	v2Payload := mustDecodeObject(t, v2Read)
	assertCookbookAllFilePathsFromPayload(t, v2Payload, "CHANGELOG.md", "recipes/default.rb")
	assertCookbookV2ReadShape(t, v2Payload, "recipes/default.rb", legacyRecipeChecksum, "ponyville")
	assertCookbookDownloadBody(t, restarted.router, cookbookFileURLByPathFromPayload(t, v2Payload, "recipes/default.rb"), "puts 'legacy recipe'")

	legacyRead := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/cookbooks/rehydrate-api-matrix/1.0.0", nil, "0")
	assertCookbookAPIVersionStatus(t, legacyRead, http.StatusOK, "rehydrated legacy cookbook read")
	legacyPayload := mustDecodeObject(t, legacyRead)
	assertCookbookLegacyReadShape(t, legacyPayload, "recipes/default.rb", legacyRecipeChecksum, "ponyville")
	assertCookbookLegacyPath(t, legacyPayload, "root_files", "CHANGELOG.md", legacyRootChecksum)

	artifactLegacyRead := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/ponyville/cookbook_artifacts/rehydrate-artifact-api-matrix/"+artifactIdentifier, nil, "0")
	assertCookbookAPIVersionStatus(t, artifactLegacyRead, http.StatusOK, "rehydrated legacy artifact read")
	assertCookbookLegacyReadShape(t, mustDecodeObject(t, artifactLegacyRead), "recipes/default.rb", artifactChecksum, "ponyville")

	artifactV2Read := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/ponyville/cookbook_artifacts/rehydrate-artifact-api-matrix/"+artifactIdentifier, nil, "2")
	assertCookbookAPIVersionStatus(t, artifactV2Read, http.StatusOK, "rehydrated v2 artifact read")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, artifactV2Read), "recipes/default.rb", artifactChecksum, "ponyville")

	updatedChecksum := uploadCookbookChecksum(t, restarted.router, []byte("puts 'updated after restart'"))
	update := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/cookbooks/rehydrate-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("rehydrate-api-matrix", "1.0.0", updatedChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, update, http.StatusOK, "update rehydrated v2 cookbook")
	assertCookbookV2WriteShape(t, mustDecodeObject(t, update), "recipes/default.rb", updatedChecksum)

	afterUpdate := restarted.restart()
	updatedLegacyRead := serveSignedAPIVersionRequest(t, afterUpdate.router, "pivotal", http.MethodGet, "/cookbooks/rehydrate-api-matrix/1.0.0", nil, "0")
	assertCookbookAPIVersionStatus(t, updatedLegacyRead, http.StatusOK, "updated legacy cookbook read")
	updatedLegacyPayload := mustDecodeObject(t, updatedLegacyRead)
	assertCookbookLegacyReadShape(t, updatedLegacyPayload, "recipes/default.rb", updatedChecksum, "ponyville")
	assertCookbookLegacySegmentEmpty(t, updatedLegacyPayload, "root_files")
	assertCookbookDownloadBody(t, afterUpdate.router, cookbookFileURLByPathFromPayload(t, mustGetCookbookObjectForAPIVersion(t, afterUpdate.router, "/cookbooks/rehydrate-api-matrix/1.0.0", "2"), "recipes/default.rb"), "puts 'updated after restart'")

	deleteCookbook := serveSignedAPIVersionRequest(t, afterUpdate.router, "pivotal", http.MethodDelete, "/cookbooks/rehydrate-api-matrix/1.0.0", nil, "2")
	assertCookbookAPIVersionStatus(t, deleteCookbook, http.StatusOK, "delete rehydrated v2 cookbook")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, deleteCookbook), "recipes/default.rb", updatedChecksum, "ponyville")

	deleteArtifact := serveSignedAPIVersionRequest(t, afterUpdate.router, "pivotal", http.MethodDelete, "/organizations/ponyville/cookbook_artifacts/rehydrate-artifact-api-matrix/"+artifactIdentifier, nil, "2")
	assertCookbookAPIVersionStatus(t, deleteArtifact, http.StatusOK, "delete rehydrated v2 artifact")
	assertCookbookV2ReadShape(t, mustDecodeObject(t, deleteArtifact), "recipes/default.rb", artifactChecksum, "ponyville")

	afterDelete := afterUpdate.restart()
	assertCookbookMissing(t, afterDelete.router, "/cookbooks/rehydrate-api-matrix/1.0.0")
	assertCookbookArtifactMissing(t, afterDelete.router, "/organizations/ponyville/cookbook_artifacts/rehydrate-artifact-api-matrix/"+artifactIdentifier)
}

func TestCookbookAPIVersionInvalidAndProviderFailuresDoNotMutate(t *testing.T) {
	fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

	stableChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'stable api version body'"))
	createCookbookVersion(t, fixture.router, "stable-api-matrix", "1.0.0", stableChecksum, nil)

	blockedChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'blocked api version body'"))
	blockedCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/cookbooks/blocked-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("blocked-api-matrix", "1.0.0", blockedChecksum, nil)), "3")
	assertInvalidServerAPIVersionResponse(t, blockedCreate, "3")
	assertCookbookMissing(t, fixture.router, "/cookbooks/blocked-api-matrix/1.0.0")
	assertPersistedCookbookVersionMissing(t, fixture.postgres, "ponyville", "blocked-api-matrix")

	blockedUpdate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/cookbooks/stable-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("stable-api-matrix", "1.0.0", blockedChecksum, nil)), "3")
	assertInvalidServerAPIVersionResponse(t, blockedUpdate, "3")
	assertCookbookDownloadBody(t, fixture.router, cookbookFileURL(t, fixture.router, "/cookbooks/stable-api-matrix/1.0.0"), "puts 'stable api version body'")

	blockedDelete := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodDelete, "/cookbooks/stable-api-matrix/1.0.0", nil, "-1")
	assertInvalidServerAPIVersionResponse(t, blockedDelete, "-1")
	assertPersistedCookbookVersion(t, fixture.postgres, "ponyville", "stable-api-matrix", "1.0.0")

	createChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'provider create api version body'"))
	faultStore, control := newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
	faulted := fixture.restartWithBlob(faultStore)
	control.setExistsErr(blob.ErrUnavailable)
	createUnavailable := serveSignedAPIVersionRequest(t, faulted.router, "pivotal", http.MethodPut, "/cookbooks/provider-create-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("provider-create-api-matrix", "1.0.0", createChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, createUnavailable, http.StatusServiceUnavailable, "provider unavailable cookbook create")
	assertCookbookAPIError(t, createUnavailable.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	control.setExistsErr(nil)
	faulted = faulted.restart()
	assertCookbookMissing(t, faulted.router, "/cookbooks/provider-create-api-matrix/1.0.0")
	assertPersistedCookbookVersionMissing(t, faulted.postgres, "ponyville", "provider-create-api-matrix")

	replacementChecksum := uploadCookbookChecksum(t, faulted.router, []byte("puts 'provider update api version body'"))
	faultStore, control = newFaultingFilesystemCookbookBlobStore(t, faulted.blobRoot)
	faulted = faulted.restartWithBlob(faultStore)
	control.setExistsErr(blob.ErrUnavailable)
	updateUnavailable := serveSignedAPIVersionRequest(t, faulted.router, "pivotal", http.MethodPut, "/cookbooks/stable-api-matrix/1.0.0",
		mustMarshalSandboxJSON(t, cookbookVersionPayload("stable-api-matrix", "1.0.0", replacementChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, updateUnavailable, http.StatusServiceUnavailable, "provider unavailable cookbook update")
	assertCookbookAPIError(t, updateUnavailable.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	control.setExistsErr(nil)
	faulted = faulted.restart()
	assertCookbookDownloadBody(t, faulted.router, cookbookFileURL(t, faulted.router, "/cookbooks/stable-api-matrix/1.0.0"), "puts 'stable api version body'")

	artifactChecksum := uploadCookbookChecksum(t, faulted.router, []byte("puts 'provider artifact api version body'"))
	artifactIdentifier := "4444444444444444444444444444444444444444"
	faultStore, control = newFaultingFilesystemCookbookBlobStore(t, faulted.blobRoot)
	faulted = faulted.restartWithBlob(faultStore)
	control.setExistsErr(blob.ErrUnavailable)
	artifactUnavailable := serveSignedAPIVersionRequest(t, faulted.router, "pivotal", http.MethodPut, "/organizations/ponyville/cookbook_artifacts/provider-artifact-api-matrix/"+artifactIdentifier,
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("provider-artifact-api-matrix", artifactIdentifier, "1.0.0", artifactChecksum, nil)), "2")
	assertCookbookAPIVersionStatus(t, artifactUnavailable, http.StatusServiceUnavailable, "provider unavailable artifact create")
	assertCookbookAPIError(t, artifactUnavailable.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")
	control.setExistsErr(nil)
	faulted = faulted.restart()
	assertCookbookArtifactMissing(t, faulted.router, "/organizations/ponyville/cookbook_artifacts/provider-artifact-api-matrix/"+artifactIdentifier)
	assertPersistedCookbookArtifactMissing(t, faulted.postgres, "ponyville", "provider-artifact-api-matrix", artifactIdentifier)

	faultStore, control = newFaultingFilesystemCookbookBlobStore(t, faulted.blobRoot)
	faulted = faulted.restartWithBlob(faultStore)
	control.setGetErr(blob.ErrUnavailable)
	downloadURL := cookbookFileURL(t, faulted.router, "/cookbooks/stable-api-matrix/1.0.0")
	downloadReq := httptest.NewRequest(http.MethodGet, downloadURL, nil)
	downloadRec := httptest.NewRecorder()
	faulted.router.ServeHTTP(downloadRec, downloadReq)
	assertCookbookAPIVersionStatus(t, downloadRec, http.StatusServiceUnavailable, "provider unavailable cookbook download")
	assertCookbookAPIError(t, downloadRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
}

func assertCookbookAPIVersionStatus(t *testing.T, rec *httptest.ResponseRecorder, want int, context string) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("%s status = %d, want %d, body = %s", context, rec.Code, want, rec.Body.String())
	}
}

func assertCookbookVersionCollectionForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion, basePath, name string, wantVersions ...string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "cookbook collection "+path)
	payload := mustDecodeObject(t, rec)
	entry, ok := payload[name].(map[string]any)
	if !ok {
		t.Fatalf("%s missing cookbook %q: %v", path, name, payload)
	}
	if got := entry["url"]; got != basePath+"/"+name {
		t.Fatalf("%s %q url = %v, want %q", path, name, got, basePath+"/"+name)
	}
	rawVersions, ok := entry["versions"].([]any)
	if !ok {
		t.Fatalf("%s versions = %T, want []any (%v)", path, entry["versions"], entry)
	}
	gotVersions := make([]string, 0, len(rawVersions))
	for _, raw := range rawVersions {
		version, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("%s version entry = %T, want map[string]any", path, raw)
		}
		gotVersions = append(gotVersions, version["version"].(string))
		if wantURL := basePath + "/" + name + "/" + version["version"].(string); version["url"] != wantURL {
			t.Fatalf("%s version url = %v, want %q", path, version["url"], wantURL)
		}
	}
	if !reflect.DeepEqual(gotVersions, wantVersions) {
		t.Fatalf("%s versions = %v, want %v", path, gotVersions, wantVersions)
	}
}

func assertCookbookLatestMapForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion, cookbook, wantURL string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "cookbook latest "+path)
	payload := mustDecodeObject(t, rec)
	if got := payload[cookbook]; got != wantURL {
		t.Fatalf("%s payload[%q] = %v, want %q (%v)", path, cookbook, got, wantURL, payload)
	}
}

func assertCookbookRecipesContainForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion string, want ...string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "cookbook recipes "+path)
	got := mustDecodeStringSlice(t, rec)
	sort.Strings(got)
	for _, recipe := range want {
		idx := sort.SearchStrings(got, recipe)
		if idx >= len(got) || got[idx] != recipe {
			t.Fatalf("%s recipes = %v, want to contain %q", path, got, recipe)
		}
	}
}

func assertUniverseContainsForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion, cookbook, version, locationPath string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "universe "+path)
	payload := mustDecodeObject(t, rec)
	cookbookEntry, ok := payload[cookbook].(map[string]any)
	if !ok {
		t.Fatalf("%s missing universe cookbook %q: %v", path, cookbook, payload)
	}
	versionEntry, ok := cookbookEntry[version].(map[string]any)
	if !ok {
		t.Fatalf("%s missing universe version %q/%q: %v", path, cookbook, version, cookbookEntry)
	}
	if got := versionEntry["location_path"]; got != locationPath {
		t.Fatalf("%s universe location_path = %v, want %q", path, got, locationPath)
	}
	if got := versionEntry["location_type"]; got != "chef_server" {
		t.Fatalf("%s universe location_type = %v, want chef_server", path, got)
	}
	if _, ok := versionEntry["dependencies"].(map[string]any); !ok {
		t.Fatalf("%s universe dependencies = %T, want map[string]any", path, versionEntry["dependencies"])
	}
}

func assertCookbookArtifactCollectionForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion, basePath, name, identifier string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "artifact collection "+path)
	payload := mustDecodeObject(t, rec)
	entry, ok := payload[name].(map[string]any)
	if !ok {
		t.Fatalf("%s missing artifact %q: %v", path, name, payload)
	}
	if got := entry["url"]; got != basePath+"/"+name {
		t.Fatalf("%s %q url = %v, want %q", path, name, got, basePath+"/"+name)
	}
	rawVersions, ok := entry["versions"].([]any)
	if !ok || len(rawVersions) != 1 {
		t.Fatalf("%s versions = %T/%v, want one entry", path, entry["versions"], entry["versions"])
	}
	version := rawVersions[0].(map[string]any)
	if got := version["identifier"]; got != identifier {
		t.Fatalf("%s identifier = %v, want %q", path, got, identifier)
	}
	if wantURL := basePath + "/" + name + "/" + identifier; version["url"] != wantURL {
		t.Fatalf("%s artifact url = %v, want %q", path, version["url"], wantURL)
	}
}

func assertCookbookLegacyWriteShape(t *testing.T, payload map[string]any, path, checksum string) {
	t.Helper()

	if _, ok := payload["all_files"]; ok {
		t.Fatalf("legacy write payload unexpectedly includes all_files: %v", payload)
	}
	file := assertCookbookLegacyPath(t, payload, cookbookFileSegment(path), path, checksum)
	if _, ok := file["url"]; ok {
		t.Fatalf("legacy write file unexpectedly includes url: %v", file)
	}
}

func assertCookbookLegacyReadShape(t *testing.T, payload map[string]any, path, checksum, org string) {
	t.Helper()

	if _, ok := payload["all_files"]; ok {
		t.Fatalf("legacy read payload unexpectedly includes all_files: %v", payload)
	}
	file := assertCookbookLegacyPath(t, payload, cookbookFileSegment(path), path, checksum)
	rawURL, ok := file["url"].(string)
	if !ok {
		t.Fatalf("legacy read file url = %T, want string (%v)", file["url"], file)
	}
	assertCookbookDownloadURLShape(t, rawURL, checksum, org)
}

func assertCookbookLegacyPath(t *testing.T, payload map[string]any, segment, path, checksum string) map[string]any {
	t.Helper()

	rawFiles, ok := payload[segment].([]any)
	if !ok {
		t.Fatalf("legacy segment %q = %T, want []any (%v)", segment, payload[segment], payload)
	}
	for _, raw := range rawFiles {
		file, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("legacy file = %T, want map[string]any", raw)
		}
		if file["path"] == path {
			if file["checksum"] != checksum {
				t.Fatalf("legacy file %q checksum = %v, want %q", path, file["checksum"], checksum)
			}
			return file
		}
	}
	t.Fatalf("legacy segment %q missing path %q in %v", segment, path, rawFiles)
	return nil
}

func assertCookbookLegacySegmentEmpty(t *testing.T, payload map[string]any, segment string) {
	t.Helper()

	rawFiles, ok := payload[segment].([]any)
	if !ok {
		t.Fatalf("legacy segment %q = %T, want []any (%v)", segment, payload[segment], payload)
	}
	if len(rawFiles) != 0 {
		t.Fatalf("legacy segment %q = %v, want empty", segment, rawFiles)
	}
}

func assertCookbookV2WriteShape(t *testing.T, payload map[string]any, path, checksum string) {
	t.Helper()

	assertCookbookNoLegacySegments(t, payload)
	file := cookbookFileByPath(t, payload, path)
	if file["checksum"] != checksum {
		t.Fatalf("v2 write file %q checksum = %v, want %q", path, file["checksum"], checksum)
	}
	if _, ok := file["url"]; ok {
		t.Fatalf("v2 write file unexpectedly includes url: %v", file)
	}
}

func assertCookbookV2ReadShape(t *testing.T, payload map[string]any, path, checksum, org string) {
	t.Helper()

	assertCookbookNoLegacySegments(t, payload)
	file := cookbookFileByPath(t, payload, path)
	if file["checksum"] != checksum {
		t.Fatalf("v2 read file %q checksum = %v, want %q", path, file["checksum"], checksum)
	}
	rawURL, ok := file["url"].(string)
	if !ok {
		t.Fatalf("v2 read file url = %T, want string (%v)", file["url"], file)
	}
	assertCookbookDownloadURLShape(t, rawURL, checksum, org)
}

func assertCookbookNoLegacySegments(t *testing.T, payload map[string]any) {
	t.Helper()

	if _, ok := payload["all_files"].([]any); !ok {
		t.Fatalf("payload all_files = %T, want []any (%v)", payload["all_files"], payload)
	}
	for _, segment := range legacyCookbookSegments {
		if _, ok := payload[segment]; ok {
			t.Fatalf("v2 payload unexpectedly includes legacy segment %q: %v", segment, payload)
		}
	}
}

func cookbookFileByPath(t *testing.T, payload map[string]any, path string) map[string]any {
	t.Helper()

	rawFiles, ok := payload["all_files"].([]any)
	if !ok {
		t.Fatalf("all_files = %T, want []any (%v)", payload["all_files"], payload)
	}
	for _, raw := range rawFiles {
		file, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("all_files entry = %T, want map[string]any", raw)
		}
		if file["path"] == path {
			return file
		}
	}
	t.Fatalf("all_files missing path %q in %v", path, rawFiles)
	return nil
}

func assertCookbookAllFilePathsFromPayload(t *testing.T, payload map[string]any, want ...string) {
	t.Helper()

	rawFiles, ok := payload["all_files"].([]any)
	if !ok {
		t.Fatalf("all_files = %T, want []any (%v)", payload["all_files"], payload)
	}
	got := make([]string, 0, len(rawFiles))
	for _, raw := range rawFiles {
		file, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("all_files entry = %T, want map[string]any", raw)
		}
		got = append(got, file["path"].(string))
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("all_files paths = %v, want %v", got, want)
	}
}

func cookbookFileURLByPathFromPayload(t *testing.T, payload map[string]any, path string) string {
	t.Helper()

	file := cookbookFileByPath(t, payload, path)
	rawURL, ok := file["url"].(string)
	if !ok {
		t.Fatalf("file %q url = %T, want string (%v)", path, file["url"], file)
	}
	return rawURL
}

func assertCookbookDownloadURLShape(t *testing.T, rawURL, checksum, org string) {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", rawURL, err)
	}
	if !strings.HasSuffix(parsed.Path, "/_blob/checksums/"+checksum) {
		t.Fatalf("download URL path = %q, want suffix /_blob/checksums/%s", parsed.Path, checksum)
	}
	values := parsed.Query()
	if values.Get("org") != org {
		t.Fatalf("download URL org = %q, want %q (%s)", values.Get("org"), org, rawURL)
	}
	if values.Get("expires") == "" {
		t.Fatalf("download URL missing expires: %s", rawURL)
	}
	if values.Get("signature") == "" {
		t.Fatalf("download URL missing signature: %s", rawURL)
	}
}

func mustGetCookbookObjectForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion string) map[string]any {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	assertCookbookAPIVersionStatus(t, rec, http.StatusOK, "GET "+path)
	return mustDecodeObject(t, rec)
}

func legacySegmentCookbookPayload(name, version, recipeChecksum, rootChecksum string) map[string]any {
	payload := cookbookVersionPayload(name, version, "", nil)
	delete(payload, "all_files")
	payload["recipes"] = []any{
		map[string]any{
			"name":        "default.rb",
			"path":        "recipes/default.rb",
			"checksum":    recipeChecksum,
			"specificity": "default",
		},
	}
	payload["root_files"] = []any{
		map[string]any{
			"name":        "CHANGELOG.md",
			"path":        "CHANGELOG.md",
			"checksum":    rootChecksum,
			"specificity": "default",
		},
	}
	return payload
}
