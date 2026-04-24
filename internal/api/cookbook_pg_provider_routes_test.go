package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

type activePostgresCookbookRouteFixture struct {
	t         *testing.T
	state     *pgtest.State
	postgres  *pg.Store
	blobStore blob.Store
	blobRoot  string
	router    http.Handler
}

func newActivePostgresFilesystemCookbookRouteFixture(t *testing.T) *activePostgresCookbookRouteFixture {
	t.Helper()
	return newActivePostgresFilesystemCookbookRouteFixtureWithSeed(t, pgtest.Seed{})
}

func newActivePostgresFilesystemCookbookRouteFixtureWithSeed(t *testing.T, seed pgtest.Seed) *activePostgresCookbookRouteFixture {
	t.Helper()

	root := t.TempDir()
	fileStore, err := blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	return newActivePostgresCookbookRouteFixtureWithBlob(t, pgtest.NewState(seed), fileStore, root)
}

func newActivePostgresCookbookRouteFixtureWithBlob(t *testing.T, state *pgtest.State, blobStore blob.Store, blobRoot string) *activePostgresCookbookRouteFixture {
	t.Helper()

	db, cleanup, err := state.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup postgres state error = %v", err)
		}
	})

	postgresStore := pg.New("postgres://pgtest")
	if err := postgresStore.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v", err)
	}

	opts := bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return postgresStore.CookbookStore()
		},
	}

	router := newTestRouterWithBootstrapOptionsAndBlobAndPostgres(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, opts, nil, nil, blobStore, postgresStore)

	return &activePostgresCookbookRouteFixture{
		t:         t,
		state:     state,
		postgres:  postgresStore,
		blobStore: blobStore,
		blobRoot:  blobRoot,
		router:    router,
	}
}

func (f *activePostgresCookbookRouteFixture) restart() *activePostgresCookbookRouteFixture {
	f.t.Helper()

	fileStore, err := blob.NewFileStore(f.blobRoot)
	if err != nil {
		f.t.Fatalf("NewFileStore() error = %v", err)
	}
	return newActivePostgresCookbookRouteFixtureWithBlob(f.t, f.state, fileStore, f.blobRoot)
}

func (f *activePostgresCookbookRouteFixture) restartWithBlob(blobStore blob.Store) *activePostgresCookbookRouteFixture {
	f.t.Helper()
	return newActivePostgresCookbookRouteFixtureWithBlob(f.t, f.state, blobStore, f.blobRoot)
}

func (f *activePostgresCookbookRouteFixture) assertBlobExists(checksum string, want bool) {
	f.t.Helper()

	fileStore, err := blob.NewFileStore(f.blobRoot)
	if err != nil {
		f.t.Fatalf("NewFileStore() error = %v", err)
	}
	exists, err := fileStore.Exists(context.Background(), checksum)
	if err != nil {
		f.t.Fatalf("Exists(%q) error = %v", checksum, err)
	}
	if exists != want {
		f.t.Fatalf("Exists(%q) = %v, want %v", checksum, exists, want)
	}
}

type cookbookRouteBlobFaultControl struct {
	mu         sync.Mutex
	existsErr  error
	getErr     error
	deleteErrs map[string]error
}

func newFaultingFilesystemCookbookBlobStore(t *testing.T, root string) (blob.Store, *cookbookRouteBlobFaultControl) {
	t.Helper()

	base, err := blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	control := &cookbookRouteBlobFaultControl{
		deleteErrs: map[string]error{},
	}
	return &faultingCookbookRouteBlobStore{base: base, control: control}, control
}

func (c *cookbookRouteBlobFaultControl) setExistsErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.existsErr = err
}

func (c *cookbookRouteBlobFaultControl) setGetErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.getErr = err
}

func (c *cookbookRouteBlobFaultControl) setDeleteErr(checksum string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	checksum = strings.ToLower(strings.TrimSpace(checksum))
	if err == nil {
		delete(c.deleteErrs, checksum)
		return
	}
	c.deleteErrs[checksum] = err
}

func (c *cookbookRouteBlobFaultControl) existsFault() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.existsErr
}

func (c *cookbookRouteBlobFaultControl) getFault() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getErr
}

func (c *cookbookRouteBlobFaultControl) deleteFault(checksum string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteErrs[strings.ToLower(strings.TrimSpace(checksum))]
}

type faultingCookbookRouteBlobStore struct {
	base    *blob.FileStore
	control *cookbookRouteBlobFaultControl
}

func (s *faultingCookbookRouteBlobStore) Name() string {
	return s.base.Name()
}

func (s *faultingCookbookRouteBlobStore) Status() blob.Status {
	return s.base.Status()
}

func (s *faultingCookbookRouteBlobStore) Put(ctx context.Context, req blob.PutRequest) (blob.PutResult, error) {
	return s.base.Put(ctx, req)
}

func (s *faultingCookbookRouteBlobStore) Exists(ctx context.Context, checksum string) (bool, error) {
	if err := s.control.existsFault(); err != nil {
		return false, err
	}
	return s.base.Exists(ctx, checksum)
}

func (s *faultingCookbookRouteBlobStore) Get(ctx context.Context, checksum string) ([]byte, error) {
	if err := s.control.getFault(); err != nil {
		return nil, err
	}
	return s.base.Get(ctx, checksum)
}

func (s *faultingCookbookRouteBlobStore) Delete(ctx context.Context, checksum string) error {
	if err := s.control.deleteFault(checksum); err != nil {
		return err
	}
	return s.base.Delete(ctx, checksum)
}

func assertCookbookCollectionVersions(t *testing.T, router http.Handler, path, cookbook string, wantVersions ...string) {
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
	assertCookbookVersionList(t, payload, cookbook, wantVersions...)
}

func assertCookbookRecipes(t *testing.T, router http.Handler, path string, want ...string) {
	t.Helper()

	req := newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload []string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}

	got := append([]string(nil), payload...)
	sort.Strings(got)
	wantCopy := append([]string(nil), want...)
	sort.Strings(wantCopy)
	if strings.Join(got, ",") != strings.Join(wantCopy, ",") {
		t.Fatalf("%s recipes = %v, want %v", path, got, wantCopy)
	}
}

func assertCookbookLatestEntry(t *testing.T, router http.Handler, path, cookbook, want string) {
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
	if payload[cookbook] != want {
		t.Fatalf("%s payload[%q] = %v, want %q (%v)", path, cookbook, payload[cookbook], want, payload)
	}
}

func assertUniverseHasVersion(t *testing.T, router http.Handler, path, cookbook, version string) {
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

	entry, ok := payload[cookbook].(map[string]any)
	if !ok {
		t.Fatalf("%s payload[%q] missing: %v", path, cookbook, payload)
	}
	if _, ok := entry[version]; !ok {
		t.Fatalf("%s payload[%q] missing version %q: %v", path, cookbook, version, entry)
	}
}

func assertCookbookDownloadBody(t *testing.T, router http.Handler, url, want string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", url, rec.Code, http.StatusOK, rec.Body.String())
	}
	if rec.Body.String() != want {
		t.Fatalf("GET %s body = %q, want %q", url, rec.Body.String(), want)
	}
}

func uploadCookbookChecksumWithoutCommit(t *testing.T, router http.Handler, content []byte) string {
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

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/x-binary")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}

	return checksum
}

func assertPersistedCookbookVersion(t *testing.T, store *pg.Store, org, name string, wantVersions ...string) {
	t.Helper()

	versions, orgOK, found := store.CookbookStore().ListCookbookVersionsByName(org, name)
	if !orgOK || !found {
		t.Fatalf("ListCookbookVersionsByName(%q, %q) = %v/%v/%v, want versions %v", org, name, orgOK, found, versions, wantVersions)
	}
	if len(versions) != len(wantVersions) {
		t.Fatalf("persisted version len = %d, want %d (%v)", len(versions), len(wantVersions), versions)
	}
	for idx, want := range wantVersions {
		if versions[idx].Version != want {
			t.Fatalf("persisted version[%d] = %q, want %q (%v)", idx, versions[idx].Version, want, versions)
		}
	}
}

func assertPersistedCookbookVersionMissing(t *testing.T, store *pg.Store, org, name string) {
	t.Helper()

	versions, orgOK, found := store.CookbookStore().ListCookbookVersionsByName(org, name)
	if !orgOK {
		t.Fatalf("organization %q missing while checking %q", org, name)
	}
	if found || len(versions) != 0 {
		t.Fatalf("ListCookbookVersionsByName(%q, %q) = %v/%v/%v, want no persisted versions", org, name, orgOK, found, versions)
	}
}

func assertPersistedCookbookArtifact(t *testing.T, store *pg.Store, org, name, identifier string) {
	t.Helper()

	artifact, orgOK, found := store.CookbookStore().GetCookbookArtifact(org, name, identifier)
	if !orgOK || !found {
		t.Fatalf("GetCookbookArtifact(%q, %q, %q) = %v/%v/%v, want persisted artifact", org, name, identifier, orgOK, found, artifact)
	}
	if artifact.Identifier != identifier {
		t.Fatalf("artifact.Identifier = %q, want %q", artifact.Identifier, identifier)
	}
}

func assertPersistedCookbookArtifactMissing(t *testing.T, store *pg.Store, org, name, identifier string) {
	t.Helper()

	artifact, orgOK, found := store.CookbookStore().GetCookbookArtifact(org, name, identifier)
	if !orgOK {
		t.Fatalf("organization %q missing while checking artifact %q/%q", org, name, identifier)
	}
	if found {
		t.Fatalf("GetCookbookArtifact(%q, %q, %q) = %v/%v/%v, want missing artifact", org, name, identifier, orgOK, found, artifact)
	}
}

func TestActivePostgresFilesystemCookbookReadDownloadParityAfterRestart(t *testing.T) {
	fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

	defaultChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'default postgres provider body'"))
	createCookbookVersion(t, fixture.router, "demo", "1.2.3", defaultChecksum, map[string]string{"apt": ">= 1.0.0"})
	createCookbookArtifact(t, fixture.router, "demo", "1111111111111111111111111111111111111111", "1.2.3", defaultChecksum, map[string]string{"apt": ">= 1.0.0"})

	orgChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'org postgres provider body'"))
	createOrgCookbookVersion(t, fixture.router, "ponyville", "org-demo", "2.0.0", orgChecksum, map[string]string{"yum": ">= 2.0.0"})
	createOrgCookbookArtifact(t, fixture.router, "ponyville", "org-demo", "2222222222222222222222222222222222222222", "2.0.0", orgChecksum, map[string]string{"yum": ">= 2.0.0"})

	restarted := fixture.restart()

	assertCookbookCollectionVersions(t, restarted.router, "/cookbooks", "demo", "1.2.3")
	assertCookbookCollectionVersions(t, restarted.router, "/cookbooks/demo", "demo", "1.2.3")
	assertCookbookDescription(t, restarted.router, "/cookbooks/demo/1.2.3", "compatibility cookbook")
	assertCookbookLatestEntry(t, restarted.router, "/cookbooks/_latest", "demo", "/cookbooks/demo/1.2.3")
	assertCookbookDescription(t, restarted.router, "/cookbooks/demo/_latest", "compatibility cookbook")
	assertCookbookRecipes(t, restarted.router, "/cookbooks/_recipes", "demo", "org-demo")
	assertUniverseHasVersion(t, restarted.router, "/universe", "demo", "1.2.3")
	assertCookbookArtifactDescription(t, restarted.router, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", "compatibility cookbook")

	defaultCookbookURL := cookbookFileURL(t, restarted.router, "/cookbooks/demo/1.2.3")
	assertCookbookDownloadBody(t, restarted.router, defaultCookbookURL, "puts 'default postgres provider body'")
	defaultArtifactURL := cookbookArtifactFileURL(t, restarted.router, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111")
	assertCookbookDownloadBody(t, restarted.router, defaultArtifactURL, "puts 'default postgres provider body'")

	assertCookbookCollectionVersions(t, restarted.router, "/organizations/ponyville/cookbooks", "org-demo", "2.0.0")
	assertCookbookCollectionVersions(t, restarted.router, "/organizations/ponyville/cookbooks/org-demo", "org-demo", "2.0.0")
	assertCookbookDescription(t, restarted.router, "/organizations/ponyville/cookbooks/org-demo/2.0.0", "compatibility cookbook")
	assertCookbookLatestEntry(t, restarted.router, "/organizations/ponyville/cookbooks/_latest", "org-demo", "/organizations/ponyville/cookbooks/org-demo/2.0.0")
	assertCookbookDescription(t, restarted.router, "/organizations/ponyville/cookbooks/org-demo/_latest", "compatibility cookbook")
	assertCookbookRecipes(t, restarted.router, "/organizations/ponyville/cookbooks/_recipes", "demo", "org-demo")
	assertUniverseHasVersion(t, restarted.router, "/organizations/ponyville/universe", "org-demo", "2.0.0")
	assertCookbookArtifactDescription(t, restarted.router, "/organizations/ponyville/cookbook_artifacts/org-demo/2222222222222222222222222222222222222222", "compatibility cookbook")

	orgCookbookURL := cookbookFileURL(t, restarted.router, "/organizations/ponyville/cookbooks/org-demo/2.0.0")
	assertCookbookDownloadBody(t, restarted.router, orgCookbookURL, "puts 'org postgres provider body'")
	orgArtifactURL := cookbookArtifactFileURL(t, restarted.router, "/organizations/ponyville/cookbook_artifacts/org-demo/2222222222222222222222222222222222222222")
	assertCookbookDownloadBody(t, restarted.router, orgArtifactURL, "puts 'org postgres provider body'")
}

func TestActivePostgresFilesystemCookbookMutationParityAndPersistence(t *testing.T) {
	fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

	originalChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'version original body'"))
	createPayload := cookbookVersionPayload("demo", "1.2.3", originalChecksum, nil)
	createPayload["metadata"].(map[string]any)["description"] = "postgres original description"
	createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, createPayload))
	createRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create cookbook status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	assertPersistedCookbookVersion(t, fixture.postgres, "ponyville", "demo", "1.2.3")

	restarted := fixture.restart()
	assertCookbookDescription(t, restarted.router, "/cookbooks/demo/1.2.3", "postgres original description")
	originalURL := cookbookFileURL(t, restarted.router, "/cookbooks/demo/1.2.3")
	assertCookbookDownloadBody(t, restarted.router, originalURL, "puts 'version original body'")

	replacementChecksum := uploadCookbookChecksum(t, restarted.router, []byte("puts 'version replacement body'"))
	updatePayload := cookbookVersionPayload("demo", "1.2.3", replacementChecksum, nil)
	updatePayload["metadata"].(map[string]any)["description"] = "postgres updated description"
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, updatePayload))
	updateRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update cookbook status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	restarted = restarted.restart()
	assertCookbookDescription(t, restarted.router, "/cookbooks/demo/1.2.3", "postgres updated description")
	updatedURL := cookbookFileURL(t, restarted.router, "/cookbooks/demo/1.2.3")
	assertCookbookDownloadBody(t, restarted.router, updatedURL, "puts 'version replacement body'")

	missingChecksumPayload := cookbookVersionPayload("demo", "1.2.3", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil)
	missingChecksumPayload["metadata"].(map[string]any)["description"] = "should not persist"
	invalidReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/demo/1.2.3", mustMarshalSandboxJSON(t, missingChecksumPayload))
	invalidRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid cookbook update status = %d, want %d, body = %s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}

	restarted = restarted.restart()
	assertCookbookDescription(t, restarted.router, "/cookbooks/demo/1.2.3", "postgres updated description")
	updatedURL = cookbookFileURL(t, restarted.router, "/cookbooks/demo/1.2.3")
	assertCookbookDownloadBody(t, restarted.router, updatedURL, "puts 'version replacement body'")

	deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/demo/1.2.3", nil)
	deleteRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	restarted = restarted.restart()
	assertCookbookMissing(t, restarted.router, "/cookbooks/demo/1.2.3")
	assertPersistedCookbookVersionMissing(t, restarted.postgres, "ponyville", "demo")

	artifactChecksum := uploadCookbookChecksum(t, restarted.router, []byte("puts 'artifact postgres body'"))
	createOrgCookbookArtifact(t, restarted.router, "ponyville", "org-artifact", "1111111111111111111111111111111111111111", "2.0.0", artifactChecksum, nil)
	assertPersistedCookbookArtifact(t, restarted.postgres, "ponyville", "org-artifact", "1111111111111111111111111111111111111111")

	restarted = restarted.restart()
	assertCookbookArtifactDescription(t, restarted.router, "/organizations/ponyville/cookbook_artifacts/org-artifact/1111111111111111111111111111111111111111", "compatibility cookbook")
	artifactURL := cookbookArtifactFileURL(t, restarted.router, "/organizations/ponyville/cookbook_artifacts/org-artifact/1111111111111111111111111111111111111111")
	assertCookbookDownloadBody(t, restarted.router, artifactURL, "puts 'artifact postgres body'")

	conflictReq := newSignedJSONRequest(t, http.MethodPut, "/organizations/ponyville/cookbook_artifacts/org-artifact/1111111111111111111111111111111111111111",
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("org-artifact", "1111111111111111111111111111111111111111", "2.0.0", artifactChecksum, nil)))
	conflictRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(conflictRec, conflictReq)
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("artifact repeated put status = %d, want %d, body = %s", conflictRec.Code, http.StatusConflict, conflictRec.Body.String())
	}

	missingArtifactReq := newSignedJSONRequest(t, http.MethodPut, "/organizations/ponyville/cookbook_artifacts/missing-artifact/2222222222222222222222222222222222222222",
		mustMarshalSandboxJSON(t, cookbookArtifactPayload("missing-artifact", "2222222222222222222222222222222222222222", "2.0.0", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", nil)))
	missingArtifactRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(missingArtifactRec, missingArtifactReq)
	if missingArtifactRec.Code != http.StatusBadRequest {
		t.Fatalf("artifact missing checksum status = %d, want %d, body = %s", missingArtifactRec.Code, http.StatusBadRequest, missingArtifactRec.Body.String())
	}

	restarted = restarted.restart()
	assertPersistedCookbookArtifact(t, restarted.postgres, "ponyville", "org-artifact", "1111111111111111111111111111111111111111")
	assertPersistedCookbookArtifactMissing(t, restarted.postgres, "ponyville", "missing-artifact", "2222222222222222222222222222222222222222")

	deleteArtifactReq := newSignedJSONRequest(t, http.MethodDelete, "/organizations/ponyville/cookbook_artifacts/org-artifact/1111111111111111111111111111111111111111", nil)
	deleteArtifactRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(deleteArtifactRec, deleteArtifactReq)
	if deleteArtifactRec.Code != http.StatusOK {
		t.Fatalf("delete artifact status = %d, want %d, body = %s", deleteArtifactRec.Code, http.StatusOK, deleteArtifactRec.Body.String())
	}

	restarted = restarted.restart()
	assertCookbookArtifactMissing(t, restarted.router, "/organizations/ponyville/cookbook_artifacts/org-artifact/1111111111111111111111111111111111111111")
	assertPersistedCookbookArtifactMissing(t, restarted.postgres, "ponyville", "org-artifact", "1111111111111111111111111111111111111111")
}

func TestActivePostgresFilesystemChecksumCleanupRetention(t *testing.T) {
	t.Run("shared_across_versions_after_restart", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		sharedChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'shared version body'"))
		createCookbookVersion(t, fixture.router, "shared-demo", "1.0.0", sharedChecksum, nil)
		createCookbookVersion(t, fixture.router, "shared-demo", "1.0.1", sharedChecksum, nil)

		restarted := fixture.restart()
		sharedURL := cookbookFileURL(t, restarted.router, "/cookbooks/shared-demo/1.0.1")

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/shared-demo/1.0.0", nil)
		deleteRec := httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("delete first shared version status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertCookbookDownloadBody(t, restarted.router, sharedURL, "puts 'shared version body'")

		deleteReq = newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/shared-demo/1.0.1", nil)
		deleteRec = httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("delete final shared version status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertBlobDownloadStatus(t, restarted.router, sharedURL, http.StatusNotFound)
	})

	t.Run("shared_across_artifacts_after_restart", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		sharedChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'shared artifact body'"))
		createCookbookArtifact(t, fixture.router, "shared-artifact", "1111111111111111111111111111111111111111", "1.0.0", sharedChecksum, nil)
		createCookbookArtifact(t, fixture.router, "shared-artifact", "2222222222222222222222222222222222222222", "1.0.0", sharedChecksum, nil)

		restarted := fixture.restart()
		sharedURL := cookbookArtifactFileURL(t, restarted.router, "/cookbook_artifacts/shared-artifact/2222222222222222222222222222222222222222")

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/shared-artifact/1111111111111111111111111111111111111111", nil)
		deleteRec := httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("delete first shared artifact status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertCookbookDownloadBody(t, restarted.router, sharedURL, "puts 'shared artifact body'")

		deleteReq = newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/shared-artifact/2222222222222222222222222222222222222222", nil)
		deleteRec = httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("delete final shared artifact status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertBlobDownloadStatus(t, restarted.router, sharedURL, http.StatusNotFound)
	})

	t.Run("shared_between_version_and_artifact_after_restart", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		sharedChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'shared mixed body'"))
		createCookbookVersion(t, fixture.router, "shared-mixed", "1.0.0", sharedChecksum, nil)
		createCookbookArtifact(t, fixture.router, "shared-mixed", "3333333333333333333333333333333333333333", "1.0.0", sharedChecksum, nil)

		restarted := fixture.restart()
		sharedURL := cookbookFileURL(t, restarted.router, "/cookbooks/shared-mixed/1.0.0")

		deleteArtifactReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbook_artifacts/shared-mixed/3333333333333333333333333333333333333333", nil)
		deleteArtifactRec := httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteArtifactRec, deleteArtifactReq)
		if deleteArtifactRec.Code != http.StatusOK {
			t.Fatalf("delete shared artifact status = %d, want %d, body = %s", deleteArtifactRec.Code, http.StatusOK, deleteArtifactRec.Body.String())
		}
		assertCookbookDownloadBody(t, restarted.router, sharedURL, "puts 'shared mixed body'")

		deleteVersionReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/shared-mixed/1.0.0", nil)
		deleteVersionRec := httptest.NewRecorder()
		restarted.router.ServeHTTP(deleteVersionRec, deleteVersionReq)
		if deleteVersionRec.Code != http.StatusOK {
			t.Fatalf("delete shared mixed version status = %d, want %d, body = %s", deleteVersionRec.Code, http.StatusOK, deleteVersionRec.Body.String())
		}
		assertBlobDownloadStatus(t, restarted.router, sharedURL, http.StatusNotFound)
	})

	t.Run("sandbox_reference_preserves_blob", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		checksum := uploadCookbookChecksumWithoutCommit(t, fixture.router, []byte("puts 'sandbox held body'"))
		createCookbookVersion(t, fixture.router, "sandbox-held", "1.0.0", checksum, nil)

		downloadURL := cookbookFileURL(t, fixture.router, "/cookbooks/sandbox-held/1.0.0")

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/sandbox-held/1.0.0", nil)
		deleteRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("delete sandbox-held cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertCookbookDownloadBody(t, fixture.router, downloadURL, "puts 'sandbox held body'")
		fixture.assertBlobExists(checksum, true)
	})
}

func TestActivePostgresProviderUnavailableDegradation(t *testing.T) {
	t.Run("existence_failures_do_not_persist_invalid_rows", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)
		faultStore, control := newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
		fixture = fixture.restartWithBlob(faultStore)

		createChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'create unavailable body'"))
		control.setExistsErr(blob.ErrUnavailable)
		defer control.setExistsErr(nil)

		createReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/unavailable-create/1.2.3",
			mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-create", "1.2.3", createChecksum, nil)))
		createRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(createRec, createReq)
		if createRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("unavailable create status = %d, want %d, body = %s", createRec.Code, http.StatusServiceUnavailable, createRec.Body.String())
		}
		assertCookbookAPIError(t, createRec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")

		control.setExistsErr(nil)
		fixture = fixture.restart()
		assertCookbookMissing(t, fixture.router, "/cookbooks/unavailable-create/1.2.3")
		assertPersistedCookbookVersionMissing(t, fixture.postgres, "ponyville", "unavailable-create")

		originalChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'existing version body'"))
		createCookbookVersion(t, fixture.router, "unavailable-update", "1.2.3", originalChecksum, nil)

		replacementChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'replacement unavailable body'"))
		faultStore, control = newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
		fixture = fixture.restartWithBlob(faultStore)
		control.setExistsErr(blob.ErrUnavailable)
		defer control.setExistsErr(nil)

		updateReq := newSignedJSONRequest(t, http.MethodPut, "/cookbooks/unavailable-update/1.2.3",
			mustMarshalSandboxJSON(t, cookbookVersionPayload("unavailable-update", "1.2.3", replacementChecksum, nil)))
		updateRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(updateRec, updateReq)
		if updateRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("unavailable update status = %d, want %d, body = %s", updateRec.Code, http.StatusServiceUnavailable, updateRec.Body.String())
		}
		assertCookbookAPIError(t, updateRec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")

		control.setExistsErr(nil)
		fixture = fixture.restart()
		downloadURL := cookbookFileURL(t, fixture.router, "/cookbooks/unavailable-update/1.2.3")
		assertCookbookDownloadBody(t, fixture.router, downloadURL, "puts 'existing version body'")

		orgChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'org unavailable artifact body'"))
		faultStore, control = newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
		fixture = fixture.restartWithBlob(faultStore)
		control.setExistsErr(blob.ErrUnavailable)
		defer control.setExistsErr(nil)

		artifactReq := newSignedJSONRequest(t, http.MethodPut, "/organizations/ponyville/cookbook_artifacts/unavailable-artifact/1111111111111111111111111111111111111111",
			mustMarshalSandboxJSON(t, cookbookArtifactPayload("unavailable-artifact", "1111111111111111111111111111111111111111", "1.2.3", orgChecksum, nil)))
		artifactRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(artifactRec, artifactReq)
		if artifactRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("org artifact unavailable create status = %d, want %d, body = %s", artifactRec.Code, http.StatusServiceUnavailable, artifactRec.Body.String())
		}
		assertCookbookAPIError(t, artifactRec.Body.Bytes(), "blob_unavailable", "blob existence backend is not available")

		control.setExistsErr(nil)
		fixture = fixture.restart()
		assertCookbookArtifactMissing(t, fixture.router, "/organizations/ponyville/cookbook_artifacts/unavailable-artifact/1111111111111111111111111111111111111111")
		assertPersistedCookbookArtifactMissing(t, fixture.postgres, "ponyville", "unavailable-artifact", "1111111111111111111111111111111111111111")
	})

	t.Run("download_failures_preserve_error_shape", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		defaultChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'default unavailable download body'"))
		createCookbookVersion(t, fixture.router, "download-unavailable", "1.2.3", defaultChecksum, nil)
		orgChecksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'org unavailable download body'"))
		createOrgCookbookArtifact(t, fixture.router, "ponyville", "artifact-download-unavailable", "1111111111111111111111111111111111111111", "1.2.3", orgChecksum, nil)

		faultStore, control := newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
		fixture = fixture.restartWithBlob(faultStore)
		control.setGetErr(blob.ErrUnavailable)
		defer control.setGetErr(nil)

		defaultURL := cookbookFileURL(t, fixture.router, "/cookbooks/download-unavailable/1.2.3")
		defaultReq := httptest.NewRequest(http.MethodGet, defaultURL, nil)
		defaultRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(defaultRec, defaultReq)
		if defaultRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("default unavailable download status = %d, want %d, body = %s", defaultRec.Code, http.StatusServiceUnavailable, defaultRec.Body.String())
		}
		assertCookbookAPIError(t, defaultRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")

		orgURL := cookbookArtifactFileURL(t, fixture.router, "/organizations/ponyville/cookbook_artifacts/artifact-download-unavailable/1111111111111111111111111111111111111111")
		orgReq := httptest.NewRequest(http.MethodGet, orgURL, nil)
		orgRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(orgRec, orgReq)
		if orgRec.Code != http.StatusServiceUnavailable {
			t.Fatalf("org unavailable artifact download status = %d, want %d, body = %s", orgRec.Code, http.StatusServiceUnavailable, orgRec.Body.String())
		}
		assertCookbookAPIError(t, orgRec.Body.Bytes(), "blob_unavailable", "blob download backend is not available")
	})

	t.Run("cleanup_delete_failures_do_not_block_metadata_deletes", func(t *testing.T) {
		fixture := newActivePostgresFilesystemCookbookRouteFixture(t)

		checksum := uploadCookbookChecksum(t, fixture.router, []byte("puts 'cleanup failure body'"))
		createCookbookVersion(t, fixture.router, "cleanup-failure", "1.2.3", checksum, nil)

		faultStore, control := newFaultingFilesystemCookbookBlobStore(t, fixture.blobRoot)
		fixture = fixture.restartWithBlob(faultStore)
		downloadURL := cookbookFileURL(t, fixture.router, "/cookbooks/cleanup-failure/1.2.3")
		control.setDeleteErr(checksum, errors.New("simulated blob delete failure"))
		defer control.setDeleteErr(checksum, nil)

		deleteReq := newSignedJSONRequest(t, http.MethodDelete, "/cookbooks/cleanup-failure/1.2.3", nil)
		deleteRec := httptest.NewRecorder()
		fixture.router.ServeHTTP(deleteRec, deleteReq)
		if deleteRec.Code != http.StatusOK {
			t.Fatalf("cleanup failure delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
		}
		assertCookbookDownloadBody(t, fixture.router, downloadURL, "puts 'cleanup failure body'")

		fixture = fixture.restart()
		assertCookbookMissing(t, fixture.router, "/cookbooks/cleanup-failure/1.2.3")
		fixture.assertBlobExists(checksum, true)
	})
}
