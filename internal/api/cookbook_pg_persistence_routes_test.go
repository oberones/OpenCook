package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg"
)

func TestCookbookRoutesUseConfiguredPostgresCookbookStore(t *testing.T) {
	postgresStore := pg.New("postgres://example")
	router := newTestRouterWithBootstrapOptions(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return postgresStore.CookbookStore()
		},
	})

	defaultChecksum := uploadCookbookChecksum(t, router, []byte("puts 'default postgres cookbook store'"))
	createCookbookVersion(t, router, "demo", "1.2.3", defaultChecksum, map[string]string{"apt": ">= 1.0.0"})
	createCookbookArtifact(t, router, "demo", "1111111111111111111111111111111111111111", "1.2.3", defaultChecksum, map[string]string{"apt": ">= 1.0.0"})

	orgChecksum := uploadCookbookChecksum(t, router, []byte("puts 'org postgres cookbook store'"))
	createOrgCookbookVersion(t, router, "ponyville", "org-demo", "2.0.0", orgChecksum, map[string]string{"yum": ">= 2.0.0"})
	createOrgCookbookArtifact(t, router, "ponyville", "org-demo", "2222222222222222222222222222222222222222", "2.0.0", orgChecksum, map[string]string{"yum": ">= 2.0.0"})

	t.Run("default-org cookbook reads", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/1.2.3", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("default cookbook status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(default cookbook) error = %v", err)
		}
		if payload["version"] != "1.2.3" {
			t.Fatalf("payload.version = %v, want %q", payload["version"], "1.2.3")
		}

		artifactReq := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
		artifactRec := httptest.NewRecorder()
		router.ServeHTTP(artifactRec, artifactReq)
		if artifactRec.Code != http.StatusOK {
			t.Fatalf("default artifact status = %d, want %d, body = %s", artifactRec.Code, http.StatusOK, artifactRec.Body.String())
		}
	})

	t.Run("org-scoped cookbook reads", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/cookbooks/org-demo/2.0.0", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("org cookbook status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(org cookbook) error = %v", err)
		}
		if payload["version"] != "2.0.0" {
			t.Fatalf("payload.version = %v, want %q", payload["version"], "2.0.0")
		}

		artifactReq := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/cookbook_artifacts/org-demo/2222222222222222222222222222222222222222", nil)
		artifactRec := httptest.NewRecorder()
		router.ServeHTTP(artifactRec, artifactReq)
		if artifactRec.Code != http.StatusOK {
			t.Fatalf("org artifact status = %d, want %d, body = %s", artifactRec.Code, http.StatusOK, artifactRec.Body.String())
		}
	})

	cookbookStore := postgresStore.CookbookStore()
	if versions, orgOK, found := cookbookStore.ListCookbookVersionsByName("ponyville", "org-demo"); !orgOK || !found || len(versions) != 1 || versions[0].Version != "2.0.0" {
		t.Fatalf("postgres cookbook versions = %v/%v/%v, want org-demo 2.0.0 persisted in postgres store", orgOK, found, versions)
	}
	if artifacts, orgOK, found := cookbookStore.ListCookbookArtifactsByName("ponyville", "org-demo"); !orgOK || !found || len(artifacts) != 1 || artifacts[0].Identifier != "2222222222222222222222222222222222222222" {
		t.Fatalf("postgres cookbook artifacts = %v/%v/%v, want org-demo artifact persisted in postgres store", orgOK, found, artifacts)
	}
}
