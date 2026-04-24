package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/version"
)

type activePostgresBootstrapFixture struct {
	t       *testing.T
	pgState *pgtest.State
	router  http.Handler
}

func TestActivePostgresBootstrapUserKeysAuthenticateAfterRestart(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	privateKey := mustParsePrivateKey(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)

	body := []byte(`{"username":"rainbow","display_name":"Rainbow Dash","public_key":` + strconvQuote(publicKeyPEM) + `}`)
	req := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/users", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeadersWithPrivateKey(t, getReq, privateKey, "rainbow", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("persisted signed user read status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	keysReq := httptest.NewRequest(http.MethodGet, "/users/rainbow/keys/default", nil)
	applySignedHeaders(t, keysReq, "pivotal", "", http.MethodGet, "/users/rainbow/keys/default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	keysRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(keysRec, keysReq)
	if keysRec.Code != http.StatusOK {
		t.Fatalf("persisted user key read status = %d, want %d, body = %s", keysRec.Code, http.StatusOK, keysRec.Body.String())
	}
}

func TestActivePostgresBootstrapOrganizationArtifactsRehydrate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))

	body := []byte(`{"name":"canterlot","full_name":"Canterlot","org_type":"Business"}`)
	req := httptest.NewRequest(http.MethodPost, "/organizations", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/organizations", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create organization status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	for _, tc := range []struct {
		path string
		want int
	}{
		{path: "/organizations/canterlot", want: http.StatusOK},
		{path: "/organizations/canterlot/clients/canterlot-validator", want: http.StatusOK},
		{path: "/organizations/canterlot/groups/admins", want: http.StatusOK},
		{path: "/organizations/canterlot/containers/clients", want: http.StatusOK},
		{path: "/organizations/canterlot/_acl", want: http.StatusOK},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		applySignedHeaders(t, req, "pivotal", "", http.MethodGet, tc.path, nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, "2026-04-02T15:04:05Z")
		rec := httptest.NewRecorder()
		restarted.router.ServeHTTP(rec, req)
		if rec.Code != tc.want {
			t.Fatalf("%s status = %d, want %d, body = %s", tc.path, rec.Code, tc.want, rec.Body.String())
		}
	}

	groupReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot/groups/admins", nil)
	applySignedHeaders(t, groupReq, "pivotal", "", http.MethodGet, "/organizations/canterlot/groups/admins", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	groupRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(groupRec, groupReq)
	if groupRec.Code != http.StatusOK {
		t.Fatalf("group read status = %d, want %d, body = %s", groupRec.Code, http.StatusOK, groupRec.Body.String())
	}

	var groupPayload map[string]any
	if err := json.Unmarshal(groupRec.Body.Bytes(), &groupPayload); err != nil {
		t.Fatalf("json.Unmarshal(group) error = %v", err)
	}
	if values, ok := groupPayload["clients"].([]any); !ok || len(values) != 0 {
		t.Fatalf("group clients = %#v, want empty JSON array after restart", groupPayload["clients"])
	}
}

func newActivePostgresBootstrapFixture(t *testing.T, pgState *pgtest.State) *activePostgresBootstrapFixture {
	t.Helper()

	db, cleanup, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	t.Cleanup(func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	})

	postgresStore := pg.New("postgres://bootstrap-test")
	if err := postgresStore.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v", err)
	}

	keyStore := authn.NewMemoryKeyStore()
	initial, err := postgresStore.BootstrapCore().LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	state := bootstrap.NewService(keyStore, bootstrap.Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &initial,
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return postgresStore.CookbookStore()
		},
		BootstrapCoreStoreFactory: func(*bootstrap.Service) bootstrap.BootstrapCoreStore {
			return postgresStore.BootstrapCore()
		},
	})
	if err := state.RehydrateKeyStore(); err != nil {
		t.Fatalf("RehydrateKeyStore() error = %v", err)
	}

	privateKey := mustParsePrivateKey(t)
	if _, ok := state.GetUser("pivotal"); ok {
		keys, _ := state.ListUserKeys("pivotal")
		if len(keys) == 0 {
			mustPutKey(t, keyStore, authn.Key{
				ID: "default",
				Principal: authn.Principal{
					Type: "user",
					Name: "pivotal",
				},
				PublicKey: &privateKey.PublicKey,
			})
			publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)
			if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", publicKeyPEM); err != nil {
				t.Fatalf("SeedPublicKey(pivotal) error = %v", err)
			}
		}
	}

	skew := 15 * time.Minute
	now := func() time.Time {
		return mustParseTime(t, "2026-04-02T15:04:35Z")
	}
	router := NewRouter(Dependencies{
		Logger:           log.New(ioDiscard{}, "", 0),
		Config:           config.Config{ServiceName: "opencook", Environment: "test", MaxAuthBodyBytes: config.DefaultMaxAuthBodyBytes},
		Version:          version.Current(),
		Compat:           compat.NewDefaultRegistry(),
		Now:              now,
		Authn:            authn.NewChefVerifier(keyStore, authn.Options{AllowedClockSkew: &skew, Now: now}),
		Authz:            authz.NewACLAuthorizer(state),
		Bootstrap:        state,
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         postgresStore,
		CookbookBackend:  "postgres",
	})
	return &activePostgresBootstrapFixture{
		t:       t,
		pgState: pgState,
		router:  router,
	}
}

func strconvQuote(value string) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}
