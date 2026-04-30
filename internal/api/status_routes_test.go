package api

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"github.com/oberones/OpenCook/internal/version"
)

func TestReadyzReportsDependencyFailuresWithoutSecrets(t *testing.T) {
	cfg := config.Config{
		ServiceName:                     "opencook",
		Environment:                     "test",
		PostgresDSN:                     "postgres://opencook:postgres-secret@postgres.example/opencook?sslmode=require",
		OpenSearchURL:                   "https://search-user:search-secret@opensearch.example",
		BlobBackend:                     "s3",
		BlobStorageURL:                  "s3://private-bucket/opencook",
		BlobS3Endpoint:                  "https://access:secret@s3.example",
		BlobS3AccessKeyID:               "ACCESSSECRET",
		BlobS3SecretKey:                 "SUPERSECRET",
		BlobS3SessionToken:              "SESSIONSECRET",
		AuthSkew:                        15 * time.Minute,
		MaxAuthBodyBytes:                config.DefaultMaxAuthBodyBytes,
		MaxBlobUploadBytes:              config.DefaultMaxBlobUploadBytes,
		BootstrapRequestorPublicKeyPath: "/tmp/bootstrap-secret.pem",
	}
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{
		cfg:      cfg,
		postgres: pg.New(cfg.PostgresDSN),
		searchIndex: statusRouteSearchIndex{status: search.Status{
			Backend:    "opensearch",
			Configured: true,
			Message:    "OpenSearch is configured but unavailable; search routes cannot reach the provider",
		}},
		blobStore: statusRouteBlobStore{status: blob.Status{
			Backend:    "s3-compatible",
			Configured: false,
			Message:    "set OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY to enable S3-compatible blob request operations",
		}},
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	body := rec.Body.String()
	for _, secret := range []string{"postgres-secret", "search-secret", "access:secret", "SUPERSECRET", "SESSIONSECRET", "private-bucket"} {
		if strings.Contains(body, secret) {
			t.Fatalf("/readyz leaked secret %q in body: %s", secret, body)
		}
	}

	payload := decodeStatusRoutePayload(t, rec)
	if payload["mode"] != "bootstrap" {
		t.Fatalf("/readyz mode = %v, want bootstrap", payload["mode"])
	}
	readiness := statusRouteMap(t, payload, "readiness")
	if readiness["ready"] != false || readiness["status"] != "not_ready" {
		t.Fatalf("readiness = %v, want not_ready", readiness)
	}
	checks := statusRouteMap(t, readiness, "checks")
	for _, name := range []string{"postgres", "opensearch", "blob"} {
		check := statusRouteMap(t, checks, name)
		if check["ready"] != false {
			t.Fatalf("readiness.checks.%s.ready = %v, want false", name, check["ready"])
		}
	}
}

func TestStatusAndReadyzReportInMemoryFallbackAsReady(t *testing.T) {
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{})

	for _, path := range []string{"/_status", "/readyz"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
		}
		payload := decodeStatusRoutePayload(t, rec)
		readiness := statusRouteMap(t, payload, "readiness")
		if readiness["ready"] != true || readiness["status"] != "ready" {
			t.Fatalf("%s readiness = %v, want ready", path, readiness)
		}
		dependencies := statusRouteMap(t, payload, "dependencies")
		postgresStatus := statusRouteMap(t, dependencies, "postgres")
		if !strings.Contains(postgresStatus["message"].(string), "in-memory persistence") {
			t.Fatalf("%s postgres status = %v, want in-memory wording", path, postgresStatus)
		}
		openSearchStatus := statusRouteMap(t, dependencies, "opensearch")
		if !strings.Contains(openSearchStatus["message"].(string), "in-memory compatibility index") {
			t.Fatalf("%s opensearch status = %v, want in-memory wording", path, openSearchStatus)
		}
		blobStatus := statusRouteMap(t, dependencies, "blob")
		if !strings.Contains(blobStatus["message"].(string), "in-memory content-addressed storage") {
			t.Fatalf("%s blob status = %v, want in-memory wording", path, blobStatus)
		}
	}
}

func TestStatusRouteStaysInformationalWhenReadinessFails(t *testing.T) {
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{
		omitBootstrap: true,
	})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/_status", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/_status status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeStatusRoutePayload(t, rec)
	readiness := statusRouteMap(t, payload, "readiness")
	if readiness["ready"] != false {
		t.Fatalf("/_status readiness = %v, want false while status remains informational", readiness)
	}
}

type statusRouteTestDeps struct {
	cfg            config.Config
	bootstrapState *bootstrap.Service
	omitBootstrap  bool
	logger         *log.Logger
	postgres       *pg.Store
	searchIndex    search.Index
	blobStore      blob.Store
}

func newStatusRouteTestRouter(t *testing.T, deps statusRouteTestDeps) http.Handler {
	t.Helper()

	cfg := deps.cfg
	if cfg.ServiceName == "" {
		cfg.ServiceName = "opencook"
	}
	if cfg.Environment == "" {
		cfg.Environment = "test"
	}
	if cfg.AuthSkew == 0 {
		cfg.AuthSkew = 15 * time.Minute
	}
	if cfg.MaxAuthBodyBytes == 0 {
		cfg.MaxAuthBodyBytes = config.DefaultMaxAuthBodyBytes
	}
	if cfg.MaxBlobUploadBytes == 0 {
		cfg.MaxBlobUploadBytes = config.DefaultMaxBlobUploadBytes
	}

	keyStore := authn.NewMemoryKeyStore()
	state := deps.bootstrapState
	if state == nil && !deps.omitBootstrap {
		state = bootstrap.NewService(keyStore, bootstrap.Options{})
	}
	postgresStore := deps.postgres
	if postgresStore == nil {
		postgresStore = pg.New("")
	}
	searchIndex := deps.searchIndex
	if searchIndex == nil {
		searchIndex = search.NewMemoryIndex(state, "")
	}
	blobStore := deps.blobStore
	if blobStore == nil {
		blobStore = blob.NewMemoryStore("")
	}

	return NewRouter(Dependencies{
		Config:           cfg,
		Logger:           deps.logger,
		Version:          version.Info{Version: "test"},
		Compat:           compat.NewDefaultRegistry(),
		Authn:            authn.NewChefVerifier(keyStore, authn.Options{AllowedClockSkew: &cfg.AuthSkew}),
		Authz:            authz.NoopAuthorizer{},
		Bootstrap:        state,
		Blob:             blobStore,
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           searchIndex,
		Postgres:         postgresStore,
		CookbookBackend:  "memory-bootstrap",
	})
}

type statusRouteSearchIndex struct {
	status search.Status
}

func (i statusRouteSearchIndex) Name() string {
	return "status-route-search"
}

func (i statusRouteSearchIndex) Status() search.Status {
	return i.status
}

func (i statusRouteSearchIndex) Indexes(context.Context, string) ([]string, error) {
	return nil, search.ErrUnavailable
}

func (i statusRouteSearchIndex) Search(context.Context, search.Query) (search.Result, error) {
	return search.Result{}, search.ErrUnavailable
}

type statusRouteBlobStore struct {
	status blob.Status
}

func (s statusRouteBlobStore) Name() string {
	return "status-route-blob"
}

func (s statusRouteBlobStore) Status() blob.Status {
	return s.status
}

func decodeStatusRoutePayload(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal status body error = %v; body = %s", err, rec.Body.String())
	}
	return payload
}

func statusRouteMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any (%v)", key, parent[key], parent)
	}
	return value
}
