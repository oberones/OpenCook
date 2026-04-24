package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/version"
)

func TestBootstrapOptionsUseMemoryCookbookStoreWithoutConfiguredPostgres(t *testing.T) {
	opts := bootstrapOptions(config.Config{}, pg.New(""))
	if opts.CookbookStoreFactory != nil {
		t.Fatal("CookbookStoreFactory = non-nil, want nil when postgres is not configured")
	}
	if got := resolveCookbookBackend(pg.New("")); got != "memory-bootstrap" {
		t.Fatalf("resolveCookbookBackend(unconfigured) = %q, want %q", got, "memory-bootstrap")
	}
}

func TestBootstrapOptionsUsePostgresCookbookStoreWhenConfigured(t *testing.T) {
	store := pg.New("postgres://example")

	opts := bootstrapOptions(config.Config{}, store)
	if opts.CookbookStoreFactory == nil {
		t.Fatal("CookbookStoreFactory = nil, want postgres-backed cookbook store factory")
	}
	if got := resolveCookbookBackend(store); got != "postgres-configured" {
		t.Fatalf("resolveCookbookBackend(configured) = %q, want %q", got, "postgres-configured")
	}
}

func TestNewReturnsActivationFailure(t *testing.T) {
	previous := activatePostgresCookbookPersistence
	activatePostgresCookbookPersistence = func(context.Context, *pg.Store) error {
		return errors.New("activation failed")
	}
	defer func() {
		activatePostgresCookbookPersistence = previous
	}()

	_, err := New(config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		PostgresDSN:   "postgres://activation-test",
	}, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if err == nil {
		t.Fatal("New() error = nil, want activation failure")
	}
	if got := err.Error(); got != "activate postgres cookbook persistence: activation failed" {
		t.Fatalf("New() error = %q, want activation failure message", got)
	}
}

func TestSeedCookbookOrganizationsFromPostgresIsIdempotentAndPreservesFullName(t *testing.T) {
	state := pgtest.NewState(pgtest.Seed{
		Organizations: []pg.CookbookOrganizationRecord{
			{Name: "ponyville", FullName: "Ponyville"},
		},
	})
	db, cleanup, err := state.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	store := pg.New("postgres://seed-test")
	if err := store.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v", err)
	}

	bootstrapState := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return store.CookbookStore()
		},
	})

	if err := seedCookbookOrganizationsFromPostgres(bootstrapState, store); err != nil {
		t.Fatalf("seedCookbookOrganizationsFromPostgres() first error = %v", err)
	}
	if err := seedCookbookOrganizationsFromPostgres(bootstrapState, store); err != nil {
		t.Fatalf("seedCookbookOrganizationsFromPostgres() second error = %v", err)
	}

	org, ok := bootstrapState.GetOrganization("ponyville")
	if !ok {
		t.Fatal("GetOrganization(ponyville) = false, want true")
	}
	if org.FullName != "Ponyville" {
		t.Fatalf("org.FullName = %q, want %q", org.FullName, "Ponyville")
	}
}

func TestNewStatusReportsActivePostgresAndFilesystemBlob(t *testing.T) {
	state := pgtest.NewState(pgtest.Seed{
		Organizations: []pg.CookbookOrganizationRecord{
			{Name: "ponyville", FullName: "Ponyville"},
		},
	})
	db, cleanup, err := state.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	previous := activatePostgresCookbookPersistence
	activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
		return store.ActivateCookbookPersistenceWithDB(ctx, db)
	}
	defer func() {
		activatePostgresCookbookPersistence = previous
	}()

	app, err := New(config.Config{
		ServiceName:    "opencook",
		Environment:    "test",
		ListenAddress:  ":4000",
		AuthSkew:       15 * time.Minute,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		PostgresDSN:    "postgres://status-test",
		BlobBackend:    "filesystem",
		BlobStorageURL: t.TempDir(),
	}, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/_status", nil)
	rec := httptest.NewRecorder()
	app.server.Handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/_status status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(/_status) error = %v", err)
	}

	dependencies, ok := payload["dependencies"].(map[string]any)
	if !ok {
		t.Fatalf("dependencies = %T, want map[string]any (%v)", payload["dependencies"], payload)
	}
	cookbooks, ok := dependencies["cookbooks"].(map[string]any)
	if !ok {
		t.Fatalf("dependencies.cookbooks = %T, want map[string]any (%v)", dependencies["cookbooks"], dependencies)
	}
	if cookbooks["backend"] != "postgres" {
		t.Fatalf("dependencies.cookbooks.backend = %v, want %q", cookbooks["backend"], "postgres")
	}

	postgresStatus, ok := dependencies["postgres"].(map[string]any)
	if !ok {
		t.Fatalf("dependencies.postgres = %T, want map[string]any (%v)", dependencies["postgres"], dependencies)
	}
	if postgresStatus["message"] != "PostgreSQL cookbook and bootstrap core persistence active" {
		t.Fatalf("dependencies.postgres.message = %v, want active status", postgresStatus["message"])
	}

	blobStatus, ok := dependencies["blob"].(map[string]any)
	if !ok {
		t.Fatalf("dependencies.blob = %T, want map[string]any (%v)", dependencies["blob"], dependencies)
	}
	if blobStatus["backend"] != "filesystem" {
		t.Fatalf("dependencies.blob.backend = %v, want %q", blobStatus["backend"], "filesystem")
	}
}
