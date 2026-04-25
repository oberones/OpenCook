package app

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/version"
)

func TestBootstrapOptionsUseMemoryCookbookStoreWithoutConfiguredPostgres(t *testing.T) {
	opts, err := bootstrapOptions(config.Config{}, pg.New(""), nil)
	if err != nil {
		t.Fatalf("bootstrapOptions() error = %v", err)
	}
	if opts.CookbookStoreFactory != nil {
		t.Fatal("CookbookStoreFactory = non-nil, want nil when postgres is not configured")
	}
	if opts.BootstrapCoreStoreFactory != nil {
		t.Fatal("BootstrapCoreStoreFactory = non-nil, want nil when postgres is not configured")
	}
	if opts.CoreObjectStoreFactory != nil {
		t.Fatal("CoreObjectStoreFactory = non-nil, want nil when postgres is not configured")
	}
	if got := resolveCookbookBackend(pg.New("")); got != "memory-bootstrap" {
		t.Fatalf("resolveCookbookBackend(unconfigured) = %q, want %q", got, "memory-bootstrap")
	}
}

func TestBootstrapOptionsUsePostgresCookbookStoreWhenConfigured(t *testing.T) {
	store := pg.New("postgres://example")

	opts, err := bootstrapOptions(config.Config{}, store, nil)
	if err != nil {
		t.Fatalf("bootstrapOptions() error = %v", err)
	}
	if opts.CookbookStoreFactory == nil {
		t.Fatal("CookbookStoreFactory = nil, want postgres-backed cookbook store factory")
	}
	if opts.BootstrapCoreStoreFactory == nil {
		t.Fatal("BootstrapCoreStoreFactory = nil, want postgres-backed bootstrap core store factory")
	}
	if opts.CoreObjectStoreFactory == nil {
		t.Fatal("CoreObjectStoreFactory = nil, want postgres-backed core object store factory")
	}
	if got := resolveCookbookBackend(store); got != "postgres-configured" {
		t.Fatalf("resolveCookbookBackend(configured) = %q, want %q", got, "postgres-configured")
	}
}

func TestBootstrapOptionsLoadActivePostgresState(t *testing.T) {
	state := pgtest.NewState(pgtest.Seed{
		BootstrapCore: bootstrap.BootstrapCoreState{
			Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
				"ponyville": {
					Organization: bootstrap.Organization{
						Name:     "ponyville",
						FullName: "Ponyville",
						OrgType:  "Business",
						GUID:     "ponyville",
					},
				},
			},
		},
		CoreObjects: bootstrap.CoreObjectState{
			Orgs: map[string]bootstrap.CoreObjectOrganizationState{
				"ponyville": {
					Nodes: map[string]bootstrap.Node{
						"twilight": {
							Name:            "twilight",
							JSONClass:       "Chef::Node",
							ChefType:        "node",
							ChefEnvironment: "_default",
						},
					},
				},
			},
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

	store := pg.New("postgres://active-state-test")
	if err := store.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v", err)
	}

	opts, err := bootstrapOptions(config.Config{}, store, nil)
	if err != nil {
		t.Fatalf("bootstrapOptions() error = %v", err)
	}
	if opts.InitialBootstrapCoreState == nil {
		t.Fatal("InitialBootstrapCoreState = nil, want active postgres bootstrap state")
	}
	if org := opts.InitialBootstrapCoreState.Orgs["ponyville"].Organization; org.FullName != "Ponyville" {
		t.Fatalf("InitialBootstrapCoreState org = %#v, want Ponyville", org)
	}
	if opts.InitialCoreObjectState == nil {
		t.Fatal("InitialCoreObjectState = nil, want active postgres core object state")
	}
	node := opts.InitialCoreObjectState.Orgs["ponyville"].Nodes["twilight"]
	if node.Name != "twilight" || node.ChefEnvironment != "_default" {
		t.Fatalf("InitialCoreObjectState node = %#v, want rehydrated twilight node", node)
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

func TestNewReturnsOpenSearchEndpointValidationFailure(t *testing.T) {
	_, err := New(config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		OpenSearchURL: "ftp://opensearch.example",
	}, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if err == nil {
		t.Fatal("New() error = nil, want OpenSearch validation failure")
	}
	if got := err.Error(); got != "configure opensearch search: search backend invalid configuration: OPENCOOK_OPENSEARCH_URL must use http or https" {
		t.Fatalf("New() error = %q, want OpenSearch validation failure", got)
	}
}

func TestNewReturnsOpenSearchActivationFailure(t *testing.T) {
	state := pgtest.NewState(pgtest.Seed{})
	db, cleanup, err := state.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	previousPostgres := activatePostgresCookbookPersistence
	activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
		return store.ActivateCookbookPersistenceWithDB(ctx, db)
	}
	defer func() {
		activatePostgresCookbookPersistence = previousPostgres
	}()

	previousOpenSearch := activateOpenSearchIndexing
	activateOpenSearchIndexing = func(context.Context, config.Config, *pg.Store, *bootstrap.Service, *search.OpenSearchClient) (search.Index, error) {
		return nil, search.ErrUnavailable
	}
	defer func() {
		activateOpenSearchIndexing = previousOpenSearch
	}()

	_, err = New(config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		PostgresDSN:   "postgres://opensearch-activation-test",
		OpenSearchURL: "http://opensearch.example",
	}, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if !errors.Is(err, search.ErrUnavailable) {
		t.Fatalf("New() error = %v, want ErrUnavailable", err)
	}
	if got := err.Error(); got != "activate opensearch indexing: search backend unavailable" {
		t.Fatalf("New() error = %q, want stable OpenSearch activation failure", got)
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
	if postgresStatus["message"] != "PostgreSQL cookbook, bootstrap core, and core object persistence active" {
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

func TestNewCanConstructRepeatedlyAgainstSameActivePostgresState(t *testing.T) {
	state := pgtest.NewState(pgtest.Seed{
		BootstrapCore: bootstrap.BootstrapCoreState{
			Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
				"ponyville": {
					Organization: bootstrap.Organization{
						Name:     "ponyville",
						FullName: "Ponyville",
						OrgType:  "Business",
						GUID:     "ponyville",
					},
				},
			},
		},
		CoreObjects: bootstrap.CoreObjectState{
			Orgs: map[string]bootstrap.CoreObjectOrganizationState{
				"ponyville": {
					Environments: map[string]bootstrap.Environment{
						"production": {
							Name:               "production",
							Description:        "Production",
							CookbookVersions:   map[string]string{},
							JSONClass:          "Chef::Environment",
							ChefType:           "environment",
							DefaultAttributes:  map[string]any{},
							OverrideAttributes: map[string]any{},
						},
					},
				},
			},
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

	cfg := config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		PostgresDSN:   "postgres://repeat-test",
	}
	for i := 0; i < 2; i++ {
		app, err := New(cfg, log.New(io.Discard, "", 0), version.Info{Version: "test"})
		if err != nil {
			t.Fatalf("New() #%d error = %v", i+1, err)
		}
		req := httptest.NewRequest(http.MethodGet, "/_status", nil)
		rec := httptest.NewRecorder()
		app.server.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("/_status #%d status = %d, want %d, body = %s", i+1, rec.Code, http.StatusOK, rec.Body.String())
		}
	}
}

func TestNewRebuildsOpenSearchIndexFromActivePostgresStateOnStartup(t *testing.T) {
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

	previousPostgres := activatePostgresCookbookPersistence
	activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
		return store.ActivateCookbookPersistenceWithDB(ctx, db)
	}
	defer func() {
		activatePostgresCookbookPersistence = previousPostgres
	}()

	previousOpenSearch := activateOpenSearchIndexing
	var snapshots [][]search.DocumentRef
	activateOpenSearchIndexing = func(_ context.Context, cfg config.Config, store *pg.Store, bootstrapState *bootstrap.Service, _ *search.OpenSearchClient) (search.Index, error) {
		if cfg.OpenSearchURL != "http://opensearch.example" {
			t.Fatalf("OpenSearchURL = %q, want configured endpoint", cfg.OpenSearchURL)
		}
		if store == nil || !store.BootstrapCorePersistenceActive() || !store.CoreObjectPersistenceActive() {
			t.Fatalf("postgres store was not active for OpenSearch indexing: %v", store)
		}
		docs, err := search.DocumentsFromBootstrapState(bootstrapState)
		if err != nil {
			t.Fatalf("DocumentsFromBootstrapState() error = %v", err)
		}
		snapshots = append(snapshots, documentRefs(docs))
		return activatedAppTestSearchIndex{}, nil
	}
	defer func() {
		activateOpenSearchIndexing = previousOpenSearch
	}()

	cfg := config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
		PostgresDSN:   "postgres://opensearch-rebuild-test",
		OpenSearchURL: "http://opensearch.example",
	}
	for i := 0; i < 2; i++ {
		app, err := New(cfg, log.New(io.Discard, "", 0), version.Info{Version: "test"})
		if err != nil {
			t.Fatalf("New() #%d error = %v", i+1, err)
		}
		req := httptest.NewRequest(http.MethodGet, "/_status", nil)
		rec := httptest.NewRecorder()
		app.server.Handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("/_status #%d status = %d, want %d, body = %s", i+1, rec.Code, http.StatusOK, rec.Body.String())
		}
		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(/_status #%d) error = %v", i+1, err)
		}
		dependencies := payload["dependencies"].(map[string]any)
		openSearchStatus := dependencies["opensearch"].(map[string]any)
		if openSearchStatus["backend"] != "activated-test" {
			t.Fatalf("dependencies.opensearch.backend = %v, want activated-test", openSearchStatus["backend"])
		}
	}
	if len(snapshots) != 2 {
		t.Fatalf("OpenSearch activation calls = %d, want 2", len(snapshots))
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		t.Fatalf("OpenSearch rebuild snapshots differed: first=%v second=%v", snapshots[0], snapshots[1])
	}
	requireDocumentRef(t, snapshots[0], "client", "ponyville-validator")
	requireDocumentRef(t, snapshots[0], "environment", "_default")
}

func TestAdminHTTPBootstrapCoreChangesSurvivePostgresRestart(t *testing.T) {
	privateKey := mustGenerateAppAdminPrivateKey(t)
	publicKeyPath := filepath.Join(t.TempDir(), "pivotal.pub")
	if err := os.WriteFile(publicKeyPath, mustMarshalAppAdminPublicKeyPEM(t, &privateKey.PublicKey), 0o600); err != nil {
		t.Fatalf("WriteFile(public key) error = %v", err)
	}

	state := pgtest.NewState(pgtest.Seed{})
	db, cleanup, err := state.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer cleanup()

	previousPostgres := activatePostgresCookbookPersistence
	activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
		return store.ActivateCookbookPersistenceWithDB(ctx, db)
	}
	defer func() {
		activatePostgresCookbookPersistence = previousPostgres
	}()

	cfg := config.Config{
		ServiceName:                     "opencook",
		Environment:                     "test",
		PostgresDSN:                     "postgres://admin-http-restart-test",
		AuthSkew:                        15 * time.Minute,
		BootstrapRequestorName:          "pivotal",
		BootstrapRequestorType:          "user",
		BootstrapRequestorKeyID:         "default",
		BootstrapRequestorPublicKeyPath: publicKeyPath,
		MaxAuthBodyBytes:                config.DefaultMaxAuthBodyBytes,
	}

	firstApp, err := New(cfg, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if err != nil {
		t.Fatalf("New() first error = %v", err)
	}
	firstClient := newAppAdminClient(t, firstApp.server.Handler, privateKey)

	var userCreate map[string]any
	if err := firstClient.DoJSON(context.Background(), http.MethodPost, "/users", map[string]any{
		"username":   "rarity",
		"first_name": "Rarity",
		"last_name":  "Belle",
		"email":      "rarity@example.test",
	}, &userCreate); err != nil {
		t.Fatalf("create user through admin HTTP client: %v", err)
	}
	var orgCreate map[string]any
	if err := firstClient.DoJSON(context.Background(), http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, &orgCreate); err != nil {
		t.Fatalf("create organization through admin HTTP client: %v", err)
	}
	var userKeyCreate map[string]any
	if err := firstClient.DoJSON(context.Background(), http.MethodPost, "/users/rarity/keys", map[string]any{
		"name":            "laptop",
		"create_key":      true,
		"expiration_date": "infinity",
	}, &userKeyCreate); err != nil {
		t.Fatalf("create user key through admin HTTP client: %v", err)
	}
	var clientKeyCreate map[string]any
	if err := firstClient.DoJSON(context.Background(), http.MethodPost, "/organizations/ponyville/clients/ponyville-validator/keys", map[string]any{
		"name":            "rotation",
		"create_key":      true,
		"expiration_date": "infinity",
	}, &clientKeyCreate); err != nil {
		t.Fatalf("create client key through admin HTTP client: %v", err)
	}

	secondApp, err := New(cfg, log.New(io.Discard, "", 0), version.Info{Version: "test"})
	if err != nil {
		t.Fatalf("New() second error = %v", err)
	}
	secondClient := newAppAdminClient(t, secondApp.server.Handler, privateKey)

	var user map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/users/rarity", nil, &user); err != nil {
		t.Fatalf("get rehydrated user: %v", err)
	}
	if user["username"] != "rarity" {
		t.Fatalf("rehydrated user username = %v, want rarity", user["username"])
	}
	var org map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/organizations/ponyville", nil, &org); err != nil {
		t.Fatalf("get rehydrated organization: %v", err)
	}
	if org["name"] != "ponyville" {
		t.Fatalf("rehydrated org name = %v, want ponyville", org["name"])
	}
	var userKey map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/users/rarity/keys/laptop", nil, &userKey); err != nil {
		t.Fatalf("get rehydrated user key: %v", err)
	}
	if userKey["name"] != "laptop" {
		t.Fatalf("rehydrated user key name = %v, want laptop", userKey["name"])
	}
	var validator map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/organizations/ponyville/clients/ponyville-validator", nil, &validator); err != nil {
		t.Fatalf("get rehydrated validator client: %v", err)
	}
	if validator["name"] != "ponyville-validator" {
		t.Fatalf("rehydrated validator name = %v, want ponyville-validator", validator["name"])
	}
	var validatorDefaultKey map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/organizations/ponyville/clients/ponyville-validator/keys/default", nil, &validatorDefaultKey); err != nil {
		t.Fatalf("get rehydrated validator default key: %v", err)
	}
	if validatorDefaultKey["name"] != "default" {
		t.Fatalf("rehydrated validator default key name = %v, want default", validatorDefaultKey["name"])
	}
	var validatorRotationKey map[string]any
	if err := secondClient.DoJSON(context.Background(), http.MethodGet, "/organizations/ponyville/clients/ponyville-validator/keys/rotation", nil, &validatorRotationKey); err != nil {
		t.Fatalf("get rehydrated validator rotation key: %v", err)
	}
	if validatorRotationKey["name"] != "rotation" {
		t.Fatalf("rehydrated validator rotation key name = %v, want rotation", validatorRotationKey["name"])
	}
}

func newAppAdminClient(t *testing.T, handler http.Handler, privateKey *rsa.PrivateKey) *admin.Client {
	t.Helper()
	client, err := admin.NewClient(admin.Config{
		ServerURL:        "http://opencook.test",
		RequestorName:    "pivotal",
		RequestorType:    "user",
		ServerAPIVersion: "1",
	}, admin.WithPrivateKey(privateKey), admin.WithHTTPDoer(appAdminHandlerDoer{handler: handler}))
	if err != nil {
		t.Fatalf("admin.NewClient() error = %v", err)
	}
	return client
}

type appAdminHandlerDoer struct {
	handler http.Handler
}

func (d appAdminHandlerDoer) Do(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func mustGenerateAppAdminPrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	return key
}

func mustMarshalAppAdminPublicKeyPEM(t *testing.T, key *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: der,
	})
}

type activatedAppTestSearchIndex struct{}

func (activatedAppTestSearchIndex) Name() string {
	return "activated-test"
}

func (activatedAppTestSearchIndex) Status() search.Status {
	return search.Status{
		Backend:    "activated-test",
		Configured: true,
		Message:    "activated OpenSearch test provider",
	}
}

func (activatedAppTestSearchIndex) Indexes(context.Context, string) ([]string, error) {
	return nil, search.ErrUnavailable
}

func (activatedAppTestSearchIndex) Search(context.Context, search.Query) (search.Result, error) {
	return search.Result{}, search.ErrUnavailable
}

func TestNewStatusReportsDefaultInMemoryModeWithoutPostgres(t *testing.T) {
	app, err := New(config.Config{
		ServiceName:   "opencook",
		Environment:   "test",
		ListenAddress: ":4000",
		AuthSkew:      15 * time.Minute,
		ReadTimeout:   5 * time.Second,
		WriteTimeout:  5 * time.Second,
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
	dependencies := payload["dependencies"].(map[string]any)
	cookbooks := dependencies["cookbooks"].(map[string]any)
	if cookbooks["backend"] != "memory-bootstrap" {
		t.Fatalf("dependencies.cookbooks.backend = %v, want memory-bootstrap", cookbooks["backend"])
	}
	postgresStatus := dependencies["postgres"].(map[string]any)
	if postgresStatus["configured"] != false {
		t.Fatalf("dependencies.postgres.configured = %v, want false", postgresStatus["configured"])
	}
}

func documentRefs(docs []search.Document) []search.DocumentRef {
	refs := make([]search.DocumentRef, 0, len(docs))
	for _, doc := range docs {
		refs = append(refs, search.DocumentRef{
			Organization: doc.Resource.Organization,
			Index:        doc.Index,
			Name:         doc.Name,
		})
	}
	return refs
}

func requireDocumentRef(t *testing.T, refs []search.DocumentRef, index, name string) {
	t.Helper()

	for _, ref := range refs {
		if ref.Organization == "ponyville" && ref.Index == index && ref.Name == name {
			return
		}
	}
	t.Fatalf("refs = %v, want ponyville/%s/%s", refs, index, name)
}
