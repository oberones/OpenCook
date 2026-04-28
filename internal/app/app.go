package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/api"
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

type Application struct {
	cfg                 config.Config
	logger              *log.Logger
	server              *http.Server
	postgresStatus      pg.Status
	searchStatus        search.Status
	postgresConnected   bool
	openSearchConnected bool
}

var activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
	return store.ActivateCookbookPersistence(ctx)
}

var newOpenSearchClient = search.NewOpenSearchClient

var activateOpenSearchIndexing = func(ctx context.Context, cfg config.Config, store *pg.Store, state *bootstrap.Service, client *search.OpenSearchClient) (search.Index, error) {
	if strings.TrimSpace(cfg.OpenSearchURL) == "" || store == nil || !store.BootstrapCorePersistenceActive() || !store.CoreObjectPersistenceActive() {
		return nil, nil
	}
	if client == nil {
		var err error
		client, err = newOpenSearchClient(cfg.OpenSearchURL)
		if err != nil {
			return nil, err
		}
	}
	if _, err := client.DiscoverProvider(ctx); err != nil {
		return nil, err
	}
	if err := search.RebuildOpenSearchIndex(ctx, client, state); err != nil {
		return nil, err
	}
	return search.NewOpenSearchIndex(state, client, cfg.OpenSearchURL), nil
}

func New(cfg config.Config, logger *log.Logger, build version.Info) (*Application, error) {
	if err := search.ValidateOpenSearchEndpoint(cfg.OpenSearchURL); err != nil {
		return nil, fmt.Errorf("configure opensearch search: %w", err)
	}
	compatRegistry := compat.NewDefaultRegistry()
	postgresStore := pg.New(cfg.PostgresDSN)
	if err := activatePostgresCookbookPersistence(context.Background(), postgresStore); err != nil {
		return nil, fmt.Errorf("activate postgres cookbook persistence: %w", err)
	}
	blobStore, err := blob.NewStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure blob storage: %w", err)
	}
	openSearchClient, err := activeOpenSearchDocumentIndexer(cfg, postgresStore)
	if err != nil {
		return nil, fmt.Errorf("configure opensearch indexing: %w", err)
	}
	var documentIndexer search.DocumentIndexer
	if openSearchClient != nil {
		documentIndexer = openSearchClient
	}
	keyStore := authn.NewMemoryKeyStore()
	opts, err := bootstrapOptions(cfg, postgresStore, documentIndexer)
	if err != nil {
		return nil, fmt.Errorf("load postgres bootstrap state: %w", err)
	}
	bootstrapState := bootstrap.NewService(keyStore, opts)
	if err := bootstrapState.RehydrateKeyStore(); err != nil {
		return nil, fmt.Errorf("hydrate bootstrap verifier keys: %w", err)
	}
	if err := seedCookbookOrganizationsFromPostgres(bootstrapState, postgresStore); err != nil {
		return nil, err
	}
	var searchIndex search.Index = search.NewMemoryIndex(bootstrapState, cfg.OpenSearchURL)
	if err := seedBootstrapRequestor(keyStore, cfg); err != nil {
		return nil, err
	}
	if principal, ok := bootstrapPrincipalFromConfig(cfg); ok {
		bootstrapState.SeedPrincipal(principal)
		if err := seedBootstrapRequestorState(bootstrapState, cfg, principal); err != nil {
			return nil, err
		}
	}
	activeSearchIndex, err := activateOpenSearchIndexing(context.Background(), cfg, postgresStore, bootstrapState, openSearchClient)
	if err != nil {
		return nil, fmt.Errorf("activate opensearch indexing: %w", err)
	}
	if activeSearchIndex != nil {
		searchIndex = activeSearchIndex
	}
	authSkew := cfg.AuthSkew
	authnVerifier := authn.NewChefVerifier(keyStore, authn.Options{
		AllowedClockSkew: &authSkew,
	})
	authzAuthorizer := authz.NewACLAuthorizer(bootstrapState)
	blobUploadSecret, err := randomBytes(32)
	if err != nil {
		return nil, fmt.Errorf("generate blob upload secret: %w", err)
	}

	handler := api.NewRouter(api.Dependencies{
		Logger:           logger,
		Config:           cfg,
		Version:          build,
		Compat:           compatRegistry,
		Now:              time.Now,
		Authn:            authnVerifier,
		Authz:            authzAuthorizer,
		Bootstrap:        bootstrapState,
		Blob:             blobStore,
		BlobUploadSecret: blobUploadSecret,
		Search:           searchIndex,
		Postgres:         postgresStore,
		CookbookBackend:  resolveCookbookBackend(postgresStore),
	})

	server := &http.Server{
		Addr:         cfg.ListenAddress,
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return &Application{
		cfg:                 cfg,
		logger:              logger,
		server:              server,
		postgresStatus:      postgresStore.Status(),
		searchStatus:        searchIndex.Status(),
		postgresConnected:   postgresStore.CookbookPersistenceActive() && postgresStore.BootstrapCorePersistenceActive() && postgresStore.CoreObjectPersistenceActive(),
		openSearchConnected: activeSearchIndex != nil,
	}, nil
}

func bootstrapOptions(cfg config.Config, postgresStore *pg.Store, indexer search.DocumentIndexer) (bootstrap.Options, error) {
	opts := bootstrap.Options{
		SuperuserName: resolveSuperuserName(cfg),
	}
	if postgresStore != nil && postgresStore.Configured() {
		bootstrapCoreStore := bootstrap.BootstrapCoreStore(postgresStore.BootstrapCore())
		coreObjectStore := bootstrap.CoreObjectStore(postgresStore.CoreObjects())
		if indexer != nil {
			bootstrapCoreStore = search.NewIndexingBootstrapCoreStore(bootstrapCoreStore, indexer)
			coreObjectStore = search.NewIndexingCoreObjectStore(coreObjectStore, indexer)
		}
		opts.CookbookStoreFactory = func(*bootstrap.Service) bootstrap.CookbookStore {
			return postgresStore.CookbookStore()
		}
		opts.BootstrapCoreStoreFactory = func(*bootstrap.Service) bootstrap.BootstrapCoreStore {
			return bootstrapCoreStore
		}
		opts.CoreObjectStoreFactory = func(*bootstrap.Service) bootstrap.CoreObjectStore {
			return coreObjectStore
		}
		if postgresStore.BootstrapCorePersistenceActive() {
			state, err := bootstrapCoreStore.LoadBootstrapCore()
			if err != nil {
				return bootstrap.Options{}, fmt.Errorf("load bootstrap core state: %w", err)
			}
			opts.InitialBootstrapCoreState = &state
		}
		if postgresStore.CoreObjectPersistenceActive() {
			state, err := coreObjectStore.LoadCoreObjects()
			if err != nil {
				return bootstrap.Options{}, fmt.Errorf("load core object state: %w", err)
			}
			opts.InitialCoreObjectState = &state
		}
	}
	return opts, nil
}

func activeOpenSearchDocumentIndexer(cfg config.Config, postgresStore *pg.Store) (*search.OpenSearchClient, error) {
	if strings.TrimSpace(cfg.OpenSearchURL) == "" || postgresStore == nil || !postgresStore.BootstrapCorePersistenceActive() || !postgresStore.CoreObjectPersistenceActive() {
		return nil, nil
	}
	return newOpenSearchClient(cfg.OpenSearchURL)
}

func resolveCookbookBackend(postgresStore *pg.Store) string {
	if postgresStore != nil && postgresStore.CookbookPersistenceActive() {
		return "postgres"
	}
	if postgresStore != nil && postgresStore.Configured() {
		return "postgres-configured"
	}
	return "memory-bootstrap"
}

func seedCookbookOrganizationsFromPostgres(state *bootstrap.Service, postgresStore *pg.Store) error {
	if state == nil || postgresStore == nil || !postgresStore.CookbookPersistenceActive() {
		return nil
	}

	for _, org := range postgresStore.Cookbooks().OrganizationRecords() {
		if _, exists := state.GetOrganization(org.Name); exists {
			continue
		}
		fullName := org.FullName
		if fullName == "" {
			fullName = org.Name
		}
		if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
			Name:     org.Name,
			FullName: fullName,
		}); err != nil && !errors.Is(err, bootstrap.ErrConflict) {
			return fmt.Errorf("seed cookbook organization %s from postgres: %w", org.Name, err)
		}
	}
	return nil
}

func seedBootstrapRequestor(store *authn.MemoryKeyStore, cfg config.Config) error {
	if cfg.BootstrapRequestorName == "" || cfg.BootstrapRequestorPublicKeyPath == "" {
		return nil
	}

	data, err := os.ReadFile(cfg.BootstrapRequestorPublicKeyPath)
	if err != nil {
		return fmt.Errorf("read bootstrap public key: %w", err)
	}

	publicKey, err := authn.ParseRSAPublicKeyPEM(data)
	if err != nil {
		return fmt.Errorf("parse bootstrap public key: %w", err)
	}

	return store.Put(authn.Key{
		ID: cfg.BootstrapRequestorKeyID,
		Principal: authn.Principal{
			Type:         cfg.BootstrapRequestorType,
			Name:         cfg.BootstrapRequestorName,
			Organization: cfg.BootstrapRequestorOrganization,
		},
		PublicKey: publicKey,
	})
}

func seedBootstrapRequestorState(state *bootstrap.Service, cfg config.Config, principal authn.Principal) error {
	if state == nil || principal.Type != "user" || cfg.BootstrapRequestorName == "" || cfg.BootstrapRequestorPublicKeyPath == "" {
		return nil
	}

	data, err := os.ReadFile(cfg.BootstrapRequestorPublicKeyPath)
	if err != nil {
		return fmt.Errorf("read bootstrap public key for state seed: %w", err)
	}

	if err := state.SeedPublicKey(principal, cfg.BootstrapRequestorKeyID, string(data)); err != nil {
		return fmt.Errorf("seed bootstrap requestor state: %w", err)
	}

	return nil
}

func bootstrapPrincipalFromConfig(cfg config.Config) (authn.Principal, bool) {
	if cfg.BootstrapRequestorName == "" {
		return authn.Principal{}, false
	}

	return authn.Principal{
		Type:         cfg.BootstrapRequestorType,
		Name:         cfg.BootstrapRequestorName,
		Organization: cfg.BootstrapRequestorOrganization,
	}, true
}

func resolveSuperuserName(cfg config.Config) string {
	if cfg.BootstrapRequestorType == "user" && cfg.BootstrapRequestorName != "" {
		return cfg.BootstrapRequestorName
	}
	return "pivotal"
}

func randomBytes(size int) ([]byte, error) {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

func (a *Application) StartupSummary() string {
	if a == nil {
		return ""
	}
	return formatStartupSummary(a.postgresStatus, a.postgresConnected, a.searchStatus, a.openSearchConnected)
}

func formatStartupSummary(postgresStatus pg.Status, postgresConnected bool, searchStatus search.Status, openSearchConnected bool) string {
	var out strings.Builder
	out.WriteString("External integrations:\n")
	fmt.Fprintf(&out, "  PostgreSQL: %s - %s\n", integrationConnectionState(postgresConnected), strings.TrimSpace(postgresStatus.Message))
	fmt.Fprintf(&out, "  OpenSearch: %s - %s\n", integrationConnectionState(openSearchConnected), strings.TrimSpace(searchStatus.Message))
	if !postgresConnected {
		out.WriteString("  Reminder: OpenCook is running with in-memory persistence; all data will be lost on restart\n")
	}
	return out.String()
}

func integrationConnectionState(connected bool) string {
	if connected {
		return "connected"
	}
	return "not connected"
}

func (a *Application) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		a.logger.Printf("opencook listening on %s", a.server.Addr)
		err := a.server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
		defer cancel()
		return a.server.Shutdown(shutdownCtx)
	}
}
