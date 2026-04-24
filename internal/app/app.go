package app

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
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
	cfg    config.Config
	logger *log.Logger
	server *http.Server
}

var activatePostgresCookbookPersistence = func(ctx context.Context, store *pg.Store) error {
	return store.ActivateCookbookPersistence(ctx)
}

func New(cfg config.Config, logger *log.Logger, build version.Info) (*Application, error) {
	compatRegistry := compat.NewDefaultRegistry()
	postgresStore := pg.New(cfg.PostgresDSN)
	if err := activatePostgresCookbookPersistence(context.Background(), postgresStore); err != nil {
		return nil, fmt.Errorf("activate postgres cookbook persistence: %w", err)
	}
	blobStore, err := blob.NewStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("configure blob storage: %w", err)
	}
	keyStore := authn.NewMemoryKeyStore()
	bootstrapState := bootstrap.NewService(keyStore, bootstrapOptions(cfg, postgresStore))
	if err := seedCookbookOrganizationsFromPostgres(bootstrapState, postgresStore); err != nil {
		return nil, err
	}
	searchIndex := search.NewMemoryIndex(bootstrapState, cfg.OpenSearchURL)
	if err := seedBootstrapRequestor(keyStore, cfg); err != nil {
		return nil, err
	}
	if principal, ok := bootstrapPrincipalFromConfig(cfg); ok {
		bootstrapState.SeedPrincipal(principal)
		if err := seedBootstrapRequestorState(bootstrapState, cfg, principal); err != nil {
			return nil, err
		}
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
		cfg:    cfg,
		logger: logger,
		server: server,
	}, nil
}

func bootstrapOptions(cfg config.Config, postgresStore *pg.Store) bootstrap.Options {
	opts := bootstrap.Options{
		SuperuserName: resolveSuperuserName(cfg),
	}
	if postgresStore != nil && postgresStore.Configured() {
		opts.CookbookStoreFactory = func(*bootstrap.Service) bootstrap.CookbookStore {
			return postgresStore.CookbookStore()
		}
	}
	return opts
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
