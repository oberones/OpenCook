package app

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/oberones/OpenCook/internal/api"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
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

func New(cfg config.Config, logger *log.Logger, build version.Info) (*Application, error) {
	compatRegistry := compat.NewDefaultRegistry()
	postgresStore := pg.New(cfg.PostgresDSN)
	searchIndex := search.NewNoopIndex(cfg.OpenSearchURL)
	blobStore := blob.NewNoopStore(cfg.BlobStorageURL)
	keyStore := authn.NewMemoryKeyStore()
	if err := seedBootstrapRequestor(keyStore, cfg); err != nil {
		return nil, err
	}
	authSkew := cfg.AuthSkew
	authnVerifier := authn.NewChefVerifier(keyStore, authn.Options{
		AllowedClockSkew: &authSkew,
	})
	authzAuthorizer := authz.NoopAuthorizer{}

	handler := api.NewRouter(api.Dependencies{
		Logger:   logger,
		Config:   cfg,
		Version:  build,
		Compat:   compatRegistry,
		Authn:    authnVerifier,
		Authz:    authzAuthorizer,
		Blob:     blobStore,
		Search:   searchIndex,
		Postgres: postgresStore,
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

func (a *Application) Run(ctx context.Context) error {
	errCh := make(chan error, 1)

	go func() {
		a.logger.Printf("opencook scaffold listening on %s", a.server.Addr)
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
