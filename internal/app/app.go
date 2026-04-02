package app

import (
	"context"
	"errors"
	"log"
	"net/http"

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
	authnVerifier := authn.NoopVerifier{}
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

