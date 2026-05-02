package pg

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

var sqlDriverName = "pgx"

type Status struct {
	Driver     string `json:"driver"`
	Configured bool   `json:"configured"`
	Message    string `json:"message"`
}

type Store struct {
	dsn           string
	db            *sql.DB
	cookbooks     *CookbookRepository
	bootstrapCore *BootstrapCoreRepository
	coreObjects   *CoreObjectRepository
	maintenance   *MaintenanceRepository
}

func New(dsn string) *Store {
	store := &Store{dsn: dsn}
	store.cookbooks = newCookbookRepository(store)
	store.bootstrapCore = newBootstrapCoreRepository(store)
	store.coreObjects = newCoreObjectRepository(store)
	store.maintenance = newMaintenanceRepository(store)
	return store
}

func (s *Store) Name() string {
	return "postgres"
}

func (s *Store) Configured() bool {
	return s != nil && s.dsn != ""
}

func (s *Store) CookbookPersistenceActive() bool {
	return s != nil && s.db != nil
}

func (s *Store) BootstrapCorePersistenceActive() bool {
	return s != nil && s.db != nil && s.bootstrapCore != nil
}

func (s *Store) CoreObjectPersistenceActive() bool {
	return s != nil && s.db != nil && s.coreObjects != nil
}

// MaintenancePersistenceActive reports whether the shared PostgreSQL
// maintenance-state repository is active for this store.
func (s *Store) MaintenancePersistenceActive() bool {
	return s != nil && s.db != nil && s.maintenance != nil
}

func (s *Store) ActivateCookbookPersistence(ctx context.Context) error {
	if !s.Configured() || s.CookbookPersistenceActive() {
		return nil
	}

	db, err := sql.Open(sqlDriverName, s.dsn)
	if err != nil {
		return fmt.Errorf("open postgres connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}

	if err := s.ActivateCookbookPersistenceWithDB(ctx, db); err != nil {
		_ = db.Close()
		return err
	}
	return nil
}

// ActivateCookbookPersistenceWithDB activates all PostgreSQL-backed persistence
// repositories on an existing connection. The historical name remains because
// callers already use it as the app-wide PostgreSQL activation seam.
func (s *Store) ActivateCookbookPersistenceWithDB(ctx context.Context, db *sql.DB) error {
	if s == nil {
		return fmt.Errorf("postgres store is required")
	}
	if s.CookbookPersistenceActive() {
		return nil
	}
	if db == nil {
		return fmt.Errorf("postgres connection is required")
	}
	if err := s.cookbooks.activate(ctx, db); err != nil {
		return err
	}
	if err := s.bootstrapCore.activate(ctx, db); err != nil {
		return err
	}
	if err := s.coreObjects.activate(ctx, db); err != nil {
		return err
	}
	if err := s.maintenance.activate(ctx, db); err != nil {
		return err
	}
	s.db = db
	return nil
}

// ReloadPersistence refreshes repository snapshots that are otherwise cached
// in-process. It loads every snapshot before publishing any of them, preventing
// future repair flows from mixing fresh cookbook state with stale identity or
// core-object state after a mid-reload failure.
func (s *Store) ReloadPersistence(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	orgs, err := loadCookbookOrganizations(ctx, s.db)
	if err != nil {
		return fmt.Errorf("load cookbook organizations: %w", err)
	}
	versions, err := loadCookbookVersions(ctx, s.db)
	if err != nil {
		return fmt.Errorf("load cookbook versions: %w", err)
	}
	artifacts, err := loadCookbookArtifacts(ctx, s.db)
	if err != nil {
		return fmt.Errorf("load cookbook artifacts: %w", err)
	}
	bootstrapState, err := loadBootstrapCore(ctx, s.db)
	if err != nil {
		return fmt.Errorf("load bootstrap core state: %w", err)
	}
	coreObjectState, err := loadCoreObjects(ctx, s.db)
	if err != nil {
		return fmt.Errorf("load core object state: %w", err)
	}

	s.cookbooks.mu.Lock()
	s.cookbooks.orgs = orgs
	s.cookbooks.versions = versions
	s.cookbooks.artifacts = artifacts
	s.cookbooks.mu.Unlock()

	s.bootstrapCore.mu.Lock()
	s.bootstrapCore.state = bootstrap.CloneBootstrapCoreState(bootstrapState)
	s.bootstrapCore.mu.Unlock()

	s.coreObjects.mu.Lock()
	s.coreObjects.state = bootstrap.CloneCoreObjectState(coreObjectState)
	s.coreObjects.mu.Unlock()
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) Status() Status {
	if !s.Configured() {
		return Status{
			Driver:     "postgres",
			Configured: false,
			Message:    "PostgreSQL is not configured; cookbook, bootstrap core, and core object metadata use in-memory persistence and will be lost on restart; maintenance state is process-local",
		}
	}

	if s.CookbookPersistenceActive() {
		return Status{
			Driver:     "postgres",
			Configured: true,
			Message:    "PostgreSQL-backed cookbook, bootstrap core, core object, and maintenance state persistence is active",
		}
	}

	return Status{
		Driver:     "postgres",
		Configured: true,
		Message:    "PostgreSQL is configured for cookbook, bootstrap core, core object, and maintenance state persistence but activation is not active",
	}
}
