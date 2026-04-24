package pg

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
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
}

func New(dsn string) *Store {
	store := &Store{dsn: dsn}
	store.cookbooks = newCookbookRepository(store)
	store.bootstrapCore = newBootstrapCoreRepository(store)
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
	s.db = db
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
			Message:    "set OPENCOOK_POSTGRES_DSN to configure persistence",
		}
	}

	if s.CookbookPersistenceActive() {
		return Status{
			Driver:     "postgres",
			Configured: true,
			Message:    "PostgreSQL cookbook and bootstrap core persistence active",
		}
	}

	return Status{
		Driver:     "postgres",
		Configured: true,
		Message:    "PostgreSQL configured for cookbook and bootstrap core persistence; waiting for app activation",
	}
}
