package pg_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestMaintenanceRepositoryExposesMaintenanceMigration(t *testing.T) {
	repo := pg.New("postgres://example").Maintenance()

	migrations := repo.Migrations()
	if len(migrations) != 1 {
		t.Fatalf("len(Migrations()) = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "0004_maintenance_state.sql" {
		t.Fatalf("Migrations()[0].Name = %q, want maintenance state migration", migrations[0].Name)
	}

	sql := migrations[0].SQL
	for _, want := range []string{
		"oc_maintenance_state",
		"singleton",
		"enabled",
		"mode",
		"reason",
		"actor",
		"created_at",
		"expires_at",
	} {
		if !strings.Contains(sql, want) {
			t.Fatalf("migration SQL missing %q", want)
		}
	}
}

func TestMaintenanceRepositoryInactiveStoreDefaultsToDisabled(t *testing.T) {
	repo := pg.New("postgres://example").Maintenance()

	state, err := repo.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if state.Enabled {
		t.Fatalf("Read().Enabled = true, want disabled default")
	}

	check, err := repo.Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if check.Active || check.Expired {
		t.Fatalf("Check() = active=%v expired=%v, want inactive and unexpired", check.Active, check.Expired)
	}
}

func TestMaintenanceRepositoryInactiveStoreRoundTripsState(t *testing.T) {
	repo := pg.New("postgres://example").Maintenance()
	createdAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(2 * time.Hour)
	want := maintenance.State{
		Enabled:   true,
		Mode:      "backup",
		Reason:    "logical backup",
		Actor:     "operator",
		CreatedAt: createdAt,
		ExpiresAt: &expiresAt,
	}

	got, err := repo.Enable(context.Background(), maintenance.EnableInput{
		Mode:      "backup",
		Reason:    "logical backup",
		Actor:     "operator",
		CreatedAt: createdAt,
		ExpiresAt: &expiresAt,
	})
	if err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Enable() = %#v, want %#v", got, want)
	}

	read, err := repo.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !reflect.DeepEqual(read, want) {
		t.Fatalf("Read() = %#v, want %#v", read, want)
	}

	if _, err := repo.Disable(context.Background(), maintenance.DisableInput{Actor: "operator"}); err != nil {
		t.Fatalf("Disable() error = %v", err)
	}
	read, err = repo.Read(context.Background())
	if err != nil {
		t.Fatalf("Read(disabled) error = %v", err)
	}
	if read.Enabled {
		t.Fatalf("Read(disabled).Enabled = true, want false")
	}
}

func TestMaintenanceRepositoryActivePostgresRoundTripIsSharedAcrossStores(t *testing.T) {
	pgState := pgtest.NewState(pgtest.Seed{})
	db, cleanup, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	first := pg.New("postgres://maintenance-shared")
	if err := first.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB(first) error = %v", err)
	}
	if !first.MaintenancePersistenceActive() {
		t.Fatal("MaintenancePersistenceActive() = false after activation")
	}

	createdAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(time.Hour)
	if _, err := first.Maintenance().Enable(context.Background(), maintenance.EnableInput{
		Mode:      "source sync",
		Reason:    "sync before cutover",
		Actor:     "operator",
		CreatedAt: createdAt,
		ExpiresAt: &expiresAt,
	}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}

	second := pg.New("postgres://maintenance-shared")
	if err := second.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB(second) error = %v", err)
	}
	read, err := second.Maintenance().Read(context.Background())
	if err != nil {
		t.Fatalf("Read(second) error = %v", err)
	}
	if !read.Enabled || read.Mode != "source_sync" || read.Reason != "sync before cutover" || read.Actor != "operator" {
		t.Fatalf("Read(second) = %#v, want shared normalized state", read)
	}

	if _, err := second.Maintenance().Disable(context.Background(), maintenance.DisableInput{Actor: "operator"}); err != nil {
		t.Fatalf("Disable(second) error = %v", err)
	}
	read, err = first.Maintenance().Read(context.Background())
	if err != nil {
		t.Fatalf("Read(first after disable) error = %v", err)
	}
	if read.Enabled {
		t.Fatalf("Read(first after disable).Enabled = true, want shared disabled state")
	}
}

func TestMaintenanceRepositoryActivePostgresReportsExpiredStateInactive(t *testing.T) {
	createdAt := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	expiresAt := time.Date(2000, 1, 1, 1, 0, 0, 0, time.UTC)
	pgState := pgtest.NewState(pgtest.Seed{
		Maintenance: maintenance.State{
			Enabled:   true,
			Mode:      "backup",
			Reason:    "old backup",
			Actor:     "operator",
			CreatedAt: createdAt,
			ExpiresAt: &expiresAt,
		},
	})
	db, cleanup, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	store := pg.New("postgres://maintenance-expired")
	if err := store.ActivateCookbookPersistenceWithDB(context.Background(), db); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v", err)
	}
	check, err := store.Maintenance().Check(context.Background())
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}
	if check.Active || !check.Expired || !check.State.Enabled {
		t.Fatalf("Check() = active=%v expired=%v state=%#v, want inactive expired stored state", check.Active, check.Expired, check.State)
	}
}

func TestMaintenanceRepositoryRejectsMalformedPersistedStateOnActivation(t *testing.T) {
	pgState := pgtest.NewState(pgtest.Seed{
		Maintenance: maintenance.State{
			Enabled:   true,
			Mode:      "backup/restore",
			Reason:    "bad mode",
			CreatedAt: time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC),
		},
	})
	db, cleanup, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB() error = %v", err)
	}
	defer func() {
		if err := cleanup(); err != nil {
			t.Fatalf("cleanup() error = %v", err)
		}
	}()

	store := pg.New("postgres://maintenance-malformed")
	err = store.ActivateCookbookPersistenceWithDB(context.Background(), db)
	if !errors.Is(err, maintenance.ErrInvalidInput) {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %v, want %v", err, maintenance.ErrInvalidInput)
	}
	if !strings.Contains(err.Error(), "load maintenance state") {
		t.Fatalf("ActivateCookbookPersistenceWithDB() error = %q, want load context", err)
	}
	if store.MaintenancePersistenceActive() {
		t.Fatal("MaintenancePersistenceActive() = true after malformed activation failure")
	}
}
