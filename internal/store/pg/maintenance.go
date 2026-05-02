package pg

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oberones/OpenCook/internal/maintenance"
)

//go:embed schema/0004_maintenance_state.sql
var maintenanceStateSchemaSQL string

type MaintenanceRepository struct {
	store *Store
	mu    sync.RWMutex
	db    *sql.DB
	state maintenance.State
}

// newMaintenanceRepository builds the maintenance repository bound to a parent
// store. It starts in process-local mode until activation supplies a database.
func newMaintenanceRepository(store *Store) *MaintenanceRepository {
	return &MaintenanceRepository{store: store}
}

// Maintenance returns the repository used for operator-visible maintenance
// state. It is process-local before activation and PostgreSQL-backed after the
// parent store activates persistence.
func (s *Store) Maintenance() *MaintenanceRepository {
	if s == nil {
		return nil
	}
	return s.maintenance
}

// Migrations exposes the maintenance schema migration for startup activation
// and migration-exposure tests.
func (r *MaintenanceRepository) Migrations() []Migration {
	if r == nil {
		return nil
	}
	return []Migration{
		{
			Name: "0004_maintenance_state.sql",
			SQL:  maintenanceStateSchemaSQL,
		},
	}
}

// Read returns the current maintenance state. Once activated, it reads through
// PostgreSQL on every call so sibling OpenCook processes observe the same gate.
func (r *MaintenanceRepository) Read(ctx context.Context) (maintenance.State, error) {
	if err := ctx.Err(); err != nil {
		return maintenance.State{}, err
	}
	if r == nil {
		return maintenance.State{}, nil
	}

	r.mu.RLock()
	db := r.db
	cached := maintenance.CloneState(r.state)
	r.mu.RUnlock()

	if db == nil {
		return cached, nil
	}
	return loadMaintenanceState(ctx, db)
}

// Enable normalizes and persists an active maintenance window. Active
// PostgreSQL repositories upsert the singleton row so repeated enable commands
// are deterministic and cross-process visible.
func (r *MaintenanceRepository) Enable(ctx context.Context, input maintenance.EnableInput) (maintenance.State, error) {
	if err := ctx.Err(); err != nil {
		return maintenance.State{}, err
	}
	if r == nil {
		return maintenance.State{}, fmt.Errorf("maintenance repository is required")
	}
	state, err := maintenance.NormalizeEnableInput(input, time.Now().UTC())
	if err != nil {
		return maintenance.State{}, err
	}

	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()
	if db != nil {
		if err := saveMaintenanceState(ctx, db, state); err != nil {
			return maintenance.State{}, err
		}
	}

	r.mu.Lock()
	r.state = maintenance.CloneState(state)
	r.mu.Unlock()
	return maintenance.CloneState(state), nil
}

// Disable clears the singleton active maintenance row. The operation is
// idempotent so cleanup can be retried safely after interrupted admin flows.
func (r *MaintenanceRepository) Disable(ctx context.Context, input maintenance.DisableInput) (maintenance.State, error) {
	if err := ctx.Err(); err != nil {
		return maintenance.State{}, err
	}
	if r == nil {
		return maintenance.State{}, nil
	}
	if _, err := maintenance.NormalizeDisableInput(input); err != nil {
		return maintenance.State{}, err
	}

	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()
	if db != nil {
		if err := deleteMaintenanceState(ctx, db); err != nil {
			return maintenance.State{}, err
		}
	}

	r.mu.Lock()
	r.state = maintenance.State{}
	r.mu.Unlock()
	return maintenance.State{}, nil
}

// Check evaluates whether the persisted maintenance state should currently
// block writes while preserving expired state for truthful diagnostics.
func (r *MaintenanceRepository) Check(ctx context.Context) (maintenance.CheckResult, error) {
	if err := ctx.Err(); err != nil {
		return maintenance.CheckResult{}, err
	}
	now := time.Now().UTC().Round(0)
	state, err := r.Read(ctx)
	if err != nil {
		return maintenance.CheckResult{}, err
	}
	expired := state.ExpiredAt(now)
	return maintenance.CheckResult{
		State:     state,
		Active:    state.Enabled && !expired,
		Expired:   expired,
		CheckedAt: now,
	}, nil
}

// activate applies the maintenance schema and loads the current singleton row.
// It does not mark the parent store active; Store activation does that only
// after every repository has successfully loaded.
func (r *MaintenanceRepository) activate(ctx context.Context, db *sql.DB) error {
	if r == nil {
		return fmt.Errorf("maintenance repository is required")
	}
	for _, migration := range r.Migrations() {
		if _, err := db.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("apply %s: %w", migration.Name, err)
		}
	}

	state, err := loadMaintenanceState(ctx, db)
	if err != nil {
		return fmt.Errorf("load maintenance state: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.db = db
	r.state = maintenance.CloneState(state)
	return nil
}

// loadMaintenanceState reads and validates the singleton maintenance row. A
// missing row is the normal disabled state, while malformed active rows fail
// startup so operators do not run with an ambiguous write gate.
func loadMaintenanceState(ctx context.Context, db *sql.DB) (maintenance.State, error) {
	var (
		enabled   bool
		mode      string
		reason    string
		actor     string
		createdAt time.Time
		expiresAt sql.NullTime
	)
	err := db.QueryRowContext(ctx, `
SELECT enabled, mode, reason, actor, created_at, expires_at
FROM oc_maintenance_state
WHERE singleton = TRUE`).Scan(&enabled, &mode, &reason, &actor, &createdAt, &expiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return maintenance.State{}, nil
	}
	if err != nil {
		return maintenance.State{}, err
	}
	if !enabled {
		return maintenance.State{}, nil
	}

	var expires *time.Time
	if expiresAt.Valid {
		expires = &expiresAt.Time
	}
	state, err := maintenance.NormalizeEnableInput(maintenance.EnableInput{
		Mode:      mode,
		Reason:    reason,
		Actor:     actor,
		CreatedAt: createdAt,
		ExpiresAt: expires,
	}, createdAt)
	if err != nil {
		return maintenance.State{}, err
	}
	return state, nil
}

// saveMaintenanceState upserts the singleton active row instead of appending
// history, keeping Task 3 focused on the current cross-process gate.
func saveMaintenanceState(ctx context.Context, db *sql.DB, state maintenance.State) error {
	if _, err := db.ExecContext(ctx, `
INSERT INTO oc_maintenance_state (singleton, enabled, mode, reason, actor, created_at, expires_at, updated_at)
VALUES (TRUE, $1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (singleton) DO UPDATE SET
    enabled = EXCLUDED.enabled,
    mode = EXCLUDED.mode,
    reason = EXCLUDED.reason,
    actor = EXCLUDED.actor,
    created_at = EXCLUDED.created_at,
    expires_at = EXCLUDED.expires_at,
    updated_at = NOW()`,
		state.Enabled, state.Mode, state.Reason, state.Actor, state.CreatedAt, nullableTime(state.ExpiresAt)); err != nil {
		return fmt.Errorf("upsert maintenance state: %w", err)
	}
	return nil
}

// deleteMaintenanceState clears the singleton row so future reads converge on
// the disabled default across every OpenCook process.
func deleteMaintenanceState(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, "DELETE FROM oc_maintenance_state WHERE singleton = TRUE"); err != nil {
		return fmt.Errorf("delete maintenance state: %w", err)
	}
	return nil
}
