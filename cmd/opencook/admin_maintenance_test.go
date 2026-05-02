package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/maintenance"
)

func TestAdminMaintenanceStatusUsesMemoryFallbackTruthfully(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{}, nil
	}
	cmd.newMaintenanceStore = newAdminMaintenanceStore

	code := cmd.Run(context.Background(), []string{"admin", "maintenance", "status", "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(maintenance status) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminMaintenanceOutput(t, stdout.String())
	if out.Command != "maintenance_status" {
		t.Fatalf("command = %q, want maintenance_status", out.Command)
	}
	if out.Active || out.State.Enabled {
		t.Fatalf("active/state = %v/%v, want disabled fallback", out.Active, out.State.Enabled)
	}
	if out.Backend.Name != "memory" || out.Backend.Shared {
		t.Fatalf("backend = %+v, want process-local memory backend", out.Backend)
	}
	if len(out.Warnings) == 0 || !strings.Contains(out.Warnings[0], "process-local") {
		t.Fatalf("warnings = %v, want process-local warning", out.Warnings)
	}
	if out.DurationMS == nil {
		t.Fatalf("duration_ms missing from JSON output with --with-timing")
	}
}

func TestAdminMaintenanceEnableDisableRoundTripSharedStore(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	store := maintenance.NewMemoryStore()
	backend := adminMaintenanceBackend{
		Name:       "postgres",
		Configured: true,
		Shared:     true,
		Message:    "test shared backend",
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://maintenance-test"}, nil
	}
	cmd.newMaintenanceStore = func(_ context.Context, dsn string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
		if dsn != "postgres://maintenance-test" {
			t.Fatalf("dsn = %q, want postgres://maintenance-test", dsn)
		}
		return store, backend, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "maintenance", "enable", "--reason", "logical backup", "--mode", "source sync", "--actor", "operator", "--expires-in", "30m", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(maintenance enable) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	enableOut := decodeAdminMaintenanceOutput(t, stdout.String())
	if !enableOut.Active || !enableOut.State.Enabled {
		t.Fatalf("enable output active/state = %v/%v, want enabled", enableOut.Active, enableOut.State.Enabled)
	}
	if enableOut.State.Mode != "source_sync" || enableOut.State.Reason != "logical backup" || enableOut.State.Actor != "operator" {
		t.Fatalf("enable state = %+v, want normalized mode/reason/actor", enableOut.State)
	}
	if enableOut.State.ExpiresAt == nil {
		t.Fatalf("expires_at missing after --expires-in")
	}

	stdout.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "maintenance", "enable", "--reason", "cutover window", "--mode", "cutover", "--actor", "operator", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(repeated maintenance enable) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	repeatedOut := decodeAdminMaintenanceOutput(t, stdout.String())
	if repeatedOut.State.Mode != "cutover" || repeatedOut.State.Reason != "cutover window" {
		t.Fatalf("repeated enable state = %+v, want overwritten active window", repeatedOut.State)
	}

	stdout.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "maintenance", "disable", "--actor", "operator", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(maintenance disable) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	disableOut := decodeAdminMaintenanceOutput(t, stdout.String())
	if disableOut.Active || disableOut.State.Enabled {
		t.Fatalf("disable output active/state = %v/%v, want disabled", disableOut.Active, disableOut.State.Enabled)
	}

	stdout.Reset()
	code = cmd.Run(context.Background(), []string{"admin", "maintenance", "disable", "--actor", "operator", "--yes", "--json"})
	if code != exitOK {
		t.Fatalf("Run(repeated maintenance disable) exit = %d, want idempotent success; stderr = %s", code, stderr.String())
	}
}

func TestAdminMaintenanceValidationDoesNotMutateStore(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	store := maintenance.NewMemoryStore()
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://maintenance-test"}, nil
	}
	cmd.newMaintenanceStore = func(context.Context, string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
		return store, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "maintenance", "enable", "--reason", "missing confirmation", "--json"})
	if code != exitUsage {
		t.Fatalf("Run(enable without --yes) exit = %d, want %d", code, exitUsage)
	}
	state, err := store.Read(context.Background())
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if state.Enabled {
		t.Fatal("missing --yes mutated maintenance state")
	}

	code = cmd.Run(context.Background(), []string{"admin", "maintenance", "enable", "--yes", "--json"})
	if code != exitUsage {
		t.Fatalf("Run(enable missing reason) exit = %d, want %d; stderr = %s", code, exitUsage, stderr.String())
	}
	state, err = store.Read(context.Background())
	if err != nil {
		t.Fatalf("Read(after missing reason) error = %v", err)
	}
	if state.Enabled {
		t.Fatal("missing reason mutated maintenance state")
	}
}

func TestAdminMaintenanceStoreUnavailableRedactsDSN(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	rawDSN := "postgres://opencook:secret-password@db.example/opencook"
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: rawDSN}, nil
	}
	cmd.newMaintenanceStore = func(_ context.Context, dsn string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
		return nil, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true}, nil, fmt.Errorf("dial %s failed", dsn)
	}

	code := cmd.Run(context.Background(), []string{"admin", "maintenance", "status", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(maintenance status unavailable) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}
	if strings.Contains(stdout.String(), rawDSN) || strings.Contains(stdout.String(), "secret-password") {
		t.Fatalf("stdout leaked DSN: %s", stdout.String())
	}
	out := decodeAdminMaintenanceOutput(t, stdout.String())
	if out.OK || len(out.Errors) != 1 {
		t.Fatalf("output = %+v, want one redacted error", out)
	}
	if !strings.Contains(out.Errors[0].Message, "[REDACTED]") {
		t.Fatalf("error message = %q, want redacted DSN marker", out.Errors[0].Message)
	}
}

func TestAdminMaintenanceHumanOutputIsCompact(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	store := maintenance.NewMemoryStore()
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://maintenance-test"}, nil
	}
	cmd.newMaintenanceStore = func(context.Context, string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
		return store, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true}, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "maintenance", "enable", "--reason", strings.Repeat("maintenance ", 40), "--actor", "operator", "--yes"})
	if code != exitOK {
		t.Fatalf("Run(maintenance enable human) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if strings.HasPrefix(strings.TrimSpace(stdout.String()), "{") {
		t.Fatalf("human output looked like JSON: %s", stdout.String())
	}
	if !strings.Contains(stdout.String(), "maintenance: enabled") || !strings.Contains(stdout.String(), "reason: ") {
		t.Fatalf("stdout = %q, want compact maintenance status", stdout.String())
	}
	if len(stdout.String()) > 700 {
		t.Fatalf("stdout = %d bytes, want bounded human output", len(stdout.String()))
	}
}

func decodeAdminMaintenanceOutput(t *testing.T, raw string) adminMaintenanceOutput {
	t.Helper()
	var out adminMaintenanceOutput
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		t.Fatalf("Unmarshal maintenance output error = %v; raw = %s", err, raw)
	}
	return out
}
