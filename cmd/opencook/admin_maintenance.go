package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/store/pg"
)

const adminMaintenanceErrorRuneLimit = 240

type adminMaintenanceBackend struct {
	Name       string `json:"name"`
	Configured bool   `json:"configured"`
	Shared     bool   `json:"shared"`
	Message    string `json:"message"`
}

type adminMaintenanceOutput struct {
	OK         bool                    `json:"ok"`
	Command    string                  `json:"command"`
	Backend    adminMaintenanceBackend `json:"backend"`
	Active     bool                    `json:"active"`
	Expired    bool                    `json:"expired,omitempty"`
	CheckedAt  time.Time               `json:"checked_at,omitempty"`
	State      maintenance.SafeState   `json:"state"`
	Warnings   []string                `json:"warnings,omitempty"`
	Errors     []adminCLIError         `json:"errors,omitempty"`
	Duration   string                  `json:"duration,omitempty"`
	DurationMS *int64                  `json:"duration_ms,omitempty"`
}

// runAdminMaintenance dispatches the local maintenance controls that coordinate
// Chef-facing write blocking without adding a new Chef-compatible route.
func (c *command) runAdminMaintenance(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin maintenance requires status, check, enable, or disable\n\n")
	}

	switch args[0] {
	case "status":
		return c.runAdminMaintenanceStatus(ctx, args[1:], inheritedJSON)
	case "check":
		return c.runAdminMaintenanceCheck(ctx, args[1:], inheritedJSON)
	case "enable":
		return c.runAdminMaintenanceEnable(ctx, args[1:], inheritedJSON)
	case "disable":
		return c.runAdminMaintenanceDisable(ctx, args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminMaintenanceUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin maintenance command %q\n\n", args[0])
	}
}

// runAdminMaintenanceStatus reports the shared maintenance gate without
// mutating it, keeping status safe for health checks and operator scripts.
func (c *command) runAdminMaintenanceStatus(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin maintenance status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := adminMaintenanceOptions{}
	bindAdminMaintenanceCommonFlags(fs, &opts, inheritedJSON)
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin maintenance status", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin maintenance status received unexpected arguments: %v\n\n", fs.Args())
	}
	return c.runAdminMaintenanceReadCommand(ctx, "maintenance_status", opts, start)
}

// runAdminMaintenanceCheck mirrors status but uses a command name intended for
// scripts that only need to know whether writes are currently blocked.
func (c *command) runAdminMaintenanceCheck(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin maintenance check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := adminMaintenanceOptions{}
	bindAdminMaintenanceCommonFlags(fs, &opts, inheritedJSON)
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin maintenance check", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin maintenance check received unexpected arguments: %v\n\n", fs.Args())
	}
	return c.runAdminMaintenanceReadCommand(ctx, "maintenance_check", opts, start)
}

// runAdminMaintenanceEnable validates the explicit operator confirmation and
// writes a normalized maintenance window through the configured store.
func (c *command) runAdminMaintenanceEnable(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin maintenance enable", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := adminMaintenanceOptions{}
	bindAdminMaintenanceCommonFlags(fs, &opts, inheritedJSON)
	yes := fs.Bool("yes", false, "confirm enabling maintenance mode")
	reason := fs.String("reason", "", "operator-visible maintenance reason")
	mode := fs.String("mode", "", "maintenance mode token, for example backup, repair, or cutover")
	actor := fs.String("actor", adminMaintenanceDefaultActor(), "operator identifier to store with the maintenance window")
	expiresIn := fs.String("expires-in", "", "optional duration after which the write gate stops blocking writes")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin maintenance enable", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin maintenance enable received unexpected arguments: %v\n\n", fs.Args())
	}
	if !*yes {
		return c.adminUsageError("admin maintenance enable requires --yes\n\n")
	}

	createdAt := time.Now().UTC().Round(0)
	var expiresAt *time.Time
	if strings.TrimSpace(*expiresIn) != "" {
		duration, err := time.ParseDuration(strings.TrimSpace(*expiresIn))
		if err != nil {
			return c.adminFlagError("admin maintenance enable", fmt.Errorf("invalid --expires-in: %w", err))
		}
		if duration <= 0 {
			return c.adminUsageError("admin maintenance enable --expires-in must be positive\n\n")
		}
		expires := createdAt.Add(duration)
		expiresAt = &expires
	}

	store, backend, closeStore, err := c.openAdminMaintenanceStore(ctx, opts.postgresDSN)
	if err != nil {
		return c.writeAdminMaintenanceStoreError("maintenance_enable", opts, start, backend, err)
	}
	defer closeAdminMaintenanceStore(closeStore)

	state, err := store.Enable(ctx, maintenance.EnableInput{
		Mode:      *mode,
		Reason:    *reason,
		Actor:     *actor,
		CreatedAt: createdAt,
		ExpiresAt: expiresAt,
	})
	if err != nil {
		if errors.Is(err, maintenance.ErrInvalidInput) {
			return c.adminUsageError("admin maintenance enable: %v\n\n", err)
		}
		return c.writeAdminMaintenanceStoreError("maintenance_enable", opts, start, backend, err)
	}
	out := adminMaintenanceOutputFromState("maintenance_enable", backend, state, true, false, createdAt, adminMaintenanceBackendWarnings(backend), nil)
	return c.writeAdminMaintenanceResult(out, opts.jsonOutput, opts.withTiming, start, exitOK)
}

// runAdminMaintenanceDisable clears the active gate. It remains idempotent so
// operators can safely rerun cleanup after interrupted maintenance workflows.
func (c *command) runAdminMaintenanceDisable(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin maintenance disable", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := adminMaintenanceOptions{}
	bindAdminMaintenanceCommonFlags(fs, &opts, inheritedJSON)
	yes := fs.Bool("yes", false, "confirm disabling maintenance mode")
	actor := fs.String("actor", adminMaintenanceDefaultActor(), "operator identifier to validate for future audit fields")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin maintenance disable", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin maintenance disable received unexpected arguments: %v\n\n", fs.Args())
	}
	if !*yes {
		return c.adminUsageError("admin maintenance disable requires --yes\n\n")
	}

	store, backend, closeStore, err := c.openAdminMaintenanceStore(ctx, opts.postgresDSN)
	if err != nil {
		return c.writeAdminMaintenanceStoreError("maintenance_disable", opts, start, backend, err)
	}
	defer closeAdminMaintenanceStore(closeStore)

	state, err := store.Disable(ctx, maintenance.DisableInput{Actor: *actor})
	if err != nil {
		if errors.Is(err, maintenance.ErrInvalidInput) {
			return c.adminUsageError("admin maintenance disable: %v\n\n", err)
		}
		return c.writeAdminMaintenanceStoreError("maintenance_disable", opts, start, backend, err)
	}
	out := adminMaintenanceOutputFromState("maintenance_disable", backend, state, false, false, time.Now().UTC().Round(0), adminMaintenanceBackendWarnings(backend), nil)
	return c.writeAdminMaintenanceResult(out, opts.jsonOutput, opts.withTiming, start, exitOK)
}

type adminMaintenanceOptions struct {
	jsonOutput  bool
	withTiming  bool
	postgresDSN string
}

// bindAdminMaintenanceCommonFlags keeps the maintenance subcommands consistent
// with the other operational admin commands while avoiding signed HTTP config.
func bindAdminMaintenanceCommonFlags(fs *flag.FlagSet, opts *adminMaintenanceOptions, inheritedJSON bool) {
	if opts == nil {
		return
	}
	fs.BoolVar(&opts.jsonOutput, "json", inheritedJSON, "print JSON output")
	fs.BoolVar(&opts.withTiming, "with-timing", false, "include duration_ms in output")
	fs.StringVar(&opts.postgresDSN, "postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
}

// runAdminMaintenanceReadCommand opens the configured store and converts its
// Check result into the shared status/check output envelope.
func (c *command) runAdminMaintenanceReadCommand(ctx context.Context, commandName string, opts adminMaintenanceOptions, start time.Time) int {
	store, backend, closeStore, err := c.openAdminMaintenanceStore(ctx, opts.postgresDSN)
	if err != nil {
		return c.writeAdminMaintenanceStoreError(commandName, opts, start, backend, err)
	}
	defer closeAdminMaintenanceStore(closeStore)

	check, err := store.Check(ctx)
	if err != nil {
		return c.writeAdminMaintenanceStoreError(commandName, opts, start, backend, err)
	}
	out := adminMaintenanceOutputFromCheck(commandName, backend, check, adminMaintenanceBackendWarnings(backend), nil)
	return c.writeAdminMaintenanceResult(out, opts.jsonOutput, opts.withTiming, start, exitOK)
}

// openAdminMaintenanceStore resolves the DSN from flags or environment and
// returns either a PostgreSQL-backed shared store or a truthful memory fallback.
func (c *command) openAdminMaintenanceStore(ctx context.Context, postgresDSN string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
	if strings.TrimSpace(postgresDSN) == "" {
		cfg, err := c.loadOffline()
		if err != nil {
			return nil, adminMaintenanceBackend{}, nil, fmt.Errorf("load maintenance config: %w", err)
		}
		postgresDSN = cfg.PostgresDSN
	}
	if c.newMaintenanceStore == nil {
		c.newMaintenanceStore = newAdminMaintenanceStore
	}
	store, backend, closeStore, err := c.newMaintenanceStore(ctx, postgresDSN)
	if err != nil {
		err = errors.New(adminMaintenanceSafeError(err, postgresDSN))
	}
	if err == nil && store == nil {
		err = fmt.Errorf("maintenance store is required")
	}
	return store, backend, closeStore, err
}

// newAdminMaintenanceStore is the production opener used by the CLI. It uses
// PostgreSQL when configured; otherwise it returns a command-local memory store
// and labels that limitation in the backend metadata.
func newAdminMaintenanceStore(ctx context.Context, postgresDSN string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
	postgresDSN = strings.TrimSpace(postgresDSN)
	if postgresDSN == "" {
		return maintenance.NewMemoryStore(), adminMaintenanceBackend{
			Name:       "memory",
			Configured: false,
			Shared:     false,
			Message:    "process-local maintenance state only; a separate CLI process cannot coordinate a running standalone server",
		}, nil, nil
	}

	store := pg.New(postgresDSN)
	if err := store.ActivateCookbookPersistence(ctx); err != nil {
		return nil, adminMaintenanceBackend{
			Name:       "postgres",
			Configured: true,
			Shared:     true,
			Message:    "PostgreSQL-backed maintenance state could not be opened",
		}, nil, err
	}
	return store.Maintenance(), adminMaintenanceBackend{
		Name:       "postgres",
		Configured: true,
		Shared:     true,
		Message:    "PostgreSQL-backed maintenance state is shared across OpenCook processes",
	}, store.Close, nil
}

// closeAdminMaintenanceStore centralizes close handling so maintenance commands
// stay readable and close failures can be ignored without shadowing command work.
func closeAdminMaintenanceStore(closeStore func() error) {
	if closeStore != nil {
		_ = closeStore()
	}
}

// adminMaintenanceOutputFromCheck converts raw store checks into safe output:
// operator-provided text is passed through SafeStatus before it reaches stdout.
func adminMaintenanceOutputFromCheck(commandName string, backend adminMaintenanceBackend, check maintenance.CheckResult, warnings []string, errs []adminCLIError) adminMaintenanceOutput {
	return adminMaintenanceOutputFromState(commandName, backend, check.State, check.Active, check.Expired, check.CheckedAt, warnings, errs)
}

// adminMaintenanceOutputFromState builds the stable JSON envelope used by both
// mutating and read-only maintenance commands.
func adminMaintenanceOutputFromState(commandName string, backend adminMaintenanceBackend, state maintenance.State, active bool, expired bool, checkedAt time.Time, warnings []string, errs []adminCLIError) adminMaintenanceOutput {
	return adminMaintenanceOutput{
		OK:        len(errs) == 0,
		Command:   commandName,
		Backend:   backend,
		Active:    active,
		Expired:   expired,
		CheckedAt: checkedAt,
		State:     state.SafeStatus(),
		Warnings:  warnings,
		Errors:    errs,
	}
}

// adminMaintenanceBackendWarnings makes memory-mode limitations visible without
// changing any Chef-facing response shape.
func adminMaintenanceBackendWarnings(backend adminMaintenanceBackend) []string {
	if backend.Shared {
		return nil
	}
	return []string{"maintenance state is process-local; use PostgreSQL-backed maintenance state to coordinate multiple OpenCook processes"}
}

// writeAdminMaintenanceStoreError preserves a stable error shape while
// redacting raw DSNs and bounding provider details before they reach output.
func (c *command) writeAdminMaintenanceStoreError(commandName string, opts adminMaintenanceOptions, start time.Time, backend adminMaintenanceBackend, err error) int {
	if backend.Name == "" {
		backend = adminMaintenanceBackend{Name: "unknown", Message: "maintenance backend could not be opened"}
	}
	message := adminMaintenanceSafeError(err, opts.postgresDSN)
	out := adminMaintenanceOutputFromState(commandName, backend, maintenance.State{}, false, false, time.Now().UTC().Round(0), adminMaintenanceBackendWarnings(backend), []adminCLIError{{
		Code:    "maintenance_store_unavailable",
		Message: message,
	}})
	if opts.jsonOutput {
		return c.writeAdminMaintenanceResult(out, true, opts.withTiming, start, exitDependencyUnavailable)
	}
	fmt.Fprintf(c.stderr, "maintenance store unavailable: %s\n", message)
	return exitDependencyUnavailable
}

// writeAdminMaintenanceResult emits JSON for automation and a compact human
// status view for interactive operators.
func (c *command) writeAdminMaintenanceResult(out adminMaintenanceOutput, jsonOutput bool, withTiming bool, start time.Time, exitCode int) int {
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if jsonOutput {
		if err := writePrettyJSON(c.stdout, out); err != nil {
			fmt.Fprintf(c.stderr, "write maintenance output: %v\n", err)
			return exitDependencyUnavailable
		}
		return exitCode
	}
	if err := c.writeAdminMaintenanceHuman(out); err != nil {
		fmt.Fprintf(c.stderr, "write maintenance output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// writeAdminMaintenanceHuman keeps the interactive format short and explicitly
// avoids raw DSNs, signatures, private keys, or unbounded operator notes.
func (c *command) writeAdminMaintenanceHuman(out adminMaintenanceOutput) error {
	status := "disabled"
	if out.Active {
		status = "enabled"
	} else if out.State.Enabled && out.Expired {
		status = "expired"
	}
	if _, err := fmt.Fprintf(c.stdout, "maintenance: %s\n", status); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(c.stdout, "backend: %s shared=%t\n", out.Backend.Name, out.Backend.Shared); err != nil {
		return err
	}
	if out.State.Mode != "" {
		if _, err := fmt.Fprintf(c.stdout, "mode: %s\n", out.State.Mode); err != nil {
			return err
		}
	}
	if out.State.Reason != "" {
		if _, err := fmt.Fprintf(c.stdout, "reason: %s\n", out.State.Reason); err != nil {
			return err
		}
	}
	if out.State.Actor != "" {
		if _, err := fmt.Fprintf(c.stdout, "actor: %s\n", out.State.Actor); err != nil {
			return err
		}
	}
	if !out.CheckedAt.IsZero() {
		if _, err := fmt.Fprintf(c.stdout, "checked_at: %s\n", out.CheckedAt.Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	for _, warning := range out.Warnings {
		if _, err := fmt.Fprintf(c.stdout, "warning: %s\n", warning); err != nil {
			return err
		}
	}
	if out.Duration != "" {
		if _, err := fmt.Fprintf(c.stdout, "duration: %s\n", out.Duration); err != nil {
			return err
		}
	}
	return nil
}

// adminMaintenanceSafeError redacts exact DSN text and collapses untrusted
// backend errors so command output cannot accidentally expose credentials.
func adminMaintenanceSafeError(err error, postgresDSN string) string {
	if err == nil {
		return ""
	}
	message := strings.Join(strings.Fields(strings.ToValidUTF8(err.Error(), "")), " ")
	if dsn := strings.TrimSpace(postgresDSN); dsn != "" {
		message = strings.ReplaceAll(message, dsn, "[REDACTED]")
	}
	if utf8.RuneCountInString(message) <= adminMaintenanceErrorRuneLimit {
		return message
	}
	runes := []rune(message)
	return string(runes[:adminMaintenanceErrorRuneLimit-3]) + "..."
}

// adminMaintenanceDefaultActor uses non-sensitive local identity hints for
// auditability without requiring signed HTTP admin credentials.
func adminMaintenanceDefaultActor() string {
	for _, key := range []string{"OPENCOOK_ADMIN_REQUESTOR_NAME", "USER", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

// printAdminMaintenanceUsage documents maintenance controls separately from
// Chef-facing signed admin operations.
func (c *command) printAdminMaintenanceUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin maintenance status [--json] [--with-timing] [--postgres-dsn DSN]
  opencook admin maintenance check [--json] [--with-timing] [--postgres-dsn DSN]
  opencook admin maintenance enable --reason TEXT --yes [--mode MODE] [--actor ACTOR] [--expires-in DURATION] [--json] [--with-timing] [--postgres-dsn DSN]
  opencook admin maintenance disable --yes [--actor ACTOR] [--json] [--with-timing] [--postgres-dsn DSN]

PostgreSQL-backed maintenance state is shared by OpenCook processes. Without
PostgreSQL, this command can only report process-local fallback limitations.
`)
}
