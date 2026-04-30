package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

type adminConfigCheckOutput struct {
	OK         bool               `json:"ok"`
	Command    string             `json:"command"`
	Offline    bool               `json:"offline,omitempty"`
	Config     map[string]string  `json:"config,omitempty"`
	Checks     []adminConfigCheck `json:"checks"`
	Warnings   []string           `json:"warnings,omitempty"`
	Errors     []adminCLIError    `json:"errors,omitempty"`
	Duration   string             `json:"duration,omitempty"`
	DurationMS *int64             `json:"duration_ms,omitempty"`
}

type adminConfigCheck struct {
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	Message    string            `json:"message"`
	Details    map[string]string `json:"details,omitempty"`
	Configured bool              `json:"configured"`
}

type adminConfigCheckAccumulator struct {
	checks   []adminConfigCheck
	warnings []string
	errors   []adminCLIError
}

// runAdminConfig dispatches config-oriented operational commands that inspect
// local OPENCOOK_* configuration without constructing a signed HTTP client.
func (c *command) runAdminConfig(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin config requires check\n\n")
	}

	switch args[0] {
	case "check":
		return c.runAdminConfigCheck(ctx, args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminConfigUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin config command %q\n\n", args[0])
	}
}

// runAdminConfigCheck validates server-side runtime configuration and writes a
// redacted JSON report, giving operators preflight feedback before startup.
func (c *command) runAdminConfigCheck(_ context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin config check", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	offline := fs.Bool("offline", false, "report that validation is intended for an offline maintenance window")
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin config check", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin config check received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput // Admin operational commands currently emit JSON as their stable output mode.

	out := adminConfigCheckOutput{
		OK:      true,
		Command: "config_check",
		Offline: *offline,
		Checks:  []adminConfigCheck{},
	}

	cfg, err := c.loadOffline()
	if err != nil {
		out.OK = false
		out.Checks = append(out.Checks, adminConfigCheck{
			Name:       "runtime_config",
			Status:     "error",
			Message:    "OPENCOOK_* configuration could not be loaded",
			Configured: false,
		})
		out.Errors = append(out.Errors, adminCLIError{Code: "config_load_failed", Message: err.Error()})
		return c.writeAdminConfigCheckResult(out, *withTiming, start, exitDependencyUnavailable)
	}

	out.Config = cfg.Redacted()
	acc := c.adminConfigCheck(cfg, *offline)
	out.Checks = acc.checks
	out.Warnings = acc.warnings
	out.Errors = acc.errors
	out.OK = len(acc.errors) == 0
	exitCode := exitOK
	if !out.OK {
		exitCode = exitDependencyUnavailable
	}
	return c.writeAdminConfigCheckResult(out, *withTiming, start, exitCode)
}

// adminConfigCheck runs each static validator and returns an accumulator so the
// CLI can report every discovered issue in one operator-friendly response.
func (c *command) adminConfigCheck(cfg config.Config, offline bool) adminConfigCheckAccumulator {
	acc := adminConfigCheckAccumulator{}
	acc.add(adminConfigRuntimeCheck())
	acc.add(adminConfigServerCheck(cfg))
	acc.add(adminConfigPostgresCheck(cfg))
	acc.add(adminConfigOpenSearchCheck(cfg))
	acc.add(c.adminConfigBlobCheck(cfg))
	acc.add(adminConfigBootstrapRequestorCheck(cfg))
	if offline {
		acc.add(adminConfigCheck{
			Name:       "offline",
			Status:     "ok",
			Message:    "offline validation requested; no live OpenCook server request is required",
			Configured: true,
		})
	}
	return acc
}

// add records one check and promotes warning/error statuses to the top-level
// warning and error lists without duplicating secret-bearing configuration.
func (a *adminConfigCheckAccumulator) add(check adminConfigCheck) {
	a.checks = append(a.checks, check)
	switch check.Status {
	case "warning":
		a.warnings = append(a.warnings, check.Message)
	case "error":
		a.errors = append(a.errors, adminCLIError{
			Code:    check.Name + "_invalid",
			Message: check.Message,
		})
	}
}

// adminConfigRuntimeCheck records successful config loading as an explicit
// check so failures and successes share the same JSON shape.
func adminConfigRuntimeCheck() adminConfigCheck {
	return adminConfigCheck{
		Name:       "runtime_config",
		Status:     "ok",
		Message:    "OPENCOOK_* configuration loaded successfully",
		Configured: true,
	}
}

// adminConfigServerCheck validates process-local settings that affect startup,
// request handling limits, and listener safety without opening a socket.
func adminConfigServerCheck(cfg config.Config) adminConfigCheck {
	var problems []string
	var warnings []string
	if strings.TrimSpace(cfg.ServiceName) == "" {
		problems = append(problems, "OPENCOOK_SERVICE_NAME must not be empty")
	}
	if strings.TrimSpace(cfg.Environment) == "" {
		problems = append(problems, "OPENCOOK_ENV must not be empty")
	}
	if err := adminConfigValidateListenAddress(cfg.ListenAddress); err != nil {
		problems = append(problems, "OPENCOOK_LISTEN_ADDRESS must be a host:port listener address")
	}
	adminConfigCheckDuration("OPENCOOK_READ_TIMEOUT", cfg.ReadTimeout, &problems, &warnings)
	adminConfigCheckDuration("OPENCOOK_WRITE_TIMEOUT", cfg.WriteTimeout, &problems, &warnings)
	adminConfigCheckDuration("OPENCOOK_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout, &problems, &warnings)
	adminConfigCheckDuration("OPENCOOK_AUTH_SKEW", cfg.AuthSkew, &problems, &warnings)
	if cfg.MaxAuthBodyBytes <= 0 {
		problems = append(problems, "OPENCOOK_MAX_AUTH_BODY_BYTES must be positive")
	}
	if cfg.MaxBlobUploadBytes <= 0 {
		problems = append(problems, "OPENCOOK_MAX_BLOB_UPLOAD_BYTES must be positive")
	}

	return adminConfigCheckFromFindings("server", "server runtime settings are valid", problems, warnings, map[string]string{
		"listen_address":        cfg.ListenAddress,
		"read_timeout":          cfg.ReadTimeout.String(),
		"write_timeout":         cfg.WriteTimeout.String(),
		"shutdown_timeout":      cfg.ShutdownTimeout.String(),
		"auth_skew":             cfg.AuthSkew.String(),
		"max_auth_body_bytes":   fmt.Sprintf("%d", cfg.MaxAuthBodyBytes),
		"max_blob_upload_bytes": fmt.Sprintf("%d", cfg.MaxBlobUploadBytes),
	})
}

// adminConfigPostgresCheck validates PostgreSQL connection-string syntax while
// deliberately avoiding a database connection in the config preflight path.
func adminConfigPostgresCheck(cfg config.Config) adminConfigCheck {
	if strings.TrimSpace(cfg.PostgresDSN) == "" {
		return adminConfigCheck{
			Name:       "postgres",
			Status:     "warning",
			Message:    "OPENCOOK_POSTGRES_DSN is not configured; runtime state will use in-memory persistence",
			Configured: false,
		}
	}
	if _, err := pgconn.ParseConfig(cfg.PostgresDSN); err != nil {
		return adminConfigCheck{
			Name:       "postgres",
			Status:     "error",
			Message:    "OPENCOOK_POSTGRES_DSN is not a valid PostgreSQL connection string",
			Configured: true,
		}
	}
	return adminConfigCheck{
		Name:       "postgres",
		Status:     "ok",
		Message:    "PostgreSQL persistence configuration is syntactically valid",
		Configured: true,
	}
}

// adminConfigOpenSearchCheck validates the configured OpenSearch endpoint shape
// without probing the provider, so credentials and network availability stay out of scope.
func adminConfigOpenSearchCheck(cfg config.Config) adminConfigCheck {
	if strings.TrimSpace(cfg.OpenSearchURL) == "" {
		return adminConfigCheck{
			Name:       "opensearch",
			Status:     "warning",
			Message:    "OPENCOOK_OPENSEARCH_URL is not configured; search routes will use the in-memory compatibility index",
			Configured: false,
		}
	}
	if err := search.ValidateOpenSearchEndpoint(cfg.OpenSearchURL); err != nil {
		return adminConfigCheck{
			Name:       "opensearch",
			Status:     "error",
			Message:    err.Error(),
			Configured: true,
		}
	}
	return adminConfigCheck{
		Name:       "opensearch",
		Status:     "ok",
		Message:    "OpenSearch endpoint configuration is syntactically valid",
		Configured: true,
	}
}

// adminConfigBlobCheck constructs the selected blob adapter far enough to catch
// backend, filesystem, and S3-compatible configuration mistakes without network I/O.
func (c *command) adminConfigBlobCheck(cfg config.Config) adminConfigCheck {
	store, err := c.newBlobStore(cfg)
	if err != nil {
		return adminConfigCheck{
			Name:       "blob",
			Status:     "error",
			Message:    "blob storage configuration is invalid; check OPENCOOK_BLOB_* settings",
			Configured: strings.TrimSpace(cfg.BlobBackend) != "" || strings.TrimSpace(cfg.BlobStorageURL) != "",
		}
	}
	if store == nil {
		return adminConfigCheck{
			Name:       "blob",
			Status:     "error",
			Message:    "blob storage configuration did not produce an active adapter",
			Configured: false,
		}
	}
	status := store.Status()
	check := adminConfigCheck{
		Name:       "blob",
		Status:     "ok",
		Message:    status.Message,
		Configured: status.Configured,
		Details: map[string]string{
			"backend": status.Backend,
		},
	}
	if !status.Configured {
		check.Status = "error"
		return check
	}
	if status.Backend == "memory-compat" {
		check.Status = "warning"
	}
	return check
}

// adminConfigBootstrapRequestorCheck verifies that configured bootstrap
// requestor keys are complete and parseable without exposing PEM material.
func adminConfigBootstrapRequestorCheck(cfg config.Config) adminConfigCheck {
	name := strings.TrimSpace(cfg.BootstrapRequestorName)
	keyPath := strings.TrimSpace(cfg.BootstrapRequestorPublicKeyPath)
	requestorType := strings.TrimSpace(cfg.BootstrapRequestorType)
	if name == "" && keyPath == "" {
		return adminConfigCheck{
			Name:       "bootstrap_requestor",
			Status:     "ok",
			Message:    "bootstrap requestor seeding is not configured",
			Configured: false,
		}
	}

	var problems []string
	if name == "" {
		problems = append(problems, "OPENCOOK_BOOTSTRAP_REQUESTOR_NAME is required when a bootstrap public key is configured")
	}
	if keyPath == "" {
		problems = append(problems, "OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH is required when a bootstrap requestor is configured")
	}
	if requestorType != "user" && requestorType != "client" {
		problems = append(problems, "OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE must be user or client")
	}
	if strings.TrimSpace(cfg.BootstrapRequestorKeyID) == "" {
		problems = append(problems, "OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID must not be empty")
	}
	if keyPath != "" {
		data, err := readOptionalPublicKey(keyPath)
		if err != nil {
			problems = append(problems, "bootstrap public key file is not readable")
		} else if _, err := authn.ParseRSAPublicKeyPEM([]byte(data)); err != nil {
			problems = append(problems, "bootstrap public key file must contain an RSA public key PEM")
		}
	}

	return adminConfigCheckFromFindings("bootstrap_requestor", "bootstrap requestor configuration is valid", problems, nil, map[string]string{
		"requestor_type": requestorType,
		"key_id":         cfg.BootstrapRequestorKeyID,
	})
}

// adminConfigValidateListenAddress checks the listener syntax accepted by
// net/http without binding the configured port.
func adminConfigValidateListenAddress(address string) error {
	address = strings.TrimSpace(address)
	if address == "" {
		return fmt.Errorf("listen address is empty")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if strings.TrimSpace(port) == "" {
		return fmt.Errorf("listen address port is empty")
	}
	_, err = net.LookupPort("tcp", port)
	return err
}

// adminConfigCheckDuration treats negative durations as invalid and zero
// durations as explicit timeout disabling, matching net/http's runtime model.
func adminConfigCheckDuration(name string, value time.Duration, problems, warnings *[]string) {
	switch {
	case value < 0:
		*problems = append(*problems, name+" must not be negative")
	case value == 0:
		*warnings = append(*warnings, name+" is zero; the corresponding timeout is disabled")
	}
}

// adminConfigCheckFromFindings normalizes a section's findings into a single
// check row so the output remains compact while still reporting all issues.
func adminConfigCheckFromFindings(name, okMessage string, problems, warnings []string, details map[string]string) adminConfigCheck {
	check := adminConfigCheck{
		Name:       name,
		Status:     "ok",
		Message:    okMessage,
		Configured: true,
		Details:    details,
	}
	if len(problems) > 0 {
		check.Status = "error"
		check.Message = strings.Join(problems, "; ")
		return check
	}
	if len(warnings) > 0 {
		check.Status = "warning"
		check.Message = strings.Join(warnings, "; ")
	}
	return check
}

// writeAdminConfigCheckResult attaches optional timing data and writes the
// shared config-check JSON envelope.
func (c *command) writeAdminConfigCheckResult(out adminConfigCheckOutput, withTiming bool, start time.Time, exitCode int) int {
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write config check output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// printAdminConfigUsage documents the local config preflight command separately
// from live signed HTTP admin workflows.
func (c *command) printAdminConfigUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin config check [--offline] [--json] [--with-timing]

Validate OPENCOOK_* runtime configuration without starting the server listener
or sending live Chef-facing API requests.
`)
}
