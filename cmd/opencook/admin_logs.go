package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/oberones/OpenCook/internal/config"
)

type adminLogsPathsOutput struct {
	OK          bool              `json:"ok"`
	Command     string            `json:"command"`
	ServiceName string            `json:"service_name,omitempty"`
	Environment string            `json:"environment,omitempty"`
	Paths       []adminLogPath    `json:"paths"`
	Warnings    []string          `json:"warnings,omitempty"`
	Errors      []adminCLIError   `json:"errors,omitempty"`
	Duration    string            `json:"duration,omitempty"`
	DurationMS  *int64            `json:"duration_ms,omitempty"`
	Config      map[string]string `json:"config,omitempty"`
}

type adminLogPath struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Configured bool   `json:"configured"`
	Location   string `json:"location,omitempty"`
	Command    string `json:"command,omitempty"`
	Message    string `json:"message"`
}

// runAdminLogs dispatches log-discovery commands. These commands describe
// where OpenCook writes operational logs without opening or copying log files.
func (c *command) runAdminLogs(_ context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin logs requires paths\n\n")
	}

	switch args[0] {
	case "paths":
		return c.runAdminLogsPaths(args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminLogsUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin logs command %q\n\n", args[0])
	}
}

// runAdminLogsPaths prints a redacted, machine-readable list of log discovery
// locations for stdout/stderr and common external supervisors.
func (c *command) runAdminLogsPaths(args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin logs paths", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin logs paths", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin logs paths received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput

	out, exitCode := c.buildAdminLogsPathsOutput()
	return c.writeAdminLogsPathsResult(out, *withTiming, start, exitCode)
}

// buildAdminLogsPathsOutput derives log references from redacted runtime
// configuration. OpenCook currently logs to process streams and expects
// systemd, Docker, Kubernetes, or another supervisor to persist those streams.
func (c *command) buildAdminLogsPathsOutput() (adminLogsPathsOutput, int) {
	out := adminLogsPathsOutput{
		OK:      true,
		Command: "logs_paths",
		Paths:   []adminLogPath{},
	}
	cfg, err := c.loadOffline()
	if err != nil {
		out.OK = false
		out.Errors = append(out.Errors, adminCLIError{Code: "config_load_failed", Message: err.Error()})
		out.Paths = defaultAdminLogPaths(config.Config{ServiceName: "opencook", Environment: "unknown"})
		return out, exitDependencyUnavailable
	}

	out.ServiceName = cfg.ServiceName
	out.Environment = cfg.Environment
	out.Config = cfg.Redacted()
	out.Paths = defaultAdminLogPaths(cfg)
	out.Warnings = append(out.Warnings, "OpenCook does not manage log files directly; capture process stdout/stderr with your supervisor")
	return out, exitOK
}

// defaultAdminLogPaths returns stable operator hints without reading arbitrary
// filesystem paths or embedding deployment-specific credentials.
func defaultAdminLogPaths(cfg config.Config) []adminLogPath {
	serviceName := cfg.ServiceName
	if serviceName == "" {
		serviceName = "opencook"
	}
	return []adminLogPath{
		{
			Name:       "stdout",
			Type:       "stream",
			Configured: true,
			Location:   "process stdout",
			Message:    "opencook serve writes startup summaries and structured request logs to stdout",
		},
		{
			Name:       "stderr",
			Type:       "stream",
			Configured: true,
			Location:   "process stderr",
			Message:    "CLI startup and fatal process errors are written to stderr by the invoking supervisor",
		},
		{
			Name:       "systemd_journal",
			Type:       "supervisor",
			Configured: false,
			Command:    fmt.Sprintf("journalctl -u %s", serviceName),
			Message:    "use when OpenCook is supervised by systemd",
		},
		{
			Name:       "docker_compose",
			Type:       "supervisor",
			Configured: false,
			Command:    "docker compose logs opencook",
			Message:    "use when OpenCook is supervised by Docker Compose",
		},
		{
			Name:       "kubernetes",
			Type:       "supervisor",
			Configured: false,
			Command:    fmt.Sprintf("kubectl logs deployment/%s", serviceName),
			Message:    "use when OpenCook is supervised by Kubernetes",
		},
	}
}

// writeAdminLogsPathsResult attaches optional timing data and writes the shared
// log-discovery JSON envelope.
func (c *command) writeAdminLogsPathsResult(out adminLogsPathsOutput, withTiming bool, start time.Time, exitCode int) int {
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write logs paths output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// printAdminLogsUsage documents log discovery separately from diagnostics
// collection because it is read-only and does not create any artifacts.
func (c *command) printAdminLogsUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin logs paths [--json] [--with-timing]

Show safe log-discovery references for stdout/stderr and common external
supervisors. OpenCook does not copy or persist log files itself.
`)
}
