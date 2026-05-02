package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type adminDiagnosticsCollectOutput struct {
	OK         bool            `json:"ok"`
	Command    string          `json:"command"`
	Offline    bool            `json:"offline,omitempty"`
	BundlePath string          `json:"bundle_path"`
	Files      []string        `json:"files"`
	Warnings   []string        `json:"warnings,omitempty"`
	Errors     []adminCLIError `json:"errors,omitempty"`
	Duration   string          `json:"duration,omitempty"`
	DurationMS *int64          `json:"duration_ms,omitempty"`
}

type adminDiagnosticsManifest struct {
	FormatVersion string                         `json:"format_version"`
	GeneratedAt   string                         `json:"generated_at"`
	Command       string                         `json:"command"`
	Offline       bool                           `json:"offline"`
	Files         []adminDiagnosticsManifestFile `json:"files"`
	Warnings      []string                       `json:"warnings,omitempty"`
}

type adminDiagnosticsManifestFile struct {
	Path        string `json:"path"`
	Description string `json:"description"`
}

// runAdminDiagnostics dispatches artifact-producing diagnostic commands. The
// bundle intentionally contains summaries and references, not raw logs, keys,
// database rows, blob objects, or provider credentials.
func (c *command) runAdminDiagnostics(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin diagnostics requires collect\n\n")
	}

	switch args[0] {
	case "collect":
		return c.runAdminDiagnosticsCollect(ctx, args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminDiagnosticsUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin diagnostics command %q\n\n", args[0])
	}
}

// runAdminDiagnosticsCollect writes a tar.gz diagnostics bundle and prints a
// JSON result describing the generated files. Existing output paths require
// --yes so reruns do not accidentally overwrite prior evidence.
func (c *command) runAdminDiagnosticsCollect(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin diagnostics collect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	outputPath := fs.String("output", "", "path to write diagnostics tar.gz bundle")
	offline := fs.Bool("offline", false, "allow offline-only direct PostgreSQL summaries in the bundle")
	yes := fs.Bool("yes", false, "overwrite output path if it already exists")
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin diagnostics collect", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin diagnostics collect received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput
	if *outputPath == "" {
		return c.adminUsageError("admin diagnostics collect requires --output PATH\n\n")
	}
	if _, err := os.Stat(*outputPath); err == nil && !*yes {
		return c.adminUsageError("admin diagnostics collect refuses to overwrite %q without --yes\n\n", *outputPath)
	} else if err != nil && !os.IsNotExist(err) {
		fmt.Fprintf(c.stderr, "inspect diagnostics output path: %v\n", err)
		return exitDependencyUnavailable
	}

	files, warnings, err := c.buildAdminDiagnosticsFiles(ctx, *offline)
	if err != nil {
		out := adminDiagnosticsCollectOutput{
			OK:         false,
			Command:    "diagnostics_collect",
			Offline:    *offline,
			BundlePath: *outputPath,
			Errors:     []adminCLIError{{Code: "diagnostics_build_failed", Message: err.Error()}},
		}
		return c.writeAdminDiagnosticsCollectResult(out, *withTiming, start, exitDependencyUnavailable)
	}

	if err := writeAdminDiagnosticsArchive(*outputPath, files); err != nil {
		out := adminDiagnosticsCollectOutput{
			OK:         false,
			Command:    "diagnostics_collect",
			Offline:    *offline,
			BundlePath: *outputPath,
			Errors:     []adminCLIError{{Code: "diagnostics_write_failed", Message: err.Error()}},
		}
		return c.writeAdminDiagnosticsCollectResult(out, *withTiming, start, exitDependencyUnavailable)
	}

	out := adminDiagnosticsCollectOutput{
		OK:         true,
		Command:    "diagnostics_collect",
		Offline:    *offline,
		BundlePath: *outputPath,
		Files:      sortedDiagnosticFileNames(files),
		Warnings:   warnings,
	}
	return c.writeAdminDiagnosticsCollectResult(out, *withTiming, start, exitOK)
}

// buildAdminDiagnosticsFiles assembles redacted JSON payloads for the archive.
// It reuses existing service/config checks so diagnostics stay aligned with the
// operator commands they summarize.
func (c *command) buildAdminDiagnosticsFiles(ctx context.Context, offline bool) (map[string][]byte, []string, error) {
	files := map[string][]byte{}
	warnings := []string{
		"diagnostics bundles include summaries and log references only; collect external log files separately if needed",
		"do not attach private keys, request signatures, raw DSNs, database dumps, or blob object contents to support tickets",
		"diagnostics collection is read-only and does not acquire maintenance mode; enable maintenance separately before online repair, reindex, or migration cutover work",
	}

	logsOutput, _ := c.buildAdminLogsPathsOutput()
	statusOutput, _ := c.buildAdminServiceStatusOutput()
	doctorOutput, _ := c.buildAdminServiceDoctorOutput(ctx, offline)
	configOutput := c.buildAdminDiagnosticsConfigOutput()
	runbooks := adminDiagnosticsRunbooks()

	payloads := map[string]any{
		"logs/paths.json":       logsOutput,
		"config/redacted.json":  configOutput,
		"service/status.json":   statusOutput,
		"service/doctor.json":   doctorOutput,
		"runbooks/summary.json": runbooks,
	}
	for path, value := range payloads {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			return nil, nil, fmt.Errorf("marshal diagnostics payload %s: %w", path, err)
		}
		files[path] = append(data, '\n')
	}

	manifestNames := append(sortedDiagnosticFileNames(files), "manifest.json")
	sort.Strings(manifestNames)
	manifest, err := adminDiagnosticsManifestJSON(manifestNames, offline, warnings)
	if err != nil {
		return nil, nil, err
	}
	files["manifest.json"] = manifest
	return files, warnings, nil
}

// buildAdminDiagnosticsConfigOutput captures config validation in the bundle
// without forcing diagnostics collection to fail when validation finds issues.
func (c *command) buildAdminDiagnosticsConfigOutput() adminConfigCheckOutput {
	out := adminConfigCheckOutput{
		OK:      true,
		Command: "config_check",
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
		return out
	}
	out.Config = cfg.Redacted()
	acc := c.adminConfigCheck(cfg, false)
	out.Checks = acc.checks
	out.Warnings = acc.warnings
	out.Errors = acc.errors
	out.OK = len(acc.errors) == 0
	return out
}

// adminDiagnosticsManifestJSON records bundle contents and human-safe warnings
// before the archive is written.
func adminDiagnosticsManifestJSON(fileNames []string, offline bool, warnings []string) ([]byte, error) {
	files := make([]adminDiagnosticsManifestFile, 0, len(fileNames))
	for _, name := range fileNames {
		files = append(files, adminDiagnosticsManifestFile{
			Path:        name,
			Description: adminDiagnosticsFileDescription(name),
		})
	}
	manifest := adminDiagnosticsManifest{
		FormatVersion: "opencook.diagnostics.v1",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		Command:       "diagnostics_collect",
		Offline:       offline,
		Files:         files,
		Warnings:      warnings,
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal diagnostics manifest: %w", err)
	}
	return append(data, '\n'), nil
}

// adminDiagnosticsFileDescription gives each archive member a short purpose so
// operators can inspect manifest.json before sharing the bundle.
func adminDiagnosticsFileDescription(path string) string {
	switch path {
	case "config/redacted.json":
		return "redacted runtime configuration and static config validation checks"
	case "logs/paths.json":
		return "log discovery references for stdout/stderr and common supervisors"
	case "service/status.json":
		return "local service status summary and static dependency checks"
	case "service/doctor.json":
		return "non-mutating dependency diagnostics and optional offline aggregate state counts"
	case "runbooks/summary.json":
		return "selected operational runbook command references"
	default:
		return "diagnostics payload"
	}
}

// writeAdminDiagnosticsArchive writes a tar.gz archive atomically enough for
// operator use: data is written to a temp file in the target directory before
// being renamed into place.
func writeAdminDiagnosticsArchive(path string, files map[string][]byte) error {
	if len(files) == 0 {
		return fmt.Errorf("diagnostics archive requires at least one payload")
	}
	cleanPath := filepath.Clean(path)
	dir := filepath.Dir(cleanPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".opencook-diagnostics-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)

	if err := writeAdminDiagnosticsTarGzip(tmp, files); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, cleanPath); err != nil {
		return err
	}
	return nil
}

// writeAdminDiagnosticsTarGzip serializes the provided JSON payloads with
// conservative file permissions and stable member ordering.
func writeAdminDiagnosticsTarGzip(w io.Writer, files map[string][]byte) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	now := time.Now().UTC()
	for _, name := range sortedDiagnosticFileNames(files) {
		data := files[name]
		if err := tw.WriteHeader(&tar.Header{
			Name:    name,
			Mode:    0o600,
			Size:    int64(len(data)),
			ModTime: now,
		}); err != nil {
			return err
		}
		if _, err := io.Copy(tw, bytes.NewReader(data)); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gz.Close()
}

// sortedDiagnosticFileNames returns archive member names in deterministic order
// for stable tests and predictable operator inspection.
func sortedDiagnosticFileNames(files map[string][]byte) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// writeAdminDiagnosticsCollectResult attaches optional timing data and writes
// the JSON summary for diagnostics collection.
func (c *command) writeAdminDiagnosticsCollectResult(out adminDiagnosticsCollectOutput, withTiming bool, start time.Time, exitCode int) int {
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write diagnostics output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// printAdminDiagnosticsUsage documents the safe diagnostics bundle workflow and
// makes the non-goals explicit so operators do not expect database or blob dumps.
func (c *command) printAdminDiagnosticsUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin diagnostics collect --output PATH [--offline] [--yes] [--json] [--with-timing]

Create a tar.gz diagnostics bundle containing redacted config, service summaries,
dependency diagnostics, log references, and runbook metadata. The bundle does not
include private keys, request signatures, raw DSNs, database dumps, blob objects,
or copied log file contents.
`)
}
