package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

func TestAdminLogsPathsReportsSafeSupervisorReferences(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return validAdminConfigCheckConfig(t, ""), nil
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		t.Fatal("admin logs paths should not construct a live admin client")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "logs", "paths", "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(admin logs paths) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminLogsPathsOutput(t, stdout.String())
	if out["ok"] != true || out["command"] != "logs_paths" {
		t.Fatalf("logs paths output = %v, want ok logs_paths", out)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("duration_ms missing from logs paths output: %v", out)
	}
	paths := adminLogsPathRows(t, out)
	for _, name := range []string{"stdout", "stderr", "systemd_journal", "docker_compose", "kubernetes"} {
		if paths[name] == nil {
			t.Fatalf("logs paths missing %q: %v", name, paths)
		}
	}
	for _, secret := range []string{"pgsecret", "searchsecret", "private-bucket"} {
		if strings.Contains(stdout.String(), secret) {
			t.Fatalf("logs paths leaked secret %q: %s", secret, stdout.String())
		}
	}
}

func TestAdminDiagnosticsCollectWritesRedactedBundle(t *testing.T) {
	publicKeyPath := filepath.Join(t.TempDir(), "bootstrap.pub")
	key := mustGenerateAdminPrivateKey(t)
	if err := os.WriteFile(publicKeyPath, mustMarshalPublicKeyPEM(t, &key.PublicKey), 0o600); err != nil {
		t.Fatalf("WriteFile(public key) error = %v", err)
	}

	cfg := validAdminConfigCheckConfig(t, publicKeyPath)
	outputPath := filepath.Join(t.TempDir(), "diagnostics.tar.gz")
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		if !strings.Contains(raw, "searchsecret") {
			t.Fatalf("OpenSearch URL = %q, want configured secret-bearing URL before redaction", raw)
		}
		return &fakeServiceSearchTarget{}, nil
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		t.Fatal("admin diagnostics collect should not construct a live admin client")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "diagnostics", "collect", "--output", outputPath, "--yes", "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(admin diagnostics collect) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	var out map[string]any
	if err := json.Unmarshal([]byte(stdout.String()), &out); err != nil {
		t.Fatalf("json.Unmarshal(diagnostics output) error = %v; body = %s", err, stdout.String())
	}
	if out["ok"] != true || out["bundle_path"] != outputPath {
		t.Fatalf("diagnostics output = %v, want ok with bundle path %q", out, outputPath)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("duration_ms missing from diagnostics output: %v", out)
	}

	files := readDiagnosticsArchive(t, outputPath)
	for _, name := range []string{
		"manifest.json",
		"config/redacted.json",
		"logs/paths.json",
		"service/status.json",
		"service/doctor.json",
		"runbooks/summary.json",
	} {
		if files[name] == "" {
			t.Fatalf("diagnostics archive missing %s; files = %v", name, sortedArchiveNames(files))
		}
	}
	if !strings.Contains(files["manifest.json"], "opencook.diagnostics.v1") {
		t.Fatalf("manifest missing format version: %s", files["manifest.json"])
	}
	if !strings.Contains(files["runbooks/summary.json"], "opencook admin migration backup create") {
		t.Fatalf("runbook summary missing migration backup reference: %s", files["runbooks/summary.json"])
	}

	archiveText := strings.Join(mapValues(files), "\n")
	for _, secret := range []string{"pgsecret", "searchsecret", "private-bucket", publicKeyPath, "PRIVATE KEY"} {
		if strings.Contains(archiveText, secret) {
			t.Fatalf("diagnostics archive leaked %q:\n%s", secret, archiveText)
		}
	}
}

func TestAdminDiagnosticsCollectRefusesOverwriteWithoutYes(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "diagnostics.tar.gz")
	if err := os.WriteFile(outputPath, []byte("existing"), 0o600); err != nil {
		t.Fatalf("WriteFile(existing output) error = %v", err)
	}

	cmd, _, stderr := newTestCommand(t)
	code := cmd.Run(context.Background(), []string{"admin", "diagnostics", "collect", "--output", outputPath, "--json"})
	if code != exitUsage {
		t.Fatalf("Run(admin diagnostics collect existing output) exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "without --yes") {
		t.Fatalf("stderr = %q, want overwrite guidance", stderr.String())
	}
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("ReadFile(existing output) error = %v", err)
	}
	if string(data) != "existing" {
		t.Fatalf("existing output was overwritten without --yes: %q", string(data))
	}
}

func decodeAdminLogsPathsOutput(t *testing.T, body string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("json.Unmarshal(logs paths output) error = %v; body = %s", err, body)
	}
	return out
}

func adminLogsPathRows(t *testing.T, out map[string]any) map[string]map[string]any {
	t.Helper()

	rawPaths, ok := out["paths"].([]any)
	if !ok {
		t.Fatalf("paths = %T, want []any (%v)", out["paths"], out)
	}
	paths := make(map[string]map[string]any, len(rawPaths))
	for _, raw := range rawPaths {
		path, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("path = %T, want map[string]any", raw)
		}
		name, ok := path["name"].(string)
		if !ok || name == "" {
			t.Fatalf("path name = %v, want non-empty string", path["name"])
		}
		paths[name] = path
	}
	return paths
}

func readDiagnosticsArchive(t *testing.T, path string) map[string]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(diagnostics archive) error = %v", err)
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip.NewReader(diagnostics archive) error = %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	files := map[string]string{}
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("tar.Next(diagnostics archive) error = %v", err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatalf("ReadAll(%s) error = %v", header.Name, err)
		}
		files[header.Name] = string(data)
	}
	return files
}

func sortedArchiveNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func mapValues(files map[string]string) []string {
	values := make([]string, 0, len(files))
	for _, value := range files {
		values = append(values, value)
	}
	return values
}
