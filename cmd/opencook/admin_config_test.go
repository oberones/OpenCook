package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/config"
)

func TestAdminConfigCheckReportsValidRedactedConfiguration(t *testing.T) {
	publicKeyPath := filepath.Join(t.TempDir(), "bootstrap.pub")
	key := mustGenerateAdminPrivateKey(t)
	if err := os.WriteFile(publicKeyPath, mustMarshalPublicKeyPEM(t, &key.PublicKey), 0o600); err != nil {
		t.Fatalf("WriteFile(public key) error = %v", err)
	}

	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return validAdminConfigCheckConfig(t, publicKeyPath), nil
	}
	cmd.newBlobStore = blob.NewStore
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		t.Fatal("admin config check should not construct a live admin client")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "config", "check", "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(admin config check) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminConfigCheckOutput(t, stdout.String())
	if out["ok"] != true {
		t.Fatalf("ok = %v, want true; output = %v", out["ok"], out)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("duration_ms missing from output: %v", out)
	}
	checks := adminConfigCheckRows(t, out)
	for _, name := range []string{"runtime_config", "server", "postgres", "opensearch", "blob", "bootstrap_requestor"} {
		check := checks[name]
		if check["status"] != "ok" {
			t.Fatalf("check %s = %v, want ok", name, check)
		}
	}
	body := stdout.String()
	for _, secret := range []string{"pgsecret", "searchsecret", publicKeyPath} {
		if strings.Contains(body, secret) {
			t.Fatalf("config check leaked secret/path %q in output: %s", secret, body)
		}
	}
}

func TestAdminConfigCheckAllowsStandaloneFallbacksWithWarnings(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{}
	}
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, "")
		cfg.PostgresDSN = ""
		cfg.OpenSearchURL = ""
		cfg.BlobBackend = ""
		cfg.BlobStorageURL = ""
		cfg.BootstrapRequestorName = ""
		cfg.BootstrapRequestorPublicKeyPath = ""
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore

	code := cmd.Run(context.Background(), []string{"admin", "--json", "config", "check"})
	if code != exitOK {
		t.Fatalf("Run(admin --json config check) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminConfigCheckOutput(t, stdout.String())
	if out["ok"] != true {
		t.Fatalf("ok = %v, want true; output = %v", out["ok"], out)
	}
	checks := adminConfigCheckRows(t, out)
	for _, name := range []string{"postgres", "opensearch", "blob"} {
		if checks[name]["status"] != "warning" {
			t.Fatalf("check %s = %v, want warning", name, checks[name])
		}
	}
}

func TestAdminConfigCheckReportsErrorsWithoutProviderSecrets(t *testing.T) {
	publicKeyPath := filepath.Join(t.TempDir(), "bootstrap.pub")
	if err := os.WriteFile(publicKeyPath, []byte("not a public key"), 0o600); err != nil {
		t.Fatalf("WriteFile(public key) error = %v", err)
	}

	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, publicKeyPath)
		cfg.OpenSearchURL = "ftp://search-user:searchsecret@opensearch.example"
		cfg.BlobBackend = "s3"
		cfg.BlobStorageURL = "s3://private-bucket/opencook"
		cfg.BlobS3AccessKeyID = "ACCESSSECRET"
		cfg.BlobS3SecretKey = ""
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore

	code := cmd.Run(context.Background(), []string{"admin", "config", "check", "--offline", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(invalid admin config check) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}

	out := decodeAdminConfigCheckOutput(t, stdout.String())
	if out["ok"] != false || out["offline"] != true {
		t.Fatalf("output = %v, want failed offline check", out)
	}
	checks := adminConfigCheckRows(t, out)
	for _, name := range []string{"opensearch", "blob", "bootstrap_requestor"} {
		if checks[name]["status"] != "error" {
			t.Fatalf("check %s = %v, want error", name, checks[name])
		}
	}
	body := stdout.String()
	for _, secret := range []string{"searchsecret", "private-bucket", "ACCESSSECRET", publicKeyPath} {
		if strings.Contains(body, secret) {
			t.Fatalf("invalid config check leaked secret/path %q in output: %s", secret, body)
		}
	}
}

func TestAdminConfigCheckReportsConfigLoadFailureAsJSON(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{}, errors.New("OPENCOOK_READ_TIMEOUT: invalid duration")
	}

	code := cmd.Run(context.Background(), []string{"admin", "config", "check", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(load failure admin config check) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}

	out := decodeAdminConfigCheckOutput(t, stdout.String())
	if out["ok"] != false {
		t.Fatalf("ok = %v, want false; output = %v", out["ok"], out)
	}
	checks := adminConfigCheckRows(t, out)
	if checks["runtime_config"]["status"] != "error" {
		t.Fatalf("runtime_config check = %v, want error", checks["runtime_config"])
	}
}

func validAdminConfigCheckConfig(t *testing.T, publicKeyPath string) config.Config {
	t.Helper()

	return config.Config{
		ServiceName:                     "opencook",
		Environment:                     "test",
		ListenAddress:                   ":4000",
		ReadTimeout:                     15 * time.Second,
		WriteTimeout:                    30 * time.Second,
		ShutdownTimeout:                 10 * time.Second,
		AuthSkew:                        15 * time.Minute,
		MaxAuthBodyBytes:                config.DefaultMaxAuthBodyBytes,
		MaxBlobUploadBytes:              config.DefaultMaxBlobUploadBytes,
		PostgresDSN:                     "postgres://opencook:pgsecret@postgres.example/opencook",
		OpenSearchURL:                   "https://search-user:searchsecret@opensearch.example",
		BlobBackend:                     "filesystem",
		BlobStorageURL:                  t.TempDir(),
		BootstrapRequestorName:          "pivotal",
		BootstrapRequestorType:          "user",
		BootstrapRequestorKeyID:         "default",
		BootstrapRequestorPublicKeyPath: publicKeyPath,
	}
}

func decodeAdminConfigCheckOutput(t *testing.T, body string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("json.Unmarshal(config check output) error = %v; body = %s", err, body)
	}
	return out
}

func adminConfigCheckRows(t *testing.T, out map[string]any) map[string]map[string]any {
	t.Helper()

	rawChecks, ok := out["checks"].([]any)
	if !ok {
		t.Fatalf("checks = %T, want []any (%v)", out["checks"], out)
	}
	checks := make(map[string]map[string]any, len(rawChecks))
	for _, raw := range rawChecks {
		check, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("check = %T, want map[string]any", raw)
		}
		name, ok := check["name"].(string)
		if !ok || name == "" {
			t.Fatalf("check name = %v, want non-empty string", check["name"])
		}
		checks[name] = check
	}
	return checks
}
