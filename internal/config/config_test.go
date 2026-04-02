package config

import "testing"

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("OPENCOOK_SERVICE_NAME", "")
	t.Setenv("OPENCOOK_ENV", "")
	t.Setenv("OPENCOOK_LISTEN_ADDRESS", "")
	t.Setenv("OPENCOOK_READ_TIMEOUT", "")
	t.Setenv("OPENCOOK_WRITE_TIMEOUT", "")
	t.Setenv("OPENCOOK_SHUTDOWN_TIMEOUT", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_MODE", "")
	t.Setenv("OPENCOOK_POSTGRES_DSN", "")
	t.Setenv("OPENCOOK_OPENSEARCH_URL", "")
	t.Setenv("OPENCOOK_BLOB_STORAGE_URL", "")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.ServiceName != "opencook" {
		t.Fatalf("ServiceName = %q, want %q", cfg.ServiceName, "opencook")
	}

	if cfg.ListenAddress != ":4000" {
		t.Fatalf("ListenAddress = %q, want %q", cfg.ListenAddress, ":4000")
	}

	if !cfg.BootstrapMode {
		t.Fatalf("BootstrapMode = %v, want true", cfg.BootstrapMode)
	}
}

