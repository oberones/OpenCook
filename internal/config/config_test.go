package config

import "testing"

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("OPENCOOK_SERVICE_NAME", "")
	t.Setenv("OPENCOOK_ENV", "")
	t.Setenv("OPENCOOK_LISTEN_ADDRESS", "")
	t.Setenv("OPENCOOK_DEFAULT_ORGANIZATION", "")
	t.Setenv("OPENCOOK_READ_TIMEOUT", "")
	t.Setenv("OPENCOOK_WRITE_TIMEOUT", "")
	t.Setenv("OPENCOOK_SHUTDOWN_TIMEOUT", "")
	t.Setenv("OPENCOOK_AUTH_SKEW", "")
	t.Setenv("OPENCOOK_MAX_AUTH_BODY_BYTES", "")
	t.Setenv("OPENCOOK_MAX_BLOB_UPLOAD_BYTES", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_MODE", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_REQUESTOR_NAME", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_REQUESTOR_ORG", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID", "")
	t.Setenv("OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH", "")
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

	if cfg.DefaultOrganization != "" {
		t.Fatalf("DefaultOrganization = %q, want empty string", cfg.DefaultOrganization)
	}

	if cfg.AuthSkew.String() != "15m0s" {
		t.Fatalf("AuthSkew = %q, want %q", cfg.AuthSkew.String(), "15m0s")
	}

	if cfg.MaxAuthBodyBytes != DefaultMaxAuthBodyBytes {
		t.Fatalf("MaxAuthBodyBytes = %d, want %d", cfg.MaxAuthBodyBytes, DefaultMaxAuthBodyBytes)
	}

	if cfg.MaxBlobUploadBytes != DefaultMaxBlobUploadBytes {
		t.Fatalf("MaxBlobUploadBytes = %d, want %d", cfg.MaxBlobUploadBytes, DefaultMaxBlobUploadBytes)
	}

	if !cfg.BootstrapMode {
		t.Fatalf("BootstrapMode = %v, want true", cfg.BootstrapMode)
	}

	if cfg.BootstrapRequestorType != "user" {
		t.Fatalf("BootstrapRequestorType = %q, want %q", cfg.BootstrapRequestorType, "user")
	}

	if cfg.BootstrapRequestorKeyID != "default" {
		t.Fatalf("BootstrapRequestorKeyID = %q, want %q", cfg.BootstrapRequestorKeyID, "default")
	}
}

func TestLoadFromEnvMaxAuthBodyBytes(t *testing.T) {
	t.Setenv("OPENCOOK_MAX_AUTH_BODY_BYTES", "2048")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.MaxAuthBodyBytes != 2048 {
		t.Fatalf("MaxAuthBodyBytes = %d, want %d", cfg.MaxAuthBodyBytes, 2048)
	}
}

func TestLoadFromEnvMaxBlobUploadBytes(t *testing.T) {
	t.Setenv("OPENCOOK_MAX_BLOB_UPLOAD_BYTES", "4096")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.MaxBlobUploadBytes != 4096 {
		t.Fatalf("MaxBlobUploadBytes = %d, want %d", cfg.MaxBlobUploadBytes, 4096)
	}
}

func TestLoadFromEnvDefaultOrganization(t *testing.T) {
	t.Setenv("OPENCOOK_DEFAULT_ORGANIZATION", "ponyville")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.DefaultOrganization != "ponyville" {
		t.Fatalf("DefaultOrganization = %q, want %q", cfg.DefaultOrganization, "ponyville")
	}
}
