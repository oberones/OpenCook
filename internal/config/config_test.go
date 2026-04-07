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
	t.Setenv("OPENCOOK_BLOB_BACKEND", "")
	t.Setenv("OPENCOOK_BLOB_STORAGE_URL", "")
	t.Setenv("OPENCOOK_BLOB_S3_ENDPOINT", "")
	t.Setenv("OPENCOOK_BLOB_S3_REGION", "")
	t.Setenv("OPENCOOK_BLOB_S3_FORCE_PATH_STYLE", "")
	t.Setenv("OPENCOOK_BLOB_S3_DISABLE_TLS", "")
	t.Setenv("OPENCOOK_BLOB_S3_ACCESS_KEY_ID", "")
	t.Setenv("OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY", "")
	t.Setenv("OPENCOOK_BLOB_S3_SESSION_TOKEN", "")

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

func TestLoadFromEnvBlobProviderSettings(t *testing.T) {
	t.Setenv("OPENCOOK_BLOB_BACKEND", "s3")
	t.Setenv("OPENCOOK_BLOB_STORAGE_URL", "s3://chef-bucket/checksums")
	t.Setenv("OPENCOOK_BLOB_S3_ENDPOINT", "http://minio.local:9000")
	t.Setenv("OPENCOOK_BLOB_S3_REGION", "us-east-1")
	t.Setenv("OPENCOOK_BLOB_S3_FORCE_PATH_STYLE", "true")
	t.Setenv("OPENCOOK_BLOB_S3_DISABLE_TLS", "true")
	t.Setenv("OPENCOOK_BLOB_S3_ACCESS_KEY_ID", "access-key")
	t.Setenv("OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY", "secret-key")
	t.Setenv("OPENCOOK_BLOB_S3_SESSION_TOKEN", "session-token")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}

	if cfg.BlobBackend != "s3" {
		t.Fatalf("BlobBackend = %q, want %q", cfg.BlobBackend, "s3")
	}
	if cfg.BlobStorageURL != "s3://chef-bucket/checksums" {
		t.Fatalf("BlobStorageURL = %q, want %q", cfg.BlobStorageURL, "s3://chef-bucket/checksums")
	}
	if cfg.BlobS3Endpoint != "http://minio.local:9000" {
		t.Fatalf("BlobS3Endpoint = %q, want %q", cfg.BlobS3Endpoint, "http://minio.local:9000")
	}
	if cfg.BlobS3Region != "us-east-1" {
		t.Fatalf("BlobS3Region = %q, want %q", cfg.BlobS3Region, "us-east-1")
	}
	if !cfg.BlobS3ForcePathStyle {
		t.Fatal("BlobS3ForcePathStyle = false, want true")
	}
	if !cfg.BlobS3DisableTLS {
		t.Fatal("BlobS3DisableTLS = false, want true")
	}
	if cfg.BlobS3AccessKeyID != "access-key" {
		t.Fatalf("BlobS3AccessKeyID = %q, want %q", cfg.BlobS3AccessKeyID, "access-key")
	}
	if cfg.BlobS3SecretKey != "secret-key" {
		t.Fatalf("BlobS3SecretKey = %q, want %q", cfg.BlobS3SecretKey, "secret-key")
	}
	if cfg.BlobS3SessionToken != "session-token" {
		t.Fatalf("BlobS3SessionToken = %q, want %q", cfg.BlobS3SessionToken, "session-token")
	}
}
