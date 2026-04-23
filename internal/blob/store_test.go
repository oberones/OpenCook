package blob

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/config"
)

func TestNewStoreUsesMemoryByDefault(t *testing.T) {
	store, err := NewStore(config.Config{})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if store.Name() != "memory-blob-store" {
		t.Fatalf("store.Name() = %q, want %q", store.Name(), "memory-blob-store")
	}
}

func TestNewStoreInfersFilesystemBackendFromFileURL(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobStorageURL: "file://" + t.TempDir(),
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if store.Name() != "filesystem-blob-store" {
		t.Fatalf("store.Name() = %q, want %q", store.Name(), "filesystem-blob-store")
	}
}

func TestNewStoreInfersFilesystemBackendFromRelativePath(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobStorageURL: "tmp/opencook-objects",
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if store.Name() != "filesystem-blob-store" {
		t.Fatalf("store.Name() = %q, want %q", store.Name(), "filesystem-blob-store")
	}
}

func TestNewStoreRejectsMalformedBlobStorageURL(t *testing.T) {
	_, err := NewStore(config.Config{
		BlobStorageURL: "://bad-url",
	})
	if err == nil {
		t.Fatal("NewStore() error = nil, want parse error")
	}
}

func TestNewStoreRejectsS3StorageURLWithoutBucket(t *testing.T) {
	_, err := NewStore(config.Config{
		BlobBackend:    BackendS3,
		BlobStorageURL: "s3:///checksums",
	})
	if err == nil {
		t.Fatal("NewStore() error = nil, want missing bucket error")
	}
	if !strings.Contains(err.Error(), "requires a bucket") {
		t.Fatalf("NewStore() error = %v, want missing bucket error", err)
	}
}

func TestNewStoreRejectsMalformedS3Endpoint(t *testing.T) {
	_, err := NewStore(config.Config{
		BlobBackend:       BackendS3,
		BlobStorageURL:    "s3://chef-bucket/checksums",
		BlobS3Endpoint:    "http://",
		BlobS3AccessKeyID: "access-key",
		BlobS3SecretKey:   "secret-key",
	})
	if err == nil {
		t.Fatal("NewStore() error = nil, want malformed endpoint error")
	}
	if !strings.Contains(err.Error(), "parse s3 endpoint") {
		t.Fatalf("NewStore() error = %v, want parse s3 endpoint error", err)
	}
}

func TestNewStoreSelectsS3CompatibleScaffold(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobBackend:    BackendS3,
		BlobStorageURL: "s3://chef-bucket/checksums",
		BlobS3Endpoint: "http://minio.local:9000",
		BlobS3Region:   "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	status := store.Status()
	if status.Backend != "s3-compatible" {
		t.Fatalf("Status().Backend = %q, want %q", status.Backend, "s3-compatible")
	}
	if status.Configured {
		t.Fatal("Status().Configured = true, want false without credentials")
	}
	if !strings.Contains(status.Message, "OPENCOOK_BLOB_S3_ACCESS_KEY_ID") || !strings.Contains(status.Message, "OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY") {
		t.Fatalf("Status().Message = %q, want both missing credential vars", status.Message)
	}

	getter, ok := store.(Getter)
	if !ok {
		t.Fatalf("store does not implement Getter")
	}
	_, err = getter.Get(context.Background(), "abcdef")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, want ErrUnavailable", err)
	}
}

func TestNewStoreReportsConfiguredS3CompatibleBackendWithCredentials(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobBackend:       BackendS3,
		BlobStorageURL:    "s3://chef-bucket/checksums",
		BlobS3Endpoint:    "http://minio.local:9000",
		BlobS3Region:      "us-east-1",
		BlobS3AccessKeyID: "access-key",
		BlobS3SecretKey:   "secret-key",
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	status := store.Status()
	if !status.Configured {
		t.Fatal("Status().Configured = false, want true")
	}
}

func TestNewStoreReportsMissingAccessKeyStatus(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobBackend:     BackendS3,
		BlobStorageURL:  "s3://chef-bucket/checksums",
		BlobS3Endpoint:  "http://minio.local:9000",
		BlobS3Region:    "us-east-1",
		BlobS3SecretKey: "secret-key",
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	status := store.Status()
	if status.Configured {
		t.Fatal("Status().Configured = true, want false")
	}
	if status.Message != "set OPENCOOK_BLOB_S3_ACCESS_KEY_ID to enable S3-compatible blob request operations" {
		t.Fatalf("Status().Message = %q, want missing access key message", status.Message)
	}
}

func TestNewStoreReportsMissingSecretKeyStatus(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobBackend:       BackendS3,
		BlobStorageURL:    "s3://chef-bucket/checksums",
		BlobS3Endpoint:    "http://minio.local:9000",
		BlobS3Region:      "us-east-1",
		BlobS3AccessKeyID: "access-key",
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	status := store.Status()
	if status.Configured {
		t.Fatal("Status().Configured = true, want false")
	}
	if status.Message != "set OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY to enable S3-compatible blob request operations" {
		t.Fatalf("Status().Message = %q, want missing secret key message", status.Message)
	}
}

func TestNewStoreAppliesS3TimeoutAndRetrySettings(t *testing.T) {
	store, err := NewStore(config.Config{
		BlobBackend:          BackendS3,
		BlobStorageURL:       "s3://chef-bucket/checksums",
		BlobS3Endpoint:       "http://minio.local:9000",
		BlobS3Region:         "us-east-1",
		BlobS3AccessKeyID:    "access-key",
		BlobS3SecretKey:      "secret-key",
		BlobS3RequestTimeout: 42 * time.Second,
		BlobS3MaxRetries:     5,
	})
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	s3Store, ok := store.(*S3CompatibleStore)
	if !ok {
		t.Fatalf("store type = %T, want *S3CompatibleStore", store)
	}
	if s3Store.client.Timeout != 42*time.Second {
		t.Fatalf("client.Timeout = %v, want %v", s3Store.client.Timeout, 42*time.Second)
	}
	if s3Store.maxRetries != 5 {
		t.Fatalf("maxRetries = %d, want %d", s3Store.maxRetries, 5)
	}
}
