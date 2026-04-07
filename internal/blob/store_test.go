package blob

import (
	"context"
	"errors"
	"testing"

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
	if !status.Configured {
		t.Fatal("Status().Configured = false, want true")
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
