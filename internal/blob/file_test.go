package blob

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestFileStorePutGetExistsAndDelete(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	if _, err := store.Put(context.Background(), PutRequest{
		Key:  "abcdef0123456789",
		Body: []byte("rainbow"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	exists, err := store.Exists(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Fatal("Exists() = false, want true")
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}

	if err := store.Delete(context.Background(), "abcdef0123456789"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	exists, err = store.Exists(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Exists() after delete error = %v", err)
	}
	if exists {
		t.Fatal("Exists() = true after delete, want false")
	}
}

func TestNewFileStoreAcceptsFileURL(t *testing.T) {
	root := filepath.Join(t.TempDir(), "blobs")
	store, err := NewFileStore("file://" + root)
	if err != nil {
		t.Fatalf("NewFileStore(file://) error = %v", err)
	}

	status := store.Status()
	if status.Backend != "filesystem" {
		t.Fatalf("Status().Backend = %q, want %q", status.Backend, "filesystem")
	}
	if !status.Configured {
		t.Fatal("Status().Configured = false, want true")
	}
}

func TestFileStoreRejectsInvalidKeys(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	_, err = store.Put(context.Background(), PutRequest{
		Key:  "nested/path",
		Body: []byte("rainbow"),
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Put() error = %v, want ErrInvalidInput", err)
	}
}

func TestFileStorePutOverwritesExistingObjects(t *testing.T) {
	store, err := NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}

	if _, err := store.Put(context.Background(), PutRequest{
		Key:  "abcdef0123456789",
		Body: []byte("first"),
	}); err != nil {
		t.Fatalf("Put(first) error = %v", err)
	}

	if _, err := store.Put(context.Background(), PutRequest{
		Key:  "abcdef0123456789",
		Body: []byte("second"),
	}); err != nil {
		t.Fatalf("Put(second) error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "second" {
		t.Fatalf("Get() = %q, want %q", string(body), "second")
	}
}
