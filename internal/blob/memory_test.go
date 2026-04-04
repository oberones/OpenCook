package blob

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStorePutRejectsEmptyKey(t *testing.T) {
	store := NewMemoryStore("")

	_, err := store.Put(context.Background(), PutRequest{
		Key:  "   ",
		Body: []byte("rainbow"),
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("Put() error = %v, want ErrInvalidInput", err)
	}
}

func TestMemoryStoreDeleteRemovesExistingObjects(t *testing.T) {
	store := NewMemoryStore("")

	if _, err := store.Put(context.Background(), PutRequest{
		Key:  "rainbow",
		Body: []byte("pony"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if err := store.Delete(context.Background(), "rainbow"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	exists, err := store.Exists(context.Background(), "rainbow")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Fatal("Exists() = true, want false after delete")
	}
}
