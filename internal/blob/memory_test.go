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
