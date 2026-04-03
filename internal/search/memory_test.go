package search

import (
	"context"
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestMemoryIndexIndexesRequiresConfiguredState(t *testing.T) {
	index := NewMemoryIndex(nil, "")

	_, err := index.Indexes(context.Background(), "ponyville")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Indexes() error = %v, want ErrUnavailable", err)
	}
}

func TestMemoryIndexIndexesReturnsNotFoundForUnknownOrganization(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	index := NewMemoryIndex(state, "")

	_, err := index.Indexes(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Indexes() error = %v, want ErrNotFound", err)
	}
}

func TestMemoryIndexSearchReturnsNotFoundForUnknownOrganization(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	index := NewMemoryIndex(state, "")

	_, err := index.Search(context.Background(), Query{
		Organization: "missing",
		Index:        "node",
		Q:            "*:*",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Search() error = %v, want ErrNotFound", err)
	}
}
