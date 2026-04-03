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

func TestMemoryIndexIndexesIncludeClient(t *testing.T) {
	state := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "pivotal"})
	if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	index := NewMemoryIndex(state, "")
	indexes, err := index.Indexes(context.Background(), "ponyville")
	if err != nil {
		t.Fatalf("Indexes() error = %v", err)
	}
	if len(indexes) != 4 {
		t.Fatalf("indexes len = %d, want 4 (%v)", len(indexes), indexes)
	}
	if indexes[0] != "client" {
		t.Fatalf("indexes[0] = %q, want %q", indexes[0], "client")
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
