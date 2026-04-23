package app

import (
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg"
)

func TestBootstrapOptionsUseMemoryCookbookStoreWithoutConfiguredPostgres(t *testing.T) {
	opts := bootstrapOptions(config.Config{}, pg.New(""))
	if opts.CookbookStoreFactory != nil {
		t.Fatal("CookbookStoreFactory = non-nil, want nil when postgres is not configured")
	}
	if got := resolveCookbookBackend(pg.New("")); got != "memory-bootstrap" {
		t.Fatalf("resolveCookbookBackend(unconfigured) = %q, want %q", got, "memory-bootstrap")
	}
}

func TestBootstrapOptionsUsePostgresCookbookStoreWhenConfigured(t *testing.T) {
	store := pg.New("postgres://example")

	opts := bootstrapOptions(config.Config{}, store)
	if opts.CookbookStoreFactory == nil {
		t.Fatal("CookbookStoreFactory = nil, want postgres-backed cookbook store factory")
	}
	if got := resolveCookbookBackend(store); got != "postgres-scaffold" {
		t.Fatalf("resolveCookbookBackend(configured) = %q, want %q", got, "postgres-scaffold")
	}
}
