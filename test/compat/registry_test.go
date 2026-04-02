package compat_test

import (
	"testing"

	"github.com/oberones/OpenCook/internal/compat"
)

func TestDefaultRegistryIncludesCoreSurfaces(t *testing.T) {
	registry := compat.NewDefaultRegistry()
	surfaces := registry.Surfaces()

	if len(surfaces) < 6 {
		t.Fatalf("len(Surfaces()) = %d, want at least 6", len(surfaces))
	}

	if registry.RouteCount() == 0 {
		t.Fatal("RouteCount() = 0, want non-zero")
	}
}
