package compat

import "testing"

func TestNewDefaultRegistryIncludesOrganizationsCollectionForms(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{"/organizations", "/organizations/"} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesRoleEnvironmentRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/roles",
		"/roles/",
		"/roles/{name}/environments",
		"/roles/{name}/environments/{environment}",
		"/organizations/{org}/roles",
		"/organizations/{org}/roles/{name}/environments/{environment}",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}
