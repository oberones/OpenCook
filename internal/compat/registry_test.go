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

func TestNewDefaultRegistryIncludesSearchRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/search",
		"/search/{index}",
		"/organizations/{org}/search",
		"/organizations/{org}/search/{index}",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesDefaultClientKeyRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/clients/{name}/keys",
		"/clients/{name}/keys/",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesDataBagRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/data",
		"/data/",
		"/organizations/{org}/data",
		"/organizations/{org}/data/",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesPolicyRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/policies",
		"/policies/{name}",
		"/policies/{name}/revisions",
		"/policies/{name}/revisions/{revision}",
		"/policy_groups",
		"/policy_groups/{group}",
		"/policy_groups/{group}/policies/{name}",
		"/organizations/{org}/policies",
		"/organizations/{org}/policies/{name}",
		"/organizations/{org}/policies/{name}/revisions",
		"/organizations/{org}/policies/{name}/revisions/{revision}",
		"/organizations/{org}/policy_groups",
		"/organizations/{org}/policy_groups/{group}",
		"/organizations/{org}/policy_groups/{group}/policies/{name}",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesSandboxRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/sandboxes",
		"/sandboxes/{id}",
		"/organizations/{org}/sandboxes",
		"/organizations/{org}/sandboxes/{id}",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}

func TestNewDefaultRegistryIncludesCookbookAndBlobRoutes(t *testing.T) {
	registry := NewDefaultRegistry()

	patterns := make(map[string]struct{})
	for _, surface := range registry.Surfaces() {
		for _, pattern := range surface.Patterns {
			patterns[pattern] = struct{}{}
		}
	}

	for _, pattern := range []string{
		"/_blob/checksums/{checksum}",
		"/cookbooks",
		"/cookbooks/_latest",
		"/cookbooks/_recipes",
		"/cookbooks/{name}",
		"/cookbooks/{name}/{version}",
		"/cookbook_artifacts",
		"/cookbook_artifacts/{name}",
		"/cookbook_artifacts/{name}/{identifier}",
		"/universe",
		"/organizations/{org}/cookbooks",
		"/organizations/{org}/cookbooks/_latest",
		"/organizations/{org}/cookbooks/_recipes",
		"/organizations/{org}/cookbooks/{name}",
		"/organizations/{org}/cookbooks/{name}/{version}",
		"/organizations/{org}/cookbook_artifacts",
		"/organizations/{org}/cookbook_artifacts/{name}",
		"/organizations/{org}/cookbook_artifacts/{name}/{identifier}",
		"/organizations/{org}/universe",
	} {
		if _, ok := patterns[pattern]; !ok {
			t.Fatalf("pattern %q missing from compatibility registry", pattern)
		}
	}
}
