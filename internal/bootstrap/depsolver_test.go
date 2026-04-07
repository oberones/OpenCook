package bootstrap

import (
	"errors"
	"testing"
)

func TestSolveEnvironmentCookbookVersionsResolvesRecursiveDependencies(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":                "production",
			"json_class":          "Chef::Environment",
			"chef_type":           "environment",
			"description":         "",
			"cookbook_versions":   map[string]any{},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "2.0.0", map[string]any{
		"dependencies": map[string]any{"foo": "= 1.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "quux", "4.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 2.0.0"},
	}, nil)

	solution, orgExists, envExists, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"quux"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}
	if !orgExists || !envExists {
		t.Fatalf("SolveEnvironmentCookbookVersions() orgExists/envExists = %v/%v, want true/true", orgExists, envExists)
	}
	if len(solution) != 3 {
		t.Fatalf("len(solution) = %d, want %d (%v)", len(solution), 3, solution)
	}
	if solution["foo"].Version != "1.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "1.0.0")
	}
	if solution["bar"].Version != "2.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "2.0.0")
	}
	if solution["quux"].Version != "4.0.0" {
		t.Fatalf("quux version = %q, want %q", solution["quux"].Version, "4.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsReturnsDepsolverErrorForMissingDependency(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":                "production",
			"json_class":          "Chef::Environment",
			"chef_type":           "environment",
			"description":         "",
			"cookbook_versions":   map[string]any{},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.2.3", map[string]any{
		"dependencies": map[string]any{"this_does_not_exist": ">= 0.0.0"},
	}, nil)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want depsolver error")
	}

	var depsolverErr *DepsolverError
	if !errors.As(err, &depsolverErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *DepsolverError", err)
	}
	if depsolverErr.Detail["unsatisfiable_run_list_item"] != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", depsolverErr.Detail["unsatisfiable_run_list_item"], "(foo >= 0.0.0)")
	}
}
