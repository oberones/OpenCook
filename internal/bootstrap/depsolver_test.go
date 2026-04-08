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

func TestSolveEnvironmentCookbookVersionsChecksScopeBeforeValidation(t *testing.T) {
	service := newTestBootstrapService(t)

	_, orgExists, envExists, err := service.SolveEnvironmentCookbookVersions("missing", "production", nil)
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v, want nil for missing org", err)
	}
	if orgExists || envExists {
		t.Fatalf("SolveEnvironmentCookbookVersions() orgExists/envExists = %v/%v, want false/false", orgExists, envExists)
	}

	createTestCookbookOrg(t, service)
	_, orgExists, envExists, err = service.SolveEnvironmentCookbookVersions("ponyville", "missing", map[string]any{
		"run_list": []any{},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v, want nil for missing environment", err)
	}
	if !orgExists || envExists {
		t.Fatalf("SolveEnvironmentCookbookVersions() orgExists/envExists = %v/%v, want true/false", orgExists, envExists)
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

func TestSolveEnvironmentCookbookVersionsExpandsNestedRolesWithEnvironmentOverrides(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "apache2", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "nginx", "2.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "users", "3.0.0", nil, nil)

	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "base",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{"recipe[nginx]"},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(base) error = %v", err)
	}
	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[base]", "recipe[users]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	productionSolution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"role[web]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions(production) error = %v", err)
	}
	if _, ok := productionSolution["nginx"]; !ok {
		t.Fatalf("production solution missing nginx: %v", productionSolution)
	}
	if _, ok := productionSolution["users"]; !ok {
		t.Fatalf("production solution missing users: %v", productionSolution)
	}
	if _, ok := productionSolution["apache2"]; ok {
		t.Fatalf("production solution unexpectedly included apache2: %v", productionSolution)
	}

	defaultSolution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"role[web]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions(_default) error = %v", err)
	}
	if _, ok := defaultSolution["apache2"]; !ok {
		t.Fatalf("_default solution missing apache2: %v", defaultSolution)
	}
	if _, ok := defaultSolution["users"]; !ok {
		t.Fatalf("_default solution missing users: %v", defaultSolution)
	}
	if _, ok := defaultSolution["nginx"]; ok {
		t.Fatalf("_default solution unexpectedly included nginx: %v", defaultSolution)
	}
}

func TestSolveEnvironmentCookbookVersionsUsesExplicitEmptyEnvironmentRoleRunList(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "apache2", "1.0.0", nil, nil)

	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	productionSolution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"role[web]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions(production) error = %v", err)
	}
	if len(productionSolution) != 0 {
		t.Fatalf("production solution = %v, want empty solution", productionSolution)
	}

	defaultSolution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"role[web]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions(_default) error = %v", err)
	}
	if len(defaultSolution) != 1 {
		t.Fatalf("len(defaultSolution) = %d, want 1 (%v)", len(defaultSolution), defaultSolution)
	}
	if defaultSolution["apache2"].Version != "1.0.0" {
		t.Fatalf("apache2 version = %q, want %q", defaultSolution["apache2"].Version, "1.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsRejectsMissingRole(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"role[missing]"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' contains unknown role item role[missing]" {
		t.Fatalf("validation messages = %v, want unknown role message", validationErr.Messages)
	}
}

func TestSolveEnvironmentCookbookVersionsRejectsRecursiveRole(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[db]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "db",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[web]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(db) error = %v", err)
	}

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"role[web]"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' contains recursive role item role[web]" {
		t.Fatalf("validation messages = %v, want recursive role message", validationErr.Messages)
	}
}

func TestSolveEnvironmentCookbookVersionsRejectsMalformedVersionedRunListItem(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"foo@not_a_version"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' is not a valid run list" {
		t.Fatalf("validation messages = %v, want invalid run_list message", validationErr.Messages)
	}
}

func TestSolveEnvironmentCookbookVersionsRejectsBogusBracketedRunListItem(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"fake[not_good]"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' is not a valid run list" {
		t.Fatalf("validation messages = %v, want invalid run_list message", validationErr.Messages)
	}
}

func TestSolveEnvironmentCookbookVersionsReturnsEmptySolutionForEmptyRunList(t *testing.T) {
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

	solution, orgExists, envExists, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}
	if !orgExists || !envExists {
		t.Fatalf("SolveEnvironmentCookbookVersions() orgExists/envExists = %v/%v, want true/true", orgExists, envExists)
	}
	if len(solution) != 0 {
		t.Fatalf("len(solution) = %d, want 0 (%v)", len(solution), solution)
	}
}

func TestSolveEnvironmentCookbookVersionsSupportsQualifiedAndVersionedRecipeRunListItems(t *testing.T) {
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
	createTestCookbookVersion(t, service, "ponyville", "foo", "2.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo::default", "recipe[bar::install]", "recipe[foo::server@1.0.0]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "1.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "1.0.0")
	}
	if solution["bar"].Version != "1.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "1.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsDeduplicatesEquivalentRunListForms(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 1.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo", "recipe[foo]", "foo::default", "recipe[foo::default]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "1.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "1.0.0")
	}
	if solution["bar"].Version != "1.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "1.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsSelectsPinnedVersionWhenEquivalentFormsArePresent(t *testing.T) {
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
	createTestCookbookVersion(t, service, "ponyville", "foo", "2.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "2.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo", "recipe[foo::default@2.0.0]"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "2.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "2.0.0")
	}
	if solution["bar"].Version != "2.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "2.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsDeduplicatesRoleExpandedEquivalentRunListForms(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 1.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", nil, nil)

	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[foo]", "recipe[foo::default]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"role[web]", "foo"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "1.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "1.0.0")
	}
	if solution["bar"].Version != "1.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "1.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsBacktracksAcrossCompatibleAlternatives(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "1.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.1.0",
			"app5": "= 2.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app4": ">= 0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "1.0.0", map[string]any{
		"dependencies": map[string]any{
			"app4": ">= 0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "0.3.0", map[string]any{
		"dependencies": map[string]any{
			"app5": "= 0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "5.0.0", map[string]any{
		"dependencies": map[string]any{
			"app5": "= 2.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "6.0.0", map[string]any{
		"dependencies": map[string]any{
			"app5": "= 6.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "0.3.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "2.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app4"].Version != "5.0.0" {
		t.Fatalf("app4 version = %q, want %q", solution["app4"].Version, "5.0.0")
	}
	if solution["app5"].Version != "2.0.0" {
		t.Fatalf("app5 version = %q, want %q", solution["app5"].Version, "2.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsReportsFullConstraintPathForTransitiveConflict(t *testing.T) {
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
		"dependencies": map[string]any{
			"bar":  "= 1.0.0",
			"buzz": "= 1.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", map[string]any{
		"dependencies": map[string]any{
			"baz": "= 1.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "buzz", "1.0.0", map[string]any{
		"dependencies": map[string]any{
			"baz": "> 1.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "baz", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "baz", "2.0.0", nil, nil)

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

	if got := depsolverErr.Detail["message"]; got != "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]" {
		t.Fatalf("message = %v, want transitive conflict message", got)
	}
	if got := depsolverErr.Detail["unsatisfiable_run_list_item"]; got != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", got, "(foo >= 0.0.0)")
	}
	if got := depsolverErr.Detail["most_constrained_cookbooks"]; len(got.([]string)) != 1 || got.([]string)[0] != "baz = 1.0.0 -> []" {
		t.Fatalf("most_constrained_cookbooks = %v, want [baz = 1.0.0 -> []]", got)
	}
}

func TestSolveEnvironmentCookbookVersionsSupportsPessimisticConstraintMajorMinorPatch(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": "~> 2.1.1",
			"app3": ">= 0.1.1",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "2.1.5", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "2.2.0", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "3.0.0", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.3", map[string]any{
		"dependencies": map[string]any{"app5": ">= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "6.0.0", map[string]any{
		"dependencies": map[string]any{"app5": ">= 0.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@3.0.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app2"].Version != "2.1.5" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "2.1.5")
	}
}

func TestSolveEnvironmentCookbookVersionsSupportsPessimisticConstraintMajorMinor(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": "~> 2.1",
			"app3": ">= 0.1.1",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "2.1.5", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "2.2.0", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "3.0.0", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.3", map[string]any{
		"dependencies": map[string]any{"app5": ">= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "6.0.0", map[string]any{
		"dependencies": map[string]any{"app5": ">= 0.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@3.0.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app2"].Version != "2.2.0" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "2.2.0")
	}
}

func TestSolveEnvironmentCookbookVersionsResolvesMultiRootCompatibleRunList(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.1.0",
			"app5": "= 2.0.0",
			"app4": ">= 0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "3.0.0", map[string]any{
		"dependencies": map[string]any{"app4": ">= 3.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.3", map[string]any{
		"dependencies": map[string]any{"app5": ">= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "5.0.0", map[string]any{
		"dependencies": map[string]any{"app5": "= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "6.0.0", map[string]any{
		"dependencies": map[string]any{"app5": "= 6.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "2.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1", "app2", "app5"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app4"].Version != "5.0.0" {
		t.Fatalf("app4 version = %q, want %q", solution["app4"].Version, "5.0.0")
	}
	if solution["app5"].Version != "2.0.0" {
		t.Fatalf("app5 version = %q, want %q", solution["app5"].Version, "2.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsReportsMultiRootConflictWithAllKnownConstraintPaths(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.0.0",
			"app5": "= 2.0.0",
			"app4": ">= 0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.0.1", map[string]any{
		"dependencies": map[string]any{"app4": ">= 5.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.0", map[string]any{
		"dependencies": map[string]any{"app5": "= 6.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "5.0.0", map[string]any{
		"dependencies": map[string]any{"app5": "= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "2.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1", "app3"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want depsolver error")
	}

	var depsolverErr *DepsolverError
	if !errors.As(err, &depsolverErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *DepsolverError", err)
	}
	if got := depsolverErr.Detail["unsatisfiable_run_list_item"]; got != "(app3 >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", got, "(app3 >= 0.0.0)")
	}
	if got := depsolverErr.Detail["message"]; got != "Unable to satisfy constraints on package app5 due to solution constraint (app3 >= 0.0.0). Solution constraints that may result in a constraint on app5: [(app1 = 3.0.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app4 >= 0.3.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app2 >= 0.0.0) -> (app4 >= 5.0.0) -> (app5 = 2.0.0), (app3 = 0.1.0) -> (app5 = 6.0.0)]" {
		t.Fatalf("message = %v, want multi-root conflict message", got)
	}
}

func TestSolveEnvironmentCookbookVersionsMatchesUpstreamFirstGraph(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app2": "0.2.33",
			"app3": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.1.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.2.33", map[string]any{
		"dependencies": map[string]any{
			"app3": "0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.3.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.2.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.3.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@0.1.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app1"].Version != "0.1.0" {
		t.Fatalf("app1 version = %q, want %q", solution["app1"].Version, "0.1.0")
	}
	if solution["app2"].Version != "0.2.33" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "0.2.33")
	}
	if solution["app3"].Version != "0.3.0" {
		t.Fatalf("app3 version = %q, want %q", solution["app3"].Version, "0.3.0")
	}
}

func TestSolveEnvironmentCookbookVersionsMatchesUpstreamSecondGraph(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.1.0",
			"app4": "0.2.0",
			"app3": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app3": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.2.0", map[string]any{
		"dependencies": map[string]any{
			"app3": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.3.0", map[string]any{
		"dependencies": map[string]any{
			"app3": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app4": ">= 0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.2.0", map[string]any{
		"dependencies": map[string]any{
			"app4": "0.2.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.3.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "0.1.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "0.2.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.2.0",
			"app3": "0.3.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "0.3.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app1"].Version != "0.1.0" {
		t.Fatalf("app1 version = %q, want %q", solution["app1"].Version, "0.1.0")
	}
	if solution["app2"].Version != "0.3.0" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "0.3.0")
	}
	if solution["app3"].Version != "0.3.0" {
		t.Fatalf("app3 version = %q, want %q", solution["app3"].Version, "0.3.0")
	}
	if solution["app4"].Version != "0.2.0" {
		t.Fatalf("app4 version = %q, want %q", solution["app4"].Version, "0.2.0")
	}
}

func TestSolveEnvironmentCookbookVersionsCombinesEnvironmentAndDependencyConstraints(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":        "production",
			"json_class":  "Chef::Environment",
			"chef_type":   "environment",
			"description": "",
			"cookbook_versions": map[string]any{
				"app3": "<= 0.1.5",
			},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	createTestCookbookVersion(t, service, "ponyville", "app1", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.1.0",
			"app3": ">= 0.1.1",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "3.0.0", map[string]any{
		"dependencies": map[string]any{
			"app4": ">= 5.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app5": ">= 2.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "0.1.3", map[string]any{
		"dependencies": map[string]any{
			"app5": ">= 2.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app3", "2.0.0", map[string]any{
		"dependencies": map[string]any{
			"app5": ">= 2.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app4", "6.0.0", map[string]any{
		"dependencies": map[string]any{
			"app5": ">= 0.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app5", "6.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@3.0.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 5 {
		t.Fatalf("len(solution) = %d, want 5 (%v)", len(solution), solution)
	}
	if solution["app1"].Version != "3.0.0" {
		t.Fatalf("app1 version = %q, want %q", solution["app1"].Version, "3.0.0")
	}
	if solution["app2"].Version != "3.0.0" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "3.0.0")
	}
	if solution["app3"].Version != "0.1.3" {
		t.Fatalf("app3 version = %q, want %q", solution["app3"].Version, "0.1.3")
	}
	if solution["app4"].Version != "6.0.0" {
		t.Fatalf("app4 version = %q, want %q", solution["app4"].Version, "6.0.0")
	}
	if solution["app5"].Version != "6.0.0" {
		t.Fatalf("app5 version = %q, want %q", solution["app5"].Version, "6.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsAllowsCircularDependencies(t *testing.T) {
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

	createTestCookbookVersion(t, service, "ponyville", "app1", "0.1.0", map[string]any{
		"dependencies": map[string]any{
			"app2": ">= 0.0.0",
		},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app2", "0.0.1", map[string]any{
		"dependencies": map[string]any{
			"app1": ">= 0.0.0",
		},
	}, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"app1@0.1.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if solution["app1"].Version != "0.1.0" {
		t.Fatalf("app1 version = %q, want %q", solution["app1"].Version, "0.1.0")
	}
	if solution["app2"].Version != "0.0.1" {
		t.Fatalf("app2 version = %q, want %q", solution["app2"].Version, "0.0.1")
	}
}

func TestSolveEnvironmentCookbookVersionsReportsImpossibleDependencyViaEnvironmentConstraint(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":        "production",
			"json_class":  "Chef::Environment",
			"chef_type":   "environment",
			"description": "",
			"cookbook_versions": map[string]any{
				"bar": "= 1.0.0",
			},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.2.3", map[string]any{
		"dependencies": map[string]any{"bar": "> 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "3.0.0", nil, nil)

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
	if got := depsolverErr.Detail["message"]; got != "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]" {
		t.Fatalf("message = %v, want impossible-via-environment message", got)
	}
	if got := depsolverErr.Detail["unsatisfiable_run_list_item"]; got != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", got, "(foo >= 0.0.0)")
	}
	if got := depsolverErr.Detail["most_constrained_cookbooks"]; len(got.([]string)) != 1 || got.([]string)[0] != "bar = 1.0.0 -> []" {
		t.Fatalf("most_constrained_cookbooks = %v, want [bar = 1.0.0 -> []]", got)
	}
}

func TestSolveEnvironmentCookbookVersionsSelectsRootVersionThatRespectsEnvironmentConstraints(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":        "production",
			"json_class":  "Chef::Environment",
			"chef_type":   "environment",
			"description": "",
			"cookbook_versions": map[string]any{
				"bar": "= 1.0.0",
			},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.2.3", map[string]any{
		"dependencies": map[string]any{"bar": "> 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "foo", "1.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 1.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "3.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "1.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "1.0.0")
	}
	if solution["bar"].Version != "1.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "1.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsSelectsPinnedVersionForRepeatedRootCookbook(t *testing.T) {
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
	createTestCookbookVersion(t, service, "ponyville", "foo", "2.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "= 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "2.0.0", nil, nil)

	solution, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	})
	if err != nil {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %v", err)
	}

	if len(solution) != 2 {
		t.Fatalf("len(solution) = %d, want 2 (%v)", len(solution), solution)
	}
	if solution["foo"].Version != "2.0.0" {
		t.Fatalf("foo version = %q, want %q", solution["foo"].Version, "2.0.0")
	}
	if solution["bar"].Version != "2.0.0" {
		t.Fatalf("bar version = %q, want %q", solution["bar"].Version, "2.0.0")
	}
}

func TestSolveEnvironmentCookbookVersionsKeepsFirstRootLabelForRepeatedCookbook(t *testing.T) {
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
	createTestCookbookVersion(t, service, "ponyville", "foo", "2.0.0", map[string]any{
		"dependencies": map[string]any{"bar": "> 2.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "bar", "2.0.0", nil, nil)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "production", map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want depsolver error")
	}

	var depsolverErr *DepsolverError
	if !errors.As(err, &depsolverErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *DepsolverError", err)
	}
	if got := depsolverErr.Detail["message"]; got != "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 2.0.0) -> (bar > 2.0.0)]" {
		t.Fatalf("message = %v, want first-root-label message", got)
	}
	if got := depsolverErr.Detail["unsatisfiable_run_list_item"]; got != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", got, "(foo >= 0.0.0)")
	}
	if got := depsolverErr.Detail["most_constrained_cookbooks"]; len(got.([]string)) != 1 || got.([]string)[0] != "bar = 2.0.0 -> []" {
		t.Fatalf("most_constrained_cookbooks = %v, want [bar = 2.0.0 -> []]", got)
	}
}

func TestSolveEnvironmentCookbookVersionsPrefersMissingRootCookbooksOverNoVersionRoots(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookVersion(t, service, "ponyville", "foo", "1.2.3", nil, nil)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"this_does_not_exist", "foo@2.0.0"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want depsolver error")
	}

	var depsolverErr *DepsolverError
	if !errors.As(err, &depsolverErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *DepsolverError", err)
	}
	if got := depsolverErr.Detail["message"]; got != "Run list contains invalid items: no such cookbook this_does_not_exist." {
		t.Fatalf("message = %v, want missing-root precedence message", got)
	}
	if got := depsolverErr.Detail["non_existent_cookbooks"]; len(got.([]string)) != 1 || got.([]string)[0] != "this_does_not_exist" {
		t.Fatalf("non_existent_cookbooks = %v, want [this_does_not_exist]", got)
	}
	if got := depsolverErr.Detail["cookbooks_with_no_versions"]; len(got.([]string)) != 0 {
		t.Fatalf("cookbooks_with_no_versions = %v, want []", got)
	}
}

func TestSolveEnvironmentCookbookVersionsReportsPluralMissingRootCookbooks(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	_, _, _, err := service.SolveEnvironmentCookbookVersions("ponyville", "_default", map[string]any{
		"run_list": []any{"z_missing", "a_missing"},
	})
	if err == nil {
		t.Fatal("SolveEnvironmentCookbookVersions() error = nil, want depsolver error")
	}

	var depsolverErr *DepsolverError
	if !errors.As(err, &depsolverErr) {
		t.Fatalf("SolveEnvironmentCookbookVersions() error = %T, want *DepsolverError", err)
	}
	if got := depsolverErr.Detail["message"]; got != "Run list contains invalid items: no such cookbooks a_missing, z_missing." {
		t.Fatalf("message = %v, want plural missing-root message", got)
	}
	if got := depsolverErr.Detail["non_existent_cookbooks"]; len(got.([]string)) != 2 || got.([]string)[0] != "a_missing" || got.([]string)[1] != "z_missing" {
		t.Fatalf("non_existent_cookbooks = %v, want sorted missing roots", got)
	}
}
