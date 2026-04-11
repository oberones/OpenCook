package bootstrap

import (
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateRoleAcceptsAnyEnvRunLists(t *testing.T) {
	service := newRoleTestService(t)
	role, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "Web tier",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[base]"},
			"env_run_lists": map[string]any{
				"production": []any{"recipe[nginx]"},
			},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	if len(role.RunList) != 1 || role.RunList[0] != "role[base]" {
		t.Fatalf("RunList = %v, want [role[base]]", role.RunList)
	}
	if len(role.EnvRunLists["production"]) != 1 || role.EnvRunLists["production"][0] != "recipe[nginx]" {
		t.Fatalf("EnvRunLists[production] = %v, want [recipe[nginx]]", role.EnvRunLists["production"])
	}
}

func TestCreateRoleNormalizesRunLists(t *testing.T) {
	service := newRoleTestService(t)

	role, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "Web tier",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"base", "recipe[base]", "foo::default", "recipe[foo::default]", "role[db]", "role[db]"},
			"env_run_lists": map[string]any{
				"production": []any{"nginx", "recipe[nginx]", "role[app]", "role[app]"},
				"staging":    []any{},
			},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	assertStringSliceEqual(t, role.RunList, []string{"recipe[base]", "recipe[foo::default]", "role[db]"})
	assertStringSliceEqual(t, role.EnvRunLists["production"], []string{"recipe[nginx]", "role[app]"})
	assertStringSliceEqual(t, role.EnvRunLists["staging"], []string{})
}

func TestUpdateRoleUsesURLNameWhenPayloadOmitsName(t *testing.T) {
	service := newRoleTestService(t)

	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "Web tier",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []string{},
			"env_run_lists":       map[string]any{},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}

	role, err := service.UpdateRole("ponyville", "web", UpdateRoleInput{
		Payload: map[string]any{
			"description": "Updated role",
			"json_class":  "Chef::Role",
			"chef_type":   "role",
		},
	})
	if err != nil {
		t.Fatalf("UpdateRole() error = %v", err)
	}

	if role.Name != "web" {
		t.Fatalf("Name = %q, want %q", role.Name, "web")
	}
	if role.Description != "Updated role" {
		t.Fatalf("Description = %q, want %q", role.Description, "Updated role")
	}
}

func TestUpdateRoleNormalizesStoredRunLists(t *testing.T) {
	service := newRoleTestService(t)

	service.mu.Lock()
	service.orgs["ponyville"].roles["web"] = Role{
		Name:               "web",
		Description:        "Web tier",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
		RunList:            []string{"base", "recipe[base]", "role[db]", "role[db]"},
		EnvRunLists: map[string][]string{
			"production": []string{"nginx", "recipe[nginx]"},
		},
	}
	service.mu.Unlock()

	role, err := service.UpdateRole("ponyville", "web", UpdateRoleInput{
		Payload: map[string]any{
			"description": "Normalized",
			"json_class":  "Chef::Role",
			"chef_type":   "role",
		},
	})
	if err != nil {
		t.Fatalf("UpdateRole() error = %v", err)
	}

	assertStringSliceEqual(t, role.RunList, []string{"recipe[base]", "role[db]"})
	assertStringSliceEqual(t, role.EnvRunLists["production"], []string{"recipe[nginx]"})
}

func newRoleTestService(t *testing.T) *Service {
	t.Helper()

	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	return service
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(%v) = %d, want %d", got, len(got), len(want))
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("got[%d] = %q, want %q (full slice %v)", idx, got[idx], want[idx], got)
		}
	}
}
