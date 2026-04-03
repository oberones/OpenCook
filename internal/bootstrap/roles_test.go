package bootstrap

import (
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateRoleAcceptsAnyEnvRunLists(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

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

func TestUpdateRoleUsesURLNameWhenPayloadOmitsName(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

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
