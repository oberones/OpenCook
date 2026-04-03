package bootstrap

import (
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateEnvironmentAcceptsStringCookbookVersions(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	env, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":                "production",
			"json_class":          "Chef::Environment",
			"chef_type":           "environment",
			"description":         "",
			"cookbook_versions":   map[string]string{"apache": ">= 1.2.3"},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	if env.CookbookVersions["apache"] != ">= 1.2.3" {
		t.Fatalf("cookbook_versions[apache] = %q, want %q", env.CookbookVersions["apache"], ">= 1.2.3")
	}
}

func TestCreateEnvironmentAcceptsAnyCookbookVersions(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	env, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name":                "staging",
			"json_class":          "Chef::Environment",
			"chef_type":           "environment",
			"description":         "",
			"cookbook_versions":   map[string]any{"apache": ">= 1.2.3"},
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}

	if env.CookbookVersions["apache"] != ">= 1.2.3" {
		t.Fatalf("cookbook_versions[apache] = %q, want %q", env.CookbookVersions["apache"], ">= 1.2.3")
	}
}
