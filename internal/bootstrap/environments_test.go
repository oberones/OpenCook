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

func TestEnvironmentCookbookVersionsApplyConstraintsAndLimits(t *testing.T) {
	cookbooks := map[string]map[string]CookbookVersion{
		"cb_one": {
			"1.0.0": {CookbookName: "cb_one", Version: "1.0.0"},
			"2.0.0": {CookbookName: "cb_one", Version: "2.0.0"},
			"3.0.0": {CookbookName: "cb_one", Version: "3.0.0"},
		},
		"cb_two": {
			"1.0.0": {CookbookName: "cb_two", Version: "1.0.0"},
			"1.2.0": {CookbookName: "cb_two", Version: "1.2.0"},
			"1.2.5": {CookbookName: "cb_two", Version: "1.2.5"},
		},
		"cb_three": {
			"0.5.1": {CookbookName: "cb_three", Version: "0.5.1"},
			"0.6.0": {CookbookName: "cb_three", Version: "0.6.0"},
			"1.0.0": {CookbookName: "cb_three", Version: "1.0.0"},
		},
	}

	env := Environment{
		Name: "production",
		CookbookVersions: map[string]string{
			"cb_one":   "> 1.0.0",
			"cb_three": "~> 0.5",
		},
	}

	filtered := environmentCookbookVersions(cookbooks, env, 2, false)

	assertCookbookVersions(t, filtered["cb_one"], "3.0.0", "2.0.0")
	assertCookbookVersions(t, filtered["cb_two"], "1.2.5", "1.2.0")
	assertCookbookVersions(t, filtered["cb_three"], "0.6.0", "0.5.1")
}

func TestEnvironmentCookbookVersionsReturnsEmptyListsWhenEverythingFiltered(t *testing.T) {
	cookbooks := map[string]map[string]CookbookVersion{
		"cb_one": {
			"1.0.0": {CookbookName: "cb_one", Version: "1.0.0"},
		},
		"cb_two": {
			"1.2.0": {CookbookName: "cb_two", Version: "1.2.0"},
		},
	}

	env := Environment{
		Name: "production",
		CookbookVersions: map[string]string{
			"cb_one": "= 6.6.6",
			"cb_two": "= 6.6.6",
		},
	}

	filtered := environmentCookbookVersions(cookbooks, env, 1, false)
	if len(filtered) != 2 {
		t.Fatalf("len(filtered) = %d, want 2 (%v)", len(filtered), filtered)
	}
	assertCookbookVersions(t, filtered["cb_one"])
	assertCookbookVersions(t, filtered["cb_two"])
}

func assertCookbookVersions(t *testing.T, refs []CookbookVersionRef, want ...string) {
	t.Helper()

	if len(refs) != len(want) {
		t.Fatalf("len(refs) = %d, want %d (%v)", len(refs), len(want), refs)
	}
	for idx, version := range want {
		if refs[idx].Version != version {
			t.Fatalf("refs[%d].Version = %q, want %q (%v)", idx, refs[idx].Version, version, refs)
		}
	}
}
