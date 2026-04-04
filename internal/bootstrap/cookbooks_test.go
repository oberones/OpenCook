package bootstrap

import (
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateCookbookArtifactRejectsMissingUploadedChecksum(t *testing.T) {
	service := newTestBootstrapService(t)
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	_, err := service.CreateCookbookArtifact("ponyville", CreateCookbookArtifactInput{
		Name:       "app",
		Identifier: "1111111111111111111111111111111111111111",
		Payload: map[string]any{
			"name":       "app",
			"identifier": "1111111111111111111111111111111111111111",
			"version":    "1.2.3",
			"chef_type":  "cookbook_version",
			"metadata": map[string]any{
				"version":      "1.2.3",
				"name":         "app",
				"dependencies": map[string]any{},
				"recipes":      map[string]any{"app::default": ""},
			},
			"recipes": []any{
				map[string]any{
					"name":        "default.rb",
					"path":        "recipes/default.rb",
					"checksum":    "8288b67da0793b5abec709d6226e6b73",
					"specificity": "default",
				},
			},
		},
		ChecksumExists: func(string) (bool, error) {
			return false, nil
		},
	})
	if err == nil {
		t.Fatal("CreateCookbookArtifact() error = nil, want validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("CreateCookbookArtifact() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Manifest has a checksum that hasn't been uploaded." {
		t.Fatalf("validation messages = %v, want missing checksum message", validationErr.Messages)
	}
}

func TestListCookbookVersionsOrdersLatestFirst(t *testing.T) {
	service := newTestBootstrapService(t)
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	for identifier, version := range map[string]string{
		"1111111111111111111111111111111111111111": "1.0.0",
		"2222222222222222222222222222222222222222": "1.2.0",
		"3333333333333333333333333333333333333333": "1.2.0.beta.1",
	} {
		if _, err := service.CreateCookbookArtifact("ponyville", CreateCookbookArtifactInput{
			Name:       "app",
			Identifier: identifier,
			Payload: map[string]any{
				"name":       "app",
				"identifier": identifier,
				"version":    version,
				"chef_type":  "cookbook_version",
				"metadata": map[string]any{
					"version":      version,
					"name":         "app",
					"dependencies": map[string]any{},
					"recipes":      map[string]any{},
				},
			},
			ChecksumExists: func(string) (bool, error) {
				return true, nil
			},
		}); err != nil {
			t.Fatalf("CreateCookbookArtifact(%s) error = %v", version, err)
		}
	}

	versions, ok, found := service.ListCookbookVersionsByName("ponyville", "app")
	if !ok || !found {
		t.Fatalf("ListCookbookVersionsByName() = ok:%v found:%v, want true/true", ok, found)
	}

	got := make([]string, 0, len(versions))
	for _, version := range versions {
		got = append(got, version.Version)
	}
	want := []string{"1.2.0", "1.2.0.beta.1", "1.0.0"}
	if len(got) != len(want) {
		t.Fatalf("versions len = %d, want %d (%v)", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("versions[%d] = %q, want %q (%v)", idx, got[idx], want[idx], got)
		}
	}
}

func newTestBootstrapService(t *testing.T) *Service {
	t.Helper()

	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	service.SeedPrincipal(authn.Principal{Type: "user", Name: "pivotal"})
	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", mustGeneratePublicKeyPEM(t)); err != nil {
		t.Fatalf("SeedPublicKey(pivotal) error = %v", err)
	}
	return service
}
