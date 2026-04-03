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

func TestGetCookbookVersionChoosesLowestIdentifierForDuplicateVersion(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookArtifact(t, service, "ponyville", "app", "2222222222222222222222222222222222222222", "1.2.3", map[string]any{
		"description": "higher identifier",
	})
	createTestCookbookArtifact(t, service, "ponyville", "app", "1111111111111111111111111111111111111111", "1.2.3", map[string]any{
		"description": "lower identifier",
	})

	version, ok, found := service.GetCookbookVersion("ponyville", "app", "1.2.3")
	if !ok || !found {
		t.Fatalf("GetCookbookVersion() = ok:%v found:%v, want true/true", ok, found)
	}

	if got := version.Metadata["description"]; got != "lower identifier" {
		t.Fatalf("metadata.description = %v, want %q", got, "lower identifier")
	}
}

func TestCookbookUniverseChoosesLowestIdentifierForDuplicateVersion(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookArtifact(t, service, "ponyville", "app", "2222222222222222222222222222222222222222", "1.2.3", map[string]any{
		"dependencies": map[string]any{"apt": ">= 1.0.0"},
	})
	createTestCookbookArtifact(t, service, "ponyville", "app", "1111111111111111111111111111111111111111", "1.2.3", map[string]any{
		"dependencies": map[string]any{"apt": ">= 2.0.0"},
	})

	universe, ok := service.CookbookUniverse("ponyville")
	if !ok {
		t.Fatal("CookbookUniverse() = ok:false, want true")
	}

	entries := universe["app"]
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1 (%v)", len(entries), entries)
	}
	if got := entries[0].Dependencies["apt"]; got != ">= 2.0.0" {
		t.Fatalf("dependencies.apt = %q, want %q", got, ">= 2.0.0")
	}
}

func TestNormalizeCookbookArtifactPayloadAcceptsRootLevelAllFiles(t *testing.T) {
	artifact, err := normalizeCookbookArtifactPayload("app", "1111111111111111111111111111111111111111", map[string]any{
		"name":       "app",
		"identifier": "1111111111111111111111111111111111111111",
		"version":    "1.2.3",
		"chef_type":  "cookbook_version",
		"metadata": map[string]any{
			"version":      "1.2.3",
			"name":         "app",
			"dependencies": map[string]any{},
			"recipes":      map[string]any{},
		},
		"all_files": []any{
			map[string]any{
				"name":        "metadata.rb",
				"path":        "metadata.rb",
				"checksum":    "8288b67da0793b5abec709d6226e6b73",
				"specificity": "default",
			},
		},
	}, func(string) (bool, error) {
		return true, nil
	})
	if err != nil {
		t.Fatalf("normalizeCookbookArtifactPayload() error = %v", err)
	}

	if len(artifact.AllFiles) != 1 {
		t.Fatalf("all_files len = %d, want 1 (%v)", len(artifact.AllFiles), artifact.AllFiles)
	}
	if artifact.AllFiles[0].Path != "metadata.rb" {
		t.Fatalf("all_files[0].path = %q, want %q", artifact.AllFiles[0].Path, "metadata.rb")
	}
	if artifact.AllFiles[0].Name != "metadata.rb" {
		t.Fatalf("all_files[0].name = %q, want %q", artifact.AllFiles[0].Name, "metadata.rb")
	}
}

func createTestCookbookOrg(t *testing.T, service *Service) {
	t.Helper()

	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
}

func createTestCookbookArtifact(t *testing.T, service *Service, org, name, identifier, version string, metadataOverrides map[string]any) {
	t.Helper()

	metadata := map[string]any{
		"version":      version,
		"name":         name,
		"dependencies": map[string]any{},
		"recipes":      map[string]any{},
	}
	for key, value := range metadataOverrides {
		metadata[key] = value
	}

	if _, err := service.CreateCookbookArtifact(org, CreateCookbookArtifactInput{
		Name:       name,
		Identifier: identifier,
		Payload: map[string]any{
			"name":       name,
			"identifier": identifier,
			"version":    version,
			"chef_type":  "cookbook_version",
			"metadata":   metadata,
		},
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	}); err != nil {
		t.Fatalf("CreateCookbookArtifact(%s, %s, %s) error = %v", name, identifier, version, err)
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
