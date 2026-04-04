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

	var checksumErr *MissingChecksumError
	if !errors.As(err, &checksumErr) {
		t.Fatalf("CreateCookbookArtifact() error = %T, want *MissingChecksumError", err)
	}
	if checksumErr.Checksum != "8288b67da0793b5abec709d6226e6b73" {
		t.Fatalf("checksum = %q, want missing checksum", checksumErr.Checksum)
	}
	if checksumErr.Error() != "Manifest has checksum 8288b67da0793b5abec709d6226e6b73 but it hasn't yet been uploaded" {
		t.Fatalf("error message = %q, want checksum-specific message", checksumErr.Error())
	}
}

func TestListCookbookVersionsOrdersLatestFirst(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)
	createTestCookbookVersion(t, service, "ponyville", "app", "1.0.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.0", nil, nil)
	createTestCookbookVersion(t, service, "ponyville", "app", "2.0.0", nil, nil)

	versions, ok, found := service.ListCookbookVersionsByName("ponyville", "app")
	if !ok || !found {
		t.Fatalf("ListCookbookVersionsByName() = ok:%v found:%v, want true/true", ok, found)
	}

	got := make([]string, 0, len(versions))
	for _, version := range versions {
		got = append(got, version.Version)
	}
	want := []string{"2.0.0", "1.2.0", "1.0.0"}
	if len(got) != len(want) {
		t.Fatalf("versions len = %d, want %d (%v)", len(got), len(want), got)
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("versions[%d] = %q, want %q (%v)", idx, got[idx], want[idx], got)
		}
	}
}

func TestUpsertCookbookVersionUpdatesExistingVersion(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", map[string]any{
		"description": "first",
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", map[string]any{
		"description": "second",
	}, []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    "8288b67da0793b5abec709d6226e6b73",
			"specificity": "default",
		},
	})

	version, ok, found := service.GetCookbookVersion("ponyville", "app", "1.2.3")
	if !ok || !found {
		t.Fatalf("GetCookbookVersion() = ok:%v found:%v, want true/true", ok, found)
	}

	if got := version.Metadata["description"]; got != "second" {
		t.Fatalf("metadata.description = %v, want %q", got, "second")
	}
	if len(version.AllFiles) != 1 || version.AllFiles[0].Path != "recipes/default.rb" {
		t.Fatalf("all_files = %v, want updated recipe payload", version.AllFiles)
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

func TestCookbookUniverseUsesCookbookVersions(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookVersion(t, service, "ponyville", "app", "1.0.0", map[string]any{
		"dependencies": map[string]any{"apt": ">= 1.0.0"},
	}, nil)
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.0", map[string]any{
		"dependencies": map[string]any{"apt": ">= 2.0.0"},
	}, nil)

	universe, ok := service.CookbookUniverse("ponyville")
	if !ok {
		t.Fatal("CookbookUniverse() = ok:false, want true")
	}

	entries := universe["app"]
	if len(entries) != 2 {
		t.Fatalf("entries len = %d, want 2 (%v)", len(entries), entries)
	}
	if got := entries[0].Dependencies["apt"]; got != ">= 2.0.0" {
		t.Fatalf("dependencies.apt = %q, want %q", got, ">= 2.0.0")
	}
}

func TestUpsertCookbookVersionWithReleasedChecksumsTracksUnreferencedBlobs(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	shared := "11111111111111111111111111111111"
	oldOnly := "22222222222222222222222222222222"
	newOnly := "33333333333333333333333333333333"

	createTestCookbookVersion(t, service, "ponyville", "shared", "0.1.0", nil, []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    shared,
			"specificity": "default",
		},
	})
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", nil, []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    shared,
			"specificity": "default",
		},
		map[string]any{
			"name":        "files/default/config",
			"path":        "files/default/config",
			"checksum":    oldOnly,
			"specificity": "default",
		},
	})

	version, released, created, err := service.UpsertCookbookVersionWithReleasedChecksums("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: cookbookVersionTestPayload("app", "1.2.3", nil, []any{
			map[string]any{
				"name":        "recipes/default.rb",
				"path":        "recipes/default.rb",
				"checksum":    shared,
				"specificity": "default",
			},
			map[string]any{
				"name":        "files/default/updated",
				"path":        "files/default/updated",
				"checksum":    newOnly,
				"specificity": "default",
			},
		}),
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	})
	if err != nil {
		t.Fatalf("UpsertCookbookVersionWithReleasedChecksums() error = %v", err)
	}
	if created {
		t.Fatal("created = true, want false")
	}
	if version.Version != "1.2.3" {
		t.Fatalf("version.Version = %q, want %q", version.Version, "1.2.3")
	}
	if len(released) != 1 || released[0] != oldOnly {
		t.Fatalf("released = %v, want [%s]", released, oldOnly)
	}
}

func TestDeleteCookbookVersionWithReleasedChecksumsKeepsSandboxReferencedBlob(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	checksum := "11111111111111111111111111111111"
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", nil, []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    checksum,
			"specificity": "default",
		},
	})
	if _, err := service.CreateSandbox("ponyville", CreateSandboxInput{
		Checksums: []string{checksum},
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	_, released, err := service.DeleteCookbookVersionWithReleasedChecksums("ponyville", "app", "1.2.3")
	if err != nil {
		t.Fatalf("DeleteCookbookVersionWithReleasedChecksums() error = %v", err)
	}
	if len(released) != 0 {
		t.Fatalf("released = %v, want no cleanup while sandbox still references checksum", released)
	}
}

func TestDeleteCookbookArtifactWithReleasedChecksumsOnlyReleasesUnsharedChecksums(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	shared := "11111111111111111111111111111111"
	unique := "22222222222222222222222222222222"
	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", nil, []any{
		map[string]any{
			"name":        "recipes/default.rb",
			"path":        "recipes/default.rb",
			"checksum":    shared,
			"specificity": "default",
		},
	})
	artifactPayload := map[string]any{
		"name":       "artifact-app",
		"identifier": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"version":    "1.2.3",
		"chef_type":  "cookbook_version",
		"metadata": map[string]any{
			"version":      "1.2.3",
			"name":         "artifact-app",
			"dependencies": map[string]any{},
			"recipes":      map[string]any{},
		},
		"all_files": []any{
			map[string]any{
				"name":        "recipes/default.rb",
				"path":        "recipes/default.rb",
				"checksum":    shared,
				"specificity": "default",
			},
			map[string]any{
				"name":        "files/default/only",
				"path":        "files/default/only",
				"checksum":    unique,
				"specificity": "default",
			},
		},
	}
	if _, err := service.CreateCookbookArtifact("ponyville", CreateCookbookArtifactInput{
		Name:       "artifact-app",
		Identifier: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Payload:    artifactPayload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	}); err != nil {
		t.Fatalf("CreateCookbookArtifact() error = %v", err)
	}

	_, released, err := service.DeleteCookbookArtifactWithReleasedChecksums("ponyville", "artifact-app", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("DeleteCookbookArtifactWithReleasedChecksums() error = %v", err)
	}
	if len(released) != 1 || released[0] != unique {
		t.Fatalf("released = %v, want [%s]", released, unique)
	}
}

func TestUpsertCookbookVersionReturnsFieldSpecificValidationMessages(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	tests := []struct {
		name    string
		mutate  func(map[string]any)
		message string
	}{
		{
			name: "missing cookbook_name",
			mutate: func(payload map[string]any) {
				delete(payload, "cookbook_name")
			},
			message: "Field 'cookbook_name' missing",
		},
		{
			name: "invalid cookbook_name",
			mutate: func(payload map[string]any) {
				payload["cookbook_name"] = "new_name"
			},
			message: "Field 'cookbook_name' invalid",
		},
		{
			name: "invalid version",
			mutate: func(payload map[string]any) {
				payload["version"] = "1.2"
			},
			message: "Field 'version' invalid",
		},
		{
			name: "invalid metadata.version",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["version"] = "1.2"
			},
			message: "Field 'metadata.version' invalid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := cookbookVersionTestPayload("app", "1.2.3", nil, nil)
			tc.mutate(payload)

			_, _, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
				Name:    "app",
				Version: "1.2.3",
				Payload: payload,
				ChecksumExists: func(string) (bool, error) {
					return true, nil
				},
			})
			if err == nil {
				t.Fatal("UpsertCookbookVersion() error = nil, want validation error")
			}

			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("UpsertCookbookVersion() error = %T, want *ValidationError", err)
			}
			if len(validationErr.Messages) != 1 || validationErr.Messages[0] != tc.message {
				t.Fatalf("validation messages = %v, want %q", validationErr.Messages, tc.message)
			}
		})
	}
}

func TestUpsertCookbookVersionAllowsMetadataNameChange(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	payload := cookbookVersionTestPayload("app", "1.2.3", map[string]any{
		"name": "renamed-app",
	}, nil)
	version, created, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: payload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	})
	if err != nil {
		t.Fatalf("UpsertCookbookVersion() error = %v", err)
	}
	if !created {
		t.Fatal("UpsertCookbookVersion() created = false, want true")
	}
	if got := version.Metadata["name"]; got != "renamed-app" {
		t.Fatalf("metadata.name = %v, want %q", got, "renamed-app")
	}
}

func TestUpsertCookbookVersionRejectsFrozenUpdateWithoutForce(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	createTestCookbookVersion(t, service, "ponyville", "app", "1.2.3", map[string]any{
		"description": "first",
	}, nil)

	payload := cookbookVersionTestPayload("app", "1.2.3", map[string]any{
		"description": "frozen",
	}, nil)
	payload["frozen?"] = true
	if _, _, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: payload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	}); err != nil {
		t.Fatalf("freeze UpsertCookbookVersion() error = %v", err)
	}

	updatePayload := cookbookVersionTestPayload("app", "1.2.3", map[string]any{
		"description": "second",
	}, nil)
	updatePayload["frozen?"] = false
	_, _, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: updatePayload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	})
	if err == nil {
		t.Fatal("UpsertCookbookVersion() error = nil, want frozen conflict")
	}

	var frozenErr *FrozenCookbookError
	if !errors.As(err, &frozenErr) {
		t.Fatalf("UpsertCookbookVersion() error = %T, want *FrozenCookbookError", err)
	}
	if frozenErr.Error() != "The cookbook app at version 1.2.3 is frozen. Use the 'force' option to override." {
		t.Fatalf("error message = %q, want frozen conflict", frozenErr.Error())
	}

	version, ok, found := service.GetCookbookVersion("ponyville", "app", "1.2.3")
	if !ok || !found {
		t.Fatalf("GetCookbookVersion() = ok:%v found:%v, want true/true", ok, found)
	}
	if got := version.Metadata["description"]; got != "frozen" {
		t.Fatalf("metadata.description = %v, want frozen value", got)
	}
	if !version.Frozen {
		t.Fatal("version.Frozen = false, want true")
	}
}

func TestUpsertCookbookVersionForceKeepsFrozenState(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	payload := cookbookVersionTestPayload("app", "1.2.3", map[string]any{
		"description": "first",
	}, nil)
	payload["frozen?"] = true
	if _, _, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: payload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	}); err != nil {
		t.Fatalf("freeze UpsertCookbookVersion() error = %v", err)
	}

	updatePayload := cookbookVersionTestPayload("app", "1.2.3", map[string]any{
		"description": "second",
	}, nil)
	updatePayload["frozen?"] = false
	version, created, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
		Name:    "app",
		Version: "1.2.3",
		Payload: updatePayload,
		Force:   true,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	})
	if err != nil {
		t.Fatalf("forced UpsertCookbookVersion() error = %v", err)
	}
	if created {
		t.Fatal("UpsertCookbookVersion() created = true, want false")
	}
	if !version.Frozen {
		t.Fatal("version.Frozen = false, want true")
	}
	if got := version.Metadata["description"]; got != "second" {
		t.Fatalf("metadata.description = %v, want updated value", got)
	}
}

func TestUpsertCookbookVersionValidatesPedantMetadataShapes(t *testing.T) {
	service := newTestBootstrapService(t)
	createTestCookbookOrg(t, service)

	tests := []struct {
		name    string
		mutate  func(map[string]any)
		message string
	}{
		{
			name: "description type",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["description"] = 1
			},
			message: "Field 'metadata.description' invalid",
		},
		{
			name: "long_description type",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["long_description"] = false
			},
			message: "Field 'metadata.long_description' invalid",
		},
		{
			name: "dependencies section type",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["dependencies"] = []any{"foo"}
			},
			message: "Field 'metadata.dependencies' invalid",
		},
		{
			name: "dependencies constraint value",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["dependencies"] = map[string]any{"apt": "s395dss@#"}
			},
			message: "Invalid value 's395dss@#' for metadata.dependencies",
		},
		{
			name: "platforms invalid nested value",
			mutate: func(payload map[string]any) {
				payload["metadata"].(map[string]any)["platforms"] = map[string]any{"ubuntu": map[string]any{}}
			},
			message: "Invalid value '{[]}' for metadata.platforms",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := cookbookVersionTestPayload("app", "1.2.3", nil, nil)
			tc.mutate(payload)

			_, _, err := service.UpsertCookbookVersion("ponyville", UpsertCookbookVersionInput{
				Name:    "app",
				Version: "1.2.3",
				Payload: payload,
				ChecksumExists: func(string) (bool, error) {
					return true, nil
				},
			})
			if err == nil {
				t.Fatal("UpsertCookbookVersion() error = nil, want validation error")
			}

			var validationErr *ValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("UpsertCookbookVersion() error = %T, want *ValidationError", err)
			}
			if len(validationErr.Messages) != 1 || validationErr.Messages[0] != tc.message {
				t.Fatalf("validation messages = %v, want %q", validationErr.Messages, tc.message)
			}
		})
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

func createTestCookbookVersion(t *testing.T, service *Service, org, name, version string, metadataOverrides map[string]any, allFiles []any) {
	t.Helper()
	payload := cookbookVersionTestPayload(name, version, metadataOverrides, allFiles)
	if _, _, err := service.UpsertCookbookVersion(org, UpsertCookbookVersionInput{
		Name:    name,
		Version: version,
		Payload: payload,
		ChecksumExists: func(string) (bool, error) {
			return true, nil
		},
	}); err != nil {
		t.Fatalf("UpsertCookbookVersion(%s, %s) error = %v", name, version, err)
	}
}

func cookbookVersionTestPayload(name, version string, metadataOverrides map[string]any, allFiles []any) map[string]any {
	metadata := map[string]any{
		"version":      version,
		"name":         name,
		"dependencies": map[string]any{},
		"recipes":      map[string]any{},
	}
	for key, value := range metadataOverrides {
		metadata[key] = value
	}

	payload := map[string]any{
		"name":          name + "-" + version,
		"cookbook_name": name,
		"version":       version,
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"metadata":      metadata,
	}
	if allFiles != nil {
		payload["all_files"] = allFiles
	}
	return payload
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
