package pg

import (
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestCookbookStoreRequiresRegisteredOrganization(t *testing.T) {
	store := New("postgres://example")
	backend := store.CookbookStore()

	_, _, _, err := backend.UpsertCookbookVersionWithReleasedChecksums("ponyville", bootstrap.CookbookVersion{
		Name:         "demo-1.2.3",
		CookbookName: "demo",
		Version:      "1.2.3",
		JSONClass:    "Chef::CookbookVersion",
		ChefType:     "cookbook_version",
		Metadata: map[string]any{
			"name":    "demo",
			"version": "1.2.3",
		},
	}, false)
	if !errors.Is(err, bootstrap.ErrNotFound) {
		t.Fatalf("UpsertCookbookVersionWithReleasedChecksums() error = %v, want ErrNotFound", err)
	}
}

func TestCookbookStoreRoundTripsCookbooksAndArtifacts(t *testing.T) {
	store := New("postgres://example")
	backend := store.CookbookStore()
	backend.(interface{ EnsureOrganization(string) }).EnsureOrganization("ponyville")
	orgs := store.Cookbooks().OrganizationRecords()
	if len(orgs) != 1 || orgs[0].Name != "ponyville" {
		t.Fatalf("OrganizationRecords() = %v, want ponyville registration", orgs)
	}

	version := bootstrap.CookbookVersion{
		Name:         "demo-1.2.3",
		CookbookName: "demo",
		Version:      "1.2.3",
		JSONClass:    "Chef::CookbookVersion",
		ChefType:     "cookbook_version",
		Metadata: map[string]any{
			"name":         "demo",
			"version":      "1.2.3",
			"dependencies": map[string]any{"apt": ">= 1.0.0"},
		},
		AllFiles: []bootstrap.CookbookFile{
			{Name: "default.rb", Path: "recipes/default.rb", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Specificity: "default"},
		},
	}

	gotVersion, released, created, err := backend.UpsertCookbookVersionWithReleasedChecksums("ponyville", version, false)
	if err != nil {
		t.Fatalf("UpsertCookbookVersionWithReleasedChecksums() error = %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if len(released) != 0 {
		t.Fatalf("released = %v, want nil", released)
	}
	if !reflect.DeepEqual(gotVersion, version) {
		t.Fatalf("version = %#v, want %#v", gotVersion, version)
	}

	if refs, ok := store.Cookbooks().listCookbookVersions("ponyville")["demo"]; !ok || len(refs) != 1 || refs[0].Version != "1.2.3" {
		t.Fatalf("listCookbookVersions() = %v, want persisted demo version", store.Cookbooks().listCookbookVersions("ponyville"))
	}

	artifact := bootstrap.CookbookArtifact{
		Name:       "demo",
		Identifier: "1111111111111111111111111111111111111111",
		Version:    "1.2.3",
		ChefType:   "cookbook_version",
		Metadata: map[string]any{
			"name":    "demo",
			"version": "1.2.3",
		},
		AllFiles: []bootstrap.CookbookFile{
			{Name: "default.rb", Path: "recipes/default.rb", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Specificity: "default"},
		},
	}

	gotArtifact, err := backend.CreateCookbookArtifact("ponyville", artifact)
	if err != nil {
		t.Fatalf("CreateCookbookArtifact() error = %v", err)
	}
	if !reflect.DeepEqual(gotArtifact, artifact) {
		t.Fatalf("artifact = %#v, want %#v", gotArtifact, artifact)
	}

	persistedArtifact, orgOK, found := backend.GetCookbookArtifact("ponyville", "demo", artifact.Identifier)
	if !orgOK || !found {
		t.Fatalf("GetCookbookArtifact() found = %v/%v, want true/true", orgOK, found)
	}
	if !reflect.DeepEqual(persistedArtifact, artifact) {
		t.Fatalf("persisted artifact = %#v, want %#v", persistedArtifact, artifact)
	}

	deletedVersion, releasedChecksums, err := backend.DeleteCookbookVersionWithReleasedChecksums("ponyville", "demo", "1.2.3")
	if err != nil {
		t.Fatalf("DeleteCookbookVersionWithReleasedChecksums() error = %v", err)
	}
	if !reflect.DeepEqual(deletedVersion, version) {
		t.Fatalf("deleted version = %#v, want %#v", deletedVersion, version)
	}
	if len(releasedChecksums) != 1 || releasedChecksums[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("releasedChecksums = %v, want version checksum", releasedChecksums)
	}
}
