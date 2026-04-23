package pg

import (
	"reflect"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestCookbookRepositoryExposesCookbookPersistenceMigration(t *testing.T) {
	repo := New("postgres://example").Cookbooks()

	migrations := repo.Migrations()
	if len(migrations) != 1 {
		t.Fatalf("len(Migrations()) = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "0001_cookbook_persistence.sql" {
		t.Fatalf("Migrations()[0].Name = %q, want cookbook persistence migration", migrations[0].Name)
	}

	sql := migrations[0].SQL
	for _, table := range []string{
		"oc_cookbook_orgs",
		"oc_cookbook_versions",
		"oc_cookbook_version_files",
		"oc_cookbook_artifacts",
		"oc_cookbook_artifact_files",
	} {
		if !strings.Contains(sql, table) {
			t.Fatalf("migration SQL missing %q table", table)
		}
	}
}

func TestCookbookRepositoryRoundTripsCookbookVersionBundle(t *testing.T) {
	repo := New("postgres://example").Cookbooks()
	version := bootstrap.CookbookVersion{
		Name:         "apache2-1.2.3",
		CookbookName: "apache2",
		Version:      "1.2.3",
		JSONClass:    "Chef::CookbookVersion",
		ChefType:     "cookbook_version",
		Frozen:       true,
		Metadata: map[string]any{
			"name":    "apache2",
			"version": "1.2.3",
			"dependencies": map[string]any{
				"apt": ">= 0.0.0",
			},
			"recipes": map[string]any{
				"apache2::default": "Base recipe",
			},
			"providing": map[string]any{
				"httpd": "Apache HTTPD",
			},
		},
		AllFiles: []bootstrap.CookbookFile{
			{Name: "default.rb", Path: "recipes/default.rb", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Specificity: "default"},
			{Name: "attrs.rb", Path: "attributes/default.rb", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Specificity: "default"},
		},
	}

	bundle, err := repo.EncodeCookbookVersion("ponyville", version)
	if err != nil {
		t.Fatalf("EncodeCookbookVersion() error = %v", err)
	}

	if bundle.Version.Organization != "ponyville" {
		t.Fatalf("bundle.Version.Organization = %q, want %q", bundle.Version.Organization, "ponyville")
	}
	if got := len(bundle.Files); got != 2 {
		t.Fatalf("len(bundle.Files) = %d, want 2", got)
	}

	// Decode from out-of-order rows to prove ordinal ordering is part of the scaffolded contract.
	bundle.Files[0], bundle.Files[1] = bundle.Files[1], bundle.Files[0]

	decoded, err := repo.DecodeCookbookVersion(bundle)
	if err != nil {
		t.Fatalf("DecodeCookbookVersion() error = %v", err)
	}

	if !reflect.DeepEqual(decoded, version) {
		t.Fatalf("DecodeCookbookVersion() = %#v, want %#v", decoded, version)
	}
}

func TestCookbookRepositoryRoundTripsCookbookArtifactBundle(t *testing.T) {
	repo := New("postgres://example").Cookbooks()
	artifact := bootstrap.CookbookArtifact{
		Name:       "apache2",
		Identifier: "1234567890abcdef1234567890abcdef12345678",
		Version:    "2.3.4",
		ChefType:   "cookbook_version",
		Frozen:     false,
		Metadata: map[string]any{
			"name":    "apache2",
			"version": "2.3.4",
			"platforms": map[string]any{
				"ubuntu": ">= 20.04",
			},
		},
		AllFiles: []bootstrap.CookbookFile{
			{Name: "default.rb", Path: "recipes/default.rb", Checksum: "cccccccccccccccccccccccccccccccccccccccc", Specificity: "default"},
			{Name: "vhost.conf.erb", Path: "templates/default/vhost.conf.erb", Checksum: "dddddddddddddddddddddddddddddddddddddddd", Specificity: "default"},
		},
	}

	bundle, err := repo.EncodeCookbookArtifact("ponyville", artifact)
	if err != nil {
		t.Fatalf("EncodeCookbookArtifact() error = %v", err)
	}

	bundle.Files[0], bundle.Files[1] = bundle.Files[1], bundle.Files[0]

	decoded, err := repo.DecodeCookbookArtifact(bundle)
	if err != nil {
		t.Fatalf("DecodeCookbookArtifact() error = %v", err)
	}

	if !reflect.DeepEqual(decoded, artifact) {
		t.Fatalf("DecodeCookbookArtifact() = %#v, want %#v", decoded, artifact)
	}
}

func TestCookbookRepositoryRejectsMismatchedVersionFileParent(t *testing.T) {
	repo := New("postgres://example").Cookbooks()
	_, err := repo.DecodeCookbookVersion(CookbookVersionBundle{
		Version: CookbookVersionRecord{
			Organization: "ponyville",
			CookbookName: "apache2",
			Version:      "1.2.3",
			FullName:     "apache2-1.2.3",
			JSONClass:    "Chef::CookbookVersion",
			ChefType:     "cookbook_version",
			MetadataJSON: []byte(`{"name":"apache2","version":"1.2.3"}`),
		},
		Files: []CookbookVersionFileRecord{
			{
				Organization: "canterlot",
				CookbookName: "apache2",
				Version:      "1.2.3",
				Ordinal:      0,
				Name:         "default.rb",
				Path:         "recipes/default.rb",
				Checksum:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Specificity:  "default",
			},
		},
	})
	if err == nil {
		t.Fatal("DecodeCookbookVersion() error = nil, want parent mismatch")
	}
}
