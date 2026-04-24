package pg

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestBootstrapCoreRepositoryExposesBootstrapCoreMigration(t *testing.T) {
	repo := New("postgres://example").BootstrapCore()

	migrations := repo.Migrations()
	if len(migrations) != 1 {
		t.Fatalf("len(Migrations()) = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "0002_bootstrap_core_persistence.sql" {
		t.Fatalf("Migrations()[0].Name = %q, want bootstrap core persistence migration", migrations[0].Name)
	}

	sql := migrations[0].SQL
	for _, table := range []string{
		"oc_bootstrap_users",
		"oc_bootstrap_user_acls",
		"oc_bootstrap_user_keys",
		"oc_bootstrap_orgs",
		"oc_bootstrap_clients",
		"oc_bootstrap_client_keys",
		"oc_bootstrap_groups",
		"oc_bootstrap_group_memberships",
		"oc_bootstrap_containers",
		"oc_bootstrap_org_acls",
	} {
		if !strings.Contains(sql, table) {
			t.Fatalf("migration SQL missing %q table", table)
		}
	}
}

func TestBootstrapCoreRepositoryRoundTripsInactiveState(t *testing.T) {
	repo := New("postgres://example").BootstrapCore()
	expiresAt := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	state := bootstrap.BootstrapCoreState{
		Users: map[string]bootstrap.User{
			"rainbow": {
				Username:    "rainbow",
				DisplayName: "Rainbow Dash",
				Email:       "rainbow@example.test",
				FirstName:   "Rainbow",
				LastName:    "Dash",
			},
		},
		UserACLs: map[string]authz.ACL{
			"rainbow": {
				Read: authz.Permission{Actors: []string{"rainbow"}},
			},
		},
		UserKeys: map[string]map[string]bootstrap.KeyRecord{
			"rainbow": {
				"default": {
					Name:           "default",
					URI:            "/users/rainbow/keys/default",
					PublicKeyPEM:   "public-key",
					ExpirationDate: "2027-01-02T03:04:05Z",
					ExpiresAt:      &expiresAt,
				},
			},
		},
		Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: bootstrap.Organization{
					Name:     "ponyville",
					FullName: "Ponyville",
					OrgType:  "Business",
					GUID:     "guid",
				},
				Clients: map[string]bootstrap.Client{
					"ponyville-validator": {
						Name:         "ponyville-validator",
						ClientName:   "ponyville-validator",
						Organization: "ponyville",
						Validator:    true,
						PublicKey:    "public-key",
						URI:          "/organizations/ponyville/clients/ponyville-validator",
					},
				},
				ClientKeys: map[string]map[string]bootstrap.KeyRecord{
					"ponyville-validator": {
						"default": {
							Name:           "default",
							URI:            "/organizations/ponyville/clients/ponyville-validator/keys/default",
							PublicKeyPEM:   "public-key",
							ExpirationDate: "infinity",
						},
					},
				},
				Groups: map[string]bootstrap.Group{
					"clients": {
						Name:         "clients",
						GroupName:    "clients",
						Organization: "ponyville",
						Actors:       []string{"ponyville-validator"},
						Users:        []string{},
						Clients:      []string{"ponyville-validator"},
						Groups:       []string{},
					},
				},
				Containers: map[string]bootstrap.Container{
					"clients": {
						Name:          "clients",
						ContainerName: "clients",
						ContainerPath: "clients",
					},
				},
				ACLs: map[string]authz.ACL{
					"organization": {
						Read: authz.Permission{Groups: []string{"admins"}},
					},
				},
			},
		},
	}

	if err := repo.SaveBootstrapCore(state); err != nil {
		t.Fatalf("SaveBootstrapCore() error = %v", err)
	}
	got, err := repo.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("LoadBootstrapCore() = %#v, want %#v", got, state)
	}
}
