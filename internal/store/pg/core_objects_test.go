package pg

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestCoreObjectRepositoryExposesCoreObjectMigration(t *testing.T) {
	repo := New("postgres://example").CoreObjects()

	migrations := repo.Migrations()
	if len(migrations) != 1 {
		t.Fatalf("len(Migrations()) = %d, want 1", len(migrations))
	}
	if migrations[0].Name != "0003_core_object_persistence.sql" {
		t.Fatalf("Migrations()[0].Name = %q, want core object persistence migration", migrations[0].Name)
	}

	sql := migrations[0].SQL
	for _, table := range []string{
		"oc_core_environments",
		"oc_core_nodes",
		"oc_core_roles",
		"oc_core_data_bags",
		"oc_core_data_bag_items",
		"oc_core_policy_revisions",
		"oc_core_policy_groups",
		"oc_core_sandboxes",
		"oc_core_sandbox_checksums",
		"oc_core_object_acls",
	} {
		if !strings.Contains(sql, table) {
			t.Fatalf("migration SQL missing %q table", table)
		}
	}
}

func TestCoreObjectRepositoryRoundTripsInactiveState(t *testing.T) {
	repo := New("postgres://example").CoreObjects()
	state := sampleCoreObjectState()

	if err := repo.SaveCoreObjects(state); err != nil {
		t.Fatalf("SaveCoreObjects() error = %v", err)
	}
	got, err := repo.LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects() error = %v", err)
	}
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("LoadCoreObjects() = %#v, want %#v", got, state)
	}
}

func TestCoreObjectRepositoryEncodeDecodeRows(t *testing.T) {
	repo := New("postgres://example").CoreObjects()
	state := sampleCoreObjectState()

	rows, err := repo.EncodeCoreObjects(state)
	if err != nil {
		t.Fatalf("EncodeCoreObjects() error = %v", err)
	}
	if len(rows.Environments) != 1 {
		t.Fatalf("len(rows.Environments) = %d, want 1", len(rows.Environments))
	}
	if len(rows.Nodes) != 1 {
		t.Fatalf("len(rows.Nodes) = %d, want 1", len(rows.Nodes))
	}
	if len(rows.Roles) != 1 {
		t.Fatalf("len(rows.Roles) = %d, want 1", len(rows.Roles))
	}
	if len(rows.DataBags) != 1 {
		t.Fatalf("len(rows.DataBags) = %d, want 1", len(rows.DataBags))
	}
	if len(rows.DataBagItems) != 1 {
		t.Fatalf("len(rows.DataBagItems) = %d, want 1", len(rows.DataBagItems))
	}
	if len(rows.PolicyRevisions) != 1 {
		t.Fatalf("len(rows.PolicyRevisions) = %d, want 1", len(rows.PolicyRevisions))
	}
	if len(rows.PolicyGroups) != 1 {
		t.Fatalf("len(rows.PolicyGroups) = %d, want 1", len(rows.PolicyGroups))
	}
	if len(rows.Sandboxes) != 1 {
		t.Fatalf("len(rows.Sandboxes) = %d, want 1", len(rows.Sandboxes))
	}
	if len(rows.SandboxChecksums) != 2 {
		t.Fatalf("len(rows.SandboxChecksums) = %d, want 2", len(rows.SandboxChecksums))
	}
	if len(rows.ACLs) != 2 {
		t.Fatalf("len(rows.ACLs) = %d, want 2", len(rows.ACLs))
	}

	got, err := repo.DecodeCoreObjects(rows)
	if err != nil {
		t.Fatalf("DecodeCoreObjects() error = %v", err)
	}
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("DecodeCoreObjects() = %#v, want %#v", got, state)
	}
}

func sampleCoreObjectState() bootstrap.CoreObjectState {
	createdAt := time.Date(2026, 4, 24, 12, 30, 0, 0, time.UTC)
	return bootstrap.CoreObjectState{
		Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {
				Environments: map[string]bootstrap.Environment{
					"prod": {
						Name:               "prod",
						Description:        "Production",
						CookbookVersions:   map[string]string{"demo": "~> 1.2"},
						JSONClass:          "Chef::Environment",
						ChefType:           "environment",
						DefaultAttributes:  map[string]any{"tier": "frontend"},
						OverrideAttributes: map[string]any{"feature": true},
					},
				},
				Nodes: map[string]bootstrap.Node{
					"node1": {
						Name:            "node1",
						JSONClass:       "Chef::Node",
						ChefType:        "node",
						ChefEnvironment: "prod",
						Override:        map[string]any{"override": "value"},
						Normal:          map[string]any{"app": "demo"},
						Default:         map[string]any{"default": "value"},
						Automatic:       map[string]any{"automatic": "value"},
						RunList:         []string{"recipe[demo::default]"},
						PolicyName:      "appserver",
						PolicyGroup:     "prod",
					},
				},
				Roles: map[string]bootstrap.Role{
					"web": {
						Name:               "web",
						Description:        "Web role",
						JSONClass:          "Chef::Role",
						ChefType:           "role",
						DefaultAttributes:  map[string]any{"role_default": "yes"},
						OverrideAttributes: map[string]any{"role_override": "yes"},
						RunList:            []string{"recipe[demo]"},
						EnvRunLists:        map[string][]string{"prod": []string{"role[base]", "recipe[demo::server]"}},
					},
				},
				DataBags: map[string]bootstrap.DataBag{
					"secrets": {
						Name:      "secrets",
						JSONClass: "Chef::DataBag",
						ChefType:  "data_bag",
					},
				},
				DataBagItems: map[string]map[string]bootstrap.DataBagItem{
					"secrets": {
						"db": {
							ID:      "db",
							RawData: map[string]any{"id": "db", "user": "root", "nested": map[string]any{"enabled": true}},
						},
					},
				},
				Policies: map[string]map[string]bootstrap.PolicyRevision{
					"appserver": {
						"1111111111111111111111111111111111111111": {
							Name:       "appserver",
							RevisionID: "1111111111111111111111111111111111111111",
							Payload: map[string]any{
								"name":        "appserver",
								"revision_id": "1111111111111111111111111111111111111111",
								"run_list":    []any{"recipe[demo::default]"},
								"cookbook_locks": map[string]any{
									"demo": map[string]any{
										"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
										"version":    "1.2.3",
									},
								},
							},
						},
					},
				},
				PolicyGroups: map[string]bootstrap.PolicyGroup{
					"prod": {
						Name:     "prod",
						Policies: map[string]string{"appserver": "1111111111111111111111111111111111111111"},
					},
				},
				Sandboxes: map[string]bootstrap.Sandbox{
					"sandbox1": {
						ID:           "sandbox1",
						Organization: "ponyville",
						Checksums:    []string{"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
						CreatedAt:    createdAt,
					},
				},
				ACLs: map[string]authz.ACL{
					"node:node1": {
						Read: authz.Permission{Actors: []string{"pivotal"}, Groups: []string{"admins", "users"}},
					},
					"role:web": {
						Read: authz.Permission{Actors: []string{"pivotal"}, Groups: []string{"admins", "users"}},
					},
				},
			},
		},
	}
}
