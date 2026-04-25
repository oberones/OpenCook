package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var errCoreObjectStoreFailed = errors.New("core object store failed")

type failingCoreObjectStore struct{}

func (failingCoreObjectStore) LoadCoreObjects() (CoreObjectState, error) {
	return CoreObjectState{}, nil
}

func (failingCoreObjectStore) SaveCoreObjects(CoreObjectState) error {
	return errCoreObjectStoreFailed
}

type controlledCoreObjectStore struct {
	delegate *MemoryCoreObjectStore
	fail     bool
}

func newControlledCoreObjectStore() *controlledCoreObjectStore {
	return &controlledCoreObjectStore{delegate: NewMemoryCoreObjectStore(CoreObjectState{})}
}

func (s *controlledCoreObjectStore) LoadCoreObjects() (CoreObjectState, error) {
	return s.delegate.LoadCoreObjects()
}

func (s *controlledCoreObjectStore) SaveCoreObjects(state CoreObjectState) error {
	if s.fail {
		return errCoreObjectStoreFailed
	}
	return s.delegate.SaveCoreObjects(state)
}

func TestCoreObjectStoreCapturesNormalizedObjectState(t *testing.T) {
	objectStore := NewMemoryCoreObjectStore(CoreObjectState{})
	service := newTestBootstrapServiceWithOptions(t, Options{
		SuperuserName: "pivotal",
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return objectStore
		},
	})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	creator := authn.Principal{Type: "user", Name: "silent-bob"}
	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{
			"name": "prod",
			"cookbook_versions": map[string]any{
				"demo": ">= 1.0.0",
			},
		},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	if _, err := service.CreateNode("ponyville", CreateNodeInput{
		Payload: map[string]any{
			"name":             "node1",
			"chef_environment": "prod",
			"normal":           map[string]any{"app": "demo"},
			"run_list":         []any{"recipe[demo::default]"},
			"policy_name":      "appserver",
			"policy_group":     "prod",
		},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}
	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":     "web",
			"run_list": []any{"demo"},
			"env_run_lists": map[string]any{
				"prod": []any{"role[base]", "demo::server"},
			},
		},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateRole() error = %v", err)
	}
	if _, err := service.CreateDataBag("ponyville", CreateDataBagInput{
		Payload: map[string]any{"name": "secrets"},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
	if _, err := service.CreateDataBagItem("ponyville", "secrets", CreateDataBagItemInput{
		Payload: map[string]any{
			"id":   "db",
			"user": "root",
			"nested": map[string]any{
				"enabled": true,
			},
		},
	}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
	revision, err := service.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
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
		Creator: creator,
	})
	if err != nil {
		t.Fatalf("CreatePolicyRevision() error = %v", err)
	}
	if _, _, err := service.UpsertPolicyGroupAssignment("ponyville", "prod", "appserver", UpdatePolicyGroupAssignmentInput{
		Payload: revision.Payload,
		Creator: creator,
	}); err != nil {
		t.Fatalf("UpsertPolicyGroupAssignment() error = %v", err)
	}
	if _, err := service.CreateSandbox("ponyville", CreateSandboxInput{
		Checksums: []string{"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}); err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	state, err := objectStore.LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects() error = %v", err)
	}
	org := state.Orgs["ponyville"]
	if _, ok := org.Environments[defaultEnvironmentName]; !ok {
		t.Fatal("persisted object state missing _default environment")
	}
	if got := org.Environments["prod"].CookbookVersions["demo"]; got != ">= 1.0.0" {
		t.Fatalf("persisted prod cookbook constraint = %q, want >= 1.0.0", got)
	}
	if got := org.Nodes["node1"].Normal["app"]; got != "demo" {
		t.Fatalf("persisted node normal app = %v, want demo", got)
	}
	if got := org.Roles["web"].RunList; !reflect.DeepEqual(got, []string{"recipe[demo]"}) {
		t.Fatalf("persisted role run_list = %v, want normalized recipe[demo]", got)
	}
	if got := org.Roles["web"].EnvRunLists["prod"]; !reflect.DeepEqual(got, []string{"role[base]", "recipe[demo::server]"}) {
		t.Fatalf("persisted prod env_run_list = %v, want normalized run list", got)
	}
	if got := org.DataBagItems["secrets"]["db"].RawData["user"]; got != "root" {
		t.Fatalf("persisted data bag item user = %v, want root", got)
	}
	if got := org.PolicyGroups["prod"].Policies["appserver"]; got != revision.RevisionID {
		t.Fatalf("persisted policy assignment = %q, want %q", got, revision.RevisionID)
	}
	if len(org.Sandboxes) != 1 {
		t.Fatalf("len(persisted sandboxes) = %d, want 1", len(org.Sandboxes))
	}
	for _, key := range []string{
		environmentACLKey("prod"),
		nodeACLKey("node1"),
		roleACLKey("web"),
		dataBagACLKey("secrets"),
		policyACLKey("appserver"),
		policyGroupACLKey("prod"),
	} {
		if _, ok := org.ACLs[key]; !ok {
			t.Fatalf("persisted object ACLs missing %q", key)
		}
	}
}

func TestInitialCoreObjectStateRehydratesObjectMapsAndACLs(t *testing.T) {
	service := newTestBootstrapServiceWithOptions(t, Options{
		SuperuserName: "pivotal",
		InitialBootstrapCoreState: &BootstrapCoreState{
			Users:    map[string]User{},
			UserACLs: map[string]authz.ACL{},
			UserKeys: map[string]map[string]KeyRecord{},
			Orgs: map[string]BootstrapCoreOrganizationState{
				"ponyville": {
					Organization: Organization{
						Name:     "ponyville",
						FullName: "Ponyville",
						OrgType:  "Business",
						GUID:     "guid",
					},
					Clients:    map[string]Client{},
					ClientKeys: map[string]map[string]KeyRecord{},
					Groups:     map[string]Group{},
					Containers: map[string]Container{},
					ACLs:       map[string]authz.ACL{organizationACLKey(): defaultOrganizationACL("pivotal")},
				},
			},
		},
		InitialCoreObjectState: &CoreObjectState{
			Orgs: map[string]CoreObjectOrganizationState{
				"ponyville": {
					Environments: map[string]Environment{
						"prod": {
							Name:               "prod",
							Description:        "Production",
							CookbookVersions:   map[string]string{"demo": "~> 1.2"},
							JSONClass:          defaultEnvironmentJSONClass,
							ChefType:           defaultEnvironmentChefType,
							DefaultAttributes:  map[string]any{},
							OverrideAttributes: map[string]any{},
						},
					},
					Nodes: map[string]Node{
						"node1": {
							Name:            "node1",
							JSONClass:       "Chef::Node",
							ChefType:        "node",
							ChefEnvironment: "prod",
							Override:        map[string]any{},
							Normal:          map[string]any{"app": "demo"},
							Default:         map[string]any{},
							Automatic:       map[string]any{},
							RunList:         []string{"recipe[demo::default]"},
						},
					},
					ACLs: map[string]authz.ACL{
						nodeACLKey("node1"): defaultNodeACL("pivotal", authn.Principal{Type: "user", Name: "silent-bob"}),
					},
				},
			},
		},
	})

	if _, orgExists, found := service.GetNode("ponyville", "node1"); !orgExists || !found {
		t.Fatalf("GetNode() existence = %t/%t, want true/true", orgExists, found)
	}
	if _, orgExists, found := service.GetEnvironment("ponyville", "prod"); !orgExists || !found {
		t.Fatalf("GetEnvironment(prod) existence = %t/%t, want true/true", orgExists, found)
	}
	if _, orgExists, found := service.GetEnvironment("ponyville", defaultEnvironmentName); !orgExists || !found {
		t.Fatalf("GetEnvironment(_default) existence = %t/%t, want true/true", orgExists, found)
	}
	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{
		Type:         "node",
		Name:         "node1",
		Organization: "ponyville",
	}); err != nil || !ok {
		t.Fatalf("ResolveACL(node1) ok/error = %t/%v, want true/nil", ok, err)
	}
}

func TestCoreObjectStoreCreateFailuresRollBackEveryObjectFamily(t *testing.T) {
	creator := authn.Principal{Type: "user", Name: "silent-bob"}

	tests := []struct {
		name   string
		mutate func(*Service) error
		assert func(*testing.T, *Service, *controlledCoreObjectStore)
	}{
		{
			name: "environment",
			mutate: func(service *Service) error {
				_, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
					Payload: map[string]any{"name": "failed-env"},
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertEnvironmentMissing(t, service, "failed-env")
				assertObjectACLExists(t, service, "environment", "failed-env", false)
				assertPersistedEnvironmentMissing(t, store, "failed-env")
			},
		},
		{
			name: "node",
			mutate: func(service *Service) error {
				_, err := service.CreateNode("ponyville", CreateNodeInput{
					Payload: map[string]any{"name": "failed-node", "chef_environment": "prod"},
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertNodeMissing(t, service, "failed-node")
				assertObjectACLExists(t, service, "node", "failed-node", false)
				assertPersistedNodeMissing(t, store, "failed-node")
			},
		},
		{
			name: "role",
			mutate: func(service *Service) error {
				_, err := service.CreateRole("ponyville", CreateRoleInput{
					Payload: map[string]any{"name": "failed-role", "run_list": []any{"recipe[demo]"}},
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertRoleMissing(t, service, "failed-role")
				assertObjectACLExists(t, service, "role", "failed-role", false)
				assertPersistedRoleMissing(t, store, "failed-role")
			},
		},
		{
			name: "data bag",
			mutate: func(service *Service) error {
				_, err := service.CreateDataBag("ponyville", CreateDataBagInput{
					Payload: map[string]any{"name": "failed-bag"},
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertDataBagMissing(t, service, "failed-bag")
				assertObjectACLExists(t, service, "data_bag", "failed-bag", false)
				assertPersistedDataBagMissing(t, store, "failed-bag")
			},
		},
		{
			name: "data bag item",
			mutate: func(service *Service) error {
				_, err := service.CreateDataBagItem("ponyville", "secrets", CreateDataBagItemInput{
					Payload: map[string]any{"id": "failed-item", "value": "nope"},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertDataBagItemMissing(t, service, "secrets", "failed-item")
				assertPersistedDataBagItemMissing(t, store, "secrets", "failed-item")
			},
		},
		{
			name: "policy revision",
			mutate: func(service *Service) error {
				_, err := service.CreatePolicyRevision("ponyville", "failed-policy", CreatePolicyRevisionInput{
					Payload: coreObjectPolicyPayload("failed-policy", "2222222222222222222222222222222222222222"),
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertPolicyMissing(t, service, "failed-policy")
				assertObjectACLExists(t, service, "policy", "failed-policy", false)
				assertPersistedPolicyMissing(t, store, "failed-policy")
			},
		},
		{
			name: "policy group assignment",
			mutate: func(service *Service) error {
				_, _, err := service.UpsertPolicyGroupAssignment("ponyville", "failed-group", "failed-assignment", UpdatePolicyGroupAssignmentInput{
					Payload: coreObjectPolicyPayload("failed-assignment", "3333333333333333333333333333333333333333"),
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertPolicyMissing(t, service, "failed-assignment")
				assertPolicyGroupMissing(t, service, "failed-group")
				assertObjectACLExists(t, service, "policy", "failed-assignment", false)
				assertObjectACLExists(t, service, "policy_group", "failed-group", false)
				assertPersistedPolicyMissing(t, store, "failed-assignment")
				assertPersistedPolicyGroupMissing(t, store, "failed-group")
			},
		},
		{
			name: "sandbox",
			mutate: func(service *Service) error {
				_, err := service.CreateSandbox("ponyville", CreateSandboxInput{
					Checksums: []string{"dddddddddddddddddddddddddddddddd"},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore) {
				assertSandboxCount(t, service, 1)
				assertCleanupCount(t, service, "dddddddddddddddddddddddddddddddd", 1)
				assertPersistedSandboxCount(t, store, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, store, _ := newCoreObjectFailureHarness(t)
			createBaselineCoreObjects(t, service, creator)

			store.fail = true
			err := tc.mutate(service)
			if !errors.Is(err, errCoreObjectStoreFailed) {
				t.Fatalf("mutation error = %v, want core object store failure", err)
			}
			tc.assert(t, service, store)
		})
	}
}

func TestCoreObjectStoreUpdateAndDeleteFailuresRollBackDerivedState(t *testing.T) {
	creator := authn.Principal{Type: "user", Name: "silent-bob"}

	tests := []struct {
		name   string
		mutate func(*Service, baselineCoreObjects) error
		assert func(*testing.T, *Service, *controlledCoreObjectStore, baselineCoreObjects)
	}{
		{
			name: "environment rename",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.UpdateEnvironment("ponyville", "prod", UpdateEnvironmentInput{
					Payload: map[string]any{"name": "staging"},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertEnvironmentExists(t, service, "prod")
				assertEnvironmentMissing(t, service, "staging")
				assertObjectACLExists(t, service, "environment", "prod", true)
				assertObjectACLExists(t, service, "environment", "staging", false)
				_, orgExists, envExists, err := service.SolveEnvironmentCookbookVersions("ponyville", "prod", map[string]any{})
				if err != nil || !orgExists || !envExists {
					t.Fatalf("depsolver env existence after rollback = %t/%t/%v, want true/true/nil", orgExists, envExists, err)
				}
				assertPersistedEnvironmentExists(t, store, "prod")
				assertPersistedEnvironmentMissing(t, store, "staging")
			},
		},
		{
			name: "environment delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeleteEnvironment("ponyville", "prod")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertEnvironmentExists(t, service, "prod")
				assertObjectACLExists(t, service, "environment", "prod", true)
				assertPersistedEnvironmentExists(t, store, "prod")
			},
		},
		{
			name: "node update",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.UpdateNode("ponyville", "node1", UpdateNodeInput{
					Payload: map[string]any{"chef_environment": "_default", "normal": map[string]any{"app": "changed"}},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				node := assertNodeExists(t, service, "node1")
				if node.ChefEnvironment != "prod" || node.Normal["app"] != "demo" {
					t.Fatalf("node after rollback = %#v, want prod/demo", node)
				}
				assertEnvironmentNodeVisible(t, service, "prod", "node1")
				assertPersistedNodeEnvironment(t, store, "node1", "prod")
			},
		},
		{
			name: "node delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeleteNode("ponyville", "node1")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertNodeExists(t, service, "node1")
				assertObjectACLExists(t, service, "node", "node1", true)
				assertEnvironmentNodeVisible(t, service, "prod", "node1")
				assertPersistedNodeExists(t, store, "node1")
			},
		},
		{
			name: "role update",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.UpdateRole("ponyville", "web", UpdateRoleInput{
					Payload: map[string]any{"run_list": []any{"recipe[changed]"}},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				role := assertRoleExists(t, service, "web")
				if !reflect.DeepEqual(role.RunList, []string{"recipe[demo]"}) {
					t.Fatalf("role run_list after rollback = %v, want [recipe[demo]]", role.RunList)
				}
				assertPersistedRoleRunList(t, store, "web", []string{"recipe[demo]"})
			},
		},
		{
			name: "role delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeleteRole("ponyville", "web")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertRoleExists(t, service, "web")
				assertObjectACLExists(t, service, "role", "web", true)
				assertPersistedRoleExists(t, store, "web")
			},
		},
		{
			name: "data bag item update",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.UpdateDataBagItem("ponyville", "secrets", "db", UpdateDataBagItemInput{
					Payload: map[string]any{"id": "db", "user": "changed"},
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				item := assertDataBagItemExists(t, service, "secrets", "db")
				if item.RawData["user"] != "root" {
					t.Fatalf("data bag item user after rollback = %v, want root", item.RawData["user"])
				}
				assertPersistedDataBagItemValue(t, store, "secrets", "db", "user", "root")
			},
		},
		{
			name: "data bag item delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeleteDataBagItem("ponyville", "secrets", "db")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertDataBagItemExists(t, service, "secrets", "db")
				assertPersistedDataBagItemExists(t, store, "secrets", "db")
			},
		},
		{
			name: "data bag delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeleteDataBag("ponyville", "secrets")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, _ baselineCoreObjects) {
				assertDataBagExists(t, service, "secrets")
				assertDataBagItemExists(t, service, "secrets", "db")
				assertObjectACLExists(t, service, "data_bag", "secrets", true)
				assertPersistedDataBagExists(t, store, "secrets")
				assertPersistedDataBagItemExists(t, store, "secrets", "db")
			},
		},
		{
			name: "policy assignment update",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, _, err := service.UpsertPolicyGroupAssignment("ponyville", "prod", "appserver", UpdatePolicyGroupAssignmentInput{
					Payload: coreObjectPolicyPayload("appserver", "4444444444444444444444444444444444444444"),
					Creator: creator,
				})
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, baseline baselineCoreObjects) {
				assertPolicyGroupAssignment(t, service, "prod", "appserver", baseline.revision.RevisionID)
				assertPolicyRevisionMissing(t, service, "appserver", "4444444444444444444444444444444444444444")
				assertPersistedPolicyGroupAssignment(t, store, "prod", "appserver", baseline.revision.RevisionID)
			},
		},
		{
			name: "policy assignment delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeletePolicyGroupAssignment("ponyville", "prod", "appserver")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, baseline baselineCoreObjects) {
				assertPolicyGroupAssignment(t, service, "prod", "appserver", baseline.revision.RevisionID)
				assertPersistedPolicyGroupAssignment(t, store, "prod", "appserver", baseline.revision.RevisionID)
			},
		},
		{
			name: "policy group delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeletePolicyGroup("ponyville", "prod")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, baseline baselineCoreObjects) {
				assertPolicyGroupAssignment(t, service, "prod", "appserver", baseline.revision.RevisionID)
				assertObjectACLExists(t, service, "policy_group", "prod", true)
				assertPersistedPolicyGroupExists(t, store, "prod")
			},
		},
		{
			name: "policy delete",
			mutate: func(service *Service, _ baselineCoreObjects) error {
				_, err := service.DeletePolicy("ponyville", "appserver")
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, baseline baselineCoreObjects) {
				assertPolicyRevisionExists(t, service, "appserver", baseline.revision.RevisionID)
				assertPolicyGroupAssignment(t, service, "prod", "appserver", baseline.revision.RevisionID)
				assertObjectACLExists(t, service, "policy", "appserver", true)
				assertPersistedPolicyExists(t, store, "appserver")
				assertPersistedPolicyGroupAssignment(t, store, "prod", "appserver", baseline.revision.RevisionID)
			},
		},
		{
			name: "sandbox delete",
			mutate: func(service *Service, baseline baselineCoreObjects) error {
				_, err := service.DeleteSandbox("ponyville", baseline.sandbox.ID)
				return err
			},
			assert: func(t *testing.T, service *Service, store *controlledCoreObjectStore, baseline baselineCoreObjects) {
				if _, orgExists, found := service.GetSandbox("ponyville", baseline.sandbox.ID); !orgExists || !found {
					t.Fatalf("GetSandbox() existence = %t/%t, want true/true after rollback", orgExists, found)
				}
				assertCleanupCount(t, service, baseline.sandboxChecksum, 0)
				assertPersistedSandboxCount(t, store, 1)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			service, store, _ := newCoreObjectFailureHarness(t)
			baseline := createBaselineCoreObjects(t, service, creator)

			store.fail = true
			err := tc.mutate(service, baseline)
			if !errors.Is(err, errCoreObjectStoreFailed) {
				t.Fatalf("mutation error = %v, want core object store failure", err)
			}
			tc.assert(t, service, store, baseline)
		})
	}
}

func TestCoreObjectStoreFailureRollsBackServiceStateAndObjectACLs(t *testing.T) {
	service := newTestBootstrapServiceWithOptions(t, Options{
		SuperuserName: "pivotal",
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return failingCoreObjectStore{}
		},
	})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	_, err := service.CreateNode("ponyville", CreateNodeInput{
		Payload: map[string]any{
			"name": "node1",
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})
	if !errors.Is(err, errCoreObjectStoreFailed) {
		t.Fatalf("CreateNode() error = %v, want core object store failure", err)
	}
	if _, orgExists, found := service.GetNode("ponyville", "node1"); !orgExists || found {
		t.Fatalf("GetNode() existence = %t/%t, want true/false after rollback", orgExists, found)
	}
	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{
		Type:         "node",
		Name:         "node1",
		Organization: "ponyville",
	}); err != nil || ok {
		t.Fatalf("ResolveACL(node1) ok/error = %t/%v, want false/nil after rollback", ok, err)
	}
}

type baselineCoreObjects struct {
	revision        PolicyRevision
	sandbox         Sandbox
	sandboxChecksum string
}

func newCoreObjectFailureHarness(t *testing.T) (*Service, *controlledCoreObjectStore, authn.Principal) {
	t.Helper()

	store := newControlledCoreObjectStore()
	service := newTestBootstrapServiceWithOptions(t, Options{
		SuperuserName: "pivotal",
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return store
		},
	})
	creator := authn.Principal{Type: "user", Name: "silent-bob"}
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OwnerName: creator.Name,
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	return service, store, creator
}

func createBaselineCoreObjects(t *testing.T, service *Service, creator authn.Principal) baselineCoreObjects {
	t.Helper()

	if _, err := service.CreateEnvironment("ponyville", CreateEnvironmentInput{
		Payload: map[string]any{"name": "prod"},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateEnvironment(prod) error = %v", err)
	}
	if _, err := service.CreateNode("ponyville", CreateNodeInput{
		Payload: map[string]any{
			"name":             "node1",
			"chef_environment": "prod",
			"normal":           map[string]any{"app": "demo"},
			"run_list":         []any{"recipe[demo]"},
		},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateNode(node1) error = %v", err)
	}
	if _, err := service.CreateRole("ponyville", CreateRoleInput{
		Payload: map[string]any{
			"name":     "web",
			"run_list": []any{"recipe[demo]"},
			"env_run_lists": map[string]any{
				"prod": []any{"recipe[demo]"},
			},
		},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := service.CreateDataBag("ponyville", CreateDataBagInput{
		Payload: map[string]any{"name": "secrets"},
		Creator: creator,
	}); err != nil {
		t.Fatalf("CreateDataBag(secrets) error = %v", err)
	}
	if _, err := service.CreateDataBagItem("ponyville", "secrets", CreateDataBagItemInput{
		Payload: map[string]any{"id": "db", "user": "root"},
	}); err != nil {
		t.Fatalf("CreateDataBagItem(db) error = %v", err)
	}
	revision, err := service.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: coreObjectPolicyPayload("appserver", "1111111111111111111111111111111111111111"),
		Creator: creator,
	})
	if err != nil {
		t.Fatalf("CreatePolicyRevision(appserver) error = %v", err)
	}
	if _, _, err := service.UpsertPolicyGroupAssignment("ponyville", "prod", "appserver", UpdatePolicyGroupAssignmentInput{
		Payload: revision.Payload,
		Creator: creator,
	}); err != nil {
		t.Fatalf("UpsertPolicyGroupAssignment(prod/appserver) error = %v", err)
	}
	checksum := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	sandbox, err := service.CreateSandbox("ponyville", CreateSandboxInput{
		Checksums: []string{checksum},
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	return baselineCoreObjects{
		revision:        revision,
		sandbox:         sandbox,
		sandboxChecksum: checksum,
	}
}

func coreObjectPolicyPayload(name, revisionID string) map[string]any {
	return map[string]any{
		"name":        name,
		"revision_id": revisionID,
		"run_list":    []any{"recipe[demo::default]"},
		"cookbook_locks": map[string]any{
			"demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "1.2.3",
			},
		},
	}
}

func assertEnvironmentExists(t *testing.T, service *Service, name string) Environment {
	t.Helper()
	env, orgExists, found := service.GetEnvironment("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetEnvironment(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return env
}

func assertEnvironmentMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetEnvironment("ponyville", name); !orgExists || found {
		t.Fatalf("GetEnvironment(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertNodeExists(t *testing.T, service *Service, name string) Node {
	t.Helper()
	node, orgExists, found := service.GetNode("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetNode(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return node
}

func assertNodeMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetNode("ponyville", name); !orgExists || found {
		t.Fatalf("GetNode(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertRoleExists(t *testing.T, service *Service, name string) Role {
	t.Helper()
	role, orgExists, found := service.GetRole("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetRole(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return role
}

func assertRoleMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetRole("ponyville", name); !orgExists || found {
		t.Fatalf("GetRole(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertDataBagExists(t *testing.T, service *Service, name string) DataBag {
	t.Helper()
	bag, orgExists, found := service.GetDataBag("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetDataBag(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return bag
}

func assertDataBagMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetDataBag("ponyville", name); !orgExists || found {
		t.Fatalf("GetDataBag(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertDataBagItemExists(t *testing.T, service *Service, bagName, itemID string) DataBagItem {
	t.Helper()
	item, orgExists, bagExists, found := service.GetDataBagItem("ponyville", bagName, itemID)
	if !orgExists || !bagExists || !found {
		t.Fatalf("GetDataBagItem(%q/%q) existence = %t/%t/%t, want true/true/true", bagName, itemID, orgExists, bagExists, found)
	}
	return item
}

func assertDataBagItemMissing(t *testing.T, service *Service, bagName, itemID string) {
	t.Helper()
	if _, orgExists, bagExists, found := service.GetDataBagItem("ponyville", bagName, itemID); !orgExists || !bagExists || found {
		t.Fatalf("GetDataBagItem(%q/%q) existence = %t/%t/%t, want true/true/false", bagName, itemID, orgExists, bagExists, found)
	}
}

func assertPolicyExists(t *testing.T, service *Service, name string) map[string]PolicyRevision {
	t.Helper()
	revisions, orgExists, found := service.GetPolicy("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetPolicy(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return revisions
}

func assertPolicyMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetPolicy("ponyville", name); !orgExists || found {
		t.Fatalf("GetPolicy(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertPolicyRevisionExists(t *testing.T, service *Service, policyName, revisionID string) {
	t.Helper()
	if _, orgExists, policyExists, found := service.GetPolicyRevision("ponyville", policyName, revisionID); !orgExists || !policyExists || !found {
		t.Fatalf("GetPolicyRevision(%q/%q) existence = %t/%t/%t, want true/true/true", policyName, revisionID, orgExists, policyExists, found)
	}
}

func assertPolicyRevisionMissing(t *testing.T, service *Service, policyName, revisionID string) {
	t.Helper()
	if _, orgExists, policyExists, found := service.GetPolicyRevision("ponyville", policyName, revisionID); !orgExists || !policyExists || found {
		t.Fatalf("GetPolicyRevision(%q/%q) existence = %t/%t/%t, want true/true/false", policyName, revisionID, orgExists, policyExists, found)
	}
}

func assertPolicyGroupExists(t *testing.T, service *Service, name string) PolicyGroup {
	t.Helper()
	group, orgExists, found := service.GetPolicyGroup("ponyville", name)
	if !orgExists || !found {
		t.Fatalf("GetPolicyGroup(%q) existence = %t/%t, want true/true", name, orgExists, found)
	}
	return group
}

func assertPolicyGroupMissing(t *testing.T, service *Service, name string) {
	t.Helper()
	if _, orgExists, found := service.GetPolicyGroup("ponyville", name); !orgExists || found {
		t.Fatalf("GetPolicyGroup(%q) existence = %t/%t, want true/false", name, orgExists, found)
	}
}

func assertPolicyGroupAssignment(t *testing.T, service *Service, groupName, policyName, revisionID string) {
	t.Helper()
	revision, orgExists, groupExists, found := service.GetPolicyGroupAssignment("ponyville", groupName, policyName)
	if !orgExists || !groupExists || !found {
		t.Fatalf("GetPolicyGroupAssignment(%q/%q) existence = %t/%t/%t, want true/true/true", groupName, policyName, orgExists, groupExists, found)
	}
	if revision.RevisionID != revisionID {
		t.Fatalf("GetPolicyGroupAssignment(%q/%q) revision = %q, want %q", groupName, policyName, revision.RevisionID, revisionID)
	}
}

func assertObjectACLExists(t *testing.T, service *Service, resourceType, name string, want bool) {
	t.Helper()
	_, ok, err := service.ResolveACL(context.Background(), authz.Resource{
		Type:         resourceType,
		Name:         name,
		Organization: "ponyville",
	})
	if err != nil {
		t.Fatalf("ResolveACL(%s/%s) error = %v", resourceType, name, err)
	}
	if ok != want {
		t.Fatalf("ResolveACL(%s/%s) ok = %t, want %t", resourceType, name, ok, want)
	}
}

func assertEnvironmentNodeVisible(t *testing.T, service *Service, environmentName, nodeName string) {
	t.Helper()
	nodes, orgExists, envExists := service.ListEnvironmentNodes("ponyville", environmentName)
	if !orgExists || !envExists {
		t.Fatalf("ListEnvironmentNodes(%q) existence = %t/%t, want true/true", environmentName, orgExists, envExists)
	}
	if nodes[nodeName] != "/organizations/ponyville/nodes/"+nodeName {
		t.Fatalf("ListEnvironmentNodes(%q)[%q] = %q, want node URL in derived view", environmentName, nodeName, nodes[nodeName])
	}
}

func assertSandboxCount(t *testing.T, service *Service, want int) {
	t.Helper()
	service.mu.RLock()
	defer service.mu.RUnlock()
	org := service.orgs["ponyville"]
	if org == nil {
		t.Fatal("org ponyville missing")
	}
	if len(org.sandboxes) != want {
		t.Fatalf("len(service sandboxes) = %d, want %d", len(org.sandboxes), want)
	}
}

func assertCleanupCount(t *testing.T, service *Service, checksum string, want int) {
	t.Helper()
	calls := 0
	if err := service.CleanupUnreferencedChecksums([]string{checksum}, func(string) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("CleanupUnreferencedChecksums(%q) error = %v", checksum, err)
	}
	if calls != want {
		t.Fatalf("cleanup calls for %q = %d, want %d", checksum, calls, want)
	}
}

func persistedCoreObjectOrg(t *testing.T, store *controlledCoreObjectStore) CoreObjectOrganizationState {
	t.Helper()
	state, err := store.LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects() error = %v", err)
	}
	return state.Orgs["ponyville"]
}

func assertPersistedEnvironmentExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Environments[name]; !ok {
		t.Fatalf("persisted environment %q missing", name)
	}
}

func assertPersistedEnvironmentMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Environments[name]; ok {
		t.Fatalf("persisted environment %q exists, want missing", name)
	}
}

func assertPersistedNodeExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Nodes[name]; !ok {
		t.Fatalf("persisted node %q missing", name)
	}
}

func assertPersistedNodeMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Nodes[name]; ok {
		t.Fatalf("persisted node %q exists, want missing", name)
	}
}

func assertPersistedNodeEnvironment(t *testing.T, store *controlledCoreObjectStore, name, environment string) {
	t.Helper()
	node := persistedCoreObjectOrg(t, store).Nodes[name]
	if node.ChefEnvironment != environment {
		t.Fatalf("persisted node %q environment = %q, want %q", name, node.ChefEnvironment, environment)
	}
}

func assertPersistedRoleExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Roles[name]; !ok {
		t.Fatalf("persisted role %q missing", name)
	}
}

func assertPersistedRoleMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Roles[name]; ok {
		t.Fatalf("persisted role %q exists, want missing", name)
	}
}

func assertPersistedRoleRunList(t *testing.T, store *controlledCoreObjectStore, name string, want []string) {
	t.Helper()
	role := persistedCoreObjectOrg(t, store).Roles[name]
	if !reflect.DeepEqual(role.RunList, want) {
		t.Fatalf("persisted role %q run_list = %v, want %v", name, role.RunList, want)
	}
}

func assertPersistedDataBagExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).DataBags[name]; !ok {
		t.Fatalf("persisted data bag %q missing", name)
	}
}

func assertPersistedDataBagMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).DataBags[name]; ok {
		t.Fatalf("persisted data bag %q exists, want missing", name)
	}
}

func assertPersistedDataBagItemExists(t *testing.T, store *controlledCoreObjectStore, bagName, itemID string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).DataBagItems[bagName][itemID]; !ok {
		t.Fatalf("persisted data bag item %q/%q missing", bagName, itemID)
	}
}

func assertPersistedDataBagItemMissing(t *testing.T, store *controlledCoreObjectStore, bagName, itemID string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).DataBagItems[bagName][itemID]; ok {
		t.Fatalf("persisted data bag item %q/%q exists, want missing", bagName, itemID)
	}
}

func assertPersistedDataBagItemValue(t *testing.T, store *controlledCoreObjectStore, bagName, itemID, key string, want any) {
	t.Helper()
	item := persistedCoreObjectOrg(t, store).DataBagItems[bagName][itemID]
	if item.RawData[key] != want {
		t.Fatalf("persisted data bag item %q/%q[%q] = %v, want %v", bagName, itemID, key, item.RawData[key], want)
	}
}

func assertPersistedPolicyExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Policies[name]; !ok {
		t.Fatalf("persisted policy %q missing", name)
	}
}

func assertPersistedPolicyMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).Policies[name]; ok {
		t.Fatalf("persisted policy %q exists, want missing", name)
	}
}

func assertPersistedPolicyGroupExists(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).PolicyGroups[name]; !ok {
		t.Fatalf("persisted policy group %q missing", name)
	}
}

func assertPersistedPolicyGroupMissing(t *testing.T, store *controlledCoreObjectStore, name string) {
	t.Helper()
	if _, ok := persistedCoreObjectOrg(t, store).PolicyGroups[name]; ok {
		t.Fatalf("persisted policy group %q exists, want missing", name)
	}
}

func assertPersistedPolicyGroupAssignment(t *testing.T, store *controlledCoreObjectStore, groupName, policyName, revisionID string) {
	t.Helper()
	group := persistedCoreObjectOrg(t, store).PolicyGroups[groupName]
	if group.Policies[policyName] != revisionID {
		t.Fatalf("persisted policy group %q policy %q = %q, want %q", groupName, policyName, group.Policies[policyName], revisionID)
	}
}

func assertPersistedSandboxCount(t *testing.T, store *controlledCoreObjectStore, want int) {
	t.Helper()
	if got := len(persistedCoreObjectOrg(t, store).Sandboxes); got != want {
		t.Fatalf("len(persisted sandboxes) = %d, want %d", got, want)
	}
}
