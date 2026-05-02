package pg_test

import (
	"context"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestStoreReloadPersistenceRefreshesRepositorySnapshots(t *testing.T) {
	ctx := context.Background()
	initialVersion := pgReloadCookbookVersion("1.0.0")
	pgState := pgtest.NewState(pgtest.Seed{
		Organizations: []pg.CookbookOrganizationRecord{{Name: "ponyville", FullName: "Ponyville"}},
		Versions:      []pg.CookbookVersionBundle{mustEncodeReloadCookbookVersion(t, "ponyville", initialVersion)},
		BootstrapCore: pgReloadBootstrapState("rainbow"),
		CoreObjects:   pgReloadCoreObjectState("old-node"),
	})

	db1, cleanup1, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB(first) error = %v", err)
	}
	defer cleanup1()
	db2, cleanup2, err := pgState.OpenDB()
	if err != nil {
		t.Fatalf("OpenDB(second) error = %v", err)
	}
	defer cleanup2()

	first := pg.New("postgres://reload-first")
	if err := first.ActivateCookbookPersistenceWithDB(ctx, db1); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB(first) error = %v", err)
	}
	second := pg.New("postgres://reload-second")
	if err := second.ActivateCookbookPersistenceWithDB(ctx, db2); err != nil {
		t.Fatalf("ActivateCookbookPersistenceWithDB(second) error = %v", err)
	}

	updatedVersion := pgReloadCookbookVersion("2.0.0")
	if _, _, _, err := second.CookbookStore().UpsertCookbookVersionWithReleasedChecksums("ponyville", updatedVersion, false); err != nil {
		t.Fatalf("UpsertCookbookVersionWithReleasedChecksums(updated) error = %v", err)
	}
	if err := second.BootstrapCore().SaveBootstrapCore(pgReloadBootstrapState("twilight")); err != nil {
		t.Fatalf("SaveBootstrapCore(updated) error = %v", err)
	}
	if err := second.CoreObjects().SaveCoreObjects(pgReloadCoreObjectState("new-node")); err != nil {
		t.Fatalf("SaveCoreObjects(updated) error = %v", err)
	}

	versions, _, _ := first.CookbookStore().ListCookbookVersionsByName("ponyville", "demo")
	if len(versions) != 1 || versions[0].Version != "1.0.0" {
		t.Fatalf("first cookbook versions before reload = %v, want stale 1.0.0 snapshot", versions)
	}
	core, err := first.BootstrapCore().LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore(before reload) error = %v", err)
	}
	if _, ok := core.Users["rainbow"]; !ok {
		t.Fatalf("first bootstrap users before reload = %v, want stale rainbow user", core.Users)
	}

	if err := first.ReloadPersistence(ctx); err != nil {
		t.Fatalf("ReloadPersistence() error = %v", err)
	}
	if err := first.ReloadPersistence(ctx); err != nil {
		t.Fatalf("ReloadPersistence(idempotent) error = %v", err)
	}

	versions, _, _ = first.CookbookStore().ListCookbookVersionsByName("ponyville", "demo")
	if len(versions) != 2 || versions[0].Version != "2.0.0" || versions[1].Version != "1.0.0" {
		t.Fatalf("first cookbook versions after reload = %v, want refreshed 2.0.0 and 1.0.0", versions)
	}
	core, err = first.BootstrapCore().LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore(after reload) error = %v", err)
	}
	if _, ok := core.Users["rainbow"]; ok {
		t.Fatalf("first bootstrap users after reload = %v, want stale rainbow removed", core.Users)
	}
	if _, ok := core.Users["twilight"]; !ok {
		t.Fatalf("first bootstrap users after reload = %v, want twilight", core.Users)
	}
	objects, err := first.CoreObjects().LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects(after reload) error = %v", err)
	}
	if _, ok := objects.Orgs["ponyville"].Nodes["old-node"]; ok {
		t.Fatalf("first object nodes after reload = %v, want stale old-node removed", objects.Orgs["ponyville"].Nodes)
	}
	if _, ok := objects.Orgs["ponyville"].Nodes["new-node"]; !ok {
		t.Fatalf("first object nodes after reload = %v, want new-node", objects.Orgs["ponyville"].Nodes)
	}
}

// mustEncodeReloadCookbookVersion creates seed rows through production
// encoding, keeping the reload test focused on cache refresh instead of JSON.
func mustEncodeReloadCookbookVersion(t *testing.T, orgName string, version bootstrap.CookbookVersion) pg.CookbookVersionBundle {
	t.Helper()
	bundle, err := pg.New("postgres://reload").Cookbooks().EncodeCookbookVersion(orgName, version)
	if err != nil {
		t.Fatalf("EncodeCookbookVersion() error = %v", err)
	}
	return bundle
}

// pgReloadCookbookVersion returns a minimal cookbook version with stable
// checksum metadata for repository snapshot tests.
func pgReloadCookbookVersion(version string) bootstrap.CookbookVersion {
	return bootstrap.CookbookVersion{
		Name:         "demo-" + version,
		CookbookName: "demo",
		Version:      version,
		JSONClass:    "Chef::CookbookVersion",
		ChefType:     "cookbook_version",
		Metadata: map[string]any{
			"name":    "demo",
			"version": version,
		},
		AllFiles: []bootstrap.CookbookFile{{Name: "default.rb", Path: "recipes/default.rb", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Specificity: "default"}},
	}
}

// pgReloadBootstrapState returns a minimal bootstrap snapshot with one user and
// one organization so repository reloads can prove stale rows disappear.
func pgReloadBootstrapState(username string) bootstrap.BootstrapCoreState {
	return bootstrap.BootstrapCoreState{
		Users: map[string]bootstrap.User{
			username: {Username: username, DisplayName: username},
		},
		UserACLs: map[string]authz.ACL{},
		UserKeys: map[string]map[string]bootstrap.KeyRecord{},
		Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: bootstrap.Organization{Name: "ponyville", FullName: "Ponyville", OrgType: "Business", GUID: "ponyville"},
				Clients:      map[string]bootstrap.Client{},
				ClientKeys:   map[string]map[string]bootstrap.KeyRecord{},
				Groups:       map[string]bootstrap.Group{},
				Containers:   map[string]bootstrap.Container{},
				ACLs:         map[string]authz.ACL{},
			},
		},
	}
}

// pgReloadCoreObjectState returns a minimal core-object snapshot with one node
// so reload assertions can catch stale object maps.
func pgReloadCoreObjectState(nodeName string) bootstrap.CoreObjectState {
	return bootstrap.CoreObjectState{
		Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {
				Environments: map[string]bootstrap.Environment{},
				Nodes: map[string]bootstrap.Node{
					nodeName: {Name: nodeName, JSONClass: "Chef::Node", ChefType: "node", ChefEnvironment: "_default"},
				},
				Roles:        map[string]bootstrap.Role{},
				DataBags:     map[string]bootstrap.DataBag{},
				DataBagItems: map[string]map[string]bootstrap.DataBagItem{},
				Sandboxes:    map[string]bootstrap.Sandbox{},
				Policies:     map[string]map[string]bootstrap.PolicyRevision{},
				PolicyGroups: map[string]bootstrap.PolicyGroup{},
				ACLs:         map[string]authz.ACL{},
			},
		},
	}
}
