package main

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

const (
	adminMigrationScaleFixtureSeed           = "opencook-production-scale-migration-fixture-v1"
	adminMigrationScaleFixtureDefaultOrg     = "ponyville"
	adminMigrationScaleFixtureSuperuser      = "pivotal"
	adminMigrationScaleFixturePublicKeyPEM   = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA49TA0y81ps0zxkOpmf5V\n4/c4IeR5yVyQFpX3JpxO4TquwnRh8VSUhrw8kkTLmB3cS39Db+3HadvhoqCEbqPE\n6915kXSuk/cWIcNozujLK7tkuPEyYVsyTioQAddSdfe+8EhQVf3oHxaKmUd6waXr\nWqYCnhxgOjxocenREYNhZ/OETIeiPbOku47vB4nJK/0GhKBytL2XnsRgfKgDxf42\nBqAi1jglIdeq8lAWZNF9TbNBU21AO1iuT7Pm6LyQujhggPznR5FJhXKRUARXBJZa\nwxpGV4dGtdcahwXNE4601aXPra+xPcRd2puCNoEDBzgVuTSsLYeKBDMSfs173W1Q\nYwIDAQAB\n-----END PUBLIC KEY-----\n"
	adminMigrationScaleFixtureExpirationDate = "infinity"
)

type adminMigrationScaleFixture struct {
	Profile             string
	Seed                string
	DefaultOrganization string
	Bootstrap           bootstrap.BootstrapCoreState
	CoreObjects         bootstrap.CoreObjectState
	Cookbooks           adminMigrationCookbookExport
	BlobCopies          []adminMigrationBackupBlobData
	SharedChecksum      string
	Inventory           adminMigrationInventory
}

type adminMigrationScaleFixtureProfileSpec struct {
	Name                     string
	Organizations            []string
	ExtraUsersPerOrg         int
	ClientsPerOrg            int
	EnvironmentsPerOrg       int
	NodesPerOrg              int
	RolesPerOrg              int
	DataBagsPerOrg           int
	DataBagItemsPerBag       int
	PoliciesPerOrg           int
	PolicyRevisionsPerPolicy int
	PolicyGroupsPerOrg       int
	SandboxesPerOrg          int
	CookbookVersionsPerOrg   int
	CookbookArtifactsPerOrg  int
}

// adminMigrationProductionScaleFixture builds deterministic PostgreSQL-shaped
// state and checksum blobs for migration validation without requiring a live DB.
func adminMigrationProductionScaleFixture(profile string) (adminMigrationScaleFixture, error) {
	spec, err := adminMigrationScaleFixtureSpec(profile)
	if err != nil {
		return adminMigrationScaleFixture{}, err
	}

	fixture := adminMigrationScaleFixture{
		Profile:             spec.Name,
		Seed:                adminMigrationScaleFixtureSeed,
		DefaultOrganization: adminMigrationScaleFixtureDefaultOrg,
		Bootstrap: bootstrap.BootstrapCoreState{
			Users:    map[string]bootstrap.User{},
			UserACLs: map[string]authz.ACL{},
			UserKeys: map[string]map[string]bootstrap.KeyRecord{},
			Orgs:     map[string]bootstrap.BootstrapCoreOrganizationState{},
		},
		CoreObjects: bootstrap.CoreObjectState{Orgs: map[string]bootstrap.CoreObjectOrganizationState{}},
		Cookbooks:   adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}

	blobBodies := map[string][]byte{}
	sharedChecksum := adminMigrationScaleFixtureBlob(blobBodies, "shared-cookbook-artifact-sandbox")
	fixture.SharedChecksum = sharedChecksum
	adminMigrationScaleFixtureAddUser(&fixture, adminMigrationScaleFixtureSuperuser, "OpenCook Bootstrap Superuser")

	for orgIndex, orgName := range spec.Organizations {
		adminMigrationScaleFixtureAddOrg(&fixture, spec, orgIndex, orgName, blobBodies, sharedChecksum)
	}

	for _, checksum := range adminMigrationSortedMapKeys(blobBodies) {
		fixture.BlobCopies = append(fixture.BlobCopies, adminMigrationBackupBlobData{Checksum: checksum, Body: append([]byte(nil), blobBodies[checksum]...)})
	}
	cookbookInventory := adminMigrationCookbookInventoryFromExport(fixture.Cookbooks)
	fixture.Inventory = adminMigrationInventoryFromState(fixture.Bootstrap, fixture.CoreObjects, cookbookInventory, "")
	return fixture, nil
}

// adminMigrationScaleFixtureSpec keeps profile sizing deterministic and bounded
// so unit tests use small while functional/operator runs can opt into more data.
func adminMigrationScaleFixtureSpec(profile string) (adminMigrationScaleFixtureProfileSpec, error) {
	switch strings.TrimSpace(profile) {
	case "", adminMigrationScaleProfileSmall:
		return adminMigrationScaleFixtureProfileSpec{
			Name:                     adminMigrationScaleProfileSmall,
			Organizations:            []string{adminMigrationScaleFixtureDefaultOrg, "canterlot"},
			ExtraUsersPerOrg:         1,
			ClientsPerOrg:            2,
			EnvironmentsPerOrg:       2,
			NodesPerOrg:              2,
			RolesPerOrg:              2,
			DataBagsPerOrg:           2,
			DataBagItemsPerBag:       2,
			PoliciesPerOrg:           2,
			PolicyRevisionsPerPolicy: 2,
			PolicyGroupsPerOrg:       2,
			SandboxesPerOrg:          2,
			CookbookVersionsPerOrg:   2,
			CookbookArtifactsPerOrg:  2,
		}, nil
	case adminMigrationScaleProfileMedium:
		return adminMigrationScaleFixtureProfileSpec{
			Name:                     adminMigrationScaleProfileMedium,
			Organizations:            []string{adminMigrationScaleFixtureDefaultOrg, "canterlot", "cloudsdale"},
			ExtraUsersPerOrg:         4,
			ClientsPerOrg:            4,
			EnvironmentsPerOrg:       4,
			NodesPerOrg:              12,
			RolesPerOrg:              6,
			DataBagsPerOrg:           3,
			DataBagItemsPerBag:       6,
			PoliciesPerOrg:           4,
			PolicyRevisionsPerPolicy: 2,
			PolicyGroupsPerOrg:       3,
			SandboxesPerOrg:          4,
			CookbookVersionsPerOrg:   6,
			CookbookArtifactsPerOrg:  4,
		}, nil
	case adminMigrationScaleProfileLarge:
		return adminMigrationScaleFixtureProfileSpec{
			Name:                     adminMigrationScaleProfileLarge,
			Organizations:            []string{adminMigrationScaleFixtureDefaultOrg, "canterlot", "cloudsdale", "manehattan", "appleloosa"},
			ExtraUsersPerOrg:         12,
			ClientsPerOrg:            12,
			EnvironmentsPerOrg:       8,
			NodesPerOrg:              100,
			RolesPerOrg:              20,
			DataBagsPerOrg:           6,
			DataBagItemsPerBag:       20,
			PoliciesPerOrg:           8,
			PolicyRevisionsPerPolicy: 3,
			PolicyGroupsPerOrg:       4,
			SandboxesPerOrg:          12,
			CookbookVersionsPerOrg:   24,
			CookbookArtifactsPerOrg:  12,
		}, nil
	default:
		return adminMigrationScaleFixtureProfileSpec{}, fmt.Errorf("unknown migration scale fixture profile %q", profile)
	}
}

// adminMigrationScaleFixtureSourceState exposes the generated state through the
// same import model used by source import/sync, shadow compare, and rehearsal.
func adminMigrationScaleFixtureSourceState(fixture adminMigrationScaleFixture) adminMigrationSourceImportState {
	return adminMigrationSourceImportState{
		Bootstrap:   bootstrap.CloneBootstrapCoreState(fixture.Bootstrap),
		CoreObjects: bootstrap.CloneCoreObjectState(fixture.CoreObjects),
		Cookbooks:   adminMigrationCloneCookbookExport(fixture.Cookbooks),
	}
}

// adminMigrationScaleFixtureSourceScopes returns every source family covered by
// the fixture so sync-diff tests can assert row IDs are complete and unique.
func adminMigrationScaleFixtureSourceScopes(fixture adminMigrationScaleFixture) map[adminMigrationSourcePayloadKey]bool {
	scopes := map[adminMigrationSourcePayloadKey]bool{
		{Family: "users"}:                    true,
		{Family: "user_acls"}:                true,
		{Family: "user_keys"}:                true,
		{Family: "server_admin_memberships"}: true,
	}
	for _, orgName := range adminMigrationOrgNames(fixture.Bootstrap, fixture.CoreObjects, adminMigrationCookbookInventoryFromExport(fixture.Cookbooks), "") {
		for _, family := range adminMigrationValidationOrganizationFamilies() {
			scopes[adminMigrationSourcePayloadKey{Organization: orgName, Family: family}] = true
		}
	}
	return scopes
}

// adminMigrationScaleFixtureAddUser registers a user, ACL, and default key in
// the global bootstrap state using deterministic public-key metadata.
func adminMigrationScaleFixtureAddUser(fixture *adminMigrationScaleFixture, username, displayName string) {
	fixture.Bootstrap.Users[username] = bootstrap.User{
		Username:    username,
		DisplayName: displayName,
		Email:       username + "@example.invalid",
		FirstName:   username,
		LastName:    "fixture",
	}
	fixture.Bootstrap.UserACLs[username] = adminMigrationScaleFixtureACL([]string{adminMigrationScaleFixtureSuperuser, username}, nil)
	fixture.Bootstrap.UserKeys[username] = map[string]bootstrap.KeyRecord{
		"default": adminMigrationScaleFixtureKey("user", "", username, "default"),
	}
}

// adminMigrationScaleFixtureAddOrg fills one organization with identity,
// authorization, core object, cookbook, sandbox, and shared-blob coverage.
func adminMigrationScaleFixtureAddOrg(fixture *adminMigrationScaleFixture, spec adminMigrationScaleFixtureProfileSpec, orgIndex int, orgName string, blobBodies map[string][]byte, sharedChecksum string) {
	owner := orgName + "-owner"
	adminMigrationScaleFixtureAddUser(fixture, owner, adminMigrationScaleFixtureTitle(owner))
	extraUsers := []string{}
	for i := 1; i <= spec.ExtraUsersPerOrg; i++ {
		username := adminMigrationScaleFixtureName(orgName, "user", i)
		adminMigrationScaleFixtureAddUser(fixture, username, adminMigrationScaleFixtureTitle(username))
		extraUsers = append(extraUsers, username)
	}

	bootstrapOrg := adminMigrationScaleFixtureBootstrapOrg(orgName, orgIndex, owner, extraUsers, spec.ClientsPerOrg)
	coreOrg := adminMigrationScaleFixtureCoreOrg(orgName, spec, blobBodies, sharedChecksum)
	cookbookOrg := adminMigrationScaleFixtureCookbookOrg(orgName, spec, blobBodies, sharedChecksum)

	fixture.Bootstrap.Orgs[orgName] = bootstrapOrg
	fixture.CoreObjects.Orgs[orgName] = coreOrg
	fixture.Cookbooks.Orgs[orgName] = adminMigrationSortedCookbookExportOrg(cookbookOrg)
}

// adminMigrationScaleFixtureBootstrapOrg mirrors the bootstrap core families
// that production migration validation must count for every organization.
func adminMigrationScaleFixtureBootstrapOrg(orgName string, orgIndex int, owner string, extraUsers []string, clientsPerOrg int) bootstrap.BootstrapCoreOrganizationState {
	org := bootstrap.BootstrapCoreOrganizationState{
		Organization: bootstrap.Organization{
			Name:     orgName,
			FullName: adminMigrationScaleFixtureTitle(orgName) + " Scale Fixture",
			OrgType:  "Business",
			GUID:     adminMigrationScaleFixtureHex(fmt.Sprintf("org-%s-%d", orgName, orgIndex), 32),
		},
		Clients:    map[string]bootstrap.Client{},
		ClientKeys: map[string]map[string]bootstrap.KeyRecord{},
		Groups:     map[string]bootstrap.Group{},
		Containers: map[string]bootstrap.Container{},
		ACLs:       map[string]authz.ACL{},
	}

	for _, container := range adminMigrationRequiredDefaultContainers() {
		org.Containers[container] = bootstrap.Container{Name: container, ContainerName: container, ContainerPath: container}
		org.ACLs[adminMigrationContainerACLKey(container)] = adminMigrationScaleFixtureACL(nil, []string{"admins"})
	}
	adminUsers := adminMigrationUniqueSortedStrings(append([]string{adminMigrationScaleFixtureSuperuser, owner}, extraUsers...))
	org.Groups["admins"] = adminMigrationScaleFixtureGroup(orgName, "admins", []string{adminMigrationScaleFixtureSuperuser, owner}, nil, nil)
	org.Groups["billing-admins"] = adminMigrationScaleFixtureGroup(orgName, "billing-admins", []string{owner}, nil, nil)
	org.Groups["users"] = adminMigrationScaleFixtureGroup(orgName, "users", adminUsers, nil, []string{orgName + "-user-access"})

	clients := []string{orgName + "-validator"}
	for i := 1; i <= clientsPerOrg; i++ {
		clients = append(clients, adminMigrationScaleFixtureName(orgName, "client", i))
	}
	for _, clientName := range clients {
		validator := clientName == orgName+"-validator"
		org.Clients[clientName] = bootstrap.Client{
			Name:         clientName,
			ClientName:   clientName,
			Organization: orgName,
			Validator:    validator,
			Admin:        false,
			PublicKey:    adminMigrationScaleFixturePublicKeyPEM,
			URI:          "/organizations/" + orgName + "/clients/" + clientName,
		}
		org.ClientKeys[clientName] = map[string]bootstrap.KeyRecord{
			"default": adminMigrationScaleFixtureKey("client", orgName, clientName, "default"),
		}
		org.ACLs[adminMigrationClientACLKey(clientName)] = adminMigrationScaleFixtureACL([]string{adminMigrationScaleFixtureSuperuser, clientName}, []string{"admins"})
	}
	org.Groups["clients"] = adminMigrationScaleFixtureGroup(orgName, "clients", nil, clients, nil)
	org.Groups["deployers"] = adminMigrationScaleFixtureGroup(orgName, "deployers", extraUsers, clients[1:], []string{"users"})

	for _, groupName := range adminMigrationSortedMapKeys(org.Groups) {
		org.ACLs[adminMigrationGroupACLKey(groupName)] = adminMigrationScaleFixtureACL(nil, []string{"admins"})
	}
	org.ACLs[adminMigrationOrganizationACLKey()] = adminMigrationScaleFixtureACL([]string{adminMigrationScaleFixtureSuperuser}, []string{"admins", "users"})
	return org
}

// adminMigrationScaleFixtureTitle converts deterministic ASCII fixture slugs
// into display strings without relying on deprecated strings.Title behavior.
func adminMigrationScaleFixtureTitle(value string) string {
	words := strings.Fields(strings.ReplaceAll(value, "-", " "))
	for i, word := range words {
		if word == "" {
			continue
		}
		bytes := []byte(word)
		if bytes[0] >= 'a' && bytes[0] <= 'z' {
			bytes[0] -= 'a' - 'A'
		}
		words[i] = string(bytes)
	}
	return strings.Join(words, " ")
}

// adminMigrationScaleFixtureCoreOrg creates persisted core-object families with
// enough cross-references to exercise search, policy, ACL, and blob validation.
func adminMigrationScaleFixtureCoreOrg(orgName string, spec adminMigrationScaleFixtureProfileSpec, blobBodies map[string][]byte, sharedChecksum string) bootstrap.CoreObjectOrganizationState {
	org := bootstrap.CoreObjectOrganizationState{
		DataBags:     map[string]bootstrap.DataBag{},
		DataBagItems: map[string]map[string]bootstrap.DataBagItem{},
		Environments: map[string]bootstrap.Environment{},
		Nodes:        map[string]bootstrap.Node{},
		Roles:        map[string]bootstrap.Role{},
		Sandboxes:    map[string]bootstrap.Sandbox{},
		Policies:     map[string]map[string]bootstrap.PolicyRevision{},
		PolicyGroups: map[string]bootstrap.PolicyGroup{},
		ACLs:         map[string]authz.ACL{},
	}

	org.Environments["_default"] = adminMigrationScaleFixtureEnvironment("_default", map[string]string{})
	org.ACLs[adminMigrationEnvironmentACLKey("_default")] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	for i := 1; i <= spec.EnvironmentsPerOrg; i++ {
		envName := adminMigrationScaleFixtureName(orgName, "env", i)
		org.Environments[envName] = adminMigrationScaleFixtureEnvironment(envName, map[string]string{"scale_app": ">= 1.0.0"})
		org.ACLs[adminMigrationEnvironmentACLKey(envName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	for i := 1; i <= spec.RolesPerOrg; i++ {
		roleName := adminMigrationScaleFixtureName(orgName, "role", i)
		org.Roles[roleName] = adminMigrationScaleFixtureRole(roleName, i)
		org.ACLs[adminMigrationRoleACLKey(roleName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	for i := 1; i <= spec.NodesPerOrg; i++ {
		nodeName := adminMigrationScaleFixtureName(orgName, "node", i)
		org.Nodes[nodeName] = adminMigrationScaleFixtureNode(orgName, nodeName, i, spec)
		org.ACLs[adminMigrationNodeACLKey(nodeName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	for i := 1; i <= spec.DataBagsPerOrg; i++ {
		bagName := adminMigrationScaleFixtureDataBagName(i)
		org.DataBags[bagName] = bootstrap.DataBag{Name: bagName, JSONClass: "Chef::DataBag", ChefType: "data_bag"}
		org.DataBagItems[bagName] = map[string]bootstrap.DataBagItem{}
		for item := 1; item <= spec.DataBagItemsPerBag; item++ {
			itemID := adminMigrationScaleFixtureName(orgName, "item", item)
			org.DataBagItems[bagName][itemID] = adminMigrationScaleFixtureDataBagItem(bagName, itemID, item)
		}
		org.ACLs[adminMigrationDataBagACLKey(bagName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	for policy := 1; policy <= spec.PoliciesPerOrg; policy++ {
		policyName := adminMigrationScaleFixtureName(orgName, "policy", policy)
		org.Policies[policyName] = map[string]bootstrap.PolicyRevision{}
		for revision := 1; revision <= spec.PolicyRevisionsPerPolicy; revision++ {
			revisionID := adminMigrationScaleFixtureHex(fmt.Sprintf("%s-policy-%d-revision-%d", orgName, policy, revision), 40)
			org.Policies[policyName][revisionID] = adminMigrationScaleFixturePolicyRevision(policyName, revisionID)
		}
		org.ACLs[adminMigrationPolicyACLKey(policyName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	policyNames := adminMigrationSortedMapKeys(org.Policies)
	for group := 1; group <= spec.PolicyGroupsPerOrg; group++ {
		groupName := adminMigrationScaleFixtureName(orgName, "policy-group", group)
		assignments := map[string]string{}
		for _, policyName := range policyNames {
			revisions := adminMigrationSortedMapKeys(org.Policies[policyName])
			assignments[policyName] = revisions[(group-1)%len(revisions)]
		}
		org.PolicyGroups[groupName] = bootstrap.PolicyGroup{Name: groupName, Policies: assignments}
		org.ACLs[adminMigrationPolicyGroupACLKey(groupName)] = adminMigrationScaleFixtureACL(nil, []string{"admins", "users", "clients"})
	}
	for i := 1; i <= spec.SandboxesPerOrg; i++ {
		uniqueChecksum := adminMigrationScaleFixtureBlob(blobBodies, fmt.Sprintf("%s-sandbox-%d", orgName, i))
		org.Sandboxes[adminMigrationScaleFixtureName(orgName, "sandbox", i)] = bootstrap.Sandbox{
			ID:           adminMigrationScaleFixtureName(orgName, "sandbox", i),
			Organization: orgName,
			Checksums:    adminMigrationUniqueSortedStrings([]string{sharedChecksum, uniqueChecksum}),
			CreatedAt:    adminMigrationScaleFixtureTime(i),
		}
	}
	return org
}

// adminMigrationScaleFixtureCookbookOrg creates cookbook and artifact rows that
// intentionally reuse one checksum across versions, artifacts, and sandboxes.
func adminMigrationScaleFixtureCookbookOrg(orgName string, spec adminMigrationScaleFixtureProfileSpec, blobBodies map[string][]byte, sharedChecksum string) adminMigrationCookbookOrgExport {
	org := adminMigrationCookbookOrgExport{}
	for i := 1; i <= spec.CookbookVersionsPerOrg; i++ {
		version := fmt.Sprintf("1.%d.0", i)
		uniqueChecksum := adminMigrationScaleFixtureBlob(blobBodies, fmt.Sprintf("%s-cookbook-version-%d", orgName, i))
		org.Versions = append(org.Versions, bootstrap.CookbookVersion{
			Name:         "scale_app-" + version,
			CookbookName: "scale_app",
			Version:      version,
			JSONClass:    "Chef::CookbookVersion",
			ChefType:     "cookbook_version",
			Frozen:       i%2 == 0,
			Metadata: map[string]any{
				"name":         "scale_app",
				"version":      version,
				"dependencies": map[string]any{"scale_dep": ">= 0.1.0"},
			},
			AllFiles: []bootstrap.CookbookFile{
				adminMigrationScaleFixtureCookbookFile("recipes/shared.rb", sharedChecksum),
				adminMigrationScaleFixtureCookbookFile(fmt.Sprintf("recipes/version_%02d.rb", i), uniqueChecksum),
			},
		})
	}
	for i := 1; i <= spec.CookbookArtifactsPerOrg; i++ {
		identifier := adminMigrationScaleFixtureHex(fmt.Sprintf("%s-artifact-%d", orgName, i), 40)
		uniqueChecksum := adminMigrationScaleFixtureBlob(blobBodies, fmt.Sprintf("%s-cookbook-artifact-%d", orgName, i))
		org.Artifacts = append(org.Artifacts, bootstrap.CookbookArtifact{
			Name:       "scale_artifact",
			Identifier: identifier,
			Version:    fmt.Sprintf("2.%d.0", i),
			ChefType:   "cookbook_artifact",
			Frozen:     i%2 == 0,
			Metadata: map[string]any{
				"name":    "scale_artifact",
				"version": fmt.Sprintf("2.%d.0", i),
			},
			AllFiles: []bootstrap.CookbookFile{
				adminMigrationScaleFixtureCookbookFile("recipes/shared.rb", sharedChecksum),
				adminMigrationScaleFixtureCookbookFile(fmt.Sprintf("recipes/artifact_%02d.rb", i), uniqueChecksum),
			},
		})
	}
	return org
}

// adminMigrationScaleFixtureGroup keeps actor slices sorted and in sync with
// the split user/client/group membership fields used by migration inventories.
func adminMigrationScaleFixtureGroup(orgName, name string, users, clients, groups []string) bootstrap.Group {
	users = adminMigrationUniqueSortedStrings(users)
	clients = adminMigrationUniqueSortedStrings(clients)
	groups = adminMigrationUniqueSortedStrings(groups)
	actors := adminMigrationUniqueSortedStrings(append(append([]string{}, users...), clients...))
	return bootstrap.Group{Name: name, GroupName: name, Organization: orgName, Actors: actors, Users: users, Clients: clients, Groups: groups}
}

// adminMigrationScaleFixtureACL returns a permissive but deterministic ACL
// document suitable for migration count and JSON round-trip validation.
func adminMigrationScaleFixtureACL(actors, groups []string) authz.ACL {
	actors = adminMigrationUniqueSortedStrings(actors)
	groups = adminMigrationUniqueSortedStrings(groups)
	permission := authz.Permission{Actors: actors, Groups: groups}
	return authz.ACL{Create: permission, Read: permission, Update: permission, Delete: permission, Grant: permission}
}

// adminMigrationScaleFixtureKey creates stable key metadata for users and
// clients; all fixture actors intentionally reuse the same non-secret public key.
func adminMigrationScaleFixtureKey(principalType, orgName, principalName, keyName string) bootstrap.KeyRecord {
	uri := "/" + principalType + "s/" + principalName + "/keys/" + keyName
	if principalType == "client" {
		uri = "/organizations/" + orgName + "/clients/" + principalName + "/keys/" + keyName
	}
	return bootstrap.KeyRecord{
		Name:           keyName,
		URI:            uri,
		PublicKeyPEM:   adminMigrationScaleFixturePublicKeyPEM,
		ExpirationDate: adminMigrationScaleFixtureExpirationDate,
		Expired:        false,
	}
}

// adminMigrationScaleFixtureEnvironment returns Chef-shaped environment data
// with stable cookbook constraints and attributes for diff normalization tests.
func adminMigrationScaleFixtureEnvironment(name string, constraints map[string]string) bootstrap.Environment {
	return bootstrap.Environment{
		Name:               name,
		Description:        "production-scale fixture environment " + name,
		CookbookVersions:   constraints,
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		DefaultAttributes:  map[string]any{"fixture": map[string]any{"environment": name}},
		OverrideAttributes: map[string]any{},
	}
}

// adminMigrationScaleFixtureNode returns searchable node data with policy
// fields so OpenSearch validation can exercise indexed compatibility fields.
func adminMigrationScaleFixtureNode(orgName, nodeName string, index int, spec adminMigrationScaleFixtureProfileSpec) bootstrap.Node {
	envName := "_default"
	if spec.EnvironmentsPerOrg > 0 && index%2 == 0 {
		envName = adminMigrationScaleFixtureName(orgName, "env", ((index-1)%spec.EnvironmentsPerOrg)+1)
	}
	return bootstrap.Node{
		Name:            nodeName,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: envName,
		Override:        map[string]any{},
		Normal:          map[string]any{"fixture": map[string]any{"node_index": index, "org": orgName}},
		Default:         map[string]any{},
		Automatic:       map[string]any{"fqdn": nodeName + ".example.invalid"},
		RunList:         []string{"recipe[scale_app::default]", "role[" + adminMigrationScaleFixtureName(orgName, "role", ((index-1)%spec.RolesPerOrg)+1) + "]"},
		PolicyName:      adminMigrationScaleFixtureName(orgName, "policy", ((index-1)%spec.PoliciesPerOrg)+1),
		PolicyGroup:     adminMigrationScaleFixtureName(orgName, "policy-group", ((index-1)%spec.PolicyGroupsPerOrg)+1),
	}
}

// adminMigrationScaleFixtureRole returns normalized role data with both top
// level and environment-specific run lists for depsolver/shadow-read coverage.
func adminMigrationScaleFixtureRole(name string, index int) bootstrap.Role {
	return bootstrap.Role{
		Name:               name,
		Description:        "production-scale fixture role " + name,
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"fixture_role": name},
		OverrideAttributes: map[string]any{},
		RunList:            []string{"recipe[scale_app::default]"},
		EnvRunLists: map[string][]string{
			"_default": {"recipe[scale_app::default]"},
			"prod":     {fmt.Sprintf("recipe[scale_app::role_%02d]", index)},
		},
	}
}

// adminMigrationScaleFixtureDataBagItem returns normal and encrypted-looking
// data bag payloads while leaving all cryptographic semantics to clients.
func adminMigrationScaleFixtureDataBagItem(bagName, itemID string, index int) bootstrap.DataBagItem {
	raw := map[string]any{"id": itemID, "bag": bagName, "fixture_index": index}
	if bagName == "encrypted_secrets" {
		raw["password"] = map[string]any{
			"encrypted_data": "ciphertext-" + itemID,
			"iv":             adminMigrationScaleFixtureHex(itemID+"-iv", 16),
			"version":        3,
			"cipher":         "aes-256-gcm",
		}
	}
	return bootstrap.DataBagItem{ID: itemID, RawData: raw}
}

// adminMigrationScaleFixturePolicyRevision creates a canonical policyfile-like
// payload with nested cookbook lock and solution dependency metadata.
func adminMigrationScaleFixturePolicyRevision(name, revisionID string) bootstrap.PolicyRevision {
	payload := map[string]any{
		"name":        name,
		"revision_id": revisionID,
		"run_list":    []any{"recipe[scale_app::default]"},
		"cookbook_locks": map[string]any{
			"scale_app": map[string]any{
				"version":                   "1.0.0",
				"identifier":                revisionID[:20],
				"dotted_decimal_identifier": "123.456.789",
			},
		},
		"solution_dependencies": map[string]any{
			"Policyfile": []any{"scale_app >= 1.0.0"},
			"dependencies": map[string]any{
				"scale_app (1.0.0)": []any{},
			},
		},
	}
	return bootstrap.PolicyRevision{Name: name, RevisionID: revisionID, Payload: payload}
}

// adminMigrationScaleFixtureCookbookFile creates Chef cookbook file metadata
// for a checksum-addressed blob path.
func adminMigrationScaleFixtureCookbookFile(path, checksum string) bootstrap.CookbookFile {
	return bootstrap.CookbookFile{Name: path, Path: path, Checksum: checksum, Specificity: "default"}
}

// adminMigrationScaleFixtureBlob stores deterministic bytes and returns the
// Chef checksum that addresses those bytes in cookbook and sandbox metadata.
func adminMigrationScaleFixtureBlob(blobs map[string][]byte, label string) string {
	body := []byte(adminMigrationScaleFixtureSeed + ":" + label + "\n")
	sum := md5.Sum(body)
	checksum := hex.EncodeToString(sum[:])
	blobs[checksum] = body
	return checksum
}

// adminMigrationScaleFixtureHex derives deterministic hex identifiers for
// GUIDs, policy revisions, cookbook artifact identifiers, and encrypted fields.
func adminMigrationScaleFixtureHex(label string, length int) string {
	sum := sha256.Sum256([]byte(adminMigrationScaleFixtureSeed + ":" + label))
	encoded := hex.EncodeToString(sum[:])
	if length > len(encoded) {
		length = len(encoded)
	}
	return encoded[:length]
}

// adminMigrationScaleFixtureName builds stable Chef-safe object names that
// remain unique within the generated organization.
func adminMigrationScaleFixtureName(orgName, family string, index int) string {
	return fmt.Sprintf("%s-%s-%02d", orgName, family, index)
}

// adminMigrationScaleFixtureDataBagName reserves the first bag for
// encrypted-looking data so every profile exercises opaque secret payloads.
func adminMigrationScaleFixtureDataBagName(index int) string {
	if index == 1 {
		return "encrypted_secrets"
	}
	return fmt.Sprintf("scale_bag_%02d", index)
}

// adminMigrationScaleFixtureTime returns stable timestamps for sandbox rows so
// JSON round-trips are reproducible across test runs.
func adminMigrationScaleFixtureTime(index int) time.Time {
	return time.Date(2026, 4, 29, 12, index, 0, 0, time.UTC)
}

// adminMigrationScaleFixtureExpectedCounts returns the generated inventory as a
// nested map, which keeps tests and later report builders independent of order.
func adminMigrationScaleFixtureExpectedCounts(fixture adminMigrationScaleFixture) map[adminMigrationSourcePayloadKey]int {
	out := map[adminMigrationSourcePayloadKey]int{}
	for _, family := range fixture.Inventory.Families {
		out[adminMigrationSourcePayloadKey{Organization: family.Organization, Family: family.Family}] = family.Count
	}
	return out
}

// adminMigrationScaleFixtureSortedChecksums returns every copied checksum in
// stable order for blob-reference and shared-checksum assertions.
func adminMigrationScaleFixtureSortedChecksums(fixture adminMigrationScaleFixture) []string {
	checksums := make([]string, 0, len(fixture.BlobCopies))
	for _, blob := range fixture.BlobCopies {
		checksums = append(checksums, blob.Checksum)
	}
	sort.Strings(checksums)
	return checksums
}

// adminMigrationScaleFixtureSourceBundle materializes a generated fixture into
// the normalized source-import bundle contract used by the functional harness.
func adminMigrationScaleFixtureSourceBundle(profile string, includeSidecars bool) (adminMigrationScaleFixture, adminMigrationSourceNormalizeBundle, error) {
	fixture, err := adminMigrationProductionScaleFixture(profile)
	if err != nil {
		return adminMigrationScaleFixture{}, adminMigrationSourceNormalizeBundle{}, err
	}
	bundle, err := adminMigrationScaleFixtureNormalizeBundle(fixture, includeSidecars)
	if err != nil {
		return adminMigrationScaleFixture{}, adminMigrationSourceNormalizeBundle{}, err
	}
	return fixture, bundle, nil
}

// adminMigrationScaleFixtureNormalizeBundle converts generated PostgreSQL-like
// state into hash-pinned source payload files plus copied checksum blob bytes.
func adminMigrationScaleFixtureNormalizeBundle(fixture adminMigrationScaleFixture, includeSidecars bool) (adminMigrationSourceNormalizeBundle, error) {
	files := map[string][]byte{}
	payloadValues, err := adminMigrationScaleFixtureSourcePayloadValuesForBundle(fixture)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{}, err
	}
	blobChecksums := map[string]struct{}{}
	for _, blobCopy := range fixture.BlobCopies {
		blobChecksums[blobCopy.Checksum] = struct{}{}
		files[path.Join("blobs", "checksums", blobCopy.Checksum)] = append([]byte(nil), blobCopy.Body...)
	}
	searchCount := 0
	unsupportedCounts := map[string]int{}
	if includeSidecars {
		searchCount = 1
		unsupportedCounts["oc_id"] = 1
		files["derived/opensearch/chef/node/scale-fixture.json"] = []byte(`{"derived":true}` + "\n")
		files["unsupported/oc_id/actors.json"] = []byte(`{"unsupported":true}` + "\n")
	}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(payloadValues, files)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{}, err
	}
	artifacts := adminMigrationSourceArtifactsFromSideChannels(blobChecksums, searchCount, unsupportedCounts)
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    "production_scale_fixture",
		Payloads:      payloads,
		Artifacts:     artifacts,
		Notes:         []string{"Generated by opencook admin migration scale-fixture create."},
	}
	return adminMigrationSourceNormalizeBundle{
		Manifest:      manifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(manifest),
		SourceType:    manifest.SourceType,
		FormatVersion: manifest.FormatVersion,
	}, nil
}

// adminMigrationScaleFixtureSourcePayloadValuesForBundle converts fixture state
// into source rows, including relation-table families needed by import/sync.
func adminMigrationScaleFixtureSourcePayloadValuesForBundle(fixture adminMigrationScaleFixture) (map[adminMigrationSourcePayloadKey][]json.RawMessage, error) {
	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	var firstErr error
	add := func(key adminMigrationSourcePayloadKey, value any) {
		if firstErr != nil {
			return
		}
		raw, err := adminMigrationScaleFixtureSourceRaw(value)
		if err != nil {
			firstErr = err
			return
		}
		payloadValues[key] = append(payloadValues[key], raw)
	}

	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.Users) {
		add(adminMigrationSourcePayloadKey{Family: "users"}, fixture.Bootstrap.Users[username])
	}
	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserACLs) {
		add(adminMigrationSourcePayloadKey{Family: "user_acls"}, adminMigrationScaleFixtureSourceACLRecord("user:"+username, fixture.Bootstrap.UserACLs[username]))
	}
	for _, username := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserKeys) {
		for _, keyName := range adminMigrationSortedMapKeys(fixture.Bootstrap.UserKeys[username]) {
			add(adminMigrationSourcePayloadKey{Family: "user_keys"}, adminMigrationScaleFixtureSourceKeyRecord("username", username, fixture.Bootstrap.UserKeys[username][keyName]))
		}
	}
	add(adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, map[string]string{"type": "user", "actor": adminMigrationScaleFixtureSuperuser})

	for _, orgName := range adminMigrationSortedMapKeys(fixture.Bootstrap.Orgs) {
		bootstrapOrg := fixture.Bootstrap.Orgs[orgName]
		coreOrg := fixture.CoreObjects.Orgs[orgName]
		cookbookOrg := fixture.Cookbooks.Orgs[orgName]
		add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "organizations"}, bootstrapOrg.Organization)
		for _, clientName := range adminMigrationSortedMapKeys(bootstrapOrg.Clients) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "clients"}, bootstrapOrg.Clients[clientName])
		}
		for _, clientName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys) {
			for _, keyName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys[clientName]) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "client_keys"}, adminMigrationScaleFixtureSourceKeyRecord("client", clientName, bootstrapOrg.ClientKeys[clientName][keyName]))
			}
		}
		sourceGroups := adminMigrationScaleFixtureSourceGroups(bootstrapOrg)
		for _, groupName := range adminMigrationSortedMapKeys(sourceGroups) {
			group := sourceGroups[groupName]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "groups"}, group)
			for _, member := range adminMigrationSourceSyncGroupMembershipRecords(group) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "group_memberships"}, member)
			}
		}
		for _, containerName := range adminMigrationSortedMapKeys(bootstrapOrg.Containers) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "containers"}, bootstrapOrg.Containers[containerName])
		}
		for _, aclKey := range adminMigrationSortedMapKeys(bootstrapOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, adminMigrationScaleFixtureSourceACLRecord(adminMigrationScaleFixtureSourceACLResource(orgName, aclKey), bootstrapOrg.ACLs[aclKey]))
		}
		for _, envName := range adminMigrationSortedMapKeys(coreOrg.Environments) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "environments"}, coreOrg.Environments[envName])
		}
		for _, nodeName := range adminMigrationSortedMapKeys(coreOrg.Nodes) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "nodes"}, coreOrg.Nodes[nodeName])
		}
		for _, roleName := range adminMigrationSortedMapKeys(coreOrg.Roles) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "roles"}, coreOrg.Roles[roleName])
		}
		for _, bagName := range adminMigrationSortedMapKeys(coreOrg.DataBags) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bags"}, coreOrg.DataBags[bagName])
		}
		for _, bagName := range adminMigrationSortedMapKeys(coreOrg.DataBagItems) {
			for _, itemID := range adminMigrationSortedMapKeys(coreOrg.DataBagItems[bagName]) {
				item := coreOrg.DataBagItems[bagName][itemID]
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bag_items"}, map[string]any{"bag": bagName, "id": item.ID, "payload": item.RawData})
			}
		}
		for _, policyName := range adminMigrationSortedMapKeys(coreOrg.Policies) {
			for _, revisionID := range adminMigrationSortedMapKeys(coreOrg.Policies[policyName]) {
				revision := coreOrg.Policies[policyName][revisionID]
				payload := adminMigrationSourceImportCloneMap(revision.Payload)
				payload["name"] = revision.Name
				payload["revision_id"] = revision.RevisionID
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_revisions"}, payload)
			}
		}
		for _, groupName := range adminMigrationSortedMapKeys(coreOrg.PolicyGroups) {
			group := coreOrg.PolicyGroups[groupName]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_groups"}, map[string]any{"name": group.Name, "policies": group.Policies})
			for _, policyName := range adminMigrationSortedMapKeys(group.Policies) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_assignments"}, map[string]string{"group": groupName, "policy": policyName, "revision_id": group.Policies[policyName]})
			}
		}
		checksumRefs := map[string]map[string]string{}
		for _, sandboxID := range adminMigrationSortedMapKeys(coreOrg.Sandboxes) {
			sandbox := coreOrg.Sandboxes[sandboxID]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "sandboxes"}, map[string]any{"sandbox_id": sandbox.ID, "checksums": sandbox.Checksums, "completed": true})
			for _, checksum := range sandbox.Checksums {
				checksumRefs["sandboxes/"+checksum] = map[string]string{"family": "sandboxes", "checksum": checksum}
			}
		}
		for _, aclKey := range adminMigrationSortedMapKeys(coreOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, adminMigrationScaleFixtureSourceACLRecord(adminMigrationScaleFixtureSourceACLResource(orgName, aclKey), coreOrg.ACLs[aclKey]))
		}
		for _, version := range cookbookOrg.Versions {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_versions"}, version)
			for _, checksum := range adminMigrationCookbookFileChecksums(version.AllFiles) {
				checksumRefs["cookbook_versions/"+checksum] = map[string]string{"family": "cookbook_versions", "checksum": checksum}
			}
		}
		for _, artifact := range cookbookOrg.Artifacts {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_artifacts"}, artifact)
			for _, checksum := range adminMigrationCookbookFileChecksums(artifact.AllFiles) {
				checksumRefs["cookbook_artifacts/"+checksum] = map[string]string{"family": "cookbook_artifacts", "checksum": checksum}
			}
		}
		for _, refID := range adminMigrationSortedMapKeys(checksumRefs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}, checksumRefs[refID])
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return payloadValues, nil
}

// adminMigrationScaleFixtureSourceRaw marshals rows through JSON so generated
// functional fixtures obey the same payload constraints as source normalizer IO.
func adminMigrationScaleFixtureSourceRaw(value any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// adminMigrationScaleFixtureSourceKeyRecord exposes key records with owner
// fields expected by the normalized user_key and client_key source families.
func adminMigrationScaleFixtureSourceKeyRecord(ownerField, owner string, record bootstrap.KeyRecord) map[string]string {
	return map[string]string{
		ownerField:        owner,
		"key_name":        record.Name,
		"public_key":      record.PublicKeyPEM,
		"expiration_date": record.ExpirationDate,
	}
}

// adminMigrationScaleFixtureSourceGroups makes nested group placeholders
// explicit because source import reads group and membership rows separately.
func adminMigrationScaleFixtureSourceGroups(org bootstrap.BootstrapCoreOrganizationState) map[string]bootstrap.Group {
	groups := make(map[string]bootstrap.Group, len(org.Groups))
	for name, group := range org.Groups {
		groups[name] = group
	}
	for _, group := range org.Groups {
		for _, nested := range group.Groups {
			if _, ok := groups[nested]; ok {
				continue
			}
			groups[nested] = bootstrap.Group{Name: nested, GroupName: nested, Organization: org.Organization.Name}
		}
	}
	return groups
}

// adminMigrationScaleFixtureSourceACLRecord stores ACL documents with explicit
// resource identifiers so one org-scoped ACL source family can carry all ACLs.
func adminMigrationScaleFixtureSourceACLRecord(resource string, acl authz.ACL) map[string]any {
	return map[string]any{
		"resource": resource,
		"create":   adminMigrationSourceSyncCanonicalPermission(acl.Create),
		"read":     adminMigrationSourceSyncCanonicalPermission(acl.Read),
		"update":   adminMigrationSourceSyncCanonicalPermission(acl.Update),
		"delete":   adminMigrationSourceSyncCanonicalPermission(acl.Delete),
		"grant":    adminMigrationSourceSyncCanonicalPermission(acl.Grant),
	}
}

// adminMigrationScaleFixtureSourceACLResource rewrites internal ACL keys back
// into source-import resource identifiers.
func adminMigrationScaleFixtureSourceACLResource(orgName, aclKey string) string {
	if aclKey == adminMigrationOrganizationACLKey() {
		return "organization:" + orgName
	}
	return aclKey
}
