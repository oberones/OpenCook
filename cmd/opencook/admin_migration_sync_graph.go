package main

import (
	"fmt"
)

// Definitions and relationship payloads have separate coverage. For sync an
// explicitly empty relationship payload overrides any inline definition data.
func adminMigrationSourceSyncStateFromRead(read adminMigrationSourceImportRead) (adminMigrationSourceImportState, error) {
	state, err := adminMigrationSourceImportStateFromRead(read, true)
	if err != nil {
		return state, err
	}
	for key := range adminMigrationSourceSyncCoveredScopes(read) {
		switch key.Family {
		case "group_memberships":
			adminMigrationEnsureSourceSyncOrg(&state, key.Organization)
			org := state.Bootstrap.Orgs[key.Organization]
			for name, group := range org.Groups {
				group.Users = nil
				group.Clients = nil
				group.Groups = nil
				group.Actors = nil
				org.Groups[name] = group
			}
			for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, key.Organization, key.Family) {
				name := adminMigrationSourceString(object, "group", "groupname")
				group := org.Groups[name]
				org.Groups[name] = adminMigrationSourceImportAddGroupMember(group, adminMigrationSourceStringDefault(object, "user", "type", "actor_type"), adminMigrationSourceString(object, "actor", "name"))
			}
			state.Bootstrap.Orgs[key.Organization] = org
		case "policy_assignments":
			adminMigrationEnsureSourceSyncOrg(&state, key.Organization)
			org := state.CoreObjects.Orgs[key.Organization]
			for name, group := range org.PolicyGroups {
				group.Policies = map[string]string{}
				org.PolicyGroups[name] = group
			}
			for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, key.Organization, key.Family) {
				name := adminMigrationSourceString(object, "group", "policy_group")
				group := org.PolicyGroups[name]
				if group.Policies == nil {
					group.Policies = map[string]string{}
				}
				group.Policies[adminMigrationSourceString(object, "policy", "policy_name")] = adminMigrationSourceString(object, "revision_id", "revision")
				org.PolicyGroups[name] = group
			}
			state.CoreObjects.Orgs[key.Organization] = org
		}
	}
	// Relationship reconstruction must finish before explicit global admins are
	// added, otherwise an unchanged sync erases their imported memberships.
	for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, "", "server_admin_memberships") {
		if adminMigrationSourceStringDefault(object, "user", "type", "actor_type") != "user" {
			continue
		}
		for name, org := range state.Bootstrap.Orgs {
			if group, ok := org.Groups["admins"]; ok {
				org.Groups["admins"] = adminMigrationSourceImportAddGroupMember(group, "user", adminMigrationSourceString(object, "actor", "username", "name"))
				state.Bootstrap.Orgs[name] = org
			}
		}
	}
	return state, nil
}

// Parent deletions also delete attached relationships, so include those in the
// reported diff even when the relationship scope itself was omitted.
func adminMigrationSourceSyncDiffScopes(scopes map[adminMigrationSourcePayloadKey]bool) map[adminMigrationSourcePayloadKey]bool {
	out := map[adminMigrationSourcePayloadKey]bool{}
	for key, covered := range scopes {
		out[key] = covered
		switch key.Family {
		case "groups":
			key.Family = "group_memberships"
			out[key] = covered
		case "policy_groups":
			key.Family = "policy_assignments"
			out[key] = covered
		}
	}
	return out
}

func adminMigrationValidateSyncGraph(state, source adminMigrationSourceImportState, scopes map[adminMigrationSourcePayloadKey]bool) error {
	invalid := func(family string) error {
		return fmt.Errorf("final merged %s graph references a missing parent or subject", family)
	}
	for _, orgName := range adminMigrationSortedMapKeys(state.Bootstrap.Orgs) {
		org := state.Bootstrap.Orgs[orgName]
		if scopes[adminMigrationSourcePayloadKey{Organization: orgName, Family: "group_memberships"}] {
			for name, group := range source.Bootstrap.Orgs[orgName].Groups {
				if len(group.Users)+len(group.Clients)+len(group.Groups)+len(group.Actors) > 0 {
					if _, ok := org.Groups[name]; !ok {
						return invalid("group_memberships")
					}
				}
			}
		}
		for _, name := range adminMigrationSortedMapKeys(org.Groups) {
			group := org.Groups[name]
			for _, actor := range group.Users {
				if _, ok := state.Bootstrap.Users[actor]; !ok {
					return invalid("group_memberships")
				}
			}
			for _, actor := range group.Clients {
				if _, ok := org.Clients[actor]; !ok {
					return invalid("group_memberships")
				}
			}
			for _, child := range group.Groups {
				if _, ok := org.Groups[child]; !ok {
					return invalid("group_memberships")
				}
			}
		}
	}
	for _, orgName := range adminMigrationSortedMapKeys(state.CoreObjects.Orgs) {
		org := state.CoreObjects.Orgs[orgName]
		if scopes[adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_assignments"}] {
			for name, group := range source.CoreObjects.Orgs[orgName].PolicyGroups {
				if len(group.Policies) > 0 {
					if _, ok := org.PolicyGroups[name]; !ok {
						return invalid("policy_assignments")
					}
				}
			}
		}
		for _, name := range adminMigrationSortedMapKeys(org.PolicyGroups) {
			for policy, revision := range org.PolicyGroups[name].Policies {
				if _, ok := org.Policies[policy][revision]; !ok {
					return invalid("policy_assignments")
				}
			}
		}
	}
	return nil
}

// A partial relationship bundle is usable for sync, but initial import has no
// retained target parents against which deferred references can be resolved.
func adminMigrationValidateImportedRelationships(read adminMigrationSourceImportRead, state adminMigrationSourceImportState) error {
	for key := range read.PayloadValues {
		switch key.Family {
		case "group_memberships":
			for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, key.Organization, key.Family) {
				if _, ok := state.Bootstrap.Orgs[key.Organization].Groups[adminMigrationSourceString(object, "group", "groupname")]; !ok {
					return fmt.Errorf("source membership references a missing group")
				}
			}
		case "policy_assignments":
			definitions := map[string]bool{}
			for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, key.Organization, "policy_groups") {
				name := adminMigrationSourceString(object, "name", "group")
				if name != "" {
					definitions[name] = true
				}
			}
			for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, key.Organization, key.Family) {
				if !definitions[adminMigrationSourceString(object, "group", "policy_group")] {
					return fmt.Errorf("source assignment references a missing policy group")
				}
			}
		}
	}
	return adminMigrationValidateSyncGraph(state, state, nil)
}
