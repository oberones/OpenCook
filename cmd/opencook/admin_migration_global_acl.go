package main

// Global grants must become actors because the target has no global groups.
func adminMigrationExpandGlobalACL(catalog adminMigrationLiveSourceAuthorizationCatalog, graph map[string][]adminMigrationLiveSourceBifrostGroupMembershipRow, target adminMigrationLiveSourceAuthorizationRef, row adminMigrationLiveSourceBifrostACLRow) ([]adminMigrationLiveSourceACLRow, error) {
	subject, err := catalog.resolve(row.AuthorizeeType, row.AuthorizeeAuthzID, target.Organization, "acl_subject")
	if err != nil {
		return nil, err
	}
	if row.AuthorizeeType == "group" && subject.SubjectOnly {
		actors, err := adminMigrationEffectiveGroupActors(catalog, graph, subject.AuthzID, target.Organization)
		if err != nil {
			return nil, err
		}
		rows := make([]adminMigrationLiveSourceACLRow, 0, len(actors))
		for _, actor := range actors {
			rows = append(rows, adminMigrationLiveSourceACLRow{OrgName: target.Organization, Resource: target.Resource, Permission: row.Permission, AuthorizeeType: "actor", Authorizee: actor.Name})
		}
		return rows, nil
	}
	return []adminMigrationLiveSourceACLRow{{OrgName: target.Organization, Resource: target.Resource, Permission: row.Permission, AuthorizeeType: row.AuthorizeeType, Authorizee: subject.Name}}, nil
}

// Traverse identities, not names, so cycles and repeated paths cannot grant
// twice. A client can only be represented inside its own organization's ACL.
func adminMigrationEffectiveGroupActors(catalog adminMigrationLiveSourceAuthorizationCatalog, graph map[string][]adminMigrationLiveSourceBifrostGroupMembershipRow, groupID, organization string) ([]adminMigrationLiveSourceAuthorizationRef, error) {
	seen := map[string]bool{}
	actors := map[string]adminMigrationLiveSourceAuthorizationRef{}
	pending := []string{groupID}
	for len(pending) > 0 {
		id := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		if _, err := catalog.resolve("group", id, organization, "global_group_membership"); err != nil {
			return nil, err
		}
		children, ok := graph[id]
		if !ok {
			return nil, adminMigrationLiveSourceAuthorizationIntegrityError{Code: adminMigrationFindingSourceAuthorizationTargetUnresolved, RecordType: "group", Organization: organization, AuthzID: id, Relationship: "global_group_membership"}
		}
		for _, child := range children {
			ref, err := catalog.resolve(child.ChildType, child.ChildAuthzID, organization, "global_group_membership")
			if err != nil {
				return nil, err
			}
			if child.ChildType == "group" {
				pending = append(pending, ref.AuthzID)
				continue
			}
			if ref.SubjectType != "user" && (ref.SubjectType != "client" || organization == "" || ref.Organization != organization) {
				return nil, adminMigrationLiveSourceAuthorizationIntegrityError{Code: adminMigrationFindingSourceAuthorizationSubjectUnresolved, RecordType: child.ChildType, Organization: organization, AuthzID: ref.AuthzID, Relationship: "global_group_membership"}
			}
			actors[ref.AuthzID] = ref
		}
	}
	out := make([]adminMigrationLiveSourceAuthorizationRef, 0, len(actors))
	for _, id := range adminMigrationSortedMapKeys(actors) {
		out = append(out, actors[id])
	}
	return out, nil
}
