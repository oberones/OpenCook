package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

const adminMigrationLiveSourceGlobalOrganizationID = "00000000000000000000000000000000"

// adminMigrationLiveSourceAuthorizationRef is the Erchef-side identity for a
// Bifrost authorization record. Bifrost deliberately has no organization or
// object-name context, so all human-readable resolution stays in Go.
type adminMigrationLiveSourceAuthorizationRef struct {
	RecordType   string
	Organization string
	AuthzID      string
	Resource     string
	SubjectType  string
	Name         string
	SubjectOnly  bool
}

type adminMigrationLiveSourceAuthorizationCatalog struct {
	byTypeOrgAndID map[string]adminMigrationLiveSourceAuthorizationRef
	byTypeAndID    map[string]adminMigrationLiveSourceAuthorizationRef
}

type adminMigrationLiveSourceAuthorizationIntegrityError struct {
	Code         string
	RecordType   string
	Organization string
	AuthzID      string
	Relationship string
}

func (e adminMigrationLiveSourceAuthorizationIntegrityError) Error() string {
	return fmt.Sprintf("authorization integrity failure: resource_type=%s organization=%s authz_id=%s relationship=%s",
		adminMigrationLiveSourceSafeDetail(e.RecordType),
		adminMigrationLiveSourceSafeDetail(e.Organization),
		adminMigrationLiveSourceSafeDetail(e.AuthzID),
		adminMigrationLiveSourceSafeDetail(e.Relationship))
}

func adminMigrationLiveSourceAuthorizationKey(recordType, authzID string) string {
	return strings.TrimSpace(recordType) + "|" + strings.TrimSpace(authzID)
}

func adminMigrationLiveSourceAuthorizationScopeKey(recordType, organization, authzID string) string {
	return strings.TrimSpace(recordType) + "|" + strings.TrimSpace(organization) + "|" + strings.TrimSpace(authzID)
}

func adminMigrationLiveSourceReadAuthorizationCatalog(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig) (adminMigrationLiveSourceAuthorizationCatalog, error) {
	rows, err := tx.Query(ctx, adminMigrationLiveSourceErchefAuthorizationCatalogQuery, strings.TrimSpace(cfg.Organization))
	if err != nil {
		return adminMigrationLiveSourceAuthorizationCatalog{}, err
	}
	defer rows.Close()

	catalog := adminMigrationLiveSourceAuthorizationCatalog{
		byTypeOrgAndID: map[string]adminMigrationLiveSourceAuthorizationRef{},
		byTypeAndID:    map[string]adminMigrationLiveSourceAuthorizationRef{},
	}
	for rows.Next() {
		var ref adminMigrationLiveSourceAuthorizationRef
		if err := rows.Scan(&ref.RecordType, &ref.Organization, &ref.AuthzID, &ref.Resource, &ref.SubjectType, &ref.Name); err != nil {
			return adminMigrationLiveSourceAuthorizationCatalog{}, err
		}
		ref.RecordType = strings.TrimSpace(ref.RecordType)
		ref.Organization = strings.TrimSpace(ref.Organization)
		ref.AuthzID = strings.TrimSpace(ref.AuthzID)
		ref.Resource = strings.TrimSpace(ref.Resource)
		ref.SubjectType = strings.TrimSpace(ref.SubjectType)
		ref.Name = strings.TrimSpace(ref.Name)
		if err := catalog.add(ref); err != nil {
			return adminMigrationLiveSourceAuthorizationCatalog{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return adminMigrationLiveSourceAuthorizationCatalog{}, err
	}
	rows.Close()

	globalRows, err := tx.Query(ctx, `
SELECT btrim(authz_id::text), name
FROM groups
WHERE org_id = $1
ORDER BY name, btrim(authz_id::text)`, adminMigrationLiveSourceGlobalOrganizationID)
	if err != nil {
		return adminMigrationLiveSourceAuthorizationCatalog{}, err
	}
	defer globalRows.Close()
	for globalRows.Next() {
		var authzID, name string
		if err := globalRows.Scan(&authzID, &name); err != nil {
			return adminMigrationLiveSourceAuthorizationCatalog{}, err
		}
		name = adminMigrationLiveSourceGlobalGroupSubjectName(name)
		ref := adminMigrationLiveSourceAuthorizationRef{
			RecordType:  "group",
			AuthzID:     strings.TrimSpace(authzID),
			Resource:    "group:" + name,
			SubjectType: "group",
			Name:        name,
			SubjectOnly: true,
		}
		if err := catalog.add(ref); err != nil {
			return adminMigrationLiveSourceAuthorizationCatalog{}, err
		}
	}
	if err := globalRows.Err(); err != nil {
		return adminMigrationLiveSourceAuthorizationCatalog{}, err
	}
	return catalog, nil
}

func (c *adminMigrationLiveSourceAuthorizationCatalog) add(ref adminMigrationLiveSourceAuthorizationRef) error {
	if c.byTypeOrgAndID == nil {
		c.byTypeOrgAndID = map[string]adminMigrationLiveSourceAuthorizationRef{}
	}
	if c.byTypeAndID == nil {
		c.byTypeAndID = map[string]adminMigrationLiveSourceAuthorizationRef{}
	}
	scopeKey := adminMigrationLiveSourceAuthorizationScopeKey(ref.RecordType, ref.Organization, ref.AuthzID)
	if existing, ok := c.byTypeOrgAndID[scopeKey]; ok && existing.Resource != ref.Resource {
		return adminMigrationLiveSourceAuthorizationIntegrityError{
			Code:         adminMigrationFindingSourceAuthorizationTargetUnresolved,
			RecordType:   ref.RecordType,
			Organization: ref.Organization,
			AuthzID:      ref.AuthzID,
			Relationship: "duplicate_erchef_authorization_target",
		}
	}
	c.byTypeOrgAndID[scopeKey] = ref
	key := adminMigrationLiveSourceAuthorizationKey(ref.RecordType, ref.AuthzID)
	if existing, ok := c.byTypeAndID[key]; ok && (existing.Organization != ref.Organization || existing.Resource != ref.Resource) {
		return adminMigrationLiveSourceAuthorizationIntegrityError{
			Code:         adminMigrationFindingSourceAuthorizationTargetUnresolved,
			RecordType:   ref.RecordType,
			Organization: ref.Organization,
			AuthzID:      ref.AuthzID,
			Relationship: "duplicate_erchef_authorization_target",
		}
	}
	c.byTypeAndID[key] = ref
	return nil
}

func adminMigrationLiveSourceGlobalGroupSubjectName(name string) string {
	name = strings.TrimSpace(name)
	if strings.HasPrefix(name, "::") {
		return name
	}
	return "::" + name
}

func (c adminMigrationLiveSourceAuthorizationCatalog) refs(recordType string) []adminMigrationLiveSourceAuthorizationRef {
	refs := make([]adminMigrationLiveSourceAuthorizationRef, 0)
	for _, ref := range c.byTypeAndID {
		if ref.RecordType == recordType && !ref.SubjectOnly {
			refs = append(refs, ref)
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Organization != refs[j].Organization {
			return refs[i].Organization < refs[j].Organization
		}
		if refs[i].Resource != refs[j].Resource {
			return refs[i].Resource < refs[j].Resource
		}
		return refs[i].AuthzID < refs[j].AuthzID
	})
	return refs
}

func (c adminMigrationLiveSourceAuthorizationCatalog) allIDs(recordType string) []string {
	ids := make([]string, 0)
	for _, ref := range c.byTypeAndID {
		if ref.RecordType == recordType {
			ids = append(ids, ref.AuthzID)
		}
	}
	return adminMigrationUniqueSortedStrings(ids)
}

func (c adminMigrationLiveSourceAuthorizationCatalog) subject(recordType, name string) (adminMigrationLiveSourceAuthorizationRef, bool) {
	for _, ref := range c.byTypeAndID {
		if ref.RecordType == recordType && ref.SubjectOnly && ref.Name == name {
			return ref, true
		}
	}
	return adminMigrationLiveSourceAuthorizationRef{}, false
}

func (c adminMigrationLiveSourceAuthorizationCatalog) ids(recordType string) []string {
	refs := c.refs(recordType)
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		ids = append(ids, ref.AuthzID)
	}
	return adminMigrationUniqueSortedStrings(ids)
}

func (c adminMigrationLiveSourceAuthorizationCatalog) resolve(recordType, authzID, organization, relationship string) (adminMigrationLiveSourceAuthorizationRef, error) {
	ref, ok := c.byTypeAndID[adminMigrationLiveSourceAuthorizationKey(recordType, authzID)]
	if !ok || (strings.TrimSpace(organization) != "" && ref.Organization != "" && ref.Organization != strings.TrimSpace(organization)) {
		return adminMigrationLiveSourceAuthorizationRef{}, adminMigrationLiveSourceAuthorizationIntegrityError{
			Code:         adminMigrationFindingSourceAuthorizationSubjectUnresolved,
			RecordType:   recordType,
			Organization: organization,
			AuthzID:      strings.TrimSpace(authzID),
			Relationship: relationship,
		}
	}
	return ref, nil
}

type adminMigrationLiveSourceBifrostACLSpec struct {
	recordType    string
	authTable     string
	actorACLTable string
	groupACLTable string
}

func adminMigrationLiveSourceBifrostACLSpecs() []adminMigrationLiveSourceBifrostACLSpec {
	return []adminMigrationLiveSourceBifrostACLSpec{
		{recordType: "actor", authTable: "auth_actor", actorACLTable: "actor_acl_actor", groupACLTable: "actor_acl_group"},
		{recordType: "group", authTable: "auth_group", actorACLTable: "group_acl_actor", groupACLTable: "group_acl_group"},
		{recordType: "container", authTable: "auth_container", actorACLTable: "container_acl_actor", groupACLTable: "container_acl_group"},
		{recordType: "object", authTable: "auth_object", actorACLTable: "object_acl_actor", groupACLTable: "object_acl_group"},
	}
}

func adminMigrationLiveSourceReadBifrostAuthorization(ctx context.Context, tx pgx.Tx, catalog adminMigrationLiveSourceAuthorizationCatalog, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) (int, error) {
	aclRows := make([]adminMigrationLiveSourceACLRow, 0)
	for _, spec := range adminMigrationLiveSourceBifrostACLSpecs() {
		ids := catalog.ids(spec.recordType)
		if len(ids) == 0 {
			continue
		}
		found, err := adminMigrationLiveSourceReadBifrostTargetIDs(ctx, tx, spec.authTable, ids)
		if err != nil {
			return 0, err
		}
		if err := adminMigrationLiveSourceValidateBifrostTargets(catalog, spec.recordType, found); err != nil {
			return 0, err
		}
		bifrostRows, err := adminMigrationLiveSourceReadBifrostACLRows(ctx, tx, spec, ids)
		if err != nil {
			return 0, err
		}
		for _, row := range bifrostRows {
			target, ok := catalog.byTypeAndID[adminMigrationLiveSourceAuthorizationKey(spec.recordType, row.TargetAuthzID)]
			if !ok {
				return 0, adminMigrationLiveSourceAuthorizationIntegrityError{
					Code:         adminMigrationFindingSourceAuthorizationTargetUnresolved,
					RecordType:   spec.recordType,
					AuthzID:      row.TargetAuthzID,
					Relationship: "acl_target",
				}
			}
			subject, err := catalog.resolve(row.AuthorizeeType, row.AuthorizeeAuthzID, target.Organization, "acl_subject")
			if err != nil {
				return 0, err
			}
			aclRows = append(aclRows, adminMigrationLiveSourceACLRow{
				OrgName:        target.Organization,
				Resource:       target.Resource,
				Permission:     row.Permission,
				AuthorizeeType: row.AuthorizeeType,
				Authorizee:     subject.Name,
			})
		}
	}

	for _, acl := range adminMigrationLiveSourceACLObjects(aclRows) {
		orgName := adminMigrationSourceString(acl, "orgname")
		delete(acl, "orgname")
		key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}
		if orgName == "" {
			key = adminMigrationSourcePayloadKey{Family: "user_acls"}
		}
		adminMigrationLiveSourceAppendObject(payloadValues, key, acl)
	}

	if err := adminMigrationLiveSourceReadBifrostServerAdminMemberships(ctx, tx, catalog, payloadValues); err != nil {
		return 0, err
	}
	if err := adminMigrationLiveSourceReadBifrostGroupMemberships(ctx, tx, catalog, payloadValues); err != nil {
		return 0, err
	}
	return adminMigrationLiveSourceCountUnrelatedBifrostRecords(ctx, tx, catalog)
}

func adminMigrationLiveSourceReadBifrostServerAdminMemberships(ctx context.Context, tx pgx.Tx, catalog adminMigrationLiveSourceAuthorizationCatalog, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	serverAdmins, ok := catalog.subject("group", "::server-admins")
	if !ok {
		return nil
	}
	found, err := adminMigrationLiveSourceReadBifrostTargetIDs(ctx, tx, "auth_group", []string{serverAdmins.AuthzID})
	if err != nil {
		return err
	}
	if _, ok := found[serverAdmins.AuthzID]; !ok {
		return adminMigrationLiveSourceAuthorizationIntegrityError{
			Code:         adminMigrationFindingSourceAuthorizationTargetUnresolved,
			RecordType:   "group",
			AuthzID:      serverAdmins.AuthzID,
			Relationship: "server_admin_group",
		}
	}
	rows, err := tx.Query(ctx, `
SELECT btrim(child.authz_id::text)
FROM auth_group parent
JOIN group_actor_relations relation ON relation.parent = parent.id
JOIN auth_actor child ON child.id = relation.child
WHERE btrim(parent.authz_id::text) = $1
ORDER BY btrim(child.authz_id::text)`, serverAdmins.AuthzID)
	if err != nil {
		return err
	}
	defer rows.Close()
	payloadValues[adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}] = []json.RawMessage{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var actorAuthzID string
		if err := rows.Scan(&actorAuthzID); err != nil {
			return err
		}
		actor, err := catalog.resolve("actor", strings.TrimSpace(actorAuthzID), "", "server_admin_membership")
		if err != nil {
			return err
		}
		if actor.SubjectType != "user" {
			return adminMigrationLiveSourceAuthorizationIntegrityError{
				Code:         adminMigrationFindingSourceAuthorizationSubjectUnresolved,
				RecordType:   "actor",
				AuthzID:      strings.TrimSpace(actorAuthzID),
				Relationship: "server_admin_membership",
			}
		}
		if _, ok := seen[actor.AuthzID]; ok {
			continue
		}
		seen[actor.AuthzID] = struct{}{}
		adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, map[string]any{
			"actor": actor.Name,
			"type":  "user",
		})
	}
	return rows.Err()
}

func adminMigrationLiveSourceValidateBifrostTargets(catalog adminMigrationLiveSourceAuthorizationCatalog, recordType string, found map[string]struct{}) error {
	for _, ref := range catalog.refs(recordType) {
		if _, ok := found[ref.AuthzID]; !ok {
			return adminMigrationLiveSourceAuthorizationIntegrityError{
				Code:         adminMigrationFindingSourceAuthorizationTargetUnresolved,
				RecordType:   ref.RecordType,
				Organization: ref.Organization,
				AuthzID:      ref.AuthzID,
				Relationship: "acl_target",
			}
		}
	}
	return nil
}

type adminMigrationLiveSourceBifrostACLRow struct {
	TargetAuthzID     string
	Permission        string
	AuthorizeeType    string
	AuthorizeeAuthzID string
}

type adminMigrationLiveSourceBifrostGroupMembershipRow struct {
	ParentAuthzID string
	ChildType     string
	ChildAuthzID  string
}

func adminMigrationLiveSourceReadBifrostTargetIDs(ctx context.Context, tx pgx.Tx, table string, ids []string) (map[string]struct{}, error) {
	query := fmt.Sprintf("SELECT btrim(authz_id::text) FROM %s WHERE btrim(authz_id::text) = ANY($1::text[]) ORDER BY btrim(authz_id::text)", table)
	rows, err := tx.Query(ctx, query, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := map[string]struct{}{}
	for rows.Next() {
		var authzID string
		if err := rows.Scan(&authzID); err != nil {
			return nil, err
		}
		found[strings.TrimSpace(authzID)] = struct{}{}
	}
	return found, rows.Err()
}

func adminMigrationLiveSourceReadBifrostACLRows(ctx context.Context, tx pgx.Tx, spec adminMigrationLiveSourceBifrostACLSpec, ids []string) ([]adminMigrationLiveSourceBifrostACLRow, error) {
	rows, err := tx.Query(ctx, adminMigrationLiveSourceBifrostACLQuery(spec), ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]adminMigrationLiveSourceBifrostACLRow, 0)
	for rows.Next() {
		var row adminMigrationLiveSourceBifrostACLRow
		if err := rows.Scan(&row.TargetAuthzID, &row.Permission, &row.AuthorizeeType, &row.AuthorizeeAuthzID); err != nil {
			return nil, err
		}
		row.TargetAuthzID = strings.TrimSpace(row.TargetAuthzID)
		row.AuthorizeeAuthzID = strings.TrimSpace(row.AuthorizeeAuthzID)
		out = append(out, row)
	}
	return out, rows.Err()
}

func adminMigrationLiveSourceBifrostACLQuery(spec adminMigrationLiveSourceBifrostACLSpec) string {
	return fmt.Sprintf(`
SELECT btrim(target.authz_id::text), acl.permission::text, 'actor', btrim(authorizee.authz_id::text)
FROM %s target
JOIN %s acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
WHERE btrim(target.authz_id::text) = ANY($1::text[])
UNION ALL
SELECT btrim(target.authz_id::text), acl.permission::text, 'group', btrim(authorizee.authz_id::text)
FROM %s target
JOIN %s acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
WHERE btrim(target.authz_id::text) = ANY($1::text[])
ORDER BY 1, 2, 3, 4`, spec.authTable, spec.actorACLTable, spec.authTable, spec.groupACLTable)
}

func adminMigrationLiveSourceReadBifrostGroupMemberships(ctx context.Context, tx pgx.Tx, catalog adminMigrationLiveSourceAuthorizationCatalog, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	groupIDs := catalog.ids("group")
	if len(groupIDs) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, adminMigrationLiveSourceBifrostGroupMembershipQuery, groupIDs)
	if err != nil {
		return err
	}
	defer rows.Close()
	memberships := make([]adminMigrationLiveSourceBifrostGroupMembershipRow, 0)
	for rows.Next() {
		var membership adminMigrationLiveSourceBifrostGroupMembershipRow
		if err := rows.Scan(&membership.ParentAuthzID, &membership.ChildType, &membership.ChildAuthzID); err != nil {
			return err
		}
		memberships = append(memberships, membership)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return adminMigrationLiveSourceAppendBifrostGroupMemberships(catalog, payloadValues, memberships)
}

func adminMigrationLiveSourceAppendBifrostGroupMemberships(catalog adminMigrationLiveSourceAuthorizationCatalog, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, memberships []adminMigrationLiveSourceBifrostGroupMembershipRow) error {
	seen := map[string]struct{}{}
	for _, membership := range memberships {
		parentAuthzID := strings.TrimSpace(membership.ParentAuthzID)
		childType := strings.TrimSpace(membership.ChildType)
		childAuthzID := strings.TrimSpace(membership.ChildAuthzID)
		parent, err := catalog.resolve("group", parentAuthzID, "", "group_membership_parent")
		if err != nil {
			return err
		}
		child, err := catalog.resolve(childType, childAuthzID, parent.Organization, "group_membership_child")
		if err != nil {
			return err
		}
		identity := parent.AuthzID + "|" + childType + "|" + child.AuthzID
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Organization: parent.Organization, Family: "group_memberships"}, map[string]any{
			"group": parent.Name,
			"type":  child.SubjectType,
			"actor": child.Name,
		})
	}
	return nil
}

func adminMigrationLiveSourceCountUnrelatedBifrostRecords(ctx context.Context, tx pgx.Tx, catalog adminMigrationLiveSourceAuthorizationCatalog) (int, error) {
	total := 0
	for _, spec := range adminMigrationLiveSourceBifrostACLSpecs() {
		var count int
		query := fmt.Sprintf("SELECT count(*) FROM %s WHERE NOT (btrim(authz_id::text) = ANY($1::text[]))", spec.authTable)
		if err := tx.QueryRow(ctx, query, catalog.allIDs(spec.recordType)).Scan(&count); err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

const adminMigrationLiveSourceErchefAuthorizationCatalogQuery = `
SELECT 'actor', ''::text, btrim(u.authz_id::text), 'user:' || u.username, 'user', u.username
FROM users u
UNION ALL
SELECT 'object', o.name, btrim(o.authz_id::text), 'organization:' || o.name, '', o.name
FROM orgs o
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'actor', o.name, btrim(c.authz_id::text), 'client:' || c.name, 'client', c.name
FROM clients c JOIN orgs o ON o.id = c.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'group', o.name, btrim(g.authz_id::text), 'group:' || g.name, 'group', g.name
FROM groups g JOIN orgs o ON o.id = g.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'container', o.name, btrim(c.authz_id::text), 'container:' || c.name, '', c.name
FROM containers c JOIN orgs o ON o.id = c.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(n.authz_id::text), 'node:' || n.name, '', n.name
FROM nodes n JOIN orgs o ON o.id = n.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(e.authz_id::text), 'environment:' || e.name, '', e.name
FROM environments e JOIN orgs o ON o.id = e.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(r.authz_id::text), 'role:' || r.name, '', r.name
FROM roles r JOIN orgs o ON o.id = r.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(d.authz_id::text), 'data_bag:' || d.name, '', d.name
FROM data_bags d JOIN orgs o ON o.id = d.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(p.authz_id::text), 'policy:' || p.name, '', p.name
FROM policies p JOIN orgs o ON o.id = p.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT 'object', o.name, btrim(pg.authz_id::text), 'policy_group:' || pg.name, '', pg.name
FROM policy_groups pg JOIN orgs o ON o.id = pg.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY 1, 2, 4, 3`

const adminMigrationLiveSourceBifrostGroupMembershipQuery = `
SELECT btrim(parent.authz_id::text), 'actor', btrim(child.authz_id::text)
FROM auth_group parent
JOIN group_actor_relations relation ON relation.parent = parent.id
JOIN auth_actor child ON child.id = relation.child
WHERE btrim(parent.authz_id::text) = ANY($1::text[])
UNION ALL
SELECT btrim(parent.authz_id::text), 'group', btrim(child.authz_id::text)
FROM auth_group parent
JOIN group_group_relations relation ON relation.parent = parent.id
JOIN auth_group child ON child.id = relation.child
WHERE btrim(parent.authz_id::text) = ANY($1::text[])
ORDER BY 1, 2, 3`
