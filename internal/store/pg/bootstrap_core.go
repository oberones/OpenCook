package pg

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

//go:embed schema/0002_bootstrap_core_persistence.sql
var bootstrapCorePersistenceSchemaSQL string

type BootstrapCoreRepository struct {
	store *Store
	db    *sql.DB
	state bootstrap.BootstrapCoreState
}

func newBootstrapCoreRepository(store *Store) *BootstrapCoreRepository {
	return &BootstrapCoreRepository{
		store: store,
		state: bootstrap.BootstrapCoreState{},
	}
}

func (s *Store) BootstrapCore() *BootstrapCoreRepository {
	if s == nil {
		return nil
	}
	return s.bootstrapCore
}

func (r *BootstrapCoreRepository) Migrations() []Migration {
	if r == nil {
		return nil
	}

	return []Migration{
		{
			Name: "0002_bootstrap_core_persistence.sql",
			SQL:  bootstrapCorePersistenceSchemaSQL,
		},
	}
}

func (r *BootstrapCoreRepository) LoadBootstrapCore() (bootstrap.BootstrapCoreState, error) {
	if r == nil {
		return bootstrap.BootstrapCoreState{}, nil
	}
	return bootstrap.CloneBootstrapCoreState(r.state), nil
}

func (r *BootstrapCoreRepository) SaveBootstrapCore(state bootstrap.BootstrapCoreState) error {
	if r == nil {
		return nil
	}
	if r.db == nil {
		r.state = bootstrap.CloneBootstrapCoreState(state)
		return nil
	}
	if err := saveBootstrapCore(context.Background(), r.db, state); err != nil {
		return err
	}
	r.state = bootstrap.CloneBootstrapCoreState(state)
	return nil
}

func (r *BootstrapCoreRepository) activate(ctx context.Context, db *sql.DB) error {
	if r == nil {
		return fmt.Errorf("bootstrap core repository is required")
	}

	for _, migration := range r.Migrations() {
		if _, err := db.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("apply %s: %w", migration.Name, err)
		}
	}

	state, err := loadBootstrapCore(ctx, db)
	if err != nil {
		return err
	}

	r.db = db
	r.state = state
	return nil
}

func saveBootstrapCore(ctx context.Context, db *sql.DB, state bootstrap.BootstrapCoreState) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bootstrap core transaction: %w", err)
	}
	defer tx.Rollback()

	for _, table := range []string{
		"oc_bootstrap_client_keys",
		"oc_bootstrap_clients",
		"oc_bootstrap_group_memberships",
		"oc_bootstrap_groups",
		"oc_bootstrap_containers",
		"oc_bootstrap_org_acls",
		"oc_bootstrap_orgs",
		"oc_bootstrap_user_keys",
		"oc_bootstrap_user_acls",
		"oc_bootstrap_users",
	} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}

	for _, username := range sortedMapKeys(state.Users) {
		user := state.Users[username]
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_users (username, display_name, email, first_name, last_name)
VALUES ($1, $2, $3, $4, $5)`,
			user.Username, user.DisplayName, user.Email, user.FirstName, user.LastName); err != nil {
			return fmt.Errorf("insert bootstrap user %s: %w", username, err)
		}
	}
	for _, username := range sortedMapKeys(state.UserACLs) {
		aclJSON, err := json.Marshal(state.UserACLs[username])
		if err != nil {
			return fmt.Errorf("marshal user ACL %s: %w", username, err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_user_acls (username, acl_json)
VALUES ($1, $2)`, username, aclJSON); err != nil {
			return fmt.Errorf("insert bootstrap user ACL %s: %w", username, err)
		}
	}
	for _, username := range sortedMapKeys(state.UserKeys) {
		for _, keyName := range sortedMapKeys(state.UserKeys[username]) {
			key := state.UserKeys[username][keyName]
			if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_user_keys (username, key_name, uri, public_key_pem, expiration_date, expires_at)
VALUES ($1, $2, $3, $4, $5, $6)`,
				username, key.Name, key.URI, key.PublicKeyPEM, key.ExpirationDate, nullableTime(key.ExpiresAt)); err != nil {
				return fmt.Errorf("insert bootstrap user key %s/%s: %w", username, keyName, err)
			}
		}
	}

	for _, orgName := range sortedMapKeys(state.Orgs) {
		orgState := state.Orgs[orgName]
		org := orgState.Organization
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_orgs (org_name, full_name, org_type, guid)
VALUES ($1, $2, $3, $4)`,
			org.Name, org.FullName, org.OrgType, org.GUID); err != nil {
			return fmt.Errorf("insert bootstrap org %s: %w", orgName, err)
		}

		for _, clientName := range sortedMapKeys(orgState.Clients) {
			client := orgState.Clients[clientName]
			if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_clients (org_name, client_name, name, validator, admin, public_key_pem, uri)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
				org.Name, client.ClientName, client.Name, client.Validator, client.Admin, client.PublicKey, client.URI); err != nil {
				return fmt.Errorf("insert bootstrap client %s/%s: %w", orgName, clientName, err)
			}
		}
		for _, clientName := range sortedMapKeys(orgState.ClientKeys) {
			for _, keyName := range sortedMapKeys(orgState.ClientKeys[clientName]) {
				key := orgState.ClientKeys[clientName][keyName]
				if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_client_keys (org_name, client_name, key_name, uri, public_key_pem, expiration_date, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
					org.Name, clientName, key.Name, key.URI, key.PublicKeyPEM, key.ExpirationDate, nullableTime(key.ExpiresAt)); err != nil {
					return fmt.Errorf("insert bootstrap client key %s/%s/%s: %w", orgName, clientName, keyName, err)
				}
			}
		}
		for _, groupName := range sortedMapKeys(orgState.Groups) {
			group := orgState.Groups[groupName]
			if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_groups (org_name, group_name, name)
VALUES ($1, $2, $3)`, org.Name, group.GroupName, group.Name); err != nil {
				return fmt.Errorf("insert bootstrap group %s/%s: %w", orgName, groupName, err)
			}
			if err := insertGroupMemberships(ctx, tx, org.Name, group.GroupName, "actor", group.Actors); err != nil {
				return err
			}
			if err := insertGroupMemberships(ctx, tx, org.Name, group.GroupName, "user", group.Users); err != nil {
				return err
			}
			if err := insertGroupMemberships(ctx, tx, org.Name, group.GroupName, "client", group.Clients); err != nil {
				return err
			}
			if err := insertGroupMemberships(ctx, tx, org.Name, group.GroupName, "group", group.Groups); err != nil {
				return err
			}
		}
		for _, containerName := range sortedMapKeys(orgState.Containers) {
			container := orgState.Containers[containerName]
			if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_containers (org_name, container_name, name, container_path)
VALUES ($1, $2, $3, $4)`, org.Name, container.ContainerName, container.Name, container.ContainerPath); err != nil {
				return fmt.Errorf("insert bootstrap container %s/%s: %w", orgName, containerName, err)
			}
		}
		for _, aclKey := range sortedMapKeys(orgState.ACLs) {
			aclJSON, err := json.Marshal(orgState.ACLs[aclKey])
			if err != nil {
				return fmt.Errorf("marshal org ACL %s/%s: %w", orgName, aclKey, err)
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_org_acls (org_name, acl_key, acl_json)
VALUES ($1, $2, $3)`, org.Name, aclKey, aclJSON); err != nil {
				return fmt.Errorf("insert bootstrap org ACL %s/%s: %w", orgName, aclKey, err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bootstrap core transaction: %w", err)
	}
	return nil
}

func insertGroupMemberships(ctx context.Context, tx *sql.Tx, orgName, groupName, memberType string, members []string) error {
	for idx, member := range members {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_bootstrap_group_memberships (org_name, group_name, member_type, ordinal, member_name)
VALUES ($1, $2, $3, $4, $5)`, orgName, groupName, memberType, idx, member); err != nil {
			return fmt.Errorf("insert bootstrap group membership %s/%s/%s/%d: %w", orgName, groupName, memberType, idx, err)
		}
	}
	return nil
}

func loadBootstrapCore(ctx context.Context, db *sql.DB) (bootstrap.BootstrapCoreState, error) {
	state := bootstrap.BootstrapCoreState{
		Users:    make(map[string]bootstrap.User),
		UserACLs: make(map[string]authz.ACL),
		UserKeys: make(map[string]map[string]bootstrap.KeyRecord),
		Orgs:     make(map[string]bootstrap.BootstrapCoreOrganizationState),
	}

	if err := loadBootstrapUsers(ctx, db, state.Users); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapUserACLs(ctx, db, state.UserACLs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapUserKeys(ctx, db, state.UserKeys); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapOrganizations(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapClients(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapClientKeys(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapGroups(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapGroupMemberships(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapContainers(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}
	if err := loadBootstrapOrgACLs(ctx, db, state.Orgs); err != nil {
		return bootstrap.BootstrapCoreState{}, err
	}

	return state, nil
}

func loadBootstrapUsers(ctx context.Context, db *sql.DB, users map[string]bootstrap.User) error {
	rows, err := db.QueryContext(ctx, `
SELECT username, display_name, email, first_name, last_name
FROM oc_bootstrap_users
ORDER BY username`)
	if err != nil {
		return fmt.Errorf("load bootstrap users: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var user bootstrap.User
		if err := rows.Scan(&user.Username, &user.DisplayName, &user.Email, &user.FirstName, &user.LastName); err != nil {
			return fmt.Errorf("scan bootstrap user: %w", err)
		}
		users[user.Username] = user
	}
	return rows.Err()
}

func loadBootstrapUserACLs(ctx context.Context, db *sql.DB, acls map[string]authz.ACL) error {
	rows, err := db.QueryContext(ctx, `
SELECT username, acl_json
FROM oc_bootstrap_user_acls
ORDER BY username`)
	if err != nil {
		return fmt.Errorf("load bootstrap user ACLs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username string
		var raw []byte
		if err := rows.Scan(&username, &raw); err != nil {
			return fmt.Errorf("scan bootstrap user ACL: %w", err)
		}
		var acl authz.ACL
		if err := json.Unmarshal(raw, &acl); err != nil {
			return fmt.Errorf("unmarshal bootstrap user ACL %s: %w", username, err)
		}
		acls[username] = acl
	}
	return rows.Err()
}

func loadBootstrapUserKeys(ctx context.Context, db *sql.DB, keys map[string]map[string]bootstrap.KeyRecord) error {
	rows, err := db.QueryContext(ctx, `
SELECT username, key_name, uri, public_key_pem, expiration_date, expires_at
FROM oc_bootstrap_user_keys
ORDER BY username, key_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap user keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var username string
		record, err := scanKeyRecord(rows)
		if err != nil {
			return fmt.Errorf("scan bootstrap user key: %w", err)
		}
		username = record.scope
		if _, ok := keys[username]; !ok {
			keys[username] = make(map[string]bootstrap.KeyRecord)
		}
		keys[username][record.key.Name] = record.key
	}
	return rows.Err()
}

func loadBootstrapOrganizations(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, full_name, org_type, guid
FROM oc_bootstrap_orgs
ORDER BY org_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap orgs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var org bootstrap.Organization
		if err := rows.Scan(&org.Name, &org.FullName, &org.OrgType, &org.GUID); err != nil {
			return fmt.Errorf("scan bootstrap org: %w", err)
		}
		orgs[org.Name] = bootstrap.BootstrapCoreOrganizationState{
			Organization: org,
			Clients:      make(map[string]bootstrap.Client),
			ClientKeys:   make(map[string]map[string]bootstrap.KeyRecord),
			Groups:       make(map[string]bootstrap.Group),
			Containers:   make(map[string]bootstrap.Container),
			ACLs:         make(map[string]authz.ACL),
		}
	}
	return rows.Err()
}

func loadBootstrapClients(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, client_name, name, validator, admin, public_key_pem, uri
FROM oc_bootstrap_clients
ORDER BY org_name, client_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap clients: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orgName string
		var client bootstrap.Client
		if err := rows.Scan(&orgName, &client.ClientName, &client.Name, &client.Validator, &client.Admin, &client.PublicKey, &client.URI); err != nil {
			return fmt.Errorf("scan bootstrap client: %w", err)
		}
		client.Organization = orgName
		orgState := orgs[orgName]
		orgState.Clients[client.ClientName] = client
		orgs[orgName] = orgState
	}
	return rows.Err()
}

func loadBootstrapClientKeys(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, client_name, key_name, uri, public_key_pem, expiration_date, expires_at
FROM oc_bootstrap_client_keys
ORDER BY org_name, client_name, key_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap client keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		record, err := scanClientKeyRecord(rows)
		if err != nil {
			return fmt.Errorf("scan bootstrap client key: %w", err)
		}
		orgState := orgs[record.orgName]
		if _, ok := orgState.ClientKeys[record.clientName]; !ok {
			orgState.ClientKeys[record.clientName] = make(map[string]bootstrap.KeyRecord)
		}
		orgState.ClientKeys[record.clientName][record.key.Name] = record.key
		orgs[record.orgName] = orgState
	}
	return rows.Err()
}

func loadBootstrapGroups(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, group_name, name
FROM oc_bootstrap_groups
ORDER BY org_name, group_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap groups: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orgName string
		group := bootstrap.Group{
			Actors:  []string{},
			Users:   []string{},
			Clients: []string{},
			Groups:  []string{},
		}
		if err := rows.Scan(&orgName, &group.GroupName, &group.Name); err != nil {
			return fmt.Errorf("scan bootstrap group: %w", err)
		}
		group.Organization = orgName
		orgState := orgs[orgName]
		orgState.Groups[group.GroupName] = group
		orgs[orgName] = orgState
	}
	return rows.Err()
}

func loadBootstrapGroupMemberships(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, group_name, member_type, member_name
FROM oc_bootstrap_group_memberships
ORDER BY org_name, group_name, member_type, ordinal`)
	if err != nil {
		return fmt.Errorf("load bootstrap group memberships: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orgName, groupName, memberType, memberName string
		if err := rows.Scan(&orgName, &groupName, &memberType, &memberName); err != nil {
			return fmt.Errorf("scan bootstrap group membership: %w", err)
		}
		orgState := orgs[orgName]
		group := orgState.Groups[groupName]
		switch memberType {
		case "actor":
			group.Actors = append(group.Actors, memberName)
		case "user":
			group.Users = append(group.Users, memberName)
		case "client":
			group.Clients = append(group.Clients, memberName)
		case "group":
			group.Groups = append(group.Groups, memberName)
		default:
			return fmt.Errorf("unknown bootstrap group member type %q", memberType)
		}
		orgState.Groups[groupName] = group
		orgs[orgName] = orgState
	}
	return rows.Err()
}

func loadBootstrapContainers(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, container_name, name, container_path
FROM oc_bootstrap_containers
ORDER BY org_name, container_name`)
	if err != nil {
		return fmt.Errorf("load bootstrap containers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orgName string
		var container bootstrap.Container
		if err := rows.Scan(&orgName, &container.ContainerName, &container.Name, &container.ContainerPath); err != nil {
			return fmt.Errorf("scan bootstrap container: %w", err)
		}
		orgState := orgs[orgName]
		orgState.Containers[container.ContainerName] = container
		orgs[orgName] = orgState
	}
	return rows.Err()
}

func loadBootstrapOrgACLs(ctx context.Context, db *sql.DB, orgs map[string]bootstrap.BootstrapCoreOrganizationState) error {
	rows, err := db.QueryContext(ctx, `
SELECT org_name, acl_key, acl_json
FROM oc_bootstrap_org_acls
ORDER BY org_name, acl_key`)
	if err != nil {
		return fmt.Errorf("load bootstrap org ACLs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var orgName, aclKey string
		var raw []byte
		if err := rows.Scan(&orgName, &aclKey, &raw); err != nil {
			return fmt.Errorf("scan bootstrap org ACL: %w", err)
		}
		var acl authz.ACL
		if err := json.Unmarshal(raw, &acl); err != nil {
			return fmt.Errorf("unmarshal bootstrap org ACL %s/%s: %w", orgName, aclKey, err)
		}
		orgState := orgs[orgName]
		orgState.ACLs[aclKey] = acl
		orgs[orgName] = orgState
	}
	return rows.Err()
}

type userKeyRow struct {
	scope string
	key   bootstrap.KeyRecord
}

func scanKeyRecord(rows interface {
	Scan(dest ...any) error
}) (userKeyRow, error) {
	var row userKeyRow
	var expiresAt sql.NullTime
	if err := rows.Scan(&row.scope, &row.key.Name, &row.key.URI, &row.key.PublicKeyPEM, &row.key.ExpirationDate, &expiresAt); err != nil {
		return userKeyRow{}, err
	}
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		row.key.ExpiresAt = &t
	}
	row.key.Expired = isExpired(row.key.ExpiresAt)
	return row, nil
}

type clientKeyRow struct {
	orgName    string
	clientName string
	key        bootstrap.KeyRecord
}

func scanClientKeyRecord(rows interface {
	Scan(dest ...any) error
}) (clientKeyRow, error) {
	var row clientKeyRow
	var expiresAt sql.NullTime
	if err := rows.Scan(&row.orgName, &row.clientName, &row.key.Name, &row.key.URI, &row.key.PublicKeyPEM, &row.key.ExpirationDate, &expiresAt); err != nil {
		return clientKeyRow{}, err
	}
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		row.key.ExpiresAt = &t
	}
	row.key.Expired = isExpired(row.key.ExpiresAt)
	return row, nil
}

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC()
}

func isExpired(t *time.Time) bool {
	return t != nil && !t.After(time.Now().UTC())
}

func sortedMapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
