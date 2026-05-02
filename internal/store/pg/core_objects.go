package pg

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

//go:embed schema/0003_core_object_persistence.sql
var coreObjectPersistenceSchemaSQL string

type CoreObjectRepository struct {
	store *Store
	mu    sync.RWMutex
	db    *sql.DB
	state bootstrap.CoreObjectState
}

type CoreObjectJSONRecord struct {
	Organization string
	Name         string
	PayloadJSON  []byte
}

type CoreDataBagItemRecord struct {
	Organization string
	BagName      string
	ItemID       string
	PayloadJSON  []byte
}

type CorePolicyRevisionRecord struct {
	Organization string
	PolicyName   string
	RevisionID   string
	PayloadJSON  []byte
}

type CoreSandboxRecord struct {
	Organization string
	ID           string
	CreatedAt    time.Time
}

type CoreSandboxChecksumRecord struct {
	Organization string
	SandboxID    string
	Ordinal      int
	Checksum     string
}

type CoreObjectACLRecord struct {
	Organization string
	ACLKey       string
	ACLJSON      []byte
}

type CoreObjectRows struct {
	Environments     []CoreObjectJSONRecord
	Nodes            []CoreObjectJSONRecord
	Roles            []CoreObjectJSONRecord
	DataBags         []CoreObjectJSONRecord
	DataBagItems     []CoreDataBagItemRecord
	PolicyRevisions  []CorePolicyRevisionRecord
	PolicyGroups     []CoreObjectJSONRecord
	Sandboxes        []CoreSandboxRecord
	SandboxChecksums []CoreSandboxChecksumRecord
	ACLs             []CoreObjectACLRecord
}

func newCoreObjectRepository(store *Store) *CoreObjectRepository {
	return &CoreObjectRepository{
		store: store,
		state: bootstrap.CoreObjectState{},
	}
}

func (s *Store) CoreObjects() *CoreObjectRepository {
	if s == nil {
		return nil
	}
	return s.coreObjects
}

func (r *CoreObjectRepository) Migrations() []Migration {
	if r == nil {
		return nil
	}

	return []Migration{
		{
			Name: "0003_core_object_persistence.sql",
			SQL:  coreObjectPersistenceSchemaSQL,
		},
	}
}

func (r *CoreObjectRepository) LoadCoreObjects() (bootstrap.CoreObjectState, error) {
	if r == nil {
		return bootstrap.CoreObjectState{}, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	return bootstrap.CloneCoreObjectState(r.state), nil
}

func (r *CoreObjectRepository) SaveCoreObjects(state bootstrap.CoreObjectState) error {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()
	if db == nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.state = bootstrap.CloneCoreObjectState(state)
		return nil
	}
	if err := saveCoreObjects(context.Background(), db, state); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = bootstrap.CloneCoreObjectState(state)
	return nil
}

// Reload refreshes the repository snapshot from PostgreSQL without indexing or
// write-path side effects. Live services can use it before reloading their
// in-memory core object maps after a controlled direct repair.
func (r *CoreObjectRepository) Reload(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.RLock()
	db := r.db
	r.mu.RUnlock()
	if db == nil {
		return nil
	}
	state, err := loadCoreObjects(ctx, db)
	if err != nil {
		return fmt.Errorf("load core object state: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = bootstrap.CloneCoreObjectState(state)
	return nil
}

func (r *CoreObjectRepository) activate(ctx context.Context, db *sql.DB) error {
	if r == nil {
		return fmt.Errorf("core object repository is required")
	}

	for _, migration := range r.Migrations() {
		if _, err := db.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("apply %s: %w", migration.Name, err)
		}
	}

	state, err := loadCoreObjects(ctx, db)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.db = db
	r.state = bootstrap.CloneCoreObjectState(state)
	return nil
}

func (r *CoreObjectRepository) EncodeCoreObjects(state bootstrap.CoreObjectState) (CoreObjectRows, error) {
	var rows CoreObjectRows
	for _, orgName := range sortedMapKeys(state.Orgs) {
		org := state.Orgs[orgName]
		for _, name := range sortedMapKeys(org.Environments) {
			payload, err := json.Marshal(org.Environments[name])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal environment %s/%s: %w", orgName, name, err)
			}
			rows.Environments = append(rows.Environments, CoreObjectJSONRecord{Organization: orgName, Name: name, PayloadJSON: payload})
		}
		for _, name := range sortedMapKeys(org.Nodes) {
			payload, err := json.Marshal(org.Nodes[name])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal node %s/%s: %w", orgName, name, err)
			}
			rows.Nodes = append(rows.Nodes, CoreObjectJSONRecord{Organization: orgName, Name: name, PayloadJSON: payload})
		}
		for _, name := range sortedMapKeys(org.Roles) {
			payload, err := json.Marshal(org.Roles[name])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal role %s/%s: %w", orgName, name, err)
			}
			rows.Roles = append(rows.Roles, CoreObjectJSONRecord{Organization: orgName, Name: name, PayloadJSON: payload})
		}
		for _, name := range sortedMapKeys(org.DataBags) {
			payload, err := json.Marshal(org.DataBags[name])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal data bag %s/%s: %w", orgName, name, err)
			}
			rows.DataBags = append(rows.DataBags, CoreObjectJSONRecord{Organization: orgName, Name: name, PayloadJSON: payload})
		}
		for _, bagName := range sortedMapKeys(org.DataBagItems) {
			for _, itemID := range sortedMapKeys(org.DataBagItems[bagName]) {
				payload, err := json.Marshal(org.DataBagItems[bagName][itemID])
				if err != nil {
					return CoreObjectRows{}, fmt.Errorf("marshal data bag item %s/%s/%s: %w", orgName, bagName, itemID, err)
				}
				rows.DataBagItems = append(rows.DataBagItems, CoreDataBagItemRecord{
					Organization: orgName,
					BagName:      bagName,
					ItemID:       itemID,
					PayloadJSON:  payload,
				})
			}
		}
		for _, policyName := range sortedMapKeys(org.Policies) {
			for _, revisionID := range sortedMapKeys(org.Policies[policyName]) {
				payload, err := json.Marshal(org.Policies[policyName][revisionID].Payload)
				if err != nil {
					return CoreObjectRows{}, fmt.Errorf("marshal policy revision %s/%s/%s: %w", orgName, policyName, revisionID, err)
				}
				rows.PolicyRevisions = append(rows.PolicyRevisions, CorePolicyRevisionRecord{
					Organization: orgName,
					PolicyName:   policyName,
					RevisionID:   revisionID,
					PayloadJSON:  payload,
				})
			}
		}
		for _, name := range sortedMapKeys(org.PolicyGroups) {
			payload, err := json.Marshal(org.PolicyGroups[name])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal policy group %s/%s: %w", orgName, name, err)
			}
			rows.PolicyGroups = append(rows.PolicyGroups, CoreObjectJSONRecord{Organization: orgName, Name: name, PayloadJSON: payload})
		}
		for _, id := range sortedMapKeys(org.Sandboxes) {
			sandbox := org.Sandboxes[id]
			rows.Sandboxes = append(rows.Sandboxes, CoreSandboxRecord{
				Organization: orgName,
				ID:           sandbox.ID,
				CreatedAt:    sandbox.CreatedAt.UTC(),
			})
			for idx, checksum := range sandbox.Checksums {
				rows.SandboxChecksums = append(rows.SandboxChecksums, CoreSandboxChecksumRecord{
					Organization: orgName,
					SandboxID:    sandbox.ID,
					Ordinal:      idx,
					Checksum:     checksum,
				})
			}
		}
		for _, aclKey := range sortedMapKeys(org.ACLs) {
			payload, err := json.Marshal(org.ACLs[aclKey])
			if err != nil {
				return CoreObjectRows{}, fmt.Errorf("marshal object ACL %s/%s: %w", orgName, aclKey, err)
			}
			rows.ACLs = append(rows.ACLs, CoreObjectACLRecord{Organization: orgName, ACLKey: aclKey, ACLJSON: payload})
		}
	}
	return rows, nil
}

func (r *CoreObjectRepository) DecodeCoreObjects(rows CoreObjectRows) (bootstrap.CoreObjectState, error) {
	state := bootstrap.CoreObjectState{
		Orgs: make(map[string]bootstrap.CoreObjectOrganizationState),
	}

	for _, row := range rows.Environments {
		org := ensureCoreObjectOrg(state, row.Organization)
		var env bootstrap.Environment
		if err := json.Unmarshal(row.PayloadJSON, &env); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal environment %s/%s: %w", row.Organization, row.Name, err)
		}
		org.Environments[row.Name] = env
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.Nodes {
		org := ensureCoreObjectOrg(state, row.Organization)
		var node bootstrap.Node
		if err := json.Unmarshal(row.PayloadJSON, &node); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal node %s/%s: %w", row.Organization, row.Name, err)
		}
		org.Nodes[row.Name] = node
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.Roles {
		org := ensureCoreObjectOrg(state, row.Organization)
		var role bootstrap.Role
		if err := json.Unmarshal(row.PayloadJSON, &role); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal role %s/%s: %w", row.Organization, row.Name, err)
		}
		org.Roles[row.Name] = role
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.DataBags {
		org := ensureCoreObjectOrg(state, row.Organization)
		var bag bootstrap.DataBag
		if err := json.Unmarshal(row.PayloadJSON, &bag); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal data bag %s/%s: %w", row.Organization, row.Name, err)
		}
		org.DataBags[row.Name] = bag
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.DataBagItems {
		org := ensureCoreObjectOrg(state, row.Organization)
		if org.DataBagItems[row.BagName] == nil {
			org.DataBagItems[row.BagName] = make(map[string]bootstrap.DataBagItem)
		}
		var item bootstrap.DataBagItem
		if err := json.Unmarshal(row.PayloadJSON, &item); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal data bag item %s/%s/%s: %w", row.Organization, row.BagName, row.ItemID, err)
		}
		org.DataBagItems[row.BagName][row.ItemID] = item
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.PolicyRevisions {
		org := ensureCoreObjectOrg(state, row.Organization)
		if org.Policies[row.PolicyName] == nil {
			org.Policies[row.PolicyName] = make(map[string]bootstrap.PolicyRevision)
		}
		var payload map[string]any
		if err := json.Unmarshal(row.PayloadJSON, &payload); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal policy revision %s/%s/%s: %w", row.Organization, row.PolicyName, row.RevisionID, err)
		}
		org.Policies[row.PolicyName][row.RevisionID] = bootstrap.PolicyRevision{
			Name:       row.PolicyName,
			RevisionID: row.RevisionID,
			Payload:    payload,
		}
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.PolicyGroups {
		org := ensureCoreObjectOrg(state, row.Organization)
		var group bootstrap.PolicyGroup
		if err := json.Unmarshal(row.PayloadJSON, &group); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal policy group %s/%s: %w", row.Organization, row.Name, err)
		}
		org.PolicyGroups[row.Name] = group
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.Sandboxes {
		org := ensureCoreObjectOrg(state, row.Organization)
		org.Sandboxes[row.ID] = bootstrap.Sandbox{
			ID:           row.ID,
			Organization: row.Organization,
			CreatedAt:    row.CreatedAt.UTC(),
			Checksums:    []string{},
		}
		state.Orgs[row.Organization] = org
	}
	sort.Slice(rows.SandboxChecksums, func(i, j int) bool {
		if rows.SandboxChecksums[i].Organization != rows.SandboxChecksums[j].Organization {
			return rows.SandboxChecksums[i].Organization < rows.SandboxChecksums[j].Organization
		}
		if rows.SandboxChecksums[i].SandboxID != rows.SandboxChecksums[j].SandboxID {
			return rows.SandboxChecksums[i].SandboxID < rows.SandboxChecksums[j].SandboxID
		}
		return rows.SandboxChecksums[i].Ordinal < rows.SandboxChecksums[j].Ordinal
	})
	for _, row := range rows.SandboxChecksums {
		org := ensureCoreObjectOrg(state, row.Organization)
		sandbox := org.Sandboxes[row.SandboxID]
		if sandbox.ID == "" {
			sandbox = bootstrap.Sandbox{
				ID:           row.SandboxID,
				Organization: row.Organization,
				Checksums:    []string{},
			}
		}
		sandbox.Checksums = append(sandbox.Checksums, row.Checksum)
		org.Sandboxes[row.SandboxID] = sandbox
		state.Orgs[row.Organization] = org
	}
	for _, row := range rows.ACLs {
		org := ensureCoreObjectOrg(state, row.Organization)
		var acl authz.ACL
		if err := json.Unmarshal(row.ACLJSON, &acl); err != nil {
			return bootstrap.CoreObjectState{}, fmt.Errorf("unmarshal object ACL %s/%s: %w", row.Organization, row.ACLKey, err)
		}
		org.ACLs[row.ACLKey] = acl
		state.Orgs[row.Organization] = org
	}

	return state, nil
}

func saveCoreObjects(ctx context.Context, db *sql.DB, state bootstrap.CoreObjectState) error {
	repo := newCoreObjectRepository(nil)
	rows, err := repo.EncodeCoreObjects(state)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin core object transaction: %w", err)
	}
	defer tx.Rollback()

	for _, table := range []string{
		"oc_core_sandbox_checksums",
		"oc_core_data_bag_items",
		"oc_core_object_acls",
		"oc_core_sandboxes",
		"oc_core_policy_groups",
		"oc_core_policy_revisions",
		"oc_core_data_bags",
		"oc_core_roles",
		"oc_core_nodes",
		"oc_core_environments",
	} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}

	for _, row := range rows.Environments {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_environments (org_name, environment_name, payload_json)
VALUES ($1, $2, $3)`, row.Organization, row.Name, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core environment %s/%s: %w", row.Organization, row.Name, err)
		}
	}
	for _, row := range rows.Nodes {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_nodes (org_name, node_name, payload_json)
VALUES ($1, $2, $3)`, row.Organization, row.Name, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core node %s/%s: %w", row.Organization, row.Name, err)
		}
	}
	for _, row := range rows.Roles {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_roles (org_name, role_name, payload_json)
VALUES ($1, $2, $3)`, row.Organization, row.Name, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core role %s/%s: %w", row.Organization, row.Name, err)
		}
	}
	for _, row := range rows.DataBags {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_data_bags (org_name, bag_name, payload_json)
VALUES ($1, $2, $3)`, row.Organization, row.Name, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core data bag %s/%s: %w", row.Organization, row.Name, err)
		}
	}
	for _, row := range rows.DataBagItems {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_data_bag_items (org_name, bag_name, item_id, payload_json)
VALUES ($1, $2, $3, $4)`, row.Organization, row.BagName, row.ItemID, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core data bag item %s/%s/%s: %w", row.Organization, row.BagName, row.ItemID, err)
		}
	}
	for _, row := range rows.PolicyRevisions {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_policy_revisions (org_name, policy_name, revision_id, payload_json)
VALUES ($1, $2, $3, $4)`, row.Organization, row.PolicyName, row.RevisionID, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core policy revision %s/%s/%s: %w", row.Organization, row.PolicyName, row.RevisionID, err)
		}
	}
	for _, row := range rows.PolicyGroups {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_policy_groups (org_name, group_name, payload_json)
VALUES ($1, $2, $3)`, row.Organization, row.Name, row.PayloadJSON); err != nil {
			return fmt.Errorf("insert core policy group %s/%s: %w", row.Organization, row.Name, err)
		}
	}
	for _, row := range rows.Sandboxes {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_sandboxes (org_name, sandbox_id, created_at)
VALUES ($1, $2, $3)`, row.Organization, row.ID, row.CreatedAt); err != nil {
			return fmt.Errorf("insert core sandbox %s/%s: %w", row.Organization, row.ID, err)
		}
	}
	for _, row := range rows.SandboxChecksums {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_sandbox_checksums (org_name, sandbox_id, ordinal, checksum)
VALUES ($1, $2, $3, $4)`, row.Organization, row.SandboxID, row.Ordinal, row.Checksum); err != nil {
			return fmt.Errorf("insert core sandbox checksum %s/%s/%d: %w", row.Organization, row.SandboxID, row.Ordinal, err)
		}
	}
	for _, row := range rows.ACLs {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO oc_core_object_acls (org_name, acl_key, acl_json)
VALUES ($1, $2, $3)`, row.Organization, row.ACLKey, row.ACLJSON); err != nil {
			return fmt.Errorf("insert core object ACL %s/%s: %w", row.Organization, row.ACLKey, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit core object transaction: %w", err)
	}
	return nil
}

func loadCoreObjects(ctx context.Context, db *sql.DB) (bootstrap.CoreObjectState, error) {
	rows := CoreObjectRows{}
	var err error
	if rows.Environments, err = loadCoreJSONRows(ctx, db, "oc_core_environments", "environment_name"); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.Nodes, err = loadCoreJSONRows(ctx, db, "oc_core_nodes", "node_name"); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.Roles, err = loadCoreJSONRows(ctx, db, "oc_core_roles", "role_name"); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.DataBags, err = loadCoreJSONRows(ctx, db, "oc_core_data_bags", "bag_name"); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.DataBagItems, err = loadCoreDataBagItemRows(ctx, db); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.PolicyRevisions, err = loadCorePolicyRevisionRows(ctx, db); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.PolicyGroups, err = loadCoreJSONRows(ctx, db, "oc_core_policy_groups", "group_name"); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.Sandboxes, err = loadCoreSandboxRows(ctx, db); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.SandboxChecksums, err = loadCoreSandboxChecksumRows(ctx, db); err != nil {
		return bootstrap.CoreObjectState{}, err
	}
	if rows.ACLs, err = loadCoreObjectACLRows(ctx, db); err != nil {
		return bootstrap.CoreObjectState{}, err
	}

	return newCoreObjectRepository(nil).DecodeCoreObjects(rows)
}

func loadCoreJSONRows(ctx context.Context, db *sql.DB, table, nameColumn string) ([]CoreObjectJSONRecord, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT org_name, %s, payload_json FROM %s", nameColumn, table))
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", table, err)
	}
	defer rows.Close()

	var out []CoreObjectJSONRecord
	for rows.Next() {
		var row CoreObjectJSONRecord
		if err := rows.Scan(&row.Organization, &row.Name, &row.PayloadJSON); err != nil {
			return nil, fmt.Errorf("scan %s: %w", table, err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func loadCoreDataBagItemRows(ctx context.Context, db *sql.DB) ([]CoreDataBagItemRecord, error) {
	rows, err := db.QueryContext(ctx, "SELECT org_name, bag_name, item_id, payload_json FROM oc_core_data_bag_items")
	if err != nil {
		return nil, fmt.Errorf("load core data bag items: %w", err)
	}
	defer rows.Close()

	var out []CoreDataBagItemRecord
	for rows.Next() {
		var row CoreDataBagItemRecord
		if err := rows.Scan(&row.Organization, &row.BagName, &row.ItemID, &row.PayloadJSON); err != nil {
			return nil, fmt.Errorf("scan core data bag item: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func loadCorePolicyRevisionRows(ctx context.Context, db *sql.DB) ([]CorePolicyRevisionRecord, error) {
	rows, err := db.QueryContext(ctx, "SELECT org_name, policy_name, revision_id, payload_json FROM oc_core_policy_revisions")
	if err != nil {
		return nil, fmt.Errorf("load core policy revisions: %w", err)
	}
	defer rows.Close()

	var out []CorePolicyRevisionRecord
	for rows.Next() {
		var row CorePolicyRevisionRecord
		if err := rows.Scan(&row.Organization, &row.PolicyName, &row.RevisionID, &row.PayloadJSON); err != nil {
			return nil, fmt.Errorf("scan core policy revision: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func loadCoreSandboxRows(ctx context.Context, db *sql.DB) ([]CoreSandboxRecord, error) {
	rows, err := db.QueryContext(ctx, "SELECT org_name, sandbox_id, created_at FROM oc_core_sandboxes")
	if err != nil {
		return nil, fmt.Errorf("load core sandboxes: %w", err)
	}
	defer rows.Close()

	var out []CoreSandboxRecord
	for rows.Next() {
		var row CoreSandboxRecord
		if err := rows.Scan(&row.Organization, &row.ID, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan core sandbox: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func loadCoreSandboxChecksumRows(ctx context.Context, db *sql.DB) ([]CoreSandboxChecksumRecord, error) {
	rows, err := db.QueryContext(ctx, "SELECT org_name, sandbox_id, ordinal, checksum FROM oc_core_sandbox_checksums")
	if err != nil {
		return nil, fmt.Errorf("load core sandbox checksums: %w", err)
	}
	defer rows.Close()

	var out []CoreSandboxChecksumRecord
	for rows.Next() {
		var row CoreSandboxChecksumRecord
		if err := rows.Scan(&row.Organization, &row.SandboxID, &row.Ordinal, &row.Checksum); err != nil {
			return nil, fmt.Errorf("scan core sandbox checksum: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func loadCoreObjectACLRows(ctx context.Context, db *sql.DB) ([]CoreObjectACLRecord, error) {
	rows, err := db.QueryContext(ctx, "SELECT org_name, acl_key, acl_json FROM oc_core_object_acls")
	if err != nil {
		return nil, fmt.Errorf("load core object ACLs: %w", err)
	}
	defer rows.Close()

	var out []CoreObjectACLRecord
	for rows.Next() {
		var row CoreObjectACLRecord
		if err := rows.Scan(&row.Organization, &row.ACLKey, &row.ACLJSON); err != nil {
			return nil, fmt.Errorf("scan core object ACL: %w", err)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func ensureCoreObjectOrg(state bootstrap.CoreObjectState, orgName string) bootstrap.CoreObjectOrganizationState {
	org := state.Orgs[orgName]
	if org.DataBags == nil {
		org.DataBags = make(map[string]bootstrap.DataBag)
	}
	if org.DataBagItems == nil {
		org.DataBagItems = make(map[string]map[string]bootstrap.DataBagItem)
	}
	if org.Environments == nil {
		org.Environments = make(map[string]bootstrap.Environment)
	}
	if org.Nodes == nil {
		org.Nodes = make(map[string]bootstrap.Node)
	}
	if org.Roles == nil {
		org.Roles = make(map[string]bootstrap.Role)
	}
	if org.Sandboxes == nil {
		org.Sandboxes = make(map[string]bootstrap.Sandbox)
	}
	if org.Policies == nil {
		org.Policies = make(map[string]map[string]bootstrap.PolicyRevision)
	}
	if org.PolicyGroups == nil {
		org.PolicyGroups = make(map[string]bootstrap.PolicyGroup)
	}
	if org.ACLs == nil {
		org.ACLs = make(map[string]authz.ACL)
	}
	return org
}
