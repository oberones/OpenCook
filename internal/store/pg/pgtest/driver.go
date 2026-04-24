package pgtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/store/pg"
)

const fakeDriverName = "opencook-pgtest"

var (
	registerFakeDriverOnce sync.Once
	openStateSeq           atomic.Uint64
	openStates             sync.Map
)

type Seed struct {
	Organizations []pg.CookbookOrganizationRecord
	Versions      []pg.CookbookVersionBundle
	Artifacts     []pg.CookbookArtifactBundle
	BootstrapCore bootstrap.BootstrapCoreState
}

type State struct {
	mu        sync.Mutex
	orgs      map[string]pg.CookbookOrganizationRecord
	versions  map[versionKey]pg.CookbookVersionBundle
	artifacts map[artifactKey]pg.CookbookArtifactBundle
	bootstrap bootstrap.BootstrapCoreState
}

type versionKey struct {
	org  string
	name string
	ver  string
}

type artifactKey struct {
	org  string
	name string
	id   string
}

func NewState(seed Seed) *State {
	state := &State{
		orgs:      make(map[string]pg.CookbookOrganizationRecord),
		versions:  make(map[versionKey]pg.CookbookVersionBundle),
		artifacts: make(map[artifactKey]pg.CookbookArtifactBundle),
		bootstrap: bootstrap.CloneBootstrapCoreState(seed.BootstrapCore),
	}

	for _, org := range seed.Organizations {
		state.orgs[strings.TrimSpace(org.Name)] = pg.CookbookOrganizationRecord{
			Name:     strings.TrimSpace(org.Name),
			FullName: normalizedFullName(org.Name, org.FullName),
		}
	}
	for _, bundle := range seed.Versions {
		key := versionKey{
			org:  strings.TrimSpace(bundle.Version.Organization),
			name: strings.TrimSpace(bundle.Version.CookbookName),
			ver:  strings.TrimSpace(bundle.Version.Version),
		}
		state.versions[key] = copyVersionBundle(bundle)
		state.ensureOrgRecord(key.org, key.org)
	}
	for _, bundle := range seed.Artifacts {
		key := artifactKey{
			org:  strings.TrimSpace(bundle.Artifact.Organization),
			name: strings.TrimSpace(bundle.Artifact.Name),
			id:   strings.TrimSpace(bundle.Artifact.Identifier),
		}
		state.artifacts[key] = copyArtifactBundle(bundle)
		state.ensureOrgRecord(key.org, key.org)
	}

	return state
}

func (s *State) OpenDB() (*sql.DB, func() error, error) {
	if s == nil {
		return nil, nil, fmt.Errorf("pgtest state is required")
	}

	registerFakeDriverOnce.Do(func() {
		sql.Register(fakeDriverName, fakeDriver{})
	})

	token := fmt.Sprintf("pgtest-%d", openStateSeq.Add(1))
	openStates.Store(token, s)

	db, err := sql.Open(fakeDriverName, token)
	if err != nil {
		openStates.Delete(token)
		return nil, nil, err
	}

	cleanup := func() error {
		openStates.Delete(token)
		return db.Close()
	}
	return db, cleanup, nil
}

func normalizedFullName(name, fullName string) string {
	name = strings.TrimSpace(name)
	fullName = strings.TrimSpace(fullName)
	if fullName == "" {
		fullName = name
	}
	return fullName
}

func copyVersionBundle(bundle pg.CookbookVersionBundle) pg.CookbookVersionBundle {
	out := bundle
	out.Files = append([]pg.CookbookVersionFileRecord(nil), bundle.Files...)
	return out
}

func copyArtifactBundle(bundle pg.CookbookArtifactBundle) pg.CookbookArtifactBundle {
	out := bundle
	out.Files = append([]pg.CookbookArtifactFileRecord(nil), bundle.Files...)
	return out
}

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	raw, ok := openStates.Load(name)
	if !ok {
		return nil, fmt.Errorf("unknown pgtest state %q", name)
	}
	state, ok := raw.(*State)
	if !ok {
		return nil, fmt.Errorf("invalid pgtest state %q", name)
	}
	return &fakeConn{state: state}, nil
}

type fakeConn struct {
	state *State
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *fakeConn) Close() error                        { return nil }
func (c *fakeConn) Begin() (driver.Tx, error)           { return fakeTx{}, nil }
func (c *fakeConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return fakeTx{}, nil
}
func (c *fakeConn) Ping(context.Context) error { return nil }

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return c.state.exec(query, args)
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.state.query(query, args)
}

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

func (s *State) exec(query string, args []driver.NamedValue) (driver.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case strings.Contains(query, "CREATE TABLE IF NOT EXISTS oc_bootstrap_"):
		return driver.RowsAffected(0), nil
	case strings.Contains(query, "DELETE FROM oc_bootstrap_"):
		s.bootstrap = bootstrap.BootstrapCoreState{
			Users:    make(map[string]bootstrap.User),
			UserACLs: make(map[string]authz.ACL),
			UserKeys: make(map[string]map[string]bootstrap.KeyRecord),
			Orgs:     make(map[string]bootstrap.BootstrapCoreOrganizationState),
		}
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_users"):
		username := namedString(args, 0)
		s.bootstrap.Users[username] = bootstrap.User{
			Username:    username,
			DisplayName: namedString(args, 1),
			Email:       namedString(args, 2),
			FirstName:   namedString(args, 3),
			LastName:    namedString(args, 4),
		}
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_user_acls"):
		username := namedString(args, 0)
		var acl authz.ACL
		_ = json.Unmarshal(namedBytes(args, 1), &acl)
		s.bootstrap.UserACLs[username] = acl
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_user_keys"):
		username := namedString(args, 0)
		if _, ok := s.bootstrap.UserKeys[username]; !ok {
			s.bootstrap.UserKeys[username] = make(map[string]bootstrap.KeyRecord)
		}
		record := bootstrap.KeyRecord{
			Name:           namedString(args, 1),
			URI:            namedString(args, 2),
			PublicKeyPEM:   namedString(args, 3),
			ExpirationDate: namedString(args, 4),
			ExpiresAt:      namedTime(args, 5),
		}
		s.bootstrap.UserKeys[username][record.Name] = record
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_orgs"):
		orgName := namedString(args, 0)
		s.bootstrap.Orgs[orgName] = bootstrap.BootstrapCoreOrganizationState{
			Organization: bootstrap.Organization{
				Name:     orgName,
				FullName: namedString(args, 1),
				OrgType:  namedString(args, 2),
				GUID:     namedString(args, 3),
			},
			Clients:    make(map[string]bootstrap.Client),
			ClientKeys: make(map[string]map[string]bootstrap.KeyRecord),
			Groups:     make(map[string]bootstrap.Group),
			Containers: make(map[string]bootstrap.Container),
			ACLs:       make(map[string]authz.ACL),
		}
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_clients"):
		orgName := namedString(args, 0)
		clientName := namedString(args, 1)
		org := s.bootstrap.Orgs[orgName]
		org.Clients[clientName] = bootstrap.Client{
			Name:         namedString(args, 2),
			ClientName:   clientName,
			Organization: orgName,
			Validator:    namedBool(args, 3),
			Admin:        namedBool(args, 4),
			PublicKey:    namedString(args, 5),
			URI:          namedString(args, 6),
		}
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_client_keys"):
		orgName := namedString(args, 0)
		clientName := namedString(args, 1)
		org := s.bootstrap.Orgs[orgName]
		if _, ok := org.ClientKeys[clientName]; !ok {
			org.ClientKeys[clientName] = make(map[string]bootstrap.KeyRecord)
		}
		record := bootstrap.KeyRecord{
			Name:           namedString(args, 2),
			URI:            namedString(args, 3),
			PublicKeyPEM:   namedString(args, 4),
			ExpirationDate: namedString(args, 5),
			ExpiresAt:      namedTime(args, 6),
		}
		org.ClientKeys[clientName][record.Name] = record
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_groups"):
		orgName := namedString(args, 0)
		groupName := namedString(args, 1)
		org := s.bootstrap.Orgs[orgName]
		org.Groups[groupName] = bootstrap.Group{
			Name:         namedString(args, 2),
			GroupName:    groupName,
			Organization: orgName,
		}
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_group_memberships"):
		orgName := namedString(args, 0)
		groupName := namedString(args, 1)
		memberType := namedString(args, 2)
		memberName := namedString(args, 4)
		org := s.bootstrap.Orgs[orgName]
		group := org.Groups[groupName]
		switch memberType {
		case "actor":
			group.Actors = append(group.Actors, memberName)
		case "user":
			group.Users = append(group.Users, memberName)
		case "client":
			group.Clients = append(group.Clients, memberName)
		case "group":
			group.Groups = append(group.Groups, memberName)
		}
		org.Groups[groupName] = group
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_containers"):
		orgName := namedString(args, 0)
		containerName := namedString(args, 1)
		org := s.bootstrap.Orgs[orgName]
		org.Containers[containerName] = bootstrap.Container{
			ContainerName: containerName,
			Name:          namedString(args, 2),
			ContainerPath: namedString(args, 3),
		}
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_bootstrap_org_acls"):
		orgName := namedString(args, 0)
		aclKey := namedString(args, 1)
		var acl authz.ACL
		_ = json.Unmarshal(namedBytes(args, 2), &acl)
		org := s.bootstrap.Orgs[orgName]
		org.ACLs[aclKey] = acl
		s.bootstrap.Orgs[orgName] = org
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "CREATE TABLE IF NOT EXISTS oc_cookbook_orgs"):
		return driver.RowsAffected(0), nil
	case strings.Contains(query, "INSERT INTO oc_cookbook_orgs"):
		orgName := namedString(args, 0)
		fullName := namedString(args, 1)
		if strings.Contains(query, "DO NOTHING") {
			s.ensureOrgRecord(orgName, fullName)
			return driver.RowsAffected(1), nil
		}
		s.upsertOrganization(orgName, fullName)
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_cookbook_versions"):
		key := versionKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			ver:  namedString(args, 2),
		}
		bundle := s.versions[key]
		bundle.Version = pg.CookbookVersionRecord{
			Organization: key.org,
			CookbookName: key.name,
			Version:      key.ver,
			FullName:     namedString(args, 3),
			JSONClass:    namedString(args, 4),
			ChefType:     namedString(args, 5),
			Frozen:       namedBool(args, 6),
			MetadataJSON: append([]byte(nil), namedBytes(args, 7)...),
		}
		s.versions[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM oc_cookbook_version_files"):
		key := versionKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			ver:  namedString(args, 2),
		}
		bundle := s.versions[key]
		bundle.Files = nil
		s.versions[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_cookbook_version_files"):
		key := versionKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			ver:  namedString(args, 2),
		}
		bundle := s.versions[key]
		bundle.Files = append(bundle.Files, pg.CookbookVersionFileRecord{
			Organization: key.org,
			CookbookName: key.name,
			Version:      key.ver,
			Ordinal:      namedInt(args, 3),
			Name:         namedString(args, 4),
			Path:         namedString(args, 5),
			Checksum:     namedString(args, 6),
			Specificity:  namedString(args, 7),
		})
		s.versions[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM oc_cookbook_versions"):
		key := versionKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			ver:  namedString(args, 2),
		}
		delete(s.versions, key)
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_cookbook_artifacts"):
		key := artifactKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			id:   namedString(args, 2),
		}
		bundle := s.artifacts[key]
		bundle.Artifact = pg.CookbookArtifactRecord{
			Organization: key.org,
			Name:         key.name,
			Identifier:   key.id,
			Version:      namedString(args, 3),
			ChefType:     namedString(args, 4),
			Frozen:       namedBool(args, 5),
			MetadataJSON: append([]byte(nil), namedBytes(args, 6)...),
		}
		s.artifacts[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM oc_cookbook_artifact_files"):
		key := artifactKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			id:   namedString(args, 2),
		}
		bundle := s.artifacts[key]
		bundle.Files = nil
		s.artifacts[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "INSERT INTO oc_cookbook_artifact_files"):
		key := artifactKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			id:   namedString(args, 2),
		}
		bundle := s.artifacts[key]
		bundle.Files = append(bundle.Files, pg.CookbookArtifactFileRecord{
			Organization: key.org,
			Name:         key.name,
			Identifier:   key.id,
			Ordinal:      namedInt(args, 3),
			FileName:     namedString(args, 4),
			FilePath:     namedString(args, 5),
			Checksum:     namedString(args, 6),
			Specificity:  namedString(args, 7),
		})
		s.artifacts[key] = bundle
		return driver.RowsAffected(1), nil
	case strings.Contains(query, "DELETE FROM oc_cookbook_artifacts"):
		key := artifactKey{
			org:  namedString(args, 0),
			name: namedString(args, 1),
			id:   namedString(args, 2),
		}
		delete(s.artifacts, key)
		return driver.RowsAffected(1), nil
	default:
		return driver.RowsAffected(0), nil
	}
}

func (s *State) query(query string, _ []driver.NamedValue) (driver.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case strings.Contains(query, "FROM oc_bootstrap_users"):
		keys := sortedBootstrapKeys(s.bootstrap.Users)
		values := make([][]driver.Value, 0, len(keys))
		for _, username := range keys {
			user := s.bootstrap.Users[username]
			values = append(values, []driver.Value{user.Username, user.DisplayName, user.Email, user.FirstName, user.LastName})
		}
		return &fakeRows{columns: []string{"username", "display_name", "email", "first_name", "last_name"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_user_acls"):
		keys := sortedBootstrapKeys(s.bootstrap.UserACLs)
		values := make([][]driver.Value, 0, len(keys))
		for _, username := range keys {
			raw, _ := json.Marshal(s.bootstrap.UserACLs[username])
			values = append(values, []driver.Value{username, raw})
		}
		return &fakeRows{columns: []string{"username", "acl_json"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_user_keys"):
		users := sortedBootstrapKeys(s.bootstrap.UserKeys)
		values := make([][]driver.Value, 0)
		for _, username := range users {
			keyNames := sortedBootstrapKeys(s.bootstrap.UserKeys[username])
			for _, keyName := range keyNames {
				record := s.bootstrap.UserKeys[username][keyName]
				values = append(values, []driver.Value{username, record.Name, record.URI, record.PublicKeyPEM, record.ExpirationDate, nullableDriverTime(record.ExpiresAt)})
			}
		}
		return &fakeRows{columns: []string{"username", "key_name", "uri", "public_key_pem", "expiration_date", "expires_at"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_orgs"):
		keys := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0, len(keys))
		for _, orgName := range keys {
			org := s.bootstrap.Orgs[orgName].Organization
			values = append(values, []driver.Value{org.Name, org.FullName, org.OrgType, org.GUID})
		}
		return &fakeRows{columns: []string{"org_name", "full_name", "org_type", "guid"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_clients"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			clientNames := sortedBootstrapKeys(org.Clients)
			for _, clientName := range clientNames {
				client := org.Clients[clientName]
				values = append(values, []driver.Value{orgName, client.ClientName, client.Name, client.Validator, client.Admin, client.PublicKey, client.URI})
			}
		}
		return &fakeRows{columns: []string{"org_name", "client_name", "name", "validator", "admin", "public_key_pem", "uri"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_client_keys"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			clientNames := sortedBootstrapKeys(org.ClientKeys)
			for _, clientName := range clientNames {
				keyNames := sortedBootstrapKeys(org.ClientKeys[clientName])
				for _, keyName := range keyNames {
					record := org.ClientKeys[clientName][keyName]
					values = append(values, []driver.Value{orgName, clientName, record.Name, record.URI, record.PublicKeyPEM, record.ExpirationDate, nullableDriverTime(record.ExpiresAt)})
				}
			}
		}
		return &fakeRows{columns: []string{"org_name", "client_name", "key_name", "uri", "public_key_pem", "expiration_date", "expires_at"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_groups"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			groupNames := sortedBootstrapKeys(org.Groups)
			for _, groupName := range groupNames {
				group := org.Groups[groupName]
				values = append(values, []driver.Value{orgName, group.GroupName, group.Name})
			}
		}
		return &fakeRows{columns: []string{"org_name", "group_name", "name"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_group_memberships"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			groupNames := sortedBootstrapKeys(org.Groups)
			for _, groupName := range groupNames {
				group := org.Groups[groupName]
				values = appendBootstrapMembershipValues(values, orgName, group.GroupName, "actor", group.Actors)
				values = appendBootstrapMembershipValues(values, orgName, group.GroupName, "user", group.Users)
				values = appendBootstrapMembershipValues(values, orgName, group.GroupName, "client", group.Clients)
				values = appendBootstrapMembershipValues(values, orgName, group.GroupName, "group", group.Groups)
			}
		}
		return &fakeRows{columns: []string{"org_name", "group_name", "member_type", "member_name"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_containers"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			containerNames := sortedBootstrapKeys(org.Containers)
			for _, containerName := range containerNames {
				container := org.Containers[containerName]
				values = append(values, []driver.Value{orgName, container.ContainerName, container.Name, container.ContainerPath})
			}
		}
		return &fakeRows{columns: []string{"org_name", "container_name", "name", "container_path"}, values: values}, nil
	case strings.Contains(query, "FROM oc_bootstrap_org_acls"):
		orgNames := sortedBootstrapKeys(s.bootstrap.Orgs)
		values := make([][]driver.Value, 0)
		for _, orgName := range orgNames {
			org := s.bootstrap.Orgs[orgName]
			aclKeys := sortedBootstrapKeys(org.ACLs)
			for _, aclKey := range aclKeys {
				raw, _ := json.Marshal(org.ACLs[aclKey])
				values = append(values, []driver.Value{orgName, aclKey, raw})
			}
		}
		return &fakeRows{columns: []string{"org_name", "acl_key", "acl_json"}, values: values}, nil
	case strings.Contains(query, "FROM oc_cookbook_orgs"):
		keys := make([]string, 0, len(s.orgs))
		for name := range s.orgs {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		values := make([][]driver.Value, 0, len(keys))
		for _, name := range keys {
			org := s.orgs[name]
			values = append(values, []driver.Value{org.Name, org.FullName})
		}
		return &fakeRows{columns: []string{"org_name", "full_name"}, values: values}, nil
	case strings.Contains(query, "FROM oc_cookbook_versions"):
		keys := make([]versionKey, 0, len(s.versions))
		for key := range s.versions {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].org != keys[j].org {
				return keys[i].org < keys[j].org
			}
			if keys[i].name != keys[j].name {
				return keys[i].name < keys[j].name
			}
			return keys[i].ver < keys[j].ver
		})
		values := make([][]driver.Value, 0, len(keys))
		for _, key := range keys {
			record := s.versions[key].Version
			values = append(values, []driver.Value{
				record.Organization,
				record.CookbookName,
				record.Version,
				record.FullName,
				record.JSONClass,
				record.ChefType,
				record.Frozen,
				append([]byte(nil), record.MetadataJSON...),
			})
		}
		return &fakeRows{columns: []string{"org_name", "cookbook_name", "version", "full_name", "json_class", "chef_type", "frozen", "metadata_json"}, values: values}, nil
	case strings.Contains(query, "FROM oc_cookbook_version_files"):
		keys := make([]versionKey, 0, len(s.versions))
		for key := range s.versions {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].org != keys[j].org {
				return keys[i].org < keys[j].org
			}
			if keys[i].name != keys[j].name {
				return keys[i].name < keys[j].name
			}
			return keys[i].ver < keys[j].ver
		})
		values := make([][]driver.Value, 0)
		for _, key := range keys {
			files := append([]pg.CookbookVersionFileRecord(nil), s.versions[key].Files...)
			sort.Slice(files, func(i, j int) bool { return files[i].Ordinal < files[j].Ordinal })
			for _, file := range files {
				values = append(values, []driver.Value{
					file.Organization,
					file.CookbookName,
					file.Version,
					int64(file.Ordinal),
					file.Name,
					file.Path,
					file.Checksum,
					file.Specificity,
				})
			}
		}
		return &fakeRows{columns: []string{"org_name", "cookbook_name", "version", "ordinal", "file_name", "file_path", "checksum", "specificity"}, values: values}, nil
	case strings.Contains(query, "FROM oc_cookbook_artifacts"):
		keys := make([]artifactKey, 0, len(s.artifacts))
		for key := range s.artifacts {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].org != keys[j].org {
				return keys[i].org < keys[j].org
			}
			if keys[i].name != keys[j].name {
				return keys[i].name < keys[j].name
			}
			return keys[i].id < keys[j].id
		})
		values := make([][]driver.Value, 0, len(keys))
		for _, key := range keys {
			record := s.artifacts[key].Artifact
			values = append(values, []driver.Value{
				record.Organization,
				record.Name,
				record.Identifier,
				record.Version,
				record.ChefType,
				record.Frozen,
				append([]byte(nil), record.MetadataJSON...),
			})
		}
		return &fakeRows{columns: []string{"org_name", "name", "identifier", "version", "chef_type", "frozen", "metadata_json"}, values: values}, nil
	case strings.Contains(query, "FROM oc_cookbook_artifact_files"):
		keys := make([]artifactKey, 0, len(s.artifacts))
		for key := range s.artifacts {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].org != keys[j].org {
				return keys[i].org < keys[j].org
			}
			if keys[i].name != keys[j].name {
				return keys[i].name < keys[j].name
			}
			return keys[i].id < keys[j].id
		})
		values := make([][]driver.Value, 0)
		for _, key := range keys {
			files := append([]pg.CookbookArtifactFileRecord(nil), s.artifacts[key].Files...)
			sort.Slice(files, func(i, j int) bool { return files[i].Ordinal < files[j].Ordinal })
			for _, file := range files {
				values = append(values, []driver.Value{
					file.Organization,
					file.Name,
					file.Identifier,
					int64(file.Ordinal),
					file.FileName,
					file.FilePath,
					file.Checksum,
					file.Specificity,
				})
			}
		}
		return &fakeRows{columns: []string{"org_name", "name", "identifier", "ordinal", "file_name", "file_path", "checksum", "specificity"}, values: values}, nil
	default:
		return &fakeRows{}, nil
	}
}

func (s *State) ensureOrgRecord(name, fullName string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}
	if _, ok := s.orgs[name]; ok {
		return
	}
	s.orgs[name] = pg.CookbookOrganizationRecord{
		Name:     name,
		FullName: normalizedFullName(name, fullName),
	}
}

func (s *State) upsertOrganization(name, fullName string) {
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}

	incoming := pg.CookbookOrganizationRecord{
		Name:     name,
		FullName: normalizedFullName(name, fullName),
	}
	existing, ok := s.orgs[name]
	if !ok {
		s.orgs[name] = incoming
		return
	}
	if strings.TrimSpace(existing.FullName) != "" &&
		existing.FullName != existing.Name &&
		incoming.FullName == incoming.Name {
		s.orgs[name] = pg.CookbookOrganizationRecord{
			Name:     name,
			FullName: existing.FullName,
		}
		return
	}
	s.orgs[name] = incoming
}

func namedString(args []driver.NamedValue, idx int) string {
	if idx >= len(args) || args[idx].Value == nil {
		return ""
	}
	value, _ := args[idx].Value.(string)
	return strings.TrimSpace(value)
}

func namedBool(args []driver.NamedValue, idx int) bool {
	if idx >= len(args) || args[idx].Value == nil {
		return false
	}
	value, _ := args[idx].Value.(bool)
	return value
}

func namedInt(args []driver.NamedValue, idx int) int {
	if idx >= len(args) || args[idx].Value == nil {
		return 0
	}
	switch value := args[idx].Value.(type) {
	case int64:
		return int(value)
	case int:
		return value
	default:
		return 0
	}
}

func namedBytes(args []driver.NamedValue, idx int) []byte {
	if idx >= len(args) || args[idx].Value == nil {
		return nil
	}
	value, _ := args[idx].Value.([]byte)
	return value
}

func namedTime(args []driver.NamedValue, idx int) *time.Time {
	if idx >= len(args) || args[idx].Value == nil {
		return nil
	}
	value, ok := args[idx].Value.(time.Time)
	if !ok {
		return nil
	}
	value = value.UTC()
	return &value
}

func nullableDriverTime(value *time.Time) driver.Value {
	if value == nil {
		return nil
	}
	return value.UTC()
}

func sortedBootstrapKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func appendBootstrapMembershipValues(values [][]driver.Value, orgName, groupName, memberType string, members []string) [][]driver.Value {
	for _, member := range members {
		values = append(values, []driver.Value{orgName, groupName, memberType, member})
	}
	return values
}

type fakeRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
