package pgtest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

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
}

type State struct {
	mu        sync.Mutex
	orgs      map[string]pg.CookbookOrganizationRecord
	versions  map[versionKey]pg.CookbookVersionBundle
	artifacts map[artifactKey]pg.CookbookArtifactBundle
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
