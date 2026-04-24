package pg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"
)

const fakeSQLDriverName = "opencook-pg-activation-fake"

var registerFakeSQLDriver sync.Once

func TestStoreActivateCookbookPersistenceLoadsRepositoryState(t *testing.T) {
	registerFakeSQLDriverOnce()

	previousDriverName := sqlDriverName
	sqlDriverName = fakeSQLDriverName
	defer func() {
		sqlDriverName = previousDriverName
	}()

	store := New("postgres://activation-test")
	if store.CookbookPersistenceActive() {
		t.Fatal("CookbookPersistenceActive() = true before activation")
	}

	if err := store.ActivateCookbookPersistence(context.Background()); err != nil {
		t.Fatalf("ActivateCookbookPersistence() error = %v", err)
	}
	defer store.Close()

	if !store.CookbookPersistenceActive() {
		t.Fatal("CookbookPersistenceActive() = false after activation")
	}

	status := store.Status()
	if status.Message != "PostgreSQL cookbook and bootstrap core persistence active" {
		t.Fatalf("Status().Message = %q, want active persistence message", status.Message)
	}

	orgs := store.Cookbooks().OrganizationRecords()
	if len(orgs) != 1 || orgs[0].Name != "ponyville" {
		t.Fatalf("OrganizationRecords() = %v, want persisted ponyville org", orgs)
	}

	backend := store.CookbookStore()
	versions, orgOK, found := backend.ListCookbookVersionsByName("ponyville", "demo")
	if !orgOK || !found || len(versions) != 1 || versions[0].Version != "1.2.3" {
		t.Fatalf("ListCookbookVersionsByName() = %v/%v/%v, want demo 1.2.3", orgOK, found, versions)
	}

	artifact, orgOK, found := backend.GetCookbookArtifact("ponyville", "demo", "1111111111111111111111111111111111111111")
	if !orgOK || !found {
		t.Fatalf("GetCookbookArtifact() found = %v/%v, want true/true", orgOK, found)
	}
	if artifact.Version != "1.2.3" {
		t.Fatalf("artifact.Version = %q, want %q", artifact.Version, "1.2.3")
	}
}

func registerFakeSQLDriverOnce() {
	registerFakeSQLDriver.Do(func() {
		sql.Register(fakeSQLDriverName, fakeActivationDriver{})
	})
}

type fakeActivationDriver struct{}

func (fakeActivationDriver) Open(name string) (driver.Conn, error) {
	return &fakeActivationConn{}, nil
}

type fakeActivationConn struct{}

func (c *fakeActivationConn) Prepare(query string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *fakeActivationConn) Close() error                              { return nil }
func (c *fakeActivationConn) Begin() (driver.Tx, error)                 { return nil, driver.ErrSkip }

func (c *fakeActivationConn) Ping(ctx context.Context) error { return nil }

func (c *fakeActivationConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(1), nil
}

func (c *fakeActivationConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(query, "FROM oc_cookbook_orgs"):
		return &fakeRows{
			columns: []string{"org_name", "full_name"},
			values: [][]driver.Value{
				{"ponyville", "Ponyville"},
			},
		}, nil
	case strings.Contains(query, "FROM oc_cookbook_versions"):
		return &fakeRows{
			columns: []string{"org_name", "cookbook_name", "version", "full_name", "json_class", "chef_type", "frozen", "metadata_json"},
			values: [][]driver.Value{
				{"ponyville", "demo", "1.2.3", "demo-1.2.3", "Chef::CookbookVersion", "cookbook_version", false, []byte(`{"name":"demo","version":"1.2.3","dependencies":{"apt":">= 1.0.0"}}`)},
			},
		}, nil
	case strings.Contains(query, "FROM oc_cookbook_version_files"):
		return &fakeRows{
			columns: []string{"org_name", "cookbook_name", "version", "ordinal", "file_name", "file_path", "checksum", "specificity"},
			values: [][]driver.Value{
				{"ponyville", "demo", "1.2.3", int64(0), "default.rb", "recipes/default.rb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "default"},
			},
		}, nil
	case strings.Contains(query, "FROM oc_cookbook_artifacts"):
		return &fakeRows{
			columns: []string{"org_name", "name", "identifier", "version", "chef_type", "frozen", "metadata_json"},
			values: [][]driver.Value{
				{"ponyville", "demo", "1111111111111111111111111111111111111111", "1.2.3", "cookbook_version", false, []byte(`{"name":"demo","version":"1.2.3"}`)},
			},
		}, nil
	case strings.Contains(query, "FROM oc_cookbook_artifact_files"):
		return &fakeRows{
			columns: []string{"org_name", "name", "identifier", "ordinal", "file_name", "file_path", "checksum", "specificity"},
			values: [][]driver.Value{
				{"ponyville", "demo", "1111111111111111111111111111111111111111", int64(0), "default.rb", "recipes/default.rb", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "default"},
			},
		}, nil
	default:
		return &fakeRows{}, nil
	}
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
