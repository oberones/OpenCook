package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/api"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

func TestAdminUsersAndOrganizationsCommandsUseLiveHTTPPaths(t *testing.T) {
	for _, tc := range []struct {
		name        string
		args        []string
		wantMethod  string
		wantPath    string
		wantPayload map[string]any
	}{
		{
			name:       "users list",
			args:       []string{"admin", "users", "list"},
			wantMethod: http.MethodGet,
			wantPath:   "/users",
		},
		{
			name:       "users show",
			args:       []string{"admin", "users", "show", "twilight"},
			wantMethod: http.MethodGet,
			wantPath:   "/users/twilight",
		},
		{
			name:       "users create",
			args:       []string{"admin", "users", "create", "twilight", "--display-name", "Twilight Sparkle", "--first-name", "Twilight", "--last-name", "Sparkle", "--email", "twilight@example.test"},
			wantMethod: http.MethodPost,
			wantPath:   "/users",
			wantPayload: map[string]any{
				"username":     "twilight",
				"display_name": "Twilight Sparkle",
				"first_name":   "Twilight",
				"last_name":    "Sparkle",
				"email":        "twilight@example.test",
				"public_key":   "",
			},
		},
		{
			name:       "orgs list",
			args:       []string{"admin", "orgs", "list"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations",
		},
		{
			name:       "orgs show",
			args:       []string{"admin", "orgs", "show", "ponyville"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville",
		},
		{
			name:       "orgs create",
			args:       []string{"admin", "orgs", "create", "ponyville", "--full-name", "Ponyville", "--org-type", "Business"},
			wantMethod: http.MethodPost,
			wantPath:   "/organizations",
			wantPayload: map[string]any{
				"name":      "ponyville",
				"full_name": "Ponyville",
				"org_type":  "Business",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			fake := &fakeAdminClient{response: map[string]any{"ok": true}}
			cmd.loadAdminConfig = func() admin.Config {
				return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
			}
			cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
				return fake, nil
			}

			if code := cmd.Run(context.Background(), tc.args); code != exitOK {
				t.Fatalf("Run(%v) exit = %d, want %d; stderr = %s", tc.args, code, exitOK, stderr.String())
			}
			if len(fake.calls) != 1 {
				t.Fatalf("admin calls = %d, want 1", len(fake.calls))
			}
			call := fake.calls[0]
			if call.method != tc.wantMethod || call.path != tc.wantPath {
				t.Fatalf("call = %s %s, want %s %s", call.method, call.path, tc.wantMethod, tc.wantPath)
			}
			if tc.wantPayload == nil && call.payload != nil {
				t.Fatalf("payload = %#v, want nil", call.payload)
			}
			if tc.wantPayload != nil && !payloadEqual(call.payload, tc.wantPayload) {
				t.Fatalf("payload = %#v, want %#v", call.payload, tc.wantPayload)
			}
			if !strings.Contains(stdout.String(), `"ok": true`) {
				t.Fatalf("stdout = %q, want JSON response", stdout.String())
			}
		})
	}
}

func TestAdminStatusSurfacesOpenSearchProviderWording(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminClient{response: map[string]any{
		"dependencies": map[string]any{
			"opensearch": map[string]any{
				"backend":    "opensearch",
				"configured": true,
				"message":    "OpenSearch-backed search provider active (opensearch 2.12.0; search-after pagination, delete-by-query, object total hits)",
			},
		},
	}}
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return fake, nil
	}

	if code := cmd.Run(context.Background(), []string{"admin", "status"}); code != exitOK {
		t.Fatalf("Run(admin status) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if len(fake.calls) != 1 || fake.calls[0].method != http.MethodGet || fake.calls[0].path != "/_status" {
		t.Fatalf("admin status call = %+v, want GET /_status", fake.calls)
	}
	for _, want := range []string{"opensearch 2.12.0", "search-after pagination", "delete-by-query"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("admin status stdout = %q, want %q", stdout.String(), want)
		}
	}
}

func TestAdminKeyCommandsUseLiveHTTPPaths(t *testing.T) {
	publicKeyPath := filepath.Join(t.TempDir(), "actor.pub")
	if err := os.WriteFile(publicKeyPath, []byte("PUBLIC KEY\n"), 0o600); err != nil {
		t.Fatalf("WriteFile(public key) error = %v", err)
	}

	for _, tc := range []struct {
		name        string
		args        []string
		wantMethod  string
		wantPath    string
		wantPayload map[string]any
	}{
		{
			name:       "user keys list",
			args:       []string{"admin", "users", "keys", "list", "twilight"},
			wantMethod: http.MethodGet,
			wantPath:   "/users/twilight/keys",
		},
		{
			name:       "user keys show",
			args:       []string{"admin", "users", "keys", "show", "twilight", "default"},
			wantMethod: http.MethodGet,
			wantPath:   "/users/twilight/keys/default",
		},
		{
			name:       "user keys add public key",
			args:       []string{"admin", "users", "keys", "add", "twilight", "--key-name", "laptop", "--public-key", publicKeyPath, "--expiration-date", "infinity"},
			wantMethod: http.MethodPost,
			wantPath:   "/users/twilight/keys",
			wantPayload: map[string]any{
				"name":            "laptop",
				"public_key":      "PUBLIC KEY\n",
				"create_key":      false,
				"expiration_date": "infinity",
			},
		},
		{
			name:       "user keys update generated key",
			args:       []string{"admin", "users", "keys", "update", "twilight", "laptop", "--new-name", "workstation", "--create-key"},
			wantMethod: http.MethodPut,
			wantPath:   "/users/twilight/keys/laptop",
			wantPayload: map[string]any{
				"name":       "workstation",
				"create_key": true,
			},
		},
		{
			name:       "user keys delete",
			args:       []string{"admin", "users", "keys", "delete", "twilight", "laptop", "--yes"},
			wantMethod: http.MethodDelete,
			wantPath:   "/users/twilight/keys/laptop",
		},
		{
			name:       "client keys add generated key",
			args:       []string{"admin", "clients", "keys", "add", "ponyville", "web01", "--key-name", "default"},
			wantMethod: http.MethodPost,
			wantPath:   "/organizations/ponyville/clients/web01/keys",
			wantPayload: map[string]any{
				"name":            "default",
				"public_key":      "",
				"create_key":      true,
				"expiration_date": "infinity",
			},
		},
		{
			name:       "client keys update",
			args:       []string{"admin", "clients", "keys", "update", "ponyville", "web01", "default", "--expiration-date", "2030-01-01T00:00:00Z"},
			wantMethod: http.MethodPut,
			wantPath:   "/organizations/ponyville/clients/web01/keys/default",
			wantPayload: map[string]any{
				"expiration_date": "2030-01-01T00:00:00Z",
			},
		},
		{
			name:       "client keys delete",
			args:       []string{"admin", "clients", "keys", "delete", "ponyville", "web01", "default", "--yes"},
			wantMethod: http.MethodDelete,
			wantPath:   "/organizations/ponyville/clients/web01/keys/default",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, stderr := newTestCommand(t)
			fake := &fakeAdminClient{response: map[string]any{"ok": true}}
			cmd.loadAdminConfig = func() admin.Config {
				return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
			}
			cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
				return fake, nil
			}

			if code := cmd.Run(context.Background(), tc.args); code != exitOK {
				t.Fatalf("Run(%v) exit = %d, want %d; stderr = %s", tc.args, code, exitOK, stderr.String())
			}
			if len(fake.calls) != 1 {
				t.Fatalf("admin calls = %d, want 1", len(fake.calls))
			}
			call := fake.calls[0]
			if call.method != tc.wantMethod || call.path != tc.wantPath {
				t.Fatalf("call = %s %s, want %s %s", call.method, call.path, tc.wantMethod, tc.wantPath)
			}
			if tc.wantPayload == nil && call.payload != nil {
				t.Fatalf("payload = %#v, want nil", call.payload)
			}
			if tc.wantPayload != nil && !payloadEqual(call.payload, tc.wantPayload) {
				t.Fatalf("payload = %#v, want %#v", call.payload, tc.wantPayload)
			}
		})
	}
}

func TestAdminInspectionCommandsUseLiveHTTPPaths(t *testing.T) {
	for _, tc := range []struct {
		name       string
		args       []string
		wantMethod string
		wantPath   string
	}{
		{
			name:       "groups list",
			args:       []string{"admin", "groups", "list", "ponyville"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/groups",
		},
		{
			name:       "groups show",
			args:       []string{"admin", "groups", "show", "ponyville", "admins"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/groups/admins",
		},
		{
			name:       "containers list",
			args:       []string{"admin", "containers", "list", "ponyville"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/containers",
		},
		{
			name:       "containers show",
			args:       []string{"admin", "containers", "show", "ponyville", "clients"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/containers/clients",
		},
		{
			name:       "user acl",
			args:       []string{"admin", "acls", "get", "user", "pivotal"},
			wantMethod: http.MethodGet,
			wantPath:   "/users/pivotal/_acl",
		},
		{
			name:       "org acl",
			args:       []string{"admin", "acls", "get", "org", "ponyville"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/_acl",
		},
		{
			name:       "group acl",
			args:       []string{"admin", "acls", "get", "group", "ponyville", "admins"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/groups/admins/_acl",
		},
		{
			name:       "container acl",
			args:       []string{"admin", "acls", "get", "container", "ponyville", "clients"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/containers/clients/_acl",
		},
		{
			name:       "client acl",
			args:       []string{"admin", "acls", "get", "client", "ponyville", "ponyville-validator"},
			wantMethod: http.MethodGet,
			wantPath:   "/organizations/ponyville/clients/ponyville-validator/_acl",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, _, stderr := newTestCommand(t)
			fake := &fakeAdminClient{response: map[string]any{"ok": true}}
			cmd.loadAdminConfig = func() admin.Config {
				return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
			}
			cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
				return fake, nil
			}

			if code := cmd.Run(context.Background(), tc.args); code != exitOK {
				t.Fatalf("Run(%v) exit = %d, want %d; stderr = %s", tc.args, code, exitOK, stderr.String())
			}
			if len(fake.calls) != 1 {
				t.Fatalf("admin calls = %d, want 1", len(fake.calls))
			}
			call := fake.calls[0]
			if call.method != tc.wantMethod || call.path != tc.wantPath {
				t.Fatalf("call = %s %s, want %s %s", call.method, call.path, tc.wantMethod, tc.wantPath)
			}
		})
	}
}

func TestAdminOfflineMembershipCommandsMutateBootstrapCoreStore(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	store := &fakeOfflineStore{bootstrap: adminOfflineTestState()}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if dsn != "postgres://offline-test" {
			t.Fatalf("dsn = %q, want postgres://offline-test", dsn)
		}
		return store, nil, nil
	}

	if code := cmd.Run(context.Background(), []string{"admin", "orgs", "add-user", "ponyville", "rarity", "--admin", "--offline", "--yes"}); code != exitOK {
		t.Fatalf("Run(orgs add-user) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	org := store.bootstrap.Orgs["ponyville"]
	if !containsString(org.Groups["users"].Users, "rarity") {
		t.Fatalf("users group = %v, want rarity", org.Groups["users"].Users)
	}
	if !containsString(org.Groups["admins"].Users, "rarity") {
		t.Fatalf("admins group = %v, want rarity", org.Groups["admins"].Users)
	}
	if store.bootstrapSaves != 1 {
		t.Fatalf("bootstrap saves = %d, want 1", store.bootstrapSaves)
	}
	if !strings.Contains(stdout.String(), offlineRestartNote) {
		t.Fatalf("stdout = %q, want restart note", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "groups", "add-actor", "ponyville", "clients", "web01", "--actor-type", "client", "--offline", "--yes"}); code != exitOK {
		t.Fatalf("Run(groups add-actor) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	org = store.bootstrap.Orgs["ponyville"]
	if !containsString(org.Groups["clients"].Clients, "web01") {
		t.Fatalf("clients group clients = %v, want web01", org.Groups["clients"].Clients)
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "server-admins", "grant", "rarity", "--offline", "--yes"}); code != exitOK {
		t.Fatalf("Run(server-admins grant) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	org = store.bootstrap.Orgs["canterlot"]
	if !containsString(org.Groups["admins"].Users, "rarity") {
		t.Fatalf("canterlot admins = %v, want rarity after server-admin grant", org.Groups["admins"].Users)
	}
}

func TestAdminOfflineMembershipFailuresDoNotSave(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	initial := adminOfflineTestState()
	store := &fakeOfflineStore{bootstrap: initial}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return store, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "orgs", "add-user", "ponyville", "missing", "--offline", "--yes"})
	if code != exitNotFound {
		t.Fatalf("Run(missing user) exit = %d, want %d; stderr = %s", code, exitNotFound, stderr.String())
	}
	if store.bootstrapSaves != 0 {
		t.Fatalf("bootstrap saves = %d, want 0", store.bootstrapSaves)
	}
	if !bootstrapStatesEqual(store.bootstrap, initial) {
		t.Fatal("offline failed membership changed bootstrap state")
	}
}

func TestAdminOfflineACLRepairDryRunDoesNotSave(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	store := &fakeOfflineStore{
		bootstrap: adminOfflineTestStateWithoutACLs(),
		objects: bootstrap.CoreObjectState{
			Orgs: map[string]bootstrap.CoreObjectOrganizationState{
				"ponyville": {
					Nodes: map[string]bootstrap.Node{
						"node1": {Name: "node1"},
					},
					ACLs: map[string]authz.ACL{},
				},
			},
		},
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return store, nil, nil
	}

	if code := cmd.Run(context.Background(), []string{"admin", "acls", "repair-defaults", "--offline", "--dry-run", "--org", "ponyville"}); code != exitOK {
		t.Fatalf("Run(acls repair dry-run) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if store.bootstrapSaves != 0 || store.objectSaves != 0 {
		t.Fatalf("saves = %d/%d, want 0/0", store.bootstrapSaves, store.objectSaves)
	}
	if !strings.Contains(stdout.String(), "ponyville/organization") || !strings.Contains(stdout.String(), "ponyville/node:node1") {
		t.Fatalf("stdout = %q, want bootstrap and core object ACL repair previews", stdout.String())
	}
}

func TestAdminOfflineACLRepairRejectsMissingOrgFilter(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	store := &fakeOfflineStore{
		bootstrap: adminOfflineTestStateWithoutACLs(),
		objects: bootstrap.CoreObjectState{
			Orgs: map[string]bootstrap.CoreObjectOrganizationState{
				"ponyville": {
					Nodes: map[string]bootstrap.Node{
						"node1": {Name: "node1"},
					},
					ACLs: map[string]authz.ACL{},
				},
			},
		},
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return store, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "acls", "repair-defaults", "--offline", "--dry-run", "--org", "ponvyille"})
	if code != exitNotFound {
		t.Fatalf("Run(acls repair missing org) exit = %d, want %d; stderr = %s", code, exitNotFound, stderr.String())
	}
	if store.bootstrapSaves != 0 || store.objectSaves != 0 {
		t.Fatalf("saves = %d/%d, want 0/0", store.bootstrapSaves, store.objectSaves)
	}
	if !strings.Contains(stderr.String(), "organization ponvyille not found") {
		t.Fatalf("stderr = %q, want missing org detail", stderr.String())
	}
}

func TestAdminOfflineACLRepairNoMutationOnLoadFailure(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	store := &fakeOfflineStore{loadErr: errOfflineTest}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return store, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "acls", "repair-defaults", "--offline", "--yes"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(acls repair load failure) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}
	if store.bootstrapSaves != 0 || store.objectSaves != 0 {
		t.Fatalf("saves = %d/%d, want 0/0", store.bootstrapSaves, store.objectSaves)
	}
}

func TestAdminOfflineACLRepairNoMutationOnSaveFailure(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	initial := adminOfflineTestStateWithoutACLs()
	store := &fakeOfflineStore{bootstrap: initial, saveErr: errOfflineTest}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://offline-test"}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return store, nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "acls", "repair-defaults", "--offline", "--yes"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(acls repair save failure) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}
	if store.bootstrapSaves != 0 || store.objectSaves != 0 {
		t.Fatalf("saves = %d/%d, want 0/0", store.bootstrapSaves, store.objectSaves)
	}
	if !bootstrapStatesEqual(store.bootstrap, initial) {
		t.Fatal("offline failed ACL repair changed bootstrap state")
	}
}

func TestAdminGeneratedPrivateKeyHandling(t *testing.T) {
	t.Run("redacts unless output requested", func(t *testing.T) {
		cmd, stdout, stderr := newTestCommand(t)
		fake := &fakeAdminClient{response: map[string]any{"uri": "/users/twilight", "private_key": "SECRET\n"}}
		cmd.loadAdminConfig = func() admin.Config {
			return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
		}
		cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
			return fake, nil
		}

		if code := cmd.Run(context.Background(), []string{"admin", "users", "create", "twilight"}); code != exitOK {
			t.Fatalf("Run(admin users create) exit = %d, want %d", code, exitOK)
		}
		if strings.Contains(stdout.String(), "SECRET") {
			t.Fatalf("stdout leaked private key: %q", stdout.String())
		}
		if !strings.Contains(stderr.String(), "private key omitted") {
			t.Fatalf("stderr = %q, want omission warning", stderr.String())
		}
	})

	t.Run("writes requested file restrictively", func(t *testing.T) {
		cmd, stdout, stderr := newTestCommand(t)
		keyPath := filepath.Join(t.TempDir(), "twilight.pem")
		fake := &fakeAdminClient{response: map[string]any{"uri": "/users/twilight", "private_key": "SECRET\n"}}
		cmd.loadAdminConfig = func() admin.Config {
			return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
		}
		cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
			return fake, nil
		}

		if code := cmd.Run(context.Background(), []string{"admin", "users", "create", "twilight", "--private-key-out", keyPath}); code != exitOK {
			t.Fatalf("Run(admin users create) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
		}
		data, err := os.ReadFile(keyPath)
		if err != nil {
			t.Fatalf("ReadFile(private key) error = %v", err)
		}
		if string(data) != "SECRET\n" {
			t.Fatalf("private key file = %q, want SECRET", string(data))
		}
		info, err := os.Stat(keyPath)
		if err != nil {
			t.Fatalf("Stat(private key) error = %v", err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("private key permissions = %#o, want 0600", got)
		}
		if strings.Contains(stdout.String(), "SECRET") {
			t.Fatalf("stdout leaked private key after file write: %q", stdout.String())
		}
	})

	t.Run("prints only when stdout explicitly requested", func(t *testing.T) {
		cmd, stdout, stderr := newTestCommand(t)
		fake := &fakeAdminClient{response: map[string]any{"uri": "/users/twilight", "private_key": "SECRET\n"}}
		cmd.loadAdminConfig = func() admin.Config {
			return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
		}
		cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
			return fake, nil
		}

		if code := cmd.Run(context.Background(), []string{"admin", "users", "create", "twilight", "--private-key-out", "-"}); code != exitOK {
			t.Fatalf("Run(admin users create) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
		}
		if stdout.String() != "SECRET\n" {
			t.Fatalf("stdout = %q, want private key only", stdout.String())
		}
	})
}

func TestAdminOrganizationCreateValidatorKeyOverwriteConfirmation(t *testing.T) {
	t.Run("cancel keeps existing file and skips request", func(t *testing.T) {
		cmd, _, stderr := newTestCommand(t)
		cmd.stdin = strings.NewReader("n\n")

		validatorKeyPath := filepath.Join(t.TempDir(), "ponyville-validator.pem")
		if err := os.WriteFile(validatorKeyPath, []byte("OLD-KEY\n"), 0o600); err != nil {
			t.Fatalf("WriteFile(existing validator key) error = %v", err)
		}

		fake := &fakeAdminClient{response: map[string]any{"private_key": "NEW-KEY\n"}}
		cmd.loadAdminConfig = func() admin.Config {
			return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
		}
		cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
			return fake, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "orgs", "create", "ponyville", "--full-name", "Ponyville", "--validator-key-out", validatorKeyPath})
		if code != exitUsage {
			t.Fatalf("Run(admin orgs create cancel overwrite) exit = %d, want %d", code, exitUsage)
		}
		if len(fake.calls) != 0 {
			t.Fatalf("admin calls = %d, want 0", len(fake.calls))
		}
		data, err := os.ReadFile(validatorKeyPath)
		if err != nil {
			t.Fatalf("ReadFile(existing validator key) error = %v", err)
		}
		if string(data) != "OLD-KEY\n" {
			t.Fatalf("validator key file = %q, want preserved content", string(data))
		}
		if !strings.Contains(stderr.String(), "already exists and will be overwritten") {
			t.Fatalf("stderr = %q, want overwrite warning", stderr.String())
		}
		if !strings.Contains(stderr.String(), "organization creation canceled") {
			t.Fatalf("stderr = %q, want cancellation message", stderr.String())
		}
	})

	t.Run("confirmed overwrite replaces existing file", func(t *testing.T) {
		cmd, stdout, stderr := newTestCommand(t)
		cmd.stdin = strings.NewReader("y\n")

		validatorKeyPath := filepath.Join(t.TempDir(), "ponyville-validator.pem")
		if err := os.WriteFile(validatorKeyPath, []byte("OLD-KEY\n"), 0o600); err != nil {
			t.Fatalf("WriteFile(existing validator key) error = %v", err)
		}

		fake := &fakeAdminClient{response: map[string]any{"uri": "/organizations/ponyville", "private_key": "NEW-KEY\n"}}
		cmd.loadAdminConfig = func() admin.Config {
			return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
		}
		cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
			return fake, nil
		}

		code := cmd.Run(context.Background(), []string{"admin", "orgs", "create", "ponyville", "--full-name", "Ponyville", "--validator-key-out", validatorKeyPath})
		if code != exitOK {
			t.Fatalf("Run(admin orgs create confirm overwrite) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
		}
		if len(fake.calls) != 1 {
			t.Fatalf("admin calls = %d, want 1", len(fake.calls))
		}
		data, err := os.ReadFile(validatorKeyPath)
		if err != nil {
			t.Fatalf("ReadFile(overwritten validator key) error = %v", err)
		}
		if string(data) != "NEW-KEY\n" {
			t.Fatalf("validator key file = %q, want NEW-KEY", string(data))
		}
		if !strings.Contains(stderr.String(), "already exists and will be overwritten") {
			t.Fatalf("stderr = %q, want overwrite warning", stderr.String())
		}
		if !strings.Contains(stderr.String(), "private key written to "+validatorKeyPath) {
			t.Fatalf("stderr = %q, want file write confirmation", stderr.String())
		}
		if strings.Contains(stdout.String(), "NEW-KEY") {
			t.Fatalf("stdout leaked private key after overwrite: %q", stdout.String())
		}
	})
}

func TestAdminKeyDeleteRequiresConfirmation(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)
	fake := &fakeAdminClient{response: map[string]any{"ok": true}}
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "redacted.pem"}
	}
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return fake, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "users", "keys", "delete", "twilight", "default"})
	if code != exitUsage {
		t.Fatalf("Run(delete without --yes) exit = %d, want %d", code, exitUsage)
	}
	if len(fake.calls) != 0 {
		t.Fatalf("admin calls = %d, want 0", len(fake.calls))
	}
	if !strings.Contains(stderr.String(), "without --yes") {
		t.Fatalf("stderr = %q, want --yes warning", stderr.String())
	}
}

func TestAdminCommandsCanRunAgainstInProcessServer(t *testing.T) {
	privateKey := mustGenerateAdminPrivateKey(t)
	handler := newAdminCommandTestRouter(t, privateKey)

	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{
			ServerURL:        "http://opencook.test",
			RequestorName:    "pivotal",
			RequestorType:    "user",
			ServerAPIVersion: "1",
		}
	}
	cmd.newAdmin = func(cfg admin.Config) (adminJSONClient, error) {
		return admin.NewClient(cfg, admin.WithPrivateKey(privateKey), admin.WithHTTPDoer(handlerDoer{handler: handler}))
	}

	userKeyPath := filepath.Join(t.TempDir(), "rarity.pem")
	args := []string{"admin", "users", "create", "rarity", "--first-name", "Rarity", "--last-name", "Belle", "--email", "rarity@example.test", "--private-key-out", userKeyPath}
	if code := cmd.Run(context.Background(), args); code != exitOK {
		t.Fatalf("Run(users create) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if _, err := os.Stat(userKeyPath); err != nil {
		t.Fatalf("generated user key was not written: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "users", "show", "rarity"}); code != exitOK {
		t.Fatalf("Run(users show) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"username": "rarity"`) {
		t.Fatalf("users show stdout = %q, want created user", stdout.String())
	}

	validatorKeyPath := filepath.Join(t.TempDir(), "ponyville-validator.pem")
	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "orgs", "create", "ponyville", "--full-name", "Ponyville", "--validator-key-out", validatorKeyPath}); code != exitOK {
		t.Fatalf("Run(orgs create) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if _, err := os.Stat(validatorKeyPath); err != nil {
		t.Fatalf("generated validator key was not written: %v", err)
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "groups", "show", "ponyville", "admins"}); code != exitOK {
		t.Fatalf("Run(groups show) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"groupname": "admins"`) {
		t.Fatalf("groups show stdout = %q, want admins group", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "containers", "show", "ponyville", "clients"}); code != exitOK {
		t.Fatalf("Run(containers show) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"containername": "clients"`) {
		t.Fatalf("containers show stdout = %q, want clients container", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "acls", "get", "group", "ponyville", "admins"}); code != exitOK {
		t.Fatalf("Run(acls get group) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"read"`) {
		t.Fatalf("acls get stdout = %q, want ACL payload", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := cmd.Run(context.Background(), []string{"admin", "clients", "keys", "list", "ponyville", "ponyville-validator"}); code != exitOK {
		t.Fatalf("Run(client keys list) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"name": "default"`) {
		t.Fatalf("client keys stdout = %q, want validator default key", stdout.String())
	}
}

type fakeAdminClient struct {
	calls    []fakeAdminCall
	response any
	err      error
}

type fakeAdminCall struct {
	method  string
	path    string
	payload any
}

type handlerDoer struct {
	handler http.Handler
}

func (d handlerDoer) Do(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	d.handler.ServeHTTP(rec, req)
	return rec.Result(), nil
}

func (f *fakeAdminClient) DoJSON(_ context.Context, method, path string, in, out any) error {
	f.calls = append(f.calls, fakeAdminCall{
		method:  method,
		path:    path,
		payload: cloneJSONValue(in),
	})
	if f.err != nil {
		return f.err
	}
	if out == nil {
		return nil
	}
	response := f.response
	if response == nil {
		response = map[string]any{"ok": true}
	}
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

type fakeOfflineStore struct {
	bootstrap      bootstrap.BootstrapCoreState
	objects        bootstrap.CoreObjectState
	loadErr        error
	saveErr        error
	bootstrapSaves int
	objectSaves    int
}

func (s *fakeOfflineStore) LoadBootstrapCore() (bootstrap.BootstrapCoreState, error) {
	if s.loadErr != nil {
		return bootstrap.BootstrapCoreState{}, s.loadErr
	}
	return bootstrap.CloneBootstrapCoreState(s.bootstrap), nil
}

func (s *fakeOfflineStore) SaveBootstrapCore(state bootstrap.BootstrapCoreState) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.bootstrapSaves++
	s.bootstrap = bootstrap.CloneBootstrapCoreState(state)
	return nil
}

func (s *fakeOfflineStore) LoadCoreObjects() (bootstrap.CoreObjectState, error) {
	if s.loadErr != nil {
		return bootstrap.CoreObjectState{}, s.loadErr
	}
	return bootstrap.CloneCoreObjectState(s.objects), nil
}

func (s *fakeOfflineStore) SaveCoreObjects(state bootstrap.CoreObjectState) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.objectSaves++
	s.objects = bootstrap.CloneCoreObjectState(state)
	return nil
}

func adminOfflineTestState() bootstrap.BootstrapCoreState {
	return bootstrap.BootstrapCoreState{
		Users: map[string]bootstrap.User{
			"pivotal": {Username: "pivotal"},
			"rarity":  {Username: "rarity"},
		},
		UserACLs: map[string]authz.ACL{},
		Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: bootstrap.Organization{Name: "ponyville", FullName: "Ponyville", OrgType: "Business"},
				Clients: map[string]bootstrap.Client{
					"web01": {Name: "web01", ClientName: "web01", Organization: "ponyville"},
				},
				Groups: map[string]bootstrap.Group{
					"admins":  {Name: "admins", GroupName: "admins", Organization: "ponyville", Users: []string{"pivotal"}, Actors: []string{"pivotal"}},
					"users":   {Name: "users", GroupName: "users", Organization: "ponyville", Users: []string{"pivotal"}, Actors: []string{"pivotal"}},
					"clients": {Name: "clients", GroupName: "clients", Organization: "ponyville"},
				},
				Containers: map[string]bootstrap.Container{
					"clients": {Name: "clients", ContainerName: "clients", ContainerPath: "clients"},
				},
				ACLs: map[string]authz.ACL{
					"organization": {Read: authz.Permission{Actors: []string{"pivotal"}}},
				},
			},
			"canterlot": {
				Organization: bootstrap.Organization{Name: "canterlot", FullName: "Canterlot", OrgType: "Business"},
				Clients:      map[string]bootstrap.Client{},
				Groups: map[string]bootstrap.Group{
					"admins": {Name: "admins", GroupName: "admins", Organization: "canterlot", Users: []string{"pivotal"}, Actors: []string{"pivotal"}},
					"users":  {Name: "users", GroupName: "users", Organization: "canterlot", Users: []string{"pivotal"}, Actors: []string{"pivotal"}},
				},
				Containers: map[string]bootstrap.Container{},
				ACLs:       map[string]authz.ACL{},
			},
		},
	}
}

func adminOfflineTestStateWithoutACLs() bootstrap.BootstrapCoreState {
	state := adminOfflineTestState()
	state.UserACLs = map[string]authz.ACL{}
	for orgName, org := range state.Orgs {
		org.ACLs = map[string]authz.ACL{}
		state.Orgs[orgName] = org
	}
	return state
}

func bootstrapStatesEqual(got, want bootstrap.BootstrapCoreState) bool {
	gotData, gotErr := json.Marshal(got)
	wantData, wantErr := json.Marshal(want)
	return gotErr == nil && wantErr == nil && string(gotData) == string(wantData)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func payloadEqual(got, want any) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	gotData, gotErr := json.Marshal(got)
	wantData, wantErr := json.Marshal(want)
	return gotErr == nil && wantErr == nil && string(gotData) == string(wantData)
}

func cloneJSONValue(value any) any {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

func newAdminCommandTestRouter(t *testing.T, privateKey *rsa.PrivateKey) http.Handler {
	t.Helper()

	store := authn.NewMemoryKeyStore()
	if err := store.Put(authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "pivotal",
		},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("Put(pivotal key) error = %v", err)
	}

	state := bootstrap.NewService(store, bootstrap.Options{SuperuserName: "pivotal"})
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "pivotal"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", string(mustMarshalPublicKeyPEM(t, &privateKey.PublicKey))); err != nil {
		t.Fatalf("SeedPublicKey(pivotal) error = %v", err)
	}

	skew := 15 * time.Minute
	return api.NewRouter(api.Dependencies{
		Logger: log.New(io.Discard, "", 0),
		Config: config.Config{
			ServiceName:      "opencook",
			Environment:      "test",
			AuthSkew:         skew,
			MaxAuthBodyBytes: config.DefaultMaxAuthBodyBytes,
		},
		Version:          version.Info{Version: "test"},
		Compat:           compat.NewDefaultRegistry(),
		Now:              time.Now,
		Authn:            authn.NewChefVerifier(store, authn.Options{AllowedClockSkew: &skew}),
		Authz:            authz.NewACLAuthorizer(state),
		Bootstrap:        state,
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         pg.New(""),
		CookbookBackend:  "memory-bootstrap",
	})
}

func mustGenerateAdminPrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	return key
}

func mustMarshalPrivateKeyPEM(t *testing.T, key *rsa.PrivateKey) []byte {
	t.Helper()
	return pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
}

func mustMarshalPublicKeyPEM(t *testing.T, key *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(key)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey() error = %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: der,
	})
}

var errOfflineTest = errors.New("offline test error")
