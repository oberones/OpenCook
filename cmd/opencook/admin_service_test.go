package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/search"
)

func TestAdminServiceStatusReportsStaticSummary(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		return validAdminConfigCheckConfig(t, ""), nil
	}
	cmd.newBlobStore = blob.NewStore
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		t.Fatal("admin service status should not construct a live admin client")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "service", "status", "--json", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(admin service status) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminServiceOutput(t, stdout.String())
	if out["ok"] != true || out["command"] != "service_status" {
		t.Fatalf("service status output = %v, want ok service_status", out)
	}
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("duration_ms missing from service status output: %v", out)
	}
	summary := adminServiceOutputMap(t, out, "summary")
	if summary["persistence"] != "postgres-configured" || summary["search"] != "opensearch-configured" || summary["blob"] != "filesystem" {
		t.Fatalf("summary = %v, want configured postgres/opensearch/filesystem", summary)
	}
	if summary["maintenance"] != "postgres-shared-configured" {
		t.Fatalf("summary.maintenance = %v, want postgres-shared-configured", summary["maintenance"])
	}
	for _, secret := range []string{"pgsecret", "searchsecret"} {
		if strings.Contains(stdout.String(), secret) {
			t.Fatalf("service status leaked secret %q: %s", secret, stdout.String())
		}
	}
}

func TestAdminServiceDoctorSkipsPostgresStateWithoutOffline(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadAdminConfig = func() admin.Config {
		return admin.Config{}
	}
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, "")
		cfg.OpenSearchURL = ""
		cfg.BlobBackend = ""
		cfg.BlobStorageURL = ""
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore
	setTestMaintenanceStore(cmd, maintenance.NewMemoryStore(), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})

	code := cmd.Run(context.Background(), []string{"admin", "--json", "service", "doctor"})
	if code != exitOK {
		t.Fatalf("Run(admin service doctor) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminServiceOutput(t, stdout.String())
	checks := adminConfigCheckRows(t, out)
	if checks["postgres_state"]["status"] != "skipped" {
		t.Fatalf("postgres_state = %v, want skipped without --offline", checks["postgres_state"])
	}
	if checks["opensearch_ping"]["status"] != "skipped" {
		t.Fatalf("opensearch_ping = %v, want skipped without OpenSearch URL", checks["opensearch_ping"])
	}
	if checks["blob_inventory"]["status"] != "ok" {
		t.Fatalf("blob_inventory = %v, want safe memory listing", checks["blob_inventory"])
	}
	if checks["maintenance_state"]["status"] != "ok" {
		t.Fatalf("maintenance_state = %v, want ok", checks["maintenance_state"])
	}
}

func TestAdminServiceDoctorOfflineLoadsPostgresStateAndPingsOpenSearch(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, "")
		cfg.BlobBackend = ""
		cfg.BlobStorageURL = ""
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore
	setTestMaintenanceStore(cmd, maintenance.NewMemoryStore(), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	closed := false
	cmd.newOfflineStore = func(_ context.Context, dsn string) (adminOfflineStore, func() error, error) {
		if !strings.Contains(dsn, "postgres.example") {
			t.Fatalf("offline DSN = %q, want configured PostgreSQL DSN", dsn)
		}
		store := &fakeOfflineStore{
			bootstrap: adminOfflineTestState(),
			objects: bootstrap.CoreObjectState{
				Orgs: map[string]bootstrap.CoreObjectOrganizationState{
					"ponyville": {
						Nodes:        map[string]bootstrap.Node{"twilight": {Name: "twilight"}},
						Roles:        map[string]bootstrap.Role{"web": {Name: "web"}},
						Environments: map[string]bootstrap.Environment{"_default": {Name: "_default"}},
					},
				},
			},
		}
		return store, func() error {
			closed = true
			return nil
		}, nil
	}
	target := &fakeServiceSearchTarget{}
	cmd.newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
		if !strings.HasPrefix(raw, "https://") {
			t.Fatalf("OpenSearch URL = %q, want configured URL", raw)
		}
		return target, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "service", "doctor", "--offline", "--with-timing"})
	if code != exitOK {
		t.Fatalf("Run(admin service doctor --offline) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	if !closed {
		t.Fatal("offline store close was not called")
	}
	if target.pings != 1 {
		t.Fatalf("OpenSearch pings = %d, want 1", target.pings)
	}

	out := decodeAdminServiceOutput(t, stdout.String())
	checks := adminConfigCheckRows(t, out)
	postgresState := checks["postgres_state"]
	if postgresState["status"] != "ok" {
		t.Fatalf("postgres_state = %v, want ok", postgresState)
	}
	details := adminServiceOutputMap(t, postgresState, "details")
	if details["organizations"] != "2" || details["nodes"] != "1" {
		t.Fatalf("postgres_state details = %v, want aggregate counts", details)
	}
	if checks["opensearch_ping"]["status"] != "ok" {
		t.Fatalf("opensearch_ping = %v, want ok", checks["opensearch_ping"])
	}
}

func TestAdminServiceDoctorReportsActiveMaintenance(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, "")
		cfg.OpenSearchURL = ""
		cfg.BlobBackend = ""
		cfg.BlobStorageURL = ""
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore
	store := maintenance.NewMemoryStore()
	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Mode:   "repair",
		Reason: "safe online inspection",
		Actor:  "operator",
	}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	setTestMaintenanceStore(cmd, store, adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})

	code := cmd.Run(context.Background(), []string{"admin", "service", "doctor", "--json"})
	if code != exitOK {
		t.Fatalf("Run(admin service doctor active maintenance) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}
	out := decodeAdminServiceOutput(t, stdout.String())
	checks := adminConfigCheckRows(t, out)
	maintenanceState := checks["maintenance_state"]
	if maintenanceState["status"] != "ok" || !strings.Contains(maintenanceState["message"].(string), "writes are blocked") {
		t.Fatalf("maintenance_state = %v, want active maintenance message", maintenanceState)
	}
	details := adminServiceOutputMap(t, maintenanceState, "details")
	if details["active"] != "true" || details["mode"] != "repair" || details["shared"] != "true" {
		t.Fatalf("maintenance details = %v, want active repair shared state", details)
	}
}

func TestAdminServiceDoctorProviderFailureIsRedacted(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.loadOffline = func() (config.Config, error) {
		cfg := validAdminConfigCheckConfig(t, "")
		cfg.OpenSearchURL = "https://admin:searchsecret@opensearch.example"
		return cfg, nil
	}
	cmd.newBlobStore = blob.NewStore
	setTestMaintenanceStore(cmd, maintenance.NewMemoryStore(), adminMaintenanceBackend{Name: "postgres", Configured: true, Shared: true})
	cmd.newSearchTarget = func(string) (search.ConsistencyTarget, error) {
		return &fakeServiceSearchTarget{pingErr: errors.New("dial tcp admin:searchsecret@opensearch.example")}, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "service", "doctor", "--json"})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(admin service doctor provider failure) exit = %d, want %d; stderr = %s", code, exitDependencyUnavailable, stderr.String())
	}
	if strings.Contains(stdout.String(), "searchsecret") {
		t.Fatalf("service doctor leaked provider secret: %s", stdout.String())
	}
	out := decodeAdminServiceOutput(t, stdout.String())
	checks := adminConfigCheckRows(t, out)
	if checks["opensearch_ping"]["status"] != "error" {
		t.Fatalf("opensearch_ping = %v, want error", checks["opensearch_ping"])
	}
}

// setTestMaintenanceStore injects a deterministic maintenance backend so
// service-doctor tests never open a real PostgreSQL connection.
func setTestMaintenanceStore(cmd *command, store maintenance.Store, backend adminMaintenanceBackend) {
	cmd.newMaintenanceStore = func(context.Context, string) (maintenance.Store, adminMaintenanceBackend, func() error, error) {
		return store, backend, nil, nil
	}
}

type fakeServiceSearchTarget struct {
	pings   int
	pingErr error
}

func (t *fakeServiceSearchTarget) Ping(context.Context) error {
	t.pings++
	return t.pingErr
}

func (t *fakeServiceSearchTarget) EnsureChefIndex(context.Context) error {
	return nil
}

func (t *fakeServiceSearchTarget) SearchIDs(context.Context, search.Query) ([]string, error) {
	return nil, nil
}

func (t *fakeServiceSearchTarget) BulkUpsert(context.Context, []search.Document) error {
	return nil
}

func (t *fakeServiceSearchTarget) DeleteDocument(context.Context, string) error {
	return nil
}

func (t *fakeServiceSearchTarget) Refresh(context.Context) error {
	return nil
}

func decodeAdminServiceOutput(t *testing.T, body string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("json.Unmarshal(service output) error = %v; body = %s", err, body)
	}
	return out
}

func adminServiceOutputMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any (%v)", key, parent[key], parent)
	}
	return value
}
