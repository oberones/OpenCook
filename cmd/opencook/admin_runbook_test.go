package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/admin"
)

func TestAdminRunbookListReportsCatalogWithoutLiveClient(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		t.Fatal("admin runbook list should not construct a live admin client")
		return nil, nil
	}

	code := cmd.Run(context.Background(), []string{"admin", "runbook", "list", "--json"})
	if code != exitOK {
		t.Fatalf("Run(admin runbook list) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminRunbookListOutput(t, stdout.String())
	if out["ok"] != true || out["command"] != "runbook_list" {
		t.Fatalf("runbook list output = %v, want ok runbook_list", out)
	}
	runbooks := adminRunbookRows(t, out)
	for _, name := range []string{"service-management", "observability", "backup-restore", "search-reindex", "migration-cutover", "identity-acl-repair", "unsupported-omnibus"} {
		if runbooks[name] == nil {
			t.Fatalf("runbook list missing %q: %v", name, runbooks)
		}
	}
}

func TestAdminRunbookShowReportsServiceManagementPatterns(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{"admin", "runbook", "show", "service_management", "--json"})
	if code != exitOK {
		t.Fatalf("Run(admin runbook show service_management) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	out := decodeAdminRunbookShowOutput(t, stdout.String())
	if out["ok"] != true || out["command"] != "runbook_show" {
		t.Fatalf("runbook show output = %v, want ok runbook_show", out)
	}
	runbook := adminRunbookOutputMap(t, out, "runbook")
	if runbook["name"] != "service-management" {
		t.Fatalf("runbook name = %v, want service-management", runbook["name"])
	}
	body := stdout.String()
	for _, want := range []string{"systemd", "docker-compose", "kubernetes", "opencook admin config check --json"} {
		if !strings.Contains(body, want) {
			t.Fatalf("runbook show missing %q: %s", want, body)
		}
	}
	for _, leaked := range []string{"pgsecret", "searchsecret", "PRIVATE KEY", "X-Ops-Authorization"} {
		if strings.Contains(body, leaked) {
			t.Fatalf("runbook show leaked %q: %s", leaked, body)
		}
	}
}

func TestAdminRunbookShowReportsUnsupportedOmnibusWorkflows(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{"admin", "runbooks", "show", "unsupported-omnibus", "--json"})
	if code != exitOK {
		t.Fatalf("Run(admin runbooks show unsupported-omnibus) exit = %d, want %d; stderr = %s", code, exitOK, stderr.String())
	}

	body := stdout.String()
	for _, want := range []string{"embedded process supervisor", "omnibus reconfigure", "licensing and license telemetry", "maintenance-mode traffic blocking"} {
		if !strings.Contains(body, want) {
			t.Fatalf("unsupported runbook missing %q: %s", want, body)
		}
	}
}

func TestAdminRunbookShowRejectsUnknownName(t *testing.T) {
	cmd, _, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{"admin", "runbook", "show", "does-not-exist"})
	if code != exitUsage {
		t.Fatalf("Run(admin runbook show unknown) exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "unknown admin runbook") {
		t.Fatalf("stderr = %q, want unknown runbook guidance", stderr.String())
	}
}

func decodeAdminRunbookListOutput(t *testing.T, body string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("json.Unmarshal(runbook list output) error = %v; body = %s", err, body)
	}
	return out
}

func decodeAdminRunbookShowOutput(t *testing.T, body string) map[string]any {
	t.Helper()

	var out map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatalf("json.Unmarshal(runbook show output) error = %v; body = %s", err, body)
	}
	return out
}

func adminRunbookRows(t *testing.T, out map[string]any) map[string]map[string]any {
	t.Helper()

	rawRunbooks, ok := out["runbooks"].([]any)
	if !ok {
		t.Fatalf("runbooks = %T, want []any (%v)", out["runbooks"], out)
	}
	runbooks := make(map[string]map[string]any, len(rawRunbooks))
	for _, raw := range rawRunbooks {
		runbook, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("runbook = %T, want map[string]any", raw)
		}
		name, ok := runbook["name"].(string)
		if !ok || name == "" {
			t.Fatalf("runbook name = %v, want non-empty string", runbook["name"])
		}
		runbooks[name] = runbook
	}
	return runbooks
}

func adminRunbookOutputMap(t *testing.T, parent map[string]any, key string) map[string]any {
	t.Helper()

	value, ok := parent[key].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any (%v)", key, parent[key], parent)
	}
	return value
}
