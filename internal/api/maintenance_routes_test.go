package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/maintenance"
)

var errMaintenanceCheckFailed = errors.New("maintenance check failed")

func TestMaintenanceGateBlocksRepresentativeWrites(t *testing.T) {
	store := enabledMaintenanceStore(t)
	router := newTestRouterWithMaintenance(t, store)

	for _, tc := range []struct {
		name   string
		method string
		path   string
	}{
		{name: "node_create", method: http.MethodPost, path: "/nodes"},
		{name: "org_node_update", method: http.MethodPut, path: "/organizations/ponyville/nodes/twilight"},
		{name: "role_delete", method: http.MethodDelete, path: "/roles/web"},
		{name: "environment_create", method: http.MethodPost, path: "/environments"},
		{name: "data_bag_write", method: http.MethodPut, path: "/data/secrets/api"},
		{name: "cookbook_update", method: http.MethodPut, path: "/cookbooks/demo/1.0.0"},
		{name: "artifact_delete", method: http.MethodDelete, path: "/organizations/ponyville/cookbook_artifacts/demo/1111111111111111111111111111111111111111"},
		{name: "sandbox_create", method: http.MethodPost, path: "/sandboxes"},
		{name: "sandbox_commit", method: http.MethodPut, path: "/organizations/ponyville/sandboxes/sandbox-id"},
		{name: "checksum_upload", method: http.MethodPut, path: "/_blob/checksums/0123456789abcdef0123456789abcdef"},
		{name: "user_create", method: http.MethodPost, path: "/users"},
		{name: "user_key_create", method: http.MethodPost, path: "/users/silent-bob/keys"},
		{name: "client_create", method: http.MethodPost, path: "/clients"},
		{name: "organization_create", method: http.MethodPost, path: "/organizations"},
		{name: "group_write", method: http.MethodPost, path: "/organizations/ponyville/groups"},
		{name: "container_write", method: http.MethodPut, path: "/organizations/ponyville/containers/nodes"},
		{name: "acl_write", method: http.MethodPut, path: "/organizations/ponyville/containers/nodes/_acl"},
		{name: "policy_group_assignment", method: http.MethodPut, path: "/policy_groups/dev/policies/app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assertMaintenanceBlocked(t, rec)
		})
	}
}

func TestMaintenanceGatePreservesReadsAndReadLikePOSTs(t *testing.T) {
	store := enabledMaintenanceStore(t)
	router := newTestRouterWithMaintenance(t, store)

	for _, tc := range []struct {
		name       string
		req        *http.Request
		wantStatus int
	}{
		{
			name:       "read_collection",
			req:        newSignedJSONRequest(t, http.MethodGet, "/nodes", nil),
			wantStatus: http.StatusOK,
		},
		{
			name:       "depsolver_read_like_post",
			req:        newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", []byte(`{"run_list":[]}`)),
			wantStatus: http.StatusOK,
		},
		{
			name:       "signed_blob_download_not_blocked",
			req:        httptest.NewRequest(http.MethodGet, "/_blob/checksums/0123456789abcdef0123456789abcdef", nil),
			wantStatus: http.StatusBadRequest,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, tc.req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s status = %d, want %d, body = %s", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestMaintenanceGateDoesNotMutateBlockedWrite(t *testing.T) {
	store := enabledMaintenanceStore(t)
	router := newTestRouterWithMaintenance(t, store)
	createBody := []byte(`{"name":"blocked-maintenance-node","chef_environment":"_default"}`)
	createReq := newSignedJSONRequest(t, http.MethodPost, "/nodes", createBody)
	createRec := httptest.NewRecorder()

	router.ServeHTTP(createRec, createReq)

	assertMaintenanceBlocked(t, createRec)

	getReq := newSignedJSONRequest(t, http.MethodGet, "/nodes/blocked-maintenance-node", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("GET blocked node status = %d, want %d, body = %s", getRec.Code, http.StatusNotFound, getRec.Body.String())
	}
}

func TestMaintenanceGateExpiredStateDoesNotBlockWrites(t *testing.T) {
	createdAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	expiresAt := createdAt.Add(time.Hour)
	now := expiresAt.Add(time.Second)
	store := maintenance.NewMemoryStore(maintenance.WithClock(func() time.Time { return now }))
	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Reason:    "expired window",
		CreatedAt: createdAt,
		ExpiresAt: &expiresAt,
	}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	router := newTestRouterWithMaintenance(t, store)

	req := httptest.NewRequest(http.MethodPost, "/nodes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code == maintenanceBlockedHTTPStatus {
		t.Fatalf("expired maintenance write status = %d, want request to reach auth/route layer", rec.Code)
	}
}

func TestMaintenanceGateFailsClosedForWriteWhenStoreCheckFails(t *testing.T) {
	router := newTestRouterWithMaintenance(t, failingMaintenanceStore{})

	writeReq := httptest.NewRequest(http.MethodPost, "/nodes", nil)
	writeRec := httptest.NewRecorder()
	router.ServeHTTP(writeRec, writeReq)
	assertMaintenanceBlocked(t, writeRec)

	readReq := newSignedJSONRequest(t, http.MethodGet, "/nodes", nil)
	readRec := httptest.NewRecorder()
	router.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("read status = %d, want %d despite failing maintenance store, body = %s", readRec.Code, http.StatusOK, readRec.Body.String())
	}
}

// enabledMaintenanceStore creates a deterministic active gate for route tests
// without relying on PostgreSQL or wall-clock timing.
func enabledMaintenanceStore(t *testing.T) *maintenance.MemoryStore {
	t.Helper()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	store := maintenance.NewMemoryStore(maintenance.WithClock(func() time.Time { return now }))
	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Mode:      "repair",
		Reason:    "route gate test",
		Actor:     "test",
		CreatedAt: now,
	}); err != nil {
		t.Fatalf("Enable() error = %v", err)
	}
	return store
}

// assertMaintenanceBlocked pins the exact upstream-shaped 503 response that the
// maintenance gate must return without leaking local maintenance details.
func assertMaintenanceBlocked(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != maintenanceBlockedHTTPStatus {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, maintenanceBlockedHTTPStatus, rec.Body.String())
	}
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v, body = %s", err, rec.Body.String())
	}
	if payload["error"] != maintenanceBlockedError {
		t.Fatalf("error = %q, want %q", payload["error"], maintenanceBlockedError)
	}
}

// failingMaintenanceStore lets route tests prove write candidates fail closed
// when the maintenance backend cannot be checked.
type failingMaintenanceStore struct{}

func (failingMaintenanceStore) Read(context.Context) (maintenance.State, error) {
	return maintenance.State{}, nil
}

func (failingMaintenanceStore) Enable(context.Context, maintenance.EnableInput) (maintenance.State, error) {
	return maintenance.State{}, nil
}

func (failingMaintenanceStore) Disable(context.Context, maintenance.DisableInput) (maintenance.State, error) {
	return maintenance.State{}, nil
}

// Check fails only when a write candidate asks for maintenance state, allowing
// tests to prove reads do not touch the store and writes fail closed.
func (failingMaintenanceStore) Check(context.Context) (maintenance.CheckResult, error) {
	return maintenance.CheckResult{}, errMaintenanceCheckFailed
}
