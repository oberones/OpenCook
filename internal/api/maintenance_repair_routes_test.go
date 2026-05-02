package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

func TestMaintenanceRepairDefaultACLsRequiresActiveMaintenanceAndConfirmation(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairDefaultACLsPath, []byte(`{"yes":true,"org":"ponyville"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("inactive repair status = %d, want %d, body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	if _, ok, err := state.ResolveACL(context.Background(), authz.Resource{Type: "container", Name: "clients", Organization: "ponyville"}); err != nil || ok {
		t.Fatalf("ResolveACL(container after inactive repair) ok/error = %t/%v, want false/nil", ok, err)
	}

	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Reason: "repair ACL defaults",
		Mode:   "repair",
		Actor:  "operator",
	}); err != nil {
		t.Fatalf("maintenance Enable() error = %v", err)
	}
	missingConfirmation := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairDefaultACLsPath, []byte(`{"org":"ponyville"}`))
	missingConfirmationRec := httptest.NewRecorder()
	router.ServeHTTP(missingConfirmationRec, missingConfirmation)
	if missingConfirmationRec.Code != http.StatusBadRequest {
		t.Fatalf("missing confirmation status = %d, want %d, body = %s", missingConfirmationRec.Code, http.StatusBadRequest, missingConfirmationRec.Body.String())
	}
	if _, ok, err := state.ResolveACL(context.Background(), authz.Resource{Type: "container", Name: "clients", Organization: "ponyville"}); err != nil || ok {
		t.Fatalf("ResolveACL(container after missing confirmation) ok/error = %t/%v, want false/nil", ok, err)
	}
}

func TestMaintenanceRepairDefaultACLsRepairsLiveStateWithoutRestart(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)
	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Reason: "repair ACL defaults",
		Mode:   "repair",
		Actor:  "operator",
	}); err != nil {
		t.Fatalf("maintenance Enable() error = %v", err)
	}

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairDefaultACLsPath, []byte(`{"yes":true,"org":"ponyville"}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("repair status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(repair) error = %v", err)
	}
	if payload["mode"] != "online" || payload["changed"] != true || payload["verifier_cache"] != "unchanged" {
		t.Fatalf("repair payload = %v, want online changed response with verifier cache unchanged", payload)
	}
	if _, ok, err := state.ResolveACL(context.Background(), authz.Resource{Type: "node", Name: "node1", Organization: "ponyville"}); err != nil || !ok {
		t.Fatalf("ResolveACL(node after repair) ok/error = %t/%v, want true/nil", ok, err)
	}

	aclReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/organizations/ponyville/containers/clients/_acl", nil)
	aclRec := httptest.NewRecorder()
	router.ServeHTTP(aclRec, aclReq)
	if aclRec.Code != http.StatusOK {
		t.Fatalf("container ACL read status = %d, want %d, body = %s", aclRec.Code, http.StatusOK, aclRec.Body.String())
	}

	usersReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/users", nil)
	usersRec := httptest.NewRecorder()
	router.ServeHTTP(usersRec, usersReq)
	if usersRec.Code != http.StatusOK {
		t.Fatalf("signed follow-up user read status = %d, want %d, body = %s", usersRec.Code, http.StatusOK, usersRec.Body.String())
	}
}

// newMaintenanceRepairRouter creates intentionally incomplete ACL state so the
// online repair route can prove it refreshes the live authz view immediately.
func newMaintenanceRepairRouter(t *testing.T) (http.Handler, *bootstrap.Service, *maintenance.MemoryStore) {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)
	keyStore := authn.NewMemoryKeyStore()
	mustPutKey(t, keyStore, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "pivotal",
		},
		PublicKey: &privateKey.PublicKey,
	})
	bootstrapState := maintenanceRepairBootstrapState(publicKeyPEM)
	coreObjectState := maintenanceRepairCoreObjectState()
	state := bootstrap.NewService(keyStore, bootstrap.Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		InitialCoreObjectState:    &coreObjectState,
	})

	now := func() time.Time {
		return mustParseTime(t, "2026-04-02T15:04:35Z")
	}
	skew := 15 * time.Minute
	maintenanceStore := maintenance.NewMemoryStore(maintenance.WithClock(now))
	router := NewRouter(Dependencies{
		Logger: log.New(io.Discard, "", 0),
		Config: config.Config{
			ServiceName:      "opencook",
			Environment:      "test",
			AuthSkew:         skew,
			MaxAuthBodyBytes: config.DefaultMaxAuthBodyBytes,
		},
		Version:          version.Current(),
		Compat:           compat.NewDefaultRegistry(),
		Now:              now,
		Authn:            authn.NewChefVerifier(keyStore, authn.Options{AllowedClockSkew: &skew, Now: now}),
		Authz:            authz.NewACLAuthorizer(state),
		Bootstrap:        state,
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         pg.New(""),
		Maintenance:      maintenanceStore,
		CookbookBackend:  "memory-bootstrap",
	})
	return router, state, maintenanceStore
}

// maintenanceRepairBootstrapState omits organization, group, container, and
// client ACL documents while preserving enough identity data for signed repair
// requests and follow-up reads.
func maintenanceRepairBootstrapState(publicKeyPEM string) bootstrap.BootstrapCoreState {
	return bootstrap.BootstrapCoreState{
		Users: map[string]bootstrap.User{
			"pivotal": {Username: "pivotal", DisplayName: "pivotal"},
		},
		UserACLs: map[string]authz.ACL{
			"pivotal": {
				Read: authz.Permission{Actors: []string{"pivotal"}},
			},
		},
		UserKeys: map[string]map[string]bootstrap.KeyRecord{
			"pivotal": {
				"default": {
					Name:           "default",
					URI:            "/users/pivotal/keys/default",
					PublicKeyPEM:   publicKeyPEM,
					ExpirationDate: "infinity",
				},
			},
		},
		Orgs: map[string]bootstrap.BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: bootstrap.Organization{Name: "ponyville", FullName: "Ponyville", OrgType: "Business", GUID: "ponyville"},
				Groups: map[string]bootstrap.Group{
					"admins": {Name: "admins", GroupName: "admins", Organization: "ponyville", Users: []string{"pivotal"}, Actors: []string{"pivotal"}},
				},
				Containers: map[string]bootstrap.Container{
					"clients": {Name: "clients", ContainerName: "clients", ContainerPath: "clients"},
				},
				Clients:    map[string]bootstrap.Client{},
				ClientKeys: map[string]map[string]bootstrap.KeyRecord{},
				ACLs:       map[string]authz.ACL{},
			},
		},
	}
}

// maintenanceRepairCoreObjectState omits the node ACL so the test can assert
// live core-object authorization state changes before any process restart.
func maintenanceRepairCoreObjectState() bootstrap.CoreObjectState {
	return bootstrap.CoreObjectState{
		Orgs: map[string]bootstrap.CoreObjectOrganizationState{
			"ponyville": {
				Nodes: map[string]bootstrap.Node{
					"node1": {
						Name:            "node1",
						JSONClass:       "Chef::Node",
						ChefType:        "node",
						ChefEnvironment: "_default",
						Automatic:       map[string]any{},
						Default:         map[string]any{},
						Normal:          map[string]any{},
						Override:        map[string]any{},
						RunList:         []string{},
					},
				},
				Environments: map[string]bootstrap.Environment{},
				Roles:        map[string]bootstrap.Role{},
				DataBags:     map[string]bootstrap.DataBag{},
				DataBagItems: map[string]map[string]bootstrap.DataBagItem{},
				Sandboxes:    map[string]bootstrap.Sandbox{},
				Policies:     map[string]map[string]bootstrap.PolicyRevision{},
				PolicyGroups: map[string]bootstrap.PolicyGroup{},
				ACLs:         map[string]authz.ACL{},
			},
		},
	}
}
