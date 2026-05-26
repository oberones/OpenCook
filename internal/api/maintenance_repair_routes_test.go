package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestMaintenanceRepairOrgMembershipRepairsLiveStateWithoutRestart(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)
	enableMaintenanceRepair(t, store)

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairOrgMembershipPath, []byte(`{"yes":true,"action":"add-user","org":"ponyville","user":"rarity","admin":true}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org membership repair status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := decodeMaintenanceRepairPayload(t, rec.Body.Bytes())
	if payload["operation"] != "org-membership-repair" || payload["changed"] != true || payload["verifier_cache"] != "unchanged" {
		t.Fatalf("org membership repair payload = %v, want changed online repair", payload)
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{"admins", "users"})

	removeReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairOrgMembershipPath, []byte(`{"yes":true,"action":"remove-user","org":"ponyville","user":"rarity"}`))
	removeRec := httptest.NewRecorder()
	router.ServeHTTP(removeRec, removeReq)
	if removeRec.Code != http.StatusOK {
		t.Fatalf("org membership remove status = %d, want %d, body = %s", removeRec.Code, http.StatusOK, removeRec.Body.String())
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{})
}

func TestMaintenanceRepairOrgMembershipRequiresMaintenanceConfirmationAndValidUser(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)

	inactive := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairOrgMembershipPath, []byte(`{"yes":true,"action":"add-user","org":"ponyville","user":"rarity"}`))
	inactiveRec := httptest.NewRecorder()
	router.ServeHTTP(inactiveRec, inactive)
	if inactiveRec.Code != http.StatusConflict {
		t.Fatalf("inactive org repair status = %d, want %d, body = %s", inactiveRec.Code, http.StatusConflict, inactiveRec.Body.String())
	}

	enableMaintenanceRepair(t, store)
	missingConfirmation := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairOrgMembershipPath, []byte(`{"action":"add-user","org":"ponyville","user":"rarity"}`))
	missingConfirmationRec := httptest.NewRecorder()
	router.ServeHTTP(missingConfirmationRec, missingConfirmation)
	if missingConfirmationRec.Code != http.StatusBadRequest {
		t.Fatalf("missing confirmation status = %d, want %d, body = %s", missingConfirmationRec.Code, http.StatusBadRequest, missingConfirmationRec.Body.String())
	}

	missingUser := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairOrgMembershipPath, []byte(`{"yes":true,"action":"add-user","org":"ponyville","user":"missing"}`))
	missingUserRec := httptest.NewRecorder()
	router.ServeHTTP(missingUserRec, missingUser)
	if missingUserRec.Code != http.StatusNotFound {
		t.Fatalf("missing user status = %d, want %d, body = %s", missingUserRec.Code, http.StatusNotFound, missingUserRec.Body.String())
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{})
}

func TestMaintenanceRepairGroupMembershipRepairsActorTypes(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)
	enableMaintenanceRepair(t, store)

	clientReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairGroupMembershipPath, []byte(`{"yes":true,"action":"add-actor","org":"ponyville","group":"clients","actor_type":"client","actor":"web01"}`))
	clientRec := httptest.NewRecorder()
	router.ServeHTTP(clientRec, clientReq)
	if clientRec.Code != http.StatusOK {
		t.Fatalf("group client repair status = %d, want %d, body = %s", clientRec.Code, http.StatusOK, clientRec.Body.String())
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "client", Name: "web01", Organization: "ponyville"}, []string{"clients"})

	groupReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairGroupMembershipPath, []byte(`{"yes":true,"action":"add-actor","org":"ponyville","group":"admins","actor_type":"group","actor":"clients"}`))
	groupRec := httptest.NewRecorder()
	router.ServeHTTP(groupRec, groupReq)
	if groupRec.Code != http.StatusOK {
		t.Fatalf("group nested repair status = %d, want %d, body = %s", groupRec.Code, http.StatusOK, groupRec.Body.String())
	}
	payload := decodeMaintenanceRepairPayload(t, groupRec.Body.Bytes())
	if payload["operation"] != "group-membership-repair" || payload["changed"] != true {
		t.Fatalf("group nested repair payload = %v, want changed group repair", payload)
	}

	invalidReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairGroupMembershipPath, []byte(`{"yes":true,"action":"add-actor","org":"ponyville","group":"clients","actor_type":"node","actor":"node1"}`))
	invalidRec := httptest.NewRecorder()
	router.ServeHTTP(invalidRec, invalidReq)
	if invalidRec.Code != http.StatusBadRequest {
		t.Fatalf("invalid actor type status = %d, want %d, body = %s", invalidRec.Code, http.StatusBadRequest, invalidRec.Body.String())
	}
}

func TestInternalServerAdminsListAndRepair(t *testing.T) {
	router, state, store := newMaintenanceRepairRouter(t)

	listReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, internalAdminServerAdminsPath, nil)
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("server-admin list status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	if admins := decodeMaintenanceRepairPayload(t, listRec.Body.Bytes())["server_admins"]; !reflect.DeepEqual(admins, []any{"pivotal"}) {
		t.Fatalf("server_admins = %v, want pivotal", admins)
	}

	enableMaintenanceRepair(t, store)
	grantReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairServerAdminsPath, []byte(`{"yes":true,"action":"grant","user":"rarity"}`))
	grantRec := httptest.NewRecorder()
	router.ServeHTTP(grantRec, grantReq)
	if grantRec.Code != http.StatusOK {
		t.Fatalf("server-admin grant status = %d, want %d, body = %s", grantRec.Code, http.StatusOK, grantRec.Body.String())
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{"admins"})

	revokeReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, maintenanceRepairServerAdminsPath, []byte(`{"yes":true,"action":"revoke","user":"rarity"}`))
	revokeRec := httptest.NewRecorder()
	router.ServeHTTP(revokeRec, revokeReq)
	if revokeRec.Code != http.StatusOK {
		t.Fatalf("server-admin revoke status = %d, want %d, body = %s", revokeRec.Code, http.StatusOK, revokeRec.Body.String())
	}
	assertMaintenanceRepairGroups(t, state, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{})
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

func enableMaintenanceRepair(t *testing.T, store *maintenance.MemoryStore) {
	t.Helper()

	if _, err := store.Enable(context.Background(), maintenance.EnableInput{
		Reason: "online repair",
		Mode:   "repair",
		Actor:  "operator",
	}); err != nil {
		t.Fatalf("maintenance Enable() error = %v", err)
	}
}

func decodeMaintenanceRepairPayload(t *testing.T, raw []byte) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("json.Unmarshal(repair payload) error = %v", err)
	}
	return payload
}

func assertMaintenanceRepairGroups(t *testing.T, state *bootstrap.Service, subject authz.Subject, want []string) {
	t.Helper()

	groups, err := state.GroupsFor(context.Background(), subject)
	if err != nil {
		t.Fatalf("GroupsFor(%#v) error = %v", subject, err)
	}
	if groups == nil {
		groups = []string{}
	}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("GroupsFor(%#v) = %v, want %v", subject, groups, want)
	}
}

// maintenanceRepairBootstrapState omits organization, group, container, and
// client ACL documents while preserving enough identity data for signed repair
// requests and follow-up reads.
func maintenanceRepairBootstrapState(publicKeyPEM string) bootstrap.BootstrapCoreState {
	return bootstrap.BootstrapCoreState{
		Users: map[string]bootstrap.User{
			"pivotal": {Username: "pivotal", DisplayName: "pivotal"},
			"rarity":  {Username: "rarity", DisplayName: "rarity"},
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
					"users":  {Name: "users", GroupName: "users", Organization: "ponyville"},
					"clients": {
						Name:         "clients",
						GroupName:    "clients",
						Organization: "ponyville",
					},
				},
				Containers: map[string]bootstrap.Container{
					"clients": {Name: "clients", ContainerName: "clients", ContainerPath: "clients"},
				},
				Clients: map[string]bootstrap.Client{
					"web01": {Name: "web01", ClientName: "web01", Organization: "ponyville"},
				},
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
