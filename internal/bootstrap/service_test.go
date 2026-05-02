package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var errBootstrapCoreStoreFailed = errors.New("bootstrap core store failed")

type failingBootstrapCoreStore struct{}

func (failingBootstrapCoreStore) LoadBootstrapCore() (BootstrapCoreState, error) {
	return BootstrapCoreState{}, nil
}

func (failingBootstrapCoreStore) SaveBootstrapCore(BootstrapCoreState) error {
	return errBootstrapCoreStoreFailed
}

func TestSeedPublicKeyRejectsUnsupportedPrincipalType(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	err := service.SeedPublicKey(authn.Principal{
		Type: "node",
		Name: "sparkle",
	}, "default", publicKeyPEM)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("SeedPublicKey() error = %v, want %v", err, ErrInvalidInput)
	}

	if _, ok := service.GetUser("sparkle"); ok {
		t.Fatalf("unexpected user seeded for unsupported principal type")
	}
}

func TestSeedPublicKeyRejectsInvalidPrincipalScopes(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	tests := []struct {
		name      string
		principal authn.Principal
	}{
		{
			name: "user with organization",
			principal: authn.Principal{
				Type:         "user",
				Name:         "sparkle",
				Organization: "ponyville",
			},
		},
		{
			name: "client without organization",
			principal: authn.Principal{
				Type: "client",
				Name: "sparkle-client",
			},
		},
		{
			name: "missing name",
			principal: authn.Principal{
				Type: "user",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.SeedPublicKey(tt.principal, "default", publicKeyPEM)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("SeedPublicKey() error = %v, want %v", err, ErrInvalidInput)
			}
		})
	}
}

func TestSeedPublicKeyRejectsEmptyPublicKey(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})

	err := service.SeedPublicKey(authn.Principal{
		Type: "user",
		Name: "sparkle",
	}, "default", "   ")
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("SeedPublicKey() error = %v, want %v", err, ErrInvalidInput)
	}
}

func TestBootstrapCoreStoreCapturesNormalizedCoreState(t *testing.T) {
	coreStore := NewMemoryBootstrapCoreStore(BootstrapCoreState{})
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName: "pivotal",
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return coreStore
		},
	})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	if _, _, err := service.CreateUser(CreateUserInput{
		Username:  "rainbow",
		PublicKey: publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OwnerName: "rainbow",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	state, err := coreStore.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	if state.Users["rainbow"].DisplayName != "rainbow" {
		t.Fatalf("persisted user display name = %q, want normalized fallback", state.Users["rainbow"].DisplayName)
	}
	if _, ok := state.UserKeys["rainbow"]["default"]; !ok {
		t.Fatalf("persisted user default key missing")
	}
	org := state.Orgs["ponyville"]
	if org.Organization.FullName != "Ponyville" {
		t.Fatalf("persisted org full name = %q, want Ponyville", org.Organization.FullName)
	}
	if _, ok := org.Clients["ponyville-validator"]; !ok {
		t.Fatalf("persisted validator client missing")
	}
	if _, ok := org.Groups["admins"]; !ok {
		t.Fatalf("persisted default admins group missing")
	}
	if _, ok := org.Containers["clients"]; !ok {
		t.Fatalf("persisted default clients container missing")
	}
}

func TestBootstrapCoreStoreFailureRollsBackServiceStateAndVerifierCache(t *testing.T) {
	keyStore := authn.NewMemoryKeyStore()
	service := NewService(keyStore, Options{
		SuperuserName: "pivotal",
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return failingBootstrapCoreStore{}
		},
	})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	_, _, err := service.CreateUser(CreateUserInput{
		Username:  "rainbow",
		PublicKey: publicKeyPEM,
	})
	if !errors.Is(err, errBootstrapCoreStoreFailed) {
		t.Fatalf("CreateUser() error = %v, want store failure", err)
	}
	if _, ok := service.GetUser("rainbow"); ok {
		t.Fatalf("GetUser(rainbow) ok = true after failed persistence")
	}
	keys, err := keyStore.Lookup(context.Background(), "rainbow", "")
	if err != nil {
		t.Fatalf("Lookup(rainbow) error = %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("Lookup(rainbow) = %d keys, want verifier cache rollback", len(keys))
	}
}

func TestReloadPersistedStateRefreshesServiceStateAndVerifierCache(t *testing.T) {
	keyStore := authn.NewMemoryKeyStore()
	coreStore := NewMemoryBootstrapCoreStore(reloadBootstrapState("rainbow", mustGeneratePublicKeyPEM(t)))
	objectStore := NewMemoryCoreObjectStore(reloadCoreObjectState("old-node"))
	service := NewService(keyStore, Options{
		SuperuserName: "pivotal",
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return coreStore
		},
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return objectStore
		},
	})

	if err := service.ReloadPersistedState(); err != nil {
		t.Fatalf("ReloadPersistedState(initial) error = %v", err)
	}
	if _, ok := service.GetUser("rainbow"); !ok {
		t.Fatal("GetUser(rainbow) ok = false after initial reload")
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "old-node"); !orgOK || !nodeOK {
		t.Fatalf("GetNode(old-node) orgOK=%v nodeOK=%v, want true/true", orgOK, nodeOK)
	}
	if keys, err := keyStore.Lookup(context.Background(), "rainbow", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(rainbow) keys=%d err=%v, want one verifier key", len(keys), err)
	}

	if err := coreStore.SaveBootstrapCore(reloadBootstrapState("twilight", mustGeneratePublicKeyPEM(t))); err != nil {
		t.Fatalf("SaveBootstrapCore(updated) error = %v", err)
	}
	if err := objectStore.SaveCoreObjects(reloadCoreObjectState("new-node")); err != nil {
		t.Fatalf("SaveCoreObjects(updated) error = %v", err)
	}
	if err := service.ReloadPersistedState(); err != nil {
		t.Fatalf("ReloadPersistedState(updated) error = %v", err)
	}
	if _, ok := service.GetUser("rainbow"); ok {
		t.Fatal("GetUser(rainbow) ok = true after reload removed stale user")
	}
	if keys, err := keyStore.Lookup(context.Background(), "rainbow", ""); err != nil || len(keys) != 0 {
		t.Fatalf("Lookup(rainbow) keys=%d err=%v, want stale verifier key removed", len(keys), err)
	}
	if _, ok := service.GetUser("twilight"); !ok {
		t.Fatal("GetUser(twilight) ok = false after updated reload")
	}
	if keys, err := keyStore.Lookup(context.Background(), "twilight", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(twilight) keys=%d err=%v, want one verifier key", len(keys), err)
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "old-node"); !orgOK || nodeOK {
		t.Fatalf("GetNode(old-node) orgOK=%v nodeOK=%v, want true/false after stale object removal", orgOK, nodeOK)
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "new-node"); !orgOK || !nodeOK {
		t.Fatalf("GetNode(new-node) orgOK=%v nodeOK=%v, want true/true after reload", orgOK, nodeOK)
	}
	if err := service.ReloadPersistedState(); err != nil {
		t.Fatalf("ReloadPersistedState(idempotent) error = %v", err)
	}
}

func TestReloadPersistedStateRollsBackOnVerifierHydrationFailure(t *testing.T) {
	keyStore := authn.NewMemoryKeyStore()
	coreStore := NewMemoryBootstrapCoreStore(reloadBootstrapState("rainbow", mustGeneratePublicKeyPEM(t)))
	objectStore := NewMemoryCoreObjectStore(reloadCoreObjectState("old-node"))
	service := NewService(keyStore, Options{
		SuperuserName: "pivotal",
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return coreStore
		},
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return objectStore
		},
	})
	if err := service.ReloadPersistedState(); err != nil {
		t.Fatalf("ReloadPersistedState(initial) error = %v", err)
	}

	if err := coreStore.SaveBootstrapCore(reloadBootstrapState("broken", "not a pem public key")); err != nil {
		t.Fatalf("SaveBootstrapCore(invalid) error = %v", err)
	}
	if err := objectStore.SaveCoreObjects(reloadCoreObjectState("new-node")); err != nil {
		t.Fatalf("SaveCoreObjects(updated) error = %v", err)
	}
	if err := service.ReloadPersistedState(); err == nil {
		t.Fatal("ReloadPersistedState(invalid key) error = nil, want verifier hydration failure")
	}
	if _, ok := service.GetUser("rainbow"); !ok {
		t.Fatal("GetUser(rainbow) ok = false after failed reload, want previous state restored")
	}
	if keys, err := keyStore.Lookup(context.Background(), "rainbow", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(rainbow) keys=%d err=%v, want previous verifier key restored", len(keys), err)
	}
	if _, ok := service.GetUser("broken"); ok {
		t.Fatal("GetUser(broken) ok = true after failed reload, want rollback")
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "old-node"); !orgOK || !nodeOK {
		t.Fatalf("GetNode(old-node) orgOK=%v nodeOK=%v, want previous object state restored", orgOK, nodeOK)
	}
	if _, orgOK, nodeOK := service.GetNode("ponyville", "new-node"); !orgOK || nodeOK {
		t.Fatalf("GetNode(new-node) orgOK=%v nodeOK=%v, want rollback to hide new object", orgOK, nodeOK)
	}
}

func TestRepairDefaultACLsUpdatesLiveStateThroughStores(t *testing.T) {
	keyStore := authn.NewMemoryKeyStore()
	publicKeyPEM := mustGeneratePublicKeyPEM(t)
	bootstrapState := aclRepairBootstrapStateWithoutACLs(publicKeyPEM)
	coreObjectState := aclRepairCoreObjectStateWithoutACLs()
	bootstrapStore := NewMemoryBootstrapCoreStore(bootstrapState)
	objectStore := NewMemoryCoreObjectStore(coreObjectState)
	service := NewService(keyStore, Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		InitialCoreObjectState:    &coreObjectState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return bootstrapStore
		},
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return objectStore
		},
	})
	if err := service.RehydrateKeyStore(); err != nil {
		t.Fatalf("RehydrateKeyStore() error = %v", err)
	}

	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{Type: "container", Name: "clients", Organization: "ponyville"}); err != nil || ok {
		t.Fatalf("ResolveACL(container before repair) ok/error = %t/%v, want false/nil", ok, err)
	}
	if keys, err := keyStore.Lookup(context.Background(), "pivotal", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(pivotal before repair) keys=%d err=%v, want one verifier key", len(keys), err)
	}

	result, err := service.RepairDefaultACLs(RepairDefaultACLsInput{Organization: "ponyville"})
	if err != nil {
		t.Fatalf("RepairDefaultACLs() error = %v", err)
	}
	if !result.Changed || !contains(result.BootstrapRepaired, "ponyville/container:clients") || !contains(result.CoreObjectRepaired, "ponyville/node:node1") {
		t.Fatalf("RepairDefaultACLs() = %#v, want bootstrap and core object ACL repairs", result)
	}
	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{Type: "container", Name: "clients", Organization: "ponyville"}); err != nil || !ok {
		t.Fatalf("ResolveACL(container after repair) ok/error = %t/%v, want true/nil", ok, err)
	}
	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{Type: "node", Name: "node1", Organization: "ponyville"}); err != nil || !ok {
		t.Fatalf("ResolveACL(node after repair) ok/error = %t/%v, want true/nil", ok, err)
	}
	if keys, err := keyStore.Lookup(context.Background(), "pivotal", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(pivotal after repair) keys=%d err=%v, want verifier cache unchanged", len(keys), err)
	}

	persistedBootstrap, err := bootstrapStore.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	if _, ok := persistedBootstrap.Orgs["ponyville"].ACLs[containerACLKey("clients")]; !ok {
		t.Fatalf("persisted bootstrap ACLs = %v, want clients container ACL", persistedBootstrap.Orgs["ponyville"].ACLs)
	}
	persistedObjects, err := objectStore.LoadCoreObjects()
	if err != nil {
		t.Fatalf("LoadCoreObjects() error = %v", err)
	}
	if _, ok := persistedObjects.Orgs["ponyville"].ACLs[nodeACLKey("node1")]; !ok {
		t.Fatalf("persisted object ACLs = %v, want node ACL", persistedObjects.Orgs["ponyville"].ACLs)
	}
}

func TestRepairDefaultACLsRollsBackWhenCoreObjectPersistenceFails(t *testing.T) {
	bootstrapState := aclRepairBootstrapStateWithoutACLs(mustGeneratePublicKeyPEM(t))
	coreObjectState := aclRepairCoreObjectStateWithoutACLs()
	bootstrapStore := NewMemoryBootstrapCoreStore(bootstrapState)
	objectStore := &controlledCoreObjectStore{delegate: NewMemoryCoreObjectStore(coreObjectState)}
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		InitialCoreObjectState:    &coreObjectState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return bootstrapStore
		},
		CoreObjectStoreFactory: func(*Service) CoreObjectStore {
			return objectStore
		},
	})
	objectStore.fail = true

	if _, err := service.RepairDefaultACLs(RepairDefaultACLsInput{Organization: "ponyville"}); !errors.Is(err, errCoreObjectStoreFailed) {
		t.Fatalf("RepairDefaultACLs() error = %v, want core object store failure", err)
	}
	if _, ok, err := service.ResolveACL(context.Background(), authz.Resource{Type: "container", Name: "clients", Organization: "ponyville"}); err != nil || ok {
		t.Fatalf("ResolveACL(container after failed repair) ok/error = %t/%v, want false/nil", ok, err)
	}
	persistedBootstrap, err := bootstrapStore.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	if _, ok := persistedBootstrap.Orgs["ponyville"].ACLs[containerACLKey("clients")]; ok {
		t.Fatalf("persisted bootstrap ACLs = %v, want rollback without clients ACL", persistedBootstrap.Orgs["ponyville"].ACLs)
	}
}

func TestDeleteClientFailsBeforeMutatingStateWhenKeyDeletionFails(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	service.SeedPrincipal(authn.Principal{Type: "user", Name: "silent-bob"})
	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
	}
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	if _, _, err := service.CreateClient("ponyville", CreateClientInput{
		Name:      "twilight",
		PublicKey: publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateClient() error = %v", err)
	}

	service.mu.Lock()
	service.orgs["ponyville"].clientKeys["twilight"][""] = KeyRecord{Name: "", PublicKeyPEM: publicKeyPEM}
	service.mu.Unlock()

	if _, err := service.DeleteClient("ponyville", "twilight"); err == nil {
		t.Fatalf("DeleteClient() error = nil, want key deletion failure")
	}

	if _, ok := service.GetClient("ponyville", "twilight"); !ok {
		t.Fatalf("client was removed despite key deletion failure")
	}
	keys, orgExists, clientExists := service.ListClientKeys("ponyville", "twilight")
	if !orgExists || !clientExists {
		t.Fatalf("client keys missing after failed delete: org=%v client=%v", orgExists, clientExists)
	}
	if len(keys) == 0 {
		t.Fatalf("client keys unexpectedly removed after failed delete")
	}
}

func TestUpdateClientRefreshesGeneratedACLWhenValidatorFlagChanges(t *testing.T) {
	service := newServiceWithClientForUpdateACLTest(t, false)

	assertClientACL(t, service, "twilight", defaultClientACL("pivotal", "twilight"))
	client, _, err := service.UpdateClient("ponyville", "twilight", UpdateClientInput{Validator: boolPtr(true)})
	if err != nil {
		t.Fatalf("UpdateClient(validator=true) error = %v", err)
	}
	if !client.Validator {
		t.Fatalf("updated client validator = false, want true")
	}
	assertClientACL(t, service, "twilight", defaultClientACL("pivotal"))

	client, _, err = service.UpdateClient("ponyville", "twilight", UpdateClientInput{Validator: boolPtr(false)})
	if err != nil {
		t.Fatalf("UpdateClient(validator=false) error = %v", err)
	}
	if client.Validator {
		t.Fatalf("updated client validator = true, want false")
	}
	assertClientACL(t, service, "twilight", defaultClientACL("pivotal", "twilight"))
}

func TestUpdateClientPreservesCustomACLWhenValidatorFlagChanges(t *testing.T) {
	service := newServiceWithClientForUpdateACLTest(t, false)
	customACL := defaultClientACL("pivotal", "twilight")
	customACL.Read.Actors = append(customACL.Read.Actors, "custom-reader")

	service.mu.Lock()
	service.orgs["ponyville"].acls[clientACLKey("twilight")] = customACL
	service.mu.Unlock()

	client, _, err := service.UpdateClient("ponyville", "twilight", UpdateClientInput{Validator: boolPtr(true)})
	if err != nil {
		t.Fatalf("UpdateClient(validator=true) error = %v", err)
	}
	if !client.Validator {
		t.Fatalf("updated client validator = false, want true")
	}
	assertClientACL(t, service, "twilight", customACL)
}

func TestAddUserToGroupAddsMembershipToAuthzResolution(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
	}
	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "normal-user"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(normal-user) error = %v", err)
	}
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	if err := service.AddUserToGroup("ponyville", "users", "normal-user"); err != nil {
		t.Fatalf("AddUserToGroup() error = %v", err)
	}

	group, ok := service.GetGroup("ponyville", "users")
	if !ok {
		t.Fatalf("GetGroup(users) ok = false, want true")
	}
	if !contains(group.Users, "normal-user") {
		t.Fatalf("group.Users = %v, want normal-user", group.Users)
	}
	if !contains(group.Actors, "normal-user") {
		t.Fatalf("group.Actors = %v, want normal-user", group.Actors)
	}

	groups, err := service.GroupsFor(context.Background(), authz.Subject{
		Type:         "user",
		Name:         "normal-user",
		Organization: "ponyville",
	})
	if err != nil {
		t.Fatalf("GroupsFor() error = %v", err)
	}
	if !contains(groups, "users") {
		t.Fatalf("GroupsFor() = %v, want users membership", groups)
	}
}

func newServiceWithClientForUpdateACLTest(t *testing.T, validator bool) *Service {
	t.Helper()

	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:     "ponyville",
		FullName: "Ponyville",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	if _, _, err := service.CreateClient("ponyville", CreateClientInput{
		Name:      "twilight",
		Validator: validator,
	}); err != nil {
		t.Fatalf("CreateClient() error = %v", err)
	}
	return service
}

func assertClientACL(t *testing.T, service *Service, clientName string, want authz.ACL) {
	t.Helper()

	acl, ok, err := service.ResolveACL(context.Background(), authz.Resource{
		Type:         "client",
		Name:         clientName,
		Organization: "ponyville",
	})
	if err != nil {
		t.Fatalf("ResolveACL(client/%s) error = %v", clientName, err)
	}
	if !ok {
		t.Fatalf("ResolveACL(client/%s) ok = false, want true", clientName)
	}
	if !reflect.DeepEqual(acl, want) {
		t.Fatalf("ResolveACL(client/%s) = %#v, want %#v", clientName, acl, want)
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func TestAddUserToGroupRejectsMissingMembersAndScopes(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustGeneratePublicKeyPEM(t)

	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
	}
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	tests := []struct {
		name      string
		orgName   string
		groupName string
		username  string
		wantErr   error
	}{
		{
			name:      "missing user",
			orgName:   "ponyville",
			groupName: "users",
			username:  "normal-user",
			wantErr:   ErrNotFound,
		},
		{
			name:      "missing group",
			orgName:   "ponyville",
			groupName: "missing",
			username:  "silent-bob",
			wantErr:   ErrNotFound,
		},
		{
			name:      "missing organization",
			orgName:   "missing",
			groupName: "users",
			username:  "silent-bob",
			wantErr:   ErrNotFound,
		},
		{
			name:      "empty username",
			orgName:   "ponyville",
			groupName: "users",
			username:  " ",
			wantErr:   ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := service.AddUserToGroup(tt.orgName, tt.groupName, tt.username)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("AddUserToGroup() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// reloadBootstrapState builds a minimal persisted identity snapshot for reload
// tests so verifier-cache assertions can focus on one user/key at a time.
func reloadBootstrapState(username, publicKeyPEM string) BootstrapCoreState {
	return BootstrapCoreState{
		Users: map[string]User{
			username: {
				Username:    username,
				DisplayName: username,
			},
		},
		UserACLs: map[string]authz.ACL{
			username: defaultUserACL("pivotal", username),
		},
		UserKeys: map[string]map[string]KeyRecord{
			username: {
				"default": {
					Name:           "default",
					URI:            "/users/" + username + "/keys/default",
					PublicKeyPEM:   publicKeyPEM,
					ExpirationDate: "infinity",
				},
			},
		},
		Orgs: map[string]BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: Organization{
					Name:     "ponyville",
					FullName: "Ponyville",
					OrgType:  "Business",
					GUID:     "ponyville",
				},
				Clients:    map[string]Client{},
				ClientKeys: map[string]map[string]KeyRecord{},
				Groups:     map[string]Group{},
				Containers: map[string]Container{},
				ACLs:       map[string]authz.ACL{organizationACLKey(): defaultOrganizationACL("pivotal")},
			},
		},
	}
}

// reloadCoreObjectState builds a minimal persisted object snapshot tied to the
// same test organization as reloadBootstrapState.
func reloadCoreObjectState(nodeName string) CoreObjectState {
	return CoreObjectState{
		Orgs: map[string]CoreObjectOrganizationState{
			"ponyville": {
				Environments: map[string]Environment{
					defaultEnvironmentName: defaultEnvironment(),
				},
				Nodes: map[string]Node{
					nodeName: {
						Name:            nodeName,
						JSONClass:       "Chef::Node",
						ChefType:        "node",
						ChefEnvironment: defaultEnvironmentName,
						Automatic:       map[string]any{},
						Default:         map[string]any{},
						Normal:          map[string]any{},
						Override:        map[string]any{},
						RunList:         []string{},
					},
				},
				Roles:        map[string]Role{},
				DataBags:     map[string]DataBag{},
				DataBagItems: map[string]map[string]DataBagItem{},
				Sandboxes:    map[string]Sandbox{},
				Policies:     map[string]map[string]PolicyRevision{},
				PolicyGroups: map[string]PolicyGroup{},
				ACLs: map[string]authz.ACL{
					environmentACLKey(defaultEnvironmentName): defaultEnvironmentACL("pivotal", authn.Principal{Type: "user", Name: "pivotal"}),
				},
			},
		},
	}
}

// aclRepairBootstrapStateWithoutACLs builds intentionally incomplete
// bootstrap-core state so online repair tests can prove missing ACLs are filled
// without relying on route create helpers that would generate them.
func aclRepairBootstrapStateWithoutACLs(publicKeyPEM string) BootstrapCoreState {
	return BootstrapCoreState{
		Users: map[string]User{
			"pivotal": {
				Username:    "pivotal",
				DisplayName: "pivotal",
			},
		},
		UserACLs: map[string]authz.ACL{
			"pivotal": defaultUserACL("pivotal", "pivotal"),
		},
		UserKeys: map[string]map[string]KeyRecord{
			"pivotal": {
				"default": {
					Name:           "default",
					URI:            "/users/pivotal/keys/default",
					PublicKeyPEM:   publicKeyPEM,
					ExpirationDate: "infinity",
				},
			},
		},
		Orgs: map[string]BootstrapCoreOrganizationState{
			"ponyville": {
				Organization: Organization{
					Name:     "ponyville",
					FullName: "Ponyville",
					OrgType:  "Business",
					GUID:     "ponyville",
				},
				Clients:    map[string]Client{},
				ClientKeys: map[string]map[string]KeyRecord{},
				Groups: map[string]Group{
					"admins": {
						Name:         "admins",
						GroupName:    "admins",
						Organization: "ponyville",
						Users:        []string{"pivotal"},
						Actors:       []string{"pivotal"},
					},
				},
				Containers: map[string]Container{
					"clients": {
						Name:          "clients",
						ContainerName: "clients",
						ContainerPath: "clients",
					},
				},
				ACLs: map[string]authz.ACL{},
			},
		},
	}
}

// aclRepairCoreObjectStateWithoutACLs builds one node without its ACL so repair
// tests cover core-object authorization state as well as bootstrap-core ACLs.
func aclRepairCoreObjectStateWithoutACLs() CoreObjectState {
	return CoreObjectState{
		Orgs: map[string]CoreObjectOrganizationState{
			"ponyville": {
				Nodes: map[string]Node{
					"node1": {
						Name:            "node1",
						JSONClass:       "Chef::Node",
						ChefType:        "node",
						ChefEnvironment: defaultEnvironmentName,
						Automatic:       map[string]any{},
						Default:         map[string]any{},
						Normal:          map[string]any{},
						Override:        map[string]any{},
						RunList:         []string{},
					},
				},
				Environments: map[string]Environment{},
				Roles:        map[string]Role{},
				DataBags:     map[string]DataBag{},
				DataBagItems: map[string]map[string]DataBagItem{},
				Sandboxes:    map[string]Sandbox{},
				Policies:     map[string]map[string]PolicyRevision{},
				PolicyGroups: map[string]PolicyGroup{},
				ACLs:         map[string]authz.ACL{},
			},
		},
	}
}

func mustGeneratePublicKeyPEM(t *testing.T) string {
	t.Helper()

	_, publicKeyPEM, _, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("generateRSAKeyPair() error = %v", err)
	}

	return publicKeyPEM
}
