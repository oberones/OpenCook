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

type countingBootstrapCoreStore struct {
	delegate *MemoryBootstrapCoreStore
	saves    int
}

func newCountingBootstrapCoreStore(initial BootstrapCoreState) *countingBootstrapCoreStore {
	return &countingBootstrapCoreStore{delegate: NewMemoryBootstrapCoreStore(initial)}
}

func (s *countingBootstrapCoreStore) LoadBootstrapCore() (BootstrapCoreState, error) {
	return s.delegate.LoadBootstrapCore()
}

func (s *countingBootstrapCoreStore) SaveBootstrapCore(state BootstrapCoreState) error {
	s.saves++
	return s.delegate.SaveBootstrapCore(state)
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

func TestSeedPrincipalSkipsPersistenceWhenUserAlreadyExists(t *testing.T) {
	initial := BootstrapCoreState{
		Users: map[string]User{
			"pivotal": {Username: "pivotal", DisplayName: "pivotal"},
		},
		UserACLs: map[string]authz.ACL{
			"pivotal": defaultUserACL("pivotal", "pivotal"),
		},
		UserKeys: map[string]map[string]KeyRecord{},
		Orgs:     map[string]BootstrapCoreOrganizationState{},
	}
	store := newCountingBootstrapCoreStore(initial)
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &initial,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return store
		},
	})

	service.SeedPrincipal(authn.Principal{Type: "user", Name: "pivotal"})

	if store.saves != 0 {
		t.Fatalf("SeedPrincipal(existing) saved bootstrap core %d times, want 0", store.saves)
	}
}

func TestSeedPublicKeySkipsPersistenceWhenRecordAlreadyMatches(t *testing.T) {
	publicKeyPEM := mustGeneratePublicKeyPEM(t)
	initial := BootstrapCoreState{
		Users: map[string]User{
			"pivotal": {Username: "pivotal", DisplayName: "pivotal"},
		},
		UserACLs: map[string]authz.ACL{
			"pivotal": defaultUserACL("pivotal", "pivotal"),
		},
		UserKeys: map[string]map[string]KeyRecord{
			"pivotal": {
				"default": {
					Name:           "default",
					URI:            keyURI(authn.Principal{Type: "user", Name: "pivotal"}, "default"),
					PublicKeyPEM:   publicKeyPEM,
					ExpirationDate: "infinity",
				},
			},
		},
		Orgs: map[string]BootstrapCoreOrganizationState{},
	}
	store := newCountingBootstrapCoreStore(initial)
	keyStore := authn.NewMemoryKeyStore()
	service := NewService(keyStore, Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &initial,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return store
		},
	})

	if err := service.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(existing) error = %v", err)
	}

	if store.saves != 0 {
		t.Fatalf("SeedPublicKey(existing) saved bootstrap core %d times, want 0", store.saves)
	}
	if keys, err := keyStore.Lookup(context.Background(), "pivotal", ""); err != nil || len(keys) != 1 {
		t.Fatalf("Lookup(pivotal) keys=%d err=%v, want verifier key hydrated", len(keys), err)
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

func TestRepairOrgMembershipUpdatesLiveStateThroughStore(t *testing.T) {
	bootstrapState := membershipRepairBootstrapState()
	bootstrapStore := NewMemoryBootstrapCoreStore(bootstrapState)
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return bootstrapStore
		},
	})

	result, err := service.RepairOrgMembership(RepairOrgMembershipInput{
		Action:       "add-user",
		Organization: "ponyville",
		Username:     "rarity",
		Admin:        true,
	})
	if err != nil {
		t.Fatalf("RepairOrgMembership(add-user) error = %v", err)
	}
	if !result.Changed || !contains(result.Members, "admins/user:rarity") || !contains(result.Members, "users/user:rarity") {
		t.Fatalf("RepairOrgMembership(add-user) = %#v, want admins and users memberships", result)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{"admins", "users"})

	persisted, err := bootstrapStore.LoadBootstrapCore()
	if err != nil {
		t.Fatalf("LoadBootstrapCore() error = %v", err)
	}
	if !contains(persisted.Orgs["ponyville"].Groups["admins"].Users, "rarity") || !contains(persisted.Orgs["ponyville"].Groups["users"].Users, "rarity") {
		t.Fatalf("persisted ponyville groups = %#v, want rarity persisted", persisted.Orgs["ponyville"].Groups)
	}

	idempotent, err := service.RepairOrgMembership(RepairOrgMembershipInput{
		Action:       "add-user",
		Organization: "ponyville",
		Username:     "rarity",
		Admin:        true,
	})
	if err != nil {
		t.Fatalf("RepairOrgMembership(idempotent add-user) error = %v", err)
	}
	if idempotent.Changed || len(idempotent.Members) != 0 {
		t.Fatalf("RepairOrgMembership(idempotent add-user) = %#v, want no change", idempotent)
	}

	removed, err := service.RepairOrgMembership(RepairOrgMembershipInput{
		Action:       "remove-user",
		Organization: "ponyville",
		Username:     "rarity",
	})
	if err != nil {
		t.Fatalf("RepairOrgMembership(remove-user) error = %v", err)
	}
	if !removed.Changed || !contains(removed.Members, "admins/user:rarity") || !contains(removed.Members, "users/user:rarity") {
		t.Fatalf("RepairOrgMembership(remove-user) = %#v, want removed memberships", removed)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{})
}

func TestRepairGroupMembershipUpdatesLiveStateThroughStore(t *testing.T) {
	bootstrapState := membershipRepairBootstrapState()
	bootstrapStore := NewMemoryBootstrapCoreStore(bootstrapState)
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return bootstrapStore
		},
	})

	result, err := service.RepairGroupMembership(RepairGroupMembershipInput{
		Action:       "add-actor",
		Organization: "ponyville",
		Group:        "clients",
		ActorType:    "client",
		Actor:        "web01",
	})
	if err != nil {
		t.Fatalf("RepairGroupMembership(add-actor client) error = %v", err)
	}
	if !result.Changed || !contains(result.Members, "client:web01") {
		t.Fatalf("RepairGroupMembership(add-actor client) = %#v, want client membership", result)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "client", Name: "web01", Organization: "ponyville"}, []string{"clients"})

	nested, err := service.RepairGroupMembership(RepairGroupMembershipInput{
		Action:       "add-actor",
		Organization: "ponyville",
		Group:        "admins",
		ActorType:    "group",
		Actor:        "clients",
	})
	if err != nil {
		t.Fatalf("RepairGroupMembership(add-actor group) error = %v", err)
	}
	if !nested.Changed || !contains(nested.Members, "group:clients") {
		t.Fatalf("RepairGroupMembership(add-actor group) = %#v, want nested group membership", nested)
	}

	removed, err := service.RepairGroupMembership(RepairGroupMembershipInput{
		Action:       "remove-actor",
		Organization: "ponyville",
		Group:        "clients",
		ActorType:    "client",
		Actor:        "web01",
	})
	if err != nil {
		t.Fatalf("RepairGroupMembership(remove-actor client) error = %v", err)
	}
	if !removed.Changed || !contains(removed.Members, "client:web01") {
		t.Fatalf("RepairGroupMembership(remove-actor client) = %#v, want removed membership", removed)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "client", Name: "web01", Organization: "ponyville"}, []string{})
}

func TestRepairServerAdminMembershipUpdatesAllOrgAdmins(t *testing.T) {
	bootstrapState := membershipRepairBootstrapState()
	bootstrapStore := NewMemoryBootstrapCoreStore(bootstrapState)
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return bootstrapStore
		},
	})

	result, err := service.RepairServerAdminMembership(RepairServerAdminMembershipInput{Action: "grant", Username: "rarity"})
	if err != nil {
		t.Fatalf("RepairServerAdminMembership(grant) error = %v", err)
	}
	if !result.Changed || !contains(result.Members, "canterlot/admins/user:rarity") || !contains(result.Members, "ponyville/admins/user:rarity") {
		t.Fatalf("RepairServerAdminMembership(grant) = %#v, want all org admin memberships", result)
	}
	if admins := service.ListServerAdmins(); !reflect.DeepEqual(admins, []string{"pivotal", "rarity"}) {
		t.Fatalf("ListServerAdmins() = %v, want pivotal and rarity", admins)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "user", Name: "rarity", Organization: "canterlot"}, []string{"admins"})

	removed, err := service.RepairServerAdminMembership(RepairServerAdminMembershipInput{Action: "revoke", Username: "rarity"})
	if err != nil {
		t.Fatalf("RepairServerAdminMembership(revoke) error = %v", err)
	}
	if !removed.Changed || !contains(removed.Members, "canterlot/admins/user:rarity") || !contains(removed.Members, "ponyville/admins/user:rarity") {
		t.Fatalf("RepairServerAdminMembership(revoke) = %#v, want removed all org admin memberships", removed)
	}
	if admins := service.ListServerAdmins(); !reflect.DeepEqual(admins, []string{"pivotal"}) {
		t.Fatalf("ListServerAdmins() = %v, want pivotal after revoke", admins)
	}
}

func TestRepairMembershipRollsBackWhenPersistenceFails(t *testing.T) {
	bootstrapState := membershipRepairBootstrapState()
	service := NewService(authn.NewMemoryKeyStore(), Options{
		SuperuserName:             "pivotal",
		InitialBootstrapCoreState: &bootstrapState,
		BootstrapCoreStoreFactory: func(*Service) BootstrapCoreStore {
			return failingBootstrapCoreStore{}
		},
	})

	_, err := service.RepairOrgMembership(RepairOrgMembershipInput{
		Action:       "add-user",
		Organization: "ponyville",
		Username:     "rarity",
	})
	if !errors.Is(err, errBootstrapCoreStoreFailed) {
		t.Fatalf("RepairOrgMembership() error = %v, want bootstrap store failure", err)
	}
	assertSubjectGroups(t, service, authz.Subject{Type: "user", Name: "rarity", Organization: "ponyville"}, []string{})
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

func assertSubjectGroups(t *testing.T, service *Service, subject authz.Subject, want []string) {
	t.Helper()

	groups, err := service.GroupsFor(context.Background(), subject)
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

// membershipRepairBootstrapState provides two orgs sharing the default admin
// topology so membership repair tests can verify live authz and persistence.
func membershipRepairBootstrapState() BootstrapCoreState {
	return BootstrapCoreState{
		Users: map[string]User{
			"pivotal": {Username: "pivotal", DisplayName: "pivotal"},
			"rarity":  {Username: "rarity", DisplayName: "rarity"},
		},
		UserACLs: map[string]authz.ACL{
			"pivotal": defaultUserACL("pivotal", "pivotal"),
			"rarity":  defaultUserACL("pivotal", "rarity"),
		},
		UserKeys: map[string]map[string]KeyRecord{},
		Orgs: map[string]BootstrapCoreOrganizationState{
			"ponyville": membershipRepairOrgState("ponyville"),
			"canterlot": membershipRepairOrgState("canterlot"),
		},
	}
}

func membershipRepairOrgState(name string) BootstrapCoreOrganizationState {
	return BootstrapCoreOrganizationState{
		Organization: Organization{
			Name:     name,
			FullName: name,
			OrgType:  "Business",
			GUID:     name,
		},
		Clients: map[string]Client{
			"web01": {
				Name:         "web01",
				ClientName:   "web01",
				Organization: name,
			},
		},
		ClientKeys: map[string]map[string]KeyRecord{},
		Groups: map[string]Group{
			"admins": {
				Name:         "admins",
				GroupName:    "admins",
				Organization: name,
				Users:        []string{"pivotal"},
				Actors:       []string{"pivotal"},
			},
			"users": {
				Name:         "users",
				GroupName:    "users",
				Organization: name,
			},
			"clients": {
				Name:         "clients",
				GroupName:    "clients",
				Organization: name,
			},
		},
		Containers: map[string]Container{},
		ACLs:       map[string]authz.ACL{organizationACLKey(): defaultOrganizationACL("pivotal")},
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
