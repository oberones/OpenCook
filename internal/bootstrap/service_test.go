package bootstrap

import (
	"context"
	"errors"
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

func mustGeneratePublicKeyPEM(t *testing.T) string {
	t.Helper()

	_, publicKeyPEM, _, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("generateRSAKeyPair() error = %v", err)
	}

	return publicKeyPEM
}
