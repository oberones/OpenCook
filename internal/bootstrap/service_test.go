package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

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
