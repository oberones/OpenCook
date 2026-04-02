package bootstrap

import (
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
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

func mustGeneratePublicKeyPEM(t *testing.T) string {
	t.Helper()

	_, publicKeyPEM, _, err := generateRSAKeyPair()
	if err != nil {
		t.Fatalf("generateRSAKeyPair() error = %v", err)
	}

	return publicKeyPEM
}
