package bootstrap

import (
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateSandboxNormalizesChecksumsAndDeleteLifecycle(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	sandbox, err := service.CreateSandbox("ponyville", CreateSandboxInput{
		Checksums: []string{
			"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	if len(sandbox.Checksums) != 2 {
		t.Fatalf("len(Checksums) = %d, want 2", len(sandbox.Checksums))
	}
	if sandbox.Checksums[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || sandbox.Checksums[1] != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("Checksums = %v, want normalized sorted unique checksums", sandbox.Checksums)
	}

	stored, orgExists, sandboxExists := service.GetSandbox("ponyville", sandbox.ID)
	if !orgExists || !sandboxExists {
		t.Fatalf("GetSandbox existence = %t/%t, want true/true", orgExists, sandboxExists)
	}
	if stored.ID != sandbox.ID {
		t.Fatalf("stored sandbox id = %q, want %q", stored.ID, sandbox.ID)
	}

	deleted, err := service.DeleteSandbox("ponyville", sandbox.ID)
	if err != nil {
		t.Fatalf("DeleteSandbox() error = %v", err)
	}
	if deleted.ID != sandbox.ID {
		t.Fatalf("deleted sandbox id = %q, want %q", deleted.ID, sandbox.ID)
	}

	_, _, sandboxExists = service.GetSandbox("ponyville", sandbox.ID)
	if sandboxExists {
		t.Fatal("sandboxExists = true, want false after delete")
	}
}

func TestCreateSandboxRejectsInvalidChecksums(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	_, err := service.CreateSandbox("ponyville", CreateSandboxInput{
		Checksums: []string{"not-a-checksum"},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("CreateSandbox() error = %v, want ErrInvalidInput", err)
	}
}
