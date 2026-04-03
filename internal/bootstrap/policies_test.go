package bootstrap

import (
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreatePolicyRevisionAndPolicyGroupAssignment(t *testing.T) {
	state := newPolicyTestService(t)

	revision, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: map[string]any{
			"name":        "appserver",
			"revision_id": "1111111111111111111111111111111111111111",
			"run_list":    []any{"recipe[demo::default]"},
			"cookbook_locks": map[string]any{
				"demo": map[string]any{
					"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
					"version":    "1.2.3",
				},
			},
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})
	if err != nil {
		t.Fatalf("CreatePolicyRevision() error = %v", err)
	}
	if revision.Name != "appserver" || revision.RevisionID != "1111111111111111111111111111111111111111" {
		t.Fatalf("revision = %+v, want appserver/111...1", revision)
	}

	groupRevision, created, err := state.UpsertPolicyGroupAssignment("ponyville", "dev", "appserver", UpdatePolicyGroupAssignmentInput{
		Payload: revision.Payload,
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})
	if err != nil {
		t.Fatalf("UpsertPolicyGroupAssignment() error = %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if groupRevision.RevisionID != revision.RevisionID {
		t.Fatalf("group revision id = %q, want %q", groupRevision.RevisionID, revision.RevisionID)
	}

	groups, orgExists, policyExists, revisionExists := state.PolicyGroupsForRevision("ponyville", "appserver", revision.RevisionID)
	if !orgExists || !policyExists || !revisionExists {
		t.Fatalf("PolicyGroupsForRevision existence = %t/%t/%t, want true/true/true", orgExists, policyExists, revisionExists)
	}
	if len(groups) != 1 || groups[0] != "dev" {
		t.Fatalf("groups = %v, want [dev]", groups)
	}
}

func TestCreatePolicyRevisionRejectsInvalidRunList(t *testing.T) {
	state := newPolicyTestService(t)

	_, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: map[string]any{
			"name":        "appserver",
			"revision_id": "1111111111111111111111111111111111111111",
			"run_list":    []any{"recipe[demo]"},
			"cookbook_locks": map[string]any{
				"demo": map[string]any{
					"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
					"version":    "1.2.3",
				},
			},
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("CreatePolicyRevision() error = %v, want ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'run_list' is not a valid run list" {
		t.Fatalf("validation messages = %v, want run_list validation", validationErr.Messages)
	}
}

func newPolicyTestService(t *testing.T) *Service {
	t.Helper()

	state := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := state.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	return state
}
