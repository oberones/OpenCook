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

func TestCreatePolicyRevisionAcceptsCanonicalPolicyPayload(t *testing.T) {
	state := newPolicyTestService(t)

	revision, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: canonicalPolicyPayload("appserver", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})
	if err != nil {
		t.Fatalf("CreatePolicyRevision() error = %v", err)
	}

	namedRunLists, ok := revision.Payload["named_run_lists"].(map[string]any)
	if !ok {
		t.Fatalf("named_run_lists = %T, want map[string]any", revision.Payload["named_run_lists"])
	}
	updateJenkins, ok := namedRunLists["update_jenkins"].([]any)
	if !ok || len(updateJenkins) != 1 || updateJenkins[0] != "recipe[policyfile_demo::other_recipe]" {
		t.Fatalf("named_run_lists[update_jenkins] = %v, want canonical recipe list", namedRunLists["update_jenkins"])
	}

	locks := revision.Payload["cookbook_locks"].(map[string]any)
	lock := locks["policyfile_demo"].(map[string]any)
	if lock["dotted_decimal_identifier"] != "67638399371010690.23642238397896298.25512023620585" {
		t.Fatalf("dotted_decimal_identifier = %v, want canonical value", lock["dotted_decimal_identifier"])
	}
	if _, ok := lock["scm_info"].(map[string]any); !ok {
		t.Fatalf("scm_info = %T, want map[string]any", lock["scm_info"])
	}
	if _, ok := lock["source_options"].(map[string]any); !ok {
		t.Fatalf("source_options = %T, want map[string]any", lock["source_options"])
	}

	solutionDependencies, ok := revision.Payload["solution_dependencies"].(map[string]any)
	if !ok {
		t.Fatalf("solution_dependencies = %T, want map[string]any", revision.Payload["solution_dependencies"])
	}
	if _, ok := solutionDependencies["Policyfile"].([]any); !ok {
		t.Fatalf("solution_dependencies[Policyfile] = %T, want []any", solutionDependencies["Policyfile"])
	}
}

func TestCreatePolicyRevisionRejectsMissingCookbookLockVersion(t *testing.T) {
	state := newPolicyTestService(t)

	_, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: map[string]any{
			"name":        "appserver",
			"revision_id": "cccccccccccccccccccccccccccccccccccccccc",
			"run_list":    []any{"recipe[demo::default]"},
			"cookbook_locks": map[string]any{
				"demo": map[string]any{
					"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				},
			},
		},
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("CreatePolicyRevision() error = %v, want ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "Field 'version' missing" {
		t.Fatalf("validation messages = %v, want missing version message", validationErr.Messages)
	}
}

func TestCreatePolicyRevisionAcceptsStringNamedRunLists(t *testing.T) {
	state := newPolicyTestService(t)

	revision, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: map[string]any{
			"name":        "appserver",
			"revision_id": "dddddddddddddddddddddddddddddddddddddddd",
			"run_list":    []any{"recipe[demo::default]"},
			"named_run_lists": map[string][]string{
				"deploy": {"recipe[demo::deploy]"},
			},
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

	namedRunLists := revision.Payload["named_run_lists"].(map[string]any)
	deployRunList := namedRunLists["deploy"].([]any)
	if len(deployRunList) != 1 || deployRunList[0] != "recipe[demo::deploy]" {
		t.Fatalf("deploy named run list = %v, want [recipe[demo::deploy]]", deployRunList)
	}
}

func TestUpsertPolicyGroupAssignmentRejectsConflictingExistingRevision(t *testing.T) {
	state := newPolicyTestService(t)

	originalPayload := map[string]any{
		"name":        "appserver",
		"revision_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"run_list":    []any{"recipe[demo::default]"},
		"cookbook_locks": map[string]any{
			"demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "1.2.3",
			},
		},
	}
	if _, err := state.CreatePolicyRevision("ponyville", "appserver", CreatePolicyRevisionInput{
		Payload: originalPayload,
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	}); err != nil {
		t.Fatalf("CreatePolicyRevision() error = %v", err)
	}

	conflictingPayload := map[string]any{
		"name":        "appserver",
		"revision_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"run_list":    []any{"recipe[demo::other]"},
		"cookbook_locks": map[string]any{
			"demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "9.9.9",
			},
		},
	}

	_, _, err := state.UpsertPolicyGroupAssignment("ponyville", "dev", "appserver", UpdatePolicyGroupAssignmentInput{
		Payload: conflictingPayload,
		Creator: authn.Principal{Type: "user", Name: "silent-bob"},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("UpsertPolicyGroupAssignment() error = %v, want ErrConflict", err)
	}

	stored, orgExists, policyExists, revisionExists := state.GetPolicyRevision("ponyville", "appserver", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if !orgExists || !policyExists || !revisionExists {
		t.Fatalf("GetPolicyRevision existence = %t/%t/%t, want true/true/true", orgExists, policyExists, revisionExists)
	}
	runList := stored.Payload["run_list"].([]any)
	if len(runList) != 1 || runList[0] != "recipe[demo::default]" {
		t.Fatalf("stored run_list = %v, want original payload to remain", runList)
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

func canonicalPolicyPayload(name, revisionID string) map[string]any {
	return map[string]any{
		"name":        name,
		"revision_id": revisionID,
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"named_run_lists": map[string]any{
			"update_jenkins": []any{"recipe[policyfile_demo::other_recipe]"},
		},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{
				"version":                   "0.1.0",
				"identifier":                "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"dotted_decimal_identifier": "67638399371010690.23642238397896298.25512023620585",
				"source":                    "cookbooks/policyfile_demo",
				"cache_key":                 nil,
				"scm_info": map[string]any{
					"scm":                          "git",
					"remote":                       "git@github.com:danielsdeleo/policyfile-jenkins-demo.git",
					"revision":                     "edd40c30c4e0ebb3658abde4620597597d2e9c17",
					"working_tree_clean":           false,
					"published":                    false,
					"synchronized_remote_branches": []any{},
				},
				"source_options": map[string]any{
					"path": "cookbooks/policyfile_demo",
				},
			},
		},
		"solution_dependencies": map[string]any{
			"Policyfile": []any{
				[]any{"policyfile_demo", ">= 0.0.0"},
			},
			"dependencies": map[string]any{
				"policyfile_demo (0.1.0)": []any{},
			},
		},
	}
}
