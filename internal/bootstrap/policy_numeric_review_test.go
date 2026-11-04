package bootstrap

import (
	"encoding/json"
	"testing"
)

func TestReviewPolicyNumericRepresentations(t *testing.T) {
	state := newPolicyTestService(t)
	payload := map[string]any{"name": "numeric", "revision_id": "1111111111111111111111111111111111111111", "run_list": []any{}, "cookbook_locks": map[string]any{}, "default_attributes": map[string]any{"n": []any{json.Number("100.00"), json.Number("9007199254740993")}}}
	if _, err := state.CreatePolicyRevision("ponyville", "numeric", CreatePolicyRevisionInput{Payload: payload}); err != nil {
		t.Fatal(err)
	}
	payload["default_attributes"] = map[string]any{"n": []any{float64(100), json.Number("9007199254740993.0")}}
	if _, _, err := state.UpsertPolicyGroupAssignment("ponyville", "dev", "numeric", UpdatePolicyGroupAssignmentInput{Payload: payload}); err != nil {
		t.Fatal(err)
	}
}
