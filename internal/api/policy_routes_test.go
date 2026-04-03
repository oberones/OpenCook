package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPoliciesAndPolicyGroupsEndpointsSupportCoreLifecycle(t *testing.T) {
	router := newTestRouter(t)

	listPoliciesReq := newSignedJSONRequest(t, http.MethodGet, "/policies", nil)
	listPoliciesRec := httptest.NewRecorder()
	router.ServeHTTP(listPoliciesRec, listPoliciesReq)
	if listPoliciesRec.Code != http.StatusOK {
		t.Fatalf("list policies status = %d, want %d, body = %s", listPoliciesRec.Code, http.StatusOK, listPoliciesRec.Body.String())
	}

	var initialPolicies map[string]any
	if err := json.Unmarshal(listPoliciesRec.Body.Bytes(), &initialPolicies); err != nil {
		t.Fatalf("json.Unmarshal(initial policies) error = %v", err)
	}
	if len(initialPolicies) != 0 {
		t.Fatalf("initial policies = %v, want empty object", initialPolicies)
	}

	listGroupsReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups", nil)
	listGroupsRec := httptest.NewRecorder()
	router.ServeHTTP(listGroupsRec, listGroupsReq)
	if listGroupsRec.Code != http.StatusOK {
		t.Fatalf("list policy groups status = %d, want %d, body = %s", listGroupsRec.Code, http.StatusOK, listGroupsRec.Body.String())
	}

	var initialGroups map[string]any
	if err := json.Unmarshal(listGroupsRec.Body.Bytes(), &initialGroups); err != nil {
		t.Fatalf("json.Unmarshal(initial policy groups) error = %v", err)
	}
	if len(initialGroups) != 0 {
		t.Fatalf("initial policy groups = %v, want empty object", initialGroups)
	}

	revisionID := "1111111111111111111111111111111111111111"
	putBody := mustMarshalPolicyJSON(t, minimalPolicyPayload("appserver", revisionID))
	putReq := newSignedJSONRequest(t, http.MethodPut, "/policy_groups/dev/policies/appserver", putBody)
	putRec := httptest.NewRecorder()
	router.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusCreated {
		t.Fatalf("put policy group assignment status = %d, want %d, body = %s", putRec.Code, http.StatusCreated, putRec.Body.String())
	}

	policiesReq := newSignedJSONRequest(t, http.MethodGet, "/policies", nil)
	policiesRec := httptest.NewRecorder()
	router.ServeHTTP(policiesRec, policiesReq)
	if policiesRec.Code != http.StatusOK {
		t.Fatalf("list policies status = %d, want %d, body = %s", policiesRec.Code, http.StatusOK, policiesRec.Body.String())
	}

	var policiesPayload map[string]any
	if err := json.Unmarshal(policiesRec.Body.Bytes(), &policiesPayload); err != nil {
		t.Fatalf("json.Unmarshal(policies) error = %v", err)
	}
	appserverPolicy, ok := policiesPayload["appserver"].(map[string]any)
	if !ok {
		t.Fatalf("policies[appserver] = %T, want map[string]any", policiesPayload["appserver"])
	}
	if appserverPolicy["uri"] != "/policies/appserver" {
		t.Fatalf("policy uri = %v, want %q", appserverPolicy["uri"], "/policies/appserver")
	}

	policyGroupsReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups", nil)
	policyGroupsRec := httptest.NewRecorder()
	router.ServeHTTP(policyGroupsRec, policyGroupsReq)
	if policyGroupsRec.Code != http.StatusOK {
		t.Fatalf("list policy groups status = %d, want %d, body = %s", policyGroupsRec.Code, http.StatusOK, policyGroupsRec.Body.String())
	}

	var policyGroupsPayload map[string]any
	if err := json.Unmarshal(policyGroupsRec.Body.Bytes(), &policyGroupsPayload); err != nil {
		t.Fatalf("json.Unmarshal(policy groups) error = %v", err)
	}
	devGroup, ok := policyGroupsPayload["dev"].(map[string]any)
	if !ok {
		t.Fatalf("policy_groups[dev] = %T, want map[string]any", policyGroupsPayload["dev"])
	}
	if devGroup["uri"] != "/policy_groups/dev" {
		t.Fatalf("policy group uri = %v, want %q", devGroup["uri"], "/policy_groups/dev")
	}
	devPolicies := devGroup["policies"].(map[string]any)
	devAppserver := devPolicies["appserver"].(map[string]any)
	if devAppserver["revision_id"] != revisionID {
		t.Fatalf("group revision_id = %v, want %q", devAppserver["revision_id"], revisionID)
	}

	namedGroupReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups/dev", nil)
	namedGroupRec := httptest.NewRecorder()
	router.ServeHTTP(namedGroupRec, namedGroupReq)
	if namedGroupRec.Code != http.StatusOK {
		t.Fatalf("get named policy group status = %d, want %d, body = %s", namedGroupRec.Code, http.StatusOK, namedGroupRec.Body.String())
	}

	var namedGroupPayload map[string]any
	if err := json.Unmarshal(namedGroupRec.Body.Bytes(), &namedGroupPayload); err != nil {
		t.Fatalf("json.Unmarshal(named group) error = %v", err)
	}
	if namedGroupPayload["uri"] != "/policy_groups/dev" {
		t.Fatalf("named group uri = %v, want %q", namedGroupPayload["uri"], "/policy_groups/dev")
	}

	assignmentReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups/dev/policies/appserver", nil)
	assignmentRec := httptest.NewRecorder()
	router.ServeHTTP(assignmentRec, assignmentReq)
	if assignmentRec.Code != http.StatusOK {
		t.Fatalf("get policy group assignment status = %d, want %d, body = %s", assignmentRec.Code, http.StatusOK, assignmentRec.Body.String())
	}

	var assignmentPayload map[string]any
	if err := json.Unmarshal(assignmentRec.Body.Bytes(), &assignmentPayload); err != nil {
		t.Fatalf("json.Unmarshal(group assignment) error = %v", err)
	}
	if assignmentPayload["revision_id"] != revisionID {
		t.Fatalf("assignment revision_id = %v, want %q", assignmentPayload["revision_id"], revisionID)
	}
	if _, ok := assignmentPayload["policy_group_list"]; ok {
		t.Fatalf("assignment unexpectedly included policy_group_list: %v", assignmentPayload)
	}

	revisionReq := newSignedJSONRequest(t, http.MethodGet, "/policies/appserver/revisions/"+revisionID, nil)
	revisionRec := httptest.NewRecorder()
	router.ServeHTTP(revisionRec, revisionReq)
	if revisionRec.Code != http.StatusOK {
		t.Fatalf("get policy revision status = %d, want %d, body = %s", revisionRec.Code, http.StatusOK, revisionRec.Body.String())
	}

	var revisionPayload map[string]any
	if err := json.Unmarshal(revisionRec.Body.Bytes(), &revisionPayload); err != nil {
		t.Fatalf("json.Unmarshal(policy revision) error = %v", err)
	}
	groupList := stringSliceFromAny(t, revisionPayload["policy_group_list"])
	if len(groupList) != 1 || groupList[0] != "dev" {
		t.Fatalf("policy_group_list = %v, want [dev]", groupList)
	}

	deleteAssignmentReq := newSignedJSONRequest(t, http.MethodDelete, "/policy_groups/dev/policies/appserver", nil)
	deleteAssignmentRec := httptest.NewRecorder()
	router.ServeHTTP(deleteAssignmentRec, deleteAssignmentReq)
	if deleteAssignmentRec.Code != http.StatusOK {
		t.Fatalf("delete policy group assignment status = %d, want %d, body = %s", deleteAssignmentRec.Code, http.StatusOK, deleteAssignmentRec.Body.String())
	}

	emptyGroupReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups/dev", nil)
	emptyGroupRec := httptest.NewRecorder()
	router.ServeHTTP(emptyGroupRec, emptyGroupReq)
	if emptyGroupRec.Code != http.StatusOK {
		t.Fatalf("get empty policy group status = %d, want %d, body = %s", emptyGroupRec.Code, http.StatusOK, emptyGroupRec.Body.String())
	}

	var emptyGroupPayload map[string]any
	if err := json.Unmarshal(emptyGroupRec.Body.Bytes(), &emptyGroupPayload); err != nil {
		t.Fatalf("json.Unmarshal(empty group) error = %v", err)
	}
	emptyPolicies := emptyGroupPayload["policies"].(map[string]any)
	if len(emptyPolicies) != 0 {
		t.Fatalf("empty group policies = %v, want empty object", emptyPolicies)
	}
}

func TestPoliciesEndpointSupportsRevisionLifecycle(t *testing.T) {
	router := newTestRouter(t)

	revisionID := "2222222222222222222222222222222222222222"
	body := mustMarshalPolicyJSON(t, minimalPolicyPayload("appserver", revisionID))

	createReq := newSignedJSONRequest(t, http.MethodPost, "/policies/appserver/revisions", body)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create policy revision status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	conflictReq := newSignedJSONRequest(t, http.MethodPost, "/policies/appserver/revisions", body)
	conflictRec := httptest.NewRecorder()
	router.ServeHTTP(conflictRec, conflictReq)
	if conflictRec.Code != http.StatusConflict {
		t.Fatalf("duplicate policy revision status = %d, want %d, body = %s", conflictRec.Code, http.StatusConflict, conflictRec.Body.String())
	}

	namedPolicyReq := newSignedJSONRequest(t, http.MethodGet, "/policies/appserver", nil)
	namedPolicyRec := httptest.NewRecorder()
	router.ServeHTTP(namedPolicyRec, namedPolicyReq)
	if namedPolicyRec.Code != http.StatusOK {
		t.Fatalf("get named policy status = %d, want %d, body = %s", namedPolicyRec.Code, http.StatusOK, namedPolicyRec.Body.String())
	}

	var namedPolicyPayload map[string]any
	if err := json.Unmarshal(namedPolicyRec.Body.Bytes(), &namedPolicyPayload); err != nil {
		t.Fatalf("json.Unmarshal(named policy) error = %v", err)
	}
	revisions := namedPolicyPayload["revisions"].(map[string]any)
	if _, ok := revisions[revisionID]; !ok {
		t.Fatalf("revisions = %v, want to include %q", revisions, revisionID)
	}

	getRevisionReq := newSignedJSONRequest(t, http.MethodGet, "/policies/appserver/revisions/"+revisionID, nil)
	getRevisionRec := httptest.NewRecorder()
	router.ServeHTTP(getRevisionRec, getRevisionReq)
	if getRevisionRec.Code != http.StatusOK {
		t.Fatalf("get policy revision status = %d, want %d, body = %s", getRevisionRec.Code, http.StatusOK, getRevisionRec.Body.String())
	}

	var revisionPayload map[string]any
	if err := json.Unmarshal(getRevisionRec.Body.Bytes(), &revisionPayload); err != nil {
		t.Fatalf("json.Unmarshal(policy revision) error = %v", err)
	}
	groupList := stringSliceFromAny(t, revisionPayload["policy_group_list"])
	if len(groupList) != 0 {
		t.Fatalf("policy_group_list = %v, want empty list", groupList)
	}

	deleteRevisionReq := newSignedJSONRequest(t, http.MethodDelete, "/policies/appserver/revisions/"+revisionID, nil)
	deleteRevisionRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRevisionRec, deleteRevisionReq)
	if deleteRevisionRec.Code != http.StatusOK {
		t.Fatalf("delete policy revision status = %d, want %d, body = %s", deleteRevisionRec.Code, http.StatusOK, deleteRevisionRec.Body.String())
	}

	missingRevisionReq := newSignedJSONRequest(t, http.MethodGet, "/policies/appserver/revisions/"+revisionID, nil)
	missingRevisionRec := httptest.NewRecorder()
	router.ServeHTTP(missingRevisionRec, missingRevisionReq)
	if missingRevisionRec.Code != http.StatusNotFound {
		t.Fatalf("missing policy revision status = %d, want %d, body = %s", missingRevisionRec.Code, http.StatusNotFound, missingRevisionRec.Body.String())
	}
}

func TestPolicyGroupAssignmentPutUpdatesExistingGroup(t *testing.T) {
	router := newTestRouter(t)

	rev1 := mustMarshalPolicyJSON(t, minimalPolicyPayload("appserver", "3333333333333333333333333333333333333333"))
	createReq := newSignedJSONRequest(t, http.MethodPut, "/policy_groups/dev/policies/appserver", rev1)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create assignment status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	rev2 := mustMarshalPolicyJSON(t, minimalPolicyPayload("appserver", "4444444444444444444444444444444444444444"))
	updateReq := newSignedJSONRequest(t, http.MethodPut, "/policy_groups/dev/policies/appserver", rev2)
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update assignment status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	getReq := newSignedJSONRequest(t, http.MethodGet, "/policy_groups/dev/policies/appserver", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get updated assignment status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(updated assignment) error = %v", err)
	}
	if payload["revision_id"] != "4444444444444444444444444444444444444444" {
		t.Fatalf("updated revision_id = %v, want %q", payload["revision_id"], "4444444444444444444444444444444444444444")
	}
}

func TestPolicyEndpointsRejectMismatchedName(t *testing.T) {
	router := newTestRouter(t)

	body := mustMarshalPolicyJSON(t, minimalPolicyPayload("other-name", "5555555555555555555555555555555555555555"))
	req := newSignedJSONRequest(t, http.MethodPut, "/policy_groups/dev/policies/appserver", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("mismatched name status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(mismatched name) error = %v", err)
	}
	messages := payload["error"].([]any)
	if len(messages) != 1 || messages[0] != "Field 'name' invalid : appserver does not match other-name" {
		t.Fatalf("error messages = %v, want mismatched-name validation", messages)
	}
}

func minimalPolicyPayload(name, revisionID string) map[string]any {
	return map[string]any{
		"name":        name,
		"revision_id": revisionID,
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "1.2.3",
			},
		},
	}
}

func mustMarshalPolicyJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return body
}
