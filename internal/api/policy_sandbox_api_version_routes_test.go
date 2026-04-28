package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestPolicyAndSandboxAPIVersionMemoryMatrix(t *testing.T) {
	tests := []struct {
		serverAPIVersion string
		policyName       string
		groupName        string
		revisionID       string
	}{
		{
			serverAPIVersion: "0",
			policyName:       "api_policy_zero",
			groupName:        "dev_zero",
			revisionID:       "1010101010101010101010101010101010101010",
		},
		{
			serverAPIVersion: "1",
			policyName:       "api_policy_one",
			groupName:        "dev_one",
			revisionID:       "2121212121212121212121212121212121212121",
		},
		{
			serverAPIVersion: "2",
			policyName:       "api_policy_two",
			groupName:        "dev_two",
			revisionID:       "3232323232323232323232323232323232323232",
		},
	}

	for _, tt := range tests {
		t.Run("v"+tt.serverAPIVersion, func(t *testing.T) {
			router := newTestRouter(t)

			createRevision := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/policies/"+tt.policyName+"/revisions",
				mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI(tt.policyName, tt.revisionID)), tt.serverAPIVersion)
			assertPolicySandboxAPIVersionStatus(t, createRevision, http.StatusCreated, "create policy revision")
			assertPolicyAPIVersionPayload(t, mustDecodeObject(t, createRevision), tt.policyName, tt.revisionID)
			assertPolicyPayloadHasNoGroupList(t, mustDecodeObject(t, createRevision))

			assertPolicyAPIVersionCollection(t, router, "/policies", tt.serverAPIVersion, "/policies", tt.policyName, tt.revisionID)
			assertPolicyAPIVersionNamed(t, router, "/policies/"+tt.policyName, tt.serverAPIVersion, tt.revisionID)
			revisionPayload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, "/policies/"+tt.policyName+"/revisions/"+tt.revisionID, nil, tt.serverAPIVersion, http.StatusOK)
			assertPolicyAPIVersionPayload(t, revisionPayload, tt.policyName, tt.revisionID)
			assertPolicyPayloadGroupList(t, revisionPayload, nil)

			createAssignment := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/policy_groups/"+tt.groupName+"/policies/"+tt.policyName,
				mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI(tt.policyName, tt.revisionID)), tt.serverAPIVersion)
			assertPolicySandboxAPIVersionStatus(t, createAssignment, http.StatusCreated, "create policy group assignment")
			assertPolicyAPIVersionPayload(t, mustDecodeObject(t, createAssignment), tt.policyName, tt.revisionID)
			assertPolicyPayloadHasNoGroupList(t, mustDecodeObject(t, createAssignment))

			assertPolicyGroupAPIVersionCollection(t, router, "/policy_groups", tt.serverAPIVersion, "/policy_groups", tt.groupName, tt.policyName, tt.revisionID)
			assertPolicyGroupAPIVersionNamed(t, router, "/policy_groups/"+tt.groupName, tt.serverAPIVersion, "/policy_groups", tt.groupName, tt.policyName, tt.revisionID)
			assignmentPayload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, "/policy_groups/"+tt.groupName+"/policies/"+tt.policyName, nil, tt.serverAPIVersion, http.StatusOK)
			assertPolicyAPIVersionPayload(t, assignmentPayload, tt.policyName, tt.revisionID)
			assertPolicyPayloadHasNoGroupList(t, assignmentPayload)

			groupedRevisionPayload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, "/policies/"+tt.policyName+"/revisions/"+tt.revisionID, nil, tt.serverAPIVersion, http.StatusOK)
			assertPolicyPayloadGroupList(t, groupedRevisionPayload, []string{tt.groupName})

			deleteAssignment := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/policy_groups/"+tt.groupName+"/policies/"+tt.policyName, nil, tt.serverAPIVersion)
			assertPolicySandboxAPIVersionStatus(t, deleteAssignment, http.StatusOK, "delete policy group assignment")
			assertPolicyAPIVersionPayload(t, mustDecodeObject(t, deleteAssignment), tt.policyName, tt.revisionID)
			assertPolicyPayloadHasNoGroupList(t, mustDecodeObject(t, deleteAssignment))

			orgPolicyName := "org_" + tt.policyName
			orgGroupName := "org_" + tt.groupName
			orgRevisionID := "abababababababababababababababababababab"
			orgAssignment := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/organizations/ponyville/policy_groups/"+orgGroupName+"/policies/"+orgPolicyName,
				mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI(orgPolicyName, orgRevisionID)), tt.serverAPIVersion)
			assertPolicySandboxAPIVersionStatus(t, orgAssignment, http.StatusCreated, "create org policy group assignment")
			assertPolicyAPIVersionCollection(t, router, "/organizations/ponyville/policies", tt.serverAPIVersion, "/organizations/ponyville/policies", orgPolicyName, orgRevisionID)
			assertPolicyGroupAPIVersionNamed(t, router, "/organizations/ponyville/policy_groups/"+orgGroupName, tt.serverAPIVersion, "/organizations/ponyville/policy_groups", orgGroupName, orgPolicyName, orgRevisionID)

			content := []byte("sandbox api version " + tt.serverAPIVersion)
			sandboxID := createUploadCommitSandboxForAPIVersion(t, router, "/sandboxes", "/sandboxes", "ponyville", tt.serverAPIVersion, content)
			if sandboxID == "" {
				t.Fatal("sandboxID = empty, want committed sandbox id")
			}

			orgContent := []byte("org sandbox api version " + tt.serverAPIVersion)
			orgSandboxID := createUploadCommitSandboxForAPIVersion(t, router, "/organizations/ponyville/sandboxes", "/organizations/ponyville/sandboxes", "ponyville", tt.serverAPIVersion, orgContent)
			if orgSandboxID == "" {
				t.Fatal("orgSandboxID = empty, want committed sandbox id")
			}
		})
	}
}

func TestPolicySandboxAPIVersionActivePostgresRestart(t *testing.T) {
	root := t.TempDir()
	fileStore, err := blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	fixture := newActivePostgresBootstrapFixtureWithBlob(t, pgtest.NewState(pgtest.Seed{}), fileStore)
	fixture.createOrganizationWithValidator("ponyville")

	revisionID := "4545454545454545454545454545454545454545"
	createAssignment := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/organizations/ponyville/policy_groups/restart_dev/policies/restart_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("restart_policy", revisionID)), "2")
	assertPolicySandboxAPIVersionStatus(t, createAssignment, http.StatusCreated, "create restart policy assignment")

	content := []byte("active postgres sandbox api version")
	checksum := checksumHex(content)
	createSandbox := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/sandboxes",
		mustMarshalSandboxJSON(t, map[string]any{"checksums": map[string]any{checksum: nil}}), "2")
	assertPolicySandboxAPIVersionStatus(t, createSandbox, http.StatusCreated, "create restart sandbox")
	createSandboxPayload := mustDecodeObject(t, createSandbox)
	sandboxID, uploadURL := assertSandboxAPIVersionCreatePayload(t, createSandboxPayload, checksum, "ponyville", "/organizations/ponyville/sandboxes")
	uploadSandboxChecksum(t, fixture.router, uploadURL, content)

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(restart) error = %v", err)
	}
	restarted := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)

	revisionPayload := mustServePolicySandboxAPIVersionObject(t, restarted.router, http.MethodGet, "/policies/restart_policy/revisions/"+revisionID, nil, "0", http.StatusOK)
	assertPolicyAPIVersionPayload(t, revisionPayload, "restart_policy", revisionID)
	assertPolicyPayloadGroupList(t, revisionPayload, []string{"restart_dev"})
	assertPolicyGroupAPIVersionNamed(t, restarted.router, "/organizations/ponyville/policy_groups/restart_dev", "2", "/organizations/ponyville/policy_groups", "restart_dev", "restart_policy", revisionID)

	commitSandbox := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/organizations/ponyville/sandboxes/"+sandboxID,
		mustMarshalSandboxJSON(t, map[string]any{"is_completed": true}), "2")
	assertPolicySandboxAPIVersionStatus(t, commitSandbox, http.StatusOK, "commit restarted sandbox")
	assertSandboxAPIVersionCommitPayload(t, mustDecodeObject(t, commitSandbox), sandboxID, checksum)

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(reuse restart) error = %v", err)
	}
	afterCommit := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	reuseSandbox := serveSignedAPIVersionRequest(t, afterCommit.router, "pivotal", http.MethodPost, "/sandboxes",
		mustMarshalSandboxJSON(t, map[string]any{"checksums": map[string]any{checksum: nil}}), "1")
	assertPolicySandboxAPIVersionStatus(t, reuseSandbox, http.StatusCreated, "reuse committed checksum after restart")
	assertSandboxAPIVersionReusedChecksum(t, mustDecodeObject(t, reuseSandbox), checksum)
}

func TestPolicySandboxAPIVersionInvalidWritesDoNotMutate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("ponyville")

	revisionID := "5656565656565656565656565656565656565656"
	createAssignment := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/policy_groups/dev/policies/immutable_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("immutable_policy", revisionID)), "2")
	assertPolicySandboxAPIVersionStatus(t, createAssignment, http.StatusCreated, "create immutable policy assignment")

	blockedRevisionID := "6767676767676767676767676767676767676767"
	blockedPolicyUpdate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/policy_groups/dev/policies/immutable_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("immutable_policy", blockedRevisionID)), "3")
	assertInvalidServerAPIVersionResponse(t, blockedPolicyUpdate, "3")
	assertPolicyAssignmentRevisionForAPIVersion(t, fixture.router, "/policy_groups/dev/policies/immutable_policy", "2", revisionID)

	restarted := fixture.restart()
	assertPolicyAssignmentRevisionForAPIVersion(t, restarted.router, "/organizations/ponyville/policy_groups/dev/policies/immutable_policy", "1", revisionID)
	mustServePolicySandboxAPIVersionObject(t, restarted.router, http.MethodGet, "/policies/immutable_policy/revisions/"+blockedRevisionID, nil, "2", http.StatusNotFound)

	content := []byte("invalid version sandbox")
	checksum := checksumHex(content)
	createSandbox := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPost, "/sandboxes",
		mustMarshalSandboxJSON(t, map[string]any{"checksums": map[string]any{checksum: nil}}), "2")
	assertPolicySandboxAPIVersionStatus(t, createSandbox, http.StatusCreated, "create sandbox before invalid commit")
	sandboxID, uploadURL := assertSandboxAPIVersionCreatePayload(t, mustDecodeObject(t, createSandbox), checksum, "ponyville", "/sandboxes")
	uploadSandboxChecksum(t, restarted.router, uploadURL, content)

	blockedCommit := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/sandboxes/"+sandboxID, []byte(`{`), "3")
	assertInvalidServerAPIVersionResponse(t, blockedCommit, "3")

	validCommit := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/sandboxes/"+sandboxID,
		mustMarshalSandboxJSON(t, map[string]any{"is_completed": true}), "2")
	assertPolicySandboxAPIVersionStatus(t, validCommit, http.StatusOK, "commit sandbox after invalid-version rejection")
	assertSandboxAPIVersionCommitPayload(t, mustDecodeObject(t, validCommit), sandboxID, checksum)

	blockedSandboxCreate := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPost, "/sandboxes", []byte(`{`), "3")
	assertInvalidServerAPIVersionResponse(t, blockedSandboxCreate, "3")

	blockedSearch := serveSignedQueryAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, searchPath("/search/policy_groups", "*:*"), "/search/policy_groups", nil, "3")
	assertInvalidServerAPIVersionResponse(t, blockedSearch, "3")
}

func TestPolicySandboxAPIVersionOpenSearchSemantics(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	nodeName := "policy_search_node"
	nodeCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, nodePayloadExpectation{
		Name:            nodeName,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{"team": "friendship"},
		Normal:          map[string]any{},
		Default:         map[string]any{"build": "909"},
		Automatic:       map[string]any{},
		RunList:         []string{"base"},
		PolicyName:      "delivery-policy",
		PolicyGroup:     "prod-policy",
	}), "2")
	assertPolicySandboxAPIVersionStatus(t, nodeCreate, http.StatusCreated, "create OpenSearch policy node")

	assertSearchFullRowsForAPIVersion(t, fixture.router, searchPath("/search/node", "policy_name:delivery-policy AND policy_group:prod-policy"), "/search/node", "2", []string{nodeName})
	assertSearchPartialDataForAPIVersion(t, fixture.router, searchPath("/search/node", "policy_name:delivery-policy"), "/search/node", "2",
		[]byte(`{"policy_name":["policy_name"],"policy_group":["policy_group"],"build":["build"]}`), "/nodes/"+nodeName, map[string]any{
			"policy_name":  "delivery-policy",
			"policy_group": "prod-policy",
			"build":        "909",
		})

	wantDocs := transport.SnapshotDocuments()
	wantMutations := transport.SnapshotMutationRequests()

	policyRevisionID := "7878787878787878787878787878787878787878"
	policyCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/policy_groups/search_dev/policies/search_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("search_policy", policyRevisionID)), "2")
	assertPolicySandboxAPIVersionStatus(t, policyCreate, http.StatusCreated, "create unindexed policy assignment")
	createUploadCommitSandboxForAPIVersion(t, fixture.router, "/sandboxes", "/sandboxes", "ponyville", "2", []byte("unindexed api-version sandbox"))

	transport.RequireDocuments(t, wantDocs)
	transport.RequireMutationRequests(t, wantMutations)

	assertSearchIndexListExcludesForAPIVersion(t, fixture.router, "/search", "2", "policy", "policy_groups", "sandbox", "sandboxes", "checksums")
	assertUnsupportedSearchIndexForAPIVersion(t, fixture.router, http.MethodGet, searchPath("/search/policy_groups", "*:*"), "/search/policy_groups", nil, "policy_groups", "2")
	assertUnsupportedSearchIndexForAPIVersion(t, fixture.router, http.MethodPost, searchPath("/organizations/ponyville/search/sandbox", "name:*"), "/organizations/ponyville/search/sandbox", []byte(`{"name":["name"]}`), "sandbox", "2")
}

func mustServePolicySandboxAPIVersionObject(t *testing.T, router http.Handler, method, path string, body []byte, serverAPIVersion string, want int) map[string]any {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", method, path, body, serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, rec, want, method+" "+path)
	if rec.Body.Len() == 0 {
		return nil
	}
	return mustDecodeObject(t, rec)
}

func assertPolicySandboxAPIVersionStatus(t *testing.T, rec *httptest.ResponseRecorder, want int, context string) {
	t.Helper()

	if rec.Code != want {
		t.Fatalf("%s status = %d, want %d, body = %s", context, rec.Code, want, rec.Body.String())
	}
}

func assertPolicyAPIVersionPayload(t *testing.T, payload map[string]any, name, revisionID string) {
	t.Helper()

	if payload["name"] != name {
		t.Fatalf("policy name = %v, want %q", payload["name"], name)
	}
	if payload["revision_id"] != revisionID {
		t.Fatalf("policy revision_id = %v, want %q", payload["revision_id"], revisionID)
	}
	runList := stringSliceFromAny(t, payload["run_list"])
	if len(runList) != 1 || runList[0] != "recipe[policyfile_demo::default]" {
		t.Fatalf("policy run_list = %v, want default policyfile recipe", runList)
	}
	assertRehydratedCanonicalPolicyPayload(t, payload)
}

func assertPolicyPayloadHasNoGroupList(t *testing.T, payload map[string]any) {
	t.Helper()

	if _, ok := payload["policy_group_list"]; ok {
		t.Fatalf("policy payload unexpectedly included policy_group_list: %v", payload)
	}
}

func assertPolicyPayloadGroupList(t *testing.T, payload map[string]any, want []string) {
	t.Helper()

	got := stringSliceFromAny(t, payload["policy_group_list"])
	sort.Strings(got)
	wantCopy := append([]string(nil), want...)
	if wantCopy == nil {
		wantCopy = []string{}
	}
	sort.Strings(wantCopy)
	if !reflect.DeepEqual(got, wantCopy) {
		t.Fatalf("policy_group_list = %v, want %v", got, wantCopy)
	}
}

func assertPolicyAPIVersionCollection(t *testing.T, router http.Handler, path, serverAPIVersion, basePath, policyName, revisionID string) {
	t.Helper()

	payload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, path, nil, serverAPIVersion, http.StatusOK)
	entry, ok := payload[policyName].(map[string]any)
	if !ok {
		t.Fatalf("%s missing policy %q: %v", path, policyName, payload)
	}
	if entry["uri"] != basePath+"/"+policyName {
		t.Fatalf("%s policy uri = %v, want %q", path, entry["uri"], basePath+"/"+policyName)
	}
	revisions, ok := entry["revisions"].(map[string]any)
	if !ok {
		t.Fatalf("%s revisions = %T, want map[string]any", path, entry["revisions"])
	}
	if _, ok := revisions[revisionID].(map[string]any); !ok {
		t.Fatalf("%s revisions missing %q: %v", path, revisionID, revisions)
	}
}

func assertPolicyAPIVersionNamed(t *testing.T, router http.Handler, path, serverAPIVersion, revisionID string) {
	t.Helper()

	payload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, path, nil, serverAPIVersion, http.StatusOK)
	revisions, ok := payload["revisions"].(map[string]any)
	if !ok {
		t.Fatalf("%s revisions = %T, want map[string]any", path, payload["revisions"])
	}
	if _, ok := revisions[revisionID].(map[string]any); !ok {
		t.Fatalf("%s revisions missing %q: %v", path, revisionID, revisions)
	}
}

func assertPolicyGroupAPIVersionCollection(t *testing.T, router http.Handler, path, serverAPIVersion, basePath, groupName, policyName, revisionID string) {
	t.Helper()

	payload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, path, nil, serverAPIVersion, http.StatusOK)
	group, ok := payload[groupName].(map[string]any)
	if !ok {
		t.Fatalf("%s missing group %q: %v", path, groupName, payload)
	}
	assertPolicyGroupAPIVersionPayload(t, group, basePath, groupName, policyName, revisionID)
}

func assertPolicyGroupAPIVersionNamed(t *testing.T, router http.Handler, path, serverAPIVersion, basePath, groupName, policyName, revisionID string) {
	t.Helper()

	payload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, path, nil, serverAPIVersion, http.StatusOK)
	assertPolicyGroupAPIVersionPayload(t, payload, basePath, groupName, policyName, revisionID)
}

func assertPolicyGroupAPIVersionPayload(t *testing.T, payload map[string]any, basePath, groupName, policyName, revisionID string) {
	t.Helper()

	if payload["uri"] != basePath+"/"+groupName {
		t.Fatalf("policy group uri = %v, want %q", payload["uri"], basePath+"/"+groupName)
	}
	policies, ok := payload["policies"].(map[string]any)
	if !ok {
		t.Fatalf("policy group policies = %T, want map[string]any", payload["policies"])
	}
	policy, ok := policies[policyName].(map[string]any)
	if !ok {
		t.Fatalf("policy group missing policy %q: %v", policyName, policies)
	}
	if policy["revision_id"] != revisionID {
		t.Fatalf("policy group revision_id = %v, want %q", policy["revision_id"], revisionID)
	}
}

func assertPolicyAssignmentRevisionForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion, revisionID string) {
	t.Helper()

	payload := mustServePolicySandboxAPIVersionObject(t, router, http.MethodGet, path, nil, serverAPIVersion, http.StatusOK)
	if payload["revision_id"] != revisionID {
		t.Fatalf("%s revision_id = %v, want %q", path, payload["revision_id"], revisionID)
	}
}

func createUploadCommitSandboxForAPIVersion(t *testing.T, router http.Handler, createPath, commitBasePath, org, serverAPIVersion string, content []byte) string {
	t.Helper()

	checksum := checksumHex(content)
	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, createPath,
		mustMarshalSandboxJSON(t, map[string]any{"checksums": map[string]any{checksum: nil}}), serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, createRec, http.StatusCreated, "create sandbox "+createPath)
	sandboxID, uploadURL := assertSandboxAPIVersionCreatePayload(t, mustDecodeObject(t, createRec), checksum, org, commitBasePath)
	uploadSandboxChecksum(t, router, uploadURL, content)

	commitRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, commitBasePath+"/"+sandboxID,
		mustMarshalSandboxJSON(t, map[string]any{"is_completed": true}), serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, commitRec, http.StatusOK, "commit sandbox "+commitBasePath)
	assertSandboxAPIVersionCommitPayload(t, mustDecodeObject(t, commitRec), sandboxID, checksum)
	return sandboxID
}

func assertSandboxAPIVersionCreatePayload(t *testing.T, payload map[string]any, checksum, org, basePath string) (string, string) {
	t.Helper()

	sandboxID, ok := payload["sandbox_id"].(string)
	if !ok || sandboxID == "" {
		t.Fatalf("sandbox_id = %T/%v, want non-empty string", payload["sandbox_id"], payload["sandbox_id"])
	}
	if wantURI := "http://example.com" + basePath + "/" + sandboxID; payload["uri"] != wantURI {
		t.Fatalf("sandbox uri = %v, want %q", payload["uri"], wantURI)
	}
	checksums, ok := payload["checksums"].(map[string]any)
	if !ok {
		t.Fatalf("checksums = %T, want map[string]any", payload["checksums"])
	}
	entry, ok := checksums[checksum].(map[string]any)
	if !ok {
		t.Fatalf("checksums missing %q: %v", checksum, checksums)
	}
	if entry["needs_upload"] != true {
		t.Fatalf("needs_upload = %v, want true", entry["needs_upload"])
	}
	uploadURL, ok := entry["url"].(string)
	if !ok || uploadURL == "" {
		t.Fatalf("upload url = %T/%v, want non-empty string", entry["url"], entry["url"])
	}
	assertSandboxAPIVersionUploadURL(t, uploadURL, checksum, org, sandboxID)
	return sandboxID, uploadURL
}

func assertSandboxAPIVersionReusedChecksum(t *testing.T, payload map[string]any, checksum string) {
	t.Helper()

	checksums, ok := payload["checksums"].(map[string]any)
	if !ok {
		t.Fatalf("checksums = %T, want map[string]any", payload["checksums"])
	}
	entry, ok := checksums[checksum].(map[string]any)
	if !ok {
		t.Fatalf("checksums missing %q: %v", checksum, checksums)
	}
	if entry["needs_upload"] != false {
		t.Fatalf("reused needs_upload = %v, want false", entry["needs_upload"])
	}
	if _, ok := entry["url"]; ok {
		t.Fatalf("reused checksum unexpectedly included upload URL: %v", entry)
	}
}

func assertSandboxAPIVersionUploadURL(t *testing.T, rawURL, checksum, org, sandboxID string) {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", rawURL, err)
	}
	if parsed.Path != "/_blob/checksums/"+checksum {
		t.Fatalf("upload URL path = %q, want /_blob/checksums/%s", parsed.Path, checksum)
	}
	values := parsed.Query()
	if values.Get("org") != org {
		t.Fatalf("upload URL org = %q, want %q", values.Get("org"), org)
	}
	if values.Get("sandbox_id") != sandboxID {
		t.Fatalf("upload URL sandbox_id = %q, want %q", values.Get("sandbox_id"), sandboxID)
	}
	if values.Get("expires") == "" {
		t.Fatalf("upload URL missing expires: %s", rawURL)
	}
	if values.Get("signature") == "" {
		t.Fatalf("upload URL missing signature: %s", rawURL)
	}
}

func uploadSandboxChecksum(t *testing.T, router http.Handler, uploadURL string, content []byte) {
	t.Helper()

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/octet-stream")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload sandbox checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}
	if uploadRec.Body.Len() != 0 {
		t.Fatalf("upload sandbox checksum body len = %d, want 0", uploadRec.Body.Len())
	}
}

func assertSandboxAPIVersionCommitPayload(t *testing.T, payload map[string]any, sandboxID, checksum string) {
	t.Helper()

	if payload["guid"] != sandboxID || payload["name"] != sandboxID {
		t.Fatalf("sandbox commit guid/name = %v/%v, want %q", payload["guid"], payload["name"], sandboxID)
	}
	if payload["is_completed"] != true {
		t.Fatalf("sandbox commit is_completed = %v, want true", payload["is_completed"])
	}
	checksums := stringSliceFromAny(t, payload["checksums"])
	if len(checksums) != 1 || checksums[0] != checksum {
		t.Fatalf("sandbox commit checksums = %v, want [%s]", checksums, checksum)
	}
	createTime, ok := payload["create_time"].(string)
	if !ok || createTime == "" {
		t.Fatalf("sandbox commit create_time = %T/%v, want non-empty string", payload["create_time"], payload["create_time"])
	}
	if _, err := time.Parse(time.RFC3339, createTime); err != nil {
		t.Fatalf("sandbox commit create_time = %q, want RFC3339: %v", createTime, err)
	}
}

func serveSignedQueryAPIVersionRequest(t *testing.T, router http.Handler, userID, method, rawPath, signPath string, body []byte, serverAPIVersion string) *httptest.ResponseRecorder {
	t.Helper()

	bodyForSignature := body
	if bodyForSignature == nil {
		bodyForSignature = []byte{}
	}
	req := httptest.NewRequest(method, rawPath, bytes.NewReader(body))
	for key, value := range manufactureSignedHeaders(t, mustParsePrivateKey(t), userID, method, signPath, bodyForSignature, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z", defaultServerAPIVersionForTest(serverAPIVersion)) {
		req.Header.Set(key, value)
	}
	if serverAPIVersion != "" {
		req.Header.Set(serverAPIVersionHeader, serverAPIVersion)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func assertSearchFullRowsForAPIVersion(t *testing.T, router http.Handler, rawPath, signPath, serverAPIVersion string, wantNames []string) {
	t.Helper()

	rec := serveSignedQueryAPIVersionRequest(t, router, "pivotal", http.MethodGet, rawPath, signPath, nil, serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, rec, http.StatusOK, "search "+rawPath)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(len(wantNames)) || len(rows) != len(wantNames) {
		t.Fatalf("%s search payload = %v, want %d rows", rawPath, payload, len(wantNames))
	}
	for idx, want := range wantNames {
		if rows[idx].(map[string]any)["name"] != want {
			t.Fatalf("%s row[%d].name = %v, want %q", rawPath, idx, rows[idx].(map[string]any)["name"], want)
		}
	}
}

func assertSearchPartialDataForAPIVersion(t *testing.T, router http.Handler, rawPath, signPath, serverAPIVersion string, body []byte, wantURL string, wantData map[string]any) {
	t.Helper()

	rec := serveSignedQueryAPIVersionRequest(t, router, "pivotal", http.MethodPost, rawPath, signPath, body, serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, rec, http.StatusOK, "partial search "+rawPath)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("%s partial search payload = %v, want one row", rawPath, payload)
	}
	row := rows[0].(map[string]any)
	if row["url"] != wantURL {
		t.Fatalf("%s partial search url = %v, want %q", rawPath, row["url"], wantURL)
	}
	data := row["data"].(map[string]any)
	if !reflect.DeepEqual(data, wantData) {
		t.Fatalf("%s partial search data = %v, want %v", rawPath, data, wantData)
	}
}

func assertSearchIndexListExcludesForAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion string, excluded ...string) {
	t.Helper()

	rec := serveSignedQueryAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, path, nil, serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, rec, http.StatusOK, "search index list "+path)
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v, body = %s", path, err, rec.Body.String())
	}
	for _, index := range excluded {
		if _, ok := payload[index]; ok {
			t.Fatalf("%s unexpectedly included unsupported index %q: %v", path, index, payload)
		}
	}
}

func assertUnsupportedSearchIndexForAPIVersion(t *testing.T, router http.Handler, method, rawPath, signPath string, body []byte, index, serverAPIVersion string) {
	t.Helper()

	rec := serveSignedQueryAPIVersionRequest(t, router, "pivotal", method, rawPath, signPath, body, serverAPIVersion)
	assertPolicySandboxAPIVersionStatus(t, rec, http.StatusNotFound, "unsupported search "+rawPath)
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s %s) error = %v, body = %s", method, rawPath, err, rec.Body.String())
	}
	errors, ok := payload["error"].([]any)
	if !ok || len(errors) != 1 {
		t.Fatalf("%s %s error payload = %v, want one error message", method, rawPath, payload)
	}
	want := "I don't know how to search for " + index + " data objects."
	if errors[0] != want {
		t.Fatalf("%s %s error = %v, want %q", method, rawPath, errors[0], want)
	}
}
