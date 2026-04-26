package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestActivePostgresCoreObjectsRehydrateNodesEnvironmentsAndRoles(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{
		Versions: []pg.CookbookVersionBundle{
			{
				Version: pg.CookbookVersionRecord{
					Organization: "ponyville",
					CookbookName: "demo",
					Version:      "1.0.0",
					FullName:     "demo-1.0.0",
					JSONClass:    "Chef::CookbookVersion",
					ChefType:     "cookbook_version",
					MetadataJSON: []byte(`{"name":"demo","version":"1.0.0","dependencies":{},"recipes":{"default":"Demo recipe"}}`),
				},
			},
		},
	}))

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/environments", map[string]any{
		"name":                "production",
		"json_class":          "Chef::Environment",
		"chef_type":           "environment",
		"description":         "Production",
		"cookbook_versions":   map[string]any{"demo": "= 1.0.0"},
		"default_attributes":  map[string]any{"tier": "frontend"},
		"override_attributes": map[string]any{},
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/roles", map[string]any{
		"name":                "web",
		"description":         "Web role",
		"json_class":          "Chef::Role",
		"chef_type":           "role",
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
		"run_list":            []any{"recipe[demo]"},
		"env_run_lists":       map[string]any{"production": []any{"recipe[demo]"}},
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/nodes", map[string]any{
		"name":             "twilight",
		"json_class":       "Chef::Node",
		"chef_type":        "node",
		"chef_environment": "production",
		"normal":           map[string]any{"app": "demo"},
		"default":          map[string]any{},
		"override":         map[string]any{},
		"automatic":        map[string]any{},
		"run_list":         []any{"recipe[demo]"},
	}, http.StatusCreated)

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)

	envPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/environments/production", nil, http.StatusOK)
	if envPayload["description"] != "Production" {
		t.Fatalf("rehydrated environment description = %v, want Production", envPayload["description"])
	}
	nodePayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/nodes/twilight", nil, http.StatusOK)
	if nodePayload["chef_environment"] != "production" {
		t.Fatalf("rehydrated node chef_environment = %v, want production", nodePayload["chef_environment"])
	}
	rolePayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/roles/web", nil, http.StatusOK)
	if rolePayload["description"] != "Web role" {
		t.Fatalf("rehydrated role description = %v, want Web role", rolePayload["description"])
	}

	envNodesPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/environments/production/nodes", nil, http.StatusOK)
	if envNodesPayload["twilight"] != "/nodes/twilight" {
		t.Fatalf("rehydrated environment nodes = %v, want twilight default-org URL", envNodesPayload)
	}
	envRolePayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/environments/production/roles/web", nil, http.StatusOK)
	runList, ok := envRolePayload["run_list"].([]any)
	if !ok || len(runList) != 1 || runList[0] != "recipe[demo]" {
		t.Fatalf("rehydrated environment-linked role run_list = %v, want [recipe[demo]]", envRolePayload["run_list"])
	}

	body := mustMarshalSandboxJSON(t, map[string]any{"run_list": []any{"role[web]"}})
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/environments/production/cookbook_versions", body)
	rec := httptest.NewRecorder()
	restarted.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rehydrated depsolver status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"demo"`) {
		t.Fatalf("rehydrated depsolver body = %s, want solved demo cookbook", rec.Body.String())
	}
}

func TestActivePostgresCoreObjectsRehydrateDataBagsAndSearch(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data", map[string]any{
		"name": "ponies",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data/ponies", map[string]any{
		"id": "alice",
		"ssh": map[string]any{
			"public_key":  "---RSA Public Key--- Alice",
			"private_key": "---RSA Private Key--- Alice",
		},
	}, http.StatusCreated)

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	bagsPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/data", nil, http.StatusOK)
	if bagsPayload["ponies"] != "/data/ponies" {
		t.Fatalf("rehydrated data bag list = %v, want ponies default-org URL", bagsPayload)
	}
	bagPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/data/ponies", nil, http.StatusOK)
	if bagPayload["alice"] != "/organizations/ponyville/data/ponies/alice" {
		t.Fatalf("rehydrated bag item list = %v, want alice org URL", bagPayload)
	}
	itemPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/data/ponies/alice", nil, http.StatusOK)
	sshPayload := itemPayload["ssh"].(map[string]any)
	if sshPayload["public_key"] != "---RSA Public Key--- Alice" {
		t.Fatalf("rehydrated data bag item public key = %v, want Alice key", sshPayload["public_key"])
	}

	searchPayload := mustServeActivePostgresSearchRequest(t, restarted.router, http.MethodGet, "/search/ponies?q=id:alice", nil, http.StatusOK)
	searchRows := searchPayload["rows"].([]any)
	if len(searchRows) != 1 {
		t.Fatalf("rehydrated search rows = %v, want one data bag item", searchRows)
	}
	searchRow := searchRows[0].(map[string]any)
	if searchRow["name"] != "data_bag_item_ponies_alice" {
		t.Fatalf("rehydrated search row name = %v, want data_bag_item_ponies_alice", searchRow["name"])
	}

	partialBody := []byte(`{"private_key":["ssh","private_key"],"public_key":["ssh","public_key"]}`)
	partialPayload := mustServeActivePostgresSearchRequest(t, restarted.router, http.MethodPost, "/organizations/ponyville/search/ponies?q=ssh_public_key:*", partialBody, http.StatusOK)
	partialRows := partialPayload["rows"].([]any)
	if len(partialRows) != 1 {
		t.Fatalf("rehydrated partial search rows = %v, want one data bag item", partialRows)
	}
	partialRow := partialRows[0].(map[string]any)
	if partialRow["url"] != "/organizations/ponyville/data/ponies/alice" {
		t.Fatalf("rehydrated partial search url = %v, want org data bag item URL", partialRow["url"])
	}
	data := partialRow["data"].(map[string]any)
	if data["private_key"] != "---RSA Private Key--- Alice" {
		t.Fatalf("rehydrated partial search private_key = %v, want Alice private key", data["private_key"])
	}

	mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodPut, "/data/ponies/alice", map[string]any{
		"id":    "alice",
		"color": "purple",
	}, http.StatusOK)
	updated := newActivePostgresBootstrapFixture(t, fixture.pgState)
	updatedItem := mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodGet, "/organizations/ponyville/data/ponies/alice", nil, http.StatusOK)
	if updatedItem["color"] != "purple" {
		t.Fatalf("rehydrated updated item color = %v, want purple", updatedItem["color"])
	}

	mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodDelete, "/data/ponies/alice", nil, http.StatusOK)
	deleted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	mustServeActivePostgresCoreObjectRequest(t, deleted.router, http.MethodGet, "/data/ponies/alice", nil, http.StatusNotFound)
	mustServeActivePostgresCoreObjectRequest(t, deleted.router, http.MethodDelete, "/organizations/ponyville/data/ponies", nil, http.StatusOK)
	empty := newActivePostgresBootstrapFixture(t, fixture.pgState)
	emptyBags := mustServeActivePostgresCoreObjectRequest(t, empty.router, http.MethodGet, "/data", nil, http.StatusOK)
	if _, ok := emptyBags["ponies"]; ok {
		t.Fatalf("rehydrated data bag list = %v, want ponies deleted", emptyBags)
	}
}

// TestActivePostgresCoreObjectsRehydrateEncryptedDataBagPayloads pins restart
// behavior for encrypted-looking data bag items without introducing any server
// knowledge of encrypted field schemas or secrets.
func TestActivePostgresCoreObjectsRehydrateEncryptedDataBagPayloads(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID
	orgItemPath := "/organizations/ponyville/data/" + bagName + "/" + itemID

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data", map[string]any{
		"name": bagName,
	}, http.StatusCreated)
	createPayload := testfixtures.EncryptedDataBagItem()
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, bagPath, createPayload, http.StatusCreated)

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	createdAfterRestart := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, orgItemPath, nil, http.StatusOK)
	assertRawDataBagItemPayload(t, createdAfterRestart, createPayload)

	updatePayload := testfixtures.UpdatedEncryptedDataBagItem()
	mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodPut, itemPath, updatePayload, http.StatusOK)
	wantUpdated := testfixtures.CloneDataBagPayload(updatePayload)
	wantUpdated["id"] = itemID

	updated := newActivePostgresBootstrapFixture(t, fixture.pgState)
	updatedAfterRestart := mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodGet, orgItemPath, nil, http.StatusOK)
	assertRawDataBagItemPayload(t, updatedAfterRestart, wantUpdated)

	mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodDelete, orgItemPath, nil, http.StatusOK)
	deleted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	mustServeActivePostgresCoreObjectRequest(t, deleted.router, http.MethodGet, itemPath, nil, http.StatusNotFound)
}

// TestActivePostgresEncryptedDataBagInvalidWritesDoNotPersistOrSearch proves
// invalid encrypted-looking item writes do not mutate live state, persisted
// PostgreSQL state, or the search-visible projection rebuilt after restart.
func TestActivePostgresEncryptedDataBagInvalidWritesDoNotPersistOrSearch(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/data/" + bagName
	itemPath := bagPath + "/" + itemID

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data", map[string]any{
		"name": bagName,
	}, http.StatusCreated)
	createPayload := testfixtures.EncryptedDataBagItem()
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, bagPath, createPayload, http.StatusCreated)

	malformedRec := performDataBagRequestAs(t, fixture.router, "pivotal", http.MethodPost, bagPath, []byte(`{"id":"malformed"`))
	assertDataBagAPIError(t, malformedRec, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")

	missingID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	delete(missingID, "id")
	missingIDRec := performDataBagRequestAs(t, fixture.router, "pivotal", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, missingID))
	assertDataBagError(t, missingIDRec, http.StatusBadRequest, "Field 'id' missing")

	mismatchedUpdate := encryptedEnvelopeVariantPayload(t, itemID, func(envelope map[string]any) {
		envelope["encrypted_data"] = "blocked-persisted-ciphertext"
	})
	mismatchedUpdate["id"] = "wrong-item"
	mismatchedUpdate["environment"] = "blocked"
	mismatchRec := performDataBagRequestAs(t, fixture.router, "pivotal", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, mismatchedUpdate))
	assertDataBagError(t, mismatchRec, http.StatusBadRequest, "DataBagItem name mismatch.")

	assertActivePostgresEncryptedDataBagBaseline(t, fixture.router, itemPath, bagName, itemID, createPayload)
	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	assertActivePostgresEncryptedDataBagBaseline(t, restarted.router, itemPath, bagName, itemID, createPayload)
}

func TestActivePostgresCoreObjectsRehydratePoliciesAndPolicyGroups(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)

	revisionID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/policy_groups/dev/policies/appserver", canonicalPolicyPayloadForAPI("appserver", revisionID), http.StatusCreated)

	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	policiesPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/policies", nil, http.StatusOK)
	appserverPolicy, ok := policiesPayload["appserver"].(map[string]any)
	if !ok {
		t.Fatalf("rehydrated policies[appserver] = %T, want map[string]any", policiesPayload["appserver"])
	}
	if appserverPolicy["uri"] != "/policies/appserver" {
		t.Fatalf("rehydrated policy uri = %v, want /policies/appserver", appserverPolicy["uri"])
	}
	revisions := appserverPolicy["revisions"].(map[string]any)
	if _, ok := revisions[revisionID]; !ok {
		t.Fatalf("rehydrated policy revisions = %v, want %s", revisions, revisionID)
	}

	orgPolicyPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/policies/appserver", nil, http.StatusOK)
	orgRevisions := orgPolicyPayload["revisions"].(map[string]any)
	if _, ok := orgRevisions[revisionID]; !ok {
		t.Fatalf("rehydrated org policy revisions = %v, want %s", orgRevisions, revisionID)
	}

	groupsPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/policy_groups", nil, http.StatusOK)
	devGroup, ok := groupsPayload["dev"].(map[string]any)
	if !ok {
		t.Fatalf("rehydrated policy_groups[dev] = %T, want map[string]any", groupsPayload["dev"])
	}
	if devGroup["uri"] != "/policy_groups/dev" {
		t.Fatalf("rehydrated policy group uri = %v, want /policy_groups/dev", devGroup["uri"])
	}
	devPolicies := devGroup["policies"].(map[string]any)
	devAppserver := devPolicies["appserver"].(map[string]any)
	if devAppserver["revision_id"] != revisionID {
		t.Fatalf("rehydrated policy group revision_id = %v, want %s", devAppserver["revision_id"], revisionID)
	}

	orgGroupPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/policy_groups/dev", nil, http.StatusOK)
	if orgGroupPayload["uri"] != "/organizations/ponyville/policy_groups/dev" {
		t.Fatalf("rehydrated org policy group uri = %v, want org-scoped uri", orgGroupPayload["uri"])
	}
	orgGroupPolicies := orgGroupPayload["policies"].(map[string]any)
	orgGroupAppserver := orgGroupPolicies["appserver"].(map[string]any)
	if orgGroupAppserver["revision_id"] != revisionID {
		t.Fatalf("rehydrated org policy group revision_id = %v, want %s", orgGroupAppserver["revision_id"], revisionID)
	}

	assignmentPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/policy_groups/dev/policies/appserver", nil, http.StatusOK)
	if assignmentPayload["revision_id"] != revisionID {
		t.Fatalf("rehydrated assignment revision_id = %v, want %s", assignmentPayload["revision_id"], revisionID)
	}
	if _, ok := assignmentPayload["policy_group_list"]; ok {
		t.Fatalf("rehydrated assignment unexpectedly included policy_group_list: %v", assignmentPayload)
	}
	assertRehydratedCanonicalPolicyPayload(t, assignmentPayload)

	revisionPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodGet, "/organizations/ponyville/policies/appserver/revisions/"+revisionID, nil, http.StatusOK)
	assertRehydratedCanonicalPolicyPayload(t, revisionPayload)
	groupList := stringSliceFromAny(t, revisionPayload["policy_group_list"])
	if len(groupList) != 1 || groupList[0] != "dev" {
		t.Fatalf("rehydrated policy_group_list = %v, want [dev]", groupList)
	}

	updatedRevisionID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodPut, "/organizations/ponyville/policy_groups/dev/policies/appserver", canonicalPolicyPayloadForAPI("appserver", updatedRevisionID), http.StatusOK)

	updated := newActivePostgresBootstrapFixture(t, fixture.pgState)
	updatedAssignmentPayload := mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodGet, "/policy_groups/dev/policies/appserver", nil, http.StatusOK)
	if updatedAssignmentPayload["revision_id"] != updatedRevisionID {
		t.Fatalf("rehydrated updated assignment revision_id = %v, want %s", updatedAssignmentPayload["revision_id"], updatedRevisionID)
	}
	oldRevisionPayload := mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodGet, "/policies/appserver/revisions/"+revisionID, nil, http.StatusOK)
	oldGroupList := stringSliceFromAny(t, oldRevisionPayload["policy_group_list"])
	if len(oldGroupList) != 0 {
		t.Fatalf("rehydrated old revision policy_group_list = %v, want empty list", oldGroupList)
	}
	updatedRevisionPayload := mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodGet, "/policies/appserver/revisions/"+updatedRevisionID, nil, http.StatusOK)
	updatedGroupList := stringSliceFromAny(t, updatedRevisionPayload["policy_group_list"])
	if len(updatedGroupList) != 1 || updatedGroupList[0] != "dev" {
		t.Fatalf("rehydrated updated revision policy_group_list = %v, want [dev]", updatedGroupList)
	}

	mustServeActivePostgresCoreObjectRequest(t, updated.router, http.MethodDelete, "/policy_groups/dev/policies/appserver", nil, http.StatusOK)
	assignmentDeleted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	mustServeActivePostgresCoreObjectRequest(t, assignmentDeleted.router, http.MethodGet, "/policy_groups/dev/policies/appserver", nil, http.StatusNotFound)
	emptyGroupPayload := mustServeActivePostgresCoreObjectRequest(t, assignmentDeleted.router, http.MethodGet, "/organizations/ponyville/policy_groups/dev", nil, http.StatusOK)
	emptyPolicies := emptyGroupPayload["policies"].(map[string]any)
	if len(emptyPolicies) != 0 {
		t.Fatalf("rehydrated empty policy group policies = %v, want empty object", emptyPolicies)
	}

	mustServeActivePostgresCoreObjectRequest(t, assignmentDeleted.router, http.MethodDelete, "/organizations/ponyville/policy_groups/dev", nil, http.StatusOK)
	groupDeleted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	mustServeActivePostgresCoreObjectRequest(t, groupDeleted.router, http.MethodGet, "/policy_groups/dev", nil, http.StatusNotFound)

	mustServeActivePostgresCoreObjectRequest(t, groupDeleted.router, http.MethodDelete, "/policies/appserver", nil, http.StatusOK)
	policyDeleted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	mustServeActivePostgresCoreObjectRequest(t, policyDeleted.router, http.MethodGet, "/organizations/ponyville/policies/appserver", nil, http.StatusNotFound)
	emptyPoliciesPayload := mustServeActivePostgresCoreObjectRequest(t, policyDeleted.router, http.MethodGet, "/policies", nil, http.StatusOK)
	if _, ok := emptyPoliciesPayload["appserver"]; ok {
		t.Fatalf("rehydrated policy list = %v, want appserver deleted", emptyPoliciesPayload)
	}
}

func TestActivePostgresCoreObjectInvalidWritesDoNotPersistAcrossRestart(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/environments", map[string]any{
		"name":        "production",
		"description": "Production",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/nodes", map[string]any{
		"name":             "twilight",
		"chef_environment": "production",
		"normal":           map[string]any{"app": "demo"},
		"run_list":         []any{"recipe[demo]"},
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/roles", map[string]any{
		"name":     "web",
		"run_list": []any{"recipe[demo]"},
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data", map[string]any{
		"name": "ponies",
	}, http.StatusCreated)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/data/ponies", map[string]any{
		"id":    "alice",
		"color": "purple",
	}, http.StatusCreated)
	revisionID := "cccccccccccccccccccccccccccccccccccccccc"
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/policy_groups/dev/policies/appserver", canonicalPolicyPayloadForAPI("appserver", revisionID), http.StatusCreated)

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/environments/production", map[string]any{
		"name":              "production",
		"cookbook_versions": "not-a-map",
	}, http.StatusBadRequest)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/nodes/twilight", map[string]any{
		"name": "not-twilight",
	}, http.StatusBadRequest)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/roles/web", map[string]any{
		"run_list": "not-a-list",
	}, http.StatusBadRequest)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/data/ponies/alice", map[string]any{
		"id": "not-alice",
	}, http.StatusBadRequest)
	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPut, "/policy_groups/dev/policies/appserver", map[string]any{
		"name":        "not-appserver",
		"revision_id": "dddddddddddddddddddddddddddddddddddddddd",
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "0.1.0",
			},
		},
	}, http.StatusBadRequest)

	assertActivePostgresInvalidWriteBaseline(t, fixture.router, revisionID)
	restarted := newActivePostgresBootstrapFixture(t, fixture.pgState)
	assertActivePostgresInvalidWriteBaseline(t, restarted.router, revisionID)
}

func TestActivePostgresCoreObjectsRehydrateSandboxMetadataAndChecksums(t *testing.T) {
	root := t.TempDir()
	fileStore, err := blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	fixture := newActivePostgresBootstrapFixtureWithBlob(t, pgtest.NewState(pgtest.Seed{}), fileStore)

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)

	content := []byte("rarity packed this sandbox carefully")
	checksum := checksumHex(content)
	createPayload := mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations/ponyville/sandboxes", map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}, http.StatusCreated)
	sandboxID := createPayload["sandbox_id"].(string)
	if sandboxID == "" {
		t.Fatal("sandbox_id = empty, want non-empty id")
	}
	if createPayload["uri"] != "http://example.com/organizations/ponyville/sandboxes/"+sandboxID {
		t.Fatalf("org-scoped sandbox uri = %v, want org-scoped sandbox URI", createPayload["uri"])
	}
	checksumEntry := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)
	if checksumEntry["needs_upload"] != true {
		t.Fatalf("initial needs_upload = %v, want true", checksumEntry["needs_upload"])
	}
	uploadURL := checksumEntry["url"].(string)
	if !strings.HasPrefix(uploadURL, "http://example.com/_blob/checksums/"+checksum) {
		t.Fatalf("upload url = %q, want checksum upload URL", uploadURL)
	}

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(restart) error = %v", err)
	}
	restarted := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	uploadActivePostgresSandboxChecksum(t, restarted.router, uploadURL, content)

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(commit restart) error = %v", err)
	}
	restarted = newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	commitPayload := mustServeActivePostgresCoreObjectRequest(t, restarted.router, http.MethodPut, "/organizations/ponyville/sandboxes/"+sandboxID, map[string]any{
		"is_completed": true,
	}, http.StatusOK)
	if commitPayload["guid"] != sandboxID || commitPayload["name"] != sandboxID {
		t.Fatalf("committed guid/name = %v/%v, want %s", commitPayload["guid"], commitPayload["name"], sandboxID)
	}
	if commitPayload["is_completed"] != true {
		t.Fatalf("committed is_completed = %v, want true", commitPayload["is_completed"])
	}
	committedChecksums := stringSliceFromAny(t, commitPayload["checksums"])
	if len(committedChecksums) != 1 || committedChecksums[0] != checksum {
		t.Fatalf("committed checksums = %v, want [%s]", committedChecksums, checksum)
	}

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(deleted restart) error = %v", err)
	}
	committed := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	mustServeActivePostgresCoreObjectRequest(t, committed.router, http.MethodPut, "/sandboxes/"+sandboxID, map[string]any{
		"is_completed": true,
	}, http.StatusNotFound)
	reusePayload := mustServeActivePostgresCoreObjectRequest(t, committed.router, http.MethodPost, "/sandboxes", map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}, http.StatusCreated)
	reuseEntry := reusePayload["checksums"].(map[string]any)[checksum].(map[string]any)
	if reuseEntry["needs_upload"] != false {
		t.Fatalf("provider-backed reused needs_upload = %v, want false", reuseEntry["needs_upload"])
	}
	if _, ok := reuseEntry["url"]; ok {
		t.Fatalf("provider-backed reused checksum unexpectedly included url: %v", reuseEntry)
	}
}

func TestActivePostgresCoreObjectsRetainSandboxHeldChecksumAfterRestart(t *testing.T) {
	root := t.TempDir()
	fileStore, err := blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore() error = %v", err)
	}
	fixture := newActivePostgresBootstrapFixtureWithBlob(t, pgtest.NewState(pgtest.Seed{}), fileStore)

	mustServeActivePostgresCoreObjectRequest(t, fixture.router, http.MethodPost, "/organizations", map[string]any{
		"name":      "ponyville",
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated)

	content := []byte("puts 'sandbox held after restart'")
	checksum := uploadActivePostgresCookbookChecksumWithoutCommit(t, fixture.router, content)
	createActivePostgresCookbookVersion(t, fixture.router, "sandbox-held-restart", "1.0.0", checksum)

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(restart) error = %v", err)
	}
	restarted := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	downloadURL := activePostgresCookbookFileURL(t, restarted.router, "/cookbooks/sandbox-held-restart/1.0.0")

	deleteReq := newSignedJSONRequestAs(t, "pivotal", http.MethodDelete, "/cookbooks/sandbox-held-restart/1.0.0", nil)
	deleteRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(deleteRec, deleteReq)
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete sandbox-held cookbook status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}
	assertCookbookDownloadBody(t, restarted.router, downloadURL, string(content))

	fileStore, err = blob.NewFileStore(root)
	if err != nil {
		t.Fatalf("NewFileStore(after cleanup) error = %v", err)
	}
	afterDelete := newActivePostgresBootstrapFixtureWithBlob(t, fixture.pgState, fileStore)
	assertCookbookDownloadBody(t, afterDelete.router, downloadURL, string(content))
}

func uploadActivePostgresCookbookChecksumWithoutCommit(t *testing.T, router http.Handler, content []byte) string {
	t.Helper()

	checksum := checksumHex(content)
	createPayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodPost, "/sandboxes", map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}, http.StatusCreated)
	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)
	uploadActivePostgresSandboxChecksum(t, router, uploadURL, content)
	return checksum
}

func uploadActivePostgresSandboxChecksum(t *testing.T, router http.Handler, uploadURL string, content []byte) {
	t.Helper()

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/x-binary")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}
	if uploadRec.Body.Len() != 0 {
		t.Fatalf("upload checksum body length = %d, want 0", uploadRec.Body.Len())
	}
}

func createActivePostgresCookbookVersion(t *testing.T, router http.Handler, name, version, checksum string) {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPut, "/cookbooks/"+name+"/"+version, mustMarshalSandboxJSON(t, cookbookVersionPayload(name, version, checksum, nil)))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create cookbook %s/%s status = %d, want %d, body = %s", name, version, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func activePostgresCookbookFileURL(t *testing.T, router http.Handler, path string) string {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	req.Header.Set("X-Ops-Server-API-Version", "2")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	allFiles := payload["all_files"].([]any)
	if len(allFiles) == 0 {
		t.Fatalf("%s all_files = %v, want at least one entry", path, payload["all_files"])
	}
	return allFiles[0].(map[string]any)["url"].(string)
}

func assertRehydratedCanonicalPolicyPayload(t *testing.T, payload map[string]any) {
	t.Helper()

	namedRunLists, ok := payload["named_run_lists"].(map[string]any)
	if !ok {
		t.Fatalf("named_run_lists = %T, want map[string]any", payload["named_run_lists"])
	}
	updateJenkins := stringSliceFromAny(t, namedRunLists["update_jenkins"])
	if len(updateJenkins) != 1 || updateJenkins[0] != "recipe[policyfile_demo::other_recipe]" {
		t.Fatalf("named_run_lists[update_jenkins] = %v, want canonical list", updateJenkins)
	}

	cookbookLocks := payload["cookbook_locks"].(map[string]any)
	lock := cookbookLocks["policyfile_demo"].(map[string]any)
	if lock["version"] != "0.1.0" {
		t.Fatalf("policyfile_demo lock version = %v, want 0.1.0", lock["version"])
	}
	if _, ok := lock["scm_info"].(map[string]any); !ok {
		t.Fatalf("scm_info = %T, want map[string]any", lock["scm_info"])
	}
	if _, ok := lock["source_options"].(map[string]any); !ok {
		t.Fatalf("source_options = %T, want map[string]any", lock["source_options"])
	}
	if _, ok := payload["solution_dependencies"].(map[string]any); !ok {
		t.Fatalf("solution_dependencies = %T, want map[string]any", payload["solution_dependencies"])
	}
}

func assertActivePostgresInvalidWriteBaseline(t *testing.T, router http.Handler, revisionID string) {
	t.Helper()

	envPayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/environments/production", nil, http.StatusOK)
	if envPayload["description"] != "Production" {
		t.Fatalf("environment description after invalid writes = %v, want Production", envPayload["description"])
	}

	nodePayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/nodes/twilight", nil, http.StatusOK)
	if nodePayload["chef_environment"] != "production" {
		t.Fatalf("node chef_environment after invalid writes = %v, want production", nodePayload["chef_environment"])
	}
	normal, ok := nodePayload["normal"].(map[string]any)
	if !ok || normal["app"] != "demo" {
		t.Fatalf("node normal attrs after invalid writes = %v, want app=demo", nodePayload["normal"])
	}

	rolePayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/roles/web", nil, http.StatusOK)
	roleRunList := stringSliceFromAny(t, rolePayload["run_list"])
	if len(roleRunList) != 1 || roleRunList[0] != "recipe[demo]" {
		t.Fatalf("role run_list after invalid writes = %v, want [recipe[demo]]", roleRunList)
	}

	itemPayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/data/ponies/alice", nil, http.StatusOK)
	if itemPayload["color"] != "purple" {
		t.Fatalf("data bag item color after invalid writes = %v, want purple", itemPayload["color"])
	}

	assignmentPayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/policy_groups/dev/policies/appserver", nil, http.StatusOK)
	if assignmentPayload["revision_id"] != revisionID {
		t.Fatalf("policy assignment revision after invalid writes = %v, want %s", assignmentPayload["revision_id"], revisionID)
	}
	mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, "/policies/appserver/revisions/dddddddddddddddddddddddddddddddddddddddd", nil, http.StatusNotFound)
}

// assertActivePostgresEncryptedDataBagBaseline verifies the item body and the
// search-visible projection both remain on the original encrypted-looking data.
func assertActivePostgresEncryptedDataBagBaseline(t *testing.T, router http.Handler, itemPath, bagName, itemID string, want map[string]any) {
	t.Helper()

	itemPayload := mustServeActivePostgresCoreObjectRequest(t, router, http.MethodGet, itemPath, nil, http.StatusOK)
	assertRawDataBagItemPayload(t, itemPayload, want)
	assertActivePostgresDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "id:"+itemID), 1)
	assertActivePostgresDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "environment:blocked"), 0)
	assertActivePostgresDataBagSearchTotal(t, router, searchPath("/search/"+bagName, "id:malformed"), 0)
}

// assertActivePostgresDataBagSearchTotal signs searches as pivotal because the
// active PostgreSQL fixture does not seed the same default users as newTestRouter.
func assertActivePostgresDataBagSearchTotal(t *testing.T, router http.Handler, rawPath string, want float64) {
	t.Helper()

	payload := mustServeActivePostgresSearchRequest(t, router, http.MethodGet, rawPath, nil, http.StatusOK)
	if payload["total"] != want {
		t.Fatalf("search %s total = %v, want %v", rawPath, payload["total"], want)
	}
}

func mustServeActivePostgresCoreObjectRequest(t *testing.T, router http.Handler, method, path string, payload map[string]any, want int) map[string]any {
	t.Helper()

	var body []byte
	if payload != nil {
		body = mustMarshalSandboxJSON(t, payload)
	}
	req := newSignedJSONRequestAs(t, "pivotal", method, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, want, rec.Body.String())
	}
	if rec.Body.Len() == 0 {
		return nil
	}

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("json.Unmarshal(%s %s) error = %v, body = %s", method, path, err, rec.Body.String())
	}
	return out
}

func mustServeActivePostgresSearchRequest(t *testing.T, router http.Handler, method, path string, body []byte, want int) map[string]any {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	signPath := strings.Split(path, "?")[0]
	applySignedHeaders(t, req, "pivotal", "", method, signPath, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, want, rec.Body.String())
	}

	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("json.Unmarshal(%s %s) error = %v, body = %s", method, path, err, rec.Body.String())
	}
	return out
}
