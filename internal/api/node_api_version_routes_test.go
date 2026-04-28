package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestAPIVersionNodeCRUDPayloadSemantics(t *testing.T) {
	router := newTestRouter(t)

	for _, serverAPIVersion := range []string{"0", "1", "2"} {
		t.Run("v"+serverAPIVersion, func(t *testing.T) {
			name := "versioned-node-" + serverAPIVersion
			initial := nodePayloadExpectation{
				Name:            name,
				JSONClass:       "Chef::Node",
				ChefType:        "node",
				ChefEnvironment: "_default",
				Override:        map[string]any{"origin": "create-" + serverAPIVersion},
				Normal:          map[string]any{"team": "friendship"},
				Default:         map[string]any{"build": "010"},
				Automatic:       map[string]any{"platform": "equestria"},
				RunList:         []string{"base", "recipe[apache2::default]", "role[web]"},
				PolicyName:      "delivery-app-" + serverAPIVersion,
				PolicyGroup:     "prod-blue-" + serverAPIVersion,
			}
			createBody := mustMarshalAPIVersionNodePayload(t, initial)
			createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/nodes", createBody, serverAPIVersion)
			if createRec.Code != http.StatusCreated {
				t.Fatalf("create node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
			}
			createPayload := mustDecodeObject(t, createRec)
			if len(createPayload) != 1 || createPayload["uri"] != "/nodes/"+name {
				t.Fatalf("create payload = %v, want only default-org node URI", createPayload)
			}

			listRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/nodes", nil, serverAPIVersion)
			if listRec.Code != http.StatusOK {
				t.Fatalf("list nodes status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
			}
			listPayload := mustDecodeStringMap(t, listRec)
			if listPayload[name] != "/nodes/"+name {
				t.Fatalf("list node URI = %q, want /nodes/%s", listPayload[name], name)
			}

			assertNodeHeadStatus(t, router, "/nodes", serverAPIVersion, http.StatusOK)
			assertNodeHeadStatus(t, router, "/nodes/"+name, serverAPIVersion, http.StatusOK)

			getPayload := readNodePayloadWithVersion(t, router, "/nodes/"+name, serverAPIVersion)
			assertNodePayload(t, getPayload, initial)

			updated := nodePayloadExpectation{
				Name:            name,
				JSONClass:       "Chef::Node",
				ChefType:        "node",
				ChefEnvironment: "_default",
				Override:        map[string]any{"origin": "update-" + serverAPIVersion},
				Normal:          map[string]any{"team": "weather"},
				Default:         map[string]any{"build": "020"},
				Automatic:       map[string]any{"platform": "equestria", "kernel": "pegasus"},
				RunList:         []string{"role[db]", "mysql@1.2.3"},
				PolicyName:      "delivery-app-updated-" + serverAPIVersion,
				PolicyGroup:     "prod-green-" + serverAPIVersion,
			}
			updateBody := mustMarshalAPIVersionNodePayload(t, updated)
			updateRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/nodes/"+name, updateBody, serverAPIVersion)
			if updateRec.Code != http.StatusOK {
				t.Fatalf("update node status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
			}
			assertNodePayload(t, mustDecodeObject(t, updateRec), updated)

			deleteRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/nodes/"+name, nil, serverAPIVersion)
			if deleteRec.Code != http.StatusOK {
				t.Fatalf("delete node status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
			}
			assertNodePayload(t, mustDecodeObject(t, deleteRec), updated)

			missingRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/nodes/"+name, nil, serverAPIVersion)
			if missingRec.Code != http.StatusNotFound {
				t.Fatalf("deleted node read status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
			}
		})
	}
}

func TestAPIVersionNodeOmittedFieldsDefaultOnExplicitOrgAlias(t *testing.T) {
	router := newTestRouter(t)
	name := "minimal-versioned-node"

	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/nodes", []byte(`{"name":"`+name+`"}`), "1")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("minimal explicit-org node create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	createPayload := mustDecodeObject(t, createRec)
	if createPayload["uri"] != "/organizations/ponyville/nodes/"+name {
		t.Fatalf("create uri = %v, want explicit-org URI", createPayload["uri"])
	}

	listRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/nodes", nil, "1")
	if listRec.Code != http.StatusOK {
		t.Fatalf("explicit-org node list status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}
	listPayload := mustDecodeStringMap(t, listRec)
	if listPayload[name] != "/organizations/ponyville/nodes/"+name {
		t.Fatalf("explicit-org list URI = %q, want explicit-org node URI", listPayload[name])
	}

	payload := readNodePayloadWithVersion(t, router, "/organizations/ponyville/nodes/"+name, "1")
	assertNodePayload(t, payload, nodePayloadExpectation{
		Name:            name,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{},
		Normal:          map[string]any{},
		Default:         map[string]any{},
		Automatic:       map[string]any{},
		RunList:         []string{},
	})
	if _, ok := payload["policy_name"]; ok {
		t.Fatalf("minimal node unexpectedly included policy_name: %v", payload)
	}
	if _, ok := payload["policy_group"]; ok {
		t.Fatalf("minimal node unexpectedly included policy_group: %v", payload)
	}
}

func TestActivePostgresNodeAPIVersionPayloadsRehydrateAndMutate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("ponyville")
	name := "persisted-versioned-node"

	initial := nodePayloadExpectation{
		Name:            name,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "production",
		Override:        map[string]any{"origin": "postgres-create"},
		Normal:          map[string]any{"team": "friendship"},
		Default:         map[string]any{"build": "030"},
		Automatic:       map[string]any{"platform": "equestria"},
		RunList:         []string{"base", "role[web]"},
		PolicyName:      "delivery-app",
		PolicyGroup:     "prod-blue",
	}
	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/nodes", mustMarshalAPIVersionNodePayload(t, initial), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("active Postgres node create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	restarted := fixture.restart()
	assertNodePayload(t, readNodePayloadWithVersion(t, restarted.router, "/organizations/ponyville/nodes/"+name, "2"), initial)
	defaultAliasList := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/nodes", nil, "2")
	if defaultAliasList.Code != http.StatusOK {
		t.Fatalf("rehydrated default-org node list status = %d, want %d, body = %s", defaultAliasList.Code, http.StatusOK, defaultAliasList.Body.String())
	}
	if got := mustDecodeStringMap(t, defaultAliasList)[name]; got != "/nodes/"+name {
		t.Fatalf("rehydrated default-org node URI = %q, want /nodes/%s", got, name)
	}

	updated := nodePayloadExpectation{
		Name:            name,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "production",
		Override:        map[string]any{"origin": "postgres-update"},
		Normal:          map[string]any{"team": "weather"},
		Default:         map[string]any{"build": "040"},
		Automatic:       map[string]any{"platform": "equestria"},
		RunList:         []string{"recipe[apache2::default]"},
		PolicyName:      "delivery-app",
		PolicyGroup:     "prod-green",
	}
	updateRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/nodes/"+name, mustMarshalAPIVersionNodePayload(t, updated), "2")
	if updateRec.Code != http.StatusOK {
		t.Fatalf("rehydrated default-org node update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	assertNodePayload(t, mustDecodeObject(t, updateRec), updated)

	afterUpdateRestart := restarted.restart()
	assertNodePayload(t, readNodePayloadWithVersion(t, afterUpdateRestart.router, "/organizations/ponyville/nodes/"+name, "2"), updated)

	deleteRec := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/organizations/ponyville/nodes/"+name, nil, "2")
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("rehydrated explicit-org node delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}
	assertNodePayload(t, mustDecodeObject(t, deleteRec), updated)

	afterDeleteRestart := afterUpdateRestart.restart()
	missingRec := serveSignedAPIVersionRequest(t, afterDeleteRestart.router, "pivotal", http.MethodGet, "/organizations/ponyville/nodes/"+name, nil, "2")
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("deleted persisted node read status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

func TestActivePostgresOpenSearchNodeAPIVersionFieldsAndInvalidVersionNoMutation(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	name := "search-versioned-node"
	current := nodePayloadExpectation{
		Name:            name,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{},
		Normal:          map[string]any{"team": "friendship"},
		Default:         map[string]any{"build": "050"},
		Automatic:       map[string]any{},
		RunList:         []string{"base", "role[web]"},
		PolicyName:      "delivery-app",
		PolicyGroup:     "prod-blue",
	}
	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, current), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed node create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "policy_name:delivery-app AND policy_group:prod-blue"), "/search/node", []string{name})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship AND build:050"), "/search/node", []string{name})
	assertActiveOpenSearchPartialData(t, fixture.router, searchPath("/search/node", "policy_name:delivery-app"), "/search/node", []byte(`{"policy_name":["policy_name"],"policy_group":["policy_group"],"run_list":["run_list"],"team":["team"],"build":["build"]}`), "/nodes/"+name, map[string]any{
		"policy_name":  "delivery-app",
		"policy_group": "prod-blue",
		"run_list":     []any{"recipe[base]", "role[web]"},
		"team":         "friendship",
		"build":        "050",
	})

	snapshot := transport.SnapshotDocuments()
	blocked := nodePayloadExpectation{
		Name:            "blocked-versioned-node",
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{},
		Normal:          map[string]any{"team": "blocked"},
		Default:         map[string]any{},
		Automatic:       map[string]any{},
		RunList:         []string{},
	}
	createBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, blocked), "3")
	assertInvalidServerAPIVersionResponse(t, createBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertNodeMissingWithVersion(t, fixture.router, "/nodes/blocked-versioned-node", "2")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:blocked"), "/search/node", []string{})

	badUpdate := current
	badUpdate.Normal = map[string]any{"team": "bad-version"}
	updateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/nodes/"+name, mustMarshalAPIVersionNodePayload(t, badUpdate), "3")
	assertInvalidServerAPIVersionResponse(t, updateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertNodePayload(t, readNodePayloadWithVersion(t, fixture.router, "/nodes/"+name, "2"), current)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:bad-version"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{name})

	deleteBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodDelete, "/nodes/"+name, nil, "3")
	assertInvalidServerAPIVersionResponse(t, deleteBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertNodePayload(t, readNodePayloadWithVersion(t, fixture.router, "/nodes/"+name, "2"), current)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{name})
}

type nodePayloadExpectation struct {
	Name            string
	JSONClass       string
	ChefType        string
	ChefEnvironment string
	Override        map[string]any
	Normal          map[string]any
	Default         map[string]any
	Automatic       map[string]any
	RunList         []string
	RunListNull     bool
	PolicyName      string
	PolicyGroup     string
}

func mustMarshalAPIVersionNodePayload(t *testing.T, node nodePayloadExpectation) []byte {
	t.Helper()

	payload := map[string]any{
		"name":             node.Name,
		"json_class":       fallbackString(node.JSONClass, "Chef::Node"),
		"chef_type":        fallbackString(node.ChefType, "node"),
		"chef_environment": fallbackString(node.ChefEnvironment, "_default"),
		"override":         node.Override,
		"normal":           node.Normal,
		"default":          node.Default,
		"automatic":        node.Automatic,
		"run_list":         node.RunList,
	}
	if node.PolicyName != "" {
		payload["policy_name"] = node.PolicyName
	}
	if node.PolicyGroup != "" {
		payload["policy_group"] = node.PolicyGroup
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(node payload) error = %v", err)
	}
	return body
}

func readNodePayloadWithVersion(t *testing.T, router http.Handler, path, serverAPIVersion string) map[string]any {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	if rec.Code != http.StatusOK {
		t.Fatalf("read node %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
	return mustDecodeObject(t, rec)
}

func assertNodePayload(t *testing.T, payload map[string]any, want nodePayloadExpectation) {
	t.Helper()

	if payload["name"] != want.Name {
		t.Fatalf("node name = %v, want %q", payload["name"], want.Name)
	}
	if payload["json_class"] != fallbackString(want.JSONClass, "Chef::Node") {
		t.Fatalf("node json_class = %v, want Chef::Node", payload["json_class"])
	}
	if payload["chef_type"] != fallbackString(want.ChefType, "node") {
		t.Fatalf("node chef_type = %v, want node", payload["chef_type"])
	}
	if payload["chef_environment"] != fallbackString(want.ChefEnvironment, "_default") {
		t.Fatalf("node chef_environment = %v, want %q", payload["chef_environment"], fallbackString(want.ChefEnvironment, "_default"))
	}
	assertMapFieldEqual(t, payload, "override", want.Override)
	assertMapFieldEqual(t, payload, "normal", want.Normal)
	assertMapFieldEqual(t, payload, "default", want.Default)
	assertMapFieldEqual(t, payload, "automatic", want.Automatic)
	if want.RunListNull {
		if payload["run_list"] != nil {
			t.Fatalf("node run_list = %v, want null", payload["run_list"])
		}
	} else {
		assertStringSliceFromAnyEqual(t, payload["run_list"], want.RunList)
	}
	if want.PolicyName == "" {
		if _, ok := payload["policy_name"]; ok {
			t.Fatalf("node unexpectedly included policy_name: %v", payload)
		}
	} else if payload["policy_name"] != want.PolicyName {
		t.Fatalf("node policy_name = %v, want %q", payload["policy_name"], want.PolicyName)
	}
	if want.PolicyGroup == "" {
		if _, ok := payload["policy_group"]; ok {
			t.Fatalf("node unexpectedly included policy_group: %v", payload)
		}
	} else if payload["policy_group"] != want.PolicyGroup {
		t.Fatalf("node policy_group = %v, want %q", payload["policy_group"], want.PolicyGroup)
	}
}

func assertMapFieldEqual(t *testing.T, payload map[string]any, field string, want map[string]any) {
	t.Helper()

	got, ok := payload[field].(map[string]any)
	if !ok {
		t.Fatalf("node %s = %T, want map[string]any", field, payload[field])
	}
	if want == nil {
		want = map[string]any{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("node %s = %v, want %v", field, got, want)
	}
}

func mustDecodeStringMap(t *testing.T, rec *httptest.ResponseRecorder) map[string]string {
	t.Helper()

	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(string map) error = %v; body = %s", err, rec.Body.String())
	}
	return payload
}

func assertNodeHeadStatus(t *testing.T, router http.Handler, path, serverAPIVersion string, want int) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodHead, path, nil, serverAPIVersion)
	if rec.Code != want {
		t.Fatalf("HEAD %s status = %d, want %d, body = %s", path, rec.Code, want, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD %s body length = %d, want 0", path, rec.Body.Len())
	}
}

func assertNodeMissingWithVersion(t *testing.T, router http.Handler, path, serverAPIVersion string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing node %s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func fallbackString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
