package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestAPIVersionRoleCRUDPayloadSemantics(t *testing.T) {
	router := newTestRouter(t)
	mustCreateAPIVersionEnvironment(t, router, "production", "2")
	mustCreateAPIVersionEnvironment(t, router, "staging", "2")
	mustCreateAPIVersionEnvironment(t, router, "qa", "2")

	for _, serverAPIVersion := range []string{"0", "1", "2"} {
		t.Run("v"+serverAPIVersion, func(t *testing.T) {
			name := "versioned-role-" + serverAPIVersion
			initial := rolePayloadExpectation{
				Name:               name,
				Description:        "Role create " + serverAPIVersion,
				JSONClass:          "Chef::Role",
				ChefType:           "role",
				DefaultAttributes:  map[string]any{"tier": "frontend-" + serverAPIVersion},
				OverrideAttributes: map[string]any{"owner": "team-" + serverAPIVersion},
				RunList:            []string{"recipe[base]", "recipe[foo::default]", "role[db]"},
				EnvRunLists: map[string][]string{
					"production": {"recipe[nginx]", "role[app]"},
					"staging":    {},
				},
			}
			createBody := mustMarshalAPIVersionRolePayload(t, initial, []string{"base", "recipe[base]", "foo::default", "recipe[foo::default]", "role[db]", "role[db]"}, map[string][]string{
				"production": {"nginx", "recipe[nginx]", "role[app]", "role[app]"},
				"staging":    {},
			})
			createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/roles", createBody, serverAPIVersion)
			if createRec.Code != http.StatusCreated {
				t.Fatalf("create role status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
			}
			createPayload := mustDecodeObject(t, createRec)
			if len(createPayload) != 1 || createPayload["uri"] != "/roles/"+name {
				t.Fatalf("create role payload = %v, want only default-org URI", createPayload)
			}

			listRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/roles", nil, serverAPIVersion)
			if listRec.Code != http.StatusOK {
				t.Fatalf("list roles status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
			}
			if got := mustDecodeStringMap(t, listRec)[name]; got != "/roles/"+name {
				t.Fatalf("list role URI = %q, want /roles/%s", got, name)
			}

			assertHeadStatusWithVersion(t, router, "/organizations/ponyville/roles", serverAPIVersion, http.StatusOK)
			assertHeadStatusWithVersion(t, router, "/roles/"+name, serverAPIVersion, http.StatusOK)
			assertRolePayload(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/roles/"+name, serverAPIVersion), initial)
			assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/roles/"+name+"/environments/_default", serverAPIVersion)["run_list"], initial.RunList)
			assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/roles/"+name+"/environments/production", serverAPIVersion)["run_list"], initial.EnvRunLists["production"])
			assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/environments/production/roles/"+name, serverAPIVersion)["run_list"], initial.EnvRunLists["production"])
			assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/environments/staging/roles/"+name, serverAPIVersion)["run_list"], []string{})
			if got := readObjectWithAPIVersion(t, router, "/environments/qa/roles/"+name, serverAPIVersion)["run_list"]; got != nil {
				t.Fatalf("qa environment-linked role run_list = %v, want nil", got)
			}
			assertStringSliceEqual(t, mustDecodeStringSlice(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/roles/"+name+"/environments", nil, serverAPIVersion)), []string{"_default", "production", "staging"})

			updated := rolePayloadExpectation{
				Name:               name,
				Description:        "Role update " + serverAPIVersion,
				JSONClass:          "Chef::Role",
				ChefType:           "role",
				DefaultAttributes:  map[string]any{"tier": "backend-" + serverAPIVersion},
				OverrideAttributes: map[string]any{"owner": "updated-" + serverAPIVersion},
				RunList:            []string{"recipe[apache2]", "role[db]"},
				EnvRunLists: map[string][]string{
					"production": {},
					"qa":         {"recipe[smoke]"},
				},
			}
			updateBody := mustMarshalAPIVersionRolePayload(t, updated, []string{"apache2", "recipe[apache2]", "role[db]", "role[db]"}, map[string][]string{
				"production": {},
				"qa":         {"smoke", "recipe[smoke]"},
			})
			updateRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/roles/"+name, updateBody, serverAPIVersion)
			if updateRec.Code != http.StatusOK {
				t.Fatalf("update role status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
			}
			assertRolePayload(t, mustDecodeObject(t, updateRec), updated)

			deleteRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/organizations/ponyville/roles/"+name, nil, serverAPIVersion)
			if deleteRec.Code != http.StatusOK {
				t.Fatalf("delete role status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
			}
			assertRolePayload(t, mustDecodeObject(t, deleteRec), updated)
			assertObjectMissingWithVersion(t, router, "/roles/"+name, serverAPIVersion)
		})
	}
}

func TestAPIVersionRoleOmittedFieldsDefaultOnExplicitOrgAlias(t *testing.T) {
	router := newTestRouter(t)
	name := "minimal-versioned-role"

	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/roles", []byte(`{"name":"`+name+`"}`), "1")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("minimal explicit-org role create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	createPayload := mustDecodeObject(t, createRec)
	if createPayload["uri"] != "/organizations/ponyville/roles/"+name {
		t.Fatalf("minimal role create URI = %v, want explicit-org URI", createPayload["uri"])
	}

	assertRolePayload(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/roles/"+name, "1"), rolePayloadExpectation{
		Name:               name,
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
		RunListNull:        true,
		EnvRunLists:        map[string][]string{},
	})
}

func TestAPIVersionRoleValidationFailuresKeepExistingState(t *testing.T) {
	router := newTestRouter(t)
	name := "validated-role"
	current := rolePayloadExpectation{
		Name:               name,
		Description:        "valid role",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"tier": "frontend"},
		OverrideAttributes: map[string]any{"owner": "team-platform"},
		RunList:            []string{"recipe[base]", "role[db]"},
		EnvRunLists:        map[string][]string{"production": {"recipe[nginx]"}},
	}
	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, current, current.RunList, current.EnvRunLists), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create baseline role status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	for _, tc := range []struct {
		name          string
		method        string
		path          string
		body          []byte
		wantStatus    int
		wantMessages  []string
		wantAPIError  string
		wantMessage   string
		assertMissing string
	}{
		{
			name:          "create rejects invalid name",
			method:        http.MethodPost,
			path:          "/roles",
			body:          mustMarshalRoleMap(t, map[string]any{"name": "bad role"}),
			wantStatus:    http.StatusBadRequest,
			wantMessages:  []string{"Field 'name' invalid"},
			assertMissing: "/roles/bad-role",
		},
		{
			name:         "create rejects missing name",
			method:       http.MethodPost,
			path:         "/roles",
			body:         mustMarshalRoleMap(t, map[string]any{"description": "missing"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'name' missing"},
		},
		{
			name:          "create rejects unsupported top level fields",
			method:        http.MethodPost,
			path:          "/roles",
			body:          mustMarshalRoleMap(t, map[string]any{"name": "unsupported-role", "bogus": true}),
			wantStatus:    http.StatusBadRequest,
			wantMessages:  []string{"Invalid key bogus in request body"},
			assertMissing: "/roles/unsupported-role",
		},
		{
			name:         "create rejects malformed JSON",
			method:       http.MethodPost,
			path:         "/roles",
			body:         []byte(`{"name":"bad-json"`),
			wantStatus:   http.StatusBadRequest,
			wantAPIError: "invalid_json",
			wantMessage:  "request body must be valid JSON",
		},
		{
			name:          "create rejects trailing JSON",
			method:        http.MethodPost,
			path:          "/roles",
			body:          []byte(`{"name":"trailing-role"} {"name":"extra"}`),
			wantStatus:    http.StatusBadRequest,
			wantAPIError:  "invalid_json",
			wantMessage:   "request body must contain exactly one JSON document",
			assertMissing: "/roles/trailing-role",
		},
		{
			name:         "update rejects route and payload name mismatch",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": "other-role"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Role name mismatch."},
		},
		{
			name:         "update rejects invalid run list shape",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "run_list": "bad"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'run_list' is not a valid run list"},
		},
		{
			name:         "update rejects invalid run list entries",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "run_list": []any{"recipe[base]", "fake[bad]"}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'run_list' is not a valid run list"},
		},
		{
			name:         "update rejects non object environment run lists",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "env_run_lists": "bad"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'env_run_lists' contains invalid run lists"},
		},
		{
			name:         "update rejects invalid environment run list name",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "env_run_lists": map[string]any{"bad/env": []any{"recipe[base]"}}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'env_run_lists' contains invalid run lists"},
		},
		{
			name:         "update rejects invalid environment run list entries",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "env_run_lists": map[string]any{"production": []any{"recipe[base]", 123}}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'env_run_lists' contains invalid run lists"},
		},
		{
			name:         "update rejects non object default attributes",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "default_attributes": "bad"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'default_attributes' is not a hash"},
		},
		{
			name:         "update rejects invalid description",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "description": 123}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'description' invalid"},
		},
		{
			name:         "update rejects invalid chef_type",
			method:       http.MethodPut,
			path:         "/roles/" + name,
			body:         mustMarshalRoleMap(t, map[string]any{"name": name, "chef_type": "node"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'chef_type' invalid"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveSignedAPIVersionRequest(t, router, "pivotal", tc.method, tc.path, tc.body, "2")
			if tc.wantAPIError != "" {
				assertRoleAPIError(t, rec, tc.wantStatus, tc.wantAPIError, tc.wantMessage)
			} else {
				assertRoleValidationError(t, rec, tc.wantStatus, tc.wantMessages...)
			}

			assertRolePayload(t, readObjectWithAPIVersion(t, router, "/roles/"+name, "2"), current)
			if tc.assertMissing != "" {
				assertObjectMissingWithVersion(t, router, tc.assertMissing, "2")
			}
		})
	}
}

func TestAPIVersionRoleLinkedRoutesPreserveNormalizedRunLists(t *testing.T) {
	router := newTestRouter(t)
	mustCreateAPIVersionEnvironment(t, router, "production", "2")
	mustCreateAPIVersionEnvironment(t, router, "staging", "2")
	mustCreateAPIVersionEnvironment(t, router, "unassigned", "2")

	role := rolePayloadExpectation{
		Name:               "linked-role",
		Description:        "linked role",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"tier": "api"},
		OverrideAttributes: map[string]any{"owner": "platform"},
		RunList:            []string{"recipe[base]", "role[db]"},
		EnvRunLists: map[string][]string{
			"production": {"recipe[nginx]", "role[app]"},
			"staging":    {},
		},
	}
	createBody := mustMarshalAPIVersionRolePayload(t, role, []string{"base", "recipe[base]", "role[db]", "role[db]"}, map[string][]string{
		"production": {"nginx", "recipe[nginx]", "role[app]", "role[app]"},
		"staging":    {},
	})
	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/roles", createBody, "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create linked role status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	assertRolePayload(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/roles/"+role.Name, "2"), role)

	envNames := mustDecodeStringSlice(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/roles/"+role.Name+"/environments", nil, "2"))
	assertStringSliceEqual(t, envNames, []string{"_default", "production", "staging"})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/roles/"+role.Name+"/environments/_default", "2")["run_list"], role.RunList)
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/roles/"+role.Name+"/environments/production", "2")["run_list"], role.EnvRunLists["production"])
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/environments/production/roles/"+role.Name, "2")["run_list"], role.EnvRunLists["production"])
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/environments/staging/roles/"+role.Name, "2")["run_list"], []string{})
	if got := readObjectWithAPIVersion(t, router, "/roles/"+role.Name+"/environments/unassigned", "2")["run_list"]; got != nil {
		t.Fatalf("unassigned role environment run_list = %v, want nil", got)
	}
	if got := readObjectWithAPIVersion(t, router, "/environments/unassigned/roles/"+role.Name, "2")["run_list"]; got != nil {
		t.Fatalf("unassigned environment role run_list = %v, want nil", got)
	}

	missingEnv := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/roles/"+role.Name+"/environments/missing", nil, "2")
	assertEnvironmentValidationError(t, missingEnv, http.StatusNotFound, "Cannot load environment missing")
}

func TestAPIVersionEnvironmentCRUDPayloadSemantics(t *testing.T) {
	router := newTestRouter(t)

	for _, serverAPIVersion := range []string{"0", "1", "2"} {
		t.Run("v"+serverAPIVersion, func(t *testing.T) {
			name := "versioned-env-" + serverAPIVersion
			initial := environmentPayloadExpectation{
				Name:               name,
				Description:        "Environment create " + serverAPIVersion,
				JSONClass:          "Chef::Environment",
				ChefType:           "environment",
				CookbookVersions:   map[string]string{"apache2": "~> 2.0", "base": "1.0.0"},
				DefaultAttributes:  map[string]any{"region": "equus-" + serverAPIVersion},
				OverrideAttributes: map[string]any{"tier": "frontend-" + serverAPIVersion},
			}
			createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, initial), serverAPIVersion)
			if createRec.Code != http.StatusCreated {
				t.Fatalf("create environment status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
			}
			createPayload := mustDecodeObject(t, createRec)
			if len(createPayload) != 1 || createPayload["uri"] != "/environments/"+name {
				t.Fatalf("create environment payload = %v, want only default-org URI", createPayload)
			}

			listRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/environments", nil, serverAPIVersion)
			if listRec.Code != http.StatusOK {
				t.Fatalf("list environments status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
			}
			if got := mustDecodeStringMap(t, listRec)[name]; got != "/organizations/ponyville/environments/"+name {
				t.Fatalf("list environment URI = %q, want explicit-org URI", got)
			}

			assertHeadStatusWithVersion(t, router, "/environments", serverAPIVersion, http.StatusOK)
			assertHeadStatusWithVersion(t, router, "/organizations/ponyville/environments/"+name, serverAPIVersion, http.StatusOK)
			assertEnvironmentPayload(t, readObjectWithAPIVersion(t, router, "/environments/"+name, serverAPIVersion), initial)

			nodeName := "node-in-" + name
			nodeRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, nodePayloadExpectation{
				Name:            nodeName,
				JSONClass:       "Chef::Node",
				ChefType:        "node",
				ChefEnvironment: name,
				Override:        map[string]any{},
				Normal:          map[string]any{},
				Default:         map[string]any{},
				Automatic:       map[string]any{},
				RunList:         []string{},
			}), serverAPIVersion)
			if nodeRec.Code != http.StatusCreated {
				t.Fatalf("create environment node status = %d, want %d, body = %s", nodeRec.Code, http.StatusCreated, nodeRec.Body.String())
			}
			envNodes := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/environments/"+name+"/nodes", nil, serverAPIVersion)
			if envNodes.Code != http.StatusOK {
				t.Fatalf("environment nodes status = %d, want %d, body = %s", envNodes.Code, http.StatusOK, envNodes.Body.String())
			}
			if got := mustDecodeStringMap(t, envNodes)[nodeName]; got != "/nodes/"+nodeName {
				t.Fatalf("environment node URI = %q, want /nodes/%s", got, nodeName)
			}
			assertEmptyObjectResponse(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/environments/"+name+"/cookbooks", nil, serverAPIVersion))
			assertEmptyStringSliceResponse(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/environments/"+name+"/recipes", nil, serverAPIVersion))
			assertEmptyObjectResponse(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments/"+name+"/cookbook_versions", []byte(`{"run_list":[]}`), serverAPIVersion))

			updated := environmentPayloadExpectation{
				Name:               name,
				Description:        "Environment update " + serverAPIVersion,
				JSONClass:          "Chef::Environment",
				ChefType:           "environment",
				CookbookVersions:   map[string]string{"apache2": ">= 2.1.0"},
				DefaultAttributes:  map[string]any{"region": "canterlot-" + serverAPIVersion},
				OverrideAttributes: map[string]any{"tier": "backend-" + serverAPIVersion},
			}
			updateRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/environments/"+name, mustMarshalAPIVersionEnvironmentPayload(t, updated), serverAPIVersion)
			if updateRec.Code != http.StatusOK {
				t.Fatalf("update environment status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
			}
			assertEnvironmentPayload(t, mustDecodeObject(t, updateRec), updated)

			renamed := updated
			renamed.Name = name + "-renamed"
			renamePath := "/organizations/ponyville/environments/" + name
			renameRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, renamePath, mustMarshalAPIVersionEnvironmentPayload(t, renamed), serverAPIVersion)
			if renameRec.Code != http.StatusCreated {
				t.Fatalf("rename environment status = %d, want %d, body = %s", renameRec.Code, http.StatusCreated, renameRec.Body.String())
			}
			if got := renameRec.Header().Get("Location"); got != "/organizations/ponyville/environments/"+renamed.Name {
				t.Fatalf("rename Location = %q, want explicit-org environment URI", got)
			}
			assertEnvironmentPayload(t, mustDecodeObject(t, renameRec), renamed)
			assertObjectMissingWithVersion(t, router, "/environments/"+name, serverAPIVersion)

			deleteRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/environments/"+renamed.Name, nil, serverAPIVersion)
			if deleteRec.Code != http.StatusOK {
				t.Fatalf("delete environment status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
			}
			assertEnvironmentPayload(t, mustDecodeObject(t, deleteRec), renamed)
			assertObjectMissingWithVersion(t, router, "/organizations/ponyville/environments/"+renamed.Name, serverAPIVersion)
		})
	}
}

func TestAPIVersionEnvironmentOmittedFieldsDefaultAndDefaultImmutability(t *testing.T) {
	router := newTestRouter(t)
	name := "minimal-versioned-env"

	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/environments", []byte(`{"name":"`+name+`"}`), "1")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("minimal explicit-org environment create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	createPayload := mustDecodeObject(t, createRec)
	if createPayload["uri"] != "/organizations/ponyville/environments/"+name {
		t.Fatalf("minimal environment create URI = %v, want explicit-org URI", createPayload["uri"])
	}
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, router, "/organizations/ponyville/environments/"+name, "1"), environmentPayloadExpectation{
		Name:               name,
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{},
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	})

	deleteDefault := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/environments/_default", nil, "2")
	if deleteDefault.Code != http.StatusMethodNotAllowed {
		t.Fatalf("delete _default status = %d, want %d, body = %s", deleteDefault.Code, http.StatusMethodNotAllowed, deleteDefault.Body.String())
	}
	updateDefault := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/environments/_default", []byte(`{"name":"_default"}`), "2")
	if updateDefault.Code != http.StatusMethodNotAllowed {
		t.Fatalf("update _default status = %d, want %d, body = %s", updateDefault.Code, http.StatusMethodNotAllowed, updateDefault.Body.String())
	}
	assertInvalidServerAPIVersionResponse(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodDelete, "/environments/_default", nil, "3"), "3")
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, router, "/environments/_default", "2"), environmentPayloadExpectation{
		Name:               "_default",
		Description:        "The default Chef environment",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{},
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	})
}

func TestAPIVersionEnvironmentValidationFailuresKeepExistingState(t *testing.T) {
	router := newTestRouter(t)
	name := "validated-env"
	current := environmentPayloadExpectation{
		Name:               name,
		Description:        "valid environment",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{"demo": "= 1.0.0"},
		DefaultAttributes:  map[string]any{"region": "equus"},
		OverrideAttributes: map[string]any{"tier": "frontend"},
	}
	createRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, current), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create baseline environment status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	existing := environmentPayloadExpectation{
		Name:               "existing-env",
		Description:        "rename target",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{},
		DefaultAttributes:  map[string]any{"region": "canterlot"},
		OverrideAttributes: map[string]any{},
	}
	existingRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, existing), "2")
	if existingRec.Code != http.StatusCreated {
		t.Fatalf("create existing environment status = %d, want %d, body = %s", existingRec.Code, http.StatusCreated, existingRec.Body.String())
	}

	for _, tc := range []struct {
		name          string
		method        string
		path          string
		body          []byte
		wantStatus    int
		wantMessages  []string
		wantAPIError  string
		wantMessage   string
		assertMissing string
	}{
		{
			name:          "create rejects invalid name",
			method:        http.MethodPost,
			path:          "/environments",
			body:          mustMarshalEnvironmentMap(t, map[string]any{"name": "bad env"}),
			wantStatus:    http.StatusBadRequest,
			wantMessages:  []string{"Field 'name' invalid"},
			assertMissing: "/environments/bad-env",
		},
		{
			name:         "create rejects missing name",
			method:       http.MethodPost,
			path:         "/environments",
			body:         mustMarshalEnvironmentMap(t, map[string]any{"description": "missing"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'name' missing"},
		},
		{
			name:          "create rejects unsupported top level field",
			method:        http.MethodPost,
			path:          "/environments",
			body:          mustMarshalEnvironmentMap(t, map[string]any{"name": "unsupported-env", "bogus": true}),
			wantStatus:    http.StatusBadRequest,
			wantMessages:  []string{"Invalid key bogus in request body"},
			assertMissing: "/environments/unsupported-env",
		},
		{
			name:         "create rejects malformed JSON",
			method:       http.MethodPost,
			path:         "/environments",
			body:         []byte(`{"name":"bad-json"`),
			wantStatus:   http.StatusBadRequest,
			wantAPIError: "invalid_json",
			wantMessage:  "request body must be valid JSON",
		},
		{
			name:          "create rejects trailing JSON",
			method:        http.MethodPost,
			path:          "/environments",
			body:          []byte(`{"name":"trailing-env"} {"name":"extra"}`),
			wantStatus:    http.StatusBadRequest,
			wantAPIError:  "invalid_json",
			wantMessage:   "request body must contain exactly one JSON document",
			assertMissing: "/environments/trailing-env",
		},
		{
			name:         "update route payload mismatch conflicts with existing environment",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalAPIVersionEnvironmentPayload(t, existing),
			wantStatus:   http.StatusConflict,
			wantMessages: []string{"Environment already exists"},
		},
		{
			name:         "update rejects non object cookbook versions",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "cookbook_versions": "bad"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'cookbook_versions' is not a hash"},
		},
		{
			name:         "update rejects invalid cookbook key",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "cookbook_versions": map[string]any{"bad/name": "= 1.0.0"}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Invalid key 'bad/name' for cookbook_versions"},
		},
		{
			name:         "update rejects invalid cookbook constraint",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "cookbook_versions": map[string]any{"demo": "not a version"}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Invalid value 'not a version' for cookbook_versions"},
		},
		{
			name:         "update rejects non string cookbook constraint",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "cookbook_versions": map[string]any{"demo": 123}}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Invalid value '123' for cookbook_versions"},
		},
		{
			name:         "update rejects non object default attributes",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "default_attributes": "bad"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'default_attributes' is not a hash"},
		},
		{
			name:         "update rejects invalid json_class",
			method:       http.MethodPut,
			path:         "/environments/" + name,
			body:         mustMarshalEnvironmentMap(t, map[string]any{"name": name, "json_class": "Chef::Node"}),
			wantStatus:   http.StatusBadRequest,
			wantMessages: []string{"Field 'json_class' invalid"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := serveSignedAPIVersionRequest(t, router, "pivotal", tc.method, tc.path, tc.body, "2")
			if tc.wantAPIError != "" {
				assertEnvironmentAPIError(t, rec, tc.wantStatus, tc.wantAPIError, tc.wantMessage)
			} else {
				assertEnvironmentValidationError(t, rec, tc.wantStatus, tc.wantMessages...)
			}

			assertEnvironmentPayload(t, readObjectWithAPIVersion(t, router, "/environments/"+name, "2"), current)
			assertEnvironmentPayload(t, readObjectWithAPIVersion(t, router, "/environments/"+existing.Name, "2"), existing)
			if tc.assertMissing != "" {
				assertObjectMissingWithVersion(t, router, tc.assertMissing, "2")
			}
		})
	}
}

func TestAPIVersionEnvironmentLinkedRoutesApplyCookbookConstraints(t *testing.T) {
	router := newTestRouter(t)
	envName := "linked-env"
	env := environmentPayloadExpectation{
		Name:               envName,
		Description:        "linked environment",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{"linked-demo": "< 2.5.0"},
		DefaultAttributes:  map[string]any{"region": "equus"},
		OverrideAttributes: map[string]any{"tier": "linked"},
	}
	createEnv := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, env), "2")
	if createEnv.Code != http.StatusCreated {
		t.Fatalf("create linked environment status = %d, want %d, body = %s", createEnv.Code, http.StatusCreated, createEnv.Body.String())
	}
	createCookbookVersionWithRecipes(t, router, "linked-demo", "1.0.0", "default", "legacy")
	createCookbookVersionWithRecipes(t, router, "linked-demo", "2.0.0", "default", "users")
	createCookbookVersionWithRecipes(t, router, "linked-demo", "3.0.0", "default", "admins")
	createCookbookVersionWithRecipes(t, router, "linked-other", "0.5.0", "default", "misc")

	nodeName := "linked-node"
	createNode := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, nodePayloadExpectation{
		Name:            nodeName,
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: envName,
		Override:        map[string]any{},
		Normal:          map[string]any{},
		Default:         map[string]any{},
		Automatic:       map[string]any{},
		RunList:         []string{},
	}), "2")
	if createNode.Code != http.StatusCreated {
		t.Fatalf("create linked node status = %d, want %d, body = %s", createNode.Code, http.StatusCreated, createNode.Body.String())
	}

	cookbooks := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/environments/"+envName+"/cookbooks", nil, "2")
	if cookbooks.Code != http.StatusOK {
		t.Fatalf("environment cookbook collection status = %d, want %d, body = %s", cookbooks.Code, http.StatusOK, cookbooks.Body.String())
	}
	cookbookPayload := mustDecodeObject(t, cookbooks)
	assertCookbookVersionList(t, cookbookPayload, "linked-demo", "2.0.0")
	assertCookbookVersionList(t, cookbookPayload, "linked-other", "0.5.0")

	namedCookbook := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/environments/"+envName+"/cookbooks/linked-demo", nil, "2")
	if namedCookbook.Code != http.StatusOK {
		t.Fatalf("environment named cookbook status = %d, want %d, body = %s", namedCookbook.Code, http.StatusOK, namedCookbook.Body.String())
	}
	assertCookbookVersionList(t, mustDecodeObject(t, namedCookbook), "linked-demo", "2.0.0", "1.0.0")

	envNodes := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/environments/"+envName+"/nodes", nil, "2")
	if envNodes.Code != http.StatusOK {
		t.Fatalf("environment nodes status = %d, want %d, body = %s", envNodes.Code, http.StatusOK, envNodes.Body.String())
	}
	if got := mustDecodeStringMap(t, envNodes)[nodeName]; got != "/organizations/ponyville/nodes/"+nodeName {
		t.Fatalf("environment node URI = %q, want explicit-org node URI", got)
	}

	recipes := mustDecodeStringSlice(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/environments/"+envName+"/recipes", nil, "2"))
	assertStringSliceEqual(t, recipes, []string{"linked-demo", "linked-demo::users", "linked-other", "linked-other::misc"})
	assertEmptyObjectResponse(t, serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/environments/"+envName+"/cookbook_versions", []byte(`{"run_list":[]}`), "2"))
}

func TestActivePostgresRoleEnvironmentAPIVersionPayloadsRehydrateAndMutate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("ponyville")

	env := environmentPayloadExpectation{
		Name:               "production",
		Description:        "Persisted production",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{"demo": "= 1.0.0"},
		DefaultAttributes:  map[string]any{"region": "equus"},
		OverrideAttributes: map[string]any{"tier": "frontend"},
	}
	envCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/environments", mustMarshalAPIVersionEnvironmentPayload(t, env), "2")
	if envCreate.Code != http.StatusCreated {
		t.Fatalf("active Postgres environment create status = %d, want %d, body = %s", envCreate.Code, http.StatusCreated, envCreate.Body.String())
	}
	role := rolePayloadExpectation{
		Name:               "web",
		Description:        "Persisted web",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"role_default": "yes"},
		OverrideAttributes: map[string]any{"role_override": "no"},
		RunList:            []string{"recipe[base]", "role[db]"},
		EnvRunLists:        map[string][]string{"production": {"recipe[nginx]"}},
	}
	roleCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, role, role.RunList, role.EnvRunLists), "2")
	if roleCreate.Code != http.StatusCreated {
		t.Fatalf("active Postgres role create status = %d, want %d, body = %s", roleCreate.Code, http.StatusCreated, roleCreate.Body.String())
	}

	restarted := fixture.restart()
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, restarted.router, "/environments/production", "2"), env)
	assertRolePayload(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/roles/web", "2"), role)
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, restarted.router, "/environments/production/roles/web", "2")["run_list"], role.EnvRunLists["production"])

	updatedEnv := env
	updatedEnv.Description = "Persisted production updated"
	updatedEnv.DefaultAttributes = map[string]any{"region": "canterlot"}
	envUpdate := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/environments/production", mustMarshalAPIVersionEnvironmentPayload(t, updatedEnv), "2")
	if envUpdate.Code != http.StatusOK {
		t.Fatalf("active Postgres environment update status = %d, want %d, body = %s", envUpdate.Code, http.StatusOK, envUpdate.Body.String())
	}
	assertEnvironmentPayload(t, mustDecodeObject(t, envUpdate), updatedEnv)

	updatedRole := role
	updatedRole.Description = "Persisted web updated"
	updatedRole.RunList = []string{"recipe[apache2]"}
	updatedRole.EnvRunLists = map[string][]string{"production": {}}
	roleUpdate := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/roles/web", mustMarshalAPIVersionRolePayload(t, updatedRole, updatedRole.RunList, updatedRole.EnvRunLists), "2")
	if roleUpdate.Code != http.StatusOK {
		t.Fatalf("active Postgres role update status = %d, want %d, body = %s", roleUpdate.Code, http.StatusOK, roleUpdate.Body.String())
	}
	assertRolePayload(t, mustDecodeObject(t, roleUpdate), updatedRole)

	afterUpdateRestart := restarted.restart()
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, afterUpdateRestart.router, "/organizations/ponyville/environments/production", "2"), updatedEnv)
	assertRolePayload(t, readObjectWithAPIVersion(t, afterUpdateRestart.router, "/roles/web", "2"), updatedRole)

	roleDelete := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/organizations/ponyville/roles/web", nil, "2")
	if roleDelete.Code != http.StatusOK {
		t.Fatalf("active Postgres role delete status = %d, want %d, body = %s", roleDelete.Code, http.StatusOK, roleDelete.Body.String())
	}
	envDelete := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/environments/production", nil, "2")
	if envDelete.Code != http.StatusOK {
		t.Fatalf("active Postgres environment delete status = %d, want %d, body = %s", envDelete.Code, http.StatusOK, envDelete.Body.String())
	}

	afterDeleteRestart := afterUpdateRestart.restart()
	assertObjectMissingWithVersion(t, afterDeleteRestart.router, "/roles/web", "2")
	assertObjectMissingWithVersion(t, afterDeleteRestart.router, "/organizations/ponyville/environments/production", "2")
}

func TestActivePostgresOpenSearchEnvironmentFailuresAndRestartsPreserveStateSearchAndACLs(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "outside-user",
		DisplayName: "Outside User",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(outside-user) error = %v", err)
	}

	name := "durable-search-env"
	current := environmentPayloadExpectation{
		Name:               name,
		Description:        "env-good",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{"demo": "< 2.0.0"},
		DefaultAttributes:  map[string]any{"region": "equus"},
		OverrideAttributes: map[string]any{"tier": "frontend"},
	}
	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, current), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed environment create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "name:durable-search-env AND description:env-good"), "/search/environment", []string{name})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "region:equus AND tier:frontend"), "/search/environment", []string{name})

	snapshot := transport.SnapshotDocuments()
	aclBefore := readEnvironmentACLForTest(t, fixture, "ponyville", name)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, fixture.router, "/organizations/ponyville/environments/"+name, "2"), current)

	badUpdate := mustMarshalEnvironmentMap(t, map[string]any{
		"name":              name,
		"description":       "bad-validation",
		"cookbook_versions": map[string]any{"demo": "not a version"},
	})
	badUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/environments/"+name, badUpdate, "2")
	assertEnvironmentValidationError(t, badUpdateRec, http.StatusBadRequest, "Invalid value 'not a version' for cookbook_versions")

	outsideCreate := current
	outsideCreate.Name = "outside-blocked-env"
	outsideCreate.Description = "outside-env"
	outsideCreateRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, outsideCreate), "2")
	if outsideCreateRec.Code != http.StatusForbidden {
		t.Fatalf("outside environment create status = %d, want %d, body = %s", outsideCreateRec.Code, http.StatusForbidden, outsideCreateRec.Body.String())
	}
	outsideUpdate := current
	outsideUpdate.Description = "outside-update-env"
	outsideUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, "/environments/"+name, mustMarshalAPIVersionEnvironmentPayload(t, outsideUpdate), "2")
	if outsideUpdateRec.Code != http.StatusForbidden {
		t.Fatalf("outside environment update status = %d, want %d, body = %s", outsideUpdateRec.Code, http.StatusForbidden, outsideUpdateRec.Body.String())
	}
	outsideDeleteRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, "/environments/"+name, nil, "2")
	if outsideDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("outside environment delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
	}

	invalidCreateRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, outsideCreate), "2")
	if invalidCreateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user environment create status = %d, want %d, body = %s", invalidCreateRec.Code, http.StatusUnauthorized, invalidCreateRec.Body.String())
	}
	invalidUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodPut, "/environments/"+name, mustMarshalAPIVersionEnvironmentPayload(t, outsideUpdate), "2")
	if invalidUpdateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user environment update status = %d, want %d, body = %s", invalidUpdateRec.Code, http.StatusUnauthorized, invalidUpdateRec.Body.String())
	}
	invalidDeleteRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodDelete, "/environments/"+name, nil, "2")
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user environment delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	transport.RequireDocuments(t, snapshot)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, fixture.router, "/environments/"+name, "2"), current)
	assertEnvironmentACLEqual(t, readEnvironmentACLForTest(t, fixture, "ponyville", name), aclBefore)
	assertObjectMissingWithVersion(t, fixture.router, "/environments/outside-blocked-env", "2")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:bad-validation"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:outside-env"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:outside-update-env"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:env-good"), "/search/environment", []string{name})

	restarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/environments/"+name, "2"), current)
	assertEnvironmentACLEqual(t, readEnvironmentACLForTest(t, restarted, "ponyville", name), aclBefore)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/environment", "name:durable-search-env"), "/search/environment", []string{name})

	renamed := current
	renamed.Name = "durable-renamed-env"
	renamed.Description = "env-renamed"
	renamed.DefaultAttributes = map[string]any{"region": "cloudsdale"}
	renameRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/organizations/ponyville/environments/"+name, mustMarshalAPIVersionEnvironmentPayload(t, renamed), "2")
	if renameRec.Code != http.StatusCreated {
		t.Fatalf("rehydrated OpenSearch-backed environment rename status = %d, want %d, body = %s", renameRec.Code, http.StatusCreated, renameRec.Body.String())
	}
	if got := renameRec.Header().Get("Location"); got != "/organizations/ponyville/environments/"+renamed.Name {
		t.Fatalf("environment rename Location = %q, want explicit-org renamed URI", got)
	}
	assertEnvironmentPayload(t, mustDecodeObject(t, renameRec), renamed)
	assertObjectMissingWithVersion(t, restarted.router, "/organizations/ponyville/environments/"+name, "2")
	assertEnvironmentACLMissingForTest(t, restarted, "ponyville", name)
	assertEnvironmentACLEqual(t, readEnvironmentACLForTest(t, restarted, "ponyville", renamed.Name), aclBefore)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/environment", "name:durable-search-env"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/environment", "name:durable-renamed-env AND description:env-renamed"), "/search/environment", []string{renamed.Name})

	afterRenameRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, afterRenameRestart.router, "/environments/"+renamed.Name, "2"), renamed)
	assertEnvironmentACLEqual(t, readEnvironmentACLForTest(t, afterRenameRestart, "ponyville", renamed.Name), aclBefore)
	assertActiveOpenSearchFullRows(t, afterRenameRestart.router, searchPath("/search/environment", "description:env-renamed"), "/search/environment", []string{renamed.Name})

	updated := renamed
	updated.Description = "env-updated"
	updated.CookbookVersions = map[string]string{"demo": ">= 2.0.0"}
	updated.DefaultAttributes = map[string]any{"region": "canterlot"}
	updated.OverrideAttributes = map[string]any{"tier": "backend"}
	updateRec := serveSignedAPIVersionRequest(t, afterRenameRestart.router, "pivotal", http.MethodPut, "/environments/"+renamed.Name, mustMarshalAPIVersionEnvironmentPayload(t, updated), "2")
	if updateRec.Code != http.StatusOK {
		t.Fatalf("rehydrated OpenSearch-backed environment update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	assertEnvironmentPayload(t, mustDecodeObject(t, updateRec), updated)
	assertActiveOpenSearchFullRows(t, afterRenameRestart.router, searchPath("/search/environment", "description:env-renamed"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, afterRenameRestart.router, searchPath("/search/environment", "description:env-updated AND region:canterlot"), "/search/environment", []string{renamed.Name})

	afterUpdateRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, afterUpdateRestart.router, "/organizations/ponyville/environments/"+renamed.Name, "2"), updated)
	assertEnvironmentACLEqual(t, readEnvironmentACLForTest(t, afterUpdateRestart, "ponyville", renamed.Name), aclBefore)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/environment", "description:env-updated"), "/search/environment", []string{renamed.Name})

	deleteRec := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/environments/"+renamed.Name, nil, "2")
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("rehydrated OpenSearch-backed environment delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}
	assertEnvironmentPayload(t, mustDecodeObject(t, deleteRec), updated)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/environment", "name:durable-renamed-env"), "/search/environment", []string{})

	afterDeleteRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertObjectMissingWithVersion(t, afterDeleteRestart.router, "/organizations/ponyville/environments/"+renamed.Name, "2")
	assertEnvironmentACLMissingForTest(t, afterDeleteRestart, "ponyville", renamed.Name)
	assertActiveOpenSearchFullRows(t, afterDeleteRestart.router, searchPath("/search/environment", "description:env-updated"), "/search/environment", []string{})
}

func TestActivePostgresOpenSearchRoleFailuresAndRestartsPreserveStateSearchAndACLs(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")
	mustCreateAPIVersionEnvironment(t, fixture.router, "production", "2")
	mustCreateAPIVersionEnvironment(t, fixture.router, "qa", "2")
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "outside-user",
		DisplayName: "Outside User",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(outside-user) error = %v", err)
	}

	name := "durable-search-role"
	current := rolePayloadExpectation{
		Name:               name,
		Description:        "role-good",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"role_default": "yes"},
		OverrideAttributes: map[string]any{"role_override": "no"},
		RunList:            []string{"recipe[base]", "role[db]"},
		EnvRunLists:        map[string][]string{"production": {"recipe[nginx]"}},
	}
	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, current, current.RunList, current.EnvRunLists), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed role create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:role-good AND role_default:yes"), "/search/role", []string{name})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "recipe:base AND role:db"), "/search/role", []string{name})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, fixture.router, "/roles/"+name+"/environments/production", "2")["run_list"], current.EnvRunLists["production"])

	snapshot := transport.SnapshotDocuments()
	aclBefore := readRoleACLForTest(t, fixture, "ponyville", name)
	assertRolePayload(t, readObjectWithAPIVersion(t, fixture.router, "/organizations/ponyville/roles/"+name, "2"), current)

	badUpdate := mustMarshalRoleMap(t, map[string]any{
		"name":        name,
		"description": "bad-validation",
		"run_list":    []any{"recipe[base]", "fake[bad]"},
	})
	badUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/roles/"+name, badUpdate, "2")
	assertRoleValidationError(t, badUpdateRec, http.StatusBadRequest, "Field 'run_list' is not a valid run list")

	outsideCreate := current
	outsideCreate.Name = "outside-blocked-role"
	outsideCreate.Description = "outside-role"
	outsideCreateRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, outsideCreate, outsideCreate.RunList, outsideCreate.EnvRunLists), "2")
	if outsideCreateRec.Code != http.StatusForbidden {
		t.Fatalf("outside role create status = %d, want %d, body = %s", outsideCreateRec.Code, http.StatusForbidden, outsideCreateRec.Body.String())
	}
	outsideUpdate := current
	outsideUpdate.Description = "outside-update-role"
	outsideUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, "/roles/"+name, mustMarshalAPIVersionRolePayload(t, outsideUpdate, outsideUpdate.RunList, outsideUpdate.EnvRunLists), "2")
	if outsideUpdateRec.Code != http.StatusForbidden {
		t.Fatalf("outside role update status = %d, want %d, body = %s", outsideUpdateRec.Code, http.StatusForbidden, outsideUpdateRec.Body.String())
	}
	outsideDeleteRec := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, "/roles/"+name, nil, "2")
	if outsideDeleteRec.Code != http.StatusForbidden {
		t.Fatalf("outside role delete status = %d, want %d, body = %s", outsideDeleteRec.Code, http.StatusForbidden, outsideDeleteRec.Body.String())
	}

	invalidCreateRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, outsideCreate, outsideCreate.RunList, outsideCreate.EnvRunLists), "2")
	if invalidCreateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user role create status = %d, want %d, body = %s", invalidCreateRec.Code, http.StatusUnauthorized, invalidCreateRec.Body.String())
	}
	invalidUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodPut, "/roles/"+name, mustMarshalAPIVersionRolePayload(t, outsideUpdate, outsideUpdate.RunList, outsideUpdate.EnvRunLists), "2")
	if invalidUpdateRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user role update status = %d, want %d, body = %s", invalidUpdateRec.Code, http.StatusUnauthorized, invalidUpdateRec.Body.String())
	}
	invalidDeleteRec := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodDelete, "/roles/"+name, nil, "2")
	if invalidDeleteRec.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user role delete status = %d, want %d, body = %s", invalidDeleteRec.Code, http.StatusUnauthorized, invalidDeleteRec.Body.String())
	}

	transport.RequireDocuments(t, snapshot)
	assertRolePayload(t, readObjectWithAPIVersion(t, fixture.router, "/roles/"+name, "2"), current)
	assertRoleACLEqual(t, readRoleACLForTest(t, fixture, "ponyville", name), aclBefore)
	assertObjectMissingWithVersion(t, fixture.router, "/roles/outside-blocked-role", "2")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:bad-validation"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:outside-role"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:outside-update-role"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:role-good"), "/search/role", []string{name})

	restarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRolePayload(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/roles/"+name, "2"), current)
	assertRoleACLEqual(t, readRoleACLForTest(t, restarted, "ponyville", name), aclBefore)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "description:role-good AND role_default:yes"), "/search/role", []string{name})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/roles/"+name+"/environments/production", "2")["run_list"], current.EnvRunLists["production"])

	updated := current
	updated.Description = "role-updated"
	updated.DefaultAttributes = map[string]any{"role_default": "updated"}
	updated.OverrideAttributes = map[string]any{"role_override": "yes"}
	updated.RunList = []string{"recipe[apache2]", "role[cache]"}
	updated.EnvRunLists = map[string][]string{"production": {}, "qa": {"recipe[smoke]"}}
	updateRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodPut, "/roles/"+name, mustMarshalAPIVersionRolePayload(t, updated, updated.RunList, updated.EnvRunLists), "2")
	if updateRec.Code != http.StatusOK {
		t.Fatalf("rehydrated OpenSearch-backed role update status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}
	assertRolePayload(t, mustDecodeObject(t, updateRec), updated)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "description:role-good"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "description:role-updated AND role_default:updated"), "/search/role", []string{name})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "recipe:apache2 AND role:cache"), "/search/role", []string{name})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, restarted.router, "/environments/production/roles/"+name, "2")["run_list"], []string{})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/environments/qa/roles/"+name, "2")["run_list"], updated.EnvRunLists["qa"])

	afterUpdateRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertRolePayload(t, readObjectWithAPIVersion(t, afterUpdateRestart.router, "/roles/"+name, "2"), updated)
	assertRoleACLEqual(t, readRoleACLForTest(t, afterUpdateRestart, "ponyville", name), aclBefore)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/role", "description:role-updated"), "/search/role", []string{name})
	assertStringSliceFromAnyEqual(t, readObjectWithAPIVersion(t, afterUpdateRestart.router, "/roles/"+name+"/environments/qa", "2")["run_list"], updated.EnvRunLists["qa"])

	deleteRec := serveSignedAPIVersionRequest(t, afterUpdateRestart.router, "pivotal", http.MethodDelete, "/organizations/ponyville/roles/"+name, nil, "2")
	if deleteRec.Code != http.StatusOK {
		t.Fatalf("rehydrated OpenSearch-backed role delete status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}
	assertRolePayload(t, mustDecodeObject(t, deleteRec), updated)
	assertActiveOpenSearchFullRows(t, afterUpdateRestart.router, searchPath("/search/role", "description:role-updated"), "/search/role", []string{})

	afterDeleteRestart := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertObjectMissingWithVersion(t, afterDeleteRestart.router, "/roles/"+name, "2")
	assertRoleACLMissingForTest(t, afterDeleteRestart, "ponyville", name)
	assertActiveOpenSearchFullRows(t, afterDeleteRestart.router, searchPath("/search/role", "description:role-updated"), "/search/role", []string{})
}

func TestActivePostgresOpenSearchRoleEnvironmentAPIVersionFieldsAndRejectedWritesNoMutation(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	env := environmentPayloadExpectation{
		Name:               "search-env",
		Description:        "env-good",
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{"api": "= 1.0.0"},
		DefaultAttributes:  map[string]any{"region": "equus"},
		OverrideAttributes: map[string]any{"tier": "frontend"},
	}
	envCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, env), "2")
	if envCreate.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed environment create status = %d, want %d, body = %s", envCreate.Code, http.StatusCreated, envCreate.Body.String())
	}
	role := rolePayloadExpectation{
		Name:               "search-role",
		Description:        "role-good",
		JSONClass:          "Chef::Role",
		ChefType:           "role",
		DefaultAttributes:  map[string]any{"role_default": "yes"},
		OverrideAttributes: map[string]any{"role_override": "no"},
		RunList:            []string{"recipe[base]", "role[db]"},
		EnvRunLists:        map[string][]string{"search-env": {"recipe[api]"}},
	}
	roleCreate := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, role, role.RunList, role.EnvRunLists), "2")
	if roleCreate.Code != http.StatusCreated {
		t.Fatalf("OpenSearch-backed role create status = %d, want %d, body = %s", roleCreate.Code, http.StatusCreated, roleCreate.Body.String())
	}
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "outside-user",
		DisplayName: "Outside User",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(outside-user) error = %v", err)
	}

	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:env-good AND region:equus"), "/search/environment", []string{"search-env"})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:role-good AND role_default:yes"), "/search/role", []string{"search-role"})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "recipe:base AND role:db"), "/search/role", []string{"search-role"})
	assertActiveOpenSearchPartialData(t, fixture.router, searchPath("/search/environment", "description:env-good"), "/search/environment", []byte(`{"desc":["description"],"region":["default_attributes","region"],"api_constraint":["cookbook_versions","api"]}`), "/environments/search-env", map[string]any{
		"desc":           "env-good",
		"region":         "equus",
		"api_constraint": "= 1.0.0",
	})
	assertActiveOpenSearchPartialData(t, fixture.router, searchPath("/search/role", "description:role-good"), "/search/role", []byte(`{"desc":["description"],"run_list":["run_list"],"role_default":["default_attributes","role_default"]}`), "/roles/search-role", map[string]any{
		"desc":         "role-good",
		"run_list":     []any{"recipe[base]", "role[db]"},
		"role_default": "yes",
	})

	snapshot := transport.SnapshotDocuments()
	blockedEnv := env
	blockedEnv.Name = "blocked-env"
	blockedEnv.Description = "blocked-env-term"
	envCreateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, blockedEnv), "3")
	assertInvalidServerAPIVersionResponse(t, envCreateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertObjectMissingWithVersion(t, fixture.router, "/environments/blocked-env", "2")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:blocked-env-term"), "/search/environment", []string{})

	blockedRole := role
	blockedRole.Name = "blocked-role"
	blockedRole.Description = "blocked-role-term"
	roleCreateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, blockedRole, blockedRole.RunList, blockedRole.EnvRunLists), "3")
	assertInvalidServerAPIVersionResponse(t, roleCreateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertObjectMissingWithVersion(t, fixture.router, "/roles/blocked-role", "2")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:blocked-role-term"), "/search/role", []string{})

	badEnvUpdate := env
	badEnvUpdate.Description = "bad-env-term"
	envUpdateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/environments/search-env", mustMarshalAPIVersionEnvironmentPayload(t, badEnvUpdate), "3")
	assertInvalidServerAPIVersionResponse(t, envUpdateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, fixture.router, "/environments/search-env", "2"), env)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:bad-env-term"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:env-good"), "/search/environment", []string{"search-env"})

	badRoleUpdate := role
	badRoleUpdate.Description = "bad-role-term"
	roleUpdateBlocked := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/roles/search-role", mustMarshalAPIVersionRolePayload(t, badRoleUpdate, badRoleUpdate.RunList, badRoleUpdate.EnvRunLists), "3")
	assertInvalidServerAPIVersionResponse(t, roleUpdateBlocked, "3")
	transport.RequireDocuments(t, snapshot)
	assertRolePayload(t, readObjectWithAPIVersion(t, fixture.router, "/roles/search-role", "2"), role)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:bad-role-term"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:role-good"), "/search/role", []string{"search-role"})

	assertInvalidServerAPIVersionResponse(t, serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodDelete, "/environments/search-env", nil, "3"), "3")
	assertInvalidServerAPIVersionResponse(t, serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodDelete, "/roles/search-role", nil, "3"), "3")
	transport.RequireDocuments(t, snapshot)

	outsideEnvCreate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, blockedEnv), "2")
	if outsideEnvCreate.Code != http.StatusForbidden {
		t.Fatalf("outside environment create status = %d, want %d, body = %s", outsideEnvCreate.Code, http.StatusForbidden, outsideEnvCreate.Body.String())
	}
	outsideRoleCreate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, "/roles", mustMarshalAPIVersionRolePayload(t, blockedRole, blockedRole.RunList, blockedRole.EnvRunLists), "2")
	if outsideRoleCreate.Code != http.StatusForbidden {
		t.Fatalf("outside role create status = %d, want %d, body = %s", outsideRoleCreate.Code, http.StatusForbidden, outsideRoleCreate.Body.String())
	}
	outsideEnvUpdate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, "/environments/search-env", mustMarshalAPIVersionEnvironmentPayload(t, badEnvUpdate), "2")
	if outsideEnvUpdate.Code != http.StatusForbidden {
		t.Fatalf("outside environment update status = %d, want %d, body = %s", outsideEnvUpdate.Code, http.StatusForbidden, outsideEnvUpdate.Body.String())
	}
	outsideRoleUpdate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, "/roles/search-role", mustMarshalAPIVersionRolePayload(t, badRoleUpdate, badRoleUpdate.RunList, badRoleUpdate.EnvRunLists), "2")
	if outsideRoleUpdate.Code != http.StatusForbidden {
		t.Fatalf("outside role update status = %d, want %d, body = %s", outsideRoleUpdate.Code, http.StatusForbidden, outsideRoleUpdate.Body.String())
	}
	outsideEnvDelete := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, "/environments/search-env", nil, "2")
	if outsideEnvDelete.Code != http.StatusForbidden {
		t.Fatalf("outside environment delete status = %d, want %d, body = %s", outsideEnvDelete.Code, http.StatusForbidden, outsideEnvDelete.Body.String())
	}
	outsideRoleDelete := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodDelete, "/roles/search-role", nil, "2")
	if outsideRoleDelete.Code != http.StatusForbidden {
		t.Fatalf("outside role delete status = %d, want %d, body = %s", outsideRoleDelete.Code, http.StatusForbidden, outsideRoleDelete.Body.String())
	}
	transport.RequireDocuments(t, snapshot)
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, fixture.router, "/environments/search-env", "2"), env)
	assertRolePayload(t, readObjectWithAPIVersion(t, fixture.router, "/roles/search-role", "2"), role)

	restarted := fixture.restart()
	assertEnvironmentPayload(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/environments/search-env", "2"), env)
	assertRolePayload(t, readObjectWithAPIVersion(t, restarted.router, "/organizations/ponyville/roles/search-role", "2"), role)
}

type rolePayloadExpectation struct {
	Name               string
	Description        string
	JSONClass          string
	ChefType           string
	DefaultAttributes  map[string]any
	OverrideAttributes map[string]any
	RunList            []string
	RunListNull        bool
	EnvRunLists        map[string][]string
}

type environmentPayloadExpectation struct {
	Name               string
	Description        string
	JSONClass          string
	ChefType           string
	CookbookVersions   map[string]string
	DefaultAttributes  map[string]any
	OverrideAttributes map[string]any
}

func mustCreateAPIVersionEnvironment(t *testing.T, router http.Handler, name, serverAPIVersion string) {
	t.Helper()

	env := environmentPayloadExpectation{
		Name:               name,
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		CookbookVersions:   map[string]string{},
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	}
	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/environments", mustMarshalAPIVersionEnvironmentPayload(t, env), serverAPIVersion)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create helper environment %s status = %d, want %d, body = %s", name, rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func mustMarshalAPIVersionRolePayload(t *testing.T, role rolePayloadExpectation, runList []string, envRunLists map[string][]string) []byte {
	t.Helper()

	payload := map[string]any{
		"name":                role.Name,
		"description":         role.Description,
		"json_class":          defaultString(role.JSONClass, "Chef::Role"),
		"chef_type":           defaultString(role.ChefType, "role"),
		"default_attributes":  role.DefaultAttributes,
		"override_attributes": role.OverrideAttributes,
		"run_list":            runList,
		"env_run_lists":       envRunLists,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(role payload) error = %v", err)
	}
	return body
}

func mustMarshalAPIVersionEnvironmentPayload(t *testing.T, env environmentPayloadExpectation) []byte {
	t.Helper()

	payload := map[string]any{
		"name":                env.Name,
		"description":         env.Description,
		"json_class":          defaultString(env.JSONClass, "Chef::Environment"),
		"chef_type":           defaultString(env.ChefType, "environment"),
		"cookbook_versions":   env.CookbookVersions,
		"default_attributes":  env.DefaultAttributes,
		"override_attributes": env.OverrideAttributes,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(environment payload) error = %v", err)
	}
	return body
}

func readObjectWithAPIVersion(t *testing.T, router http.Handler, path, serverAPIVersion string) map[string]any {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	if rec.Code != http.StatusOK {
		t.Fatalf("read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
	return mustDecodeObject(t, rec)
}

func assertRolePayload(t *testing.T, payload map[string]any, want rolePayloadExpectation) {
	t.Helper()

	if payload["name"] != want.Name {
		t.Fatalf("role name = %v, want %q", payload["name"], want.Name)
	}
	if payload["description"] != want.Description {
		t.Fatalf("role description = %v, want %q", payload["description"], want.Description)
	}
	if payload["json_class"] != defaultString(want.JSONClass, "Chef::Role") {
		t.Fatalf("role json_class = %v, want Chef::Role", payload["json_class"])
	}
	if payload["chef_type"] != defaultString(want.ChefType, "role") {
		t.Fatalf("role chef_type = %v, want role", payload["chef_type"])
	}
	assertPayloadMapEqual(t, payload, "default_attributes", want.DefaultAttributes)
	assertPayloadMapEqual(t, payload, "override_attributes", want.OverrideAttributes)
	if want.RunListNull {
		if payload["run_list"] != nil {
			t.Fatalf("role run_list = %v, want null", payload["run_list"])
		}
	} else {
		assertStringSliceFromAnyEqual(t, payload["run_list"], want.RunList)
	}
	assertEnvRunListsEqual(t, payload["env_run_lists"], want.EnvRunLists)
}

func assertEnvironmentPayload(t *testing.T, payload map[string]any, want environmentPayloadExpectation) {
	t.Helper()

	if payload["name"] != want.Name {
		t.Fatalf("environment name = %v, want %q", payload["name"], want.Name)
	}
	if payload["description"] != want.Description {
		t.Fatalf("environment description = %v, want %q", payload["description"], want.Description)
	}
	if payload["json_class"] != defaultString(want.JSONClass, "Chef::Environment") {
		t.Fatalf("environment json_class = %v, want Chef::Environment", payload["json_class"])
	}
	if payload["chef_type"] != defaultString(want.ChefType, "environment") {
		t.Fatalf("environment chef_type = %v, want environment", payload["chef_type"])
	}
	assertStringMapFieldEqual(t, payload, "cookbook_versions", want.CookbookVersions)
	assertPayloadMapEqual(t, payload, "default_attributes", want.DefaultAttributes)
	assertPayloadMapEqual(t, payload, "override_attributes", want.OverrideAttributes)
}

func assertPayloadMapEqual(t *testing.T, payload map[string]any, field string, want map[string]any) {
	t.Helper()

	got, ok := payload[field].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any", field, payload[field])
	}
	if want == nil {
		want = map[string]any{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", field, got, want)
	}
}

func assertStringMapFieldEqual(t *testing.T, payload map[string]any, field string, want map[string]string) {
	t.Helper()

	raw, ok := payload[field].(map[string]any)
	if !ok {
		t.Fatalf("%s = %T, want map[string]any", field, payload[field])
	}
	if want == nil {
		want = map[string]string{}
	}
	got := make(map[string]string, len(raw))
	for key, value := range raw {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("%s[%s] = %T, want string", field, key, value)
		}
		got[key] = text
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %v, want %v", field, got, want)
	}
}

func assertEnvRunListsEqual(t *testing.T, value any, want map[string][]string) {
	t.Helper()

	got, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("env_run_lists = %T, want map[string]any", value)
	}
	if want == nil {
		want = map[string][]string{}
	}
	if len(got) != len(want) {
		t.Fatalf("env_run_lists len = %d, want %d (%v)", len(got), len(want), got)
	}
	for envName, wantRunList := range want {
		assertStringSliceFromAnyEqual(t, got[envName], wantRunList)
	}
}

func mustDecodeStringSlice(t *testing.T, rec *httptest.ResponseRecorder) []string {
	t.Helper()

	if rec.Code != http.StatusOK {
		t.Fatalf("string slice response status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var raw []any
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("json.Unmarshal(string slice) error = %v; body = %s", err, rec.Body.String())
	}
	return stringSliceFromAny(t, raw)
}

func assertStringSliceEqual(t *testing.T, got, want []string) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("slice = %v, want %v", got, want)
	}
}

func assertHeadStatusWithVersion(t *testing.T, router http.Handler, path, serverAPIVersion string, want int) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodHead, path, nil, serverAPIVersion)
	if rec.Code != want {
		t.Fatalf("HEAD %s status = %d, want %d, body = %s", path, rec.Code, want, rec.Body.String())
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD %s body length = %d, want 0", path, rec.Body.Len())
	}
}

func assertObjectMissingWithVersion(t *testing.T, router http.Handler, path, serverAPIVersion string) {
	t.Helper()

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, path, nil, serverAPIVersion)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing object %s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func assertEmptyObjectResponse(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if rec.Code != http.StatusOK {
		t.Fatalf("empty object response status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	payload := mustDecodeObject(t, rec)
	if len(payload) != 0 {
		t.Fatalf("object response = %v, want empty object", payload)
	}
}

func assertEmptyStringSliceResponse(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()

	if rec.Code != http.StatusOK {
		t.Fatalf("empty string slice response status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	var payload []string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(empty string slice) error = %v; body = %s", err, rec.Body.String())
	}
	if len(payload) != 0 {
		t.Fatalf("string slice response = %v, want empty", payload)
	}
}

func mustMarshalRoleMap(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(role map) error = %v", err)
	}
	return body
}

func assertRoleValidationError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantMessages ...string) {
	t.Helper()

	if rec.Code != wantStatus {
		t.Fatalf("role validation status = %d, want %d, body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	var payload struct {
		Error []string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(role validation error) error = %v; body = %s", err, rec.Body.String())
	}
	if !reflect.DeepEqual(payload.Error, wantMessages) {
		t.Fatalf("role validation errors = %v, want %v", payload.Error, wantMessages)
	}
}

func assertRoleAPIError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantError, wantMessage string) {
	t.Helper()

	if rec.Code != wantStatus {
		t.Fatalf("role API error status = %d, want %d, body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(role API error) error = %v; body = %s", err, rec.Body.String())
	}
	if payload["error"] != wantError {
		t.Fatalf("role API error = %v, want %q", payload["error"], wantError)
	}
	if payload["message"] != wantMessage {
		t.Fatalf("role API message = %v, want %q", payload["message"], wantMessage)
	}
}

func readRoleACLForTest(t *testing.T, fixture *activePostgresBootstrapFixture, org, name string) authz.ACL {
	t.Helper()

	acl, ok, err := fixture.state.ResolveACL(context.Background(), authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	})
	if err != nil {
		t.Fatalf("ResolveACL(role %s/%s) error = %v", org, name, err)
	}
	if !ok {
		t.Fatalf("ResolveACL(role %s/%s) missing", org, name)
	}
	return acl
}

func assertRoleACLEqual(t *testing.T, got, want authz.ACL) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("role ACL = %+v, want %+v", got, want)
	}
}

func assertRoleACLMissingForTest(t *testing.T, fixture *activePostgresBootstrapFixture, org, name string) {
	t.Helper()

	if acl, ok, err := fixture.state.ResolveACL(context.Background(), authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	}); err != nil || ok {
		t.Fatalf("ResolveACL(role %s/%s after delete) = %+v, ok %t, err %v; want missing nil", org, name, acl, ok, err)
	}
}

func mustMarshalEnvironmentMap(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal(environment map) error = %v", err)
	}
	return body
}

func assertEnvironmentValidationError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantMessages ...string) {
	t.Helper()

	if rec.Code != wantStatus {
		t.Fatalf("environment validation status = %d, want %d, body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	var payload struct {
		Error []string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(environment validation error) error = %v; body = %s", err, rec.Body.String())
	}
	if !reflect.DeepEqual(payload.Error, wantMessages) {
		t.Fatalf("environment validation errors = %v, want %v", payload.Error, wantMessages)
	}
}

func assertEnvironmentAPIError(t *testing.T, rec *httptest.ResponseRecorder, wantStatus int, wantError, wantMessage string) {
	t.Helper()

	if rec.Code != wantStatus {
		t.Fatalf("environment API error status = %d, want %d, body = %s", rec.Code, wantStatus, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(environment API error) error = %v; body = %s", err, rec.Body.String())
	}
	if payload["error"] != wantError {
		t.Fatalf("environment API error = %v, want %q", payload["error"], wantError)
	}
	if payload["message"] != wantMessage {
		t.Fatalf("environment API message = %v, want %q", payload["message"], wantMessage)
	}
}

func readEnvironmentACLForTest(t *testing.T, fixture *activePostgresBootstrapFixture, org, name string) authz.ACL {
	t.Helper()

	acl, ok, err := fixture.state.ResolveACL(context.Background(), authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	})
	if err != nil {
		t.Fatalf("ResolveACL(environment %s/%s) error = %v", org, name, err)
	}
	if !ok {
		t.Fatalf("ResolveACL(environment %s/%s) missing", org, name)
	}
	return acl
}

func assertEnvironmentACLEqual(t *testing.T, got, want authz.ACL) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("environment ACL = %+v, want %+v", got, want)
	}
}

func assertEnvironmentACLMissingForTest(t *testing.T, fixture *activePostgresBootstrapFixture, org, name string) {
	t.Helper()

	if acl, ok, err := fixture.state.ResolveACL(context.Background(), authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	}); err != nil || ok {
		t.Fatalf("ResolveACL(environment %s/%s after delete) = %+v, ok %t, err %v; want missing nil", org, name, acl, ok, err)
	}
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
