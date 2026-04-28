package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

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

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
