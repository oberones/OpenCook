package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestValidatorBootstrapRegistrationCreatesNormalClientOnExplicitOrgRoute(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	body := []byte(`{"name":"twilight","create_key":true}`)
	req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("validator client create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create client) error = %v", err)
	}
	if createPayload["uri"] != "/organizations/canterlot/clients/twilight" {
		t.Fatalf("create uri = %v, want explicit-org client uri", createPayload["uri"])
	}
	privateKey, _ := createPayload["private_key"].(string)
	if !strings.Contains(privateKey, "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("private_key = %q, want generated RSA key in legacy response field", privateKey)
	}
	chefKey, ok := createPayload["chef_key"].(map[string]any)
	if !ok {
		t.Fatalf("chef_key missing from create response: %v", createPayload)
	}
	if chefKey["uri"] != "/organizations/canterlot/clients/twilight/keys/default" {
		t.Fatalf("chef_key.uri = %v, want explicit-org default key uri", chefKey["uri"])
	}
	if chefKey["name"] != "default" || chefKey["expiration_date"] != "infinity" {
		t.Fatalf("chef_key metadata = %v, want default/infinity", chefKey)
	}
	if !strings.Contains(chefKey["private_key"].(string), "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("chef_key.private_key missing generated key material: %v", chefKey)
	}
	if !strings.Contains(chefKey["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("chef_key.public_key missing generated key material: %v", chefKey)
	}

	client := newSignedClientRequestorFromClientCreatePayload(t, "canterlot", "twilight", createPayload)
	assertSignedClientCanReadSelf(t, router, client, "/organizations/canterlot/clients/twilight")
	assertClientReadPayload(t, router, "/organizations/canterlot/clients/twilight", "twilight", false)
	assertClientsGroupContains(t, router, "canterlot", "canterlot-validator", "twilight")
	assertClientACLReadable(t, router, "canterlot", "twilight")
	assertClientSearchRow(t, router, "/organizations/canterlot/search/client?q=name:twilight", "/organizations/canterlot/search/client", "twilight", "canterlot")
}

func TestValidatorBootstrapRegistrationUsesConfiguredDefaultOrgAlias(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		DefaultOrganization: "canterlot",
	})
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	body := []byte(`{"name":"spike","create_key":true}`)
	req := validator.newSignedJSONRequest(t, http.MethodPost, "/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("validator default-org client create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(default-org create client) error = %v", err)
	}
	if createPayload["uri"] != "/clients/spike" {
		t.Fatalf("create uri = %v, want default-org client uri", createPayload["uri"])
	}
	chefKey, ok := createPayload["chef_key"].(map[string]any)
	if !ok {
		t.Fatalf("chef_key missing from default-org create response: %v", createPayload)
	}
	if chefKey["uri"] != "/clients/spike/keys/default" {
		t.Fatalf("chef_key.uri = %v, want default-org default key uri", chefKey["uri"])
	}

	client := newSignedClientRequestorFromClientCreatePayload(t, "canterlot", "spike", createPayload)
	assertSignedClientCanReadSelf(t, router, client, "/clients/spike")
	assertClientReadPayload(t, router, "/clients/spike", "spike", false)
	assertClientsGroupContains(t, router, "canterlot", "canterlot-validator", "spike")
	assertClientSearchRow(t, router, "/search/client?q=name:spike", "/search/client", "spike", "canterlot")
}

func TestValidatorBootstrapRegistrationAcceptsClientNameAndExplicitPublicKey(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
	clientPrivateKey := mustParsePrivateKey(t)
	clientPublicKey := mustMarshalPublicKeyPEM(t, &clientPrivateKey.PublicKey)

	body := []byte(`{"clientname":"rainbow","create_key":false,"public_key":` + strconvQuote(clientPublicKey) + `}`)
	req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("validator explicit-key client create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(explicit-key create client) error = %v", err)
	}
	if _, ok := createPayload["private_key"]; ok {
		t.Fatalf("create response includes generated private_key for explicit public key: %v", createPayload)
	}
	chefKey, ok := createPayload["chef_key"].(map[string]any)
	if !ok {
		t.Fatalf("chef_key missing from explicit-key create response: %v", createPayload)
	}
	if _, ok := chefKey["private_key"]; ok {
		t.Fatalf("chef_key includes generated private_key for explicit public key: %v", chefKey)
	}
	if chefKey["uri"] != "/organizations/canterlot/clients/rainbow/keys/default" {
		t.Fatalf("chef_key.uri = %v, want explicit-org default key uri", chefKey["uri"])
	}
	if !strings.Contains(chefKey["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("chef_key.public_key missing explicit public key: %v", chefKey)
	}

	client := newSignedClientRequestorFromPrivateKeyPEM(t, "canterlot", "rainbow", testSigningKeyPEM)
	assertSignedClientCanReadSelf(t, router, client, "/organizations/canterlot/clients/rainbow")
	assertClientReadPayload(t, router, "/organizations/canterlot/clients/rainbow", "rainbow", false)
	assertClientsGroupContains(t, router, "canterlot", "rainbow")
}

func assertSignedClientCanReadSelf(t *testing.T, router http.Handler, client signedClientRequestor, path string) {
	t.Helper()

	req := client.newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("signed client self-read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
}

func assertClientReadPayload(t *testing.T, router http.Handler, path, name string, validator bool) {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin client read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(client read) error = %v", err)
	}
	if payload["name"] != name || payload["clientname"] != name {
		t.Fatalf("client read name/clientname = %v/%v, want %q", payload["name"], payload["clientname"], name)
	}
	if payload["validator"] != validator {
		t.Fatalf("client read validator = %v, want %v", payload["validator"], validator)
	}
	if !strings.Contains(payload["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("client read public_key missing PEM: %v", payload["public_key"])
	}
}

func assertClientsGroupContains(t *testing.T, router http.Handler, org string, members ...string) {
	t.Helper()

	path := "/organizations/" + org + "/groups/clients"
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("clients group read status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(clients group) error = %v", err)
	}
	clients := stringSliceFromAny(t, payload["clients"])
	for _, member := range members {
		if !containsString(clients, member) {
			t.Fatalf("clients group members = %v, want %q", clients, member)
		}
	}
}

func assertClientACLReadable(t *testing.T, router http.Handler, org, name string) {
	t.Helper()

	path := "/organizations/" + org + "/clients/" + name + "/_acl"
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("client ACL read status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(client ACL) error = %v", err)
	}
	if payload["read"] == nil || payload["delete"] == nil {
		t.Fatalf("client ACL missing read/delete permissions: %v", payload)
	}
}

func assertClientSearchRow(t *testing.T, router http.Handler, requestPath, signPath, name, org string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, requestPath, nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, signPath, nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("client search status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(client search) error = %v", err)
	}
	rows := payload["rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("client search rows = %v, want one row", rows)
	}
	row := rows[0].(map[string]any)
	if row["name"] != name || row["clientname"] != name {
		t.Fatalf("client search name/clientname = %v/%v, want %q", row["name"], row["clientname"], name)
	}
	if row["orgname"] != org || row["validator"] != false {
		t.Fatalf("client search org/validator = %v/%v, want %s/false", row["orgname"], row["validator"], org)
	}
}

func TestValidatorBootstrapRegistrationPreservesAdminClientCreate(t *testing.T) {
	router := newTestRouter(t)

	body := []byte(`{"name":"rarity","create_key":true}`)
	req := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(body))
	applySignedHeaders(t, req, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("admin client create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(admin create response) error = %v", err)
	}
	if payload["uri"] != "/organizations/ponyville/clients/rarity" {
		t.Fatalf("admin create uri = %v, want explicit-org uri", payload["uri"])
	}
	if _, ok := payload["chef_key"].(map[string]any); !ok {
		t.Fatalf("admin create chef_key missing: %v", payload)
	}
}

func TestValidatorBootstrapRegistrationOnlyGeneratedValidatorGetsCreateException(t *testing.T) {
	router := newTestRouter(t)
	createOrganizationAndCaptureValidator(t, router, "canterlot")
	extraValidator := createClientAsAdmin(t, router, "canterlot", "backup-validator", true)
	normalClient := createClientAsAdmin(t, router, "canterlot", "normal-client", false)

	body := []byte(`{"name":"scootaloo","create_key":true}`)
	req := extraValidator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("extra validator client create status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	assertClientMissing(t, router, "/organizations/canterlot/clients/scootaloo")

	body = []byte(`{"name":"applebloom","create_key":true}`)
	req = normalClient.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("normal client create status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	assertClientMissing(t, router, "/organizations/canterlot/clients/applebloom")
}

func TestValidatorBootstrapRegistrationCannotCreateValidatorOrAdminClient(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	for _, tc := range []struct {
		name string
		body string
		path string
	}{
		{
			name: "validator client",
			body: `{"name":"new-validator","validator":true,"create_key":true}`,
			path: "/organizations/canterlot/clients/new-validator",
		},
		{
			name: "admin client",
			body: `{"name":"adminish","admin":true,"create_key":true}`,
			path: "/organizations/canterlot/clients/adminish",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", []byte(tc.body))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("validator create %s status = %d, want %d, body = %s", tc.name, rec.Code, http.StatusForbidden, rec.Body.String())
			}
			assertClientMissing(t, router, tc.path)
		})
	}
}

func TestValidatorBootstrapRegistrationDoesNotBroadenAdminGroupOrClientContainerACL(t *testing.T) {
	router := newTestRouter(t)
	createOrganizationAndCaptureValidator(t, router, "canterlot")

	group := readAdminJSON(t, router, "/organizations/canterlot/groups/admins")
	adminClients := stringSliceFromAny(t, group["clients"])
	if containsString(adminClients, "canterlot-validator") {
		t.Fatalf("admins group clients = %v, want generated validator excluded", adminClients)
	}
	adminActors := stringSliceFromAny(t, group["actors"])
	if containsString(adminActors, "canterlot-validator") {
		t.Fatalf("admins group actors = %v, want generated validator excluded", adminActors)
	}

	acl := readAdminACLJSON(t, router, "/organizations/canterlot/containers/clients/_acl")
	createACL := acl["create"]
	createGroups := stringSliceFromAny(t, createACL["groups"])
	if !containsString(createGroups, "admins") {
		t.Fatalf("clients container create groups = %v, want admins", createGroups)
	}
	if containsString(createGroups, "clients") {
		t.Fatalf("clients container create groups = %v, want clients group excluded", createGroups)
	}
	createActors := stringSliceFromAny(t, createACL["actors"])
	if containsString(createActors, "canterlot-validator") {
		t.Fatalf("clients container create actors = %v, want generated validator excluded", createActors)
	}
}

func TestValidatorBootstrapRegistrationCreatesSelfOwnedNormalClientACL(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
	client := createClientAsValidator(t, router, validator, "/organizations/canterlot/clients", "twilight")

	acl := readAdminACLJSON(t, router, "/organizations/canterlot/clients/twilight/_acl")
	assertACLPermission(t, acl, "create", []string{"pivotal", "twilight"}, []string{"admins"})
	assertACLPermission(t, acl, "read", []string{"pivotal", "twilight"}, []string{"admins", "users"})
	assertACLPermission(t, acl, "update", []string{"pivotal", "twilight"}, []string{"admins"})
	assertACLPermission(t, acl, "delete", []string{"pivotal", "twilight"}, []string{"admins", "users"})
	assertACLPermission(t, acl, "grant", []string{"pivotal", "twilight"}, []string{"admins"})

	validatorACL := readAdminACLJSON(t, router, "/organizations/canterlot/clients/canterlot-validator/_acl")
	assertACLPermission(t, validatorACL, "create", []string{"pivotal"}, []string{"admins"})
	assertACLPermission(t, validatorACL, "read", []string{"pivotal"}, []string{"admins", "users"})
	assertACLPermission(t, validatorACL, "update", []string{"pivotal"}, []string{"admins"})
	assertACLPermission(t, validatorACL, "delete", []string{"pivotal"}, []string{"admins", "users"})
	assertACLPermission(t, validatorACL, "grant", []string{"pivotal"}, []string{"admins"})

	assertSignedClientCanReadSelf(t, router, client, "/organizations/canterlot/clients/twilight")
}

func TestValidatorBootstrapRegistrationClientsGroupPersistsAcrossRestart(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	validator := fixture.createOrganizationWithValidator("canterlot")
	createClientAsValidator(t, fixture.router, validator, "/organizations/canterlot/clients", "twilight")

	restarted := fixture.restart()
	group := readAdminJSON(t, restarted.router, "/organizations/canterlot/groups/clients")
	for _, member := range []string{"canterlot-validator", "twilight"} {
		clients := stringSliceFromAny(t, group["clients"])
		if !containsString(clients, member) {
			t.Fatalf("clients group clients = %v, want %q after restart", clients, member)
		}
		actors := stringSliceFromAny(t, group["actors"])
		if !containsString(actors, member) {
			t.Fatalf("clients group actors = %v, want %q after restart", actors, member)
		}
	}
}

func TestValidatorBootstrapRegistrationRejectsNormalUsersAndOutsideOrgClients(t *testing.T) {
	router := newTestRouter(t)
	createOrganizationAndCaptureValidator(t, router, "canterlot")
	outsideClient := createClientAsAdmin(t, router, "ponyville", "outside-client", false)

	body := []byte(`{"name":"user-created","create_key":true}`)
	req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, "/organizations/ponyville/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("normal user client create status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
	assertClientMissing(t, router, "/organizations/ponyville/clients/user-created")

	body = []byte(`{"name":"outside-created","create_key":true}`)
	req = outsideClient.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec = httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("outside-org client create status = %d, want %d, body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	assertClientMissing(t, router, "/organizations/canterlot/clients/outside-created")
}

func TestValidatorBootstrapRegistrationURLShapesForClientCollectionAndKeys(t *testing.T) {
	t.Run("explicit org", func(t *testing.T) {
		router := newTestRouter(t)
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
		client := createClientAsValidator(t, router, validator, "/organizations/canterlot/clients", "twilight")

		collection := readAdminJSON(t, router, "/organizations/canterlot/clients")
		if collection["twilight"] != "/organizations/canterlot/clients/twilight" {
			t.Fatalf("explicit-org collection twilight URL = %v, want explicit-org URL", collection["twilight"])
		}
		if collection["canterlot-validator"] != "/organizations/canterlot/clients/canterlot-validator" {
			t.Fatalf("explicit-org collection validator URL = %v, want explicit-org URL", collection["canterlot-validator"])
		}
		assertClientReadPayload(t, router, "/organizations/canterlot/clients/twilight", "twilight", false)

		keys := readSignedClientKeyList(t, router, client, "/organizations/canterlot/clients/twilight/keys")
		if len(keys) != 1 || keys[0]["uri"] != "/organizations/canterlot/clients/twilight/keys/default" {
			t.Fatalf("explicit-org client key list = %v, want default explicit-org key URI", keys)
		}
	})

	t.Run("configured default org", func(t *testing.T) {
		router := newTestRouterWithConfig(t, config.Config{
			ServiceName:         "opencook",
			Environment:         "test",
			DefaultOrganization: "canterlot",
		})
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
		client := createClientAsValidator(t, router, validator, "/clients", "spike")

		collection := readAdminJSON(t, router, "/clients")
		if collection["spike"] != "/clients/spike" {
			t.Fatalf("default-org collection spike URL = %v, want default-org URL", collection["spike"])
		}
		if collection["canterlot-validator"] != "/clients/canterlot-validator" {
			t.Fatalf("default-org collection validator URL = %v, want default-org URL", collection["canterlot-validator"])
		}
		assertClientReadPayload(t, router, "/clients/spike", "spike", false)

		keys := readSignedClientKeyList(t, router, client, "/clients/spike/keys")
		if len(keys) != 1 || keys[0]["uri"] != "/clients/spike/keys/default" {
			t.Fatalf("default-org client key list = %v, want default-org default key URI", keys)
		}
	})
}

func createClientAsAdmin(t *testing.T, router http.Handler, org, name string, validator bool) signedClientRequestor {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":       name,
		"validator":  validator,
		"create_key": true,
	})
	if err != nil {
		t.Fatalf("json.Marshal(client create) error = %v", err)
	}
	path := "/organizations/" + org + "/clients"
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("admin create client %s/%s status = %d, want %d, body = %s", org, name, rec.Code, http.StatusCreated, rec.Body.String())
	}

	return newSignedClientRequestorFromClientCreateResponse(t, org, name, rec)
}

func createClientAsValidator(t *testing.T, router http.Handler, validator signedClientRequestor, path, name string) signedClientRequestor {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":       name,
		"create_key": true,
	})
	if err != nil {
		t.Fatalf("json.Marshal(validator client create) error = %v", err)
	}
	req := validator.newSignedJSONRequest(t, http.MethodPost, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("validator create client %s status = %d, want %d, body = %s", name, rec.Code, http.StatusCreated, rec.Body.String())
	}

	return newSignedClientRequestorFromClientCreateResponse(t, validator.Organization, name, rec)
}

func assertACLPermission(t *testing.T, acl map[string]map[string]any, action string, actors, groups []string) {
	t.Helper()

	permission, ok := acl[action]
	if !ok {
		t.Fatalf("ACL missing %s permission: %v", action, acl)
	}
	assertStringSliceFromAnyEqual(t, permission["actors"], actors)
	assertStringSliceFromAnyEqual(t, permission["groups"], groups)
}

func readSignedClientKeyList(t *testing.T, router http.Handler, client signedClientRequestor, path string) []map[string]any {
	t.Helper()

	req := client.newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("signed client key list %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	return payload
}

func assertClientMissing(t *testing.T, router http.Handler, path string) {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("client read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusNotFound, rec.Body.String())
	}
}

func readAdminJSON(t *testing.T, router http.Handler, path string) map[string]any {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	return payload
}

func readAdminACLJSON(t *testing.T, router http.Handler, path string) map[string]map[string]any {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin ACL read %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	return payload
}
