package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestValidatorBootstrapRegistrationInvalidPayloadsDoNotMutateState(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)

	for _, tc := range []struct {
		name             string
		clientName       string
		body             []byte
		wantStatus       int
		wantError        string
		verifyNoAuthnKey bool
	}{
		{
			name:       "invalid JSON",
			clientName: "badjson",
			body:       []byte(`{"name":"badjson"`),
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "trailing JSON",
			clientName: "trailing",
			body:       []byte(`{"name":"trailing","create_key":true}{"name":"ignored"}`),
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "empty payload",
			clientName: "empty-payload",
			body:       []byte{},
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_json",
		},
		{
			name:       "missing name",
			clientName: "missing-name",
			body:       []byte(`{"create_key":true}`),
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request",
		},
		{
			name:       "invalid name",
			clientName: "bad$name",
			body:       []byte(`{"name":"bad$name","create_key":true}`),
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request",
		},
		{
			name:       "invalid public key",
			clientName: "invalid-public-key",
			body:       []byte(`{"name":"invalid-public-key","public_key":"not a public key"}`),
			wantStatus: http.StatusBadRequest,
			wantError:  "invalid_request",
		},
		{
			name:             "public key with create key",
			clientName:       "mixed-key",
			body:             []byte(`{"name":"mixed-key","create_key":true,"public_key":` + strconvQuote(publicKeyPEM) + `}`),
			wantStatus:       http.StatusBadRequest,
			wantError:        "invalid_request",
			verifyNoAuthnKey: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := newTestRouter(t)
			validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

			req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", tc.body)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("validator invalid create status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			assertAPIError(t, rec, tc.wantError)
			assertClientRegistrationNoMutation(t, router, "canterlot", tc.clientName)
			if tc.verifyNoAuthnKey {
				candidate := newSignedClientRequestorFromPrivateKeyPEM(t, "canterlot", tc.clientName, testSigningKeyPEM)
				assertSignedClientUnauthenticated(t, router, candidate, "/organizations/canterlot/clients/"+tc.clientName)
			}
		})
	}
}

func TestValidatorBootstrapRegistrationDuplicateDoesNotReplaceExistingClientState(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
	client := createClientAsValidator(t, router, validator, "/organizations/canterlot/clients", "twilight")

	body := []byte(`{"name":"twilight","create_key":true}`)
	req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate validator create status = %d, want %d, body = %s", rec.Code, http.StatusConflict, rec.Body.String())
	}
	assertAPIError(t, rec, "conflict")

	assertSignedClientCanReadSelf(t, router, client, "/organizations/canterlot/clients/twilight")
	keys := readSignedClientKeyList(t, router, client, "/organizations/canterlot/clients/twilight/keys")
	if len(keys) != 1 || keys[0]["name"] != "default" {
		t.Fatalf("client key list after duplicate = %v, want only default key", keys)
	}
	assertClientsGroupMemberCount(t, router, "canterlot", "twilight", 1)
	assertClientSearchRow(t, router, "/organizations/canterlot/search/client?q=name:twilight", "/organizations/canterlot/search/client", "twilight", "canterlot")
}

func TestValidatorBootstrapRegistrationWrongOrgAndInactiveValidatorKeysDoNotMutateState(t *testing.T) {
	t.Run("wrong org validator key", func(t *testing.T) {
		router := newTestRouter(t)
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

		body := []byte(`{"name":"wrong-org-created","create_key":true}`)
		req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/clients", body)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("wrong-org validator create status = %d, want %d, body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
		}
		assertAPIError(t, rec, "requestor_not_found")
		assertClientRegistrationNoMutation(t, router, "ponyville", "wrong-org-created")
	})

	t.Run("deleted validator key", func(t *testing.T) {
		router := newTestRouter(t)
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
		deleteClientKeyAsAdmin(t, router, "canterlot", "canterlot-validator", "default")

		body := []byte(`{"name":"deleted-key-created","create_key":true}`)
		req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("deleted validator key create status = %d, want %d, body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
		}
		assertAPIError(t, rec, "requestor_not_found")
		assertClientRegistrationNoMutation(t, router, "canterlot", "deleted-key-created")
	})

	t.Run("expired validator key", func(t *testing.T) {
		router := newTestRouter(t)
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
		updateClientKeyAsAdmin(t, router, "canterlot", "canterlot-validator", "default", []byte(`{"expiration_date":"2012-01-01T00:00:00Z"}`), http.StatusOK)

		body := []byte(`{"name":"expired-key-created","create_key":true}`)
		req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expired validator key create status = %d, want %d, body = %s", rec.Code, http.StatusUnauthorized, rec.Body.String())
		}
		assertAPIError(t, rec, "requestor_not_found")
		assertClientRegistrationNoMutation(t, router, "canterlot", "expired-key-created")
	})
}

func TestValidatorBootstrapRegistrationDefaultOrgFailurePrecedence(t *testing.T) {
	t.Run("ambiguous default org rejects before body decode", func(t *testing.T) {
		router := newTestRouter(t)
		createOrganizationAndCaptureValidator(t, router, "canterlot")

		req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/clients", []byte(`{"name":"ambiguous-default"`))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("ambiguous default-org create status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertAPIError(t, rec, "organization_required")
		assertClientRegistrationNoMutation(t, router, "canterlot", "ambiguous-default")
		assertClientRegistrationNoMutation(t, router, "ponyville", "ambiguous-default")
	})

	t.Run("configured default org validates body through selected org", func(t *testing.T) {
		router := newTestRouterWithConfig(t, config.Config{
			ServiceName:         "opencook",
			Environment:         "test",
			DefaultOrganization: "canterlot",
		})
		validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

		req := validator.newSignedJSONRequest(t, http.MethodPost, "/clients", []byte(`{"name":"configured-default"`))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("configured default-org malformed create status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
		}
		assertAPIError(t, rec, "invalid_json")
		assertClientRegistrationNoMutation(t, router, "canterlot", "configured-default")
	})
}

func TestValidatorBootstrapRegistrationFailureDoesNotPersistPostgresStateOrVerifierKey(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	validator := fixture.createOrganizationWithValidator("canterlot")
	privateKey := mustParsePrivateKey(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)

	body := []byte(`{"name":"persisted-failure","create_key":true,"public_key":` + strconvQuote(publicKeyPEM) + `}`)
	req := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", body)
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("postgres invalid validator create status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	assertAPIError(t, rec, "invalid_request")
	assertClientRegistrationNoMutation(t, fixture.router, "canterlot", "persisted-failure")

	restarted := fixture.restart()
	assertClientRegistrationNoMutation(t, restarted.router, "canterlot", "persisted-failure")
	candidate := newSignedClientRequestorFromPrivateKeyPEM(t, "canterlot", "persisted-failure", testSigningKeyPEM)
	assertSignedClientUnauthenticated(t, restarted.router, candidate, "/organizations/canterlot/clients/persisted-failure")
}

func TestValidatorBootstrapRegistrationRenamedValidatorKeyRemainsUsableForPrincipalAuthentication(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
	updateClientKeyAsAdmin(t, router, "canterlot", "canterlot-validator", "default", []byte(`{"name":"renamed"}`), http.StatusCreated)

	createClientAsValidator(t, router, validator, "/organizations/canterlot/clients", "renamed-key-created")
	assertClientReadPayload(t, router, "/organizations/canterlot/clients/renamed-key-created", "renamed-key-created", false)
}

func assertAPIError(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(error response) error = %v, body = %s", err, rec.Body.String())
	}
	if payload["error"] != want {
		t.Fatalf("error = %v, want %q, payload = %v", payload["error"], want, payload)
	}
}

func assertClientRegistrationNoMutation(t *testing.T, router http.Handler, org, name string) {
	t.Helper()

	assertClientMissing(t, router, "/organizations/"+org+"/clients/"+name)
	assertClientsGroupExcludes(t, router, org, name)
	assertClientACLNotCreated(t, router, org, name)
	assertClientSearchRows(t, router, org, name, 0)
}

func assertClientsGroupExcludes(t *testing.T, router http.Handler, org, name string) {
	t.Helper()

	group := readAdminJSON(t, router, "/organizations/"+org+"/groups/clients")
	clients := stringSliceFromAny(t, group["clients"])
	if containsString(clients, name) {
		t.Fatalf("clients group clients = %v, want %q excluded", clients, name)
	}
	actors := stringSliceFromAny(t, group["actors"])
	if containsString(actors, name) {
		t.Fatalf("clients group actors = %v, want %q excluded", actors, name)
	}
}

func assertClientsGroupMemberCount(t *testing.T, router http.Handler, org, name string, want int) {
	t.Helper()

	group := readAdminJSON(t, router, "/organizations/"+org+"/groups/clients")
	clients := countString(stringSliceFromAny(t, group["clients"]), name)
	actors := countString(stringSliceFromAny(t, group["actors"]), name)
	if clients != want || actors != want {
		t.Fatalf("clients group member counts for %q = clients:%d actors:%d, want %d/%d", name, clients, actors, want, want)
	}
}

func assertClientACLNotCreated(t *testing.T, router http.Handler, org, name string) {
	t.Helper()

	path := "/organizations/" + org + "/clients/" + name + "/_acl"
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("client ACL %s status = %d, want missing/forbidden, body = %s", path, rec.Code, rec.Body.String())
	}
	if rec.Code != http.StatusForbidden && rec.Code != http.StatusNotFound {
		t.Fatalf("client ACL %s status = %d, want %d or %d, body = %s", path, rec.Code, http.StatusForbidden, http.StatusNotFound, rec.Body.String())
	}
}

func assertClientSearchRows(t *testing.T, router http.Handler, org, name string, want int) {
	t.Helper()

	requestPath := "/organizations/" + org + "/search/client?q=name:" + name
	signPath := "/organizations/" + org + "/search/client"
	req := httptest.NewRequest(http.MethodGet, requestPath, nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, signPath, nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("client search %s status = %d, want %d, body = %s", requestPath, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(client search) error = %v", err)
	}
	rows := payload["rows"].([]any)
	if len(rows) != want {
		t.Fatalf("client search rows for %q = %v, want %d row(s)", name, rows, want)
	}
}

func assertSignedClientUnauthenticated(t *testing.T, router http.Handler, client signedClientRequestor, path string) {
	t.Helper()

	req := client.newSignedJSONRequest(t, http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("signed client %s/%s status = %d, want %d, body = %s", client.Organization, client.ClientName, rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
}

func deleteClientKeyAsAdmin(t *testing.T, router http.Handler, org, client, key string) {
	t.Helper()

	path := "/organizations/" + org + "/clients/" + client + "/keys/" + key
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodDelete, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("delete client key %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}
}

func updateClientKeyAsAdmin(t *testing.T, router http.Handler, org, client, key string, body []byte, want int) {
	t.Helper()

	path := "/organizations/" + org + "/clients/" + client + "/keys/" + key
	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPut, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("update client key %s status = %d, want %d, body = %s", path, rec.Code, want, rec.Body.String())
	}
}

func countString(values []string, target string) int {
	count := 0
	for _, value := range values {
		if value == target {
			count++
		}
	}
	return count
}
