package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestAPIVersionOrganizationCreateValidatorResponseSemantics(t *testing.T) {
	router := newTestRouter(t)

	for _, serverAPIVersion := range []string{"0", "1", "2"} {
		t.Run("v"+serverAPIVersion, func(t *testing.T) {
			orgName := "apiv" + serverAPIVersion + "org"
			body := []byte(`{"name":"` + orgName + `","full_name":"` + orgName + `","org_type":"Business"}`)
			rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations", body, serverAPIVersion)
			if rec.Code != http.StatusCreated {
				t.Fatalf("organization create status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
			}

			payload := mustDecodeObject(t, rec)
			if payload["uri"] != "/organizations/"+orgName {
				t.Fatalf("uri = %v, want /organizations/%s", payload["uri"], orgName)
			}
			if payload["clientname"] != orgName+"-validator" {
				t.Fatalf("clientname = %v, want %s-validator", payload["clientname"], orgName)
			}
			privateKey, ok := payload["private_key"].(string)
			if !ok || !strings.Contains(privateKey, "BEGIN RSA PRIVATE KEY") {
				t.Fatalf("private_key missing generated RSA key: %v", payload)
			}
			if _, ok := payload["chef_key"]; ok {
				t.Fatalf("organization create response included client-create chef_key shape: %v", payload)
			}

			keyRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/"+orgName+"/clients/"+orgName+"-validator/keys/default", nil, serverAPIVersion)
			if keyRec.Code != http.StatusOK {
				t.Fatalf("validator default key status = %d, want %d, body = %s", keyRec.Code, http.StatusOK, keyRec.Body.String())
			}
			keyPayload := mustDecodeObject(t, keyRec)
			if keyPayload["name"] != "default" {
				t.Fatalf("validator key name = %v, want default", keyPayload["name"])
			}
			if keyPayload["expiration_date"] != "infinity" {
				t.Fatalf("validator key expiration_date = %v, want infinity", keyPayload["expiration_date"])
			}
			if !strings.Contains(keyPayload["public_key"].(string), "BEGIN PUBLIC KEY") {
				t.Fatalf("validator key public_key missing PEM: %v", keyPayload)
			}
		})
	}
}

func TestAPIVersionOrganizationCreateInvalidVersionPrecedesPayloadValidation(t *testing.T) {
	router := newTestRouter(t)

	rec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations", []byte(`{`), "3")
	assertInvalidServerAPIVersionResponse(t, rec, "3")

	getRec := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/invalid-version-org", nil, "1")
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("invalid-version organization lookup status = %d, want %d, body = %s", getRec.Code, http.StatusNotFound, getRec.Body.String())
	}
}

func TestActivePostgresOrganizationValidatorDefaultsAndEnvironmentRehydrate(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	orgName := "canterlot"
	body := []byte(`{"name":"` + orgName + `","full_name":"Canterlot","org_type":"Business"}`)

	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations", body, "1")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("active Postgres organization create status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}
	createPayload := mustDecodeObject(t, createRec)
	privateKeyPEM, ok := createPayload["private_key"].(string)
	if !ok || !strings.Contains(privateKeyPEM, "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("organization response missing validator private key: %v", createPayload)
	}
	validator := newSignedClientRequestorFromPrivateKeyPEM(t, orgName, orgName+"-validator", privateKeyPEM)

	restarted := fixture.restart()
	validatorReq := validator.newSignedJSONRequestWithServerAPIVersion(t, http.MethodGet, "/organizations/"+orgName+"/containers/data", nil, "1")
	validatorRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(validatorRec, validatorReq)
	if validatorRec.Code != http.StatusOK {
		t.Fatalf("rehydrated validator signed request status = %d, want %d, body = %s", validatorRec.Code, http.StatusOK, validatorRec.Body.String())
	}

	clientRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/"+orgName+"/clients/"+orgName+"-validator", nil, "1")
	if clientRec.Code != http.StatusOK {
		t.Fatalf("rehydrated validator client status = %d, want %d, body = %s", clientRec.Code, http.StatusOK, clientRec.Body.String())
	}
	clientPayload := mustDecodeObject(t, clientRec)
	if clientPayload["name"] != orgName+"-validator" || clientPayload["clientname"] != orgName+"-validator" {
		t.Fatalf("validator client name/clientname = %v/%v, want %s-validator", clientPayload["name"], clientPayload["clientname"], orgName)
	}
	if clientPayload["orgname"] != orgName || clientPayload["validator"] != true {
		t.Fatalf("validator client org/validator = %v/%v, want %s/true", clientPayload["orgname"], clientPayload["validator"], orgName)
	}
	if _, ok := clientPayload["public_key"]; ok {
		t.Fatalf("API v1 validator client read included top-level public_key: %v", clientPayload)
	}

	legacyClientRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/"+orgName+"/clients/"+orgName+"-validator", nil, "0")
	if legacyClientRec.Code != http.StatusOK {
		t.Fatalf("v0 validator client status = %d, want %d, body = %s", legacyClientRec.Code, http.StatusOK, legacyClientRec.Body.String())
	}
	if !strings.Contains(mustDecodeObject(t, legacyClientRec)["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("API v0 validator client read missing top-level public_key")
	}

	keyRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/"+orgName+"/clients/"+orgName+"-validator/keys/default", nil, "1")
	if keyRec.Code != http.StatusOK {
		t.Fatalf("rehydrated validator key status = %d, want %d, body = %s", keyRec.Code, http.StatusOK, keyRec.Body.String())
	}
	keyPayload := mustDecodeObject(t, keyRec)
	if keyPayload["name"] != "default" || keyPayload["expiration_date"] != "infinity" {
		t.Fatalf("validator key metadata = %v, want default/infinity", keyPayload)
	}
	if !strings.Contains(keyPayload["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("validator key public_key missing PEM: %v", keyPayload)
	}

	keyList := readKeyListPayload(t, restarted.router, "/organizations/"+orgName+"/clients/"+orgName+"-validator/keys")
	if len(keyList) != 1 || keyList[0]["name"] != "default" || keyList[0]["uri"] != "/organizations/"+orgName+"/clients/"+orgName+"-validator/keys/default" || keyList[0]["expired"] != false {
		t.Fatalf("validator key list = %v, want one unexpired default key", keyList)
	}

	containers := readAdminJSON(t, restarted.router, "/organizations/"+orgName+"/containers")
	assertJSONMapHasKeys(t, containers, "clients", "containers", "cookbooks", "data", "environments", "groups", "nodes", "roles", "sandboxes", "policies", "policy_groups", "cookbook_artifacts")
	groups := readAdminJSON(t, restarted.router, "/organizations/"+orgName+"/groups")
	assertJSONMapHasKeys(t, groups, "admins", "billing-admins", "users", "clients")

	orgACL := readAdminACLJSON(t, restarted.router, "/organizations/"+orgName+"/_acl")
	assertACLPermission(t, orgACL, "read", []string{"pivotal"}, []string{"admins", "users"})
	validatorACL := readAdminACLJSON(t, restarted.router, "/organizations/"+orgName+"/clients/"+orgName+"-validator/_acl")
	assertACLPermission(t, validatorACL, "read", []string{"pivotal"}, []string{"admins", "users"})
	assertACLPermission(t, validatorACL, "update", []string{"pivotal"}, []string{"admins"})

	envRec := serveSignedAPIVersionRequest(t, restarted.router, "pivotal", http.MethodGet, "/organizations/"+orgName+"/environments/_default", nil, "1")
	if envRec.Code != http.StatusOK {
		t.Fatalf("rehydrated _default environment status = %d, want %d, body = %s", envRec.Code, http.StatusOK, envRec.Body.String())
	}
	envPayload := mustDecodeObject(t, envRec)
	if envPayload["name"] != "_default" || envPayload["json_class"] != "Chef::Environment" || envPayload["chef_type"] != "environment" {
		t.Fatalf("_default environment identity = %v, want Chef::Environment/environment", envPayload)
	}
}

func readKeyListPayload(t *testing.T, router http.Handler, path string) []map[string]any {
	t.Helper()

	req := newSignedJSONRequestAsWithServerAPIVersion(t, "pivotal", http.MethodGet, path, nil, "1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("key list %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
	}
	return payload
}

func assertJSONMapHasKeys(t *testing.T, payload map[string]any, keys ...string) {
	t.Helper()

	for _, key := range keys {
		if _, ok := payload[key]; !ok {
			t.Fatalf("payload missing key %q: %v", key, payload)
		}
	}
}
