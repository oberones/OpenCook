package api

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

type signedClientRequestor struct {
	Organization  string
	ClientName    string
	PrivateKeyPEM string
	PrivateKey    *rsa.PrivateKey
}

func createOrganizationAndCaptureValidator(t *testing.T, router http.Handler, orgName string) signedClientRequestor {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"name":      orgName,
		"full_name": orgName,
		"org_type":  "Business",
	})
	if err != nil {
		t.Fatalf("json.Marshal(organization payload) error = %v", err)
	}

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/organizations", body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create organization status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(organization response) error = %v", err)
	}

	clientName, ok := payload["clientname"].(string)
	if !ok || strings.TrimSpace(clientName) == "" {
		t.Fatalf("organization response missing validator clientname: %v", payload)
	}
	privateKeyPEM, ok := payload["private_key"].(string)
	if !ok || strings.TrimSpace(privateKeyPEM) == "" {
		t.Fatalf("organization response missing validator private_key: %v", payload)
	}

	return newSignedClientRequestorFromPrivateKeyPEM(t, orgName, clientName, privateKeyPEM)
}

func newSignedClientRequestorFromClientCreateResponse(t *testing.T, orgName, clientName string, rec *httptest.ResponseRecorder) signedClientRequestor {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(client create response) error = %v", err)
	}
	return newSignedClientRequestorFromClientCreatePayload(t, orgName, clientName, payload)
}

func newSignedClientRequestorFromClientCreatePayload(t *testing.T, orgName, clientName string, payload map[string]any) signedClientRequestor {
	t.Helper()

	if chefKey, ok := payload["chef_key"].(map[string]any); ok {
		if privateKeyPEM, ok := chefKey["private_key"].(string); ok && strings.TrimSpace(privateKeyPEM) != "" {
			return newSignedClientRequestorFromPrivateKeyPEM(t, orgName, clientName, privateKeyPEM)
		}
	}
	if privateKeyPEM, ok := payload["private_key"].(string); ok && strings.TrimSpace(privateKeyPEM) != "" {
		return newSignedClientRequestorFromPrivateKeyPEM(t, orgName, clientName, privateKeyPEM)
	}

	t.Fatalf("client create payload does not contain usable private key material: %v", payload)
	return signedClientRequestor{}
}

func newSignedClientRequestorFromPrivateKeyPEM(t *testing.T, orgName, clientName, privateKeyPEM string) signedClientRequestor {
	t.Helper()

	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(privateKeyPEM))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM(%s/%s) error = %v", orgName, clientName, err)
	}
	return signedClientRequestor{
		Organization:  orgName,
		ClientName:    clientName,
		PrivateKeyPEM: privateKeyPEM,
		PrivateKey:    privateKey,
	}
}

func (r signedClientRequestor) newSignedJSONRequest(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()

	return r.newSignedJSONRequestWithServerAPIVersion(t, method, path, body, "0")
}

func (r signedClientRequestor) newSignedJSONRequestWithServerAPIVersion(t *testing.T, method, path string, body []byte, serverAPIVersion string) *http.Request {
	t.Helper()

	if r.PrivateKey == nil {
		t.Fatalf("signed client requestor %s/%s is missing private key", r.Organization, r.ClientName)
	}
	if body == nil {
		body = []byte{}
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range manufactureSignedHeaders(t, r.PrivateKey, r.ClientName, method, path, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z", serverAPIVersion) {
		req.Header.Set(key, value)
	}
	if serverAPIVersion != "" {
		req.Header.Set("X-Ops-Server-API-Version", serverAPIVersion)
	}
	if pathUsesExplicitOrganization(r.Organization, path) {
		req.SetPathValue("org", r.Organization)
	}
	return req
}

func pathUsesExplicitOrganization(orgName, path string) bool {
	if orgName == "" {
		return false
	}
	prefix := "/organizations/" + orgName
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}
