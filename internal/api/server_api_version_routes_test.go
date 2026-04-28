package api

import (
	"bytes"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerAPIVersionDiscoveryRoute(t *testing.T) {
	router := newTestRouter(t)

	for _, path := range []string{"/server_api_version", "/server_api_version/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", path, err)
		}
		if payload["min_api_version"] != float64(minServerAPIVersion) {
			t.Fatalf("%s min_api_version = %v, want %d", path, payload["min_api_version"], minServerAPIVersion)
		}
		if payload["max_api_version"] != float64(maxServerAPIVersion) {
			t.Fatalf("%s max_api_version = %v, want %d", path, payload["max_api_version"], maxServerAPIVersion)
		}
	}
}

func TestServerAPIVersionInvalidHeaderResponse(t *testing.T) {
	router := newTestRouter(t)

	cases := []struct {
		name      string
		method    string
		path      string
		requested string
		body      []byte
	}{
		{
			name:      "discovery_invalid_string",
			method:    http.MethodGet,
			path:      "/server_api_version",
			requested: "invalid",
		},
		{
			name:      "discovery_invalid_method_precedence",
			method:    http.MethodPost,
			path:      "/server_api_version",
			requested: "invalid",
		},
		{
			name:      "authenticated_invalid_string",
			method:    http.MethodGet,
			path:      "/nodes",
			requested: "invalid",
		},
		{
			name:      "authenticated_too_low",
			method:    http.MethodGet,
			path:      "/nodes",
			requested: "-1",
		},
		{
			name:      "authenticated_too_high",
			method:    http.MethodGet,
			path:      "/nodes",
			requested: "3",
		},
		{
			name:      "method_validation_precedence",
			method:    http.MethodPatch,
			path:      "/nodes",
			requested: "invalid",
		},
		{
			name:      "body_validation_precedence",
			method:    http.MethodPost,
			path:      "/users",
			requested: "invalid",
			body:      []byte(`{`),
		},
		{
			name:      "lookup_precedence",
			method:    http.MethodGet,
			path:      "/organizations/missing/nodes",
			requested: "invalid",
		},
		{
			name:      "authorization_precedence",
			method:    http.MethodGet,
			path:      "/nodes",
			requested: "invalid",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var req *http.Request
			if tc.path == "/server_api_version" {
				req = httptest.NewRequest(tc.method, tc.path, nil)
				req.Header.Set(serverAPIVersionHeader, tc.requested)
			} else if tc.name == "authorization_precedence" {
				req = newSignedJSONRequestAsWithServerAPIVersion(t, "outside-user", tc.method, tc.path, tc.body, tc.requested)
			} else {
				req = newSignedJSONRequestWithServerAPIVersion(t, tc.method, tc.path, tc.body, tc.requested)
			}

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assertInvalidServerAPIVersionResponse(t, rec, tc.requested)
		})
	}
}

func TestServerAPIVersionSupportedHeadersReachAuthenticatedRoutes(t *testing.T) {
	router := newTestRouter(t)

	for _, serverAPIVersion := range []string{"", "0", "1", "2"} {
		t.Run("version_"+defaultServerAPIVersionForTest(serverAPIVersion), func(t *testing.T) {
			req := newSignedJSONRequestWithServerAPIVersion(t, http.MethodGet, "/nodes", nil, serverAPIVersion)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("GET /nodes with API version %q status = %d, want %d, body = %s", serverAPIVersion, rec.Code, http.StatusOK, rec.Body.String())
			}
		})
	}
}

func TestServerAPIVersionParticipatesInV13SignatureVerification(t *testing.T) {
	router := newTestRouter(t)
	privateKey := mustParsePrivateKey(t)

	validReq := newSignedJSONRequestWithSignAndServerAPIVersion(t, privateKey, "silent-bob", http.MethodGet, "/nodes", nil, "1", "1")
	validRec := httptest.NewRecorder()
	router.ServeHTTP(validRec, validReq)
	if validRec.Code != http.StatusOK {
		t.Fatalf("matching v1.3 API version status = %d, want %d, body = %s", validRec.Code, http.StatusOK, validRec.Body.String())
	}

	mismatchedReq := newSignedJSONRequestWithSignAndServerAPIVersion(t, privateKey, "silent-bob", http.MethodGet, "/nodes", nil, "1", "2")
	mismatchedRec := httptest.NewRecorder()
	router.ServeHTTP(mismatchedRec, mismatchedReq)
	if mismatchedRec.Code != http.StatusUnauthorized {
		t.Fatalf("mismatched v1.3 API version status = %d, want %d, body = %s", mismatchedRec.Code, http.StatusUnauthorized, mismatchedRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(mismatchedRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(mismatched response) error = %v", err)
	}
	if payload["error"] != "bad_signature" {
		t.Fatalf("mismatched response error = %v, want bad_signature", payload["error"])
	}
}

func newSignedJSONRequestWithSignAndServerAPIVersion(t *testing.T, privateKey *rsa.PrivateKey, userID, method, path string, body []byte, signedServerAPIVersion, headerServerAPIVersion string) *http.Request {
	t.Helper()
	if body == nil {
		body = []byte{}
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range manufactureSignedHeaders(t, privateKey, userID, method, path, body, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z", signedServerAPIVersion) {
		req.Header.Set(key, value)
	}
	req.Header.Set(serverAPIVersionHeader, headerServerAPIVersion)
	return req
}

func assertInvalidServerAPIVersionResponse(t *testing.T, rec *httptest.ResponseRecorder, requested string) {
	t.Helper()

	if rec.Code != http.StatusNotAcceptable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotAcceptable, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(invalid version response) error = %v", err)
	}
	if payload["error"] != "invalid-x-ops-server-api-version" {
		t.Fatalf("error = %v, want invalid-x-ops-server-api-version", payload["error"])
	}
	wantMessage := "Specified version " + requested + " not supported"
	if payload["message"] != wantMessage {
		t.Fatalf("message = %v, want %q", payload["message"], wantMessage)
	}
	if payload["min_version"] != float64(minServerAPIVersion) {
		t.Fatalf("min_version = %v, want %d", payload["min_version"], minServerAPIVersion)
	}
	if payload["max_version"] != float64(maxServerAPIVersion) {
		t.Fatalf("max_version = %v, want %d", payload["max_version"], maxServerAPIVersion)
	}
}
