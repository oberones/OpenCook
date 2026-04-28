package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newSignedJSONRequest(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()
	return newSignedJSONRequestAs(t, "silent-bob", method, path, body)
}

func newSignedJSONRequestAs(t *testing.T, userID, method, path string, body []byte) *http.Request {
	t.Helper()
	return newSignedJSONRequestAsWithServerAPIVersion(t, userID, method, path, body, "")
}

func newSignedJSONRequestWithServerAPIVersion(t *testing.T, method, path string, body []byte, serverAPIVersion string) *http.Request {
	t.Helper()
	return newSignedJSONRequestAsWithServerAPIVersion(t, "silent-bob", method, path, body, serverAPIVersion)
}

func newSignedJSONRequestAsWithServerAPIVersion(t *testing.T, userID, method, path string, body []byte, serverAPIVersion string) *http.Request {
	t.Helper()

	var reader *bytes.Reader
	bodyForSignature := body
	if bodyForSignature == nil {
		bodyForSignature = []byte{}
	}
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	for key, value := range manufactureSignedHeaders(t, mustParsePrivateKey(t), userID, method, path, bodyForSignature, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z", defaultServerAPIVersionForTest(serverAPIVersion)) {
		req.Header.Set(key, value)
	}
	if serverAPIVersion != "" {
		req.Header.Set(serverAPIVersionHeader, serverAPIVersion)
	}
	return req
}

func defaultServerAPIVersionForTest(serverAPIVersion string) string {
	if serverAPIVersion == "" {
		return "0"
	}
	return serverAPIVersion
}
