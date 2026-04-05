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

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}

	req := httptest.NewRequest(method, path, reader)
	applySignedHeaders(t, req, userID, "", method, path, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	return req
}
