package api

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestIDIsPreservedInResponseAndStructuredLogs(t *testing.T) {
	var logs bytes.Buffer
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{
		logger: log.New(&logs, "", 0),
	})

	req := httptest.NewRequest(http.MethodGet, "/_status", nil)
	req.Header.Set(requestIDHeader, "operator-request-123")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("/_status status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := rec.Header().Get(requestIDHeader); got != "operator-request-123" {
		t.Fatalf("%s response header = %q, want preserved request ID", requestIDHeader, got)
	}

	entry := structuredLogEvent(t, logs.String(), "http_request")
	if entry["request_id"] != "operator-request-123" {
		t.Fatalf("request_id = %v, want preserved request ID; log = %s", entry["request_id"], logs.String())
	}
	if entry["surface"] != "status" || entry["status_class"] != "2xx" {
		t.Fatalf("structured request log = %v, want status surface and 2xx class", entry)
	}
}

func TestRequestLoggingGeneratesIDsAndRedactsSensitiveInputs(t *testing.T) {
	var logs bytes.Buffer
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{
		logger: log.New(&logs, "", 0),
	})

	body := strings.NewReader(`{"private_key":"PRIVATE KEY MATERIAL","password":"topsecret"}`)
	req := httptest.NewRequest(http.MethodPost, "/metrics?signature=topsecret&private_key=PRIVATE", body)
	req.Header.Set("X-Ops-Sign", "algorithm=sha256;version=1.3")
	req.Header.Set("Authorization", "Bearer topsecret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	requestID := rec.Header().Get(requestIDHeader)
	if !strings.HasPrefix(requestID, "oc-") {
		t.Fatalf("%s response header = %q, want generated OpenCook request ID", requestIDHeader, requestID)
	}

	entry := structuredLogEvent(t, logs.String(), "http_request")
	if entry["request_id"] != requestID {
		t.Fatalf("request_id = %v, want generated response request ID %q", entry["request_id"], requestID)
	}
	if entry["path_shape"] != "/metrics" {
		t.Fatalf("path_shape = %v, want /metrics without query values", entry["path_shape"])
	}

	logOutput := logs.String()
	for _, leaked := range []string{
		"PRIVATE KEY MATERIAL",
		"private_key",
		"password",
		"topsecret",
		"signature=",
		"algorithm=sha256;version=1.3",
		"Authorization",
		"X-Ops-Sign",
	} {
		if strings.Contains(logOutput, leaked) {
			t.Fatalf("structured logs leaked %q: %s", leaked, logOutput)
		}
	}
}

// structuredLogEvent extracts a JSON log event while tolerating logger prefixes
// that production deployments may configure outside the JSON payload.
func structuredLogEvent(t *testing.T, output, event string) map[string]any {
	t.Helper()

	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		start := strings.Index(line, "{")
		if start < 0 {
			continue
		}
		var entry map[string]any
		if err := json.Unmarshal([]byte(line[start:]), &entry); err != nil {
			continue
		}
		if entry["event"] == event {
			return entry
		}
	}
	t.Fatalf("log event %q not found in output: %s", event, output)
	return nil
}
