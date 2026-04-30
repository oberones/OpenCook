package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpointExposesSafePrometheusText(t *testing.T) {
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{})

	for _, target := range []string{
		"/_status",
		"/organizations/ponyville/search/node?q=name:supersecret&signature=topsecret",
		"/_blob/checksums/supersecretchecksum?signature=topsecret",
	} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if contentType := rec.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/plain; version=0.0.4") {
		t.Fatalf("/metrics Content-Type = %q, want Prometheus text", contentType)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"# HELP opencook_build_info",
		"opencook_http_requests_total",
		"opencook_http_request_duration_seconds_bucket",
		`surface="status"`,
		`surface="search"`,
		`surface="blob"`,
		"opencook_search_http_requests_total",
		"opencook_blob_http_requests_total",
		`opencook_dependency_configured{dependency="postgres",backend="postgres"}`,
		`opencook_dependency_ready{dependency="blob",backend="memory-compat"}`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("/metrics body missing %q:\n%s", want, body)
		}
	}
	for _, leaked := range []string{
		"ponyville",
		"supersecret",
		"topsecret",
		"/organizations/",
		"/_blob/checksums/",
		"q=name",
		"signature=",
	} {
		if strings.Contains(body, leaked) {
			t.Fatalf("/metrics leaked high-cardinality or secret-like value %q:\n%s", leaked, body)
		}
	}
}

func TestMetricsEndpointIsAdditiveForChefFacingRoutes(t *testing.T) {
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{})

	before := httptest.NewRecorder()
	router.ServeHTTP(before, httptest.NewRequest(http.MethodGet, "/server_api_version", nil))
	if before.Code != http.StatusOK {
		t.Fatalf("initial /server_api_version status = %d, want %d, body = %s", before.Code, http.StatusOK, before.Body.String())
	}

	metrics := httptest.NewRecorder()
	router.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metrics.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d, body = %s", metrics.Code, http.StatusOK, metrics.Body.String())
	}

	after := httptest.NewRecorder()
	router.ServeHTTP(after, httptest.NewRequest(http.MethodGet, "/server_api_version", nil))
	if after.Code != before.Code {
		t.Fatalf("/server_api_version status after /metrics = %d, want %d", after.Code, before.Code)
	}
	if after.Body.String() != before.Body.String() {
		t.Fatalf("/server_api_version body changed after /metrics:\nbefore: %s\nafter: %s", before.Body.String(), after.Body.String())
	}
}

func TestMetricsEndpointRejectsMutatingMethods(t *testing.T) {
	router := newStatusRouteTestRouter(t, statusRouteTestDeps{})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /metrics status = %d, want %d, body = %s", rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
	}
	if allow := rec.Header().Get("Allow"); allow != "GET, HEAD" {
		t.Fatalf("POST /metrics Allow = %q, want %q", allow, "GET, HEAD")
	}
}

func TestMetricSurfaceForPathKeepsOrgAdminRoutesSpecific(t *testing.T) {
	for _, tt := range []struct {
		path string
		want string
	}{
		{path: "/organizations/ponyville/groups", want: "groups"},
		{path: "/organizations/ponyville/groups/admins", want: "groups"},
		{path: "/organizations/ponyville/containers", want: "containers"},
		{path: "/organizations/ponyville/containers/clients/_acl", want: "containers"},
		{path: "/organizations/ponyville/_acl", want: "acls"},
	} {
		t.Run(tt.path, func(t *testing.T) {
			if got := metricSurfaceForPath(tt.path); got != tt.want {
				t.Fatalf("metricSurfaceForPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
