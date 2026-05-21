package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/config"
)

type coreObjectCollectionRouteCase struct {
	name         string
	defaultPath  string
	explicitPath string
	method       string
	body         []byte
	wantStatus   int
}

func TestCoreObjectCollectionRoutesShareCompatibilitySemantics(t *testing.T) {
	for _, route := range coreObjectCollectionRouteCases() {
		t.Run(route.name, func(t *testing.T) {
			t.Run("default org alias", func(t *testing.T) {
				router := newTestRouter(t)
				assertCoreObjectRouteStatus(t, router, route.method, route.defaultPath, route.body, route.wantStatus)
			})

			t.Run("explicit org alias", func(t *testing.T) {
				router := newTestRouter(t)
				assertCoreObjectRouteStatus(t, router, route.method, route.explicitPath, route.body, route.wantStatus)
			})

			t.Run("trailing slash default alias", func(t *testing.T) {
				router := newTestRouter(t)
				assertCoreObjectRouteStatus(t, router, route.method, route.defaultPath+"/", route.body, route.wantStatus)
			})

			t.Run("trailing slash explicit alias", func(t *testing.T) {
				router := newTestRouter(t)
				assertCoreObjectRouteStatus(t, router, route.method, route.explicitPath+"/", route.body, route.wantStatus)
			})

			t.Run("missing organization precedence", func(t *testing.T) {
				router := newTestRouter(t)
				missingOrgPath := strings.Replace(route.explicitPath, "/organizations/ponyville/", "/organizations/missing/", 1)
				rec := assertCoreObjectRouteStatus(t, router, route.method, missingOrgPath, route.body, http.StatusNotFound)
				assertCoreObjectAPIError(t, rec, "not_found", "organization not found")
			})

			t.Run("ambiguous default org", func(t *testing.T) {
				router := newTestRouter(t)
				createOrgForTest(t, router, "canterlot")

				rec := assertCoreObjectRouteStatus(t, router, route.method, route.defaultPath, route.body, http.StatusBadRequest)
				assertCoreObjectAPIError(t, rec, "organization_required", "organization context is required for this route")
			})

			t.Run("configured default org", func(t *testing.T) {
				router := newTestRouterWithConfig(t, coreObjectRouteSemanticsConfig("canterlot"))
				createOrgForTest(t, router, "canterlot")

				assertCoreObjectRouteStatus(t, router, route.method, route.defaultPath, route.body, route.wantStatus)
			})
		})
	}
}

func TestCoreObjectRoutesReturnNotFoundForExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	paths := []string{
		"/nodes/example/extra",
		"/organizations/ponyville/nodes/example/extra",
		"/environments/_default/extra",
		"/organizations/ponyville/environments/_default/extra",
		"/roles/example/extra",
		"/organizations/ponyville/roles/example/extra",
		"/data/example/item/extra",
		"/organizations/ponyville/data/example/item/extra",
		"/policies/example/revisions/1111111111111111111111111111111111111111/extra",
		"/organizations/ponyville/policies/example/revisions/1111111111111111111111111111111111111111/extra",
		"/policy_groups/dev/policies/example/extra",
		"/organizations/ponyville/policy_groups/dev/policies/example/extra",
		"/sandboxes/example/extra",
		"/organizations/ponyville/sandboxes/example/extra",
		"/cookbooks/example/1.0.0/extra",
		"/organizations/ponyville/cookbooks/example/1.0.0/extra",
		"/cookbook_artifacts/example/1111111111111111111111111111111111111111/extra",
		"/organizations/ponyville/cookbook_artifacts/example/1111111111111111111111111111111111111111/extra",
		"/universe/extra",
		"/organizations/ponyville/universe/extra",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			assertCoreObjectRouteStatus(t, router, http.MethodGet, path, nil, http.StatusNotFound)
		})
	}
}

func TestCoreObjectRoutesReturnMethodNotAllowedAllowHeaders(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name      string
		path      string
		method    string
		wantAllow string
	}{
		{name: "nodes collection", path: "/nodes", method: http.MethodPatch, wantAllow: "GET, HEAD, POST"},
		{name: "nodes named", path: "/nodes/example", method: http.MethodPatch, wantAllow: "GET, HEAD, PUT, DELETE"},
		{name: "environments collection", path: "/environments", method: http.MethodPatch, wantAllow: "GET, HEAD, POST"},
		{name: "environments named", path: "/environments/_default", method: http.MethodPatch, wantAllow: "GET, HEAD, PUT, DELETE"},
		{name: "environment nodes", path: "/environments/_default/nodes", method: http.MethodPost, wantAllow: "GET, HEAD"},
		{name: "environment cookbooks", path: "/environments/_default/cookbooks", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "environment recipes", path: "/environments/_default/recipes", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "environment depsolver", path: "/environments/_default/cookbook_versions", method: http.MethodGet, wantAllow: http.MethodPost},
		{name: "roles collection", path: "/roles", method: http.MethodPatch, wantAllow: "GET, HEAD, POST"},
		{name: "roles named", path: "/roles/example", method: http.MethodPatch, wantAllow: "GET, HEAD, PUT, DELETE"},
		{name: "role environments collection", path: "/roles/example/environments", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "role environment named", path: "/roles/example/environments/_default", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "data bags collection", path: "/data", method: http.MethodPatch, wantAllow: "GET, POST"},
		{name: "data bag named", path: "/data/example", method: http.MethodPatch, wantAllow: "GET, POST, DELETE"},
		{name: "data bag item", path: "/data/example/item", method: http.MethodPatch, wantAllow: "GET, PUT, DELETE"},
		{name: "policies collection", path: "/policies", method: http.MethodPatch, wantAllow: "GET, HEAD"},
		{name: "policy named", path: "/policies/example", method: http.MethodPatch, wantAllow: "GET, DELETE, HEAD"},
		{name: "policy revisions", path: "/policies/example/revisions", method: http.MethodGet, wantAllow: http.MethodPost},
		{name: "policy revision named", path: "/policies/example/revisions/1111111111111111111111111111111111111111", method: http.MethodPatch, wantAllow: "GET, HEAD, DELETE"},
		{name: "policy groups collection", path: "/policy_groups", method: http.MethodPatch, wantAllow: "GET, HEAD"},
		{name: "policy group named", path: "/policy_groups/dev", method: http.MethodPatch, wantAllow: "GET, HEAD, DELETE"},
		{name: "policy group assignment", path: "/policy_groups/dev/policies/example", method: http.MethodPatch, wantAllow: "GET, HEAD, PUT, DELETE"},
		{name: "sandboxes collection", path: "/sandboxes", method: http.MethodGet, wantAllow: http.MethodPost},
		{name: "sandbox named", path: "/sandboxes/example", method: http.MethodGet, wantAllow: http.MethodPut},
		{name: "cookbooks collection", path: "/cookbooks", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "cookbook named", path: "/cookbooks/example", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "cookbook version", path: "/cookbooks/example/1.0.0", method: http.MethodPost, wantAllow: "GET, PUT, DELETE"},
		{name: "cookbook artifacts collection", path: "/cookbook_artifacts", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "cookbook artifact named", path: "/cookbook_artifacts/example", method: http.MethodPost, wantAllow: http.MethodGet},
		{name: "cookbook artifact version", path: "/cookbook_artifacts/example/1111111111111111111111111111111111111111", method: http.MethodPost, wantAllow: "GET, PUT, DELETE"},
		{name: "universe", path: "/universe", method: http.MethodPost, wantAllow: http.MethodGet},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			for _, path := range []string{route.path, explicitCoreObjectRoutePath(route.path)} {
				t.Run(path, func(t *testing.T) {
					rec := assertCoreObjectRouteStatus(t, router, route.method, path, nil, http.StatusMethodNotAllowed)
					if rec.Header().Get("Allow") != route.wantAllow {
						t.Fatalf("%s Allow = %q, want %q", path, rec.Header().Get("Allow"), route.wantAllow)
					}
					assertCoreObjectAPIError(t, rec, "method_not_allowed", "")
				})
			}
		})
	}
}

func coreObjectCollectionRouteCases() []coreObjectCollectionRouteCase {
	return []coreObjectCollectionRouteCase{
		{name: "nodes", defaultPath: "/nodes", explicitPath: "/organizations/ponyville/nodes", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "environments", defaultPath: "/environments", explicitPath: "/organizations/ponyville/environments", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "roles", defaultPath: "/roles", explicitPath: "/organizations/ponyville/roles", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "data bags", defaultPath: "/data", explicitPath: "/organizations/ponyville/data", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "policies", defaultPath: "/policies", explicitPath: "/organizations/ponyville/policies", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "policy groups", defaultPath: "/policy_groups", explicitPath: "/organizations/ponyville/policy_groups", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "sandboxes", defaultPath: "/sandboxes", explicitPath: "/organizations/ponyville/sandboxes", method: http.MethodPost, body: coreObjectSandboxBody(), wantStatus: http.StatusCreated},
		{name: "cookbooks", defaultPath: "/cookbooks", explicitPath: "/organizations/ponyville/cookbooks", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "cookbook artifacts", defaultPath: "/cookbook_artifacts", explicitPath: "/organizations/ponyville/cookbook_artifacts", method: http.MethodGet, wantStatus: http.StatusOK},
		{name: "universe", defaultPath: "/universe", explicitPath: "/organizations/ponyville/universe", method: http.MethodGet, wantStatus: http.StatusOK},
	}
}

func coreObjectSandboxBody() []byte {
	return []byte(`{"checksums":{"d41d8cd98f00b204e9800998ecf8427e":null}}`)
}

func explicitCoreObjectRoutePath(defaultPath string) string {
	return "/organizations/ponyville" + defaultPath
}

func coreObjectRouteSemanticsConfig(defaultOrg string) config.Config {
	return config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		AuthSkew:            15 * time.Minute,
		DefaultOrganization: defaultOrg,
		MaxAuthBodyBytes:    config.DefaultMaxAuthBodyBytes,
	}
}

func assertCoreObjectRouteStatus(t *testing.T, router http.Handler, method, path string, body []byte, wantStatus int) *httptest.ResponseRecorder {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", method, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	return rec
}

func assertCoreObjectAPIError(t *testing.T, rec *httptest.ResponseRecorder, wantError, wantMessage string) {
	t.Helper()

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != wantError {
		t.Fatalf("error = %v, want %q; body = %s", payload["error"], wantError, rec.Body.String())
	}
	if wantMessage != "" && payload["message"] != wantMessage {
		t.Fatalf("message = %v, want %q; body = %s", payload["message"], wantMessage, rec.Body.String())
	}
}
