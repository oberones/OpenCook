package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
)

func TestEnvironmentCookbookVersionsRejectInvalidJSON(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", []byte("this_is_not_json"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver invalid JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationEnvironmentCookbookVersionsRejectInvalidJSON(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", []byte("this_is_not_json"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver invalid JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestDefaultEnvironmentCookbookVersionsRejectInvalidJSON(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", []byte("this_is_not_json"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver invalid JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectInvalidJSON(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", []byte("this_is_not_json"))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver invalid JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestEnvironmentCookbookVersionsRejectInvalidJSONBeforeEnvironmentReadAuthz(t *testing.T) {
	routes := []struct {
		name    string
		path    string
		envName string
	}{
		{name: "named_environment", path: "/environments/production/cookbook_versions", envName: "production"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/production/cookbook_versions", envName: "production"},
		{name: "default_environment", path: "/environments/_default/cookbook_versions", envName: "_default"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions", envName: "_default"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			var authorizer *recordingDepsolverAuthorizer
			router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
				authorizer = &recordingDepsolverAuthorizer{
					base: authz.NewACLAuthorizer(state),
					deny: map[string]struct{}{
						"read:environment:" + route.envName: {},
					},
				}
				return authorizer
			})
			if route.envName != "_default" {
				createEnvironmentForCookbookTests(t, router, route.envName)
			}
			authorizer.calls = nil

			req := newSignedJSONRequest(t, http.MethodPost, route.path, []byte("this_is_not_json"))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s invalid JSON precedence status = %d, want %d, body = %s", route.name, rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
			if len(authorizer.calls) != 0 {
				t.Fatalf("%s authz calls = %v, want none", route.name, authorizer.calls)
			}
		})
	}
}

func TestEnvironmentCookbookVersionsRejectInvalidJSONBeforeMissingEnvironment(t *testing.T) {
	routes := []struct {
		name string
		path string
	}{
		{name: "named_environment", path: "/environments/missing-env/cookbook_versions"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/missing-env/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, []byte("this_is_not_json"))
			rec := httptest.NewRecorder()
			newTestRouter(t).ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s invalid JSON before missing environment status = %d, want %d, body = %s", route.name, rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
		})
	}
}

func TestEnvironmentCookbookVersionsRejectEmptyPayload(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", []byte(""))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver empty payload status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationEnvironmentCookbookVersionsRejectEmptyPayload(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", []byte(""))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver empty payload status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestDefaultEnvironmentCookbookVersionsRejectEmptyPayload(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", []byte(""))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver empty payload status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectEmptyPayload(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", []byte(""))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver empty payload status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestEnvironmentCookbookVersionsRejectTrailingJSONDocument(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", []byte(`{"run_list":[]}{"run_list":[]}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver trailing JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationEnvironmentCookbookVersionsRejectTrailingJSONDocument(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", []byte(`{"run_list":[]}{"run_list":[]}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver trailing JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestDefaultEnvironmentCookbookVersionsRejectTrailingJSONDocument(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", []byte(`{"run_list":[]}{"run_list":[]}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver trailing JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectTrailingJSONDocument(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", []byte(`{"run_list":[]}{"run_list":[]}`))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver trailing JSON status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "invalid JSON")
}

func TestEnvironmentCookbookVersionsAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions/", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver trailing slash status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestOrganizationEnvironmentCookbookVersionsAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions/", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver trailing slash status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestDefaultEnvironmentCookbookVersionsAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions/", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default depsolver trailing slash status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestOrganizationDefaultEnvironmentCookbookVersionsAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions/", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped default depsolver trailing slash status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestEnvironmentCookbookVersionsRejectInvalidRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": "demo",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver invalid run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationEnvironmentCookbookVersionsRejectInvalidRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": "demo",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver invalid run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestDefaultEnvironmentCookbookVersionsRejectInvalidRunList(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": "demo",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver invalid run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectInvalidRunList(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": "demo",
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver invalid run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestEnvironmentCookbookVersionsRejectMalformedVersionedRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo@not_a_version"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver malformed versioned run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationEnvironmentCookbookVersionsRejectMalformedVersionedRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo@not_a_version"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver malformed versioned run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestDefaultEnvironmentCookbookVersionsRejectMalformedVersionedRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo@not_a_version"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver malformed versioned run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectMalformedVersionedRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo@not_a_version"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver malformed versioned run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestEnvironmentCookbookVersionsRejectBogusBracketedRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"fake[not_good]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver bogus bracketed run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationEnvironmentCookbookVersionsRejectBogusBracketedRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"fake[not_good]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver bogus bracketed run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestDefaultEnvironmentCookbookVersionsRejectBogusBracketedRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"fake[not_good]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver bogus bracketed run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectBogusBracketedRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"fake[not_good]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver bogus bracketed run_list status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestEnvironmentCookbookVersionsRejectChefInvalidRunListItemShapes(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	routes := []struct {
		name string
		path string
	}{
		{name: "named_environment", path: "/environments/production/cookbook_versions"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/production/cookbook_versions"},
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}
	items := []struct {
		name string
		item string
	}{
		{name: "one_part_unqualified_version", item: "gibberish@1"},
		{name: "one_part_qualified_recipe_version", item: "recipe[gibberish@1]"},
		{name: "single_colon_separator", item: "foo:bar"},
		{name: "triple_colon_separator", item: "foo:::bar"},
		{name: "qualified_role_with_colon", item: "role[foo:bar]"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			for _, item := range items {
				t.Run(item.name, func(t *testing.T) {
					req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
						"run_list": []any{item.item},
					}))
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					if rec.Code != http.StatusBadRequest {
						t.Fatalf("%s invalid run_list item status = %d, want %d, body = %s", route.name, rec.Code, http.StatusBadRequest, rec.Body.String())
					}

					assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
				})
			}
		})
	}
}

func TestEnvironmentCookbookVersionsRejectNonStringRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{12},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver non-string run_list item status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationEnvironmentCookbookVersionsRejectNonStringRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{12},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver non-string run_list item status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestDefaultEnvironmentCookbookVersionsRejectNonStringRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{12},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("default depsolver non-string run_list item status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRejectNonStringRunListItem(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{12},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped default depsolver non-string run_list item status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' is not a valid run list")
}

func TestEnvironmentCookbookVersionsReturnsEmptyObjectForEmptyRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver empty run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestOrganizationEnvironmentCookbookVersionsReturnsEmptyObjectForEmptyRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver empty run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestDefaultEnvironmentCookbookVersionsReturnsEmptyObjectForEmptyRunList(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver empty run_list status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 0 {
				t.Fatalf("payload = %v, want empty object", payload)
			}
		})
	}
}

func TestEnvironmentCookbookVersionsReturnsEmptyObjectForMissingRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver missing run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestOrganizationEnvironmentCookbookVersionsReturnsEmptyObjectForMissingRunList(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver missing run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestEnvironmentCookbookVersionsAcceptsChefCompatibleRunListItemShapes(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	routes := []struct {
		name string
		path string
	}{
		{name: "named_environment", path: "/environments/production/cookbook_versions"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/production/cookbook_versions"},
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}
	payload := map[string]any{
		"run_list": []any{
			"recipe",
			"recipe::foo",
			"recipe::bar@1.0.0",
			"role",
			"role::foo",
			"role::bar@1.0.0",
			"1",
			"recipe[1]",
			"recipe[recipe]",
			"recipe[role]",
			"recipe[recipe@1.0]",
			"recipe[role@1.0.1]",
		},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, payload))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s compatible run_list item shapes status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			assertDepsolverErrorDetail(t, decodeJSONMap(t, rec.Body.Bytes()), map[string]any{
				"message":                    "Run list contains invalid items: no such cookbooks 1, recipe, role.",
				"non_existent_cookbooks":     []string{"1", "recipe", "role"},
				"cookbooks_with_no_versions": []string{},
			})
		})
	}
}

func TestDefaultEnvironmentCookbookVersionsReturnsEmptyObjectForMissingRunList(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default depsolver missing run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestOrganizationDefaultEnvironmentCookbookVersionsReturnsEmptyObjectForMissingRunList(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped default depsolver missing run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("payload = %v, want empty object", payload)
	}
}

func TestEnvironmentCookbookVersionsReturnsNotFoundForMissingEnvironment(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/not@environment/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("depsolver missing environment status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Cannot load environment not@environment")
}

func TestOrganizationEnvironmentCookbookVersionsReturnsNotFoundForMissingEnvironment(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/not@environment/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("org-scoped depsolver missing environment status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Cannot load environment not@environment")
}

func TestOrganizationDefaultEnvironmentCookbookVersionsReturnsNotFoundForMissingOrganization(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/missing-org/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("org-scoped default depsolver missing organization status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "not_found" {
		t.Fatalf("error = %v, want %q", payload["error"], "not_found")
	}
	if payload["message"] != "organization not found" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization not found")
	}
}

func TestEnvironmentCookbookVersionsRequiresOrganizationWhenAmbiguous(t *testing.T) {
	router := newTestRouter(t)
	createOrgForTest(t, router, "canterlot")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous depsolver status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "organization_required" {
		t.Fatalf("error = %v, want %q", payload["error"], "organization_required")
	}
	if payload["message"] != "organization context is required for this route" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization context is required for this route")
	}
}

func TestDefaultEnvironmentCookbookVersionsRequiresOrganizationWhenAmbiguous(t *testing.T) {
	router := newTestRouter(t)
	createOrgForTest(t, router, "canterlot")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("ambiguous default depsolver status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "organization_required" {
		t.Fatalf("error = %v, want %q", payload["error"], "organization_required")
	}
	if payload["message"] != "organization context is required for this route" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization context is required for this route")
	}
}

func TestEnvironmentCookbookVersionsUsesConfiguredDefaultOrganizationWhenAmbiguous(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		AuthSkew:            15 * time.Minute,
		DefaultOrganization: "canterlot",
	})
	createOrgForTest(t, router, "canterlot")

	envBody := mustMarshalEnvironmentPayload(t, "production")
	envReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(envBody))
	applySignedHeaders(t, envReq, "pivotal", "", http.MethodPost, "/environments", envBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	envRec := httptest.NewRecorder()
	router.ServeHTTP(envRec, envReq)
	if envRec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", envRec.Code, http.StatusCreated, envRec.Body.String())
	}

	cookbookBody := mustMarshalSandboxJSON(t, cookbookVersionPayload("foo", "1.2.3", "", nil))
	cookbookReq := httptest.NewRequest(http.MethodPut, "/cookbooks/foo/1.2.3", bytes.NewReader(cookbookBody))
	applySignedHeaders(t, cookbookReq, "pivotal", "", http.MethodPut, "/cookbooks/foo/1.2.3", cookbookBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	cookbookRec := httptest.NewRecorder()
	router.ServeHTTP(cookbookRec, cookbookReq)
	if cookbookRec.Code != http.StatusCreated {
		t.Fatalf("create cookbook status = %d, want %d, body = %s", cookbookRec.Code, http.StatusCreated, cookbookRec.Body.String())
	}

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default-org depsolver status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 1 {
		t.Fatalf("len(payload) = %d, want %d (%v)", len(payload), 1, payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.2.3")
}

func TestDefaultEnvironmentCookbookVersionsUsesConfiguredDefaultOrganizationWhenAmbiguous(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		AuthSkew:            15 * time.Minute,
		DefaultOrganization: "canterlot",
	})
	createOrgForTest(t, router, "canterlot")

	cookbookBody := mustMarshalSandboxJSON(t, cookbookVersionPayload("foo", "1.2.3", "", nil))
	cookbookReq := httptest.NewRequest(http.MethodPut, "/cookbooks/foo/1.2.3", bytes.NewReader(cookbookBody))
	applySignedHeaders(t, cookbookReq, "pivotal", "", http.MethodPut, "/cookbooks/foo/1.2.3", cookbookBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	cookbookRec := httptest.NewRecorder()
	router.ServeHTTP(cookbookRec, cookbookReq)
	if cookbookRec.Code != http.StatusCreated {
		t.Fatalf("create cookbook status = %d, want %d, body = %s", cookbookRec.Code, http.StatusCreated, cookbookRec.Body.String())
	}

	req := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default-org default depsolver status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 1 {
		t.Fatalf("len(payload) = %d, want %d (%v)", len(payload), 1, payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.2.3")
}

func TestOrganizationEnvironmentCookbookVersionsReturnsNotFoundForMissingOrganization(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/missing-org/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("org-scoped depsolver missing organization status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "not_found" {
		t.Fatalf("error = %v, want %q", payload["error"], "not_found")
	}
	if payload["message"] != "organization not found" {
		t.Fatalf("message = %v, want %q", payload["message"], "organization not found")
	}
}

func TestEnvironmentCookbookVersionsReturnsMethodNotAllowedWithAllowHeader(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodGet, "/environments/production/cookbook_versions", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("depsolver method status = %d, want %d, body = %s", rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", rec.Header().Get("Allow"), http.MethodPost)
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "method_not_allowed" {
		t.Fatalf("error = %v, want %q", payload["error"], "method_not_allowed")
	}
	if payload["message"] != "method not allowed for environment cookbook_versions route" {
		t.Fatalf("message = %v, want %q", payload["message"], "method not allowed for environment cookbook_versions route")
	}
}

func TestOrganizationEnvironmentCookbookVersionsReturnsMethodNotAllowedWithAllowHeader(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/environments/production/cookbook_versions", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("org-scoped depsolver method status = %d, want %d, body = %s", rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
	}
	if rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", rec.Header().Get("Allow"), http.MethodPost)
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "method_not_allowed" {
		t.Fatalf("error = %v, want %q", payload["error"], "method_not_allowed")
	}
	if payload["message"] != "method not allowed for environment cookbook_versions route" {
		t.Fatalf("message = %v, want %q", payload["message"], "method not allowed for environment cookbook_versions route")
	}
}

func TestDefaultEnvironmentCookbookVersionsReturnsMethodNotAllowedWithAllowHeader(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodGet, route.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("%s depsolver method status = %d, want %d, body = %s", route.name, rec.Code, http.StatusMethodNotAllowed, rec.Body.String())
			}
			if rec.Header().Get("Allow") != http.MethodPost {
				t.Fatalf("Allow = %q, want %q", rec.Header().Get("Allow"), http.MethodPost)
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "method_not_allowed" {
				t.Fatalf("error = %v, want %q", payload["error"], "method_not_allowed")
			}
			if payload["message"] != "method not allowed for environment cookbook_versions route" {
				t.Fatalf("message = %v, want %q", payload["message"], "method not allowed for environment cookbook_versions route")
			}
		})
	}
}

func TestEnvironmentCookbookVersionsRejectsExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions/extra", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("depsolver extra path status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "not_found" {
		t.Fatalf("error = %v, want %q", payload["error"], "not_found")
	}
	if payload["message"] != "route not found in scaffold router" {
		t.Fatalf("message = %v, want %q", payload["message"], "route not found in scaffold router")
	}
}

func TestDefaultEnvironmentCookbookVersionsRejectsExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions/extra"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions/extra"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s depsolver extra path status = %d, want %d, body = %s", route.name, rec.Code, http.StatusNotFound, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "not_found" {
				t.Fatalf("error = %v, want %q", payload["error"], "not_found")
			}
			if payload["message"] != "route not found in scaffold router" {
				t.Fatalf("message = %v, want %q", payload["message"], "route not found in scaffold router")
			}
		})
	}
}

func TestOrganizationEnvironmentCookbookVersionsRejectsExtraPathSegments(t *testing.T) {
	router := newTestRouter(t)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions/extra", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("org-scoped depsolver extra path status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if payload["error"] != "not_found" {
		t.Fatalf("error = %v, want %q", payload["error"], "not_found")
	}
	if payload["message"] != "route not found in scaffold router" {
		t.Fatalf("message = %v, want %q", payload["message"], "route not found in scaffold router")
	}
}

func TestEnvironmentCookbookVersionsReturns412ForMissingAndFilteredCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"foo": "= 400.0.0",
	})

	missingReq := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"this_does_not_exist"},
	}))
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver missing cookbook status = %d, want %d, body = %s", missingRec.Code, http.StatusPreconditionFailed, missingRec.Body.String())
	}

	missingPayload := decodeJSONMap(t, missingRec.Body.Bytes())
	assertDepsolverErrorDetail(t, missingPayload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
		"non_existent_cookbooks":     []string{"this_does_not_exist"},
		"cookbooks_with_no_versions": []string{},
	})

	filteredReq := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	filteredRec := httptest.NewRecorder()
	router.ServeHTTP(filteredRec, filteredReq)
	if filteredRec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver filtered cookbook status = %d, want %d, body = %s", filteredRec.Code, http.StatusPreconditionFailed, filteredRec.Body.String())
	}

	filteredPayload := decodeJSONMap(t, filteredRec.Body.Bytes())
	assertDepsolverErrorDetail(t, filteredPayload, map[string]any{
		"message":                    "Run list contains invalid items: no versions match the constraints on cookbook (foo >= 0.0.0).",
		"non_existent_cookbooks":     []string{},
		"cookbooks_with_no_versions": []string{"(foo >= 0.0.0)"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForMissingAndFilteredCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"foo": "= 400.0.0",
	})

	missingReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"this_does_not_exist"},
	}))
	missingRec := httptest.NewRecorder()
	router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver missing cookbook status = %d, want %d, body = %s", missingRec.Code, http.StatusPreconditionFailed, missingRec.Body.String())
	}

	missingPayload := decodeJSONMap(t, missingRec.Body.Bytes())
	assertDepsolverErrorDetail(t, missingPayload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
		"non_existent_cookbooks":     []string{"this_does_not_exist"},
		"cookbooks_with_no_versions": []string{},
	})

	filteredReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	filteredRec := httptest.NewRecorder()
	router.ServeHTTP(filteredRec, filteredReq)
	if filteredRec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver filtered cookbook status = %d, want %d, body = %s", filteredRec.Code, http.StatusPreconditionFailed, filteredRec.Body.String())
	}

	filteredPayload := decodeJSONMap(t, filteredRec.Body.Bytes())
	assertDepsolverErrorDetail(t, filteredPayload, map[string]any{
		"message":                    "Run list contains invalid items: no versions match the constraints on cookbook (foo >= 0.0.0).",
		"non_existent_cookbooks":     []string{},
		"cookbooks_with_no_versions": []string{"(foo >= 0.0.0)"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForMissingAndNoVersionCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			missingReq := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"this_does_not_exist"},
			}))
			missingRec := httptest.NewRecorder()
			router.ServeHTTP(missingRec, missingReq)
			if missingRec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver missing cookbook status = %d, want %d, body = %s", route.name, missingRec.Code, http.StatusPreconditionFailed, missingRec.Body.String())
			}

			missingPayload := decodeJSONMap(t, missingRec.Body.Bytes())
			assertDepsolverErrorDetail(t, missingPayload, map[string]any{
				"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
				"non_existent_cookbooks":     []string{"this_does_not_exist"},
				"cookbooks_with_no_versions": []string{},
			})

			noVersionReq := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo@400.0.0"},
			}))
			noVersionRec := httptest.NewRecorder()
			router.ServeHTTP(noVersionRec, noVersionReq)
			if noVersionRec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver no-version cookbook status = %d, want %d, body = %s", route.name, noVersionRec.Code, http.StatusPreconditionFailed, noVersionRec.Body.String())
			}

			noVersionPayload := decodeJSONMap(t, noVersionRec.Body.Bytes())
			assertDepsolverErrorDetail(t, noVersionPayload, map[string]any{
				"message":                    "Run list contains invalid items: no versions match the constraints on cookbook (foo = 400.0.0).",
				"non_existent_cookbooks":     []string{},
				"cookbooks_with_no_versions": []string{"(foo = 400.0.0)"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsPrefersMissingRootsOverNoVersionRoots(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"this_does_not_exist", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver mixed missing/no-version status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
		"non_existent_cookbooks":     []string{"this_does_not_exist"},
		"cookbooks_with_no_versions": []string{},
	})
}

func TestOrganizationEnvironmentCookbookVersionsPrefersMissingRootsOverNoVersionRoots(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"this_does_not_exist", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver mixed missing/no-version status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
		"non_existent_cookbooks":     []string{"this_does_not_exist"},
		"cookbooks_with_no_versions": []string{},
	})
}

func TestDefaultEnvironmentCookbookVersionsPrefersMissingRootsOverNoVersionRoots(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"this_does_not_exist", "foo@2.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver mixed missing/no-version status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertDepsolverErrorDetail(t, payload, map[string]any{
				"message":                    "Run list contains invalid items: no such cookbook this_does_not_exist.",
				"non_existent_cookbooks":     []string{"this_does_not_exist"},
				"cookbooks_with_no_versions": []string{},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsReportsPluralMissingRootCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"z_missing", "a_missing"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver plural missing roots status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbooks a_missing, z_missing.",
		"non_existent_cookbooks":     []string{"a_missing", "z_missing"},
		"cookbooks_with_no_versions": []string{},
	})
}

func TestDefaultEnvironmentCookbookVersionsReportsPluralMissingRootCookbooks(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"z_missing", "a_missing"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver plural missing roots status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertDepsolverErrorDetail(t, payload, map[string]any{
				"message":                    "Run list contains invalid items: no such cookbooks a_missing, z_missing.",
				"non_existent_cookbooks":     []string{"a_missing", "z_missing"},
				"cookbooks_with_no_versions": []string{},
			})
		})
	}
}

func TestOrganizationEnvironmentCookbookVersionsReportsPluralMissingRootCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"z_missing", "a_missing"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver plural missing roots status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no such cookbooks a_missing, z_missing.",
		"non_existent_cookbooks":     []string{"a_missing", "z_missing"},
		"cookbooks_with_no_versions": []string{},
	})
}

func TestEnvironmentCookbookVersionsReportsPluralNoVersionRootCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"bar@9.0.0", "foo@4.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver plural no-version roots status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no versions match the constraints on cookbooks (bar = 9.0.0), (foo = 4.0.0).",
		"non_existent_cookbooks":     []string{},
		"cookbooks_with_no_versions": []string{"(bar = 9.0.0)", "(foo = 4.0.0)"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReportsPluralNoVersionRootCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"bar@9.0.0", "foo@4.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver plural no-version roots status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertDepsolverErrorDetail(t, payload, map[string]any{
				"message":                    "Run list contains invalid items: no versions match the constraints on cookbooks (bar = 9.0.0), (foo = 4.0.0).",
				"non_existent_cookbooks":     []string{},
				"cookbooks_with_no_versions": []string{"(bar = 9.0.0)", "(foo = 4.0.0)"},
			})
		})
	}
}

func TestOrganizationEnvironmentCookbookVersionsReportsPluralNoVersionRootCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "1.2.3", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"bar@9.0.0", "foo@4.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver plural no-version roots status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertDepsolverErrorDetail(t, payload, map[string]any{
		"message":                    "Run list contains invalid items: no versions match the constraints on cookbooks (bar = 9.0.0), (foo = 4.0.0).",
		"non_existent_cookbooks":     []string{},
		"cookbooks_with_no_versions": []string{"(bar = 9.0.0)", "(foo = 4.0.0)"},
	})
}

func TestEnvironmentCookbookVersionsRequiresEnvironmentReadAuthz(t *testing.T) {
	routes := []struct {
		name    string
		path    string
		envName string
	}{
		{name: "named_environment", path: "/environments/production/cookbook_versions", envName: "production"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/production/cookbook_versions", envName: "production"},
		{name: "default_environment", path: "/environments/_default/cookbook_versions", envName: "_default"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions", envName: "_default"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			var authorizer *recordingDepsolverAuthorizer
			router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
				authorizer = &recordingDepsolverAuthorizer{
					base: authz.NewACLAuthorizer(state),
					deny: map[string]struct{}{
						"read:environment:" + route.envName: {},
					},
				}
				return authorizer
			})
			if route.envName != "_default" {
				createEnvironmentForCookbookTests(t, router, route.envName)
			}
			authorizer.calls = nil

			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s depsolver environment authz status = %d, want %d, body = %s", route.name, rec.Code, http.StatusForbidden, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "forbidden" {
				t.Fatalf("%s error = %v, want %q", route.name, payload["error"], "forbidden")
			}

			wantCalls := []string{"read:environment:" + route.envName}
			if len(authorizer.calls) != len(wantCalls) {
				t.Fatalf("%s depsolver authz calls = %v, want %v", route.name, authorizer.calls, wantCalls)
			}
			for idx := range wantCalls {
				if authorizer.calls[idx] != wantCalls[idx] {
					t.Fatalf("%s depsolver authz calls[%d] = %q, want %q (%v)", route.name, idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
				}
			}
		})
	}
}

func TestEnvironmentCookbookVersionsChecksEnvironmentReadBeforeRoleExpandedAuthz(t *testing.T) {
	routes := []struct {
		name    string
		path    string
		envName string
	}{
		{name: "named_environment", path: "/environments/production/cookbook_versions", envName: "production"},
		{name: "org_scoped_named_environment", path: "/organizations/ponyville/environments/production/cookbook_versions", envName: "production"},
		{name: "default_environment", path: "/environments/_default/cookbook_versions", envName: "_default"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions", envName: "_default"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			var authorizer *recordingDepsolverAuthorizer
			router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
				authorizer = &recordingDepsolverAuthorizer{
					base: authz.NewACLAuthorizer(state),
					deny: map[string]struct{}{
						"read:environment:" + route.envName: {},
					},
				}
				return authorizer
			})
			if route.envName != "_default" {
				createEnvironmentForCookbookTests(t, router, route.envName)
			}
			if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
				Payload: map[string]any{
					"name":                "web",
					"description":         "",
					"json_class":          "Chef::Role",
					"chef_type":           "role",
					"default_attributes":  map[string]any{},
					"override_attributes": map[string]any{},
					"run_list":            []any{"recipe[apache2]"},
					"env_run_lists":       map[string]any{},
				},
			}); err != nil {
				t.Fatalf("CreateRole(web) error = %v", err)
			}
			authorizer.calls = nil

			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"role[web]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Fatalf("%s depsolver role-expanded environment authz status = %d, want %d, body = %s", route.name, rec.Code, http.StatusForbidden, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if payload["error"] != "forbidden" {
				t.Fatalf("%s error = %v, want %q", route.name, payload["error"], "forbidden")
			}

			wantCalls := []string{"read:environment:" + route.envName}
			if len(authorizer.calls) != len(wantCalls) {
				t.Fatalf("%s depsolver authz calls = %v, want %v", route.name, authorizer.calls, wantCalls)
			}
			for idx := range wantCalls {
				if authorizer.calls[idx] != wantCalls[idx] {
					t.Fatalf("%s depsolver authz calls[%d] = %q, want %q (%v)", route.name, idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
				}
			}
		})
	}
}

func TestEnvironmentCookbookVersionsResolvesPinnedAndDependentCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "= 1.0.0"})
	createCookbookVersion(t, router, "quux", "4.0.0", "", map[string]string{"bar": "= 2.0.0"})

	req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"quux", "foo@1.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver success status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
	assertCookbookVersionBody(t, payload, "quux", "4.0.0")

	barDeps := payload["bar"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
	if value := barDeps["foo"]; value != "= 1.0.0" {
		t.Fatalf("bar dependency foo = %v, want %q", value, "= 1.0.0")
	}
	quuxDeps := payload["quux"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
	if value := quuxDeps["bar"]; value != "= 2.0.0" {
		t.Fatalf("quux dependency bar = %v, want %q", value, "= 2.0.0")
	}

	fooMetadata := payload["foo"].(map[string]any)["metadata"].(map[string]any)
	if _, ok := fooMetadata["attributes"]; ok {
		t.Fatalf("depsolver metadata.attributes present, want omitted (%v)", fooMetadata)
	}
	if _, ok := fooMetadata["long_description"]; ok {
		t.Fatalf("depsolver metadata.long_description present, want omitted (%v)", fooMetadata)
	}
}

func TestOrganizationEnvironmentCookbookVersionsResolvesPinnedAndDependentCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "= 1.0.0"})
	createCookbookVersion(t, router, "quux", "4.0.0", "", map[string]string{"bar": "= 2.0.0"})

	req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"quux", "foo@1.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver success status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
	assertCookbookVersionBody(t, payload, "quux", "4.0.0")

	barDeps := payload["bar"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
	if value := barDeps["foo"]; value != "= 1.0.0" {
		t.Fatalf("bar dependency foo = %v, want %q", value, "= 1.0.0")
	}
	quuxDeps := payload["quux"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
	if value := quuxDeps["bar"]; value != "= 2.0.0" {
		t.Fatalf("quux dependency bar = %v, want %q", value, "= 2.0.0")
	}

	fooMetadata := payload["foo"].(map[string]any)["metadata"].(map[string]any)
	if _, ok := fooMetadata["attributes"]; ok {
		t.Fatalf("depsolver metadata.attributes present, want omitted (%v)", fooMetadata)
	}
	if _, ok := fooMetadata["long_description"]; ok {
		t.Fatalf("depsolver metadata.long_description present, want omitted (%v)", fooMetadata)
	}
}

func TestDefaultEnvironmentCookbookVersionsResolvesPinnedAndDependentCookbooks(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "= 1.0.0"})
	createCookbookVersion(t, router, "quux", "4.0.0", "", map[string]string{"bar": "= 2.0.0"})

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"quux", "foo@1.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver success status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertCookbookVersionBody(t, payload, "foo", "1.0.0")
			assertCookbookVersionBody(t, payload, "bar", "2.0.0")
			assertCookbookVersionBody(t, payload, "quux", "4.0.0")

			barDeps := payload["bar"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
			if value := barDeps["foo"]; value != "= 1.0.0" {
				t.Fatalf("bar dependency foo = %v, want %q", value, "= 1.0.0")
			}
			quuxDeps := payload["quux"].(map[string]any)["metadata"].(map[string]any)["dependencies"].(map[string]any)
			if value := quuxDeps["bar"]; value != "= 2.0.0" {
				t.Fatalf("quux dependency bar = %v, want %q", value, "= 2.0.0")
			}

			fooMetadata := payload["foo"].(map[string]any)["metadata"].(map[string]any)
			if _, ok := fooMetadata["attributes"]; ok {
				t.Fatalf("depsolver metadata.attributes present, want omitted (%v)", fooMetadata)
			}
			if _, ok := fooMetadata["long_description"]; ok {
				t.Fatalf("depsolver metadata.long_description present, want omitted (%v)", fooMetadata)
			}
		})
	}
}

func TestDefaultEnvironmentCookbookVersionsPreservesDependencyMetadataForNormalUser(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "> 0.0.0"})
	createCookbookVersion(t, router, "baz", "3.0.0", "", nil)
	createCookbookVersion(t, router, "quux", "4.0.0", "", map[string]string{
		"bar": "= 2.0.0",
		"baz": "= 3.0.0",
	})

	req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"quux"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("default depsolver dependency metadata status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 4 {
		t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
	assertCookbookVersionBody(t, payload, "baz", "3.0.0")
	assertCookbookVersionBody(t, payload, "quux", "4.0.0")

	assertDepsolverResponseDependencies(t, payload["foo"], map[string]string{})
	assertDepsolverResponseDependencies(t, payload["bar"], map[string]string{"foo": "> 0.0.0"})
	assertDepsolverResponseDependencies(t, payload["baz"], map[string]string{})
	assertDepsolverResponseDependencies(t, payload["quux"], map[string]string{
		"bar": "= 2.0.0",
		"baz": "= 3.0.0",
	})
}

func TestOrganizationDefaultEnvironmentCookbookVersionsPreservesDependencyMetadataForNormalUser(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "> 0.0.0"})
	createCookbookVersion(t, router, "baz", "3.0.0", "", nil)
	createCookbookVersion(t, router, "quux", "4.0.0", "", map[string]string{
		"bar": "= 2.0.0",
		"baz": "= 3.0.0",
	})

	req := newSignedJSONRequestAs(t, "normal-user", http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"quux"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped default depsolver dependency metadata status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 4 {
		t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
	assertCookbookVersionBody(t, payload, "baz", "3.0.0")
	assertCookbookVersionBody(t, payload, "quux", "4.0.0")

	assertDepsolverResponseDependencies(t, payload["foo"], map[string]string{})
	assertDepsolverResponseDependencies(t, payload["bar"], map[string]string{"foo": "> 0.0.0"})
	assertDepsolverResponseDependencies(t, payload["baz"], map[string]string{})
	assertDepsolverResponseDependencies(t, payload["quux"], map[string]string{
		"bar": "= 2.0.0",
		"baz": "= 3.0.0",
	})
}

func TestEnvironmentCookbookVersionsSupportsQualifiedAndVersionedRecipeRunListItems(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo::default", "recipe[bar::install]", "recipe[foo::server@1.0.0]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver qualified run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsSupportsQualifiedAndVersionedRecipeRunListItems(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo::default", "recipe[bar::install]", "recipe[foo::server@1.0.0]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver qualified run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestDefaultEnvironmentCookbookVersionsSupportsQualifiedAndVersionedRecipeRunListItems(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo::default", "recipe[bar::install]", "recipe[foo::server@1.0.0]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver qualified run_list status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertCookbookVersionBody(t, payload, "foo", "1.0.0")
			assertCookbookVersionBody(t, payload, "bar", "1.0.0")
		})
	}
}

func TestEnvironmentCookbookVersionsDeduplicatesEquivalentRunListForms(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "recipe[foo]", "foo::default", "recipe[foo::default]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver equivalent run_list forms status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsDeduplicatesEquivalentRunListForms(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "recipe[foo]", "foo::default", "recipe[foo::default]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver equivalent run_list forms status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestDefaultEnvironmentCookbookVersionsDeduplicatesEquivalentRunListForms(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo", "recipe[foo]", "foo::default", "recipe[foo::default]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver equivalent run_list forms status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 2 {
				t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "foo", "1.0.0")
			assertCookbookVersionBody(t, payload, "bar", "1.0.0")
		})
	}
}

func TestEnvironmentCookbookVersionsSelectsPinnedVersionWhenEquivalentFormsArePresent(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "recipe[foo::default@2.0.0]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver pinned equivalent run_list form status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "2.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsSelectsPinnedVersionWhenEquivalentFormsArePresent(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "recipe[foo::default@2.0.0]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver pinned equivalent run_list form status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "2.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
}

func TestDefaultEnvironmentCookbookVersionsSelectsPinnedVersionWhenEquivalentFormsArePresent(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo", "recipe[foo::default@2.0.0]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver pinned equivalent run_list form status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 2 {
				t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "foo", "2.0.0")
			assertCookbookVersionBody(t, payload, "bar", "2.0.0")
		})
	}
}

func TestEnvironmentCookbookVersionsReturns412ForMissingDependency(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"this_does_not_exist": ">= 0.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver missing dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	errorList, ok := payload["error"].([]any)
	if !ok || len(errorList) != 1 {
		t.Fatalf("error payload = %v, want one depsolver error object", payload["error"])
	}
	detail, ok := errorList[0].(map[string]any)
	if !ok {
		t.Fatalf("error detail type = %T, want map[string]any", errorList[0])
	}
	if detail["unsatisfiable_run_list_item"] != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", detail["unsatisfiable_run_list_item"], "(foo >= 0.0.0)")
	}
	assertStringSliceValue(t, detail["non_existent_cookbooks"], []string{"this_does_not_exist"})
	assertStringSliceValue(t, detail["most_constrained_cookbooks"], []string{})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForMissingDependency(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"this_does_not_exist": ">= 0.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver missing dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	errorList, ok := payload["error"].([]any)
	if !ok || len(errorList) != 1 {
		t.Fatalf("error payload = %v, want one depsolver error object", payload["error"])
	}
	detail, ok := errorList[0].(map[string]any)
	if !ok {
		t.Fatalf("error detail type = %T, want map[string]any", errorList[0])
	}
	if detail["unsatisfiable_run_list_item"] != "(foo >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", detail["unsatisfiable_run_list_item"], "(foo >= 0.0.0)")
	}
	assertStringSliceValue(t, detail["non_existent_cookbooks"], []string{"this_does_not_exist"})
	assertStringSliceValue(t, detail["most_constrained_cookbooks"], []string{})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForMissingDependency(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"this_does_not_exist": ">= 0.0.0"})

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver missing dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package this_does_not_exist, which does not exist, due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on this_does_not_exist: [(foo = 1.2.3) -> (this_does_not_exist >= 0.0.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{"this_does_not_exist"},
				"most_constrained_cookbooks":  []string{},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsAttributesMissingDependencyToLaterRoot(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "1.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"oops": ">= 0.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app2"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver later-root missing dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package oops, which does not exist, due to solution constraint (app2 >= 0.0.0). Solution constraints that may result in a constraint on oops: [(app2 = 0.0.1) -> (oops >= 0.0.0)]",
		"unsatisfiable_run_list_item": "(app2 >= 0.0.0)",
		"non_existent_cookbooks":      []string{"oops"},
		"most_constrained_cookbooks":  []string{},
	})
}

func TestOrganizationEnvironmentCookbookVersionsAttributesMissingDependencyToLaterRoot(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "1.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"oops": ">= 0.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app2"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver later-root missing dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package oops, which does not exist, due to solution constraint (app2 >= 0.0.0). Solution constraints that may result in a constraint on oops: [(app2 = 0.0.1) -> (oops >= 0.0.0)]",
		"unsatisfiable_run_list_item": "(app2 >= 0.0.0)",
		"non_existent_cookbooks":      []string{"oops"},
		"most_constrained_cookbooks":  []string{},
	})
}

func TestDefaultEnvironmentCookbookVersionsAttributesMissingDependencyToLaterRoot(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "1.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"oops": ">= 0.0.0"})

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1", "app2"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver later-root missing dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package oops, which does not exist, due to solution constraint (app2 >= 0.0.0). Solution constraints that may result in a constraint on oops: [(app2 = 0.0.1) -> (oops >= 0.0.0)]",
				"unsatisfiable_run_list_item": "(app2 >= 0.0.0)",
				"non_existent_cookbooks":      []string{"oops"},
				"most_constrained_cookbooks":  []string{},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsReturns412ForUnsatisfiedDependencyVersion(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver unsatisfied dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForUnsatisfiedDependencyVersion(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver unsatisfied dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForUnsatisfiedDependencyVersion(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver unsatisfied dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsReturns412ForImpossibleDependency(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "> 3.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver impossible dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> [(foo > 3.0.0)]"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForImpossibleDependency(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "> 3.0.0"})

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver impossible dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> [(foo > 3.0.0)]"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForImpossibleDependency(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", map[string]string{"foo": "> 3.0.0"})

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver impossible dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> [(foo > 3.0.0)]"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsReturns412ForTransitiveConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver transitive conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForTransitiveConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver transitive conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForTransitiveConflict(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver transitive conflict status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsMatchesUpstreamComplexDependencyGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "buzz", "2.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "ack", "1.0.0", "", map[string]string{
		"foobar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "buzz"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver complex dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 2.0.0) -> (baz = 1.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamComplexDependencyGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "buzz", "2.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "ack", "1.0.0", "", map[string]string{
		"foobar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "buzz"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver complex dependency status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 2.0.0) -> (baz = 1.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsMatchesUpstreamComplexDependencyGraph(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "buzz", "2.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "ack", "1.0.0", "", map[string]string{
		"foobar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo", "buzz"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver complex dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 1.0.0) -> (baz > 1.2.0), (buzz = 2.0.0) -> (baz = 1.0.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsIgnoresUnrelatedEnvironmentConstraintsForConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"something": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver unrelated environment constraint conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsIgnoresUnrelatedEnvironmentConstraintsForConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"something": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{
		"bar":  "= 1.0.0",
		"buzz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "bar", "1.0.0", "", map[string]string{
		"baz": "= 1.0.0",
	})
	createCookbookVersion(t, router, "buzz", "1.0.0", "", map[string]string{
		"baz": "> 1.2.0",
	})
	createCookbookVersion(t, router, "baz", "1.0.0", "", nil)
	createCookbookVersion(t, router, "baz", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver unrelated environment constraint conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package baz due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on baz: [(foo = 1.2.3) -> (bar = 1.0.0) -> (baz = 1.0.0), (foo = 1.2.3) -> (buzz = 1.0.0) -> (baz > 1.2.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"baz = 1.0.0 -> []"},
	})
}

func TestEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinor(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver pessimistic constraint status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app2", "2.2.0")
}

func TestOrganizationEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinor(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver pessimistic constraint status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app2", "2.2.0")
}

func TestDefaultEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinor(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@3.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver pessimistic constraint status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertCookbookVersionBody(t, payload, "app2", "2.2.0")
		})
	}
}

func TestEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinorPatch(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver pessimistic major/minor/patch constraint status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app2", "2.1.5")
}

func TestOrganizationEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinorPatch(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver pessimistic major/minor/patch constraint status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app2", "2.1.5")
}

func TestDefaultEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraintMajorMinorPatch(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": "~> 2.1.1",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "2.1.5", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "2.2.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@3.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver pessimistic major/minor/patch constraint status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertCookbookVersionBody(t, payload, "app2", "2.1.5")
		})
	}
}

func TestEnvironmentCookbookVersionsReturns412ForMultiRootConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": ">= 0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app3"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver multi-root conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package app5 due to solution constraint (app3 >= 0.0.0). Solution constraints that may result in a constraint on app5: [(app1 = 3.0.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app4 >= 0.3.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app2 >= 0.0.0) -> (app4 >= 5.0.0) -> (app5 = 2.0.0), (app3 = 0.1.0) -> (app5 = 6.0.0)]",
		"unsatisfiable_run_list_item": "(app3 >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"app5 = 2.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForMultiRootConflict(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": ">= 0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app3"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver multi-root conflict status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package app5 due to solution constraint (app3 >= 0.0.0). Solution constraints that may result in a constraint on app5: [(app1 = 3.0.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app4 >= 0.3.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app2 >= 0.0.0) -> (app4 >= 5.0.0) -> (app5 = 2.0.0), (app3 = 0.1.0) -> (app5 = 6.0.0)]",
		"unsatisfiable_run_list_item": "(app3 >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"app5 = 2.0.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsReturns412ForMultiRootConflict(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": ">= 0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1", "app3"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver multi-root conflict status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package app5 due to solution constraint (app3 >= 0.0.0). Solution constraints that may result in a constraint on app5: [(app1 = 3.0.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app4 >= 0.3.0) -> (app5 = 2.0.0), (app1 = 3.0.0) -> (app2 >= 0.0.0) -> (app4 >= 5.0.0) -> (app5 = 2.0.0), (app3 = 0.1.0) -> (app5 = 6.0.0)]",
				"unsatisfiable_run_list_item": "(app3 >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"app5 = 2.0.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsMatchesUpstreamFirstGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.33",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.33", "", map[string]string{
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver first graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 3 {
		t.Fatalf("len(payload) = %d, want 3 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.2.33")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamFirstGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.33",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.33", "", map[string]string{
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver first graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 3 {
		t.Fatalf("len(payload) = %d, want 3 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.2.33")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
}

func TestDefaultEnvironmentCookbookVersionsMatchesUpstreamFirstGraph(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.33",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.33", "", map[string]string{
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@0.1.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver first graph status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 3 {
				t.Fatalf("len(payload) = %d, want 3 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "app1", "0.1.0")
			assertCookbookVersionBody(t, payload, "app2", "0.2.33")
			assertCookbookVersionBody(t, payload, "app3", "0.3.0")
		})
	}
}

func TestEnvironmentCookbookVersionsMatchesUpstreamPinnedRootNoSolutionGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app1", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app1", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": "0.1.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver pinned-root no-solution status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package app3 due to solution constraint (app1 = 0.1.0). Solution constraints that may result in a constraint on app3: [(app1 = 0.1.0) -> (app3 >= 0.2.0), (app1 = 0.1.0) -> (app2 0.2.0) -> (app3 0.1.0)]",
		"unsatisfiable_run_list_item": "(app1 = 0.1.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"app3 = 0.3.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamPinnedRootNoSolutionGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app1", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app1", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": "0.1.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver pinned-root no-solution status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package app3 due to solution constraint (app1 = 0.1.0). Solution constraints that may result in a constraint on app3: [(app1 = 0.1.0) -> (app3 >= 0.2.0), (app1 = 0.1.0) -> (app2 0.2.0) -> (app3 0.1.0)]",
		"unsatisfiable_run_list_item": "(app1 = 0.1.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"app3 = 0.3.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsMatchesUpstreamPinnedRootNoSolutionGraph(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app1", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app1", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": "0.1.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.2.0", "", nil)
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@0.1.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver pinned-root no-solution status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package app3 due to solution constraint (app1 = 0.1.0). Solution constraints that may result in a constraint on app3: [(app1 = 0.1.0) -> (app3 >= 0.2.0), (app1 = 0.1.0) -> (app2 0.2.0) -> (app3 0.1.0)]",
				"unsatisfiable_run_list_item": "(app1 = 0.1.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"app3 = 0.3.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsMatchesUpstreamSecondGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app4": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app4": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.2.0", "", map[string]string{
		"app4": "0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.2.0", "", map[string]string{
		"app2": ">= 0.2.0",
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app4", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver second graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.3.0")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
	assertCookbookVersionBody(t, payload, "app4", "0.2.0")
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamSecondGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app4": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app4": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.2.0", "", map[string]string{
		"app4": "0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.2.0", "", map[string]string{
		"app2": ">= 0.2.0",
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app4", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver second graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 4 {
		t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.3.0")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
	assertCookbookVersionBody(t, payload, "app4", "0.2.0")
}

func TestDefaultEnvironmentCookbookVersionsMatchesUpstreamSecondGraph(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app4": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app4": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.2.0", "", map[string]string{
		"app4": "0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.2.0", "", map[string]string{
		"app2": ">= 0.2.0",
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app4", "0.3.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver second graph status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 4 {
				t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "app1", "0.1.0")
			assertCookbookVersionBody(t, payload, "app2", "0.3.0")
			assertCookbookVersionBody(t, payload, "app3", "0.3.0")
			assertCookbookVersionBody(t, payload, "app4", "0.2.0")
		})
	}
}

func TestEnvironmentCookbookVersionsIgnoresUnrelatedEnvironmentConstraintsForSuccessfulSelection(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"something": "= 1.0.0",
	})
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app4": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app4": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.2.0", "", map[string]string{
		"app4": "0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.2.0", "", map[string]string{
		"app2": ">= 0.2.0",
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app4", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver unrelated environment constraint success status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 4 {
		t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.3.0")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
	assertCookbookVersionBody(t, payload, "app4", "0.2.0")
}

func TestOrganizationEnvironmentCookbookVersionsIgnoresUnrelatedEnvironmentConstraintsForSuccessfulSelection(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"something": "= 1.0.0",
	})
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app4": "0.2.0",
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.2.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app2", "0.3.0", "", map[string]string{
		"app3": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app4": ">= 0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.2.0", "", map[string]string{
		"app4": "0.2.0",
	})
	createCookbookVersion(t, router, "app3", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app4", "0.2.0", "", map[string]string{
		"app2": ">= 0.2.0",
		"app3": "0.3.0",
	})
	createCookbookVersion(t, router, "app4", "0.3.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0", "app2@0.3.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver unrelated environment constraint success status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 4 {
		t.Fatalf("len(payload) = %d, want 4 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.3.0")
	assertCookbookVersionBody(t, payload, "app3", "0.3.0")
	assertCookbookVersionBody(t, payload, "app4", "0.2.0")
}

func TestEnvironmentCookbookVersionsMatchesUpstreamThirdGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app1", "0.2.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "1.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "3.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "4.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "0.1.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "0.3.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app5", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver third graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "6.0.0")
	assertCookbookVersionBody(t, payload, "app5", "6.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamThirdGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app1", "0.2.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "1.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "3.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "4.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "0.1.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "0.3.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": ">= 0.0.0"})
	createCookbookVersion(t, router, "app5", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app5", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver third graph status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "6.0.0")
	assertCookbookVersionBody(t, payload, "app5", "6.0.0")
}

func TestEnvironmentCookbookVersionsMatchesUpstreamConflictingPassingGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app5": "= 2.0.0",
		"app4": "<= 5.0.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "1.0.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "3.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "4.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "0.1.0", "", map[string]string{"app5": "= 0.1.0"})
	createCookbookVersion(t, router, "app4", "0.3.0", "", map[string]string{"app5": "= 0.3.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app5", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app5", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app2", "app5"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver conflicting passing status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "5.0.0")
	assertCookbookVersionBody(t, payload, "app5", "2.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamConflictingPassingGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app5": "= 2.0.0",
		"app4": "<= 5.0.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "0.1.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "1.0.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{"app4": ">= 3.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "3.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app3", "4.0.0", "", map[string]string{"app5": ">= 2.0.0"})
	createCookbookVersion(t, router, "app4", "0.1.0", "", map[string]string{"app5": "= 0.1.0"})
	createCookbookVersion(t, router, "app4", "0.3.0", "", map[string]string{"app5": "= 0.3.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app5", "0.1.0", "", nil)
	createCookbookVersion(t, router, "app5", "0.3.0", "", nil)
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app2", "app5"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver conflicting passing status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "5.0.0")
	assertCookbookVersionBody(t, payload, "app5", "2.0.0")
}

func TestEnvironmentCookbookVersionsMatchesUpstreamConflictingFailingGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": "<= 5.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app3"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver conflicting failing status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertConflictingFailingDepsolverDetail(t, payload)
}

func TestOrganizationEnvironmentCookbookVersionsMatchesUpstreamConflictingFailingGraph(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": "<= 5.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1", "app3"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver conflicting failing status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertConflictingFailingDepsolverDetail(t, payload)
}

func TestDefaultEnvironmentCookbookVersionsMatchesUpstreamConflictingFailingGraph(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.0.0",
		"app5": "= 2.0.0",
		"app4": "<= 5.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{"app4": ">= 5.0.0"})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{"app5": "= 6.0.0"})
	createCookbookVersion(t, router, "app4", "5.0.0", "", map[string]string{"app5": "= 2.0.0"})
	createCookbookVersion(t, router, "app5", "2.0.0", "", nil)
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1", "app3"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver conflicting failing status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertConflictingFailingDepsolverDetail(t, payload)
		})
	}
}

func TestEnvironmentCookbookVersionsCombinesEnvironmentAndDependencyConstraints(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{
		"app4": ">= 5.0.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{
		"app5": ">= 0.0.0",
	})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver combined constraints status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "6.0.0")
	assertCookbookVersionBody(t, payload, "app5", "6.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsCombinesEnvironmentAndDependencyConstraints(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"app3": "<= 0.1.5",
	})
	createCookbookVersion(t, router, "app1", "3.0.0", "", map[string]string{
		"app2": ">= 0.1.0",
		"app3": ">= 0.1.1",
	})
	createCookbookVersion(t, router, "app2", "3.0.0", "", map[string]string{
		"app4": ">= 5.0.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.0", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app3", "0.1.3", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app3", "2.0.0", "", map[string]string{
		"app5": ">= 2.0.0",
	})
	createCookbookVersion(t, router, "app4", "6.0.0", "", map[string]string{
		"app5": ">= 0.0.0",
	})
	createCookbookVersion(t, router, "app5", "6.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@3.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver combined constraints status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 5 {
		t.Fatalf("len(payload) = %d, want 5 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "app1", "3.0.0")
	assertCookbookVersionBody(t, payload, "app2", "3.0.0")
	assertCookbookVersionBody(t, payload, "app3", "0.1.3")
	assertCookbookVersionBody(t, payload, "app4", "6.0.0")
	assertCookbookVersionBody(t, payload, "app5", "6.0.0")
}

func TestEnvironmentCookbookVersionsReturns412ForImpossibleDependencyViaEnvironmentConstraint(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver impossible-via-environment status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 1.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsReturns412ForImpossibleDependencyViaEnvironmentConstraint(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver impossible-via-environment status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 1.2.3) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 1.0.0 -> []"},
	})
}

func TestEnvironmentCookbookVersionsSelectsRootVersionThatRespectsEnvironmentConstraints(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver environment-respected root selection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsSelectsRootVersionThatRespectsEnvironmentConstraints(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "= 1.0.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver environment-respected root selection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestEnvironmentCookbookVersionsSelectsNewerRootVersionWhenEnvironmentAllowsIt(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "> 1.1.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver environment-respected newer-root status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.2.3")
	assertCookbookVersionBody(t, payload, "bar", "3.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsSelectsNewerRootVersionWhenEnvironmentAllowsIt(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"bar": "> 1.1.0",
	})
	createCookbookVersion(t, router, "foo", "1.2.3", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)
	createCookbookVersion(t, router, "bar", "3.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver environment-respected newer-root status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.2.3")
	assertCookbookVersionBody(t, payload, "bar", "3.0.0")
}

func TestEnvironmentCookbookVersionsSelectsPinnedVersionForRepeatedRootCookbook(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver repeated-root pinned selection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "2.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsSelectsPinnedVersionForRepeatedRootCookbook(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver repeated-root pinned selection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "2.0.0")
	assertCookbookVersionBody(t, payload, "bar", "2.0.0")
}

func TestDefaultEnvironmentCookbookVersionsSelectsPinnedVersionForRepeatedRootCookbook(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "= 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo", "foo@2.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver repeated-root pinned selection status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 2 {
				t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "foo", "2.0.0")
			assertCookbookVersionBody(t, payload, "bar", "2.0.0")
		})
	}
}

func TestEnvironmentCookbookVersionsKeepsFirstRootLabelForRepeatedCookbook(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("depsolver repeated-root label status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 2.0.0) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
	})
}

func TestOrganizationEnvironmentCookbookVersionsKeepsFirstRootLabelForRepeatedCookbook(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"foo", "foo@2.0.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusPreconditionFailed {
		t.Fatalf("org-scoped depsolver repeated-root label status = %d, want %d, body = %s", rec.Code, http.StatusPreconditionFailed, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
		"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 2.0.0) -> (bar > 2.0.0)]",
		"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
		"non_existent_cookbooks":      []string{},
		"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
	})
}

func TestDefaultEnvironmentCookbookVersionsKeepsFirstRootLabelForRepeatedCookbook(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "foo", "1.0.0", "", nil)
	createCookbookVersion(t, router, "foo", "2.0.0", "", map[string]string{"bar": "> 2.0.0"})
	createCookbookVersion(t, router, "bar", "2.0.0", "", nil)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"foo", "foo@2.0.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusPreconditionFailed {
				t.Fatalf("%s depsolver repeated-root label status = %d, want %d, body = %s", route.name, rec.Code, http.StatusPreconditionFailed, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertUnsatisfiedDepsolverDetail(t, payload, map[string]any{
				"message":                     "Unable to satisfy constraints on package bar due to solution constraint (foo >= 0.0.0). Solution constraints that may result in a constraint on bar: [(foo = 2.0.0) -> (bar > 2.0.0)]",
				"unsatisfiable_run_list_item": "(foo >= 0.0.0)",
				"non_existent_cookbooks":      []string{},
				"most_constrained_cookbooks":  []string{"bar = 2.0.0 -> []"},
			})
		})
	}
}

func TestEnvironmentCookbookVersionsAllowsCircularDependencies(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{
		"app1": ">= 0.0.0",
	})

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver circular dependency status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.0.1")
}

func TestOrganizationEnvironmentCookbookVersionsAllowsCircularDependencies(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{
		"app1": ">= 0.0.0",
	})

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"app1@0.1.0"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver circular dependency status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "app1", "0.1.0")
	assertCookbookVersionBody(t, payload, "app2", "0.0.1")
}

func TestDefaultEnvironmentCookbookVersionsAllowsCircularDependencies(t *testing.T) {
	router := newTestRouter(t)
	createCookbookVersion(t, router, "app1", "0.1.0", "", map[string]string{
		"app2": ">= 0.0.0",
	})
	createCookbookVersion(t, router, "app2", "0.0.1", "", map[string]string{
		"app1": ">= 0.0.0",
	})

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"app1@0.1.0"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver circular dependency status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			assertCookbookVersionBody(t, payload, "app1", "0.1.0")
			assertCookbookVersionBody(t, payload, "app2", "0.0.1")
		})
	}
}

func TestEnvironmentCookbookVersionsSupportsDatestampVersions(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "datestamp-env")
	createCookbookVersion(t, router, "datestamp", "1.2.20130730201745", "", nil)
	updateEnvironmentCookbookConstraints(t, router, "datestamp-env", map[string]string{
		"datestamp": ">= 1.2.20130730200000",
	})

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/datestamp-env/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"datestamp"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver datestamp status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "datestamp", "1.2.20130730201745")
}

func TestOrganizationEnvironmentCookbookVersionsSupportsDatestampVersions(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "datestamp-env")
	createCookbookVersion(t, router, "datestamp", "1.2.20130730201745", "", nil)
	updateEnvironmentCookbookConstraints(t, router, "datestamp-env", map[string]string{
		"datestamp": ">= 1.2.20130730200000",
	})

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/datestamp-env/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"datestamp"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver datestamp status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "datestamp", "1.2.20130730201745")
}

func TestEnvironmentCookbookVersionsRequiresCookbookContainerReadAuthz(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:cookbooks": {},
			},
		}
		return authorizer
	})
	createEnvironmentForCookbookTests(t, router, "production")
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("depsolver cookbook container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:production",
		"read:container:cookbooks",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestOrganizationEnvironmentCookbookVersionsRequiresCookbookContainerReadAuthz(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:cookbooks": {},
			},
		}
		return authorizer
	})
	createEnvironmentForCookbookTests(t, router, "production")
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("org-scoped depsolver cookbook container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:production",
		"read:container:cookbooks",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestDefaultEnvironmentCookbookVersionsRequiresCookbookContainerReadAuthz(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:cookbooks": {},
			},
		}
		return authorizer
	})
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("default depsolver cookbook container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:_default",
		"read:container:cookbooks",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRequiresCookbookContainerReadAuthz(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, _ := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:cookbooks": {},
			},
		}
		return authorizer
	})
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("org-scoped default depsolver cookbook container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:_default",
		"read:container:cookbooks",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestEnvironmentCookbookVersionsRequiresRolesContainerReadAuthzForRoleRunList(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:roles": {},
			},
		}
		return authorizer
	})
	createEnvironmentForCookbookTests(t, router, "production")
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("depsolver roles container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:production",
		"read:container:cookbooks",
		"read:container:roles",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestOrganizationEnvironmentCookbookVersionsRequiresRolesContainerReadAuthzForRoleRunList(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:roles": {},
			},
		}
		return authorizer
	})
	createEnvironmentForCookbookTests(t, router, "production")
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("org-scoped depsolver roles container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:production",
		"read:container:cookbooks",
		"read:container:roles",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestDefaultEnvironmentCookbookVersionsRequiresRolesContainerReadAuthzForRoleRunList(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:roles": {},
			},
		}
		return authorizer
	})
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("default depsolver roles container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:_default",
		"read:container:cookbooks",
		"read:container:roles",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestOrganizationDefaultEnvironmentCookbookVersionsRequiresRolesContainerReadAuthzForRoleRunList(t *testing.T) {
	var authorizer *recordingDepsolverAuthorizer
	router, state := newSearchTestRouterWithAuthorizer(t, func(state *bootstrap.Service) authz.Authorizer {
		authorizer = &recordingDepsolverAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"read:container:roles": {},
			},
		}
		return authorizer
	})
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	authorizer.calls = nil

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("org-scoped default depsolver roles container authz status = %d, want %d, body = %s", rec.Code, http.StatusForbidden, rec.Body.String())
	}

	wantCalls := []string{
		"read:environment:_default",
		"read:container:cookbooks",
		"read:container:roles",
	}
	if len(authorizer.calls) != len(wantCalls) {
		t.Fatalf("depsolver authz calls = %v, want %v", authorizer.calls, wantCalls)
	}
	for idx := range wantCalls {
		if authorizer.calls[idx] != wantCalls[idx] {
			t.Fatalf("depsolver authz calls[%d] = %q, want %q (%v)", idx, authorizer.calls[idx], wantCalls[idx], authorizer.calls)
		}
	}
}

func TestEnvironmentCookbookVersionsExpandsRoleRunLists(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "apache2", "1.0.0", "", nil)
	createCookbookVersion(t, router, "nginx", "2.0.0", "", nil)
	createCookbookVersion(t, router, "users", "3.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "base",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{"recipe[nginx]"},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(base) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[base]", "recipe[users]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver role expansion status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "nginx", "2.0.0")
	assertCookbookVersionBody(t, payload, "users", "3.0.0")
	if _, ok := payload["apache2"]; ok {
		t.Fatalf("production role-expanded payload unexpectedly contains apache2: %v", payload)
	}

	defaultReq := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("depsolver default role expansion status = %d, want %d, body = %s", defaultRec.Code, http.StatusOK, defaultRec.Body.String())
	}

	defaultPayload := decodeJSONMap(t, defaultRec.Body.Bytes())
	assertCookbookVersionBody(t, defaultPayload, "apache2", "1.0.0")
	assertCookbookVersionBody(t, defaultPayload, "users", "3.0.0")
	if _, ok := defaultPayload["nginx"]; ok {
		t.Fatalf("default role-expanded payload unexpectedly contains nginx: %v", defaultPayload)
	}
}

func TestOrganizationEnvironmentCookbookVersionsExpandsRoleRunLists(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "apache2", "1.0.0", "", nil)
	createCookbookVersion(t, router, "nginx", "2.0.0", "", nil)
	createCookbookVersion(t, router, "users", "3.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "base",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{"recipe[nginx]"},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(base) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[base]", "recipe[users]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver role expansion status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	assertCookbookVersionBody(t, payload, "nginx", "2.0.0")
	assertCookbookVersionBody(t, payload, "users", "3.0.0")
	if _, ok := payload["apache2"]; ok {
		t.Fatalf("production role-expanded payload unexpectedly contains apache2: %v", payload)
	}

	defaultReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("org-scoped default role expansion status = %d, want %d, body = %s", defaultRec.Code, http.StatusOK, defaultRec.Body.String())
	}

	defaultPayload := decodeJSONMap(t, defaultRec.Body.Bytes())
	assertCookbookVersionBody(t, defaultPayload, "apache2", "1.0.0")
	assertCookbookVersionBody(t, defaultPayload, "users", "3.0.0")
	if _, ok := defaultPayload["nginx"]; ok {
		t.Fatalf("org-scoped default role-expanded payload unexpectedly contains nginx: %v", defaultPayload)
	}
}

func TestEnvironmentCookbookVersionsUsesExplicitEmptyEnvironmentRoleRunList(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "apache2", "1.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver empty env role run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("production role-expanded payload = %v, want empty object", payload)
	}

	defaultReq := newSignedJSONRequest(t, http.MethodPost, "/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("depsolver default role expansion status = %d, want %d, body = %s", defaultRec.Code, http.StatusOK, defaultRec.Body.String())
	}

	defaultPayload := decodeJSONMap(t, defaultRec.Body.Bytes())
	if len(defaultPayload) != 1 {
		t.Fatalf("len(defaultPayload) = %d, want 1 (%v)", len(defaultPayload), defaultPayload)
	}
	assertCookbookVersionBody(t, defaultPayload, "apache2", "1.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsUsesExplicitEmptyEnvironmentRoleRunList(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "apache2", "1.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[apache2]"},
			"env_run_lists": map[string]any{
				"production": []any{},
			},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver empty env role run_list status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 0 {
		t.Fatalf("production role-expanded payload = %v, want empty object", payload)
	}

	defaultReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/_default/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)
	if defaultRec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver default role expansion status = %d, want %d, body = %s", defaultRec.Code, http.StatusOK, defaultRec.Body.String())
	}

	defaultPayload := decodeJSONMap(t, defaultRec.Body.Bytes())
	if len(defaultPayload) != 1 {
		t.Fatalf("len(defaultPayload) = %d, want 1 (%v)", len(defaultPayload), defaultPayload)
	}
	assertCookbookVersionBody(t, defaultPayload, "apache2", "1.0.0")
}

func TestEnvironmentCookbookVersionsDeduplicatesRoleExpandedEquivalentRunListForms(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[foo]", "recipe[foo::default]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]", "foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver role-expanded duplicate normalization status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestOrganizationEnvironmentCookbookVersionsDeduplicatesRoleExpandedEquivalentRunListForms(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[foo]", "recipe[foo::default]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]", "foo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("org-scoped depsolver role-expanded duplicate normalization status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	payload := decodeJSONMap(t, rec.Body.Bytes())
	if len(payload) != 2 {
		t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
	}
	assertCookbookVersionBody(t, payload, "foo", "1.0.0")
	assertCookbookVersionBody(t, payload, "bar", "1.0.0")
}

func TestDefaultEnvironmentCookbookVersionsDeduplicatesRoleExpandedEquivalentRunListForms(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createCookbookVersion(t, router, "foo", "1.0.0", "", map[string]string{"bar": "= 1.0.0"})
	createCookbookVersion(t, router, "bar", "1.0.0", "", nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"recipe[foo]", "recipe[foo::default]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"role[web]", "foo"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s depsolver role-expanded duplicate normalization status = %d, want %d, body = %s", route.name, rec.Code, http.StatusOK, rec.Body.String())
			}

			payload := decodeJSONMap(t, rec.Body.Bytes())
			if len(payload) != 2 {
				t.Fatalf("len(payload) = %d, want 2 (%v)", len(payload), payload)
			}
			assertCookbookVersionBody(t, payload, "foo", "1.0.0")
			assertCookbookVersionBody(t, payload, "bar", "1.0.0")
		})
	}
}

func TestEnvironmentCookbookVersionsRejectsMissingRoleRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[missing]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver missing role status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains unknown role item role[missing]")
}

func TestOrganizationEnvironmentCookbookVersionsRejectsMissingRoleRunListItem(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[missing]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver missing role status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains unknown role item role[missing]")
}

func TestDefaultEnvironmentCookbookVersionsRejectsMissingRoleRunListItemOnDefaultAliases(t *testing.T) {
	router := newTestRouter(t)

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"role[missing]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s depsolver missing role status = %d, want %d, body = %s", route.name, rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains unknown role item role[missing]")
		})
	}
}

func TestEnvironmentCookbookVersionsRejectsRecursiveRoleRunListItem(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[db]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "db",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[web]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(db) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("depsolver recursive role status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains recursive role item role[web]")
}

func TestOrganizationEnvironmentCookbookVersionsRejectsRecursiveRoleRunListItem(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)
	createEnvironmentForCookbookTests(t, router, "production")

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[db]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "db",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[web]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(db) error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"role[web]"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("org-scoped depsolver recursive role status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}

	assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains recursive role item role[web]")
}

func TestDefaultEnvironmentCookbookVersionsRejectsRecursiveRoleRunListItemOnDefaultAliases(t *testing.T) {
	router, state := newSearchTestRouterWithAuthorizer(t, nil)

	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "web",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[db]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(web) error = %v", err)
	}
	if _, err := state.CreateRole("ponyville", bootstrap.CreateRoleInput{
		Payload: map[string]any{
			"name":                "db",
			"description":         "",
			"json_class":          "Chef::Role",
			"chef_type":           "role",
			"default_attributes":  map[string]any{},
			"override_attributes": map[string]any{},
			"run_list":            []any{"role[web]"},
			"env_run_lists":       map[string]any{},
		},
	}); err != nil {
		t.Fatalf("CreateRole(db) error = %v", err)
	}

	routes := []struct {
		name string
		path string
	}{
		{name: "default_environment", path: "/environments/_default/cookbook_versions"},
		{name: "org_scoped_default_environment", path: "/organizations/ponyville/environments/_default/cookbook_versions"},
	}

	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, route.path, mustMarshalSandboxJSON(t, map[string]any{
				"run_list": []any{"role[web]"},
			}))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("%s depsolver recursive role status = %d, want %d, body = %s", route.name, rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			assertEnvironmentErrorMessages(t, rec.Body.Bytes(), "Field 'run_list' contains recursive role item role[web]")
		})
	}
}

func assertEnvironmentErrorMessages(t *testing.T, body []byte, want ...string) {
	t.Helper()

	payload := decodeJSONMap(t, body)
	assertStringSliceValue(t, payload["error"], want)
}

func assertDepsolverErrorDetail(t *testing.T, payload map[string]any, want map[string]any) {
	t.Helper()

	errorList, ok := payload["error"].([]any)
	if !ok || len(errorList) != 1 {
		t.Fatalf("error payload = %v, want one depsolver error object", payload["error"])
	}
	detail, ok := errorList[0].(map[string]any)
	if !ok {
		t.Fatalf("error detail type = %T, want map[string]any", errorList[0])
	}
	if detail["message"] != want["message"] {
		t.Fatalf("message = %v, want %v", detail["message"], want["message"])
	}
	assertStringSliceValue(t, detail["non_existent_cookbooks"], want["non_existent_cookbooks"].([]string))
	assertStringSliceValue(t, detail["cookbooks_with_no_versions"], want["cookbooks_with_no_versions"].([]string))
}

func assertUnsatisfiedDepsolverDetail(t *testing.T, payload map[string]any, want map[string]any) {
	t.Helper()

	errorList, ok := payload["error"].([]any)
	if !ok || len(errorList) != 1 {
		t.Fatalf("error payload = %v, want one depsolver error object", payload["error"])
	}
	detail, ok := errorList[0].(map[string]any)
	if !ok {
		t.Fatalf("error detail type = %T, want map[string]any", errorList[0])
	}
	if detail["message"] != want["message"] {
		t.Fatalf("message = %v, want %v", detail["message"], want["message"])
	}
	if detail["unsatisfiable_run_list_item"] != want["unsatisfiable_run_list_item"] {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %v", detail["unsatisfiable_run_list_item"], want["unsatisfiable_run_list_item"])
	}
	assertStringSliceValue(t, detail["non_existent_cookbooks"], want["non_existent_cookbooks"].([]string))
	assertStringSliceValue(t, detail["most_constrained_cookbooks"], want["most_constrained_cookbooks"].([]string))
}

func assertConflictingFailingDepsolverDetail(t *testing.T, payload map[string]any) {
	t.Helper()

	errorList, ok := payload["error"].([]any)
	if !ok || len(errorList) != 1 {
		t.Fatalf("error payload = %v, want one depsolver error object", payload["error"])
	}
	detail, ok := errorList[0].(map[string]any)
	if !ok {
		t.Fatalf("error detail type = %T, want map[string]any", errorList[0])
	}
	message, ok := detail["message"].(string)
	if !ok {
		t.Fatalf("message type = %T, want string", detail["message"])
	}
	if !strings.Contains(message, "Unable to satisfy constraints on package app5 due to solution constraint (app3 >= 0.0.0).") {
		t.Fatalf("message = %q, want later-root conflicting failing prefix", message)
	}
	if !strings.Contains(message, "(app1 = 3.0.0) -> (app5 = 2.0.0)") {
		t.Fatalf("message = %q, want direct app1 path", message)
	}
	if !strings.Contains(message, "(app3 = 0.1.0) -> (app5 = 6.0.0)") {
		t.Fatalf("message = %q, want app3 conflicting path", message)
	}
	if detail["unsatisfiable_run_list_item"] != "(app3 >= 0.0.0)" {
		t.Fatalf("unsatisfiable_run_list_item = %v, want %q", detail["unsatisfiable_run_list_item"], "(app3 >= 0.0.0)")
	}
	assertStringSliceValue(t, detail["non_existent_cookbooks"], []string{})
	assertStringSliceValue(t, detail["most_constrained_cookbooks"], []string{"app5 = 2.0.0 -> []"})
}

func assertCookbookVersionBody(t *testing.T, payload map[string]any, cookbook, version string) {
	t.Helper()

	entry, ok := payload[cookbook].(map[string]any)
	if !ok {
		t.Fatalf("payload[%q] = %T, want map[string]any", cookbook, payload[cookbook])
	}
	if entry["version"] != version {
		t.Fatalf("payload[%q].version = %v, want %q", cookbook, entry["version"], version)
	}
	if entry["cookbook_name"] != cookbook {
		t.Fatalf("payload[%q].cookbook_name = %v, want %q", cookbook, entry["cookbook_name"], cookbook)
	}
}

func assertDepsolverResponseDependencies(t *testing.T, value any, want map[string]string) {
	t.Helper()

	entry, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("entry = %T, want map[string]any (%v)", value, value)
	}
	metadata, ok := entry["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("entry.metadata = %T, want map[string]any (%v)", entry["metadata"], entry["metadata"])
	}
	raw, ok := metadata["dependencies"].(map[string]any)
	if !ok {
		t.Fatalf("metadata.dependencies = %T, want map[string]any (%v)", metadata["dependencies"], metadata["dependencies"])
	}
	if len(raw) != len(want) {
		t.Fatalf("len(metadata.dependencies) = %d, want %d (%v)", len(raw), len(want), raw)
	}
	for key, wantValue := range want {
		if raw[key] != wantValue {
			t.Fatalf("metadata.dependencies[%q] = %v, want %q (%v)", key, raw[key], wantValue, raw)
		}
	}
	if _, ok := metadata["attributes"]; ok {
		t.Fatalf("depsolver metadata.attributes present, want omitted (%v)", metadata)
	}
	if _, ok := metadata["long_description"]; ok {
		t.Fatalf("depsolver metadata.long_description present, want omitted (%v)", metadata)
	}
}

func decodeJSONMap(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	object, ok := payload.(map[string]any)
	if !ok {
		t.Fatalf("decoded JSON = %T, want top-level object (%s)", payload, bytes.TrimSpace(body))
	}
	return object
}

func assertStringSliceValue(t *testing.T, value any, want []string) {
	t.Helper()

	raw, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %T, want []any (%v)", value, value)
	}
	if len(raw) != len(want) {
		t.Fatalf("len(value) = %d, want %d (%v)", len(raw), len(want), raw)
	}
	for idx := range want {
		if raw[idx] != want[idx] {
			t.Fatalf("value[%d] = %v, want %q (%v)", idx, raw[idx], want[idx], raw)
		}
	}
}

type recordingDepsolverAuthorizer struct {
	base  authz.Authorizer
	deny  map[string]struct{}
	calls []string
}

func (a *recordingDepsolverAuthorizer) Name() string {
	return "recording-depsolver-test"
}

func (a *recordingDepsolverAuthorizer) Authorize(ctx context.Context, subject authz.Subject, action authz.Action, resource authz.Resource) (authz.Decision, error) {
	key := string(action) + ":" + resource.Type + ":" + resource.Name
	a.calls = append(a.calls, key)
	if _, denied := a.deny[key]; denied {
		return authz.Decision{Allowed: false, Reason: "denied for test"}, nil
	}
	if a.base == nil {
		return authz.Decision{Allowed: true, Reason: "no base authorizer"}, nil
	}
	return a.base.Authorize(ctx, subject, action, resource)
}
