package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
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

func TestEnvironmentCookbookVersionsResolvesPinnedAndDependentCookbooks(t *testing.T) {
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

func TestEnvironmentCookbookVersionsSupportsPessimisticDependencyConstraint(t *testing.T) {
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

func decodeJSONMap(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	return payload
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
