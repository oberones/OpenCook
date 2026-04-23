package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
)

func TestCookbookReadRoutesUseInjectedCookbookStore(t *testing.T) {
	router := newTestRouterWithBootstrapOptions(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return newReadOnlyRouteCookbookStore(
				map[string][]bootstrap.CookbookVersion{
					"demo": {
						{
							Name:         "demo-2.0.0",
							CookbookName: "demo",
							Version:      "2.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "demo",
								"version":      "2.0.0",
								"dependencies": map[string]any{"apt": ">= 2.0.0"},
								"recipes":      map[string]any{"demo::users": ""},
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "users.rb", Path: "recipes/users.rb", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Specificity: "default"},
							},
						},
						{
							Name:         "demo-1.0.0",
							CookbookName: "demo",
							Version:      "1.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "demo",
								"version":      "1.0.0",
								"dependencies": map[string]any{"apt": ">= 1.0.0"},
								"recipes":      map[string]any{"demo::default": ""},
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "default.rb", Path: "recipes/default.rb", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Specificity: "default"},
							},
						},
					},
				},
				map[string][]bootstrap.CookbookArtifact{
					"demo": {
						{
							Name:       "demo",
							Identifier: "1111111111111111111111111111111111111111",
							Version:    "2.0.0",
							ChefType:   "cookbook_version",
							Metadata: map[string]any{
								"name":    "demo",
								"version": "2.0.0",
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "default.rb", Path: "recipes/default.rb", Checksum: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Specificity: "default"},
							},
						},
					},
				},
			)
		},
	})

	t.Run("collection", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/cookbooks", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cookbook collection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(cookbook collection) error = %v", err)
		}
		assertCookbookVersionList(t, payload, "demo", "2.0.0")
	})

	t.Run("named latest", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/demo/_latest", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cookbook latest status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(cookbook latest) error = %v", err)
		}
		if payload["version"] != "2.0.0" {
			t.Fatalf("payload.version = %v, want %q", payload["version"], "2.0.0")
		}
	})

	t.Run("recipes", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/cookbooks/_recipes", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cookbook recipes status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload []string
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(cookbook recipes) error = %v", err)
		}
		want := []string{"demo::users"}
		if len(payload) != len(want) || payload[0] != want[0] {
			t.Fatalf("payload = %v, want %v", payload, want)
		}
	})

	t.Run("artifact get", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/cookbook_artifacts/demo/1111111111111111111111111111111111111111", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("cookbook artifact status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(cookbook artifact) error = %v", err)
		}
		if payload["identifier"] != "1111111111111111111111111111111111111111" {
			t.Fatalf("payload.identifier = %v, want artifact identifier", payload["identifier"])
		}
	})

	t.Run("universe", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/universe", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("universe status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(universe) error = %v", err)
		}
		demo := payload["demo"].(map[string]any)
		if _, ok := demo["2.0.0"]; !ok {
			t.Fatalf("universe payload = %v, want 2.0.0 entry", demo)
		}
	})
}

func TestEnvironmentCookbookReadRoutesUseInjectedCookbookStore(t *testing.T) {
	router := newTestRouterWithBootstrapOptions(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return newReadOnlyRouteCookbookStore(
				map[string][]bootstrap.CookbookVersion{
					"demo": {
						{
							Name:         "demo-2.0.0",
							CookbookName: "demo",
							Version:      "2.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "demo",
								"version":      "2.0.0",
								"dependencies": map[string]any{},
								"recipes": map[string]any{
									"demo::default": "",
									"demo::users":   "",
								},
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "default.rb", Path: "recipes/default.rb", Checksum: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Specificity: "default"},
								{Name: "users.rb", Path: "recipes/users.rb", Checksum: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Specificity: "default"},
							},
						},
						{
							Name:         "demo-1.0.0",
							CookbookName: "demo",
							Version:      "1.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "demo",
								"version":      "1.0.0",
								"dependencies": map[string]any{},
								"recipes":      map[string]any{"demo::legacy": ""},
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "legacy.rb", Path: "recipes/legacy.rb", Checksum: "cccccccccccccccccccccccccccccccc", Specificity: "default"},
							},
						},
					},
					"other": {
						{
							Name:         "other-0.5.0",
							CookbookName: "other",
							Version:      "0.5.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "other",
								"version":      "0.5.0",
								"dependencies": map[string]any{},
								"recipes":      map[string]any{"other::default": ""},
							},
							AllFiles: []bootstrap.CookbookFile{
								{Name: "default.rb", Path: "recipes/default.rb", Checksum: "dddddddddddddddddddddddddddddddd", Specificity: "default"},
							},
						},
					},
				},
				nil,
			)
		},
	})

	createEnvironmentForCookbookTests(t, router, "production")
	updateEnvironmentCookbookConstraints(t, router, "production", map[string]string{
		"demo": "< 2.5.0",
	})

	t.Run("collection", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/environments/production/cookbooks?num_versions=2", nil)
		applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/environments/production/cookbooks", nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, "2026-04-02T15:04:15Z")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("environment cookbook collection status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(environment cookbook collection) error = %v", err)
		}
		assertCookbookVersionList(t, payload, "demo", "2.0.0", "1.0.0")
		assertCookbookVersionList(t, payload, "other", "0.5.0")
	})

	t.Run("recipes", func(t *testing.T) {
		req := newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/environments/production/recipes", nil)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("environment recipes status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
		}

		var payload []string
		if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal(environment recipes) error = %v", err)
		}

		want := []string{"demo", "demo::users", "other"}
		if len(payload) != len(want) {
			t.Fatalf("len(payload) = %d, want %d (%v)", len(payload), len(want), payload)
		}
		for idx := range want {
			if payload[idx] != want[idx] {
				t.Fatalf("payload[%d] = %q, want %q (%v)", idx, payload[idx], want[idx], payload)
			}
		}
	})
}

func TestEnvironmentDepsolverUsesInjectedCookbookStore(t *testing.T) {
	router := newTestRouterWithBootstrapOptions(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, bootstrap.Options{
		SuperuserName: "pivotal",
		CookbookStoreFactory: func(*bootstrap.Service) bootstrap.CookbookStore {
			return newReadOnlyRouteCookbookStore(
				map[string][]bootstrap.CookbookVersion{
					"demo": {
						{
							Name:         "demo-2.0.0",
							CookbookName: "demo",
							Version:      "2.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "demo",
								"version":      "2.0.0",
								"dependencies": map[string]any{"dep": "= 1.0.0"},
								"recipes":      map[string]any{"demo::default": ""},
							},
						},
					},
					"dep": {
						{
							Name:         "dep-1.0.0",
							CookbookName: "dep",
							Version:      "1.0.0",
							JSONClass:    "Chef::CookbookVersion",
							ChefType:     "cookbook_version",
							Metadata: map[string]any{
								"name":         "dep",
								"version":      "1.0.0",
								"dependencies": map[string]any{},
								"recipes":      map[string]any{"dep::default": ""},
							},
						},
					},
				},
				nil,
			)
		},
	})

	createEnvironmentForCookbookTests(t, router, "production")

	req := newSignedJSONRequest(t, http.MethodPost, "/environments/production/cookbook_versions", mustMarshalSandboxJSON(t, map[string]any{
		"run_list": []any{"demo"},
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("depsolver status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(depsolver payload) error = %v", err)
	}
	assertCookbookVersionBody(t, payload, "demo", "2.0.0")
	assertCookbookVersionBody(t, payload, "dep", "1.0.0")
}

type readOnlyRouteCookbookStore struct {
	versions  map[string][]bootstrap.CookbookVersion
	artifacts map[string][]bootstrap.CookbookArtifact
}

func newReadOnlyRouteCookbookStore(versions map[string][]bootstrap.CookbookVersion, artifacts map[string][]bootstrap.CookbookArtifact) *readOnlyRouteCookbookStore {
	return &readOnlyRouteCookbookStore{
		versions:  cloneRouteCookbookVersions(versions),
		artifacts: cloneRouteCookbookArtifacts(artifacts),
	}
}

func (s *readOnlyRouteCookbookStore) HasCookbookVersion(_ string, name, version string) (bool, bool) {
	for _, candidate := range s.versions[strings.TrimSpace(name)] {
		if candidate.Version == strings.TrimSpace(version) {
			return true, true
		}
	}
	return false, true
}

func (s *readOnlyRouteCookbookStore) ListCookbookArtifacts(_ string) (map[string][]bootstrap.CookbookArtifact, bool) {
	out := make(map[string][]bootstrap.CookbookArtifact, len(s.artifacts))
	for name, artifacts := range s.artifacts {
		out[name] = append([]bootstrap.CookbookArtifact(nil), artifacts...)
	}
	return out, true
}

func (s *readOnlyRouteCookbookStore) ListCookbookArtifactsByName(_ string, name string) ([]bootstrap.CookbookArtifact, bool, bool) {
	artifacts, ok := s.artifacts[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}
	return append([]bootstrap.CookbookArtifact(nil), artifacts...), true, true
}

func (s *readOnlyRouteCookbookStore) GetCookbookArtifact(_ string, name, identifier string) (bootstrap.CookbookArtifact, bool, bool) {
	for _, artifact := range s.artifacts[strings.TrimSpace(name)] {
		if artifact.Identifier == strings.TrimSpace(identifier) {
			return artifact, true, true
		}
	}
	return bootstrap.CookbookArtifact{}, true, false
}

func (s *readOnlyRouteCookbookStore) CreateCookbookArtifact(_ string, _ bootstrap.CookbookArtifact) (bootstrap.CookbookArtifact, error) {
	panic("unexpected CreateCookbookArtifact call in read-only route store")
}

func (s *readOnlyRouteCookbookStore) DeleteCookbookArtifactWithReleasedChecksums(_, _, _ string) (bootstrap.CookbookArtifact, []string, error) {
	panic("unexpected DeleteCookbookArtifactWithReleasedChecksums call in read-only route store")
}

func (s *readOnlyRouteCookbookStore) ListCookbookVersions(_ string) (map[string][]bootstrap.CookbookVersionRef, bool) {
	out := make(map[string][]bootstrap.CookbookVersionRef, len(s.versions))
	for name, versions := range s.versions {
		refs := make([]bootstrap.CookbookVersionRef, 0, len(versions))
		for _, version := range versions {
			refs = append(refs, bootstrap.CookbookVersionRef{Name: version.CookbookName, Version: version.Version})
		}
		out[name] = refs
	}
	return out, true
}

func (s *readOnlyRouteCookbookStore) ListCookbookVersionsByName(_ string, name string) ([]bootstrap.CookbookVersionRef, bool, bool) {
	versions, ok := s.versions[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}
	refs := make([]bootstrap.CookbookVersionRef, 0, len(versions))
	for _, version := range versions {
		refs = append(refs, bootstrap.CookbookVersionRef{Name: version.CookbookName, Version: version.Version})
	}
	return refs, true, true
}

func (s *readOnlyRouteCookbookStore) ListCookbookVersionModelsByName(_ string, name string) ([]bootstrap.CookbookVersion, bool, bool) {
	versions, ok := s.versions[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}
	return append([]bootstrap.CookbookVersion(nil), versions...), true, true
}

func (s *readOnlyRouteCookbookStore) GetCookbookVersion(_ string, name, version string) (bootstrap.CookbookVersion, bool, bool) {
	versions, ok := s.versions[strings.TrimSpace(name)]
	if !ok || len(versions) == 0 {
		return bootstrap.CookbookVersion{}, true, false
	}
	version = strings.TrimSpace(version)
	if version == "_latest" || version == "latest" {
		return versions[0], true, true
	}
	for _, candidate := range versions {
		if candidate.Version == version {
			return candidate, true, true
		}
	}
	return bootstrap.CookbookVersion{}, true, false
}

func (s *readOnlyRouteCookbookStore) UpsertCookbookVersionWithReleasedChecksums(_ string, _ bootstrap.CookbookVersion, _ bool) (bootstrap.CookbookVersion, []string, bool, error) {
	panic("unexpected UpsertCookbookVersionWithReleasedChecksums call in read-only route store")
}

func (s *readOnlyRouteCookbookStore) DeleteCookbookVersionWithReleasedChecksums(_, _, _ string) (bootstrap.CookbookVersion, []string, error) {
	panic("unexpected DeleteCookbookVersionWithReleasedChecksums call in read-only route store")
}

func (s *readOnlyRouteCookbookStore) DeleteCookbookChecksumReferencesFromRemaining(map[string]struct{}) {
}

func (s *readOnlyRouteCookbookStore) CookbookChecksumReferenced(string) bool { return false }

func cloneRouteCookbookVersions(in map[string][]bootstrap.CookbookVersion) map[string][]bootstrap.CookbookVersion {
	out := make(map[string][]bootstrap.CookbookVersion, len(in))
	for name, versions := range in {
		out[name] = append([]bootstrap.CookbookVersion(nil), versions...)
	}
	return out
}

func cloneRouteCookbookArtifacts(in map[string][]bootstrap.CookbookArtifact) map[string][]bootstrap.CookbookArtifact {
	out := make(map[string][]bootstrap.CookbookArtifact, len(in))
	for name, artifacts := range in {
		out[name] = append([]bootstrap.CookbookArtifact(nil), artifacts...)
	}
	return out
}
