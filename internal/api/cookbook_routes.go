package api

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

var legacyCookbookSegments = []string{
	"recipes",
	"definitions",
	"libraries",
	"attributes",
	"files",
	"templates",
	"resources",
	"providers",
	"root_files",
}

func (s *server) handleCookbookArtifacts(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveCookbookScopedRoute(w, r, "cookbook_artifacts")
	if !ok {
		return
	}
	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	segments, ok := routeSegments(r.URL.Path, basePath)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": []string{"not_found"}})
		return
	}

	switch len(segments) {
	case 0:
		s.handleCookbookArtifactCollection(w, r, state, org, basePath)
	case 1:
		s.handleNamedCookbookArtifactCollection(w, r, state, org, basePath, segments[0])
	case 2:
		s.handleNamedCookbookArtifactVersion(w, r, state, org, segments[0], segments[1])
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": []string{"not_found"}})
	}
}

func (s *server) handleCookbooks(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveCookbookScopedRoute(w, r, "cookbooks")
	if !ok {
		return
	}
	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	segments, ok := routeSegments(r.URL.Path, basePath)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": cookbookVersionNotFound("cookbooks", "")})
		return
	}

	switch len(segments) {
	case 0:
		s.handleCookbookCollection(w, r, state, org, basePath)
	case 1:
		switch segments[0] {
		case "_latest":
			s.handleCookbookLatestCollection(w, r, state, org, basePath)
		case "_recipes":
			s.handleCookbookRecipesCollection(w, r, state, org)
		default:
			s.handleNamedCookbookCollection(w, r, state, org, basePath, segments[0])
		}
	case 2:
		s.handleNamedCookbookVersion(w, r, state, org, segments[0], segments[1])
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": cookbookVersionNotFound(segments[0], "")})
	}
}

func (s *server) handleUniverse(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for universe route", http.MethodGet)
		return
	}

	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveCookbookScopedRoute(w, r, "universe")
	if !ok {
		return
	}
	if !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}
	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	universe, _ := state.CookbookUniverse(org)
	writeJSON(w, http.StatusOK, renderUniverseResponse(cookbookCollectionBasePath(r, org), universe))
}

func (s *server) handleCookbookArtifactCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbook artifacts route", http.MethodGet)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbook_artifacts",
		Organization: org,
	}) {
		return
	}

	artifacts, _ := state.ListCookbookArtifacts(org)
	writeJSON(w, http.StatusOK, renderCookbookArtifactCollection(basePath, artifacts))
}

func (s *server) handleNamedCookbookArtifactCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath, name string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbook artifact route", http.MethodGet)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbook_artifacts",
		Organization: org,
	}) {
		return
	}

	artifacts, orgExists, found := state.ListCookbookArtifactsByName(org, name)
	if !orgExists || !found {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": []string{"not_found"}})
		return
	}

	writeJSON(w, http.StatusOK, renderCookbookArtifactCollection(basePath, map[string][]bootstrap.CookbookArtifact{name: artifacts}))
}

func (s *server) handleNamedCookbookArtifactVersion(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, name, identifier string) {
	switch r.Method {
	case http.MethodGet:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "cookbook_artifacts",
			Organization: org,
		}) {
			return
		}
		artifact, orgExists, found := state.GetCookbookArtifact(org, name, identifier)
		if !orgExists || !found {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": []string{"not_found"}})
			return
		}
		writeJSON(w, http.StatusOK, s.renderCookbookArtifactResponse(r, org, artifact))
	case http.MethodPut:
		if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
			Type:         "container",
			Name:         "cookbook_artifacts",
			Organization: org,
		}) {
			return
		}
		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		artifact, err := state.CreateCookbookArtifact(org, bootstrap.CreateCookbookArtifactInput{
			Name:       name,
			Identifier: identifier,
			Payload:    payload,
			ChecksumExists: func(checksum string) (bool, error) {
				return s.blobExists(r.Context(), checksum)
			},
		})
		if !s.writeCookbookArtifactError(w, err, name, identifier) {
			return
		}
		writeJSON(w, http.StatusCreated, s.renderCookbookArtifactResponse(r, org, artifact))
	case http.MethodDelete:
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "container",
			Name:         "cookbook_artifacts",
			Organization: org,
		}) {
			return
		}
		artifact, err := state.DeleteCookbookArtifact(org, name, identifier)
		if !s.writeCookbookArtifactError(w, err, name, identifier) {
			return
		}
		writeJSON(w, http.StatusOK, s.renderCookbookArtifactResponse(r, org, artifact))
	default:
		writeMethodNotAllowed(w, "method not allowed for cookbook artifact version route", http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (s *server) handleCookbookCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbooks route", http.MethodGet)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	limit, allVersions, explicitLimit, ok := parseCookbookNumVersions(r)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": []string{"You have requested an invalid number of versions (x >= 0 || 'all')"},
		})
		return
	}

	versions, _ := state.ListCookbookVersions(org)
	writeJSON(w, http.StatusOK, renderCookbookVersionCollection(basePath, versions, limit, allVersions, explicitLimit))
}

func (s *server) handleCookbookLatestCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbooks route", http.MethodGet)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	versions, _ := state.ListCookbookVersions(org)
	writeJSON(w, http.StatusOK, renderCookbookLatestCollection(basePath, versions))
}

func (s *server) handleCookbookRecipesCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbooks route", http.MethodGet)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	versions, _ := state.ListCookbookVersions(org)
	recipes := make([]string, 0)
	for name, refs := range versions {
		if len(refs) == 0 {
			continue
		}
		version, _, found := state.GetCookbookVersion(org, name, refs[0].Version)
		if !found {
			continue
		}
		recipes = append(recipes, cookbookRecipeNames(name, version.Metadata)...)
	}
	sort.Strings(recipes)
	writeJSON(w, http.StatusOK, recipes)
}

func (s *server) handleNamedCookbookCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath, name string) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for cookbook route", http.MethodGet)
		return
	}
	if !validCookbookName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": []string{invalidCookbookNameMessage(name)}})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	versions, orgExists, found := state.ListCookbookVersionsByName(org, name)
	if !orgExists || !found {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": cookbookNotFound(name)})
		return
	}

	writeJSON(w, http.StatusOK, renderCookbookVersionCollection(basePath, map[string][]bootstrap.CookbookVersionRef{name: versions}, 0, true, true))
}

func (s *server) handleNamedCookbookVersion(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, name, version string) {
	if !validCookbookName(name) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": []string{invalidCookbookNameMessage(name)}})
		return
	}
	switch r.Method {
	case http.MethodGet:
		if !validCookbookVersionPath(version, true) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": []string{invalidCookbookVersionMessage(version)}})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "cookbooks",
			Organization: org,
		}) {
			return
		}

		cookbookVersion, orgExists, found := state.GetCookbookVersion(org, name, version)
		if !orgExists || !found {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": cookbookVersionNotFound(name, version)})
			return
		}
		writeJSON(w, http.StatusOK, s.renderCookbookVersionResponse(r, org, cookbookVersion))
	case http.MethodPut:
		if !validCookbookVersionPath(version, false) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": []string{invalidCookbookVersionMessage(version)}})
			return
		}
		_, _, found := state.GetCookbookVersion(org, name, version)
		action := authz.ActionCreate
		if found {
			action = authz.ActionUpdate
		}
		if !s.authorizeRequest(w, r, action, authz.Resource{
			Type:         "container",
			Name:         "cookbooks",
			Organization: org,
		}) {
			return
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		cookbookVersion, created, err := state.UpsertCookbookVersion(org, bootstrap.UpsertCookbookVersionInput{
			Name:    name,
			Version: version,
			Payload: payload,
			ChecksumExists: func(checksum string) (bool, error) {
				return s.blobExists(r.Context(), checksum)
			},
		})
		if !writeCookbookVersionError(w, err, name, version) {
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, s.renderCookbookVersionWriteResponse(r, cookbookVersion))
	case http.MethodDelete:
		if !validCookbookVersionPath(version, true) {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": []string{invalidCookbookVersionMessage(version)}})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "container",
			Name:         "cookbooks",
			Organization: org,
		}) {
			return
		}

		cookbookVersion, err := state.DeleteCookbookVersion(org, name, version)
		if !writeCookbookVersionError(w, err, name, version) {
			return
		}
		writeJSON(w, http.StatusOK, s.renderCookbookVersionResponse(r, org, cookbookVersion))
	default:
		writeMethodNotAllowed(w, "method not allowed for cookbook version route", http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func (s *server) resolveCookbookScopedRoute(w http.ResponseWriter, r *http.Request, resource string) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/" + resource, true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}
	return org, "/" + resource, true
}

func routeSegments(path, basePath string) ([]string, bool) {
	if !strings.HasPrefix(path, basePath) {
		return nil, false
	}

	tail := strings.TrimPrefix(path, basePath)
	tail = strings.TrimPrefix(tail, "/")
	tail = strings.TrimSuffix(tail, "/")
	if tail == "" {
		return nil, true
	}
	segments := strings.Split(tail, "/")
	for _, segment := range segments {
		if strings.TrimSpace(segment) == "" {
			return nil, false
		}
	}
	return segments, true
}

func renderCookbookArtifactCollection(basePath string, artifacts map[string][]bootstrap.CookbookArtifact) map[string]any {
	if len(artifacts) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(artifacts))
	names := make([]string, 0, len(artifacts))
	for name := range artifacts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		versions := artifacts[name]
		items := make([]map[string]any, 0, len(versions))
		for _, artifact := range versions {
			items = append(items, map[string]any{
				"identifier": artifact.Identifier,
				"url":        basePath + "/" + name + "/" + artifact.Identifier,
			})
		}
		out[name] = map[string]any{
			"url":      basePath + "/" + name,
			"versions": items,
		}
	}
	return out
}

func renderCookbookVersionCollection(basePath string, versions map[string][]bootstrap.CookbookVersionRef, limit int, allVersions, explicitLimit bool) map[string]any {
	if len(versions) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(versions))
	names := make([]string, 0, len(versions))
	for name := range versions {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		refs := versions[name]
		switch {
		case !allVersions && !explicitLimit:
			if len(refs) > 1 {
				refs = refs[:1]
			}
		case !allVersions && limit >= 0 && len(refs) > limit:
			refs = refs[:limit]
		}
		versionList := make([]map[string]any, 0, len(refs))
		for _, ref := range refs {
			versionList = append(versionList, map[string]any{
				"version": ref.Version,
				"url":     basePath + "/" + name + "/" + ref.Version,
			})
		}
		out[name] = map[string]any{
			"url":      basePath + "/" + name,
			"versions": versionList,
		}
	}
	return out
}

func renderCookbookLatestCollection(basePath string, versions map[string][]bootstrap.CookbookVersionRef) map[string]any {
	if len(versions) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(versions))
	names := make([]string, 0, len(versions))
	for name := range versions {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		refs := versions[name]
		if len(refs) == 0 {
			continue
		}
		out[name] = basePath + "/" + name + "/" + refs[0].Version
	}
	return out
}

func renderUniverseResponse(basePath string, universe map[string][]bootstrap.UniverseEntry) map[string]any {
	if len(universe) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(universe))
	names := make([]string, 0, len(universe))
	for name := range universe {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		entries := universe[name]
		versions := make(map[string]any, len(entries))
		for _, entry := range entries {
			deps := make(map[string]any, len(entry.Dependencies))
			for depName, constraint := range entry.Dependencies {
				deps[depName] = constraint
			}
			versions[entry.Version] = map[string]any{
				"location_path": basePath + "/" + name + "/" + entry.Version,
				"location_type": "chef_server",
				"dependencies":  deps,
			}
		}
		out[name] = versions
	}
	return out
}

func (s *server) renderCookbookArtifactResponse(r *http.Request, org string, artifact bootstrap.CookbookArtifact) map[string]any {
	response := map[string]any{
		"name":       artifact.Name,
		"identifier": artifact.Identifier,
		"version":    artifact.Version,
		"chef_type":  artifact.ChefType,
		"frozen?":    artifact.Frozen,
		"metadata":   cloneResponseMap(artifact.Metadata),
	}

	if requestedCookbookAPIVersion(r) >= 2 {
		response["all_files"] = s.renderCookbookFiles(r, org, artifact.AllFiles, true, true)
		return response
	}

	for _, segment := range legacyCookbookSegments {
		response[segment] = []map[string]any{}
	}
	for _, file := range artifact.AllFiles {
		segment := cookbookFileSegment(file.Path)
		entry := map[string]any{
			"name":        cookbookLegacyFileName(segment, file.Path),
			"path":        file.Path,
			"checksum":    file.Checksum,
			"specificity": file.Specificity,
			"url":         s.blobDownloadURL(r, file.Checksum, org),
		}
		response[segment] = append(response[segment].([]map[string]any), entry)
	}
	return response
}

func (s *server) renderCookbookVersionResponse(r *http.Request, org string, version bootstrap.CookbookVersion) map[string]any {
	response := map[string]any{
		"name":          version.Name,
		"cookbook_name": version.CookbookName,
		"version":       version.Version,
		"json_class":    version.JSONClass,
		"chef_type":     version.ChefType,
		"frozen?":       version.Frozen,
		"metadata":      cloneResponseMap(version.Metadata),
	}

	if requestedCookbookAPIVersion(r) >= 2 {
		response["all_files"] = s.renderCookbookFiles(r, org, version.AllFiles, true, true)
		return response
	}

	segmentFiles := make(map[string][]map[string]any)
	for _, segment := range legacyCookbookSegments {
		segmentFiles[segment] = nil
	}
	for _, file := range version.AllFiles {
		segment := cookbookFileSegment(file.Path)
		entry := map[string]any{
			"name":        cookbookLegacyFileName(segment, file.Path),
			"path":        file.Path,
			"checksum":    file.Checksum,
			"specificity": file.Specificity,
			"url":         s.blobDownloadURL(r, file.Checksum, org),
		}
		segmentFiles[segment] = append(segmentFiles[segment], entry)
	}
	for _, segment := range legacyCookbookSegments {
		response[segment] = nonNilCookbookFileEntries(segmentFiles[segment])
	}
	return response
}

func (s *server) renderCookbookVersionWriteResponse(r *http.Request, version bootstrap.CookbookVersion) map[string]any {
	response := map[string]any{
		"name":          version.Name,
		"cookbook_name": version.CookbookName,
		"version":       version.Version,
		"json_class":    version.JSONClass,
		"chef_type":     version.ChefType,
		"frozen?":       version.Frozen,
		"metadata":      cloneResponseMap(version.Metadata),
	}

	if requestedCookbookAPIVersion(r) >= 2 {
		response["all_files"] = s.renderCookbookFiles(r, "", version.AllFiles, true, false)
		return response
	}

	segmentFiles := make(map[string][]map[string]any)
	for _, segment := range legacyCookbookSegments {
		segmentFiles[segment] = nil
	}
	for _, file := range version.AllFiles {
		segment := cookbookFileSegment(file.Path)
		entry := map[string]any{
			"name":        cookbookLegacyFileName(segment, file.Path),
			"path":        file.Path,
			"checksum":    file.Checksum,
			"specificity": file.Specificity,
		}
		segmentFiles[segment] = append(segmentFiles[segment], entry)
	}
	response["recipes"] = nonNilCookbookFileEntries(segmentFiles["recipes"])
	for _, segment := range legacyCookbookSegments {
		if segment == "recipes" || len(segmentFiles[segment]) == 0 {
			continue
		}
		response[segment] = segmentFiles[segment]
	}
	return response
}

func (s *server) renderCookbookFiles(r *http.Request, org string, files []bootstrap.CookbookFile, useFullName, includeURL bool) []map[string]any {
	if len(files) == 0 {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(files))
	for _, file := range files {
		name := file.Name
		if useFullName {
			name = file.Path
		}
		entry := map[string]any{
			"name":        name,
			"path":        file.Path,
			"checksum":    file.Checksum,
			"specificity": file.Specificity,
		}
		if includeURL {
			entry["url"] = s.blobDownloadURL(r, file.Checksum, org)
		}
		out = append(out, entry)
	}
	return out
}

func nonNilCookbookFileEntries(entries []map[string]any) []map[string]any {
	if len(entries) == 0 {
		return []map[string]any{}
	}
	return entries
}

func requestedCookbookAPIVersion(r *http.Request) int {
	value := strings.TrimSpace(r.Header.Get("X-Ops-Server-API-Version"))
	if value == "" {
		return 0
	}
	version, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return version
}

func cookbookFileSegment(path string) string {
	if !strings.Contains(path, "/") {
		return "root_files"
	}
	segment, _, _ := strings.Cut(path, "/")
	for _, allowed := range legacyCookbookSegments {
		if segment == allowed {
			return segment
		}
	}
	return "files"
}

func cookbookLegacyFileName(segment, path string) string {
	if segment == "root_files" {
		return path
	}
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}

func cookbookRecipeNames(cookbookName string, metadata map[string]any) []string {
	rawRecipes, ok := metadata["recipes"].(map[string]any)
	if !ok || len(rawRecipes) == 0 {
		return nil
	}

	out := make([]string, 0, len(rawRecipes))
	for recipeName := range rawRecipes {
		out = append(out, cookbookName+"::"+strings.TrimSpace(strings.TrimPrefix(recipeName, cookbookName+"::")))
	}
	sort.Strings(out)
	return out
}

func parseCookbookNumVersions(r *http.Request) (int, bool, bool, bool) {
	value := strings.TrimSpace(r.URL.Query().Get("num_versions"))
	if value == "" {
		if _, ok := r.URL.Query()["num_versions"]; ok {
			return 0, false, true, false
		}
		return 0, false, false, true
	}
	if value == "all" {
		return 0, true, true, true
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, false, true, false
	}
	return parsed, false, true, true
}

func writeCookbookVersionError(w http.ResponseWriter, err error, name, version string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": validationErr.Messages})
	case errors.Is(err, bootstrap.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": cookbookVersionNotFound(name, version)})
	default:
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "cookbook_failed",
			Message: "internal cookbook compatibility error",
		})
	}
	return false
}

func validCookbookName(name string) bool {
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	for _, ch := range name {
		switch {
		case ch >= 'A' && ch <= 'Z':
		case ch >= 'a' && ch <= 'z':
		case ch >= '0' && ch <= '9':
		case ch == '_', ch == '.', ch == '-':
		default:
			return false
		}
	}
	return true
}

func validCookbookVersionPath(version string, allowLatest bool) bool {
	version = strings.TrimSpace(version)
	if allowLatest && (version == "_latest" || version == "latest") {
		return true
	}
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseInt(part, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func invalidCookbookNameMessage(name string) string {
	return fmt.Sprintf("Invalid cookbook name '%s' using regex: 'Malformed cookbook name. Must only contain A-Z, a-z, 0-9, _, . or -'.", name)
}

func invalidCookbookVersionMessage(version string) string {
	return fmt.Sprintf("Invalid cookbook version '%s'.", version)
}

func (s *server) writeCookbookArtifactError(w http.ResponseWriter, err error, name, identifier string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": validationErr.Messages})
	case errors.Is(err, bootstrap.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]any{"error": "Cookbook artifact already exists"})
	case errors.Is(err, bootstrap.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]any{"error": []string{"not_found"}})
	default:
		s.logf("cookbook artifact compatibility failure for %s/%s: %v", name, identifier, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "cookbook_artifact_failed",
			Message: "internal cookbook artifact compatibility error",
		})
	}
	return false
}

func cookbookNotFound(name string) []string {
	return []string{fmt.Sprintf("Cannot find a cookbook named %s", name)}
}

func cookbookVersionNotFound(name, version string) []string {
	return []string{fmt.Sprintf("Cannot find a cookbook named %s with version %s", name, version)}
}

func cookbookCollectionBasePath(r *http.Request, org string) string {
	if strings.TrimSpace(r.PathValue("org")) != "" {
		return "/organizations/" + org + "/cookbooks"
	}
	return "/cookbooks"
}
