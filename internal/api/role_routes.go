package api

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleRoles(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveRoleRoute(w, r)
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

	switch r.Method {
	case http.MethodGet:
		s.handleRoleGet(w, r, state, org, basePath)
	case http.MethodHead:
		s.handleRoleHead(w, r, state, org, basePath)
	case http.MethodPost:
		s.handleRolePost(w, r, state, org, basePath)
	case http.MethodPut:
		s.handleRolePut(w, r, state, org, basePath)
	case http.MethodDelete:
		s.handleRoleDelete(w, r, state, org, basePath)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed for roles route",
		})
	}
}

func (s *server) handleRoleEnvironments(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, rolesBasePath, ok := s.resolveRoleRoute(w, r)
	if !ok {
		return
	}

	roleName := r.PathValue("name")
	envsBasePath := rolesBasePath + "/" + roleName + "/environments"
	isCollection := matchesCollectionPath(r.URL.Path, envsBasePath)
	envName := ""
	if !isCollection {
		var ok bool
		envName, ok = pathTail(r.URL.Path, envsBasePath+"/")
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w, "method not allowed for role environments route", http.MethodGet)
		return
	}

	role, orgExists, roleExists := state.GetRole(org, roleName)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !roleExists {
		writeRoleMessages(w, http.StatusNotFound, cannotLoadRoleMessage(roleName))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "role",
		Name:         roleName,
		Organization: org,
	}) {
		return
	}

	if isCollection {
		writeJSON(w, http.StatusOK, roleEnvironmentNames(role))
		return
	}

	if envName == "_default" {
		writeJSON(w, http.StatusOK, map[string]any{
			"run_list": role.RunList,
		})
		return
	}

	_, _, envExists := state.GetEnvironment(org, envName)
	if !envExists {
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(envName))
		return
	}

	runList, ok := role.EnvRunLists[envName]
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"run_list": nil,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run_list": runList,
	})
}

func (s *server) handleRoleGet(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "roles",
			Organization: org,
		}) {
			return
		}

		roles, _ := state.ListRoles(org)
		if roles == nil {
			roles = map[string]string{}
		}
		response := make(map[string]string, len(roles))
		for name := range roles {
			response[name] = roleURLForResponse(org, name, basePath)
		}
		writeJSON(w, http.StatusOK, response)
		return
	}

	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	role, orgExists, roleExists := state.GetRole(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !roleExists {
		writeRoleMessages(w, http.StatusNotFound, cannotLoadRoleMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	writeJSON(w, http.StatusOK, role)
}

func (s *server) handleRoleHead(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "roles",
			Organization: org,
		}) {
			return
		}

		w.WriteHeader(http.StatusOK)
		return
	}

	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_, orgExists, roleExists := state.GetRole(org, name)
	if !orgExists || !roleExists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *server) handleRolePost(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
		Type:         "container",
		Name:         "roles",
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	requestor, _ := requestorFromContext(r.Context())
	role, err := state.CreateRole(org, bootstrap.CreateRoleInput{
		Payload: payload,
		Creator: requestor,
	})
	if !s.writeRoleError(w, err, role.Name) {
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"uri": roleURLForResponse(org, role.Name, basePath),
	})
}

func (s *server) handleRolePut(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed on roles collection",
		})
		return
	}

	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	_, orgExists, roleExists := state.GetRole(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !roleExists {
		writeRoleMessages(w, http.StatusNotFound, cannotLoadRoleMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	role, err := state.UpdateRole(org, name, bootstrap.UpdateRoleInput{Payload: payload})
	if !s.writeRoleError(w, err, name) {
		return
	}

	writeJSON(w, http.StatusOK, role)
}

func (s *server) handleRoleDelete(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed on roles collection",
		})
		return
	}

	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	_, orgExists, roleExists := state.GetRole(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !roleExists {
		writeRoleMessages(w, http.StatusNotFound, cannotLoadRoleMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
		Type:         "role",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	role, err := state.DeleteRole(org, name)
	if !s.writeRoleError(w, err, name) {
		return
	}

	writeJSON(w, http.StatusOK, role)
}

func (s *server) resolveRoleRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/roles", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/roles", true
}

func (s *server) writeRoleError(w http.ResponseWriter, err error, name string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeRoleMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writeRoleMessages(w, http.StatusConflict, "Role already exists")
	case errors.Is(err, bootstrap.ErrNotFound):
		writeRoleMessages(w, http.StatusNotFound, cannotLoadRoleMessage(name))
	default:
		s.logf("role compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "role_failed",
			Message: "internal role compatibility error",
		})
	}
	return false
}

func writeRoleMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}

func roleURLForResponse(org, name, basePath string) string {
	if basePath == "/roles" {
		return "/roles/" + name
	}
	return "/organizations/" + org + "/roles/" + name
}

func cannotLoadRoleMessage(name string) string {
	return fmt.Sprintf("Cannot load role %s", name)
}

func roleEnvironmentNames(role bootstrap.Role) []string {
	names := make([]string, 0, len(role.EnvRunLists)+1)
	names = append(names, "_default")
	for envName := range role.EnvRunLists {
		if envName == "_default" {
			continue
		}
		names = append(names, envName)
	}
	sort.Strings(names[1:])
	return names
}
