package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleEnvironments(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveEnvironmentRoute(w, r)
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
		s.handleEnvironmentGet(w, r, state, org, basePath)
	case http.MethodHead:
		s.handleEnvironmentHead(w, r, state, org, basePath)
	case http.MethodPost:
		s.handleEnvironmentPost(w, r, state, org, basePath)
	case http.MethodPut:
		s.handleEnvironmentPut(w, r, state, org, basePath)
	case http.MethodDelete:
		s.handleEnvironmentDelete(w, r, state, org, basePath)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed for environments route",
		})
	}
}

func (s *server) handleEnvironmentNodes(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, envBasePath, ok := s.resolveEnvironmentRoute(w, r)
	if !ok {
		return
	}
	envName := r.PathValue("name")
	nodesPath := envBasePath + "/" + envName + "/nodes"

	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	switch r.Method {
	case http.MethodGet:
		if !matchesCollectionPath(r.URL.Path, nodesPath) {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		nodes, orgExists, envExists := state.ListEnvironmentNodes(org, envName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !envExists {
			writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(envName))
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "nodes",
			Organization: org,
		}) {
			return
		}

		response := make(map[string]string, len(nodes))
		for name := range nodes {
			response[name] = nodeURLForEnvironmentResponse(org, name, envBasePath)
		}
		writeJSON(w, http.StatusOK, response)
	case http.MethodHead:
		if !matchesCollectionPath(r.URL.Path, nodesPath) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, orgExists, envExists := state.ListEnvironmentNodes(org, envName)
		if !orgExists || !envExists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "nodes",
			Organization: org,
		}) {
			return
		}

		w.WriteHeader(http.StatusOK)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed for environment nodes route",
		})
	}
}

func (s *server) handleEnvironmentGet(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "environments",
			Organization: org,
		}) {
			return
		}

		environments, _ := state.ListEnvironments(org)
		if environments == nil {
			environments = map[string]string{}
		}
		response := make(map[string]string, len(environments))
		for name := range environments {
			response[name] = environmentURLForResponse(org, name, basePath)
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

	env, orgExists, envExists := state.GetEnvironment(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !envExists {
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	writeJSON(w, http.StatusOK, env)
}

func (s *server) handleEnvironmentHead(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "environments",
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

	_, orgExists, envExists := state.GetEnvironment(org, name)
	if !orgExists || !envExists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *server) handleEnvironmentPost(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
		Type:         "container",
		Name:         "environments",
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	requestor, _ := requestorFromContext(r.Context())
	env, err := state.CreateEnvironment(org, bootstrap.CreateEnvironmentInput{
		Payload: payload,
		Creator: requestor,
	})
	if !s.writeEnvironmentError(w, err, env.Name) {
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"uri": environmentURLForResponse(org, env.Name, basePath),
	})
}

func (s *server) handleEnvironmentPut(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed on environments collection",
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

	_, orgExists, envExists := state.GetEnvironment(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !envExists {
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	result, err := state.UpdateEnvironment(org, name, bootstrap.UpdateEnvironmentInput{Payload: payload})
	if !s.writeEnvironmentError(w, err, name) {
		return
	}

	status := http.StatusOK
	if result.Renamed {
		status = http.StatusCreated
		w.Header().Set("Location", environmentURLForResponse(org, result.Environment.Name, basePath))
	}
	writeJSON(w, status, result.Environment)
}

func (s *server) handleEnvironmentDelete(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method not allowed on environments collection",
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

	_, orgExists, envExists := state.GetEnvironment(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !envExists {
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
		Type:         "environment",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	env, err := state.DeleteEnvironment(org, name)
	if !s.writeEnvironmentError(w, err, name) {
		return
	}

	writeJSON(w, http.StatusOK, env)
}

func (s *server) resolveEnvironmentRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/environments", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/environments", true
}

func (s *server) writeEnvironmentError(w http.ResponseWriter, err error, name string) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeEnvironmentMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writeEnvironmentMessages(w, http.StatusConflict, "Environment already exists")
	case errors.Is(err, bootstrap.ErrImmutable):
		writeEnvironmentMessages(w, http.StatusMethodNotAllowed, bootstrapDefaultEnvironmentModifiedMessage())
	case errors.Is(err, bootstrap.ErrNotFound):
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(name))
	default:
		s.logf("environment compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "environment_failed",
			Message: "internal environment compatibility error",
		})
	}
	return false
}

func writeEnvironmentMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}

func environmentURLForResponse(org, name, basePath string) string {
	if basePath == "/environments" {
		return "/environments/" + name
	}
	return "/organizations/" + org + "/environments/" + name
}

func nodeURLForEnvironmentResponse(org, name, environmentBasePath string) string {
	if environmentBasePath == "/environments" {
		return "/nodes/" + name
	}
	return "/organizations/" + org + "/nodes/" + name
}

func cannotLoadEnvironmentMessage(name string) string {
	return fmt.Sprintf("Cannot load environment %s", name)
}

func bootstrapDefaultEnvironmentModifiedMessage() string {
	return "The '_default' environment cannot be modified."
}
