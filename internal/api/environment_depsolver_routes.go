package api

import (
	"errors"
	"net/http"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleEnvironmentCookbookVersions(w http.ResponseWriter, r *http.Request) {
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
	depsolverPath := envBasePath + "/" + envName + "/cookbook_versions"

	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, "method not allowed for environment cookbook_versions route", http.MethodPost)
		return
	}
	if !matchesCollectionPath(r.URL.Path, depsolverPath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}
	if _, orgExists, envExists := state.GetEnvironment(org, envName); !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	} else if !envExists {
		writeEnvironmentMessages(w, http.StatusNotFound, cannotLoadEnvironmentMessage(envName))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "environment",
		Name:         envName,
		Organization: org,
	}) {
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         "cookbooks",
		Organization: org,
	}) {
		return
	}

	payload, ok := decodeDepsolverJSON(w, r)
	if !ok {
		return
	}

	solution, orgExists, envExists, err := state.SolveEnvironmentCookbookVersions(org, envName, payload)
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
	if err != nil {
		var validationErr *bootstrap.ValidationError
		var depsolverErr *bootstrap.DepsolverError
		switch {
		case errors.As(err, &validationErr):
			writeEnvironmentMessages(w, http.StatusBadRequest, validationErr.Messages...)
		case errors.As(err, &depsolverErr):
			writeJSON(w, http.StatusPreconditionFailed, map[string]any{
				"error": []any{depsolverErr.Detail},
			})
		default:
			s.logf("environment depsolver compatibility failure: %v", err)
			writeJSON(w, http.StatusInternalServerError, apiError{
				Error:   "environment_failed",
				Message: "internal environment compatibility error",
			})
		}
		return
	}

	response := make(map[string]any, len(solution))
	for name, version := range solution {
		response[name] = s.renderDepsolverCookbookVersionResponse(r, org, version)
	}
	writeJSON(w, http.StatusOK, response)
}

func decodeDepsolverJSON(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var payload map[string]any
	switch decodeJSONInto(r, &payload) {
	case decodeJSONOK:
		return payload, true
	default:
		writeEnvironmentMessages(w, http.StatusBadRequest, "invalid JSON")
		return nil, false
	}
}
