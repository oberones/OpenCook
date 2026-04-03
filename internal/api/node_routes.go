package api

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleNodes(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveNodeRoute(w, r)
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
		s.handleNodeGet(w, r, state, org, basePath)
	case http.MethodHead:
		s.handleNodeHead(w, r, state, org, basePath)
	case http.MethodPost:
		s.handleNodePost(w, r, state, org, basePath)
	case http.MethodPut:
		s.handleNodePut(w, r, state, org, basePath)
	case http.MethodDelete:
		s.handleNodeDelete(w, r, state, org, basePath)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "node method is not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleNodeGet(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "nodes",
			Organization: org,
		}) {
			return
		}

		nodes, _ := state.ListNodes(org)
		if nodes == nil {
			nodes = map[string]string{}
		}
		response := make(map[string]string, len(nodes))
		for name := range nodes {
			response[name] = nodeURLForResponse(org, name, basePath)
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

	node, orgExists, nodeExists := state.GetNode(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !nodeExists {
		writeNodeMessages(w, http.StatusNotFound, fmt.Sprintf("node '%s' not found", name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "node",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	writeJSON(w, http.StatusOK, node)
}

func (s *server) handleNodeHead(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "nodes",
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

	_, orgExists, nodeExists := state.GetNode(org, name)
	if !orgExists || !nodeExists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "node",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *server) handleNodePost(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
		Type:         "container",
		Name:         "nodes",
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	requestor, _ := requestorFromContext(r.Context())
	node, err := state.CreateNode(org, bootstrap.CreateNodeInput{
		Payload: payload,
		Creator: requestor,
	})
	if !s.writeNodeError(w, err) {
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"uri": nodeURLForResponse(org, node.Name, basePath),
	})
}

func (s *server) handleNodePut(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	_, orgExists, nodeExists := state.GetNode(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !nodeExists {
		writeNodeMessages(w, http.StatusNotFound, fmt.Sprintf("node '%s' not found", name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
		Type:         "node",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	node, err := state.UpdateNode(org, name, bootstrap.UpdateNodeInput{Payload: payload})
	if !s.writeNodeError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, node)
}

func (s *server) handleNodeDelete(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	_, orgExists, nodeExists := state.GetNode(org, name)
	if !orgExists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}
	if !nodeExists {
		writeNodeMessages(w, http.StatusNotFound, fmt.Sprintf("node '%s' not found", name))
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
		Type:         "node",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	node, err := state.DeleteNode(org, name)
	if !s.writeNodeError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, node)
}

func (s *server) authnOrganization(r *http.Request) string {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org
	}
	if !isDefaultOrgScopedPath(r.URL.Path) {
		return ""
	}

	org, _ = s.resolveDefaultOrganizationName()
	return org
}

func (s *server) resolveNodeRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/nodes", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/nodes", true
}

func (s *server) resolveDefaultOrganizationName() (string, bool) {
	if org := strings.TrimSpace(s.deps.Config.DefaultOrganization); org != "" {
		return org, true
	}
	if s.deps.Bootstrap == nil {
		return "", false
	}

	orgs := s.deps.Bootstrap.ListOrganizations()
	if len(orgs) != 1 {
		return "", false
	}

	for name := range orgs {
		return name, true
	}
	return "", false
}

func (s *server) writeNodeError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writeNodeMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writeNodeMessages(w, http.StatusConflict, "Node already exists")
	case errors.Is(err, bootstrap.ErrNotFound):
		writeNodeMessages(w, http.StatusNotFound, "node not found")
	default:
		s.logf("node compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "node_failed",
			Message: "internal node compatibility error",
		})
	}
	return false
}

func writeNodeMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}

func nodeURLForResponse(org, name, basePath string) string {
	if basePath == "/nodes" {
		return "/nodes/" + name
	}
	return "/organizations/" + org + "/nodes/" + name
}

func isDefaultOrgNodePath(path string) bool {
	return path == "/nodes" || path == "/nodes/" || strings.HasPrefix(path, "/nodes/")
}

func isDefaultOrgEnvironmentPath(path string) bool {
	return path == "/environments" || path == "/environments/" || strings.HasPrefix(path, "/environments/")
}

func isDefaultOrgRolePath(path string) bool {
	return path == "/roles" || path == "/roles/" || strings.HasPrefix(path, "/roles/")
}

func isDefaultOrgClientPath(path string) bool {
	return path == "/clients" || path == "/clients/" || strings.HasPrefix(path, "/clients/")
}

func isDefaultOrgSearchPath(path string) bool {
	return path == "/search" || path == "/search/" || strings.HasPrefix(path, "/search/")
}

func isDefaultOrgDataPath(path string) bool {
	return path == "/data" || path == "/data/" || strings.HasPrefix(path, "/data/")
}

func isDefaultOrgScopedPath(path string) bool {
	return isDefaultOrgNodePath(path) ||
		isDefaultOrgEnvironmentPath(path) ||
		isDefaultOrgRolePath(path) ||
		isDefaultOrgClientPath(path) ||
		isDefaultOrgSearchPath(path) ||
		isDefaultOrgDataPath(path)
}
