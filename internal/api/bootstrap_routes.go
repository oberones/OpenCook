package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleOrganizations(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	switch r.Method {
	case http.MethodGet:
		if matchesCollectionPath(r.URL.Path, "/organizations") {
			if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{Type: "organizations"}) {
				return
			}

			writeJSON(w, http.StatusOK, state.ListOrganizations())
			return
		}

		name, ok := pathTail(r.URL.Path, "/organizations/")
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		org, exists := state.GetOrganization(name)
		if !exists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "organization",
			Organization: name,
		}) {
			return
		}

		writeJSON(w, http.StatusOK, org)
	case http.MethodPost:
		if !matchesCollectionPath(r.URL.Path, "/organizations") {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{Type: "organizations"}) {
			return
		}

		var payload struct {
			Name     string `json:"name"`
			FullName string `json:"full_name"`
			OrgType  string `json:"org_type"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}

		requestor, _ := requestorFromContext(r.Context())
		org, validator, keyMaterial, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
			Name:      payload.Name,
			FullName:  payload.FullName,
			OrgType:   payload.OrgType,
			OwnerName: requestor.Name,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}

		writeJSON(w, http.StatusCreated, map[string]any{
			"uri":         "/organizations/" + org.Name,
			"clientname":  validator.Name,
			"private_key": keyMaterial.PrivateKeyPEM,
		})
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "organization write flows beyond bootstrap creation are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleOrgGroups(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	org := r.PathValue("org")
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}
	if _, ok := state.GetOrganization(org); !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	basePath := "/organizations/" + org + "/groups"
	switch r.Method {
	case http.MethodGet:
		if matchesCollectionPath(r.URL.Path, basePath) {
			if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
				Type:         "container",
				Name:         "groups",
				Organization: org,
			}) {
				return
			}
			groups, _ := state.ListGroups(org)
			writeJSON(w, http.StatusOK, groups)
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

		group, exists := state.GetGroup(org, name)
		if !exists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "group not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "group",
			Name:         name,
			Organization: org,
		}) {
			return
		}

		writeJSON(w, http.StatusOK, group)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "group write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleOrgContainers(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	org := r.PathValue("org")
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}
	if _, ok := state.GetOrganization(org); !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	basePath := "/organizations/" + org + "/containers"
	switch r.Method {
	case http.MethodGet:
		if matchesCollectionPath(r.URL.Path, basePath) {
			if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
				Type:         "container",
				Name:         "containers",
				Organization: org,
			}) {
				return
			}
			containers, _ := state.ListContainers(org)
			writeJSON(w, http.StatusOK, containers)
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

		container, exists := state.GetContainer(org, name)
		if !exists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "container not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         name,
			Organization: org,
		}) {
			return
		}

		writeJSON(w, http.StatusOK, container)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "container write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleUserACL(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !s.authorizeUserRead(w, r, name) {
		return
	}
	s.writeACLResponse(w, r, authz.Resource{Type: "user", Name: name})
}

func (s *server) handleOrgACL(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "organization",
		Organization: org,
	}) {
		return
	}
	s.writeACLResponse(w, r, authz.Resource{Type: "organization", Organization: org})
}

func (s *server) handleOrgGroupACL(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	name := r.PathValue("name")
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "group",
		Name:         name,
		Organization: org,
	}) {
		return
	}
	s.writeACLResponse(w, r, authz.Resource{Type: "group", Name: name, Organization: org})
}

func (s *server) handleOrgContainerACL(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	name := r.PathValue("name")
	if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "container",
		Name:         name,
		Organization: org,
	}) {
		return
	}
	s.writeACLResponse(w, r, authz.Resource{Type: "container", Name: name, Organization: org})
}

func (s *server) handleOrgClientACL(w http.ResponseWriter, r *http.Request) {
	org := r.PathValue("org")
	name := r.PathValue("name")
	if !s.authorizeClientRead(w, r, org, name) {
		return
	}
	s.writeACLResponse(w, r, authz.Resource{Type: "client", Name: name, Organization: org})
}

func (s *server) writeACLResponse(w http.ResponseWriter, r *http.Request, resource authz.Resource) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "ACL write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
		return
	}

	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	acl, ok, err := s.deps.Bootstrap.ResolveACL(r.Context(), resource)
	if err != nil {
		s.logf("resolve ACL failed for %s %s: %v", resource.Type, resource.Name, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authz_failed",
			Message: "internal authorization error",
		})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "ACL not found",
		})
		return
	}

	writeJSON(w, http.StatusOK, acl)
}

func (s *server) authorizeRequest(w http.ResponseWriter, r *http.Request, action authz.Action, resource authz.Resource) bool {
	requestor, ok := requestorFromContext(r.Context())
	if !ok {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authn_context_missing",
			Message: "authenticated requestor missing from context",
		})
		return false
	}

	allowed, err := s.authorizeRequestor(r.Context(), requestor, action, resource)
	if err != nil {
		s.logf("authz failure for %s %s/%s: %v", action, resource.Type, resource.Name, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "authz_failed",
			Message: "internal authorization error",
		})
		return false
	}
	if !allowed {
		routeID, _ := routeIDFromContext(r.Context())
		subjectOrg := requestor.Organization
		if subjectOrg == "" {
			subjectOrg = resource.Organization
		}
		s.logf(
			"authz denied route=%q method=%q path=%q requestor_type=%q requestor=%q requestor_org=%q subject_org=%q action=%q resource_type=%q resource_name=%q resource_org=%q",
			routeID,
			r.Method,
			r.URL.Path,
			requestor.Type,
			requestor.Name,
			requestor.Organization,
			subjectOrg,
			action,
			resource.Type,
			resource.Name,
			resource.Organization,
		)
		writeJSON(w, http.StatusForbidden, apiError{
			Error:   "forbidden",
			Message: "requestor is not authorized for this resource",
		})
		return false
	}
	return true
}

func (s *server) authorizeRequestor(ctx context.Context, requestor authn.Principal, action authz.Action, resource authz.Resource) (bool, error) {
	subjectOrg := requestor.Organization
	if subjectOrg == "" {
		subjectOrg = resource.Organization
	}

	decision, err := s.deps.Authz.Authorize(ctx, authz.Subject{
		Type:         requestor.Type,
		Name:         requestor.Name,
		Organization: subjectOrg,
	}, action, resource)
	if err != nil {
		return false, err
	}
	return decision.Allowed, nil
}

func (s *server) authorizeUserRead(w http.ResponseWriter, r *http.Request, name string) bool {
	requestor, _ := requestorFromContext(r.Context())
	if requestor.Type == "user" && requestor.Name == name {
		return true
	}
	return s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{Type: "user", Name: name})
}

func (s *server) authorizeClientRead(w http.ResponseWriter, r *http.Request, org, name string) bool {
	requestor, _ := requestorFromContext(r.Context())
	if requestor.Type == "client" && requestor.Name == name && requestor.Organization == org {
		return true
	}
	return s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
		Type:         "client",
		Name:         name,
		Organization: org,
	})
}

func (s *server) writeBootstrapError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}

	switch {
	case errors.Is(err, bootstrap.ErrConflict):
		writeJSON(w, http.StatusConflict, apiError{
			Error:   "conflict",
			Message: err.Error(),
		})
	case errors.Is(err, bootstrap.ErrInvalidInput):
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_request",
			Message: err.Error(),
		})
	case errors.Is(err, bootstrap.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: err.Error(),
		})
	default:
		s.logf("bootstrap state failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_failed",
			Message: "internal bootstrap state error",
		})
	}
	return false
}

func pathTail(path, prefix string) (string, bool) {
	tail := strings.TrimPrefix(path, prefix)
	tail = strings.TrimSuffix(tail, "/")
	if tail == "" || strings.Contains(tail, "/") {
		return "", false
	}
	return tail, true
}
