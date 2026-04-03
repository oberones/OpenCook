package api

import (
	"errors"
	"net/http"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handlePolicies(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
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

	segments, ok := policyRouteSegments(r.URL.Path, "/policies")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	switch len(segments) {
	case 0:
		s.handlePolicyCollection(w, r, state, org)
	case 1:
		s.handleNamedPolicy(w, r, state, org, segments[0])
	case 2:
		if segments[1] != "revisions" {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		s.handlePolicyRevisionsCollection(w, r, state, org, segments[0])
	case 3:
		if segments[1] != "revisions" {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		s.handleNamedPolicyRevision(w, r, state, org, segments[0], segments[2])
	default:
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
	}
}

func (s *server) handlePolicyGroups(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
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

	segments, ok := policyRouteSegments(r.URL.Path, "/policy_groups")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	switch len(segments) {
	case 0:
		s.handlePolicyGroupCollection(w, r, state, org)
	case 1:
		s.handleNamedPolicyGroup(w, r, state, org, segments[0])
	case 3:
		if segments[1] != "policies" {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		s.handlePolicyGroupAssignment(w, r, state, org, segments[0], segments[2])
	default:
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
	}
}

func (s *server) handlePolicyCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org string) {
	switch r.Method {
	case http.MethodGet:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "policies",
			Organization: org,
		}) {
			return
		}
		policies, _ := state.ListPolicies(org)
		if policies == nil {
			policies = map[string][]string{}
		}
		writeJSON(w, http.StatusOK, policyListResponse(policies))
	case http.MethodHead:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "policies",
			Organization: org,
		}) {
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		writeMethodNotAllowed(w, "method not allowed for policies route", http.MethodGet, http.MethodHead)
	}
}

func (s *server) handleNamedPolicy(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, policyName string) {
	switch r.Method {
	case http.MethodGet:
		revisions, orgExists, policyExists := state.GetPolicy(org, policyName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !policyExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "policy",
			Name:         policyName,
			Organization: org,
		}) {
			return
		}
		writeJSON(w, http.StatusOK, namedPolicyResponse(revisions))
	case http.MethodDelete:
		if _, orgExists, policyExists := state.GetPolicy(org, policyName); !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		} else if !policyExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "policy",
			Name:         policyName,
			Organization: org,
		}) {
			return
		}
		revisions, err := state.DeletePolicy(org, policyName)
		if !s.writePolicyError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, namedPolicyRevisionIDResponse(revisions))
	case http.MethodHead:
		_, orgExists, policyExists := state.GetPolicy(org, policyName)
		if !orgExists || !policyExists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "policy",
			Name:         policyName,
			Organization: org,
		}) {
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		writeMethodNotAllowed(w, "method not allowed for policy route", http.MethodGet, http.MethodDelete, http.MethodHead)
	}
}

func (s *server) handlePolicyRevisionsCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, policyName string) {
	switch r.Method {
	case http.MethodPost:
		if _, _, exists := state.GetPolicy(org, policyName); exists {
			if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
				Type:         "policy",
				Name:         policyName,
				Organization: org,
			}) {
				return
			}
		} else {
			if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
				Type:         "container",
				Name:         "policies",
				Organization: org,
			}) {
				return
			}
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		requestor, _ := requestorFromContext(r.Context())
		revision, err := state.CreatePolicyRevision(org, policyName, bootstrap.CreatePolicyRevisionInput{
			Payload: payload,
			Creator: requestor,
		})
		if !s.writePolicyError(w, err) {
			return
		}
		writeJSON(w, http.StatusCreated, revision.Payload)
	default:
		writeMethodNotAllowed(w, "method not allowed for policy revisions route", http.MethodPost)
	}
}

func (s *server) handleNamedPolicyRevision(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, policyName, revisionID string) {
	switch r.Method {
	case http.MethodGet:
		revision, orgExists, policyExists, revisionExists := state.GetPolicyRevision(org, policyName, revisionID)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !policyExists || !revisionExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy revision not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "policy",
			Name:         policyName,
			Organization: org,
		}) {
			return
		}
		groups, _, _, _ := state.PolicyGroupsForRevision(org, policyName, revisionID)
		writeJSON(w, http.StatusOK, policyRevisionResponse(revision, groups))
	case http.MethodDelete:
		if _, _, exists := state.GetPolicy(org, policyName); !exists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "policy",
			Name:         policyName,
			Organization: org,
		}) {
			return
		}
		revision, err := state.DeletePolicyRevision(org, policyName, revisionID)
		if !s.writePolicyError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, revision.Payload)
	default:
		writeMethodNotAllowed(w, "method not allowed for policy revision route", http.MethodGet, http.MethodDelete)
	}
}

func (s *server) handlePolicyGroupCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org string) {
	switch r.Method {
	case http.MethodGet:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "policy_groups",
			Organization: org,
		}) {
			return
		}
		groups, _ := state.ListPolicyGroups(org)
		if groups == nil {
			groups = map[string]bootstrap.PolicyGroup{}
		}
		writeJSON(w, http.StatusOK, policyGroupListResponse(groups))
	case http.MethodHead:
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "policy_groups",
			Organization: org,
		}) {
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		writeMethodNotAllowed(w, "method not allowed for policy groups route", http.MethodGet, http.MethodHead)
	}
}

func (s *server) handleNamedPolicyGroup(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, groupName string) {
	switch r.Method {
	case http.MethodGet:
		group, orgExists, groupExists := state.GetPolicyGroup(org, groupName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !groupExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy group not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "policy_group",
			Name:         groupName,
			Organization: org,
		}) {
			return
		}
		writeJSON(w, http.StatusOK, namedPolicyGroupResponse(group))
	case http.MethodDelete:
		if _, orgExists, groupExists := state.GetPolicyGroup(org, groupName); !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		} else if !groupExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy group not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "policy_group",
			Name:         groupName,
			Organization: org,
		}) {
			return
		}
		group, err := state.DeletePolicyGroup(org, groupName)
		if !s.writePolicyError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, namedPolicyGroupResponse(group))
	default:
		writeMethodNotAllowed(w, "method not allowed for policy group route", http.MethodGet, http.MethodDelete)
	}
}

func (s *server) handlePolicyGroupAssignment(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, groupName, policyName string) {
	switch r.Method {
	case http.MethodGet:
		revision, orgExists, groupExists, assignmentExists := state.GetPolicyGroupAssignment(org, groupName, policyName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !groupExists || !assignmentExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy group assignment not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "policy_group",
			Name:         groupName,
			Organization: org,
		}) {
			return
		}
		writeJSON(w, http.StatusOK, revision.Payload)
	case http.MethodPut:
		if _, _, exists := state.GetPolicyGroup(org, groupName); exists {
			if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
				Type:         "policy_group",
				Name:         groupName,
				Organization: org,
			}) {
				return
			}
		} else {
			if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
				Type:         "container",
				Name:         "policy_groups",
				Organization: org,
			}) {
				return
			}
		}

		var payload map[string]any
		if !decodeJSON(w, r, &payload) {
			return
		}

		requestor, _ := requestorFromContext(r.Context())
		revision, created, err := state.UpsertPolicyGroupAssignment(org, groupName, policyName, bootstrap.UpdatePolicyGroupAssignmentInput{
			Payload: payload,
			Creator: requestor,
		})
		if !s.writePolicyError(w, err) {
			return
		}

		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeJSON(w, status, revision.Payload)
	case http.MethodDelete:
		if _, _, exists := state.GetPolicyGroup(org, groupName); !exists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "policy group not found",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
			Type:         "policy_group",
			Name:         groupName,
			Organization: org,
		}) {
			return
		}
		revision, err := state.DeletePolicyGroupAssignment(org, groupName, policyName)
		if !s.writePolicyError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, revision.Payload)
	default:
		writeMethodNotAllowed(w, "method not allowed for policy group assignment route", http.MethodGet, http.MethodPut, http.MethodDelete)
	}
}

func policyRouteSegments(path, basePath string) ([]string, bool) {
	if path == basePath || path == basePath+"/" {
		return []string{}, true
	}
	if !strings.HasPrefix(path, basePath+"/") {
		return nil, false
	}

	tail := strings.TrimPrefix(path, basePath+"/")
	tail = strings.TrimSuffix(tail, "/")
	if tail == "" {
		return []string{}, true
	}
	parts := strings.Split(tail, "/")
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			return nil, false
		}
	}
	return parts, true
}

func policyListResponse(policies map[string][]string) map[string]any {
	response := make(map[string]any, len(policies))
	names := make([]string, 0, len(policies))
	for name := range policies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		response[name] = map[string]any{
			"uri":       "/policies/" + name,
			"revisions": namedPolicyRevisionIDResponse(policies[name])["revisions"],
		}
	}
	return response
}

func namedPolicyResponse(revisions map[string]bootstrap.PolicyRevision) map[string]any {
	revisionIDs := make([]string, 0, len(revisions))
	for revisionID := range revisions {
		revisionIDs = append(revisionIDs, revisionID)
	}
	sort.Strings(revisionIDs)
	return namedPolicyRevisionIDResponse(revisionIDs)
}

func namedPolicyRevisionIDResponse(revisionIDs []string) map[string]any {
	revisions := make(map[string]any, len(revisionIDs))
	for _, revisionID := range revisionIDs {
		revisions[revisionID] = map[string]any{}
	}
	return map[string]any{
		"revisions": revisions,
	}
}

func policyRevisionResponse(revision bootstrap.PolicyRevision, groups []string) map[string]any {
	response := cloneResponseMap(revision.Payload)
	response["policy_group_list"] = stringSliceToAny(groups)
	return response
}

func policyGroupListResponse(groups map[string]bootstrap.PolicyGroup) map[string]any {
	response := make(map[string]any, len(groups))
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		group := groups[name]
		entry := map[string]any{
			"uri": "/policy_groups/" + name,
		}
		if len(group.Policies) > 0 {
			entry["policies"] = policyGroupPoliciesResponse(group.Policies)
		}
		response[name] = entry
	}
	return response
}

func namedPolicyGroupResponse(group bootstrap.PolicyGroup) map[string]any {
	return map[string]any{
		"uri":      "/policy_groups/" + group.Name,
		"policies": policyGroupPoliciesResponse(group.Policies),
	}
}

func policyGroupPoliciesResponse(policies map[string]string) map[string]any {
	response := make(map[string]any, len(policies))
	names := make([]string, 0, len(policies))
	for name := range policies {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		response[name] = map[string]any{
			"revision_id": policies[name],
		}
	}
	return response
}

func stringSliceToAny(in []string) []any {
	if len(in) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(in))
	for _, item := range in {
		out = append(out, item)
	}
	return out
}

func (s *server) writePolicyError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}

	var validationErr *bootstrap.ValidationError
	switch {
	case errors.As(err, &validationErr):
		writePolicyMessages(w, http.StatusBadRequest, validationErr.Messages...)
	case errors.Is(err, bootstrap.ErrConflict):
		writePolicyMessages(w, http.StatusConflict, "policy revision already exists")
	case errors.Is(err, bootstrap.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "policy resource not found",
		})
	default:
		s.logf("policy compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "policy_failed",
			Message: "internal policy compatibility error",
		})
	}
	return false
}

func writePolicyMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}
