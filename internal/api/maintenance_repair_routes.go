package api

import (
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/maintenance"
)

const (
	internalAdminServerAdminsPath         = "/internal/admin/server-admins"
	maintenanceRepairDefaultACLsPath      = "/internal/maintenance/repair/default-acls"
	maintenanceRepairOrgMembershipPath    = "/internal/maintenance/repair/org-membership"
	maintenanceRepairGroupMembershipPath  = "/internal/maintenance/repair/group-membership"
	maintenanceRepairServerAdminsPath     = "/internal/maintenance/repair/server-admins"
	onlineRepairCacheState                = "live bootstrap service state was updated through the normal persistence seam; restart is not required for this process"
	onlineRepairVerifierCacheState        = "unchanged"
	onlineRepairMaintenanceRequiredSuffix = "active maintenance mode is required before online repair"
)

// handleInternalServerAdmins exposes the compatibility server-admin set through
// a signed admin-only route. It is read-only and does not require maintenance.
func (s *server) handleInternalServerAdmins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "server-admin listing accepts GET only",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionGrant, authz.Resource{Type: "organizations"}) {
		return
	}
	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"operation":     "server-admin-list",
		"mode":          "online",
		"server_admins": s.deps.Bootstrap.ListServerAdmins(),
		"cache_state":   "live bootstrap service state was read directly",
	})
}

// handleMaintenanceRepairDefaultACLs performs the one online repair operation
// allowed in this bucket. It deliberately requires signed superuser access,
// active maintenance mode, and an explicit confirmation bit before touching
// live authorization state.
func (s *server) handleMaintenanceRepairDefaultACLs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "maintenance ACL repair accepts POST only",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionGrant, authz.Resource{Type: "organizations"}) {
		return
	}
	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	var payload struct {
		Yes bool   `json:"yes"`
		Org string `json:"org"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !payload.Yes {
		writeRepairConfirmationRequired(w, "maintenance ACL repair")
		return
	}

	check, ok := s.requireActiveMaintenanceRepair(w, r, "online ACL repair")
	if !ok {
		return
	}

	result, err := s.deps.Bootstrap.RepairDefaultACLs(bootstrap.RepairDefaultACLsInput{
		Organization: payload.Org,
	})
	if err != nil {
		if !s.writeBootstrapError(w, err) {
			return
		}
		return
	}

	writeJSON(w, http.StatusOK, maintenanceRepairDefaultACLsResponse(result, check.State, payload.Org))
}

// handleMaintenanceRepairOrgMembership performs online org membership repair
// through bootstrap.Service so live authorization state and persistence remain
// synchronized.
func (s *server) handleMaintenanceRepairOrgMembership(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "org membership repair accepts POST only",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionGrant, authz.Resource{Type: "organizations"}) {
		return
	}
	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	var payload struct {
		Yes    bool   `json:"yes"`
		Action string `json:"action"`
		Org    string `json:"org"`
		User   string `json:"user"`
		Admin  bool   `json:"admin"`
		Force  bool   `json:"force"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !payload.Yes {
		writeRepairConfirmationRequired(w, "org membership repair")
		return
	}
	check, ok := s.requireActiveMaintenanceRepair(w, r, "online org membership repair")
	if !ok {
		return
	}

	result, err := s.deps.Bootstrap.RepairOrgMembership(bootstrap.RepairOrgMembershipInput{
		Action:       payload.Action,
		Organization: payload.Org,
		Username:     payload.User,
		Admin:        payload.Admin,
		Force:        payload.Force,
	})
	if err != nil {
		if !s.writeBootstrapError(w, err) {
			return
		}
		return
	}
	writeJSON(w, http.StatusOK, maintenanceRepairMembershipResponse("org-membership-repair", result, check.State, payload.Org))
}

// handleMaintenanceRepairGroupMembership performs online group actor repair
// through bootstrap.Service, preserving the current actor-type validation.
func (s *server) handleMaintenanceRepairGroupMembership(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "group membership repair accepts POST only",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionGrant, authz.Resource{Type: "organizations"}) {
		return
	}
	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	var payload struct {
		Yes       bool   `json:"yes"`
		Action    string `json:"action"`
		Org       string `json:"org"`
		Group     string `json:"group"`
		ActorType string `json:"actor_type"`
		Actor     string `json:"actor"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !payload.Yes {
		writeRepairConfirmationRequired(w, "group membership repair")
		return
	}
	check, ok := s.requireActiveMaintenanceRepair(w, r, "online group membership repair")
	if !ok {
		return
	}

	result, err := s.deps.Bootstrap.RepairGroupMembership(bootstrap.RepairGroupMembershipInput{
		Action:       payload.Action,
		Organization: payload.Org,
		Group:        payload.Group,
		ActorType:    payload.ActorType,
		Actor:        payload.Actor,
	})
	if err != nil {
		if !s.writeBootstrapError(w, err) {
			return
		}
		return
	}
	writeJSON(w, http.StatusOK, maintenanceRepairMembershipResponse("group-membership-repair", result, check.State, payload.Org))
}

// handleMaintenanceRepairServerAdmins updates the compatibility server-admin
// set through every org's admins group.
func (s *server) handleMaintenanceRepairServerAdmins(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "server-admin repair accepts POST only",
		})
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionGrant, authz.Resource{Type: "organizations"}) {
		return
	}
	if s.deps.Bootstrap == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	var payload struct {
		Yes    bool   `json:"yes"`
		Action string `json:"action"`
		User   string `json:"user"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !payload.Yes {
		writeRepairConfirmationRequired(w, "server-admin repair")
		return
	}
	check, ok := s.requireActiveMaintenanceRepair(w, r, "online server-admin repair")
	if !ok {
		return
	}

	result, err := s.deps.Bootstrap.RepairServerAdminMembership(bootstrap.RepairServerAdminMembershipInput{
		Action:   payload.Action,
		Username: payload.User,
	})
	if err != nil {
		if !s.writeBootstrapError(w, err) {
			return
		}
		return
	}
	writeJSON(w, http.StatusOK, maintenanceRepairMembershipResponse("server-admin-repair", result, check.State, ""))
}

func writeRepairConfirmationRequired(w http.ResponseWriter, label string) {
	writeJSON(w, http.StatusBadRequest, apiError{
		Error:   "confirmation_required",
		Message: label + " requires yes=true",
	})
}

func (s *server) requireActiveMaintenanceRepair(w http.ResponseWriter, r *http.Request, label string) (maintenance.CheckResult, bool) {
	check, err := s.deps.Maintenance.Check(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, apiError{
			Error:   "maintenance_unavailable",
			Message: "maintenance state could not be checked",
		})
		return maintenance.CheckResult{}, false
	}
	if !check.Active {
		writeJSON(w, http.StatusConflict, apiError{
			Error:   "maintenance_required",
			Message: onlineRepairMaintenanceRequiredSuffix + " (" + label + ")",
		})
		return maintenance.CheckResult{}, false
	}
	return check, true
}

// maintenanceRepairDefaultACLsResponse keeps the operational response explicit
// about what was repaired and why no restart is required for this online path.
func maintenanceRepairDefaultACLsResponse(result bootstrap.RepairDefaultACLsResult, state maintenance.State, org string) map[string]any {
	response := map[string]any{
		"operation":                 "acl-default-repair",
		"mode":                      "online",
		"changed":                   result.Changed,
		"bootstrap_repaired_acls":   result.BootstrapRepaired,
		"core_object_repaired_acls": result.CoreObjectRepaired,
		"cache_state":               onlineRepairCacheState,
		"verifier_cache":            onlineRepairVerifierCacheState,
	}
	if org = strings.TrimSpace(org); org != "" {
		response["org"] = org
	}
	response["maintenance"] = state.SafeStatus()
	return response
}

func maintenanceRepairMembershipResponse(operation string, result bootstrap.RepairMembershipResult, state maintenance.State, org string) map[string]any {
	response := map[string]any{
		"operation":      operation,
		"mode":           "online",
		"changed":        result.Changed,
		"members":        result.Members,
		"cache_state":    onlineRepairCacheState,
		"verifier_cache": onlineRepairVerifierCacheState,
		"maintenance":    state.SafeStatus(),
	}
	if org = strings.TrimSpace(org); org != "" {
		response["org"] = org
	}
	return response
}
