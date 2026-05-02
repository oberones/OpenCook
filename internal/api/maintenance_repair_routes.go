package api

import (
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/maintenance"
)

const maintenanceRepairDefaultACLsPath = "/internal/maintenance/repair/default-acls"

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
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "confirmation_required",
			Message: "maintenance ACL repair requires yes=true",
		})
		return
	}

	check, err := s.deps.Maintenance.Check(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, apiError{
			Error:   "maintenance_unavailable",
			Message: "maintenance state could not be checked",
		})
		return
	}
	if !check.Active {
		writeJSON(w, http.StatusConflict, apiError{
			Error:   "maintenance_required",
			Message: "active maintenance mode is required before online ACL repair",
		})
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

// maintenanceRepairDefaultACLsResponse keeps the operational response explicit
// about what was repaired and why no restart is required for this online path.
func maintenanceRepairDefaultACLsResponse(result bootstrap.RepairDefaultACLsResult, state maintenance.State, org string) map[string]any {
	response := map[string]any{
		"operation":                 "acl-default-repair",
		"mode":                      "online",
		"changed":                   result.Changed,
		"bootstrap_repaired_acls":   result.BootstrapRepaired,
		"core_object_repaired_acls": result.CoreObjectRepaired,
		"cache_state":               "live bootstrap service state was updated through the normal persistence seam; restart is not required for this process",
		"verifier_cache":            "unchanged",
	}
	if org = strings.TrimSpace(org); org != "" {
		response["org"] = org
	}
	response["maintenance"] = state.SafeStatus()
	return response
}
