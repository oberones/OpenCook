package api

import (
	"net/http"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleUserKeys(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	name := r.PathValue("name")
	basePath := "/users/" + name + "/keys"

	switch r.Method {
	case http.MethodGet:
		if !s.authorizeUserRead(w, r, name) {
			return
		}

		if matchesCollectionPath(r.URL.Path, basePath) {
			keys, ok := state.ListUserKeys(name)
			if !ok {
				writeJSON(w, http.StatusNotFound, apiError{
					Error:   "not_found",
					Message: "user not found",
				})
				return
			}

			writeJSON(w, http.StatusOK, keyListPayload(keys))
			return
		}

		keyName, ok := pathTail(r.URL.Path, basePath+"/")
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		key, userExists, keyExists := state.GetUserKey(name, keyName)
		if !userExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "user not found",
			})
			return
		}
		if !keyExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "key not found",
			})
			return
		}

		writeJSON(w, http.StatusOK, keyDetailPayload(key))
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "user key write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleOrgClientKeys(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org := r.PathValue("org")
	name := r.PathValue("name")
	basePath := "/organizations/" + org + "/clients/" + name + "/keys"

	switch r.Method {
	case http.MethodGet:
		if !s.authorizeClientRead(w, r, org, name) {
			return
		}

		if matchesCollectionPath(r.URL.Path, basePath) {
			keys, orgExists, clientExists := state.ListClientKeys(org, name)
			if !orgExists {
				writeJSON(w, http.StatusNotFound, apiError{
					Error:   "not_found",
					Message: "organization not found",
				})
				return
			}
			if !clientExists {
				writeJSON(w, http.StatusNotFound, apiError{
					Error:   "not_found",
					Message: "client not found",
				})
				return
			}

			writeJSON(w, http.StatusOK, keyListPayload(keys))
			return
		}

		keyName, ok := pathTail(r.URL.Path, basePath+"/")
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		key, orgExists, clientExists, keyExists := state.GetClientKey(org, name, keyName)
		if !orgExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "organization not found",
			})
			return
		}
		if !clientExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "client not found",
			})
			return
		}
		if !keyExists {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "key not found",
			})
			return
		}

		writeJSON(w, http.StatusOK, keyDetailPayload(key))
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "client key write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func keyListPayload(keys []bootstrap.KeyRecord) []map[string]any {
	if len(keys) == 0 {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{
			"name":    key.Name,
			"uri":     key.URI,
			"expired": key.Expired,
		})
	}
	return out
}

func keyDetailPayload(key bootstrap.KeyRecord) map[string]any {
	return map[string]any{
		"name":            key.Name,
		"public_key":      key.PublicKeyPEM,
		"expiration_date": key.ExpirationDate,
	}
}
