package api

import (
	"net/http"

	"github.com/oberones/OpenCook/internal/authz"
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
	case http.MethodPost:
		if !matchesCollectionPath(r.URL.Path, basePath) {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		if !s.authorizeUserKeyWrite(w, r, name) {
			return
		}

		var payload struct {
			Name           string `json:"name"`
			PublicKey      string `json:"public_key"`
			CreateKey      bool   `json:"create_key"`
			ExpirationDate string `json:"expiration_date"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}

		keyMaterial, err := state.CreateUserKey(name, bootstrap.CreateKeyInput{
			Name:           payload.Name,
			PublicKey:      payload.PublicKey,
			CreateKey:      payload.CreateKey,
			ExpirationDate: payload.ExpirationDate,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}

		w.Header().Set("Location", keyMaterial.URI)
		response := map[string]any{
			"uri": keyMaterial.URI,
		}
		if keyMaterial.PrivateKeyPEM != "" {
			response["private_key"] = keyMaterial.PrivateKeyPEM
		}
		writeJSON(w, http.StatusCreated, response)
	case http.MethodDelete:
		if !s.authorizeUserKeyWrite(w, r, name) {
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

		if !s.writeBootstrapError(w, state.DeleteUserKey(name, keyName)) {
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"name": keyName,
			"uri":  basePath + "/" + keyName,
		})
	case http.MethodPut:
		if !s.authorizeUserKeyWrite(w, r, name) {
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

		var payload keyUpdatePayload
		if !decodeJSON(w, r, &payload) {
			return
		}

		result, err := state.UpdateUserKey(name, keyName, payload.toUpdateInput())
		if !s.writeBootstrapError(w, err) {
			return
		}

		status, body := userFacingKeyUpdateResponse(result, payload)
		if result.Renamed {
			w.Header().Set("Location", result.KeyMaterial.URI)
		}
		writeJSON(w, status, body)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "user key method is not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleClientKeys(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, clientBasePath, ok := s.resolveClientRoute(w, r)
	if !ok {
		return
	}
	name := r.PathValue("name")
	basePath := clientKeyBasePath(org, name, clientBasePath)

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

			writeJSON(w, http.StatusOK, clientFacingKeyListPayload(keys, basePath))
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
	case http.MethodPost:
		if !matchesCollectionPath(r.URL.Path, basePath) {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		if !s.authorizeClientKeyWrite(w, r, org, name) {
			return
		}

		var payload struct {
			Name           string `json:"name"`
			PublicKey      string `json:"public_key"`
			CreateKey      bool   `json:"create_key"`
			ExpirationDate string `json:"expiration_date"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}

		keyMaterial, err := state.CreateClientKey(org, name, bootstrap.CreateKeyInput{
			Name:           payload.Name,
			PublicKey:      payload.PublicKey,
			CreateKey:      payload.CreateKey,
			ExpirationDate: payload.ExpirationDate,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}

		location := basePath + "/" + keyMaterial.Name
		w.Header().Set("Location", location)
		response := map[string]any{
			"uri": location,
		}
		if keyMaterial.PrivateKeyPEM != "" {
			response["private_key"] = keyMaterial.PrivateKeyPEM
		}
		writeJSON(w, http.StatusCreated, response)
	case http.MethodDelete:
		if !s.authorizeClientKeyWrite(w, r, org, name) {
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

		if !s.writeBootstrapError(w, state.DeleteClientKey(org, name, keyName)) {
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"name": keyName,
			"uri":  basePath + "/" + keyName,
		})
	case http.MethodPut:
		if !s.authorizeClientKeyWrite(w, r, org, name) {
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

		var payload keyUpdatePayload
		if !decodeJSON(w, r, &payload) {
			return
		}

		result, err := state.UpdateClientKey(org, name, keyName, payload.toUpdateInput())
		if !s.writeBootstrapError(w, err) {
			return
		}

		status, body := userFacingKeyUpdateResponse(result, payload)
		if result.Renamed {
			w.Header().Set("Location", basePath+"/"+result.KeyMaterial.Name)
		}
		writeJSON(w, status, body)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "client key method is not implemented yet",
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

type keyUpdatePayload struct {
	Name           *string `json:"name"`
	PublicKey      *string `json:"public_key"`
	CreateKey      *bool   `json:"create_key"`
	ExpirationDate *string `json:"expiration_date"`
}

func (p keyUpdatePayload) toUpdateInput() bootstrap.UpdateKeyInput {
	return bootstrap.UpdateKeyInput{
		Name:           p.Name,
		PublicKey:      p.PublicKey,
		CreateKey:      p.CreateKey,
		ExpirationDate: p.ExpirationDate,
	}
}

func userFacingKeyUpdateResponse(result bootstrap.UpdateKeyResult, payload keyUpdatePayload) (int, map[string]any) {
	body := make(map[string]any)

	if payload.Name != nil {
		body["name"] = result.KeyMaterial.Name
	}
	if payload.ExpirationDate != nil {
		body["expiration_date"] = result.KeyMaterial.ExpirationDate
	}
	if payload.PublicKey != nil || (payload.CreateKey != nil && *payload.CreateKey) {
		body["public_key"] = result.KeyMaterial.PublicKeyPEM
	}
	if payload.CreateKey != nil && *payload.CreateKey {
		body["private_key"] = result.KeyMaterial.PrivateKeyPEM
	}

	status := http.StatusOK
	if result.Renamed {
		status = http.StatusCreated
	}

	return status, body
}

func (s *server) authorizeUserKeyWrite(w http.ResponseWriter, r *http.Request, name string) bool {
	requestor, _ := requestorFromContext(r.Context())
	if requestor.Type == "user" && requestor.Name == name {
		return true
	}
	return s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{Type: "user", Name: name})
}

func (s *server) authorizeClientKeyWrite(w http.ResponseWriter, r *http.Request, org, name string) bool {
	requestor, _ := requestorFromContext(r.Context())
	if requestor.Type == "client" && requestor.Name == name && requestor.Organization == org {
		return true
	}
	return s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
		Type:         "client",
		Name:         name,
		Organization: org,
	})
}
