package api

import (
	"errors"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

func (s *server) handleClientGet(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "clients",
			Organization: org,
		}) {
			return
		}

		clients, _ := state.ListClients(org)
		if clients == nil {
			clients = map[string]string{}
		}
		response := make(map[string]string, len(clients))
		for name := range clients {
			response[name] = clientURLForResponse(org, name, basePath)
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

	client, ok := state.GetClient(org, name)
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "client not found",
		})
		return
	}
	if !s.authorizeClientRead(w, r, org, name) {
		return
	}

	writeJSON(w, http.StatusOK, clientResponseObject(client))
}

func (s *server) handleClientHead(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if matchesCollectionPath(r.URL.Path, basePath) {
		if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{
			Type:         "container",
			Name:         "clients",
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

	if _, exists := state.GetClient(org, name); !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !s.authorizeClientRead(w, r, org, name) {
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *server) handleClientPost(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if !matchesCollectionPath(r.URL.Path, basePath) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
		Type:         "container",
		Name:         "clients",
		Organization: org,
	}) {
		return
	}

	var payload struct {
		Name       string `json:"name"`
		ClientName string `json:"clientname"`
		Validator  bool   `json:"validator"`
		Admin      bool   `json:"admin"`
		PublicKey  string `json:"public_key"`
		CreateKey  bool   `json:"create_key"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}

	name := payload.Name
	if name == "" {
		name = payload.ClientName
	}

	client, keyMaterial, err := state.CreateClient(org, bootstrap.CreateClientInput{
		Name:      name,
		Validator: payload.Validator,
		Admin:     payload.Admin,
		PublicKey: payload.PublicKey,
	})
	if !s.writeBootstrapError(w, err) {
		return
	}

	response := map[string]any{
		"uri": clientURLForResponse(org, client.Name, basePath),
	}
	if keyMaterial != nil && keyMaterial.PrivateKeyPEM != "" {
		response["private_key"] = keyMaterial.PrivateKeyPEM
	}
	if payload.CreateKey && keyMaterial != nil {
		response["chef_key"] = clientFacingKeyMaterial(*keyMaterial, clientKeyBasePath(org, client.Name, basePath))
	}
	writeJSON(w, http.StatusCreated, response)
}

func (s *server) handleClientDelete(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	name, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	if _, exists := state.GetClient(org, name); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "client not found",
		})
		return
	}

	if !s.authorizeRequest(w, r, authz.ActionDelete, authz.Resource{
		Type:         "client",
		Name:         name,
		Organization: org,
	}) {
		return
	}

	client, err := state.DeleteClient(org, name)
	if errors.Is(err, bootstrap.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "client not found",
		})
		return
	}
	if !s.writeBootstrapError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"name": client.Name,
	})
}

func (s *server) resolveClientRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/clients", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/clients", true
}

func clientResponseObject(client bootstrap.Client) map[string]any {
	response := map[string]any{
		"name":       client.Name,
		"clientname": client.ClientName,
		"json_class": "Chef::ApiClient",
		"chef_type":  "client",
		"orgname":    client.Organization,
		"validator":  client.Validator,
	}
	if strings.TrimSpace(client.PublicKey) != "" {
		response["public_key"] = client.PublicKey
	}
	return response
}

func clientURLForResponse(org, name, basePath string) string {
	if basePath == "/clients" {
		return "/clients/" + name
	}
	return "/organizations/" + org + "/clients/" + name
}

func clientKeyBasePath(org, name, basePath string) string {
	return clientURLForResponse(org, name, basePath) + "/keys"
}

func clientFacingKeyMaterial(keyMaterial bootstrap.KeyMaterial, basePath string) map[string]any {
	response := map[string]any{
		"name":            keyMaterial.Name,
		"expiration_date": keyMaterial.ExpirationDate,
		"uri":             basePath + "/" + keyMaterial.Name,
		"public_key":      keyMaterial.PublicKeyPEM,
	}
	if keyMaterial.PrivateKeyPEM != "" {
		response["private_key"] = keyMaterial.PrivateKeyPEM
	}
	return response
}

func clientFacingKeyListPayload(keys []bootstrap.KeyRecord, basePath string) []map[string]any {
	if len(keys) == 0 {
		return []map[string]any{}
	}

	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, map[string]any{
			"name":    key.Name,
			"uri":     basePath + "/" + key.Name,
			"expired": key.Expired,
		})
	}
	return out
}
