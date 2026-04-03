package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

type Dependencies struct {
	Logger    *log.Logger
	Config    config.Config
	Version   version.Info
	Compat    compat.Registry
	Authn     authn.Verifier
	Authz     authz.Authorizer
	Bootstrap *bootstrap.Service
	Blob      blob.Store
	Search    search.Index
	Postgres  *pg.Store
}

type server struct {
	deps Dependencies
}

type contextKey string

const authenticatedRequestorContextKey contextKey = "authenticated_requestor"

func NewRouter(deps Dependencies) http.Handler {
	srv := &server{deps: deps}
	mux := http.NewServeMux()

	mux.HandleFunc("/", srv.handleRoot)
	mux.HandleFunc("/_status", srv.handleStatus)
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/readyz", srv.handleReady)
	mux.HandleFunc("/internal/contracts/routes", srv.handleRouteContract)
	mux.HandleFunc("/internal/authn/capabilities", srv.handleAuthnCapabilities)
	mux.HandleFunc("/clients", srv.withAuthn("clients-root", srv.handleClients))
	mux.HandleFunc("/clients/", srv.withAuthn("clients-routes", srv.handleClients))
	mux.HandleFunc("/data", srv.withAuthn("data-root", srv.handleData))
	mux.HandleFunc("/data/", srv.withAuthn("data-routes", srv.handleData))
	mux.HandleFunc("/environments", srv.withAuthn("environments-root", srv.handleEnvironments))
	mux.HandleFunc("/environments/", srv.withAuthn("environments-routes", srv.handleEnvironments))
	mux.HandleFunc("/environments/{name}/nodes", srv.withAuthn("environment-nodes-root", srv.handleEnvironmentNodes))
	mux.HandleFunc("/environments/{name}/nodes/", srv.withAuthn("environment-nodes-routes", srv.handleEnvironmentNodes))
	mux.HandleFunc("/nodes", srv.withAuthn("nodes-root", srv.handleNodes))
	mux.HandleFunc("/nodes/", srv.withAuthn("nodes-routes", srv.handleNodes))
	mux.HandleFunc("/policies", srv.withAuthn("policies-root", srv.handlePolicies))
	mux.HandleFunc("/policies/", srv.withAuthn("policies-routes", srv.handlePolicies))
	mux.HandleFunc("/policy_groups", srv.withAuthn("policy-groups-root", srv.handlePolicyGroups))
	mux.HandleFunc("/policy_groups/", srv.withAuthn("policy-groups-routes", srv.handlePolicyGroups))
	mux.HandleFunc("/search", srv.withAuthn("search-root", srv.handleSearchIndexes))
	mux.HandleFunc("/search/", srv.withAuthn("search-root-trailing", srv.handleSearchIndexes))
	mux.HandleFunc("/search/{index}", srv.withAuthn("search-index-root", srv.handleSearchQuery))
	mux.HandleFunc("/search/{index}/", srv.withAuthn("search-index-routes", srv.handleSearchQuery))
	mux.HandleFunc("/roles/{name}/environments", srv.withAuthn("role-environments-root", srv.handleRoleEnvironments))
	mux.HandleFunc("/roles/{name}/environments/", srv.withAuthn("role-environments-routes", srv.handleRoleEnvironments))
	mux.HandleFunc("/roles", srv.withAuthn("roles-root", srv.handleRoles))
	mux.HandleFunc("/roles/", srv.withAuthn("roles-routes", srv.handleRoles))
	mux.HandleFunc("/organizations", srv.withAuthn("organizations-root", srv.handleOrganizations))
	mux.HandleFunc("/organizations/", srv.withAuthn("organizations-routes", srv.handleOrganizations))
	mux.HandleFunc("/organizations/{org}/data", srv.withAuthn("org-data-root", srv.handleData))
	mux.HandleFunc("/organizations/{org}/data/", srv.withAuthn("org-data-routes", srv.handleData))
	mux.HandleFunc("/organizations/{org}/environments", srv.withAuthn("org-environments-root", srv.handleEnvironments))
	mux.HandleFunc("/organizations/{org}/environments/", srv.withAuthn("org-environments-routes", srv.handleEnvironments))
	mux.HandleFunc("/organizations/{org}/environments/{name}/nodes", srv.withAuthn("org-environment-nodes-root", srv.handleEnvironmentNodes))
	mux.HandleFunc("/organizations/{org}/environments/{name}/nodes/", srv.withAuthn("org-environment-nodes-routes", srv.handleEnvironmentNodes))
	mux.HandleFunc("/organizations/{org}/nodes", srv.withAuthn("org-nodes-root", srv.handleNodes))
	mux.HandleFunc("/organizations/{org}/nodes/", srv.withAuthn("org-nodes-routes", srv.handleNodes))
	mux.HandleFunc("/organizations/{org}/search", srv.withAuthn("org-search-root", srv.handleSearchIndexes))
	mux.HandleFunc("/organizations/{org}/search/", srv.withAuthn("org-search-root-trailing", srv.handleSearchIndexes))
	mux.HandleFunc("/organizations/{org}/search/{index}", srv.withAuthn("org-search-index-root", srv.handleSearchQuery))
	mux.HandleFunc("/organizations/{org}/search/{index}/", srv.withAuthn("org-search-index-routes", srv.handleSearchQuery))
	mux.HandleFunc("/organizations/{org}/roles/{name}/environments", srv.withAuthn("org-role-environments-root", srv.handleRoleEnvironments))
	mux.HandleFunc("/organizations/{org}/roles/{name}/environments/", srv.withAuthn("org-role-environments-routes", srv.handleRoleEnvironments))
	mux.HandleFunc("/organizations/{org}/roles", srv.withAuthn("org-roles-root", srv.handleRoles))
	mux.HandleFunc("/organizations/{org}/roles/", srv.withAuthn("org-roles-routes", srv.handleRoles))
	mux.HandleFunc("/organizations/{org}/_acl", srv.withAuthn("org-acl", srv.handleOrgACL))
	mux.HandleFunc("/organizations/{org}/groups", srv.withAuthn("org-groups-root", srv.handleOrgGroups))
	mux.HandleFunc("/organizations/{org}/groups/", srv.withAuthn("org-groups-routes", srv.handleOrgGroups))
	mux.HandleFunc("/organizations/{org}/groups/{name}/_acl", srv.withAuthn("org-group-acl", srv.handleOrgGroupACL))
	mux.HandleFunc("/organizations/{org}/containers", srv.withAuthn("org-containers-root", srv.handleOrgContainers))
	mux.HandleFunc("/organizations/{org}/containers/", srv.withAuthn("org-containers-routes", srv.handleOrgContainers))
	mux.HandleFunc("/organizations/{org}/containers/{name}/_acl", srv.withAuthn("org-container-acl", srv.handleOrgContainerACL))
	mux.HandleFunc("/users", srv.withAuthn("users-root", srv.handleUsers))
	mux.HandleFunc("/users/", srv.withAuthn("users-named", srv.handleUsers))
	mux.HandleFunc("/users/{name}/keys", srv.withAuthn("user-keys-root", srv.handleUserKeys))
	mux.HandleFunc("/users/{name}/keys/", srv.withAuthn("user-keys-routes", srv.handleUserKeys))
	mux.HandleFunc("/users/{name}/_acl", srv.withAuthn("user-acl", srv.handleUserACL))
	mux.HandleFunc("/clients/{name}/keys", srv.withAuthn("client-keys-root", srv.handleClientKeys))
	mux.HandleFunc("/clients/{name}/keys/", srv.withAuthn("client-keys-routes", srv.handleClientKeys))
	mux.HandleFunc("/organizations/{org}/clients", srv.withAuthn("org-clients", srv.handleClients))
	mux.HandleFunc("/organizations/{org}/clients/", srv.withAuthn("org-client-named", srv.handleClients))
	mux.HandleFunc("/organizations/{org}/clients/{name}/keys", srv.withAuthn("org-client-keys-root", srv.handleClientKeys))
	mux.HandleFunc("/organizations/{org}/clients/{name}/keys/", srv.withAuthn("org-client-keys-routes", srv.handleClientKeys))
	mux.HandleFunc("/organizations/{org}/clients/{name}/_acl", srv.withAuthn("org-client-acl", srv.handleOrgClientACL))

	for _, surface := range deps.Compat.Surfaces() {
		surface := surface
		for _, pattern := range surface.Patterns {
			pattern := pattern
			if isImplementedPattern(pattern) {
				continue
			}
			mux.HandleFunc(pattern, srv.handleNotImplemented(surface, pattern))
		}
	}

	return mux
}

func (s *server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service":       s.deps.Config.ServiceName,
		"phase":         "compatibility-foundation",
		"version":       s.deps.Version,
		"compat_routes": s.deps.Compat.RouteCount(),
		"next":          "deepen policyfile and search semantics, then move stabilized slices into postgres and opensearch-backed providers",
	})
}

func (s *server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.statusPayload("status"))
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.statusPayload("health"))
}

func (s *server) handleReady(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.statusPayload("bootstrap"))
}

func (s *server) handleRouteContract(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":  s.deps.Config.ServiceName,
		"phase":    "contract-inventory",
		"count":    s.deps.Compat.RouteCount(),
		"surfaces": s.deps.Compat.Surfaces(),
	})
}

func (s *server) handleAuthnCapabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":      s.deps.Config.ServiceName,
		"authn_engine": s.deps.Authn.Name(),
		"capabilities": s.deps.Authn.Capabilities(),
	})
}

func (s *server) handleUsers(w http.ResponseWriter, r *http.Request) {
	requestor, _ := requestorFromContext(r.Context())
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
		if matchesCollectionPath(r.URL.Path, "/users") {
			if !s.authorizeRequest(w, r, authz.ActionRead, authz.Resource{Type: "users"}) {
				return
			}

			writeJSON(w, http.StatusOK, map[string]any{
				"requestor":   requestor,
				"users":       state.ListUsers(),
				"authn":       "verified",
				"persistence": "memory-bootstrap",
			})
			return
		}

		name := strings.TrimPrefix(r.URL.Path, "/users/")
		if name == "" || strings.Contains(name, "/") {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		if !s.authorizeUserRead(w, r, name) {
			return
		}

		user, ok := state.GetUser(name)
		if !ok {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "user not found",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"username":       user.Username,
			"display_name":   user.DisplayName,
			"email":          user.Email,
			"first_name":     user.FirstName,
			"last_name":      user.LastName,
			"requestor":      requestor,
			"uri":            "/users/" + name,
			"authn_status":   "verified",
			"storage_status": "memory-bootstrap",
		})
	case http.MethodPost:
		if !matchesCollectionPath(r.URL.Path, "/users") {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{Type: "users"}) {
			return
		}

		var payload struct {
			Username    string `json:"username"`
			DisplayName string `json:"display_name"`
			Email       string `json:"email"`
			FirstName   string `json:"first_name"`
			LastName    string `json:"last_name"`
			PublicKey   string `json:"public_key"`
			CreateKey   bool   `json:"create_key"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}

		user, keyMaterial, err := state.CreateUser(bootstrap.CreateUserInput{
			Username:    payload.Username,
			DisplayName: payload.DisplayName,
			Email:       payload.Email,
			FirstName:   payload.FirstName,
			LastName:    payload.LastName,
			PublicKey:   payload.PublicKey,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}

		response := map[string]any{
			"uri": "/users/" + user.Username,
		}
		if keyMaterial != nil && keyMaterial.PrivateKeyPEM != "" {
			response["private_key"] = keyMaterial.PrivateKeyPEM
		}
		if payload.CreateKey && keyMaterial != nil {
			response["chef_key"] = keyMaterial
		}
		writeJSON(w, http.StatusCreated, response)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "authenticated users endpoint write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleClients(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveClientRoute(w, r)
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
		s.handleClientGet(w, r, state, org, basePath)
	case http.MethodHead:
		s.handleClientHead(w, r, state, org, basePath)
	case http.MethodPost:
		s.handleClientPost(w, r, state, org, basePath)
	case http.MethodDelete:
		s.handleClientDelete(w, r, state, org, basePath)
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "authenticated clients endpoint write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleNotImplemented(surface compat.Surface, pattern string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "compatibility surface scaffolded but not implemented",
			"method":  r.Method,
			"path":    r.URL.Path,
			"pattern": pattern,
			"surface": surface.Name,
			"owner":   surface.Owner,
			"phase":   surface.Phase,
			"notes":   surface.Notes,
		})
	}
}

func (s *server) withAuthn(routeID string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bodyReader := http.MaxBytesReader(w, r.Body, s.authRequestBodyLimit())
		body, err := io.ReadAll(bodyReader)
		if err != nil {
			var maxErr *http.MaxBytesError
			if errors.As(err, &maxErr) {
				writeJSON(w, http.StatusRequestEntityTooLarge, apiError{
					Error:   "request_body_too_large",
					Message: "request body exceeds authentication limit",
				})
				return
			}

			s.logf("failed to read authn request body for %s %s: %v", r.Method, r.URL.Path, err)
			writeJSON(w, http.StatusInternalServerError, apiError{
				Error:   "read_body_failed",
				Message: "failed to read request body",
			})
			return
		}
		_ = bodyReader.Close()
		r.Body = io.NopCloser(bytes.NewReader(body))

		result, verifyErr := s.deps.Authn.Verify(r.Context(), authn.RequestContext{
			Method:           r.Method,
			Path:             r.URL.Path,
			Body:             body,
			Headers:          flattenHeaders(r.Header),
			Organization:     s.authnOrganization(r),
			ServerAPIVersion: r.Header.Get("X-Ops-Server-API-Version"),
		})
		if verifyErr != nil {
			s.writeAuthnFailure(w, verifyErr)
			return
		}

		ctx := context.WithValue(r.Context(), authenticatedRequestorContextKey, result.Principal)
		w.Header().Set("X-OpenCook-Authn-Route", routeID)
		next(w, r.WithContext(ctx))
	}
}

func (s *server) writeAuthnFailure(w http.ResponseWriter, err error) {
	var authErr *authn.Error
	if errors.As(err, &authErr) {
		writeJSON(w, authErr.HTTPStatus(), map[string]any{
			"error":   string(authErr.Kind),
			"message": authErr.Message,
			"headers": authErr.Headers,
		})
		return
	}

	s.logf("internal authn failure: %v", err)
	writeJSON(w, http.StatusInternalServerError, apiError{
		Error:   "authn_failed",
		Message: "internal authentication error",
	})
}

func requestorFromContext(ctx context.Context) (authn.Principal, bool) {
	value := ctx.Value(authenticatedRequestorContextKey)
	requestor, ok := value.(authn.Principal)
	return requestor, ok
}

func flattenHeaders(header http.Header) map[string]string {
	out := make(map[string]string, len(header))
	for key, values := range header {
		if len(values) == 0 {
			continue
		}
		out[key] = values[0]
	}
	return out
}

func isImplementedPattern(pattern string) bool {
	switch pattern {
	case "/users", "/users/", "/users/{name}/keys", "/users/{name}/keys/", "/organizations", "/organizations/", "/clients", "/clients/", "/clients/{name}/keys", "/clients/{name}/keys/", "/data", "/data/", "/organizations/{org}/data", "/organizations/{org}/data/", "/environments", "/environments/", "/environments/{name}/nodes", "/environments/{name}/nodes/", "/organizations/{org}/environments", "/organizations/{org}/environments/", "/organizations/{org}/environments/{name}/nodes", "/organizations/{org}/environments/{name}/nodes/", "/nodes", "/nodes/", "/organizations/{org}/nodes", "/organizations/{org}/nodes/", "/policies", "/policies/", "/policies/{name}", "/policies/{name}/", "/policies/{name}/revisions", "/policies/{name}/revisions/", "/policies/{name}/revisions/{revision}", "/policies/{name}/revisions/{revision}/", "/policy_groups", "/policy_groups/", "/policy_groups/{group}", "/policy_groups/{group}/", "/policy_groups/{group}/policies/{name}", "/policy_groups/{group}/policies/{name}/", "/search", "/search/", "/search/{index}", "/search/{index}/", "/organizations/{org}/search", "/organizations/{org}/search/", "/organizations/{org}/search/{index}", "/organizations/{org}/search/{index}/", "/roles", "/roles/", "/roles/{name}/environments", "/roles/{name}/environments/", "/roles/{name}/environments/{environment}", "/roles/{name}/environments/{environment}/", "/organizations/{org}/roles", "/organizations/{org}/roles/", "/organizations/{org}/roles/{name}/environments", "/organizations/{org}/roles/{name}/environments/", "/organizations/{org}/roles/{name}/environments/{environment}", "/organizations/{org}/roles/{name}/environments/{environment}/", "/organizations/{org}/clients", "/organizations/{org}/clients/", "/organizations/{org}/clients/{name}/keys", "/organizations/{org}/clients/{name}/keys/":
		return true
	default:
		return false
	}
}

func matchesCollectionPath(path, collectionPath string) bool {
	return path == collectionPath || path == collectionPath+"/"
}

func (s *server) authRequestBodyLimit() int64 {
	if s.deps.Config.MaxAuthBodyBytes > 0 {
		return s.deps.Config.MaxAuthBodyBytes
	}

	return config.DefaultMaxAuthBodyBytes
}

func (s *server) logf(format string, args ...any) {
	if s.deps.Logger == nil {
		return
	}

	s.deps.Logger.Printf(format, args...)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, payload any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(payload); err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_json",
			Message: "request body must be valid JSON",
		})
		return false
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_json",
			Message: "request body must contain exactly one JSON document",
		})
		return false
	}

	return true
}

func (s *server) statusPayload(mode string) map[string]any {
	return map[string]any{
		"mode":        mode,
		"service":     s.deps.Config.ServiceName,
		"environment": s.deps.Config.Environment,
		"phase":       "compatibility-foundation",
		"version":     s.deps.Version,
		"config":      s.deps.Config.Redacted(),
		"compatibility": map[string]any{
			"strategy": "contract-first",
			"surfaces": s.deps.Compat.Surfaces(),
		},
		"dependencies": map[string]any{
			"authn": map[string]string{
				"backend": s.deps.Authn.Name(),
			},
			"authn_capabilities": s.deps.Authn.Capabilities(),
			"authz": map[string]string{
				"backend": s.deps.Authz.Name(),
			},
			"postgres":   s.deps.Postgres.Status(),
			"opensearch": s.deps.Search.Status(),
			"blob":       s.deps.Blob.Status(),
		},
	}
}
