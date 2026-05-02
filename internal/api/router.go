package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/maintenance"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

type Dependencies struct {
	Logger           *log.Logger
	Config           config.Config
	Version          version.Info
	Compat           compat.Registry
	Now              func() time.Time
	Authn            authn.Verifier
	Authz            authz.Authorizer
	Bootstrap        *bootstrap.Service
	Blob             blob.Store
	BlobUploadSecret []byte
	Search           search.Index
	Postgres         *pg.Store
	Maintenance      maintenance.Store
	CookbookBackend  string
}

type server struct {
	deps    Dependencies
	metrics *metricsRegistry
}

type contextKey string

const (
	authenticatedRequestorContextKey contextKey = "authenticated_requestor"
	authenticatedRouteIDContextKey   contextKey = "authenticated_route_id"
)

// NewRouter wires the Chef-compatible HTTP surface and wraps it with
// low-cardinality operational instrumentation.
func NewRouter(deps Dependencies) http.Handler {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if len(deps.BlobUploadSecret) == 0 {
		deps.BlobUploadSecret = make([]byte, 32)
		if _, err := rand.Read(deps.BlobUploadSecret); err != nil {
			deps.BlobUploadSecret = []byte("opencook-fallback-blob-upload-secret")
		}
	}
	if deps.Maintenance == nil {
		deps.Maintenance = maintenance.NewMemoryStore(maintenance.WithClock(deps.Now))
	}

	srv := &server{deps: deps, metrics: newMetricsRegistry(deps.Now)}
	mux := http.NewServeMux()

	mux.HandleFunc("/", srv.handleRoot)
	mux.HandleFunc("/_status", srv.handleStatus)
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/readyz", srv.handleReady)
	mux.HandleFunc("/metrics", srv.handleMetrics)
	mux.HandleFunc("/server_api_version", srv.withServerAPIVersion(srv.handleServerAPIVersion))
	mux.HandleFunc("/server_api_version/", srv.withServerAPIVersion(srv.handleServerAPIVersion))
	mux.HandleFunc("/internal/contracts/routes", srv.handleRouteContract)
	mux.HandleFunc("/internal/authn/capabilities", srv.handleAuthnCapabilities)
	mux.HandleFunc("/internal/maintenance/repair/default-acls", srv.withAuthn("maintenance-repair-default-acls", srv.handleMaintenanceRepairDefaultACLs))
	mux.HandleFunc("/_blob/checksums/{checksum}", srv.handleBlobChecksumUpload)
	mux.HandleFunc("/_blob/checksums/{checksum}/", srv.handleBlobChecksumUpload)
	mux.HandleFunc("/cookbooks", srv.withAuthn("cookbooks-root", srv.handleCookbooks))
	mux.HandleFunc("/cookbooks/", srv.withAuthn("cookbooks-routes", srv.handleCookbooks))
	mux.HandleFunc("/cookbook_artifacts", srv.withAuthn("cookbook-artifacts-root", srv.handleCookbookArtifacts))
	mux.HandleFunc("/cookbook_artifacts/", srv.withAuthn("cookbook-artifacts-routes", srv.handleCookbookArtifacts))
	mux.HandleFunc("/clients", srv.withAuthn("clients-root", srv.handleClients))
	mux.HandleFunc("/clients/", srv.withAuthn("clients-routes", srv.handleClients))
	mux.HandleFunc("/data", srv.withAuthn("data-root", srv.handleData))
	mux.HandleFunc("/data/", srv.withAuthn("data-routes", srv.handleData))
	mux.HandleFunc("/environments", srv.withAuthn("environments-root", srv.handleEnvironments))
	mux.HandleFunc("/environments/", srv.withAuthn("environments-routes", srv.handleEnvironments))
	mux.HandleFunc("/environments/{name}/cookbooks", srv.withAuthn("environment-cookbooks-root", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/environments/{name}/cookbooks/{cookbook}", srv.withAuthn("environment-cookbooks-named", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/environments/{name}/cookbooks/", srv.withAuthn("environment-cookbooks-routes", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/environments/{name}/cookbook_versions", srv.withAuthn("environment-cookbook-versions-root", srv.handleEnvironmentCookbookVersions))
	mux.HandleFunc("/environments/{name}/cookbook_versions/", srv.withAuthn("environment-cookbook-versions-routes", srv.handleEnvironmentCookbookVersions))
	mux.HandleFunc("/environments/{name}/nodes", srv.withAuthn("environment-nodes-root", srv.handleEnvironmentNodes))
	mux.HandleFunc("/environments/{name}/nodes/", srv.withAuthn("environment-nodes-routes", srv.handleEnvironmentNodes))
	mux.HandleFunc("/environments/{name}/roles/{role}", srv.withAuthn("environment-roles-named", srv.handleEnvironmentRoles))
	mux.HandleFunc("/environments/{name}/roles/{role}/", srv.withAuthn("environment-roles-routes", srv.handleEnvironmentRoles))
	mux.HandleFunc("/environments/{name}/recipes", srv.withAuthn("environment-recipes-root", srv.handleEnvironmentRecipes))
	mux.HandleFunc("/environments/{name}/recipes/", srv.withAuthn("environment-recipes-routes", srv.handleEnvironmentRecipes))
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
	mux.HandleFunc("/sandboxes", srv.withAuthn("sandboxes-root", srv.handleSandboxes))
	mux.HandleFunc("/sandboxes/", srv.withAuthn("sandboxes-routes", srv.handleSandboxes))
	mux.HandleFunc("/universe", srv.withAuthn("universe-root", srv.handleUniverse))
	mux.HandleFunc("/universe/", srv.withAuthn("universe-routes", srv.handleUniverse))
	mux.HandleFunc("/roles/{name}/environments", srv.withAuthn("role-environments-root", srv.handleRoleEnvironments))
	mux.HandleFunc("/roles/{name}/environments/", srv.withAuthn("role-environments-routes", srv.handleRoleEnvironments))
	mux.HandleFunc("/roles", srv.withAuthn("roles-root", srv.handleRoles))
	mux.HandleFunc("/roles/", srv.withAuthn("roles-routes", srv.handleRoles))
	mux.HandleFunc("/organizations", srv.withAuthn("organizations-root", srv.handleOrganizations))
	mux.HandleFunc("/organizations/", srv.withAuthn("organizations-routes", srv.handleOrganizations))
	mux.HandleFunc("/organizations/{org}/data", srv.withAuthn("org-data-root", srv.handleData))
	mux.HandleFunc("/organizations/{org}/data/", srv.withAuthn("org-data-routes", srv.handleData))
	mux.HandleFunc("/organizations/{org}/cookbooks", srv.withAuthn("org-cookbooks-root", srv.handleCookbooks))
	mux.HandleFunc("/organizations/{org}/cookbooks/", srv.withAuthn("org-cookbooks-routes", srv.handleCookbooks))
	mux.HandleFunc("/organizations/{org}/cookbook_artifacts", srv.withAuthn("org-cookbook-artifacts-root", srv.handleCookbookArtifacts))
	mux.HandleFunc("/organizations/{org}/cookbook_artifacts/", srv.withAuthn("org-cookbook-artifacts-routes", srv.handleCookbookArtifacts))
	mux.HandleFunc("/organizations/{org}/environments", srv.withAuthn("org-environments-root", srv.handleEnvironments))
	mux.HandleFunc("/organizations/{org}/environments/", srv.withAuthn("org-environments-routes", srv.handleEnvironments))
	mux.HandleFunc("/organizations/{org}/environments/{name}/cookbooks", srv.withAuthn("org-environment-cookbooks-root", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/organizations/{org}/environments/{name}/cookbooks/{cookbook}", srv.withAuthn("org-environment-cookbooks-named", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/organizations/{org}/environments/{name}/cookbooks/", srv.withAuthn("org-environment-cookbooks-routes", srv.handleEnvironmentCookbooks))
	mux.HandleFunc("/organizations/{org}/environments/{name}/cookbook_versions", srv.withAuthn("org-environment-cookbook-versions-root", srv.handleEnvironmentCookbookVersions))
	mux.HandleFunc("/organizations/{org}/environments/{name}/cookbook_versions/", srv.withAuthn("org-environment-cookbook-versions-routes", srv.handleEnvironmentCookbookVersions))
	mux.HandleFunc("/organizations/{org}/environments/{name}/nodes", srv.withAuthn("org-environment-nodes-root", srv.handleEnvironmentNodes))
	mux.HandleFunc("/organizations/{org}/environments/{name}/nodes/", srv.withAuthn("org-environment-nodes-routes", srv.handleEnvironmentNodes))
	mux.HandleFunc("/organizations/{org}/environments/{name}/roles/{role}", srv.withAuthn("org-environment-roles-named", srv.handleEnvironmentRoles))
	mux.HandleFunc("/organizations/{org}/environments/{name}/roles/{role}/", srv.withAuthn("org-environment-roles-routes", srv.handleEnvironmentRoles))
	mux.HandleFunc("/organizations/{org}/environments/{name}/recipes", srv.withAuthn("org-environment-recipes-root", srv.handleEnvironmentRecipes))
	mux.HandleFunc("/organizations/{org}/environments/{name}/recipes/", srv.withAuthn("org-environment-recipes-routes", srv.handleEnvironmentRecipes))
	mux.HandleFunc("/organizations/{org}/nodes", srv.withAuthn("org-nodes-root", srv.handleNodes))
	mux.HandleFunc("/organizations/{org}/nodes/", srv.withAuthn("org-nodes-routes", srv.handleNodes))
	mux.HandleFunc("/organizations/{org}/policies", srv.withAuthn("org-policies-root", srv.handlePolicies))
	mux.HandleFunc("/organizations/{org}/policies/", srv.withAuthn("org-policies-routes", srv.handlePolicies))
	mux.HandleFunc("/organizations/{org}/policy_groups", srv.withAuthn("org-policy-groups-root", srv.handlePolicyGroups))
	mux.HandleFunc("/organizations/{org}/policy_groups/", srv.withAuthn("org-policy-groups-routes", srv.handlePolicyGroups))
	mux.HandleFunc("/organizations/{org}/search", srv.withAuthn("org-search-root", srv.handleSearchIndexes))
	mux.HandleFunc("/organizations/{org}/search/", srv.withAuthn("org-search-root-trailing", srv.handleSearchIndexes))
	mux.HandleFunc("/organizations/{org}/search/{index}", srv.withAuthn("org-search-index-root", srv.handleSearchQuery))
	mux.HandleFunc("/organizations/{org}/search/{index}/", srv.withAuthn("org-search-index-routes", srv.handleSearchQuery))
	mux.HandleFunc("/organizations/{org}/sandboxes", srv.withAuthn("org-sandboxes-root", srv.handleSandboxes))
	mux.HandleFunc("/organizations/{org}/sandboxes/", srv.withAuthn("org-sandboxes-routes", srv.handleSandboxes))
	mux.HandleFunc("/organizations/{org}/universe", srv.withAuthn("org-universe-root", srv.handleUniverse))
	mux.HandleFunc("/organizations/{org}/universe/", srv.withAuthn("org-universe-routes", srv.handleUniverse))
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

	return srv.withRequestLogging(srv.withMetrics(srv.withMaintenanceGate(mux)))
}

// withMaintenanceGate blocks only the route/method pairs frozen in the
// maintenance contract. It asks ServeMux for the matching concrete pattern and
// then lets ServeMux serve the request normally so path variables remain intact.
func (s *server) withMaintenanceGate(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.maintenanceBlocksRequest(w, r, mux) {
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// maintenanceBlocksRequest returns true after writing the Chef-compatible 503
// response for an active maintenance write gate. Read-only routes and read-like
// POST exceptions avoid touching the maintenance store entirely.
func (s *server) maintenanceBlocksRequest(w http.ResponseWriter, r *http.Request, mux *http.ServeMux) bool {
	_, pattern := mux.Handler(r)
	contract, ok := maintenanceRouteContractByPattern()[pattern]
	if !ok || !maintenanceRouteMethodListed(contract.BlockedMethods, r.Method) {
		return false
	}

	check, err := s.deps.Maintenance.Check(r.Context())
	if err != nil {
		s.metrics.recordMaintenanceBlocked(r.Method, r.URL.Path, "check_error")
		s.logMaintenanceBlocked(r, pattern, "check_error", maintenance.State{})
		writeJSON(w, maintenanceBlockedHTTPStatus, maintenanceBlockedPayload())
		return true
	}
	if !check.Active {
		return false
	}

	s.metrics.recordMaintenanceBlocked(r.Method, r.URL.Path, "active")
	s.logMaintenanceBlocked(r, pattern, "active", check.State)
	writeJSON(w, maintenanceBlockedHTTPStatus, maintenanceBlockedPayload())
	return true
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
		"next":          "continue chef-server-ctl-style operational parity with config validation, service diagnostics, metrics, logs, and runbooks",
	})
}

func (s *server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.statusPayload("status"))
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.statusPayload("health"))
}

func (s *server) handleReady(w http.ResponseWriter, _ *http.Request) {
	readiness := s.readinessPayload()
	status := http.StatusOK
	if !readiness.Ready {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, s.statusPayloadWithReadiness("bootstrap", readiness))
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

		writeJSON(w, http.StatusOK, userResponseObject(state, user, requestor, serverAPIVersionFromContext(r).Version))
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
			CreateKey   *bool  `json:"create_key"`
			PrivateKey  *bool  `json:"private_key"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}
		apiVersion := serverAPIVersionFromContext(r).Version
		requestsGeneratedKey := payload.CreateKey != nil && *payload.CreateKey
		hasPublicKey := strings.TrimSpace(payload.PublicKey) != ""
		if apiVersion > 0 && payload.PrivateKey != nil && *payload.PrivateKey {
			writeJSON(w, http.StatusBadRequest, apiError{
				Error:   "invalid_request",
				Message: "private_key is not accepted on this API version",
			})
			return
		}
		if requestsGeneratedKey && hasPublicKey {
			writeJSON(w, http.StatusBadRequest, apiError{
				Error:   "invalid_request",
				Message: "public_key and create_key cannot both be set",
			})
			return
		}

		user, keyMaterial, err := state.CreateUser(bootstrap.CreateUserInput{
			Username:         payload.Username,
			DisplayName:      payload.DisplayName,
			Email:            payload.Email,
			FirstName:        payload.FirstName,
			LastName:         payload.LastName,
			PublicKey:        payload.PublicKey,
			CreateDefaultKey: apiVersion == 0 || requestsGeneratedKey,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}

		response := map[string]any{
			"uri": "/users/" + user.Username,
		}
		if apiVersion == 0 && keyMaterial != nil && keyMaterial.PrivateKeyPEM != "" {
			response["private_key"] = keyMaterial.PrivateKeyPEM
		}
		if apiVersion == 0 && keyMaterial != nil && keyMaterial.PublicKeyPEM != "" {
			response["public_key"] = keyMaterial.PublicKeyPEM
		}
		if keyMaterial != nil && (apiVersion > 0 || requestsGeneratedKey) {
			response["chef_key"] = keyMaterial
		}
		writeJSON(w, http.StatusCreated, response)
	case http.MethodPut:
		name := strings.TrimPrefix(r.URL.Path, "/users/")
		if name == "" || strings.Contains(name, "/") {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}
		if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{Type: "user", Name: name}) {
			return
		}

		var payload struct {
			DisplayName *string `json:"display_name"`
			Email       *string `json:"email"`
			FirstName   *string `json:"first_name"`
			LastName    *string `json:"last_name"`
			PublicKey   *string `json:"public_key"`
			CreateKey   *bool   `json:"create_key"`
			PrivateKey  *bool   `json:"private_key"`
		}
		if !decodeJSON(w, r, &payload) {
			return
		}
		if serverAPIVersionFromContext(r).Version > 0 && (payload.PublicKey != nil || payload.CreateKey != nil || payload.PrivateKey != nil) {
			writeJSON(w, http.StatusBadRequest, apiError{
				Error:   "invalid_request",
				Message: "key mutation fields must be managed through the keys endpoint",
			})
			return
		}

		user, _, err := state.UpdateUser(bootstrap.UpdateUserInput{
			Username:    name,
			DisplayName: payload.DisplayName,
			Email:       payload.Email,
			FirstName:   payload.FirstName,
			LastName:    payload.LastName,
			PublicKey:   payload.PublicKey,
		})
		if !s.writeBootstrapError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, userResponseObject(state, user, requestor, serverAPIVersionFromContext(r).Version))
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "authenticated users endpoint write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func userResponseObject(state *bootstrap.Service, user bootstrap.User, requestor authn.Principal, apiVersion int) map[string]any {
	response := map[string]any{
		"username":       user.Username,
		"display_name":   user.DisplayName,
		"email":          user.Email,
		"first_name":     user.FirstName,
		"last_name":      user.LastName,
		"requestor":      requestor,
		"uri":            "/users/" + user.Username,
		"authn_status":   "verified",
		"storage_status": "memory-bootstrap",
	}
	if apiVersion == 0 {
		if key, _, ok := state.GetUserKey(user.Username, "default"); ok && strings.TrimSpace(key.PublicKeyPEM) != "" {
			response["public_key"] = key.PublicKeyPEM
		}
	}
	return response
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
	case http.MethodPut:
		s.handleClientPut(w, r, state, org, basePath)
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
		r, ok := s.validateServerAPIVersion(w, r)
		if !ok {
			return
		}

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
			s.logAuthnFailure(routeID, r, verifyErr)
			s.writeAuthnFailure(w, verifyErr)
			return
		}

		ctx := context.WithValue(r.Context(), authenticatedRequestorContextKey, result.Principal)
		ctx = context.WithValue(ctx, authenticatedRouteIDContextKey, routeID)
		w.Header().Set("X-OpenCook-Authn-Route", routeID)
		next(w, r.WithContext(ctx))
	}
}

// logAuthnFailure records authentication failures with request correlation and
// safe diagnostics while deliberately omitting Chef signature values and raw
// request headers.
func (s *server) logAuthnFailure(routeID string, r *http.Request, err error) {
	var authErr *authn.Error
	if !errors.As(err, &authErr) {
		return
	}

	sign := strings.TrimSpace(r.Header.Get("X-Ops-Sign"))
	serverAPIVersion := strings.TrimSpace(r.Header.Get(serverAPIVersionHeader))
	if serverAPIVersion == "" {
		serverAPIVersion = "0"
	}
	requestID, _ := requestIDFromContext(r.Context())

	fields := map[string]any{
		"request_id":         requestID,
		"route":              routeID,
		"method":             metricMethod(r.Method),
		"path":               r.URL.Path,
		"surface":            metricSurfaceForPath(r.URL.Path),
		"org":                s.authnOrganization(r),
		"requestor":          strings.TrimSpace(r.Header.Get("X-Ops-Userid")),
		"server_api_version": serverAPIVersion,
		"error":              string(authErr.Kind),
		"message":            authErr.Message,
	}
	if len(authErr.Headers) > 0 {
		fields["headers"] = authErr.Headers
	}
	if serverHashedPath, ok := authn.LegacyHashedPathDebugValue(sign, r.URL.Path); ok {
		fields["server_hashed_path"] = serverHashedPath
	}
	s.logStructured("authn_failure", fields)
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

func routeIDFromContext(ctx context.Context) (string, bool) {
	value := ctx.Value(authenticatedRouteIDContextKey)
	routeID, ok := value.(string)
	return routeID, ok
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
	case "/server_api_version", "/server_api_version/", "/_blob/checksums/{checksum}", "/_blob/checksums/{checksum}/", "/users", "/users/", "/users/{name}/keys", "/users/{name}/keys/", "/organizations", "/organizations/", "/clients", "/clients/", "/clients/{name}/keys", "/clients/{name}/keys/", "/cookbooks", "/cookbooks/", "/cookbooks/_latest", "/cookbooks/_latest/", "/cookbooks/_recipes", "/cookbooks/_recipes/", "/cookbooks/{name}", "/cookbooks/{name}/", "/cookbooks/{name}/{version}", "/cookbooks/{name}/{version}/", "/cookbook_artifacts", "/cookbook_artifacts/", "/cookbook_artifacts/{name}", "/cookbook_artifacts/{name}/", "/cookbook_artifacts/{name}/{identifier}", "/cookbook_artifacts/{name}/{identifier}/", "/universe", "/universe/", "/data", "/data/", "/organizations/{org}/data", "/organizations/{org}/data/", "/organizations/{org}/cookbooks", "/organizations/{org}/cookbooks/", "/organizations/{org}/cookbooks/_latest", "/organizations/{org}/cookbooks/_latest/", "/organizations/{org}/cookbooks/_recipes", "/organizations/{org}/cookbooks/_recipes/", "/organizations/{org}/cookbooks/{name}", "/organizations/{org}/cookbooks/{name}/", "/organizations/{org}/cookbooks/{name}/{version}", "/organizations/{org}/cookbooks/{name}/{version}/", "/organizations/{org}/cookbook_artifacts", "/organizations/{org}/cookbook_artifacts/", "/organizations/{org}/cookbook_artifacts/{name}", "/organizations/{org}/cookbook_artifacts/{name}/", "/organizations/{org}/cookbook_artifacts/{name}/{identifier}", "/organizations/{org}/cookbook_artifacts/{name}/{identifier}/", "/organizations/{org}/universe", "/organizations/{org}/universe/", "/environments", "/environments/", "/environments/{name}/cookbooks", "/environments/{name}/cookbooks/", "/environments/{name}/cookbooks/{cookbook}", "/environments/{name}/cookbook_versions", "/environments/{name}/cookbook_versions/", "/environments/{name}/nodes", "/environments/{name}/nodes/", "/environments/{name}/roles/{role}", "/environments/{name}/roles/{role}/", "/environments/{name}/recipes", "/environments/{name}/recipes/", "/organizations/{org}/environments", "/organizations/{org}/environments/", "/organizations/{org}/environments/{name}/cookbooks", "/organizations/{org}/environments/{name}/cookbooks/", "/organizations/{org}/environments/{name}/cookbooks/{cookbook}", "/organizations/{org}/environments/{name}/cookbook_versions", "/organizations/{org}/environments/{name}/cookbook_versions/", "/organizations/{org}/environments/{name}/nodes", "/organizations/{org}/environments/{name}/nodes/", "/organizations/{org}/environments/{name}/roles/{role}", "/organizations/{org}/environments/{name}/roles/{role}/", "/organizations/{org}/environments/{name}/recipes", "/organizations/{org}/environments/{name}/recipes/", "/nodes", "/nodes/", "/organizations/{org}/nodes", "/organizations/{org}/nodes/", "/policies", "/policies/", "/policies/{name}", "/policies/{name}/", "/policies/{name}/revisions", "/policies/{name}/revisions/", "/policies/{name}/revisions/{revision}", "/policies/{name}/revisions/{revision}/", "/policy_groups", "/policy_groups/", "/policy_groups/{group}", "/policy_groups/{group}/", "/policy_groups/{group}/policies/{name}", "/policy_groups/{group}/policies/{name}/", "/organizations/{org}/policies", "/organizations/{org}/policies/", "/organizations/{org}/policies/{name}", "/organizations/{org}/policies/{name}/", "/organizations/{org}/policies/{name}/revisions", "/organizations/{org}/policies/{name}/revisions/", "/organizations/{org}/policies/{name}/revisions/{revision}", "/organizations/{org}/policies/{name}/revisions/{revision}/", "/organizations/{org}/policy_groups", "/organizations/{org}/policy_groups/", "/organizations/{org}/policy_groups/{group}", "/organizations/{org}/policy_groups/{group}/", "/organizations/{org}/policy_groups/{group}/policies/{name}", "/organizations/{org}/policy_groups/{group}/policies/{name}/", "/search", "/search/", "/search/{index}", "/search/{index}/", "/organizations/{org}/search", "/organizations/{org}/search/", "/organizations/{org}/search/{index}", "/organizations/{org}/search/{index}/", "/sandboxes", "/sandboxes/", "/sandboxes/{id}", "/sandboxes/{id}/", "/organizations/{org}/sandboxes", "/organizations/{org}/sandboxes/", "/organizations/{org}/sandboxes/{id}", "/organizations/{org}/sandboxes/{id}/", "/roles", "/roles/", "/roles/{name}/environments", "/roles/{name}/environments/", "/roles/{name}/environments/{environment}", "/roles/{name}/environments/{environment}/", "/organizations/{org}/roles", "/organizations/{org}/roles/", "/organizations/{org}/roles/{name}/environments", "/organizations/{org}/roles/{name}/environments/", "/organizations/{org}/roles/{name}/environments/{environment}", "/organizations/{org}/roles/{name}/environments/{environment}/", "/organizations/{org}/clients", "/organizations/{org}/clients/", "/organizations/{org}/clients/{name}/keys", "/organizations/{org}/clients/{name}/keys/":
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

type decodeJSONResult int

const (
	decodeJSONOK decodeJSONResult = iota
	decodeJSONInvalid
	decodeJSONMultipleDocuments
)

func decodeJSONInto(r *http.Request, payload any) decodeJSONResult {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(payload); err != nil {
		return decodeJSONInvalid
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return decodeJSONMultipleDocuments
	}

	return decodeJSONOK
}

func decodeJSON(w http.ResponseWriter, r *http.Request, payload any) bool {
	switch decodeJSONInto(r, payload) {
	case decodeJSONOK:
		return true
	case decodeJSONInvalid:
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_json",
			Message: "request body must be valid JSON",
		})
		return false
	case decodeJSONMultipleDocuments:
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_json",
			Message: "request body must contain exactly one JSON document",
		})
		return false
	default:
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_json",
			Message: "request body must be valid JSON",
		})
		return false
	}
}

type readinessPayload struct {
	Ready   bool                      `json:"ready"`
	Status  string                    `json:"status"`
	Message string                    `json:"message"`
	Checks  map[string]readinessCheck `json:"checks"`
}

type readinessCheck struct {
	Ready   bool   `json:"ready"`
	Message string `json:"message"`
}

func (s *server) statusPayload(mode string) map[string]any {
	return s.statusPayloadWithReadiness(mode, s.readinessPayload())
}

func (s *server) statusPayloadWithReadiness(mode string, readiness readinessPayload) map[string]any {
	cookbookBackend := s.deps.CookbookBackend
	if strings.TrimSpace(cookbookBackend) == "" {
		cookbookBackend = "memory-bootstrap"
	}
	authnBackend := "unconfigured"
	authnCapabilities := authn.Capabilities{}
	if s.deps.Authn != nil {
		authnBackend = s.deps.Authn.Name()
		authnCapabilities = s.deps.Authn.Capabilities()
	}
	authzBackend := "unconfigured"
	if s.deps.Authz != nil {
		authzBackend = s.deps.Authz.Name()
	}

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
		"readiness": readiness,
		"dependencies": map[string]any{
			"cookbooks": map[string]string{
				"backend": cookbookBackend,
			},
			"authn": map[string]string{
				"backend": authnBackend,
			},
			"authn_capabilities": authnCapabilities,
			"authz": map[string]string{
				"backend": authzBackend,
			},
			"postgres":    s.postgresStatus(),
			"opensearch":  s.searchStatus(),
			"blob":        s.blobStatus(),
			"maintenance": s.maintenanceStatus(),
		},
	}
}

// readinessPayload summarizes whether the current runtime can serve traffic
// with either configured providers or intentional in-memory compatibility
// fallbacks. It avoids embedding provider endpoints or credentials.
func (s *server) readinessPayload() readinessPayload {
	checks := map[string]readinessCheck{
		"bootstrap":   s.bootstrapReadiness(),
		"postgres":    s.postgresReadiness(),
		"opensearch":  s.searchReadiness(),
		"blob":        s.blobReadiness(),
		"maintenance": s.maintenanceReadiness(),
	}

	ready := true
	for _, check := range checks {
		if !check.Ready {
			ready = false
			break
		}
	}

	if ready {
		return readinessPayload{
			Ready:   true,
			Status:  "ready",
			Message: "OpenCook is ready to serve Chef-compatible requests with the active dependency configuration",
			Checks:  checks,
		}
	}
	return readinessPayload{
		Ready:   false,
		Status:  "not_ready",
		Message: "one or more configured operational dependencies are unavailable or not active",
		Checks:  checks,
	}
}

func (s *server) bootstrapReadiness() readinessCheck {
	if s.deps.Bootstrap == nil {
		return readinessCheck{Ready: false, Message: "bootstrap state service is not configured"}
	}
	return readinessCheck{Ready: true, Message: "bootstrap state service is active"}
}

func (s *server) postgresReadiness() readinessCheck {
	if s.deps.Postgres == nil {
		return readinessCheck{Ready: false, Message: "PostgreSQL store dependency is not configured"}
	}
	if !s.deps.Postgres.Configured() {
		return readinessCheck{Ready: true, Message: "PostgreSQL is not configured; in-memory persistence fallback is active"}
	}
	if s.deps.Postgres.CookbookPersistenceActive() && s.deps.Postgres.BootstrapCorePersistenceActive() && s.deps.Postgres.CoreObjectPersistenceActive() && s.deps.Postgres.MaintenancePersistenceActive() {
		return readinessCheck{Ready: true, Message: "PostgreSQL-backed cookbook, bootstrap core, core object, and maintenance state persistence is active"}
	}
	return readinessCheck{Ready: false, Message: "PostgreSQL is configured but persistence activation is not active"}
}

func (s *server) searchReadiness() readinessCheck {
	status := s.searchStatus()
	if status.Backend == "unavailable" {
		return readinessCheck{Ready: false, Message: status.Message}
	}
	if status.Backend == "memory-compat" {
		if !status.Configured {
			return readinessCheck{Ready: false, Message: status.Message}
		}
		return readinessCheck{Ready: true, Message: status.Message}
	}
	if status.Backend == "opensearch" {
		if !status.Configured || strings.Contains(strings.ToLower(status.Message), "unavailable") {
			return readinessCheck{Ready: false, Message: "OpenSearch is configured but unavailable"}
		}
		return readinessCheck{Ready: true, Message: status.Message}
	}
	if !status.Configured {
		return readinessCheck{Ready: false, Message: status.Message}
	}
	return readinessCheck{Ready: true, Message: status.Message}
}

func (s *server) blobReadiness() readinessCheck {
	status := s.blobStatus()
	if !status.Configured {
		return readinessCheck{Ready: false, Message: status.Message}
	}
	return readinessCheck{Ready: true, Message: status.Message}
}

// maintenanceReadiness reports whether the write gate can be evaluated. Active
// maintenance does not make the server unready because reads remain available.
func (s *server) maintenanceReadiness() readinessCheck {
	status := s.maintenanceStatus()
	if status["status"] == "error" {
		message, _ := status["message"].(string)
		return readinessCheck{Ready: false, Message: message}
	}
	message, _ := status["message"].(string)
	return readinessCheck{Ready: true, Message: message}
}

func (s *server) postgresStatus() pg.Status {
	if s.deps.Postgres == nil {
		return pg.Status{
			Driver:     "postgres",
			Configured: false,
			Message:    "PostgreSQL store dependency is not configured",
		}
	}
	return s.deps.Postgres.Status()
}

func (s *server) searchStatus() search.Status {
	if s.deps.Search == nil {
		return search.Status{
			Backend:    "unavailable",
			Configured: false,
			Message:    "search index dependency is not configured",
		}
	}
	return s.deps.Search.Status()
}

func (s *server) blobStatus() blob.Status {
	if s.deps.Blob == nil {
		return blob.Status{
			Backend:    "unavailable",
			Configured: false,
			Message:    "blob store dependency is not configured",
		}
	}
	return s.deps.Blob.Status()
}

// maintenanceStatus builds an operator-safe dependency block for /_status,
// /readyz, and metrics. It exposes bounded state only and never returns raw
// provider errors, DSNs, signatures, or unbounded operator notes.
func (s *server) maintenanceStatus() map[string]any {
	backend, configured, shared, message := s.maintenanceBackend()
	status := map[string]any{
		"backend":    backend,
		"configured": configured,
		"shared":     shared,
		"active":     false,
		"expired":    false,
		"message":    message,
		"status":     "disabled",
	}
	if s.deps.Maintenance == nil {
		status["status"] = "error"
		status["message"] = "maintenance store dependency is not configured"
		return status
	}

	check, err := s.deps.Maintenance.Check(context.Background())
	if err != nil {
		status["status"] = "error"
		status["message"] = "maintenance state could not be checked; mutating writes fail closed"
		return status
	}
	status["active"] = check.Active
	status["expired"] = check.Expired
	status["checked_at"] = check.CheckedAt
	status["state"] = check.State.SafeStatus()
	switch {
	case check.Active:
		status["status"] = "enabled"
		status["message"] = message + "; mutating Chef-facing writes are currently blocked"
	case check.State.Enabled && check.Expired:
		status["status"] = "expired"
		status["message"] = message + "; stored maintenance window has expired and no longer blocks writes"
	default:
		status["status"] = "disabled"
	}
	return status
}

// maintenanceBackend describes whether the write gate is shared through
// PostgreSQL or process-local memory, without exposing configuration secrets.
func (s *server) maintenanceBackend() (backend string, configured bool, shared bool, message string) {
	if s.deps.Postgres != nil && s.deps.Postgres.MaintenancePersistenceActive() {
		return "postgres", true, true, "PostgreSQL-backed maintenance state is shared across OpenCook processes"
	}
	if s.deps.Postgres != nil && s.deps.Postgres.Configured() {
		return "postgres-configured", true, false, "PostgreSQL is configured but maintenance persistence is not active; writes fail closed if state cannot be checked"
	}
	return "memory", false, false, "process-local maintenance state is active for this OpenCook process only"
}
