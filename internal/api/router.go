package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

type Dependencies struct {
	Logger   *log.Logger
	Config   config.Config
	Version  version.Info
	Compat   compat.Registry
	Authn    authn.Verifier
	Authz    authz.Authorizer
	Blob     blob.Store
	Search   search.Index
	Postgres *pg.Store
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
	mux.HandleFunc("/users", srv.withAuthn("users-root", srv.handleUsers))
	mux.HandleFunc("/users/", srv.withAuthn("users-named", srv.handleUsers))
	mux.HandleFunc("/organizations/{org}/clients", srv.withAuthn("org-clients", srv.handleOrgClients))
	mux.HandleFunc("/organizations/{org}/clients/", srv.withAuthn("org-client-named", srv.handleOrgClients))

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
		"phase":         "scaffold",
		"version":       s.deps.Version,
		"compat_routes": s.deps.Compat.RouteCount(),
		"next":          "implement request signing compatibility slice",
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

	switch r.Method {
	case http.MethodGet:
		if matchesCollectionPath(r.URL.Path, "/users") {
			writeJSON(w, http.StatusOK, map[string]any{
				"requestor": requestor,
				"users": map[string]string{
					requestor.Name: "/users/" + requestor.Name,
				},
				"note": "authentication is wired; persistent user storage is not implemented yet",
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

		writeJSON(w, http.StatusOK, map[string]any{
			"name":           name,
			"requestor":      requestor,
			"uri":            "/users/" + name,
			"authn_status":   "verified",
			"storage_status": "stub",
			"compatibility":  "wire-compatible authn path in progress",
		})
	default:
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":   "not_implemented",
			"message": "authenticated users endpoint write flows are not implemented yet",
			"method":  r.Method,
			"path":    r.URL.Path,
		})
	}
}

func (s *server) handleOrgClients(w http.ResponseWriter, r *http.Request) {
	requestor, _ := requestorFromContext(r.Context())
	org := r.PathValue("org")
	collectionPath := "/organizations/" + org + "/clients"

	switch r.Method {
	case http.MethodGet:
		if matchesCollectionPath(r.URL.Path, collectionPath) {
			writeJSON(w, http.StatusOK, map[string]any{
				"organization": org,
				"requestor":    requestor,
				"clients":      map[string]string{},
				"note":         "authentication is wired; persistent client storage is not implemented yet",
			})
			return
		}

		name := strings.TrimPrefix(r.URL.Path, collectionPath+"/")
		if name == "" || strings.Contains(name, "/") {
			writeJSON(w, http.StatusNotFound, apiError{
				Error:   "not_found",
				Message: "route not found in scaffold router",
			})
			return
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"organization":   org,
			"name":           name,
			"requestor":      requestor,
			"uri":            "/organizations/" + org + "/clients/" + name,
			"authn_status":   "verified",
			"storage_status": "stub",
		})
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
			Organization:     r.PathValue("org"),
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
	case "/users", "/users/", "/organizations/{org}/clients", "/organizations/{org}/clients/":
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

func (s *server) statusPayload(mode string) map[string]any {
	return map[string]any{
		"mode":        mode,
		"service":     s.deps.Config.ServiceName,
		"environment": s.deps.Config.Environment,
		"phase":       "scaffold",
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
