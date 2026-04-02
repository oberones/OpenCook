package api

import (
	"log"
	"net/http"

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

func NewRouter(deps Dependencies) http.Handler {
	srv := &server{deps: deps}
	mux := http.NewServeMux()

	mux.HandleFunc("/", srv.handleRoot)
	mux.HandleFunc("/_status", srv.handleStatus)
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/readyz", srv.handleReady)
	mux.HandleFunc("/internal/contracts/routes", srv.handleRouteContract)

	for _, surface := range deps.Compat.Surfaces() {
		surface := surface
		for _, pattern := range surface.Patterns {
			pattern := pattern
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
		"service":      s.deps.Config.ServiceName,
		"phase":        "scaffold",
		"version":      s.deps.Version,
		"compat_routes": s.deps.Compat.RouteCount(),
		"next":         "implement request signing compatibility slice",
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

func (s *server) handleNotImplemented(surface compat.Surface, pattern string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotImplemented, map[string]any{
			"error":    "not_implemented",
			"message":  "compatibility surface scaffolded but not implemented",
			"method":   r.Method,
			"path":     r.URL.Path,
			"pattern":  pattern,
			"surface":  surface.Name,
			"owner":    surface.Owner,
			"phase":    surface.Phase,
			"notes":    surface.Notes,
		})
	}
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
			"authz": map[string]string{
				"backend": s.deps.Authz.Name(),
			},
			"postgres":   s.deps.Postgres.Status(),
			"opensearch": s.deps.Search.Status(),
			"blob":       s.deps.Blob.Status(),
		},
	}
}

