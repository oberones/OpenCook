package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
)

const (
	serverAPIVersionHeader = "X-Ops-Server-API-Version"
	minServerAPIVersion    = 0
	maxServerAPIVersion    = 2
)

type serverAPIVersionContextKey struct{}

type requestedServerAPIVersion struct {
	Raw      string
	Version  int
	Explicit bool
}

// withServerAPIVersion applies Chef-style API-version negotiation before a
// route performs method, body, lookup, authorization, or handler validation.
func (s *server) withServerAPIVersion(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r, ok := s.validateServerAPIVersion(w, r)
		if !ok {
			return
		}
		next(w, r)
	}
}

func (s *server) validateServerAPIVersion(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	requested, ok := parseRequestedServerAPIVersion(r)
	if !ok {
		writeInvalidServerAPIVersion(w, requested.Raw)
		return r, false
	}

	ctx := context.WithValue(r.Context(), serverAPIVersionContextKey{}, requested)
	return r.WithContext(ctx), true
}

func parseRequestedServerAPIVersion(r *http.Request) (requestedServerAPIVersion, bool) {
	raw := strings.TrimSpace(r.Header.Get(serverAPIVersionHeader))
	if raw == "" {
		return requestedServerAPIVersion{Version: minServerAPIVersion}, true
	}

	version, err := strconv.Atoi(raw)
	if err != nil || version < minServerAPIVersion || version > maxServerAPIVersion {
		return requestedServerAPIVersion{Raw: raw, Explicit: true}, false
	}

	return requestedServerAPIVersion{
		Raw:      raw,
		Version:  version,
		Explicit: true,
	}, true
}

func serverAPIVersionFromContext(r *http.Request) requestedServerAPIVersion {
	if requested, ok := r.Context().Value(serverAPIVersionContextKey{}).(requestedServerAPIVersion); ok {
		return requested
	}
	return requestedServerAPIVersion{Version: minServerAPIVersion}
}

func writeInvalidServerAPIVersion(w http.ResponseWriter, requested string) {
	writeJSON(w, http.StatusNotAcceptable, map[string]any{
		"error":       "invalid-x-ops-server-api-version",
		"message":     "Specified version " + requested + " not supported",
		"min_version": minServerAPIVersion,
		"max_version": maxServerAPIVersion,
	})
}

func (s *server) handleServerAPIVersion(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/server_api_version" && r.URL.Path != "/server_api_version/" {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, apiError{
			Error:   "method_not_allowed",
			Message: "method is not allowed for this route",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"min_api_version": minServerAPIVersion,
		"max_api_version": maxServerAPIVersion,
	})
}
