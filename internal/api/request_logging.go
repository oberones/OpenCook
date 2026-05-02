package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/maintenance"
)

const (
	requestIDContextKey contextKey = "request_id"
	requestIDHeader     string     = "X-Request-Id"
	maxRequestIDLength             = 128
)

// withRequestLogging adds an operator-facing request ID and emits one
// structured request log entry per HTTP request. The log intentionally records
// only bounded route metadata, status, duration, and byte counts; it never logs
// request bodies, Chef signatures, private keys, or raw query strings.
func (s *server) withRequestLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := requestIDForRequest(r)
		w.Header().Set(requestIDHeader, requestID)
		ctx := context.WithValue(r.Context(), requestIDContextKey, requestID)
		r = r.WithContext(ctx)

		start := s.deps.Now()
		rec := &operationalResponseWriter{ResponseWriter: w}
		defer func() {
			status := rec.statusCode()
			if recovered := recover(); recovered != nil {
				status = http.StatusInternalServerError
				s.logHTTPRequest(r, requestID, status, rec.bytesWritten, s.deps.Now().Sub(start))
				panic(recovered)
			}
			s.logHTTPRequest(r, requestID, status, rec.bytesWritten, s.deps.Now().Sub(start))
		}()

		next.ServeHTTP(rec, r)
	})
}

type operationalResponseWriter struct {
	http.ResponseWriter
	status       int
	wroteHeader  bool
	bytesWritten int64
}

// WriteHeader captures the response status for the structured request log while
// preserving net/http's normal first-header-wins behavior.
func (w *operationalResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

// Write records the response byte count without inspecting or logging the
// response body itself.
func (w *operationalResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	w.bytesWritten += int64(n)
	return n, err
}

// Unwrap keeps the logging wrapper transparent to future ResponseController
// users, matching the metrics wrapper behavior.
func (w *operationalResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// statusCode returns net/http's implicit 200 status when a handler completed
// without explicitly writing headers.
func (w *operationalResponseWriter) statusCode() int {
	if w == nil || w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// logHTTPRequest writes the per-request operational record as JSON so external
// log collectors can filter by request_id, surface, and status_class without
// parsing Chef-facing response bodies.
func (s *server) logHTTPRequest(r *http.Request, requestID string, status int, bytesWritten int64, duration time.Duration) {
	if r == nil {
		return
	}
	s.logStructured("http_request", map[string]any{
		"request_id":     requestID,
		"method":         metricMethod(r.Method),
		"surface":        metricSurfaceForPath(r.URL.Path),
		"path_shape":     operationalPathShape(r.URL.Path),
		"status":         status,
		"status_class":   metricStatusClass(status),
		"duration_ms":    duration.Seconds() * 1000,
		"content_length": r.ContentLength,
		"response_bytes": bytesWritten,
	})
}

// logMaintenanceBlocked emits a separate low-cardinality event when the
// maintenance gate prevents a write from reaching a Chef-facing handler.
func (s *server) logMaintenanceBlocked(r *http.Request, pattern, reason string, state maintenance.State) {
	if r == nil {
		return
	}
	requestID, _ := requestIDFromContext(r.Context())
	safe := state.SafeStatus()
	fields := map[string]any{
		"request_id":   requestID,
		"method":       metricMethod(r.Method),
		"surface":      metricSurfaceForPath(r.URL.Path),
		"path_shape":   operationalPathShape(r.URL.Path),
		"pattern":      pattern,
		"reason":       metricMaintenanceReason(reason),
		"status":       maintenanceBlockedHTTPStatus,
		"status_class": metricStatusClass(maintenanceBlockedHTTPStatus),
	}
	if safe.Mode != "" {
		fields["maintenance_mode"] = safe.Mode
	}
	if safe.ExpiresAt != nil {
		fields["maintenance_expires_at"] = safe.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	s.logStructured("maintenance_write_blocked", fields)
}

// logStructured emits a JSON event through the configured server logger. It is
// intentionally small and explicit so callers choose safe fields instead of
// dumping headers, request URLs, bodies, or provider configuration.
func (s *server) logStructured(event string, fields map[string]any) {
	if s == nil || s.deps.Logger == nil {
		return
	}
	entry := make(map[string]any, len(fields)+2)
	entry["event"] = event
	entry["time"] = s.deps.Now().UTC().Format(time.RFC3339Nano)
	for key, value := range fields {
		entry[key] = value
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		s.deps.Logger.Printf("event=%q log_marshal_error=%q", event, err.Error())
		return
	}
	s.deps.Logger.Print(string(payload))
}

// requestIDFromContext returns the request ID generated or preserved by the
// request logging middleware.
func requestIDFromContext(ctx context.Context) (string, bool) {
	value := ctx.Value(requestIDContextKey)
	requestID, ok := value.(string)
	return requestID, ok && requestID != ""
}

// requestIDForRequest preserves a safe upstream request ID when present and
// otherwise generates a new OpenCook-scoped ID.
func requestIDForRequest(r *http.Request) string {
	if r != nil {
		if requestID, ok := cleanRequestID(r.Header.Get(requestIDHeader)); ok {
			return requestID
		}
	}
	return generateRequestID()
}

// cleanRequestID accepts common request ID characters while rejecting control
// characters and oversized values that could make logs or response headers hard
// to parse safely.
func cleanRequestID(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > maxRequestIDLength {
		return "", false
	}
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			continue
		}
		switch r {
		case '-', '_', '.', ':':
			continue
		default:
			return "", false
		}
	}
	return raw, true
}

// generateRequestID creates an opaque correlation ID for logs and response
// headers. It falls back to time if the OS random source is unavailable; the ID
// is diagnostic, not an authentication secret.
func generateRequestID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return "oc-" + hex.EncodeToString(buf[:])
	}
	return fmt.Sprintf("oc-%x", time.Now().UTC().UnixNano())
}

// operationalPathShape keeps request logs useful without recording object
// names, organization names, checksums, query strings, or signed URL material.
func operationalPathShape(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "/"
	}
	parts := strings.Split(trimmed, "/")
	if parts[0] == "organizations" {
		if len(parts) == 1 {
			return "/organizations"
		}
		if len(parts) == 2 {
			return "/organizations/:org"
		}
		return "/" + strings.Join(append([]string{"organizations", ":org"}, operationalTailShape(parts[2:])...), "/")
	}
	return "/" + strings.Join(operationalTailShape(parts), "/")
}

// operationalTailShape preserves stable route keywords and redacts dynamic
// segments after the surface name.
func operationalTailShape(parts []string) []string {
	out := make([]string, 0, len(parts))
	for i, part := range parts {
		if i == 0 || operationalStablePathSegment(part) {
			out = append(out, part)
			continue
		}
		out = append(out, ":value")
	}
	return out
}

// operationalStablePathSegment recognizes literal route words that are useful
// in logs and safe to retain.
func operationalStablePathSegment(segment string) bool {
	switch segment {
	case "_acl", "_latest", "_recipes", "checksums", "clients", "cookbooks", "cookbook_versions", "containers", "data", "environments", "groups", "keys", "nodes", "policies", "policy_groups", "recipes", "revisions", "roles", "sandboxes", "search", "universe":
		return true
	default:
		return false
	}
}
