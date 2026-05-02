package api

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type metricsRegistry struct {
	mu sync.Mutex

	startedAt time.Time
	buckets   []float64

	requests        map[requestMetricKey]uint64
	durationBuckets map[durationMetricKey][]uint64
	durationCount   map[durationMetricKey]uint64
	durationSum     map[durationMetricKey]float64
	blockedWrites   map[maintenanceBlockedMetricKey]uint64
}

type requestMetricKey struct {
	Method      string
	Surface     string
	StatusClass string
}

type durationMetricKey struct {
	Method  string
	Surface string
}

type maintenanceBlockedMetricKey struct {
	Method  string
	Surface string
	Reason  string
}

type metricsSnapshot struct {
	startedAt time.Time
	buckets   []float64

	requests        map[requestMetricKey]uint64
	durationBuckets map[durationMetricKey][]uint64
	durationCount   map[durationMetricKey]uint64
	durationSum     map[durationMetricKey]float64
	blockedWrites   map[maintenanceBlockedMetricKey]uint64
}

// newMetricsRegistry creates the in-process metrics recorder used by the HTTP
// router. It keeps all labels bounded so operators can scrape OpenCook without
// exporting Chef object names, organization names, signatures, or provider URLs.
func newMetricsRegistry(now func() time.Time) *metricsRegistry {
	if now == nil {
		now = time.Now
	}
	return &metricsRegistry{
		startedAt:       now(),
		buckets:         []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		requests:        make(map[requestMetricKey]uint64),
		durationBuckets: make(map[durationMetricKey][]uint64),
		durationCount:   make(map[durationMetricKey]uint64),
		durationSum:     make(map[durationMetricKey]float64),
		blockedWrites:   make(map[maintenanceBlockedMetricKey]uint64),
	}
}

// record stores a single HTTP observation using a route surface instead of the
// raw path. That preserves useful operational signal while avoiding unbounded
// Prometheus cardinality from org names, cookbook names, checksums, or queries.
func (m *metricsRegistry) record(method, path string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	method = metricMethod(method)
	surface := metricSurfaceForPath(path)
	statusClass := metricStatusClass(status)
	seconds := duration.Seconds()

	m.mu.Lock()
	defer m.mu.Unlock()

	m.requests[requestMetricKey{Method: method, Surface: surface, StatusClass: statusClass}]++
	durationKey := durationMetricKey{Method: method, Surface: surface}
	if _, ok := m.durationBuckets[durationKey]; !ok {
		m.durationBuckets[durationKey] = make([]uint64, len(m.buckets))
	}
	for i, bucket := range m.buckets {
		if seconds <= bucket {
			m.durationBuckets[durationKey][i]++
		}
	}
	m.durationCount[durationKey]++
	m.durationSum[durationKey] += seconds
}

// recordMaintenanceBlocked stores one bounded observation when maintenance mode
// prevents a mutating Chef-facing request from reaching the route handler.
func (m *metricsRegistry) recordMaintenanceBlocked(method, path, reason string) {
	if m == nil {
		return
	}
	key := maintenanceBlockedMetricKey{
		Method:  metricMethod(method),
		Surface: metricSurfaceForPath(path),
		Reason:  metricMaintenanceReason(reason),
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.blockedWrites[key]++
}

// snapshot copies the current counters so Prometheus rendering never holds the
// metrics lock while it evaluates dependency status or writes the response body.
func (m *metricsRegistry) snapshot() metricsSnapshot {
	if m == nil {
		return metricsSnapshot{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	requests := make(map[requestMetricKey]uint64, len(m.requests))
	for key, count := range m.requests {
		requests[key] = count
	}
	durationBuckets := make(map[durationMetricKey][]uint64, len(m.durationBuckets))
	for key, counts := range m.durationBuckets {
		durationBuckets[key] = append([]uint64(nil), counts...)
	}
	durationCount := make(map[durationMetricKey]uint64, len(m.durationCount))
	for key, count := range m.durationCount {
		durationCount[key] = count
	}
	durationSum := make(map[durationMetricKey]float64, len(m.durationSum))
	for key, sum := range m.durationSum {
		durationSum[key] = sum
	}
	blockedWrites := make(map[maintenanceBlockedMetricKey]uint64, len(m.blockedWrites))
	for key, count := range m.blockedWrites {
		blockedWrites[key] = count
	}

	return metricsSnapshot{
		startedAt:       m.startedAt,
		buckets:         append([]float64(nil), m.buckets...),
		requests:        requests,
		durationBuckets: durationBuckets,
		durationCount:   durationCount,
		durationSum:     durationSum,
		blockedWrites:   blockedWrites,
	}
}

// withMetrics wraps every route with request counters and latency histograms.
// It records after the handler completes so status classes reflect the actual
// response Chef/Cinc clients saw, without changing the handler's body contract.
func (s *server) withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.deps.Now()
		rec := &metricsResponseWriter{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			status := rec.statusCode()
			if recovered := recover(); recovered != nil {
				status = http.StatusInternalServerError
				s.metrics.record(r.Method, r.URL.Path, status, s.deps.Now().Sub(start))
				panic(recovered)
			}
			s.metrics.record(r.Method, r.URL.Path, status, s.deps.Now().Sub(start))
		}()

		next.ServeHTTP(rec, r)
	})
}

type metricsResponseWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

// WriteHeader captures the first status code while preserving the wrapped
// ResponseWriter semantics for the actual client response.
func (w *metricsResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(status)
}

// Write preserves the implicit HTTP 200 behavior used by net/http when a
// handler writes a body without calling WriteHeader first.
func (w *metricsResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

// Unwrap exposes the original writer to ResponseController-aware code so the
// instrumentation wrapper stays transparent for future streaming extensions.
func (w *metricsResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// statusCode returns the captured status, defaulting to net/http's implicit
// 200 behavior for handlers that neither wrote a body nor a status header.
func (w *metricsResponseWriter) statusCode() int {
	if w == nil || w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// handleMetrics emits Prometheus text format for operational scraping. The
// endpoint is additive and intentionally separate from Chef-facing routes.
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeMethodNotAllowed(w, "method not allowed for metrics route", http.MethodGet, http.MethodHead)
		return
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write([]byte(s.renderMetrics()))
}

// renderMetrics builds a deterministic Prometheus text payload using only safe,
// low-cardinality labels.
func (s *server) renderMetrics() string {
	snapshot := s.metrics.snapshot()
	var b strings.Builder

	s.writeBuildMetrics(&b, snapshot)
	s.writeHTTPMetrics(&b, snapshot)
	s.writeDerivedOperationMetrics(&b, snapshot)
	s.writeMaintenanceMetrics(&b, snapshot)
	s.writeDependencyMetrics(&b)

	return b.String()
}

// writeBuildMetrics publishes process-level metadata and uptime without
// including paths, environment variables, or configuration values.
func (s *server) writeBuildMetrics(b *strings.Builder, snapshot metricsSnapshot) {
	fmt.Fprintln(b, "# HELP opencook_build_info Build metadata for the running OpenCook process.")
	fmt.Fprintln(b, "# TYPE opencook_build_info gauge")
	fmt.Fprintf(b, "opencook_build_info{version=%s,commit=%s,built_at=%s} 1\n",
		prometheusLabel(s.deps.Version.Version),
		prometheusLabel(s.deps.Version.Commit),
		prometheusLabel(s.deps.Version.BuiltAt),
	)
	fmt.Fprintln(b, "# HELP opencook_uptime_seconds Seconds since the OpenCook HTTP router was constructed.")
	fmt.Fprintln(b, "# TYPE opencook_uptime_seconds gauge")
	uptime := s.deps.Now().Sub(snapshot.startedAt).Seconds()
	if uptime < 0 {
		uptime = 0
	}
	fmt.Fprintf(b, "opencook_uptime_seconds %.6f\n", uptime)
}

// writeHTTPMetrics renders request totals and duration histograms. The surface
// label is a fixed compatibility family rather than the incoming request path.
func (s *server) writeHTTPMetrics(b *strings.Builder, snapshot metricsSnapshot) {
	fmt.Fprintln(b, "# HELP opencook_http_requests_total HTTP requests served by OpenCook, grouped by safe route surface.")
	fmt.Fprintln(b, "# TYPE opencook_http_requests_total counter")
	for _, key := range sortedRequestMetricKeys(snapshot.requests) {
		fmt.Fprintf(b, "opencook_http_requests_total{method=%s,surface=%s,status_class=%s} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.Surface),
			prometheusLabel(key.StatusClass),
			snapshot.requests[key],
		)
	}

	fmt.Fprintln(b, "# HELP opencook_http_request_duration_seconds HTTP request latency histogram by safe route surface.")
	fmt.Fprintln(b, "# TYPE opencook_http_request_duration_seconds histogram")
	for _, key := range sortedDurationMetricKeys(snapshot.durationCount) {
		counts := snapshot.durationBuckets[key]
		for i, bucket := range snapshot.buckets {
			var count uint64
			if i < len(counts) {
				count = counts[i]
			}
			fmt.Fprintf(b, "opencook_http_request_duration_seconds_bucket{method=%s,surface=%s,le=%s} %d\n",
				prometheusLabel(key.Method),
				prometheusLabel(key.Surface),
				prometheusLabel(strconv.FormatFloat(bucket, 'f', -1, 64)),
				count,
			)
		}
		fmt.Fprintf(b, "opencook_http_request_duration_seconds_bucket{method=%s,surface=%s,le=\"+Inf\"} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.Surface),
			snapshot.durationCount[key],
		)
		fmt.Fprintf(b, "opencook_http_request_duration_seconds_sum{method=%s,surface=%s} %.9f\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.Surface),
			snapshot.durationSum[key],
		)
		fmt.Fprintf(b, "opencook_http_request_duration_seconds_count{method=%s,surface=%s} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.Surface),
			snapshot.durationCount[key],
		)
	}
}

// writeDerivedOperationMetrics gives operators focused search/blob counters
// without instrumenting provider internals or adding high-cardinality labels.
func (s *server) writeDerivedOperationMetrics(b *strings.Builder, snapshot metricsSnapshot) {
	fmt.Fprintln(b, "# HELP opencook_search_http_requests_total HTTP requests that reached the Chef search route family.")
	fmt.Fprintln(b, "# TYPE opencook_search_http_requests_total counter")
	for _, key := range sortedRequestMetricKeys(snapshot.requests) {
		if key.Surface != "search" {
			continue
		}
		fmt.Fprintf(b, "opencook_search_http_requests_total{method=%s,status_class=%s} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.StatusClass),
			snapshot.requests[key],
		)
	}

	fmt.Fprintln(b, "# HELP opencook_blob_http_requests_total HTTP requests that reached the checksum blob route family.")
	fmt.Fprintln(b, "# TYPE opencook_blob_http_requests_total counter")
	for _, key := range sortedRequestMetricKeys(snapshot.requests) {
		if key.Surface != "blob" {
			continue
		}
		fmt.Fprintf(b, "opencook_blob_http_requests_total{method=%s,status_class=%s} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.StatusClass),
			snapshot.requests[key],
		)
	}
}

// writeMaintenanceMetrics exposes write-gate state and blocked-write counts
// using only backend, method, surface, and reason labels.
func (s *server) writeMaintenanceMetrics(b *strings.Builder, snapshot metricsSnapshot) {
	status := s.maintenanceStatus()
	backend, _ := status["backend"].(string)
	shared, _ := status["shared"].(bool)
	active, _ := status["active"].(bool)
	expired, _ := status["expired"].(bool)

	fmt.Fprintln(b, "# HELP opencook_maintenance_enabled Whether maintenance mode is currently blocking mutating Chef-facing writes.")
	fmt.Fprintln(b, "# TYPE opencook_maintenance_enabled gauge")
	fmt.Fprintf(b, "opencook_maintenance_enabled{backend=%s,shared=%s} %d\n",
		prometheusLabel(metricBackendLabel(backend)),
		prometheusLabel(strconv.FormatBool(shared)),
		boolMetric(active),
	)
	fmt.Fprintln(b, "# HELP opencook_maintenance_expired Whether a stored maintenance window has expired and is no longer blocking writes.")
	fmt.Fprintln(b, "# TYPE opencook_maintenance_expired gauge")
	fmt.Fprintf(b, "opencook_maintenance_expired{backend=%s,shared=%s} %d\n",
		prometheusLabel(metricBackendLabel(backend)),
		prometheusLabel(strconv.FormatBool(shared)),
		boolMetric(expired),
	)

	fmt.Fprintln(b, "# HELP opencook_maintenance_blocked_writes_total Mutating Chef-facing requests blocked by maintenance mode.")
	fmt.Fprintln(b, "# TYPE opencook_maintenance_blocked_writes_total counter")
	for _, key := range sortedMaintenanceBlockedMetricKeys(snapshot.blockedWrites) {
		fmt.Fprintf(b, "opencook_maintenance_blocked_writes_total{method=%s,surface=%s,reason=%s} %d\n",
			prometheusLabel(key.Method),
			prometheusLabel(key.Surface),
			prometheusLabel(key.Reason),
			snapshot.blockedWrites[key],
		)
	}
}

// writeDependencyMetrics exposes configured/ready gauges for the runtime
// dependencies without exporting messages that could contain provider details.
func (s *server) writeDependencyMetrics(b *strings.Builder) {
	readiness := s.readinessPayload()
	maintenanceStatus := s.maintenanceStatus()
	dependencies := []struct {
		name       string
		backend    string
		configured bool
		ready      bool
	}{
		{
			name:       "bootstrap",
			backend:    "service",
			configured: s.deps.Bootstrap != nil,
			ready:      readiness.Checks["bootstrap"].Ready,
		},
		{
			name:       "postgres",
			backend:    s.postgresStatus().Driver,
			configured: s.postgresStatus().Configured,
			ready:      readiness.Checks["postgres"].Ready,
		},
		{
			name:       "opensearch",
			backend:    s.searchStatus().Backend,
			configured: s.searchStatus().Configured,
			ready:      readiness.Checks["opensearch"].Ready,
		},
		{
			name:       "blob",
			backend:    s.blobStatus().Backend,
			configured: s.blobStatus().Configured,
			ready:      readiness.Checks["blob"].Ready,
		},
		{
			name:       "maintenance",
			backend:    stringFromMap(maintenanceStatus, "backend"),
			configured: boolFromMap(maintenanceStatus, "configured"),
			ready:      readiness.Checks["maintenance"].Ready,
		},
	}

	fmt.Fprintln(b, "# HELP opencook_dependency_configured Whether an OpenCook runtime dependency is configured.")
	fmt.Fprintln(b, "# TYPE opencook_dependency_configured gauge")
	for _, dep := range dependencies {
		fmt.Fprintf(b, "opencook_dependency_configured{dependency=%s,backend=%s} %d\n",
			prometheusLabel(dep.name),
			prometheusLabel(metricBackendLabel(dep.backend)),
			boolMetric(dep.configured),
		)
	}

	fmt.Fprintln(b, "# HELP opencook_dependency_ready Whether an OpenCook runtime dependency is ready to serve traffic.")
	fmt.Fprintln(b, "# TYPE opencook_dependency_ready gauge")
	for _, dep := range dependencies {
		fmt.Fprintf(b, "opencook_dependency_ready{dependency=%s,backend=%s} %d\n",
			prometheusLabel(dep.name),
			prometheusLabel(metricBackendLabel(dep.backend)),
			boolMetric(dep.ready),
		)
	}
}

// metricMaintenanceReason bounds maintenance blocked-write reason labels to a
// tiny vocabulary so provider errors or operator text never reach Prometheus.
func metricMaintenanceReason(reason string) string {
	switch strings.TrimSpace(strings.ToLower(reason)) {
	case "active", "check_error":
		return strings.TrimSpace(strings.ToLower(reason))
	default:
		return "unknown"
	}
}

// metricSurfaceForPath maps request paths to bounded route families. It never
// returns org names, object names, blob checksums, search queries, or signatures.
func metricSurfaceForPath(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "root"
	}
	parts := strings.Split(trimmed, "/")
	if len(parts) == 0 {
		return "other"
	}
	first := parts[0]
	if first == "organizations" {
		if len(parts) < 3 {
			return "organizations"
		}
		return metricSurfaceForSegment(parts[2])
	}
	return metricSurfaceForSegment(first)
}

// metricSurfaceForSegment translates the first stable route segment into the
// compatibility family operators expect to monitor.
func metricSurfaceForSegment(segment string) string {
	switch segment {
	case "_blob":
		return "blob"
	case "_status":
		return "status"
	case "healthz":
		return "health"
	case "readyz":
		return "ready"
	case "metrics":
		return "metrics"
	case "server_api_version":
		return "server_api_version"
	case "internal":
		return "internal"
	case "cookbooks":
		return "cookbooks"
	case "cookbook_artifacts":
		return "cookbook_artifacts"
	case "clients":
		return "clients"
	case "groups":
		return "groups"
	case "containers":
		return "containers"
	case "_acl":
		return "acls"
	case "data":
		return "data_bags"
	case "environments":
		return "environments"
	case "nodes":
		return "nodes"
	case "policies":
		return "policies"
	case "policy_groups":
		return "policy_groups"
	case "roles":
		return "roles"
	case "sandboxes":
		return "sandboxes"
	case "search":
		return "search"
	case "universe":
		return "universe"
	case "users":
		return "users"
	default:
		return "other"
	}
}

// metricMethod keeps the method label bounded while preserving the verbs used
// by Chef-compatible routes.
func metricMethod(method string) string {
	switch strings.ToUpper(method) {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodHead, http.MethodPatch, http.MethodOptions:
		return strings.ToUpper(method)
	default:
		return "OTHER"
	}
}

// metricStatusClass groups response codes into Prometheus-friendly classes
// rather than individual status values.
func metricStatusClass(status int) string {
	if status < 100 || status > 599 {
		return "unknown"
	}
	return fmt.Sprintf("%dxx", status/100)
}

// metricBackendLabel normalizes dependency backend labels while keeping the
// original provider family recognizable.
func metricBackendLabel(backend string) string {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		return "unconfigured"
	}
	var b strings.Builder
	for _, r := range backend {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('_')
	}
	return b.String()
}

// prometheusLabel quotes and escapes a string for use as a Prometheus label
// value.
func prometheusLabel(value string) string {
	escaped := strings.NewReplacer(`\`, `\\`, "\n", `\n`, `"`, `\"`).Replace(value)
	return `"` + escaped + `"`
}

// boolMetric converts booleans into the numeric values Prometheus gauges use.
func boolMetric(value bool) int {
	if value {
		return 1
	}
	return 0
}

// stringFromMap pulls a best-effort string from a small internal status map.
func stringFromMap(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

// boolFromMap pulls a best-effort boolean from a small internal status map.
func boolFromMap(values map[string]any, key string) bool {
	value, _ := values[key].(bool)
	return value
}

// sortedRequestMetricKeys provides deterministic metric output for snapshots
// and tests.
func sortedRequestMetricKeys(metrics map[requestMetricKey]uint64) []requestMetricKey {
	keys := make([]requestMetricKey, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Method != keys[j].Method {
			return keys[i].Method < keys[j].Method
		}
		if keys[i].Surface != keys[j].Surface {
			return keys[i].Surface < keys[j].Surface
		}
		return keys[i].StatusClass < keys[j].StatusClass
	})
	return keys
}

// sortedDurationMetricKeys provides deterministic histogram ordering for
// snapshots and tests.
func sortedDurationMetricKeys(metrics map[durationMetricKey]uint64) []durationMetricKey {
	keys := make([]durationMetricKey, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Method != keys[j].Method {
			return keys[i].Method < keys[j].Method
		}
		return keys[i].Surface < keys[j].Surface
	})
	return keys
}

// sortedMaintenanceBlockedMetricKeys provides deterministic blocked-write
// counter ordering for metrics tests and scraper diffs.
func sortedMaintenanceBlockedMetricKeys(metrics map[maintenanceBlockedMetricKey]uint64) []maintenanceBlockedMetricKey {
	keys := make([]maintenanceBlockedMetricKey, 0, len(metrics))
	for key := range metrics {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Method != keys[j].Method {
			return keys[i].Method < keys[j].Method
		}
		if keys[i].Surface != keys[j].Surface {
			return keys[i].Surface < keys[j].Surface
		}
		return keys[i].Reason < keys[j].Reason
	})
	return keys
}
