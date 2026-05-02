package api

import (
	"net/http"
	"os"
	"regexp"
	"sort"
	"testing"
)

func TestMaintenanceRouteContractsCoverRegisteredMuxPatterns(t *testing.T) {
	registered := registeredMuxPatternsFromRouterSource(t)
	contracts := maintenanceRouteContractByPattern()

	missing := map[string]struct{}{}
	for pattern := range registered {
		if _, ok := contracts[pattern]; !ok {
			missing[pattern] = struct{}{}
		}
	}
	extra := map[string]struct{}{}
	for pattern := range contracts {
		if _, ok := registered[pattern]; !ok {
			extra[pattern] = struct{}{}
		}
	}
	if len(missing) > 0 || len(extra) > 0 {
		t.Fatalf("maintenance route contract drift: missing=%v extra=%v", sortedMaintenancePatternKeys(missing), sortedMaintenancePatternKeys(extra))
	}
}

func TestMaintenanceRouteContractsHaveDeliberateMethodDecisions(t *testing.T) {
	seen := map[string]struct{}{}
	for _, contract := range maintenanceRouteContracts() {
		if contract.Pattern == "" {
			t.Fatal("maintenance route contract has empty pattern")
		}
		if _, ok := seen[contract.Pattern]; ok {
			t.Fatalf("duplicate maintenance route contract for %q", contract.Pattern)
		}
		seen[contract.Pattern] = struct{}{}
		if len(contract.Classes) == 0 {
			t.Fatalf("maintenance route contract %q has no classes", contract.Pattern)
		}
		if maintenanceRouteHasClass(contract, maintenanceRouteMutating) || maintenanceRouteHasClass(contract, maintenanceRouteBlobUpload) {
			if len(contract.BlockedMethods) == 0 {
				t.Fatalf("maintenance route contract %q is write-capable but has no blocked methods", contract.Pattern)
			}
		}
		if maintenanceRouteHasClass(contract, maintenanceRouteReadLikeNonGET) {
			if !maintenanceRouteMethodListed(contract.AllowedMethods, http.MethodPost) {
				t.Fatalf("read-like non-GET route %q does not explicitly allow POST", contract.Pattern)
			}
			if maintenanceRouteMethodListed(contract.BlockedMethods, http.MethodPost) {
				t.Fatalf("read-like non-GET route %q blocks POST", contract.Pattern)
			}
		}
		for _, method := range contract.AllowedMethods {
			if maintenanceRouteMethodListed(contract.BlockedMethods, method) {
				t.Fatalf("maintenance route contract %q both allows and blocks %s", contract.Pattern, method)
			}
		}
	}
}

func TestMaintenanceRouteContractsPreserveChefReadLikePostExceptions(t *testing.T) {
	contracts := maintenanceRouteContractByPattern()
	for _, pattern := range []string{
		"/environments/{name}/cookbook_versions",
		"/organizations/{org}/environments/{name}/cookbook_versions",
		"/search/{index}",
		"/organizations/{org}/search/{index}",
	} {
		contract, ok := contracts[pattern]
		if !ok {
			t.Fatalf("maintenance route contract %q missing", pattern)
		}
		if !maintenanceRouteHasClass(contract, maintenanceRouteReadLikeNonGET) {
			t.Fatalf("maintenance route contract %q classes = %v, want read-like non-GET", pattern, contract.Classes)
		}
		if !maintenanceRouteMethodListed(contract.AllowedMethods, http.MethodPost) {
			t.Fatalf("maintenance route contract %q allowed methods = %v, want POST allowed", pattern, contract.AllowedMethods)
		}
		if maintenanceRouteMethodListed(contract.BlockedMethods, http.MethodPost) {
			t.Fatalf("maintenance route contract %q blocked methods = %v, POST must stay available", pattern, contract.BlockedMethods)
		}
	}
}

func TestMaintenanceRouteContractsIdentifyRepresentativeWrites(t *testing.T) {
	contracts := maintenanceRouteContractByPattern()
	for _, tc := range []struct {
		pattern string
		method  string
		class   maintenanceRouteClass
	}{
		{pattern: "/nodes", method: http.MethodPost, class: maintenanceRouteMutating},
		{pattern: "/cookbooks/", method: http.MethodPut, class: maintenanceRouteMutating},
		{pattern: "/cookbook_artifacts/", method: http.MethodDelete, class: maintenanceRouteMutating},
		{pattern: "/_blob/checksums/{checksum}", method: http.MethodPut, class: maintenanceRouteBlobUpload},
		{pattern: "/users/{name}/keys", method: http.MethodPost, class: maintenanceRouteMutating},
		{pattern: "/organizations", method: http.MethodPost, class: maintenanceRouteMutating},
		{pattern: "/organizations/{org}/groups", method: http.MethodPost, class: maintenanceRouteMutating},
		{pattern: "/organizations/{org}/containers/{name}/_acl", method: http.MethodPut, class: maintenanceRouteMutating},
		{pattern: "/policy_groups/", method: http.MethodPut, class: maintenanceRouteMutating},
		{pattern: "/sandboxes", method: http.MethodPost, class: maintenanceRouteMutating},
	} {
		contract, ok := contracts[tc.pattern]
		if !ok {
			t.Fatalf("maintenance route contract %q missing", tc.pattern)
		}
		if !maintenanceRouteHasClass(contract, tc.class) {
			t.Fatalf("maintenance route contract %q classes = %v, want %s", tc.pattern, contract.Classes, tc.class)
		}
		if !maintenanceRouteMethodListed(contract.BlockedMethods, tc.method) {
			t.Fatalf("maintenance route contract %q blocked methods = %v, want %s", tc.pattern, contract.BlockedMethods, tc.method)
		}
	}
}

func TestMaintenanceBlockedPayloadUsesChefStatic503Shape(t *testing.T) {
	if maintenanceBlockedHTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("maintenanceBlockedHTTPStatus = %d, want %d", maintenanceBlockedHTTPStatus, http.StatusServiceUnavailable)
	}
	payload := maintenanceBlockedPayload()
	if len(payload) != 1 || payload["error"] != maintenanceBlockedError {
		t.Fatalf("maintenanceBlockedPayload() = %#v, want upstream static 503 error body", payload)
	}
}

// registeredMuxPatternsFromRouterSource extracts literal ServeMux patterns from
// NewRouter so the maintenance contract drifts only when router.go changes.
func registeredMuxPatternsFromRouterSource(t *testing.T) map[string]struct{} {
	t.Helper()
	data, err := os.ReadFile("router.go")
	if err != nil {
		t.Fatalf("read router.go: %v", err)
	}
	matches := regexp.MustCompile(`mux\.HandleFunc\("([^"]+)"`).FindAllSubmatch(data, -1)
	patterns := map[string]struct{}{}
	for _, match := range matches {
		patterns[string(match[1])] = struct{}{}
	}
	if len(patterns) == 0 {
		t.Fatal("found no mux.HandleFunc patterns in router.go")
	}
	return patterns
}

// sortedMaintenancePatternKeys keeps failure diagnostics deterministic when a
// future test needs to print a set of route patterns.
func sortedMaintenancePatternKeys(patterns map[string]struct{}) []string {
	out := make([]string, 0, len(patterns))
	for pattern := range patterns {
		out = append(out, pattern)
	}
	sort.Strings(out)
	return out
}
