package api

import "net/http"

type maintenanceRouteClass string

const (
	maintenanceRouteOperationalOnly maintenanceRouteClass = "operational_only"
	maintenanceRouteReadOnly        maintenanceRouteClass = "read_only"
	maintenanceRouteReadLikeNonGET  maintenanceRouteClass = "read_like_non_get"
	maintenanceRouteMutating        maintenanceRouteClass = "mutating"
	maintenanceRouteBlobDownload    maintenanceRouteClass = "blob_download"
	maintenanceRouteBlobUpload      maintenanceRouteClass = "blob_upload"
)

const (
	maintenanceBlockedHTTPStatus = http.StatusServiceUnavailable
	maintenanceBlockedError      = "503 - Service Unavailable: Sorry, we are unavailable right now.  Please try again later."
)

type maintenanceRouteContract struct {
	Pattern        string
	Classes        []maintenanceRouteClass
	AllowedMethods []string
	BlockedMethods []string
	Notes          string
}

var maintenanceRouteContractsByPattern = buildMaintenanceRouteContractByPattern()

// maintenanceBlockedPayload freezes the Chef-compatible 503 body used by the
// active maintenance gate. Upstream nginx serves a static /503.json body, so we
// intentionally avoid OpenCook-specific error codes in Chef-facing responses.
func maintenanceBlockedPayload() map[string]string {
	return map[string]string{"error": maintenanceBlockedError}
}

// maintenanceRouteContracts is the write-gate inventory for every concrete
// mux.HandleFunc pattern registered in NewRouter. Task 4 should enforce this
// contract rather than inferring mutability from HTTP verbs alone, because Chef
// uses POST for read-like operations such as partial search and depsolver.
func maintenanceRouteContracts() []maintenanceRouteContract {
	var contracts []maintenanceRouteContract
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/",
			"/_status",
			"/healthz",
			"/readyz",
			"/metrics",
			"/server_api_version",
			"/server_api_version/",
			"/internal/contracts/routes",
			"/internal/authn/capabilities",
		},
		[]maintenanceRouteClass{maintenanceRouteOperationalOnly},
		[]string{http.MethodGet, http.MethodHead},
		nil,
		"operational status, readiness, capability, and metrics routes are not Chef object mutations",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			maintenanceRepairDefaultACLsPath,
		},
		[]maintenanceRouteClass{maintenanceRouteOperationalOnly},
		[]string{http.MethodPost},
		nil,
		"online ACL repair is an authenticated operational route that performs its own active-maintenance and confirmation checks",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/_blob/checksums/{checksum}",
			"/_blob/checksums/{checksum}/",
		},
		[]maintenanceRouteClass{maintenanceRouteBlobDownload, maintenanceRouteBlobUpload},
		[]string{http.MethodGet},
		[]string{http.MethodPut},
		"signed checksum downloads stay available; signed sandbox uploads are writes and should be blocked",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/cookbooks",
			"/cookbook_artifacts",
			"/environments/{name}/cookbooks",
			"/environments/{name}/cookbooks/{cookbook}",
			"/environments/{name}/cookbooks/",
			"/environments/{name}/nodes",
			"/environments/{name}/nodes/",
			"/environments/{name}/roles/{role}",
			"/environments/{name}/roles/{role}/",
			"/environments/{name}/recipes",
			"/environments/{name}/recipes/",
			"/organizations/{org}/cookbooks",
			"/organizations/{org}/cookbook_artifacts",
			"/organizations/{org}/environments/{name}/cookbooks",
			"/organizations/{org}/environments/{name}/cookbooks/{cookbook}",
			"/organizations/{org}/environments/{name}/cookbooks/",
			"/organizations/{org}/environments/{name}/nodes",
			"/organizations/{org}/environments/{name}/nodes/",
			"/organizations/{org}/environments/{name}/roles/{role}",
			"/organizations/{org}/environments/{name}/roles/{role}/",
			"/organizations/{org}/environments/{name}/recipes",
			"/organizations/{org}/environments/{name}/recipes/",
			"/policies",
			"/organizations/{org}/policies",
			"/policy_groups",
			"/organizations/{org}/policy_groups",
			"/search",
			"/search/",
			"/organizations/{org}/search",
			"/organizations/{org}/search/",
			"/universe",
			"/universe/",
			"/organizations/{org}/universe",
			"/organizations/{org}/universe/",
			"/roles/{name}/environments",
			"/roles/{name}/environments/",
			"/organizations/{org}/roles/{name}/environments",
			"/organizations/{org}/roles/{name}/environments/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly},
		[]string{http.MethodGet, http.MethodHead},
		nil,
		"read-only Chef or operational views should keep working during maintenance",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/environments/{name}/cookbook_versions",
			"/environments/{name}/cookbook_versions/",
			"/organizations/{org}/environments/{name}/cookbook_versions",
			"/organizations/{org}/environments/{name}/cookbook_versions/",
			"/search/{index}",
			"/search/{index}/",
			"/organizations/{org}/search/{index}",
			"/organizations/{org}/search/{index}/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteReadLikeNonGET},
		[]string{http.MethodGet, http.MethodHead, http.MethodPost},
		nil,
		"POST is read-like for depsolver and partial search; blocking it would break Chef/Cinc clients",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/clients",
			"/organizations/{org}/clients",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost},
		"client collection create mutates bootstrap state and verifier keys",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/clients/",
			"/organizations/{org}/clients/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodPut, http.MethodDelete},
		"client subtree can create, update, or delete clients",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/clients/{name}/keys",
			"/clients/{name}/keys/",
			"/organizations/{org}/clients/{name}/keys",
			"/organizations/{org}/clients/{name}/keys/",
			"/users/{name}/keys",
			"/users/{name}/keys/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodPut, http.MethodDelete},
		"key writes mutate persisted key rows and the request verifier cache",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/organizations/{org}/groups",
			"/organizations/{org}/groups/",
			"/organizations/{org}/containers",
			"/organizations/{org}/containers/",
			"/organizations/{org}/_acl",
			"/organizations/{org}/groups/{name}/_acl",
			"/organizations/{org}/containers/{name}/_acl",
			"/organizations/{org}/clients/{name}/_acl",
			"/users/{name}/_acl",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodPut, http.MethodDelete},
		"group, container, and ACL reads stay available; write attempts must not race maintenance operations",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/cookbooks/", "/organizations/{org}/cookbooks/"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPut, http.MethodDelete},
		"cookbook version writes mutate metadata, blob references, and cleanup decisions",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/cookbook_artifacts/", "/organizations/{org}/cookbook_artifacts/"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPut, http.MethodDelete},
		"cookbook artifact writes mutate metadata and blob reference cleanup decisions",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/data",
			"/data/",
			"/organizations/{org}/data",
			"/organizations/{org}/data/",
			"/environments",
			"/environments/",
			"/organizations/{org}/environments",
			"/organizations/{org}/environments/",
			"/nodes",
			"/nodes/",
			"/organizations/{org}/nodes",
			"/organizations/{org}/nodes/",
			"/roles",
			"/roles/",
			"/organizations/{org}/roles",
			"/organizations/{org}/roles/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodPut, http.MethodDelete},
		"core Chef object writes mutate PostgreSQL-backed state and derived search documents",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/organizations"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost},
		"organization bootstrap creates org state, validator clients, groups, containers, and ACLs",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/organizations/"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodPut, http.MethodDelete},
		"organization subtree is treated as write-capable because org-scoped create/update/delete routes live below it",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/users"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost},
		"user creation mutates bootstrap identity state and verifier keys",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{"/users/"},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPut},
		"user updates mutate bootstrap identity state",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/policies/",
			"/organizations/{org}/policies/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPost, http.MethodDelete},
		"policy revision writes mutate policyfile state",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/policy_groups/",
			"/organizations/{org}/policy_groups/",
		},
		[]maintenanceRouteClass{maintenanceRouteReadOnly, maintenanceRouteMutating},
		[]string{http.MethodGet, http.MethodHead},
		[]string{http.MethodPut, http.MethodDelete},
		"policy group assignment writes mutate policyfile state",
	)...)
	contracts = append(contracts, maintenanceContractsForPatterns(
		[]string{
			"/sandboxes",
			"/sandboxes/",
			"/organizations/{org}/sandboxes",
			"/organizations/{org}/sandboxes/",
		},
		[]maintenanceRouteClass{maintenanceRouteMutating},
		nil,
		[]string{http.MethodPost, http.MethodPut},
		"sandbox create and commit mutate sandbox metadata and checksum references",
	)...)
	return contracts
}

// maintenanceContractsForPatterns expands one route-family decision into a
// contract per ServeMux pattern, keeping the inventory readable while preserving
// exact pattern-level coverage for drift tests.
func maintenanceContractsForPatterns(patterns []string, classes []maintenanceRouteClass, allowedMethods, blockedMethods []string, notes string) []maintenanceRouteContract {
	contracts := make([]maintenanceRouteContract, 0, len(patterns))
	for _, pattern := range patterns {
		contracts = append(contracts, maintenanceRouteContract{
			Pattern:        pattern,
			Classes:        append([]maintenanceRouteClass(nil), classes...),
			AllowedMethods: append([]string(nil), allowedMethods...),
			BlockedMethods: append([]string(nil), blockedMethods...),
			Notes:          notes,
		})
	}
	return contracts
}

// buildMaintenanceRouteContractByPattern builds the immutable package-level
// lookup used by the hot-path maintenance gate. Keeping the map cached avoids
// rebuilding the full route inventory for every request.
func buildMaintenanceRouteContractByPattern() map[string]maintenanceRouteContract {
	out := map[string]maintenanceRouteContract{}
	for _, contract := range maintenanceRouteContracts() {
		out[contract.Pattern] = contract
	}
	return out
}

// maintenanceRouteContractByPattern gives middleware and tests a stable lookup
// keyed by the concrete ServeMux pattern registered in NewRouter.
func maintenanceRouteContractByPattern() map[string]maintenanceRouteContract {
	return maintenanceRouteContractsByPattern
}

// maintenanceRouteHasClass reports whether a route contract includes a class;
// tests use it to make high-risk exceptions explicit without depending on slice
// order.
func maintenanceRouteHasClass(contract maintenanceRouteContract, class maintenanceRouteClass) bool {
	for _, candidate := range contract.Classes {
		if candidate == class {
			return true
		}
	}
	return false
}

// maintenanceRouteMethodListed reports whether a method is covered by one of a
// contract's method lists. It is intentionally exact so future method additions
// require a deliberate route contract update.
func maintenanceRouteMethodListed(methods []string, method string) bool {
	for _, candidate := range methods {
		if candidate == method {
			return true
		}
	}
	return false
}
