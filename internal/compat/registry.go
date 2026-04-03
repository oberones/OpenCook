package compat

type Surface struct {
	Name     string   `json:"name"`
	Owner    string   `json:"owner"`
	Phase    string   `json:"phase"`
	Patterns []string `json:"patterns"`
	Notes    string   `json:"notes"`
}

type Registry struct {
	surfaces []Surface
}

func NewDefaultRegistry() Registry {
	return Registry{
		surfaces: []Surface{
			{
				Name:     "users-and-keys",
				Owner:    "authn",
				Phase:    "phase-3",
				Patterns: []string{"/users", "/users/", "/users/{name}/keys", "/users/{name}/keys/", "/clients/{name}/keys", "/clients/{name}/keys/", "/authenticate_user", "/system_recovery"},
				Notes:    "Chef request signing, API version behavior, and /keys semantics land here.",
			},
			{
				Name:     "organizations",
				Owner:    "authz",
				Phase:    "phase-3",
				Patterns: []string{"/organizations", "/organizations/", "/organizations/{org}/clients/{name}/keys", "/organizations/{org}/clients/{name}/keys/", "/internal-organizations/"},
				Notes:    "Org bootstrap, association flows, and default ACL creation.",
			},
			{
				Name:     "infra-objects",
				Owner:    "api",
				Phase:    "phase-4",
				Patterns: []string{"/clients", "/clients/", "/nodes", "/nodes/", "/organizations/{org}/nodes", "/organizations/{org}/nodes/", "/environments", "/environments/", "/environments/{name}/nodes", "/environments/{name}/nodes/", "/organizations/{org}/environments", "/organizations/{org}/environments/", "/organizations/{org}/environments/{name}/nodes", "/organizations/{org}/environments/{name}/nodes/", "/roles", "/roles/", "/roles/{name}/environments", "/roles/{name}/environments/", "/roles/{name}/environments/{environment}", "/roles/{name}/environments/{environment}/", "/organizations/{org}/roles", "/organizations/{org}/roles/", "/organizations/{org}/roles/{name}/environments", "/organizations/{org}/roles/{name}/environments/", "/organizations/{org}/roles/{name}/environments/{environment}", "/organizations/{org}/roles/{name}/environments/{environment}/", "/data", "/data/", "/organizations/{org}/data", "/organizations/{org}/data/"},
				Notes:    "Core Chef object CRUD and organization-scoped default routes, including role environment resolution and data bag collection aliases.",
			},
			{
				Name:     "cookbooks-and-blobs",
				Owner:    "blob",
				Phase:    "phase-4",
				Patterns: []string{"/sandboxes", "/sandboxes/", "/cookbooks", "/cookbooks/", "/cookbook_artifacts", "/cookbook_artifacts/", "/universe"},
				Notes:    "Cookbook upload, checksum tracking, and object storage compatibility.",
			},
			{
				Name:     "search-and-policy",
				Owner:    "search",
				Phase:    "phase-5",
				Patterns: []string{"/search", "/search/", "/search/{index}", "/search/{index}/", "/organizations/{org}/search", "/organizations/{org}/search/", "/organizations/{org}/search/{index}", "/organizations/{org}/search/{index}/", "/policies", "/policies/", "/policies/{name}", "/policies/{name}/", "/policies/{name}/revisions", "/policies/{name}/revisions/", "/policies/{name}/revisions/{revision}", "/policies/{name}/revisions/{revision}/", "/policy_groups", "/policy_groups/", "/policy_groups/{group}", "/policy_groups/{group}/", "/policy_groups/{group}/policies/{name}", "/policy_groups/{group}/policies/{name}/"},
				Notes:    "Document expansion, search query compatibility, and the first policyfile API routes.",
			},
			{
				Name:     "api-versioning-and-ops",
				Owner:    "platform",
				Phase:    "phase-6",
				Patterns: []string{"/server_api_version"},
				Notes:    "Operational endpoints, API negotiation, and admin tooling. Licensing is intentionally excluded from OpenCook.",
			},
		},
	}
}

func (r Registry) Surfaces() []Surface {
	out := make([]Surface, len(r.surfaces))
	copy(out, r.surfaces)
	return out
}

func (r Registry) RouteCount() int {
	total := 0
	for _, surface := range r.surfaces {
		total += len(surface.Patterns)
	}
	return total
}
