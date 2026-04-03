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
				Patterns: []string{"/users", "/users/", "/users/{name}/keys", "/users/{name}/keys/", "/authenticate_user", "/system_recovery"},
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
				Patterns: []string{"/clients", "/clients/", "/nodes", "/nodes/", "/organizations/{org}/nodes", "/organizations/{org}/nodes/", "/roles", "/roles/", "/data", "/data/", "/environments", "/environments/"},
				Notes:    "Core Chef object CRUD and organization-scoped default routes.",
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
				Patterns: []string{"/search", "/search/", "/policies", "/policies/", "/policy_groups", "/policy_groups/"},
				Notes:    "Document expansion, OpenSearch-backed indexing, and policy APIs.",
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
