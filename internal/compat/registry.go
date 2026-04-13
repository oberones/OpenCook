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
				Patterns: []string{"/clients", "/clients/", "/nodes", "/nodes/", "/organizations/{org}/nodes", "/organizations/{org}/nodes/", "/environments", "/environments/", "/environments/{name}/cookbooks", "/environments/{name}/cookbooks/", "/environments/{name}/cookbooks/{cookbook}", "/environments/{name}/cookbook_versions", "/environments/{name}/cookbook_versions/", "/environments/{name}/nodes", "/environments/{name}/nodes/", "/environments/{name}/roles/{role}", "/environments/{name}/roles/{role}/", "/environments/{name}/recipes", "/environments/{name}/recipes/", "/organizations/{org}/environments", "/organizations/{org}/environments/", "/organizations/{org}/environments/{name}/cookbooks", "/organizations/{org}/environments/{name}/cookbooks/", "/organizations/{org}/environments/{name}/cookbooks/{cookbook}", "/organizations/{org}/environments/{name}/cookbook_versions", "/organizations/{org}/environments/{name}/cookbook_versions/", "/organizations/{org}/environments/{name}/nodes", "/organizations/{org}/environments/{name}/nodes/", "/organizations/{org}/environments/{name}/roles/{role}", "/organizations/{org}/environments/{name}/roles/{role}/", "/organizations/{org}/environments/{name}/recipes", "/organizations/{org}/environments/{name}/recipes/", "/roles", "/roles/", "/roles/{name}/environments", "/roles/{name}/environments/", "/roles/{name}/environments/{environment}", "/roles/{name}/environments/{environment}/", "/organizations/{org}/roles", "/organizations/{org}/roles/", "/organizations/{org}/roles/{name}/environments", "/organizations/{org}/roles/{name}/environments/", "/organizations/{org}/roles/{name}/environments/{environment}", "/organizations/{org}/roles/{name}/environments/{environment}/", "/data", "/data/", "/organizations/{org}/data", "/organizations/{org}/data/"},
				Notes:    "Core Chef object CRUD and organization-scoped default routes, including environment-filtered cookbook and depsolver views, environment-linked role reads, role environment resolution, and data bag collection aliases.",
			},
			{
				Name:  "cookbooks-and-blobs",
				Owner: "blob",
				Phase: "phase-4",
				Patterns: []string{
					"/_blob/checksums/{checksum}",
					"/_blob/checksums/{checksum}/",
					"/sandboxes",
					"/sandboxes/",
					"/sandboxes/{id}",
					"/sandboxes/{id}/",
					"/organizations/{org}/sandboxes",
					"/organizations/{org}/sandboxes/",
					"/organizations/{org}/sandboxes/{id}",
					"/organizations/{org}/sandboxes/{id}/",
					"/cookbooks",
					"/cookbooks/",
					"/cookbooks/_latest",
					"/cookbooks/_latest/",
					"/cookbooks/_recipes",
					"/cookbooks/_recipes/",
					"/cookbooks/{name}",
					"/cookbooks/{name}/",
					"/cookbooks/{name}/{version}",
					"/cookbooks/{name}/{version}/",
					"/cookbook_artifacts",
					"/cookbook_artifacts/",
					"/cookbook_artifacts/{name}",
					"/cookbook_artifacts/{name}/",
					"/cookbook_artifacts/{name}/{identifier}",
					"/cookbook_artifacts/{name}/{identifier}/",
					"/universe",
					"/universe/",
					"/organizations/{org}/cookbooks",
					"/organizations/{org}/cookbooks/",
					"/organizations/{org}/cookbooks/_latest",
					"/organizations/{org}/cookbooks/_latest/",
					"/organizations/{org}/cookbooks/_recipes",
					"/organizations/{org}/cookbooks/_recipes/",
					"/organizations/{org}/cookbooks/{name}",
					"/organizations/{org}/cookbooks/{name}/",
					"/organizations/{org}/cookbooks/{name}/{version}",
					"/organizations/{org}/cookbooks/{name}/{version}/",
					"/organizations/{org}/cookbook_artifacts",
					"/organizations/{org}/cookbook_artifacts/",
					"/organizations/{org}/cookbook_artifacts/{name}",
					"/organizations/{org}/cookbook_artifacts/{name}/",
					"/organizations/{org}/cookbook_artifacts/{name}/{identifier}",
					"/organizations/{org}/cookbook_artifacts/{name}/{identifier}/",
					"/organizations/{org}/universe",
					"/organizations/{org}/universe/",
				},
				Notes: "Sandbox create/commit flow, signed blob upload/download URLs, cookbook artifact lifecycle, writable cookbook version flows, cookbook read views, and universe compatibility.",
			},
			{
				Name:     "search-and-policy",
				Owner:    "search",
				Phase:    "phase-5",
				Patterns: []string{"/search", "/search/", "/search/{index}", "/search/{index}/", "/organizations/{org}/search", "/organizations/{org}/search/", "/organizations/{org}/search/{index}", "/organizations/{org}/search/{index}/", "/policies", "/policies/", "/policies/{name}", "/policies/{name}/", "/policies/{name}/revisions", "/policies/{name}/revisions/", "/policies/{name}/revisions/{revision}", "/policies/{name}/revisions/{revision}/", "/policy_groups", "/policy_groups/", "/policy_groups/{group}", "/policy_groups/{group}/", "/policy_groups/{group}/policies/{name}", "/policy_groups/{group}/policies/{name}/", "/organizations/{org}/policies", "/organizations/{org}/policies/", "/organizations/{org}/policies/{name}", "/organizations/{org}/policies/{name}/", "/organizations/{org}/policies/{name}/revisions", "/organizations/{org}/policies/{name}/revisions/", "/organizations/{org}/policies/{name}/revisions/{revision}", "/organizations/{org}/policies/{name}/revisions/{revision}/", "/organizations/{org}/policy_groups", "/organizations/{org}/policy_groups/", "/organizations/{org}/policy_groups/{group}", "/organizations/{org}/policy_groups/{group}/", "/organizations/{org}/policy_groups/{group}/policies/{name}", "/organizations/{org}/policy_groups/{group}/policies/{name}/"},
				Notes:    "Document expansion, search query compatibility, and the policyfile API routes, including org-scoped aliases.",
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
