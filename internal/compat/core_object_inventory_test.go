package compat

import (
	"strings"
	"testing"
)

type coreObjectCompatibilityInventoryEntry struct {
	family             string
	upstreamEvidence   []string
	localCoverage      []string
	remainingGapGroups []string
	followOnTasks      []string
	deferred           []string
}

func coreObjectCompatibilityInventory() []coreObjectCompatibilityInventoryEntry {
	return []coreObjectCompatibilityInventoryEntry{
		{
			family: "nodes",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/nodes/complete_endpoint_spec.rb",
				"oc-chef-pedant/spec/api/nodes_spec.rb",
				"oc-chef-pedant/spec/api/knife/nodes/*",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_nodes.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_node.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_node.erl",
				"src/oc_erchef/raml-docs/schemas/node.json",
			},
			localCoverage: []string{
				"internal/api/node_api_version_routes_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
				"internal/search/documents_test.go",
				"internal/search/rebuild_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "search_projection", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 3", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "environments",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/environments/*",
				"oc-chef-pedant/spec/api/environments_spec.rb",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_environments.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_environment.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_environment.erl",
				"src/oc_erchef/raml-docs/schemas/environment.json",
			},
			localCoverage: []string{
				"internal/api/role_environment_api_version_routes_test.go",
				"internal/api/environment_cookbook_routes_test.go",
				"internal/api/environment_depsolver_routes_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "search_projection", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 4", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "roles",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/roles/complete_endpoint_spec.rb",
				"oc-chef-pedant/spec/api/knife/roles/*",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_roles.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_role.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_role.erl",
				"src/oc_erchef/raml-docs/schemas/role.json",
			},
			localCoverage: []string{
				"internal/api/role_environment_api_version_routes_test.go",
				"internal/bootstrap/roles_test.go",
				"internal/api/environment_depsolver_routes_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "search_projection", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 5", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "data_bags",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/data_bag/complete_endpoint_spec.rb",
				"oc-chef-pedant/spec/api/knife/data_bag/*",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_data.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_data.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_data_item.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_data_bag.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_data_bag_item.erl",
				"src/oc_erchef/raml-docs/schemas/data-bag-item.json",
			},
			localCoverage: []string{
				"internal/api/data_bag_routes_test.go",
				"internal/api/data_bag_api_version_routes_test.go",
				"internal/bootstrap/data_bags_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "search_projection", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 6", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "policies",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/policies/complete_endpoint_spec.rb",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_policy_groups.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_named_policy.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_named_policy_revisions.erl",
				"src/oc_erchef/apps/oc_chef_authz/src/oc_chef_policy_revision.erl",
				"src/oc_erchef/raml-docs/examples/policy.json",
			},
			localCoverage: []string{
				"internal/api/policy_routes_test.go",
				"internal/api/policy_sandbox_api_version_routes_test.go",
				"internal/bootstrap/policies_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "search_projection", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 7", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "sandboxes",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/sandboxes/complete_endpoint_spec.rb",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_sandboxes.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_named_sandbox.erl",
				"src/oc_erchef/apps/chef_objects/src/chef_sandbox.erl",
				"src/oc_erchef/raml-docs/schemas/sandbox-create.json",
				"src/oc_erchef/raml-docs/schemas/sandbox-complete.json",
			},
			localCoverage: []string{
				"internal/api/sandbox_routes_test.go",
				"internal/api/policy_sandbox_api_version_routes_test.go",
				"internal/bootstrap/sandboxes_test.go",
				"internal/api/core_object_pg_persistence_routes_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "blob_checksum_retention", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 8", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "cookbooks_and_artifacts",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/cookbooks/*",
				"oc-chef-pedant/spec/api/cookbook_artifacts/*",
				"oc-chef-pedant/spec/api/knife/cookbook/*",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_cookbooks.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/chef_wm_cookbook_version.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_named_cookbook_artifact_version.erl",
				"src/oc_erchef/raml-docs/organizations/cookbooks.yaml",
				"src/oc_erchef/raml-docs/organizations/cookbook_artifacts.yaml",
			},
			localCoverage: []string{
				"internal/api/cookbook_routes_test.go",
				"internal/api/cookbook_api_version_routes_test.go",
				"internal/api/cookbook_pg_provider_routes_test.go",
				"internal/bootstrap/cookbooks_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "blob_checksum_retention", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 9", "Task 10", "Task 11", "Task 12"},
		},
		{
			family: "object_acls",
			upstreamEvidence: []string{
				"oc-chef-pedant/spec/api/groups_acl_spec.rb",
				"oc-chef-pedant/spec/api/account/account_acl_spec.rb",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_acl.erl",
				"src/oc_erchef/apps/oc_chef_wm/src/oc_chef_wm_acl_permission.erl",
				"src/oc_bifrost/schema/deploy/update_acl.sql",
				"src/oc_bifrost/schema/deploy/actor_has_permission_on.sql",
			},
			localCoverage: []string{
				"internal/api/maintenance_repair_routes_test.go",
				"internal/api/validator_bootstrap_registration_routes_test.go",
				"internal/bootstrap/service_test.go",
				"internal/store/pg/core_objects_test.go",
			},
			remainingGapGroups: []string{"route_semantics", "payload_exactness", "validation_precedence", "auth_acl_no_mutation", "postgres_rehydration", "default_org_parity"},
			followOnTasks:      []string{"Task 2", "Task 10", "Task 11", "Task 12"},
			deferred:           []string{"broader online direct PostgreSQL repair mutations beyond maintenance-gated default ACL repair"},
		},
	}
}

func TestCoreObjectCompatibilityInventoryCoversPlannedFamilies(t *testing.T) {
	wantFamilies := map[string]bool{
		"nodes":                   false,
		"environments":            false,
		"roles":                   false,
		"data_bags":               false,
		"policies":                false,
		"sandboxes":               false,
		"cookbooks_and_artifacts": false,
		"object_acls":             false,
	}
	for _, entry := range coreObjectCompatibilityInventory() {
		seen, ok := wantFamilies[entry.family]
		if !ok {
			t.Fatalf("unexpected inventory family %q", entry.family)
		}
		if seen {
			t.Fatalf("duplicate inventory family %q", entry.family)
		}
		wantFamilies[entry.family] = true
	}
	for family, seen := range wantFamilies {
		if !seen {
			t.Fatalf("missing core object compatibility inventory family %q", family)
		}
	}
}

func TestCoreObjectCompatibilityInventoryTracksEvidenceCoverageAndTasks(t *testing.T) {
	requiredGapGroups := []string{
		"route_semantics",
		"payload_exactness",
		"validation_precedence",
		"auth_acl_no_mutation",
		"postgres_rehydration",
		"default_org_parity",
	}
	for _, entry := range coreObjectCompatibilityInventory() {
		if len(entry.upstreamEvidence) == 0 {
			t.Fatalf("%s missing upstream evidence", entry.family)
		}
		if len(entry.localCoverage) == 0 {
			t.Fatalf("%s missing local coverage pointers", entry.family)
		}
		if len(entry.followOnTasks) == 0 {
			t.Fatalf("%s missing follow-on task mapping", entry.family)
		}
		for _, group := range requiredGapGroups {
			if !stringSliceContains(entry.remainingGapGroups, group) {
				t.Fatalf("%s missing gap group %q in %v", entry.family, group, entry.remainingGapGroups)
			}
		}
		if !hasEvidencePrefix(entry.upstreamEvidence, "oc-chef-pedant/") {
			t.Fatalf("%s missing pedant evidence: %v", entry.family, entry.upstreamEvidence)
		}
		if !hasEvidencePrefix(entry.upstreamEvidence, "src/oc_erchef/") && !hasEvidencePrefix(entry.upstreamEvidence, "src/oc_bifrost/") {
			t.Fatalf("%s missing upstream source evidence: %v", entry.family, entry.upstreamEvidence)
		}
	}
}

func stringSliceContains(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func hasEvidencePrefix(values []string, prefix string) bool {
	for _, value := range values {
		if strings.HasPrefix(value, prefix) {
			return true
		}
	}
	return false
}
