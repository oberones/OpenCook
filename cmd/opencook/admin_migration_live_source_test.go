package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/config"
)

func TestAdminMigrationLiveSourceMappingsCoverValidationFamilies(t *testing.T) {
	mappings := adminMigrationLiveSourceFamilyMappings()
	byScope := make(map[string]map[string]adminMigrationLiveSourceFamilyMapping)
	seen := make(map[string]bool)
	for _, mapping := range mappings {
		if mapping.Family == "" || mapping.Scope == "" || mapping.UpstreamService == "" || mapping.Notes == "" {
			t.Fatalf("live source mapping is incomplete: %+v", mapping)
		}
		if len(mapping.UpstreamReferences) == 0 {
			t.Fatalf("live source mapping %s/%s has no upstream references", mapping.Scope, mapping.Family)
		}
		key := mapping.Scope + "/" + mapping.Family
		if seen[key] {
			t.Fatalf("duplicate live source mapping for %s", key)
		}
		seen[key] = true
		if byScope[mapping.Scope] == nil {
			byScope[mapping.Scope] = make(map[string]adminMigrationLiveSourceFamilyMapping)
		}
		byScope[mapping.Scope][mapping.Family] = mapping
	}

	requireAdminMigrationLiveSourceFamilies(t, byScope[adminMigrationLiveSourceScopeGlobal], adminMigrationValidationGlobalFamilies())
	requireAdminMigrationLiveSourceFamilies(t, byScope[adminMigrationLiveSourceScopeOrganization], adminMigrationValidationOrganizationFamilies())
	requireAdminMigrationLiveSourceFamilies(t, byScope[adminMigrationLiveSourceScopeBlob], adminMigrationValidationBlobFamilies())

	if _, ok := byScope[adminMigrationLiveSourceScopeDerived]["opensearch_documents"]; !ok {
		t.Fatalf("derived live source mappings = %v, want opensearch_documents", byScope[adminMigrationLiveSourceScopeDerived])
	}
}

func TestAdminMigrationLiveSourceDeferredFamiliesAreExplicit(t *testing.T) {
	required := map[string]bool{
		"oc_id":                 false,
		"redis":                 false,
		"telemetry":             false,
		"licensing":             false,
		"service_supervisor":    false,
		"org_user_invites":      false,
		"org_migration_state":   false,
		"reporting_schema_info": false,
		"opensearch_documents_as_source_of_truth": false,
	}

	for _, mapping := range adminMigrationLiveSourceDeferredFamilies() {
		if mapping.Scope != adminMigrationLiveSourceScopeDeferred {
			t.Fatalf("deferred mapping %+v scope = %q, want %q", mapping, mapping.Scope, adminMigrationLiveSourceScopeDeferred)
		}
		if mapping.Family == "" || mapping.UpstreamService == "" || mapping.Notes == "" || len(mapping.UpstreamReferences) == 0 {
			t.Fatalf("deferred mapping is incomplete: %+v", mapping)
		}
		if _, ok := required[mapping.Family]; ok {
			required[mapping.Family] = true
		}
	}

	for family, seen := range required {
		if !seen {
			t.Fatalf("deferred live source families missing %s", family)
		}
	}
}

func TestAdminMigrationLiveSourceCapabilityProbesAreReadOnly(t *testing.T) {
	required := map[string]bool{
		"source_postgres_read_only": false,
		"source_erchef_schema":      false,
		"source_bifrost_schema":     false,
		"visible_organizations":     false,
	}
	seen := make(map[string]bool)
	for _, probe := range adminMigrationLiveSourceCapabilityProbes() {
		if probe.Name == "" || probe.Backend == "" || probe.Description == "" {
			t.Fatalf("capability probe is incomplete: %+v", probe)
		}
		if !probe.MutationFree {
			t.Fatalf("capability probe %q is not marked mutation-free", probe.Name)
		}
		if seen[probe.Name] {
			t.Fatalf("duplicate capability probe %q", probe.Name)
		}
		seen[probe.Name] = true
		if _, ok := required[probe.Name]; ok {
			if !probe.Required {
				t.Fatalf("capability probe %q must be required", probe.Name)
			}
			required[probe.Name] = true
		}
	}

	for name, seen := range required {
		if !seen {
			t.Fatalf("required capability probes missing %s", name)
		}
	}
	for _, optional := range []string{"source_bookshelf_sql", "source_bookshelf_external_s3", "source_http_server_api_version", "source_search_derived_only"} {
		if !seen[optional] {
			t.Fatalf("capability probes missing optional probe %s", optional)
		}
	}
}

func TestAdminMigrationLiveSourceFindingCodesAreStable(t *testing.T) {
	required := map[string]bool{
		adminMigrationFindingSourcePostgresUnavailable:            false,
		adminMigrationFindingSourceSchemaUnsupported:              false,
		adminMigrationFindingSourceErchefUnavailable:              false,
		adminMigrationFindingSourceErchefSchemaUnsupported:        false,
		adminMigrationFindingSourceBifrostUnavailable:             false,
		adminMigrationFindingSourceBifrostSchemaUnsupported:       false,
		adminMigrationFindingSourceAuthorizationTargetUnresolved:  false,
		adminMigrationFindingSourceAuthorizationSubjectUnresolved: false,
		adminMigrationFindingSourceCrossDatabaseConsistency:       false,
		adminMigrationFindingSourceBifrostUnrelatedRecords:        false,
		adminMigrationFindingSourceFamilyUnsupported:              false,
		adminMigrationFindingSourceBlobUnavailable:                false,
		adminMigrationFindingSourceBlobMissing:                    false,
		adminMigrationFindingSourceBlobChecksumMismatch:           false,
		adminMigrationFindingSourceHTTPReadUnavailable:            false,
		adminMigrationFindingSourceExtractionInterrupted:          false,
	}
	for _, finding := range adminMigrationLiveSourceFindingCodes() {
		if finding.Code == "" || finding.Severity == "" || finding.Family == "" || finding.Message == "" {
			t.Fatalf("live source finding code is incomplete: %+v", finding)
		}
		if _, ok := required[finding.Code]; ok {
			required[finding.Code] = true
		}
	}
	for code, seen := range required {
		if !seen {
			t.Fatalf("live source finding codes missing %s", code)
		}
	}
}

func TestAdminMigrationLiveSourceDerivesSplitDatabaseTargets(t *testing.T) {
	cfg := adminMigrationLiveSourceConfig{
		PostgresDSN: "postgresql://opscode_chef_ro:supersecret@db.example:5544/postgres?sslmode=require&application_name=opencook-extract&search_path=chef",
	}
	targets, err := adminMigrationLiveSourceDerivePostgresTargets(cfg)
	if err != nil {
		t.Fatalf("adminMigrationLiveSourceDerivePostgresTargets() error = %v", err)
	}
	if targets.Erchef.Database != "opscode_chef" || targets.Bifrost.Database != "bifrost" {
		t.Fatalf("derived databases = %q/%q, want opscode_chef/bifrost", targets.Erchef.Database, targets.Bifrost.Database)
	}
	for name, target := range map[string]any{"erchef": targets.Erchef, "bifrost": targets.Bifrost} {
		conn := target.(*pgx.ConnConfig)
		if conn.Host != "db.example" || conn.Port != 5544 || conn.User != "opscode_chef_ro" || conn.Password != "supersecret" {
			t.Fatalf("%s target did not preserve server credentials: %+v", name, conn.Config)
		}
		if conn.TLSConfig == nil || conn.RuntimeParams["application_name"] != "opencook-extract" || conn.RuntimeParams["search_path"] != "chef" {
			t.Fatalf("%s target did not preserve TLS/query parameters: %+v", name, conn.Config)
		}
	}
	targets.Erchef.RuntimeParams["clone_probe"] = "erchef_only"
	if targets.Erchef == targets.Bifrost || targets.Bifrost.RuntimeParams["clone_probe"] != "" {
		t.Fatal("derived targets must be independent deep clones")
	}
}

func TestAdminMigrationLiveSourceDerivesDatabaseOverridesAndRejectsInvalidSeed(t *testing.T) {
	targets, err := adminMigrationLiveSourceDerivePostgresTargets(adminMigrationLiveSourceConfig{
		PostgresDSN:     "host=db.example port=5433 user=reader password=secret dbname=seed sslmode=disable application_name=extractor",
		ErchefDatabase:  "chef_custom",
		BifrostDatabase: "authz_custom",
	})
	if err != nil {
		t.Fatalf("derive override targets error = %v", err)
	}
	if targets.Erchef.Database != "chef_custom" || targets.Bifrost.Database != "authz_custom" || targets.Erchef.RuntimeParams["application_name"] != "extractor" {
		t.Fatalf("override targets = %+v %+v", targets.Erchef.Config, targets.Bifrost.Config)
	}
	if _, err := adminMigrationLiveSourceDerivePostgresTargets(adminMigrationLiveSourceConfig{PostgresDSN: "postgres://reader:%zz@db.example/postgres"}); err == nil {
		t.Fatal("invalid cluster seed DSN unexpectedly parsed")
	}
}

func TestAdminMigrationLiveSourceAuthorizationResolutionIsStrictAndOrganizationScoped(t *testing.T) {
	catalog := adminMigrationLiveSourceAuthorizationCatalog{byTypeOrgAndID: map[string]adminMigrationLiveSourceAuthorizationRef{}, byTypeAndID: map[string]adminMigrationLiveSourceAuthorizationRef{}}
	for _, ref := range []adminMigrationLiveSourceAuthorizationRef{
		{RecordType: "group", Organization: "ponyville", AuthzID: "group-ponyville-admins", Resource: "group:admins", SubjectType: "group", Name: "admins"},
		{RecordType: "group", Organization: "canterlot", AuthzID: "group-canterlot-admins", Resource: "group:admins", SubjectType: "group", Name: "admins"},
		{RecordType: "actor", Organization: "", AuthzID: "actor-pivotal", Resource: "user:pivotal", SubjectType: "user", Name: "pivotal"},
	} {
		catalog.byTypeOrgAndID[adminMigrationLiveSourceAuthorizationScopeKey(ref.RecordType, ref.Organization, ref.AuthzID)] = ref
		catalog.byTypeAndID[adminMigrationLiveSourceAuthorizationKey(ref.RecordType, ref.AuthzID)] = ref
	}
	if len(catalog.byTypeOrgAndID) != 3 {
		t.Fatalf("authorization catalog scoped keys = %v", catalog.byTypeOrgAndID)
	}
	ponyville, err := catalog.resolve("group", "group-ponyville-admins", "ponyville", "acl_subject")
	if err != nil || ponyville.Organization != "ponyville" {
		t.Fatalf("ponyville scoped resolution = %+v, %v", ponyville, err)
	}
	canterlot, err := catalog.resolve("group", "group-canterlot-admins", "canterlot", "nested_group")
	if err != nil || canterlot.Organization != "canterlot" {
		t.Fatalf("canterlot scoped resolution = %+v, %v", canterlot, err)
	}
	if _, err := catalog.resolve("group", "group-canterlot-admins", "ponyville", "acl_subject"); err == nil {
		t.Fatal("cross-organization group subject unexpectedly resolved")
	}
	_, err = catalog.resolve("actor", "actor-missing", "ponyville", "group_membership_child")
	var integrity adminMigrationLiveSourceAuthorizationIntegrityError
	if !errors.As(err, &integrity) || integrity.Code != adminMigrationFindingSourceAuthorizationSubjectUnresolved || integrity.Organization != "ponyville" || integrity.AuthzID != "actor-missing" {
		t.Fatalf("missing subject error = %#v, want stable scoped integrity error", err)
	}
	err = adminMigrationLiveSourceValidateBifrostTargets(catalog, "group", map[string]struct{}{"group-canterlot-admins": {}})
	if !errors.As(err, &integrity) || integrity.Code != adminMigrationFindingSourceAuthorizationTargetUnresolved || integrity.Organization != "ponyville" || integrity.AuthzID != "group-ponyville-admins" {
		t.Fatalf("missing target error = %#v, want stable scoped target integrity error", err)
	}

	aclObjects := adminMigrationLiveSourceACLObjects([]adminMigrationLiveSourceACLRow{
		{OrgName: "ponyville", Resource: "group:admins", Permission: "read", AuthorizeeType: "actor", Authorizee: "pivotal"},
		{OrgName: "ponyville", Resource: "group:admins", Permission: "read", AuthorizeeType: "actor", Authorizee: "pivotal"},
	})
	if len(aclObjects) != 1 {
		t.Fatalf("duplicate ACL relationships produced %d objects, want 1", len(aclObjects))
	}
	readACL := aclObjects[0]["read"].(map[string]any)
	if actors := readACL["actors"].([]string); len(actors) != 1 || actors[0] != "pivotal" {
		t.Fatalf("deduplicated ACL actors = %v, want [pivotal]", actors)
	}
}

func TestAdminMigrationLiveSourceGroupMembershipResolutionHandlesNestedDuplicatesAndMissingMembers(t *testing.T) {
	catalog := adminMigrationLiveSourceAuthorizationCatalog{byTypeOrgAndID: map[string]adminMigrationLiveSourceAuthorizationRef{}, byTypeAndID: map[string]adminMigrationLiveSourceAuthorizationRef{}}
	for _, ref := range []adminMigrationLiveSourceAuthorizationRef{
		{RecordType: "group", Organization: "ponyville", AuthzID: "group-admins", Resource: "group:admins", SubjectType: "group", Name: "admins"},
		{RecordType: "group", Organization: "ponyville", AuthzID: "group-billing", Resource: "group:billing-admins", SubjectType: "group", Name: "billing-admins"},
		{RecordType: "actor", Organization: "", AuthzID: "actor-pivotal", Resource: "user:pivotal", SubjectType: "user", Name: "pivotal"},
	} {
		catalog.byTypeOrgAndID[adminMigrationLiveSourceAuthorizationScopeKey(ref.RecordType, ref.Organization, ref.AuthzID)] = ref
		catalog.byTypeAndID[adminMigrationLiveSourceAuthorizationKey(ref.RecordType, ref.AuthzID)] = ref
	}

	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	err := adminMigrationLiveSourceAppendBifrostGroupMemberships(catalog, payloadValues, []adminMigrationLiveSourceBifrostGroupMembershipRow{
		{ParentAuthzID: "group-admins", ChildType: "actor", ChildAuthzID: "actor-pivotal"},
		{ParentAuthzID: "group-admins", ChildType: "group", ChildAuthzID: "group-billing"},
		{ParentAuthzID: "group-admins", ChildType: "group", ChildAuthzID: "group-billing"},
	})
	if err != nil {
		t.Fatalf("append group memberships error = %v", err)
	}
	key := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "group_memberships"}
	if got := len(payloadValues[key]); got != 2 {
		t.Fatalf("deduplicated direct and nested memberships = %d, want 2", got)
	}
	var nested map[string]any
	if err := json.Unmarshal(payloadValues[key][1], &nested); err != nil {
		t.Fatalf("decode nested membership error = %v", err)
	}
	if nested["group"] != "admins" || nested["type"] != "group" || nested["actor"] != "billing-admins" {
		t.Fatalf("nested membership = %#v", nested)
	}

	err = adminMigrationLiveSourceAppendBifrostGroupMemberships(catalog, map[adminMigrationSourcePayloadKey][]json.RawMessage{}, []adminMigrationLiveSourceBifrostGroupMembershipRow{{
		ParentAuthzID: "group-admins", ChildType: "actor", ChildAuthzID: "actor-missing",
	}})
	var integrity adminMigrationLiveSourceAuthorizationIntegrityError
	if !errors.As(err, &integrity) || integrity.Code != adminMigrationFindingSourceAuthorizationSubjectUnresolved || integrity.Relationship != "group_membership_child" {
		t.Fatalf("missing group member error = %#v, want stable subject integrity error", err)
	}

	err = adminMigrationLiveSourceAppendBifrostGroupMemberships(catalog, map[adminMigrationSourcePayloadKey][]json.RawMessage{}, []adminMigrationLiveSourceBifrostGroupMembershipRow{{
		ParentAuthzID: "group-missing", ChildType: "actor", ChildAuthzID: "actor-pivotal",
	}})
	if !errors.As(err, &integrity) || integrity.Code != adminMigrationFindingSourceAuthorizationSubjectUnresolved || integrity.Relationship != "group_membership_parent" {
		t.Fatalf("missing parent group error = %#v, want stable parent integrity error", err)
	}
}

func TestAdminMigrationLiveSourceSQLNeverCrossesErchefAndBifrostTables(t *testing.T) {
	erchefTables := map[string]bool{}
	for _, table := range adminMigrationLiveSourceRequiredErchefTables() {
		erchefTables[table] = true
	}
	for _, table := range adminMigrationLiveSourceRequiredBifrostTables() {
		if erchefTables[table] {
			t.Fatalf("required table %q is assigned to both Erchef and Bifrost probes", table)
		}
	}
	for _, forbidden := range []string{"auth_actor", "auth_group", "auth_object", "auth_container", "_acl_", "group_actor_relations", "group_group_relations"} {
		if strings.Contains(adminMigrationLiveSourceErchefAuthorizationCatalogQuery, forbidden) {
			t.Fatalf("Erchef catalog query references Bifrost table fragment %q", forbidden)
		}
	}
	bifrostQueries := []string{adminMigrationLiveSourceBifrostGroupMembershipQuery}
	for _, spec := range adminMigrationLiveSourceBifrostACLSpecs() {
		bifrostQueries = append(bifrostQueries, adminMigrationLiveSourceBifrostACLQuery(spec))
	}
	for _, query := range bifrostQueries {
		for _, forbidden := range []string{"FROM users", "JOIN users", "FROM orgs", "JOIN orgs", "FROM clients", "JOIN clients", "FROM nodes", "JOIN nodes"} {
			if strings.Contains(query, forbidden) {
				t.Fatalf("Bifrost query references Erchef table fragment %q: %s", forbidden, query)
			}
		}
	}
}

func TestAdminMigrationLiveSourceCookbookVersionsUseNativeErchefParentTable(t *testing.T) {
	for _, required := range []string{
		"JOIN cookbooks cb ON cb.id = cv.cookbook_id",
		"JOIN orgs o ON o.id = cb.org_id",
		"cvc.org_id = cb.org_id",
		"cb.name AS cookbook_name",
	} {
		if !strings.Contains(adminMigrationLiveSourceCookbookVersionsQuery, required) {
			t.Fatalf("cookbook version query does not contain native Erchef relationship %q: %s", required, adminMigrationLiveSourceCookbookVersionsQuery)
		}
	}
	for _, forbidden := range []string{"cv.org_id", "cv.name"} {
		if strings.Contains(adminMigrationLiveSourceCookbookVersionsQuery, forbidden) {
			t.Fatalf("cookbook version query references non-native Erchef column %q: %s", forbidden, adminMigrationLiveSourceCookbookVersionsQuery)
		}
	}
}

func TestAdminMigrationLiveSourcePayloadReadErrorPreservesSafeFamily(t *testing.T) {
	underlying := errors.New("driver detail that must remain internal")
	err := adminMigrationLiveSourceWrapPayloadReadError("cookbook_versions", underlying)
	var payloadRead *adminMigrationLiveSourcePayloadReadError
	if !errors.As(err, &payloadRead) || payloadRead.Family != "cookbook_versions" || !errors.Is(err, underlying) {
		t.Fatalf("payload read error = %#v, want safe family and wrapped cause", err)
	}
	if strings.Contains(err.Error(), underlying.Error()) {
		t.Fatalf("payload read error leaked driver detail: %q", err.Error())
	}
}

func TestAdminMigrationLiveSourceCutoverRequiresFreezeEvidence(t *testing.T) {
	live := adminMigrationCLIOutput{Dependencies: []adminMigrationDependency{}, Findings: []adminMigrationFinding{}}
	adminMigrationCutoverSourceFreezeGate(&live, &adminMigrationFlagValues{}, true)
	requireAdminMigrationDependency(t, adminMigrationDependenciesAsAny(live.Dependencies), "source_freeze_evidence", "error")

	prepared := adminMigrationCLIOutput{Dependencies: []adminMigrationDependency{}, Findings: []adminMigrationFinding{}}
	adminMigrationCutoverSourceFreezeGate(&prepared, &adminMigrationFlagValues{}, false)
	requireAdminMigrationDependency(t, adminMigrationDependenciesAsAny(prepared.Dependencies), "source_freeze_evidence", "warning")

	frozen := adminMigrationCLIOutput{Dependencies: []adminMigrationDependency{}, Findings: []adminMigrationFinding{}}
	adminMigrationCutoverSourceFreezeGate(&frozen, &adminMigrationFlagValues{sourceFrozen: true}, true)
	requireAdminMigrationDependency(t, adminMigrationDependenciesAsAny(frozen.Dependencies), "source_freeze_evidence", "ok")
}

func adminMigrationDependenciesAsAny(deps []adminMigrationDependency) []any {
	out := make([]any, 0, len(deps))
	for _, dep := range deps {
		data, _ := json.Marshal(dep)
		var decoded map[string]any
		_ = json.Unmarshal(data, &decoded)
		out = append(out, decoded)
	}
	return out
}

func TestAdminMigrationSourceLivePreflightReportsRedactedShapeWithoutMutation(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	probe := &fakeAdminMigrationLiveSourcePostgresProbe{
		result: adminMigrationLiveSourcePostgresProbeResult{
			Dependencies: []adminMigrationDependency{
				{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake read-only source PostgreSQL probe passed"},
				{Name: "source_schema", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source schema probe passed"},
			},
			Inventory: adminMigrationInventory{Families: []adminMigrationInventoryFamily{
				{Family: "source_organizations", Count: 1},
				{Family: "source_required_tables", Count: len(adminMigrationLiveSourceRequiredPostgresTables())},
			}},
		},
	}
	cmd.newLiveSource = newAdminMigrationLiveSourceExtractorWithPostgresProbe(probe)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "preflight",
		"--source-postgres-dsn", "postgres://chef:pgsecret@source-db.example/chef",
		"--source-erchef-database", "chef_custom",
		"--source-bifrost-database", "authz_custom",
		"--source-blob-url", "https://blob-user:blobsecret@blob.example/checksums?token=source-token",
		"--source-server-url", "https://chef-user:httpsecret@chef-source.example",
		"--source-requestor-name", "pivotal",
		"--source-private-key", "/tmp/source-secret/pivotal.pem",
		"--org", "ponyville",
		"--reference-blobs",
		"--json",
		"--with-timing",
	})
	if code != exitOK {
		t.Fatalf("Run(source live preflight) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if probe.calls != 1 || probe.configs[0].Organization != "ponyville" {
		t.Fatalf("probe calls=%d configs=%+v, want one ponyville scoped preflight", probe.calls, probe.configs)
	}
	for _, forbidden := range []string{"pgsecret", "blobsecret", "httpsecret", "source-token", "source-secret", "pivotal.pem"} {
		if strings.Contains(stdout.String(), forbidden) || strings.Contains(stderr.String(), forbidden) {
			t.Fatalf("source live preflight leaked %q; stdout = %s stderr = %s", forbidden, stdout.String(), stderr.String())
		}
	}

	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_live_preflight" {
		t.Fatalf("command = %v, want migration_source_live_preflight", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["organization"] != "ponyville" {
		t.Fatalf("target.organization = %v, want ponyville", target["organization"])
	}
	config := requireAdminMigrationMap(t, out, "config")
	if config["source_postgres_dsn"] != "post..." || config["source_blob_url"] != "http..." || config["source_server_url"] != "http..." {
		t.Fatalf("redacted config = %v, want redacted source DSN and URLs", config)
	}
	if config["source_erchef_postgres_dsn"] != "post..." || config["source_bifrost_postgres_dsn"] != "post..." || config["source_erchef_database"] != "chef_custom" || config["source_bifrost_database"] != "authz_custom" {
		t.Fatalf("redacted split database config = %v", config)
	}
	if config["source_private_key"] != "set" || config["source_requestor_type"] != "user" {
		t.Fatalf("redacted config = %v, want private key presence and default user requestor type", config)
	}
	liveSource := requireAdminMigrationMap(t, out, "live_source")
	if liveSource["blob_mode"] != "provider_url" || liveSource["blob_copy_mode"] != "reference_blobs" {
		t.Fatalf("live_source = %v, want provider URL reference mode", liveSource)
	}
	if liveSource["erchef_postgres_dsn"] != "post..." || liveSource["bifrost_postgres_dsn"] != "post..." || liveSource["erchef_database"] != "chef_custom" || liveSource["bifrost_database"] != "authz_custom" {
		t.Fatalf("live_source split targets = %v", liveSource)
	}
	capabilities := requireAdminMigrationArray(t, out, "capabilities")
	requireAdminMigrationCapability(t, capabilities, "source_postgres_read_only", "planned")
	requireAdminMigrationCapability(t, capabilities, "source_http_server_api_version", "planned")
	requireAdminMigrationCapability(t, capabilities, "source_search_derived_only", "advisory")
	deps := requireAdminMigrationArray(t, out, "dependencies")
	requireAdminMigrationDependency(t, deps, "source_postgres", "ok")
	requireAdminMigrationDependency(t, deps, "source_schema", "ok")
	requireAdminMigrationDependency(t, deps, "source_blob", "skipped")
	requireAdminMigrationDependency(t, deps, "source_http", "skipped")
	requireAdminMigrationDependency(t, deps, "live_source_contract", "ok")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "source live extract will write a local normalized source bundle; preflight does not write source Chef or target OpenCook state")
	if _, ok := out["duration_ms"]; !ok {
		t.Fatalf("source live preflight output missing duration_ms: %v", out)
	}
}

func TestAdminMigrationSourceLivePreflightPostgresProbeFailureClasses(t *testing.T) {
	tests := []struct {
		name       string
		depMessage string
		code       string
		family     string
	}{
		{
			name:       "connection",
			depMessage: "source PostgreSQL could not be reached",
			code:       adminMigrationFindingSourcePostgresUnavailable,
			family:     "source_postgres",
		},
		{
			name:       "schema",
			depMessage: "source PostgreSQL schema is missing required Chef Server tables",
			code:       adminMigrationFindingSourceSchemaUnsupported,
			family:     "source_schema",
		},
		{
			name:       "permissions",
			depMessage: "source PostgreSQL schema tables could not be inspected",
			code:       adminMigrationFindingSourceSchemaUnsupported,
			family:     "source_schema",
		},
		{
			name:       "read_only",
			depMessage: "source PostgreSQL preflight was not running in a read-only transaction",
			code:       adminMigrationFindingSourcePostgresUnavailable,
			family:     "source_postgres",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			cmd.newLiveSource = newAdminMigrationLiveSourceExtractorWithPostgresProbe(&fakeAdminMigrationLiveSourcePostgresProbe{
				result: adminMigrationLiveSourcePostgresProbeResult{
					Dependencies: []adminMigrationDependency{{
						Name:       "source_postgres",
						Status:     "error",
						Backend:    "postgres",
						Configured: true,
						Message:    tc.depMessage,
					}},
					Findings: []adminMigrationFinding{{
						Severity: "error",
						Code:     tc.code,
						Family:   tc.family,
						Message:  "fake source PostgreSQL preflight failure",
					}},
				},
			})

			code := cmd.Run(context.Background(), []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://chef:pgsecret@source/chef", "--json"})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(source live preflight %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			if strings.Contains(stdout.String(), "pgsecret") || strings.Contains(stderr.String(), "pgsecret") {
				t.Fatalf("source live preflight %s leaked DSN secret; stdout = %s stderr = %s", tc.name, stdout.String(), stderr.String())
			}
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "findings"), tc.code)
		})
	}
}

func TestAdminMigrationSourceLivePreflightDefaultPostgresConnectionFailureIsRedacted(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "preflight",
		"--source-postgres-dsn", "postgres://chef:pgsecret@",
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(source live preflight default failure) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "pgsecret") || strings.Contains(stderr.String(), "pgsecret") {
		t.Fatalf("source live preflight leaked source DSN secret; stdout = %s stderr = %s", stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_erchef_postgres", "error")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bifrost_postgres", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), adminMigrationFindingSourceErchefUnavailable)
}

func TestAdminMigrationSourceLivePreflightRunsAgainstInjectedExtractor(t *testing.T) {
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{
		preflight: adminMigrationLiveSourceExtractorResult{
			Dependencies: []adminMigrationDependency{
				{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source PostgreSQL probe passed"},
			},
			Inventory: adminMigrationInventory{Families: []adminMigrationInventoryFamily{{Family: "users", Count: 2}}},
			Findings: []adminMigrationFinding{{
				Severity: "warning",
				Code:     adminMigrationFindingSourceFamilyUnsupported,
				Family:   "oc_id",
				Message:  "fake unsupported source family was reported",
			}},
			PlannedMutations: []adminMigrationPlannedMutation{{
				Action:  "fake_probe",
				Family:  "live_source",
				Message: "fake extractor stayed read-only",
			}},
		},
	}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "preflight",
		"--source-postgres-dsn", "postgres://source",
		"--all-orgs",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live preflight fake) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if fake.preflightCalls != 1 || fake.extractCalls != 0 {
		t.Fatalf("fake calls preflight=%d extract=%d, want preflight only", fake.preflightCalls, fake.extractCalls)
	}
	if !fake.configs[0].AllOrgs || fake.configs[0].PostgresDSN != "postgres://source" {
		t.Fatalf("fake config = %+v, want all org source postgres config", fake.configs[0])
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_postgres", "ok")
	requireAdminMigrationInventoryFamily(t, requireAdminMigrationArray(t, requireAdminMigrationMap(t, out, "inventory"), "families"), "", "users", 2)
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), adminMigrationFindingSourceFamilyUnsupported)
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "fake extractor stayed read-only")
}

func TestAdminMigrationSourceLivePreflightReportsInjectedFailureFindings(t *testing.T) {
	findingByCode := map[string]adminMigrationValidationFindingCode{}
	for _, finding := range adminMigrationLiveSourceFindingCodes() {
		findingByCode[finding.Code] = finding
	}
	for _, code := range []string{
		adminMigrationFindingSourcePostgresUnavailable,
		adminMigrationFindingSourceSchemaUnsupported,
		adminMigrationFindingSourceFamilyUnsupported,
		adminMigrationFindingSourceBlobUnavailable,
		adminMigrationFindingSourceBlobMissing,
		adminMigrationFindingSourceBlobChecksumMismatch,
		adminMigrationFindingSourceHTTPReadUnavailable,
	} {
		t.Run(code, func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			spec := findingByCode[code]
			fake := &fakeAdminMigrationLiveSourceExtractor{
				preflight: adminMigrationLiveSourceExtractorResult{
					Findings: []adminMigrationFinding{{
						Severity: spec.Severity,
						Code:     spec.Code,
						Family:   spec.Family,
						Message:  spec.Message,
					}},
				},
			}
			cmd.newLiveSource = fake.factory(t)

			got := cmd.Run(context.Background(), []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://source", "--json"})
			want := exitOK
			if spec.Severity == "error" {
				want = exitDependencyUnavailable
			}
			if got != want {
				t.Fatalf("Run(source live preflight %s) exit = %d, want %d; stdout = %s stderr = %s", code, got, want, stdout.String(), stderr.String())
			}
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "findings"), code)
		})
	}
}

func TestAdminMigrationSourceLivePreflightUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing source postgres",
			args: []string{"admin", "migration", "source", "live", "preflight", "--json"},
			want: "requires --source-postgres-dsn",
		},
		{
			name: "org plus all orgs",
			args: []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://source", "--org", "ponyville", "--all-orgs"},
			want: "cannot combine --all-orgs with --org",
		},
		{
			name: "bookshelf root plus blob url",
			args: []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://source", "--source-bookshelf-root", "/var/opt/opscode/bookshelf", "--source-blob-url", "s3://chef-bucket"},
			want: "cannot combine --source-bookshelf-root with --source-blob-url",
		},
		{
			name: "copy plus reference",
			args: []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://source", "--copy-blobs", "--reference-blobs"},
			want: "cannot combine --copy-blobs with --reference-blobs",
		},
		{
			name: "partial source HTTP",
			args: []string{"admin", "migration", "source", "live", "preflight", "--source-postgres-dsn", "postgres://source", "--source-server-url", "https://chef.example"},
			want: "HTTP probing requires --source-server-url, --source-requestor-name, and --source-private-key together",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, stdout, stderr := newTestCommand(t)
			code := cmd.Run(context.Background(), tc.args)
			if code != exitUsage {
				t.Fatalf("Run(source live preflight %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitUsage, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", stderr.String(), tc.want)
			}
		})
	}
}

func TestAdminMigrationSourceLiveExtractPublishesInjectedBundle(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	bundle := testAdminMigrationLiveSourceBundle(t)
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{
		extract: adminMigrationLiveSourceExtractorResult{
			Dependencies: []adminMigrationDependency{
				{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source PostgreSQL extraction passed"},
			},
			Inventory:        bundle.Inventory,
			Findings:         []adminMigrationFinding{{Severity: "warning", Code: adminMigrationFindingSourceHTTPReadUnavailable, Family: "source_http", Message: "fake optional HTTP read was unavailable"}},
			PlannedMutations: []adminMigrationPlannedMutation{{Action: "fake_extract", Family: "source_manifest", Message: "fake extractor emitted normalized source bundle"}},
			Bundle:           &bundle,
		},
	}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--source-bookshelf-root", "/var/opt/opscode/bookshelf",
		"--output", outputPath,
		"--copy-blobs",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live extract fake) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if fake.extractCalls != 1 || fake.preflightCalls != 0 {
		t.Fatalf("fake calls preflight=%d extract=%d, want extract only", fake.preflightCalls, fake.extractCalls)
	}
	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	if manifest.SourceType != "live_chef_infra_server" || len(manifest.Payloads) != 1 {
		t.Fatalf("manifest = %+v, want one live source payload", manifest)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_postgres", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "ok")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), adminMigrationFindingSourceHTTPReadUnavailable)
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "wrote local normalized live Chef source bundle atomically after verification")
}

func TestAdminMigrationSourceLiveExtractPublishesBootstrapBundleFromPostgresAdapter(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	checksum, body := testAdminMigrationLiveSourceCookbookBlob()
	bookshelfRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(bookshelfRoot, checksum), body, 0o644); err != nil {
		t.Fatalf("write test source blob error = %v", err)
	}
	cmd, stdout, stderr := newTestCommand(t)
	bootstrap := &fakeAdminMigrationLiveSourceBootstrapExtractor{
		payloadValues: testAdminMigrationLiveSourceBootstrapPayloadValues(t),
	}
	cmd.newLiveSource = newAdminMigrationLiveSourceExtractorWithPostgresAdapters(nil, bootstrap)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--source-bookshelf-root", bookshelfRoot,
		"--copy-blobs",
		"--output", outputPath,
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live extract bootstrap) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if bootstrap.calls != 1 || bootstrap.configs[0].PostgresDSN != "postgres://source" {
		t.Fatalf("bootstrap calls=%d configs=%+v, want one source config", bootstrap.calls, bootstrap.configs)
	}
	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	if manifest.SourceType != "live_chef_infra_server" {
		t.Fatalf("manifest source_type = %q, want live_chef_infra_server", manifest.SourceType)
	}
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/users.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/bootstrap/user_keys.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/organization.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/client_keys.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/group_memberships.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/acls.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/nodes.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/data_bag_items.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/policy_assignments.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/sandboxes.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/checksum_references.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/cookbook_versions.json")
	requireAdminMigrationSourcePayloadHash(t, outputPath, manifest.Payloads, "payloads/organizations/ponyville/cookbook_artifacts.json")
	if got := requireAdminMigrationSourceArtifactCount(t, manifest.Artifacts, "bookshelf"); got != 1 {
		t.Fatalf("bookshelf artifact count = %d, want 1", got)
	}
	copied, err := os.ReadFile(filepath.Join(outputPath, "blobs", "checksums", checksum))
	if err != nil {
		t.Fatalf("read copied live source blob error = %v", err)
	}
	if !bytes.Equal(copied, body) {
		t.Fatalf("copied live source blob = %q, want %q", copied, body)
	}

	read, err := adminMigrationReadSourceImportBundle(outputPath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(live bootstrap) error = %v", err)
	}
	state, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead(live bootstrap) error = %v", err)
	}
	if _, ok := state.Bootstrap.Users["pivotal"]; !ok {
		t.Fatalf("imported live bootstrap users = %#v, want pivotal", state.Bootstrap.Users)
	}
	org := state.Bootstrap.Orgs["ponyville"]
	if org.Organization.Name != "ponyville" || org.Clients["ponyville-validator"].Name == "" {
		t.Fatalf("imported live bootstrap org = %#v, want org and validator client", org)
	}
	if _, ok := org.ClientKeys["ponyville-validator"]["default"]; !ok {
		t.Fatalf("imported live bootstrap client keys = %#v, want validator default key", org.ClientKeys)
	}
	if !adminMigrationTestStringSliceContains(org.Groups["admins"].Users, "pivotal") {
		t.Fatalf("imported admins group users = %v, want pivotal", org.Groups["admins"].Users)
	}
	coreOrg := state.CoreObjects.Orgs["ponyville"]
	if coreOrg.Nodes["web01"].PolicyName != "base" || coreOrg.Nodes["web01"].PolicyGroup != "prod" {
		t.Fatalf("imported node policy refs = %#v, want base/prod compatibility fields", coreOrg.Nodes["web01"])
	}
	if got := coreOrg.DataBagItems["secrets"]["db"].RawData["encrypted_data"]; got != "fixture" {
		t.Fatalf("imported encrypted-looking data bag payload = %v, want fixture", got)
	}
	if coreOrg.PolicyGroups["prod"].Policies["base"] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("imported policy group prod = %#v, want base assignment", coreOrg.PolicyGroups["prod"])
	}
	if len(coreOrg.Sandboxes["sandbox-fixture"].Checksums) != 1 {
		t.Fatalf("imported sandbox = %#v, want one checksum", coreOrg.Sandboxes["sandbox-fixture"])
	}
	if _, ok := coreOrg.ACLs[adminMigrationNodeACLKey("web01")]; !ok {
		t.Fatalf("imported core ACLs = %#v, want node ACL", coreOrg.ACLs)
	}
	cookbookOrg := state.Cookbooks.Orgs["ponyville"]
	if len(cookbookOrg.Versions) != 1 || cookbookOrg.Versions[0].CookbookName != "base" || cookbookOrg.Versions[0].AllFiles[0].Checksum != checksum {
		t.Fatalf("imported live cookbook versions = %#v, want base version with shared checksum", cookbookOrg.Versions)
	}
	if len(cookbookOrg.Artifacts) != 1 || cookbookOrg.Artifacts[0].Name != "base" || cookbookOrg.Artifacts[0].AllFiles[0].Checksum != checksum {
		t.Fatalf("imported live cookbook artifacts = %#v, want base artifact with shared checksum", cookbookOrg.Artifacts)
	}

	liveSourceBundlePreflightCommand(t, outputPath, nil, exitOK, "migration_source_import_preflight")
	liveSourceBundlePreflightCommand(t, outputPath, &state, exitOK, "migration_source_sync_preflight")

	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bootstrap", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_blob", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "ok")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "extracted identity, authorization, core object, cookbook, and checksum-reference payloads from source PostgreSQL using read-only queries")
}

func TestAdminMigrationLiveSourceBundleRunsThroughMigrationPipelineEvidence(t *testing.T) {
	sourcePath := filepath.Join(t.TempDir(), "live-source")
	checksum, body := testAdminMigrationLiveSourceCookbookBlob()
	bookshelfRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(bookshelfRoot, checksum), body, 0o644); err != nil {
		t.Fatalf("write test source blob error = %v", err)
	}
	payloadValues := testAdminMigrationLiveSourceBootstrapPayloadValues(t)
	addPayload := func(key adminMigrationSourcePayloadKey, object any) {
		t.Helper()
		data, err := json.Marshal(object)
		if err != nil {
			t.Fatalf("json.Marshal(%+v) error = %v", object, err)
		}
		payloadValues[key] = append(payloadValues[key], json.RawMessage(data))
	}
	addPayload(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "groups"}, map[string]any{"name": "billing-admins", "groupname": "billing-admins", "orgname": "ponyville", "actors": []string{}, "users": []string{}, "clients": []string{}, "groups": []string{}})
	for _, group := range []string{"billing-admins", "users", "clients"} {
		addPayload(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("group:"+group, "pivotal", "admins"))
	}
	for _, container := range []string{"containers", "cookbooks", "data", "environments", "groups", "nodes", "roles", "sandboxes", "policies", "policy_groups", "cookbook_artifacts"} {
		addPayload(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "containers"}, map[string]any{"name": container, "containername": container, "containerpath": container})
		addPayload(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("container:"+container, "pivotal", "admins"))
	}
	bundle, blobExtraction, err := adminMigrationLiveSourceBundleFromPayloadValues(context.Background(), adminMigrationLiveSourceConfig{
		PostgresDSN:    "postgres://source",
		BookshelfRoot:  bookshelfRoot,
		CopyBlobs:      true,
		ExtractionMode: "copy_blobs",
		OutputPath:     sourcePath,
	}, payloadValues)
	if err != nil {
		t.Fatalf("adminMigrationLiveSourceBundleFromPayloadValues() error = %v findings = %+v", err, blobExtraction.Findings)
	}
	if err := adminMigrationWriteNormalizedSourceBundle(sourcePath, bundle, true); err != nil {
		t.Fatalf("adminMigrationWriteNormalizedSourceBundle(live) error = %v", err)
	}
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(live pipeline) error = %v", err)
	}
	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatalf("adminMigrationSourceImportStateFromRead(live pipeline) error = %v", err)
	}

	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	blobExists := map[string]bool{}
	blobPuts := map[string][]byte{}
	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "live source pipeline blob backend"},
			exists: blobExists,
			puts:   blobPuts,
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{
			DefaultOrganization: "ponyville",
			PostgresDSN:         "postgres://pipeline",
			BlobBackend:         "filesystem",
			BlobStorageURL:      t.TempDir(),
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}
	runMigration := func(args []string, wantCode int) (map[string]any, string) {
		t.Helper()
		stdout.Reset()
		stderr.Reset()
		code := cmd.Run(context.Background(), args)
		if code != wantCode {
			t.Fatalf("Run(%v) exit = %d, want %d; stdout = %s stderr = %s", args, code, wantCode, stdout.String(), stderr.String())
		}
		raw := stdout.String()
		return decodeAdminMigrationOutput(t, raw), raw
	}

	out, _ := runMigration([]string{"admin", "migration", "source", "import", "preflight", sourcePath, "--offline", "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bundle", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_target", "ok")

	importProgressPath := filepath.Join(t.TempDir(), "source-import-progress.json")
	out, _ = runMigration([]string{"admin", "migration", "source", "import", "apply", sourcePath, "--offline", "--yes", "--progress", importProgressPath, "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_write", "ok")
	if targetStore.bootstrap.Users["pivotal"].Username != "pivotal" {
		t.Fatalf("source import target user = %#v, want pivotal", targetStore.bootstrap.Users["pivotal"])
	}
	if len(targetStore.cookbookExport.Orgs["ponyville"].Versions) != 1 || len(blobPuts) != 1 {
		t.Fatalf("source import cookbook/blob state = versions:%d blobs:%d, want 1/1", len(targetStore.cookbookExport.Orgs["ponyville"].Versions), len(blobPuts))
	}
	if progress := mustReadAdminMigrationSourceImportProgress(t, importProgressPath); !progress.MetadataImported {
		t.Fatalf("source import progress = %#v, want metadata imported", progress)
	}

	out, _ = runMigration([]string{"admin", "migration", "source", "sync", "preflight", sourcePath, "--offline", "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_sync_progress", "ok")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "source sync found no PostgreSQL metadata changes for manifest-covered families")

	syncProgressPath := filepath.Join(t.TempDir(), "source-sync-progress.json")
	out, _ = runMigration([]string{"admin", "migration", "source", "sync", "apply", sourcePath, "--offline", "--yes", "--progress", syncProgressPath, "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_sync_write", "ok")
	if progress := mustReadAdminMigrationSourceSyncProgress(t, syncProgressPath); progress.SourceCursor != adminMigrationSourceSyncCursor(read) || progress.LastStatus != "applied" {
		t.Fatalf("source sync progress = %#v, want applied cursor %s", progress, adminMigrationSourceSyncCursor(read))
	}

	backupPath := filepath.Join(t.TempDir(), "opencook-backup")
	out, _ = runMigration([]string{"admin", "migration", "backup", "create", "--output", backupPath, "--offline", "--yes", "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_blob_copy", "ok")
	out, _ = runMigration([]string{"admin", "migration", "backup", "inspect", backupPath, "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "backup_bundle", "ok")

	restoreStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	restoreBlobExists := map[string]bool{}
	restoreBlobPuts := map[string][]byte{}
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "live source restore blob backend"},
			exists: restoreBlobExists,
			puts:   restoreBlobPuts,
		}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return restoreStore, nil, nil
	}
	out, _ = runMigration([]string{"admin", "migration", "restore", "preflight", backupPath, "--offline", "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_target", "ok")
	out, _ = runMigration([]string{"admin", "migration", "restore", "apply", backupPath, "--offline", "--yes", "--json"}, exitOK)
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_write", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "restore_validation", "ok")
	if restoreStore.bootstrap.Users["pivotal"].Username != "pivotal" || len(restoreStore.cookbookExport.Orgs["ponyville"].Artifacts) != 1 {
		t.Fatalf("restore state = user:%#v artifacts:%d, want pivotal and one artifact", restoreStore.bootstrap.Users["pivotal"], len(restoreStore.cookbookExport.Orgs["ponyville"].Artifacts))
	}

	reindexStore := restoreStore.fakeOfflineStore
	searchTarget := newAdminCapabilityOpenSearchTransport(t, adminCapabilityDirectDeleteByQuery)
	reindexCmd, reindexStdout, reindexStderr := newAdminCapabilityOpenSearchCommand(t, reindexStore, searchTarget)
	runSearchCommand := func(args []string, wantCode int) (map[string]any, string) {
		t.Helper()
		reindexStdout.Reset()
		reindexStderr.Reset()
		code := reindexCmd.Run(context.Background(), args)
		if code != wantCode {
			t.Fatalf("Run(%v) exit = %d, want %d; stdout = %s stderr = %s", args, code, wantCode, reindexStdout.String(), reindexStderr.String())
		}
		raw := reindexStdout.String()
		return decodeAdminMigrationOutput(t, raw), raw
	}
	out, _ = runSearchCommand([]string{"admin", "reindex", "--org", "ponyville", "--complete", "--json"}, exitOK)
	if counts := requireAdminMigrationMap(t, out, "counts"); counts["upserted"].(float64) == 0 {
		t.Fatalf("reindex counts = %v, want upserted documents", counts)
	}
	out, searchCleanRaw := runSearchCommand([]string{"admin", "search", "check", "--org", "ponyville", "--json"}, exitOK)
	if counts := requireAdminMigrationMap(t, out, "counts"); counts["clean"].(float64) != 1 {
		t.Fatalf("search check counts = %v, want clean", counts)
	}
	docIDs := searchTarget.documentIDs()
	if len(docIDs) == 0 {
		t.Fatalf("search target had no documents after reindex")
	}
	delete(searchTarget.docs, docIDs[0])
	searchTarget.forceDocument("ponyville/node/stale-live-source-node")
	runSearchCommand([]string{"admin", "search", "repair", "--org", "ponyville", "--yes", "--json"}, exitOK)
	_, searchCleanRaw = runSearchCommand([]string{"admin", "search", "check", "--org", "ponyville", "--json"}, exitOK)

	shadowFake := adminMigrationShadowClientForSource(t, sourcePath, nil)
	shadowCmd, shadowStdout, shadowStderr := newTestCommand(t)
	shadowCmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "/keys/pivotal.pem", ServerAPIVersion: "1"}
	}
	shadowCmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return shadowFake, nil
	}
	code := shadowCmd.Run(context.Background(), []string{"admin", "migration", "shadow", "compare", "--source", sourcePath, "--target-server-url", "http://opencook.test", "--json"})
	if code != exitOK {
		t.Fatalf("Run(migration shadow compare live source) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, shadowStdout.String(), shadowStderr.String())
	}
	shadowResultPath := filepath.Join(t.TempDir(), "shadow-compare.json")
	writeAdminMigrationSourceFile(t, shadowResultPath, shadowStdout.String())
	searchResultPath := filepath.Join(t.TempDir(), "search-check.json")
	writeAdminMigrationSourceFile(t, searchResultPath, searchCleanRaw)
	maintenanceResultPath := filepath.Join(t.TempDir(), "maintenance-status.json")
	writeAdminMigrationSourceJSON(t, maintenanceResultPath, map[string]any{
		"ok":      true,
		"command": "maintenance_status",
		"active":  true,
		"expired": false,
		"backend": map[string]any{"name": "postgres", "shared": true},
		"state":   map[string]any{"mode": "cutover"},
	})

	cutoverFake := adminMigrationShadowClientForSource(t, sourcePath, nil)
	cutoverCmd, cutoverStdout, cutoverStderr := newTestCommand(t)
	cutoverCmd.loadAdminConfig = func() admin.Config {
		return admin.Config{ServerURL: "http://opencook.test", RequestorName: "pivotal", PrivateKeyPath: "/keys/pivotal.pem", ServerAPIVersion: "1"}
	}
	cutoverCmd.newAdmin = func(admin.Config) (adminJSONClient, error) {
		return cutoverFake, nil
	}
	code = cutoverCmd.Run(context.Background(), []string{
		"admin", "migration", "cutover", "rehearse",
		"--manifest", filepath.Join(backupPath, adminMigrationBackupManifestPath),
		"--source", sourcePath,
		"--source-import-progress", importProgressPath,
		"--source-sync-progress", syncProgressPath,
		"--search-check-result", searchResultPath,
		"--shadow-result", shadowResultPath,
		"--maintenance-result", maintenanceResultPath,
		"--source-frozen",
		"--rollback-ready",
		"--server-url", "http://opencook.test",
		"--requestor-name", "pivotal",
		"--private-key", "/keys/pivotal.pem",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(migration cutover rehearse live source) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, cutoverStdout.String(), cutoverStderr.String())
	}
	out = decodeAdminMigrationOutput(t, cutoverStdout.String())
	deps := requireAdminMigrationArray(t, out, "dependencies")
	sourceDep := requireAdminMigrationDependency(t, deps, "cutover_source_bundle", "ok")
	sourceDetails := requireAdminMigrationMap(t, sourceDep, "details")
	if sourceDetails["source_type"] != "live_chef_infra_server" || sourceDetails["source_origin"] != "live_extraction" || sourceDetails["source_live_extraction"] != "true" {
		t.Fatalf("cutover source details = %v, want live extraction metadata", sourceDetails)
	}
	requireAdminMigrationDependency(t, deps, "source_freeze_evidence", "ok")
	requireAdminMigrationDependency(t, deps, "rollback_readiness", "ok")
	requireAdminMigrationDependency(t, deps, "cutover_evidence", "ok")
	if sourceState.Bootstrap.Users["pivotal"].Username == "" {
		t.Fatalf("live source state unexpectedly lost pivotal user: %#v", sourceState.Bootstrap.Users)
	}
}

func TestAdminMigrationSourceLiveExtractReferenceOnlyKeepsBlobValidationDeferred(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	checksum, _ := testAdminMigrationLiveSourceCookbookBlob()
	cmd, stdout, stderr := newTestCommand(t)
	bootstrap := &fakeAdminMigrationLiveSourceBootstrapExtractor{
		payloadValues: testAdminMigrationLiveSourceBootstrapPayloadValues(t),
	}
	cmd.newLiveSource = newAdminMigrationLiveSourceExtractorWithPostgresAdapters(nil, bootstrap)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--reference-blobs",
		"--output", outputPath,
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live extract reference-only) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	manifest := mustReadAdminMigrationSourceManifest(t, outputPath)
	if got := requireAdminMigrationSourceArtifactCountOrZero(manifest.Artifacts, "bookshelf"); got != 0 {
		t.Fatalf("bookshelf artifact count = %d, want 0 for reference-only extraction", got)
	}
	if _, err := os.Stat(filepath.Join(outputPath, "blobs", "checksums", checksum)); !os.IsNotExist(err) {
		t.Fatalf("reference-only source blob stat error = %v, want not exist", err)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_blob", "warning")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_blob_payload_missing")

	read, err := adminMigrationReadSourceImportBundle(outputPath)
	if err != nil {
		t.Fatalf("adminMigrationReadSourceImportBundle(reference-only live source) error = %v", err)
	}
	if len(read.ReferencedChecksums) == 0 || read.ReferencedChecksums[0].Checksum != checksum {
		t.Fatalf("reference-only checksums = %#v, want shared cookbook/sandbox checksum", read.ReferencedChecksums)
	}
	liveSourceBundlePreflightCommand(t, outputPath, nil, exitOK, "migration_source_import_preflight")
}

func TestAdminMigrationSourceLiveExtractDryRunSkipsAtomicPublish(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	if err := os.MkdirAll(outputPath, 0o755); err != nil {
		t.Fatalf("mkdir existing output error = %v", err)
	}
	marker := filepath.Join(outputPath, "operator-marker")
	if err := os.WriteFile(marker, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write output marker error = %v", err)
	}
	bundle := testAdminMigrationLiveSourceBundle(t)
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{
		extract: adminMigrationLiveSourceExtractorResult{
			Dependencies: []adminMigrationDependency{{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source PostgreSQL extraction passed"}},
			Inventory:    bundle.Inventory,
			Bundle:       &bundle,
		},
	}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--output", outputPath,
		"--dry-run",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live extract dry-run) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if fake.extractCalls != 1 {
		t.Fatalf("fake extract calls = %d, want 1", fake.extractCalls)
	}
	if _, err := os.Stat(filepath.Join(outputPath, adminMigrationSourceManifestPath)); !os.IsNotExist(err) {
		t.Fatalf("dry-run manifest stat error = %v, want not exist", err)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep me" {
		t.Fatalf("dry-run marker read = %q/%v, want existing marker preserved", data, err)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["dry_run"] != true {
		t.Fatalf("dry_run = %v, want true", out["dry_run"])
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "skipped")
	requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "would write local normalized live Chef source bundle atomically after verification")
}

func TestAdminMigrationSourceLiveExtractExistingOutputRequiresConfirmation(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	if err := os.MkdirAll(outputPath, 0o755); err != nil {
		t.Fatalf("mkdir existing output error = %v", err)
	}
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--output", outputPath,
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(source live extract existing output) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if fake.extractCalls != 0 {
		t.Fatalf("fake extract calls = %d, want pre-output-safety block", fake.extractCalls)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_normalize_output_exists")
}

func TestAdminMigrationSourceLiveExtractInterruptedTempDirRequiresConfirmation(t *testing.T) {
	parent := t.TempDir()
	outputPath := filepath.Join(parent, "live-source")
	staleTemp := filepath.Join(parent, ".live-source-normalize-stale")
	if err := os.MkdirAll(staleTemp, 0o755); err != nil {
		t.Fatalf("mkdir stale temp error = %v", err)
	}
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--output", outputPath,
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(source live extract stale temp) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if fake.extractCalls != 0 {
		t.Fatalf("fake extract calls = %d, want pre-output-safety block", fake.extractCalls)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), "source_normalize_output_interrupted")

	bundle := testAdminMigrationLiveSourceBundle(t)
	cmd, stdout, stderr = newTestCommand(t)
	fake = &fakeAdminMigrationLiveSourceExtractor{
		extract: adminMigrationLiveSourceExtractorResult{
			Dependencies: []adminMigrationDependency{{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source PostgreSQL extraction passed"}},
			Inventory:    bundle.Inventory,
			Bundle:       &bundle,
		},
	}
	cmd.newLiveSource = fake.factory(t)
	code = cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--output", outputPath,
		"--yes",
		"--json",
	})
	if code != exitOK {
		t.Fatalf("Run(source live extract stale temp --yes) exit = %d, want %d; stdout = %s stderr = %s", code, exitOK, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(staleTemp); !os.IsNotExist(err) {
		t.Fatalf("stale temp stat after --yes = %v, want removed", err)
	}
	if _, err := os.Stat(filepath.Join(outputPath, adminMigrationSourceManifestPath)); err != nil {
		t.Fatalf("published manifest stat error = %v", err)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "dependencies"), "normalized_source_output", "ok")
}

func TestAdminMigrationSourceLiveExtractVerifiesBundleBeforePublish(t *testing.T) {
	tests := []struct {
		name        string
		bundle      adminMigrationSourceNormalizeBundle
		wantFinding string
	}{
		{
			name: "manifest hash mismatch",
			bundle: func() adminMigrationSourceNormalizeBundle {
				bundle := testAdminMigrationLiveSourceBundle(t)
				bundle.Manifest.Payloads[0].SHA256 = strings.Repeat("0", 64)
				return bundle
			}(),
			wantFinding: "source_payload_hash_mismatch",
		},
		{
			name:        "invalid generated payload json",
			bundle:      testAdminMigrationLiveSourceInvalidPayloadBundle([]byte(`not json`)),
			wantFinding: "source_payload_invalid_json",
		},
		{
			name:        "trailing generated payload json",
			bundle:      testAdminMigrationLiveSourceInvalidPayloadBundle([]byte(`[{"name":"pivotal"}]{"extra":true}`)),
			wantFinding: "source_payload_invalid_json",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "live-source")
			cmd, stdout, stderr := newTestCommand(t)
			fake := &fakeAdminMigrationLiveSourceExtractor{
				extract: adminMigrationLiveSourceExtractorResult{
					Dependencies: []adminMigrationDependency{{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake source PostgreSQL extraction passed"}},
					Inventory:    tc.bundle.Inventory,
					Bundle:       &tc.bundle,
				},
			}
			cmd.newLiveSource = fake.factory(t)
			code := cmd.Run(context.Background(), []string{
				"admin", "migration", "source", "live", "extract",
				"--source-postgres-dsn", "postgres://source",
				"--output", outputPath,
				"--json",
			})
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(source live extract %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("source live extract %s output stat error = %v, want not exist", tc.name, err)
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "normalized_source_output", "error")
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantFinding)
		})
	}
}

func TestAdminMigrationSourceLiveExtractCopyBlobFailuresDoNotPublish(t *testing.T) {
	tests := []struct {
		name          string
		args          []string
		prepare       func(t *testing.T) string
		wantFinding   string
		forbidOutputs []string
	}{
		{
			name:        "missing filesystem blob",
			wantFinding: adminMigrationFindingSourceBlobMissing,
			prepare: func(t *testing.T) string {
				t.Helper()
				return t.TempDir()
			},
		},
		{
			name:        "checksum mismatch",
			wantFinding: adminMigrationFindingSourceBlobChecksumMismatch,
			prepare: func(t *testing.T) string {
				t.Helper()
				checksum, _ := testAdminMigrationLiveSourceCookbookBlob()
				root := t.TempDir()
				if err := os.WriteFile(filepath.Join(root, checksum), []byte("not the checksum body"), 0o644); err != nil {
					t.Fatalf("write mismatched source blob error = %v", err)
				}
				return root
			},
		},
		{
			name:          "provider url cannot copy deterministic bytes",
			args:          []string{"--source-blob-url", "https://blob-user:blob-secret@blob.example/checksums?token=source-token"},
			wantFinding:   adminMigrationFindingSourceBlobUnavailable,
			forbidOutputs: []string{"blob-secret", "source-token"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			outputPath := filepath.Join(t.TempDir(), "live-source")
			cmd, stdout, stderr := newTestCommand(t)
			bootstrap := &fakeAdminMigrationLiveSourceBootstrapExtractor{
				payloadValues: testAdminMigrationLiveSourceBootstrapPayloadValues(t),
			}
			cmd.newLiveSource = newAdminMigrationLiveSourceExtractorWithPostgresAdapters(nil, bootstrap)
			args := []string{
				"admin", "migration", "source", "live", "extract",
				"--source-postgres-dsn", "postgres://source",
				"--copy-blobs",
				"--output", outputPath,
				"--json",
			}
			if tc.prepare != nil {
				root := tc.prepare(t)
				args = append(args, "--source-bookshelf-root", root)
			}
			args = append(args, tc.args...)

			code := cmd.Run(context.Background(), args)
			if code != exitDependencyUnavailable {
				t.Fatalf("Run(source live extract %s) exit = %d, want %d; stdout = %s stderr = %s", tc.name, code, exitDependencyUnavailable, stdout.String(), stderr.String())
			}
			if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
				t.Fatalf("source live extract %s output stat error = %v, want not exist", tc.name, err)
			}
			combined := stdout.String() + stderr.String()
			for _, forbidden := range tc.forbidOutputs {
				if forbidden != "" && strings.Contains(combined, forbidden) {
					t.Fatalf("source live extract %s leaked %q; stdout = %s stderr = %s", tc.name, forbidden, stdout.String(), stderr.String())
				}
			}
			out := decodeAdminMigrationOutput(t, stdout.String())
			requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_blob", "error")
			requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), tc.wantFinding)
		})
	}
}

func TestAdminMigrationSourceLiveExtractInterruptedDoesNotPublish(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	cmd, stdout, stderr := newTestCommand(t)
	fake := &fakeAdminMigrationLiveSourceExtractor{
		extract: adminMigrationLiveSourceExtractorResult{
			Findings: []adminMigrationFinding{{
				Severity: "error",
				Code:     adminMigrationFindingSourceExtractionInterrupted,
				Family:   "live_source",
				Message:  "fake extractor stopped before bundle publication",
			}},
		},
	}
	cmd.newLiveSource = fake.factory(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://source",
		"--output", outputPath,
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(source live extract interrupted) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("source live extract interrupted output stat error = %v, want not exist", err)
	}
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, decodeAdminMigrationOutput(t, stdout.String()), "findings"), adminMigrationFindingSourceExtractionInterrupted)
}

func TestAdminMigrationSourceLiveExtractPostgresFailureDoesNotWrite(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "live-source")
	cmd, stdout, stderr := newTestCommand(t)

	code := cmd.Run(context.Background(), []string{
		"admin", "migration", "source", "live", "extract",
		"--source-postgres-dsn", "postgres://chef:pgsecret@",
		"--source-bookshelf-root", "/var/opt/opscode/bookshelf",
		"--output", outputPath,
		"--copy-blobs",
		"--yes",
		"--json",
	})
	if code != exitDependencyUnavailable {
		t.Fatalf("Run(source live extract) exit = %d, want %d; stdout = %s stderr = %s", code, exitDependencyUnavailable, stdout.String(), stderr.String())
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("source live extract output path stat error = %v, want not exist", err)
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != "migration_source_live_extract" {
		t.Fatalf("command = %v, want migration_source_live_extract", out["command"])
	}
	target := requireAdminMigrationMap(t, out, "target")
	if target["output_path"] != outputPath {
		t.Fatalf("target.output_path = %v, want %s", target["output_path"], outputPath)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_erchef_postgres", "error")
	requireAdminMigrationFinding(t, requireAdminMigrationArray(t, out, "findings"), adminMigrationFindingSourceErchefUnavailable)
	if strings.Contains(stdout.String(), "pgsecret") {
		t.Fatalf("source live extract leaked source DSN secret: %s", stdout.String())
	}
}

func TestAdminMigrationLiveSourceRawPayloadPreservesCompressedChefJSON(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write([]byte(`{"name":"raw-node","normal":{"secret":"opaque"},"policy_name":"base"}`)); err != nil {
		t.Fatalf("gzip write error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip close error = %v", err)
	}

	object := map[string]any{"name": "sql-node"}
	adminMigrationLiveSourceMergeRawPayload(object, adminMigrationLiveSourceStringFromBytes(compressed.Bytes()))
	if object["name"] != "sql-node" {
		t.Fatalf("merged name = %v, want SQL-selected name to remain authoritative", object["name"])
	}
	if object["policy_name"] != "base" {
		t.Fatalf("merged policy_name = %v, want base", object["policy_name"])
	}
	normal, ok := object["normal"].(map[string]any)
	if !ok || normal["secret"] != "opaque" {
		t.Fatalf("merged normal attrs = %#v, want opaque raw JSON preserved", object["normal"])
	}
}

func requireAdminMigrationLiveSourceFamilies(t *testing.T, mappings map[string]adminMigrationLiveSourceFamilyMapping, families []string) {
	t.Helper()
	for _, family := range families {
		if _, ok := mappings[family]; !ok {
			t.Fatalf("live source mappings = %v, want family %s", mappings, family)
		}
	}
}

func requireAdminMigrationCapability(t *testing.T, source []any, name, status string) map[string]any {
	t.Helper()
	for _, item := range source {
		entry, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("capability entry = %#v, want object", item)
		}
		if entry["name"] == name {
			if entry["status"] != status {
				t.Fatalf("capability %s status = %v, want %s", name, entry["status"], status)
			}
			return entry
		}
	}
	t.Fatalf("capabilities = %v, want %s/%s", source, name, status)
	return nil
}

type fakeAdminMigrationLiveSourceExtractor struct {
	preflight      adminMigrationLiveSourceExtractorResult
	extract        adminMigrationLiveSourceExtractorResult
	preflightCalls int
	extractCalls   int
	configs        []adminMigrationLiveSourceConfig
}

func (f *fakeAdminMigrationLiveSourceExtractor) factory(t *testing.T) func(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
	t.Helper()
	return func(cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
		f.configs = append(f.configs, cfg)
		return f
	}
}

func (f *fakeAdminMigrationLiveSourceExtractor) Preflight(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	f.preflightCalls++
	if ctx == nil {
		return adminMigrationLiveSourceExtractorResult{Findings: []adminMigrationFinding{{Severity: "error", Code: adminMigrationFindingSourceExtractionInterrupted, Family: "live_source", Message: "context was nil"}}}
	}
	return f.preflight
}

func (f *fakeAdminMigrationLiveSourceExtractor) Extract(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	f.extractCalls++
	if ctx == nil {
		return adminMigrationLiveSourceExtractorResult{Findings: []adminMigrationFinding{{Severity: "error", Code: adminMigrationFindingSourceExtractionInterrupted, Family: "live_source", Message: "context was nil"}}}
	}
	return f.extract
}

type fakeAdminMigrationLiveSourcePostgresProbe struct {
	result  adminMigrationLiveSourcePostgresProbeResult
	calls   int
	configs []adminMigrationLiveSourceConfig
}

func (f *fakeAdminMigrationLiveSourcePostgresProbe) Probe(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourcePostgresProbeResult {
	f.calls++
	f.configs = append(f.configs, cfg)
	if ctx == nil {
		return adminMigrationLiveSourcePostgresProbeResult{
			Findings: []adminMigrationFinding{{Severity: "error", Code: adminMigrationFindingSourceExtractionInterrupted, Family: "live_source", Message: "context was nil"}},
		}
	}
	return f.result
}

type fakeAdminMigrationLiveSourceBootstrapExtractor struct {
	payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage
	calls         int
	configs       []adminMigrationLiveSourceConfig
}

func (f *fakeAdminMigrationLiveSourceBootstrapExtractor) ExtractBootstrap(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	f.calls++
	f.configs = append(f.configs, cfg)
	if ctx == nil {
		return adminMigrationLiveSourceExtractorResult{
			Findings: []adminMigrationFinding{{Severity: "error", Code: adminMigrationFindingSourceExtractionInterrupted, Family: "live_source", Message: "context was nil"}},
		}
	}
	payloadValues := cloneAdminMigrationSourcePayloadValues(f.payloadValues)
	bundle, blobExtraction, err := adminMigrationLiveSourceBundleFromPayloadValues(ctx, cfg, payloadValues)
	dependencies := []adminMigrationDependency{
		{Name: "source_postgres", Status: "ok", Backend: "postgres", Configured: true, Message: "fake read-only source PostgreSQL extraction passed"},
		{Name: "source_bootstrap", Status: "ok", Backend: "postgres", Configured: true, Message: "fake bootstrap extraction passed"},
	}
	if blobExtraction.Dependency.Name != "" {
		dependencies = append(dependencies, blobExtraction.Dependency)
	}
	dependencies = append(dependencies, adminMigrationLiveSourceNonBlobDependencies(cfg)...)
	if err != nil {
		findings := append([]adminMigrationFinding{}, blobExtraction.Findings...)
		if blobExtraction.Dependency.Status == "error" {
			return adminMigrationLiveSourceExtractorResult{
				Dependencies: dependencies,
				Findings:     findings,
			}
		}
		return adminMigrationLiveSourceExtractorResult{
			Dependencies: dependencies,
			Findings:     append(findings, adminMigrationFinding{Severity: "error", Code: adminMigrationFindingSourceSchemaUnsupported, Family: "source_bootstrap", Message: "fake bootstrap payloads could not be normalized"}),
		}
	}
	return adminMigrationLiveSourceExtractorResult{
		Dependencies: dependencies,
		Inventory:    bundle.Inventory,
		Findings:     append(blobExtraction.Findings, bundle.Findings...),
		PlannedMutations: []adminMigrationPlannedMutation{{
			Action:  "read_source_postgres_payloads",
			Family:  "source_payloads",
			Count:   len(bundle.Manifest.Payloads),
			Message: "extracted identity, authorization, core object, cookbook, and checksum-reference payloads from source PostgreSQL using read-only queries",
		}},
		Bundle: &bundle,
	}
}

func testAdminMigrationLiveSourceBundle(t *testing.T) adminMigrationSourceNormalizeBundle {
	t.Helper()
	files := map[string][]byte{}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(map[adminMigrationSourcePayloadKey][]json.RawMessage{
		{Family: "users"}: []json.RawMessage{json.RawMessage(`{"name":"pivotal","admin":true}`)},
	}, files)
	if err != nil {
		t.Fatalf("adminMigrationMaterializeSourcePayloadFiles() error = %v", err)
	}
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    "live_chef_infra_server",
		Payloads:      payloads,
		Notes:         []string{"Generated by fake live-source extractor."},
	}
	return adminMigrationSourceNormalizeBundle{
		Manifest:      manifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(manifest),
		SourceType:    manifest.SourceType,
		FormatVersion: manifest.FormatVersion,
	}
}

func testAdminMigrationLiveSourceInvalidPayloadBundle(data []byte) adminMigrationSourceNormalizeBundle {
	path := "payloads/bootstrap/users.json"
	files := map[string][]byte{path: append([]byte(nil), data...)}
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    "live_chef_infra_server",
		Payloads: []adminMigrationSourceManifestPayload{{
			Family: "users",
			Path:   path,
			Count:  1,
			SHA256: adminMigrationSHA256Hex(data),
		}},
		Notes: []string{"Generated by fake live-source extractor."},
	}
	return adminMigrationSourceNormalizeBundle{
		Manifest:      manifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(manifest),
		SourceType:    manifest.SourceType,
		FormatVersion: manifest.FormatVersion,
	}
}

func testAdminMigrationLiveSourceBootstrapPayloadValues(t *testing.T) map[adminMigrationSourcePayloadKey][]json.RawMessage {
	t.Helper()
	publicKey := string(mustMarshalPublicKeyPEM(t, &mustGenerateAdminPrivateKey(t).PublicKey))
	checksum, _ := testAdminMigrationLiveSourceCookbookBlob()
	revisionID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	artifactIdentifier := strings.Repeat("b", 40)
	cookbookFile := map[string]any{"name": "default.rb", "path": "recipes/default.rb", "checksum": checksum, "specificity": "default"}
	cookbookMetadata := map[string]any{"name": "base", "version": "1.0.0", "dependencies": map[string]any{}, "platforms": map[string]any{}}
	values := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	add := func(key adminMigrationSourcePayloadKey, object any) {
		data, err := json.Marshal(object)
		if err != nil {
			t.Fatalf("json.Marshal(%+v) error = %v", object, err)
		}
		values[key] = append(values[key], json.RawMessage(data))
	}
	for _, family := range []string{"users", "user_acls", "user_keys", "server_admin_memberships"} {
		values[adminMigrationSourcePayloadKey{Family: family}] = []json.RawMessage{}
	}
	for _, family := range adminMigrationLiveSourceCoveredOrgFamilies() {
		values[adminMigrationSourcePayloadKey{Organization: "ponyville", Family: family}] = []json.RawMessage{}
	}
	add(adminMigrationSourcePayloadKey{Family: "users"}, map[string]any{"username": "pivotal", "name": "pivotal", "email": "pivotal@example.test", "display_name": "Pivotal User"})
	add(adminMigrationSourcePayloadKey{Family: "user_keys"}, map[string]any{"username": "pivotal", "key_name": "default", "public_key": publicKey, "expiration_date": "infinity"})
	add(adminMigrationSourcePayloadKey{Family: "user_acls"}, testAdminMigrationLiveSourceACLObject("user:pivotal", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, map[string]any{"actor": "pivotal", "type": "user"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "organizations"}, map[string]any{"name": "ponyville", "orgname": "ponyville", "full_name": "Ponyville", "org_type": "Business", "guid": "ponyville"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "clients"}, map[string]any{"name": "ponyville-validator", "clientname": "ponyville-validator", "orgname": "ponyville", "validator": true, "admin": false, "public_key": publicKey})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "client_keys"}, map[string]any{"client": "ponyville-validator", "clientname": "ponyville-validator", "key_name": "default", "public_key": publicKey, "expiration_date": "infinity"})
	for _, group := range []string{"admins", "clients", "users"} {
		add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "groups"}, map[string]any{"name": group, "groupname": group, "orgname": "ponyville", "actors": []string{}, "users": []string{}, "clients": []string{}, "groups": []string{}})
	}
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "group_memberships"}, map[string]any{"group": "admins", "actor": "pivotal", "type": "user"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "group_memberships"}, map[string]any{"group": "clients", "actor": "ponyville-validator", "type": "client"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "containers"}, map[string]any{"name": "clients", "containername": "clients", "containerpath": "clients"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("organization:ponyville", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("client:ponyville-validator", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("group:admins", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("container:clients", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "nodes"}, map[string]any{
		"name":             "web01",
		"chef_environment": "_default",
		"run_list":         []string{"role[web]"},
		"normal":           map[string]any{"app": "opencook"},
		"default":          map[string]any{},
		"override":         map[string]any{},
		"automatic":        map[string]any{},
		"policy_name":      "base",
		"policy_group":     "prod",
	})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "environments"}, map[string]any{"name": "_default", "description": "The default Chef environment", "cookbook_versions": map[string]any{}})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "roles"}, map[string]any{"name": "web", "run_list": []string{"recipe[base]"}, "env_run_lists": map[string]any{"_default": []string{"recipe[base]"}}})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "data_bags"}, map[string]any{"name": "secrets"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "data_bag_items"}, map[string]any{"bag": "secrets", "id": "db", "payload": map[string]any{"id": "db", "encrypted_data": "fixture", "iv": "still-opaque"}})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "policy_revisions"}, map[string]any{
		"name":                  "base",
		"revision_id":           revisionID,
		"run_list":              []string{"recipe[base]"},
		"named_run_lists":       map[string]any{},
		"cookbook_locks":        map[string]any{},
		"solution_dependencies": map[string]any{},
	})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "policy_groups"}, map[string]any{"name": "prod"})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "policy_assignments"}, map[string]any{"group": "prod", "policy": "base", "revision_id": revisionID})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "sandboxes"}, map[string]any{"id": "sandbox-fixture", "completed": true, "checksums": []string{checksum}})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "checksum_references"}, map[string]any{"family": "sandboxes", "id": "sandbox-fixture", "checksum": checksum})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "cookbook_versions"}, map[string]any{
		"cookbook_name": "base",
		"version":       "1.0.0",
		"name":          "base-1.0.0",
		"metadata":      cookbookMetadata,
		"all_files":     []any{cookbookFile},
		"recipes":       []any{cookbookFile},
	})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "cookbook_artifacts"}, map[string]any{
		"name":       "base",
		"identifier": artifactIdentifier,
		"version":    "1.0.0",
		"metadata":   cookbookMetadata,
		"all_files":  []any{cookbookFile},
		"recipes":    []any{cookbookFile},
	})
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("node:web01", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("environment:_default", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("role:web", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("data_bag:secrets", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("policy:base", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("policy_group:prod", "pivotal", "admins"))
	add(adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}, testAdminMigrationLiveSourceACLObject("sandbox:sandbox-fixture", "pivotal", "admins"))
	return values
}

func testAdminMigrationLiveSourceCookbookBlob() (string, []byte) {
	body := []byte("live source cookbook blob bytes")
	return adminMigrationMD5Hex(body), body
}

func liveSourceBundlePreflightCommand(t *testing.T, outputPath string, importedState *adminMigrationSourceImportState, wantCode int, wantCommand string) {
	t.Helper()
	checksum, _ := testAdminMigrationLiveSourceCookbookBlob()
	targetStore := &fakeMigrationInventoryStore{
		fakeOfflineStore:  &fakeOfflineStore{},
		cookbookInventory: map[string]adminMigrationCookbookInventory{},
		cookbookExport:    adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	args := []string{"admin", "migration", "source", "import", "preflight", outputPath, "--offline", "--json"}
	if importedState != nil {
		targetStore.fakeOfflineStore.bootstrap = importedState.Bootstrap
		targetStore.fakeOfflineStore.objects = importedState.CoreObjects
		targetStore.cookbookInventory = adminMigrationCookbookInventoryFromExport(importedState.Cookbooks)
		targetStore.cookbookExport = importedState.Cookbooks
		args = []string{"admin", "migration", "source", "sync", "preflight", outputPath, "--offline", "--json"}
	}

	cmd, stdout, stderr := newTestCommand(t)
	cmd.newBlobStore = func(config.Config) (blob.Store, error) {
		return fakeMigrationBlobStore{
			status: blob.Status{Backend: "filesystem", Configured: true, Message: "test blob backend"},
			exists: map[string]bool{checksum: true},
		}, nil
	}
	cmd.loadOffline = func() (config.Config, error) {
		return config.Config{PostgresDSN: "postgres://opencook", BlobBackend: "filesystem", BlobStorageURL: t.TempDir()}, nil
	}
	cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
		return targetStore, nil, nil
	}

	code := cmd.Run(context.Background(), args)
	if code != wantCode {
		t.Fatalf("Run(%s live source bundle) exit = %d, want %d; stdout = %s stderr = %s", wantCommand, code, wantCode, stdout.String(), stderr.String())
	}
	out := decodeAdminMigrationOutput(t, stdout.String())
	if out["command"] != wantCommand {
		t.Fatalf("command = %v, want %s", out["command"], wantCommand)
	}
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_bundle", "ok")
	requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "postgres", "ok")
	if importedState == nil {
		requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_import_target", "ok")
		requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "would create PostgreSQL-backed nodes records from normalized source payloads")
	} else {
		requireAdminMigrationDependency(t, requireAdminMigrationArray(t, out, "dependencies"), "source_sync_progress", "ok")
		requireAdminMigrationMutationMessage(t, requireAdminMigrationArray(t, out, "planned_mutations"), "source sync found no PostgreSQL metadata changes for manifest-covered families")
	}
	if targetStore.bootstrapSaves != 0 || targetStore.objectSaves != 0 {
		t.Fatalf("%s preflight mutated bootstrap=%d objects=%d", wantCommand, targetStore.bootstrapSaves, targetStore.objectSaves)
	}
}

func testAdminMigrationLiveSourceACLObject(resource, actor, group string) map[string]any {
	permission := map[string]any{"actors": []string{actor}, "groups": []string{group}}
	return map[string]any{
		"resource": resource,
		"create":   permission,
		"read":     permission,
		"update":   permission,
		"delete":   permission,
		"grant":    permission,
	}
}

func requireAdminMigrationSourceArtifactCount(t *testing.T, artifacts []adminMigrationSourceManifestArtifact, family string) int {
	t.Helper()
	for _, artifact := range artifacts {
		if artifact.Family == family {
			return artifact.Count
		}
	}
	t.Fatalf("manifest artifacts = %#v, want family %s", artifacts, family)
	return 0
}

func requireAdminMigrationSourceArtifactCountOrZero(artifacts []adminMigrationSourceManifestArtifact, family string) int {
	for _, artifact := range artifacts {
		if artifact.Family == family {
			return artifact.Count
		}
	}
	return 0
}

func cloneAdminMigrationSourcePayloadValues(values map[adminMigrationSourcePayloadKey][]json.RawMessage) map[adminMigrationSourcePayloadKey][]json.RawMessage {
	out := make(map[adminMigrationSourcePayloadKey][]json.RawMessage, len(values))
	for key, records := range values {
		out[key] = append([]json.RawMessage(nil), records...)
	}
	return out
}
