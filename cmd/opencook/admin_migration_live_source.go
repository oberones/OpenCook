package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	adminMigrationLiveSourcePostgresProbeTimeout      = 10 * time.Second
	adminMigrationLiveSourcePostgresExtractionTimeout = 30 * time.Minute
)

type adminMigrationLiveSourceExtractor interface {
	Preflight(context.Context, adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult
	Extract(context.Context, adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult
}

type adminMigrationLiveSourceExtractorResult struct {
	Dependencies     []adminMigrationDependency
	Inventory        adminMigrationInventory
	Findings         []adminMigrationFinding
	PlannedMutations []adminMigrationPlannedMutation
	Bundle           *adminMigrationSourceNormalizeBundle
}

type adminMigrationPostgresLiveSourceExtractor struct {
	postgres  adminMigrationLiveSourcePostgresProbe
	bootstrap adminMigrationLiveSourceBootstrapExtractor
}

type adminMigrationLiveSourcePostgresProbe interface {
	Probe(context.Context, adminMigrationLiveSourceConfig) adminMigrationLiveSourcePostgresProbeResult
}

type adminMigrationLiveSourcePostgresProbeResult struct {
	Dependencies []adminMigrationDependency
	Inventory    adminMigrationInventory
	Findings     []adminMigrationFinding
}

type adminMigrationPGXLiveSourcePostgresProbe struct{}

type adminMigrationLiveSourceBootstrapExtractor interface {
	ExtractBootstrap(context.Context, adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult
}

// newAdminMigrationPendingLiveSourceExtractor now performs the implemented
// read-only source PostgreSQL probe while leaving blob, HTTP, and extraction
// work explicitly pending for later tasks.
func newAdminMigrationPendingLiveSourceExtractor(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
	return adminMigrationPostgresLiveSourceExtractor{
		postgres:  adminMigrationPGXLiveSourcePostgresProbe{},
		bootstrap: adminMigrationPGXLiveSourceBootstrapExtractor{},
	}
}

// newAdminMigrationLiveSourceExtractorWithPostgresProbe lets tests exercise the
// real preflight envelope without requiring a live Chef PostgreSQL source.
func newAdminMigrationLiveSourceExtractorWithPostgresProbe(probe adminMigrationLiveSourcePostgresProbe) func(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
	return func(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
		return adminMigrationPostgresLiveSourceExtractor{
			postgres:  probe,
			bootstrap: adminMigrationPGXLiveSourceBootstrapExtractor{},
		}
	}
}

// newAdminMigrationLiveSourceExtractorWithPostgresAdapters wires deterministic
// tests into the real preflight/extract envelope without real source providers.
func newAdminMigrationLiveSourceExtractorWithPostgresAdapters(probe adminMigrationLiveSourcePostgresProbe, bootstrap adminMigrationLiveSourceBootstrapExtractor) func(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
	return func(adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractor {
		return adminMigrationPostgresLiveSourceExtractor{
			postgres:  probe,
			bootstrap: bootstrap,
		}
	}
}

// Preflight probes source PostgreSQL through a read-only transaction and
// reports the remaining blob/HTTP/extract work as non-mutating future checks.
func (e adminMigrationPostgresLiveSourceExtractor) Preflight(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	result := adminMigrationLiveSourceExtractorResult{
		PlannedMutations: []adminMigrationPlannedMutation{
			{
				Action:  "live_source_extract_write_bundle",
				Family:  "source_manifest",
				Message: "source live extract will write a local normalized source bundle; preflight does not write source Chef or target OpenCook state",
			},
			{
				Action:  "source_read_only_probe",
				Family:  "live_source",
				Message: "live-source probes use read-only source checks and never proxy source writes",
			},
		},
	}
	if finding, ok := adminMigrationLiveSourceContextFinding(ctx); ok {
		result.Dependencies = adminMigrationLiveSourceDependencies(cfg)
		result.Findings = append(result.Findings, finding)
		return result
	}
	if e.postgres == nil {
		e.postgres = adminMigrationPGXLiveSourcePostgresProbe{}
	}
	probed := e.postgres.Probe(ctx, cfg)
	result.Dependencies = append(result.Dependencies, probed.Dependencies...)
	result.Dependencies = append(result.Dependencies, adminMigrationLiveSourceNonPostgresDependencies(cfg)...)
	result.Inventory = probed.Inventory
	result.Findings = append(result.Findings, probed.Findings...)
	return result
}

// Extract preserves the documented command shape while making the missing real
// extractor explicit and retry-safe for operators.
func (e adminMigrationPostgresLiveSourceExtractor) Extract(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	if finding, ok := adminMigrationLiveSourceContextFinding(ctx); ok {
		return adminMigrationLiveSourceExtractorResult{
			Dependencies: adminMigrationLiveSourceDependencies(cfg),
			Findings:     []adminMigrationFinding{finding},
		}
	}
	if e.bootstrap == nil {
		e.bootstrap = adminMigrationPGXLiveSourceBootstrapExtractor{}
	}
	return e.bootstrap.ExtractBootstrap(ctx, cfg)
}

// Probe validates the live Chef source PostgreSQL handle using read-only SQL
// only. It deliberately returns classified migration findings instead of raw
// driver errors so credentials and backend internals stay out of CLI output.
func (adminMigrationPGXLiveSourcePostgresProbe) Probe(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourcePostgresProbeResult {
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := adminMigrationLiveSourceDerivePostgresTargets(cfg)
	if err != nil {
		return adminMigrationLiveSourcePostgresProbeFailure(
			"source PostgreSQL cluster seed DSN is not a valid connection string",
			adminMigrationFindingSourcePostgresUnavailable,
			"source_postgres_seed",
			"source PostgreSQL cluster seed connection string could not be parsed",
		)
	}
	erchef := adminMigrationLiveSourceProbeDatabase(ctx, targets.Erchef, "erchef", adminMigrationLiveSourceRequiredErchefTables(), true)
	bifrost := adminMigrationLiveSourceProbeDatabase(ctx, targets.Bifrost, "bifrost", adminMigrationLiveSourceRequiredBifrostTables(), false)
	result := adminMigrationLiveSourcePostgresProbeResult{
		Dependencies: append(append([]adminMigrationDependency{}, erchef.Dependencies...), bifrost.Dependencies...),
		Findings:     append(append([]adminMigrationFinding{}, erchef.Findings...), bifrost.Findings...),
		Inventory: adminMigrationInventory{Families: []adminMigrationInventoryFamily{
			{Family: "source_organizations", Count: len(erchef.Organizations)},
			{Family: "source_erchef_required_tables", Count: len(adminMigrationLiveSourceRequiredErchefTables())},
			{Family: "source_bifrost_required_tables", Count: len(adminMigrationLiveSourceRequiredBifrostTables())},
		}},
	}
	result.Findings = append(result.Findings, adminMigrationLiveSourceCrossDatabaseConsistencyFinding())
	if erchef.Ready && len(erchef.Organizations) == 0 {
		result.Findings = append(result.Findings, adminMigrationFinding{Severity: "error", Code: adminMigrationFindingSourceErchefSchemaUnsupported, Family: "source_erchef_schema", Message: "source Erchef PostgreSQL did not expose any organizations to the configured read-only role"})
	}
	if erchef.Ready && cfg.Organization != "" && !adminMigrationLiveSourceIncludesOrganization(erchef.Organizations, cfg.Organization) {
		result.Findings = append(result.Findings, adminMigrationFinding{Severity: "error", Code: adminMigrationFindingSourceErchefSchemaUnsupported, Family: "source_organizations", Message: "requested source organization is not visible to the configured read-only role"})
	}
	return result
}

// adminMigrationLiveSourcePostgresReadOnlyEnabled accepts PostgreSQL's common
// textual booleans so the probe remains stable across server versions.
func adminMigrationLiveSourcePostgresReadOnlyEnabled(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "on", "true", "t", "1", "yes":
		return true
	default:
		return false
	}
}

// adminMigrationLiveSourceReadOrganizations enumerates only organization names;
// later extraction tasks can read full rows after this capability is proven.
func adminMigrationLiveSourceReadOrganizations(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, "SELECT name FROM orgs WHERE name <> '' ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	organizations := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		organizations = append(organizations, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return organizations, nil
}

// adminMigrationLiveSourceIncludesOrganization keeps org filtering in the
// preflight layer so a typo fails before later extraction writes local bundles.
func adminMigrationLiveSourceIncludesOrganization(organizations []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, org := range organizations {
		if org == want {
			return true
		}
	}
	return false
}

// adminMigrationLiveSourceRequiredPostgresTables is the conservative schema
// surface needed by the implemented normalized-source families.
func adminMigrationLiveSourceRequiredPostgresTables() []string {
	return append(append([]string{}, adminMigrationLiveSourceRequiredErchefTables()...), adminMigrationLiveSourceRequiredBifrostTables()...)
}

// adminMigrationLiveSourcePostgresProbeFailure returns one blocking dependency
// and one stable finding without echoing the underlying SQL or driver error.
func adminMigrationLiveSourcePostgresProbeFailure(depMessage, code, family, findingMessage string) adminMigrationLiveSourcePostgresProbeResult {
	return adminMigrationLiveSourcePostgresProbeResult{
		Dependencies: []adminMigrationDependency{
			{
				Name:       "source_postgres",
				Status:     "error",
				Backend:    "postgres",
				Configured: true,
				Message:    depMessage,
				Details: map[string]string{
					"read_only_required": "true",
				},
			},
		},
		Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     code,
			Family:   family,
			Message:  findingMessage,
		}},
	}
}

// adminMigrationLiveSourceSafeDetail trims metadata fields and replaces empty
// values so dependency details remain useful without becoming secret carriers.
func adminMigrationLiveSourceSafeDetail(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

// adminMigrationLiveSourceBoundedList gives operators enough schema context to
// act on while keeping JSON output compact for large unsupported layouts.
func adminMigrationLiveSourceBoundedList(values []string, limit int) string {
	if len(values) == 0 {
		return ""
	}
	if limit <= 0 || len(values) <= limit {
		return strings.Join(values, ",")
	}
	return strings.Join(values[:limit], ",") + fmt.Sprintf(",+%d more", len(values)-limit)
}

// adminMigrationApplyLiveSourceExtractorResult folds deterministic extractor
// output into the shared migration envelope without creating a storage abstraction.
func adminMigrationApplyLiveSourceExtractorResult(out *adminMigrationCLIOutput, result adminMigrationLiveSourceExtractorResult) {
	out.Dependencies = append(out.Dependencies, result.Dependencies...)
	if result.Inventory.Families != nil {
		out.Inventory = result.Inventory
	}
	out.Findings = append(out.Findings, result.Findings...)
	out.PlannedMutations = append(out.PlannedMutations, result.PlannedMutations...)
	adminMigrationCollectOutputStatuses(out)
}

// adminMigrationLiveSourceContextFinding maps cancellation to a stable finding
// code so partial extraction can be retried without ambiguous provider details.
func adminMigrationLiveSourceContextFinding(ctx context.Context) (adminMigrationFinding, bool) {
	if ctx == nil || ctx.Err() == nil {
		return adminMigrationFinding{}, false
	}
	return adminMigrationFinding{
		Severity: "error",
		Code:     adminMigrationFindingSourceExtractionInterrupted,
		Family:   "live_source",
		Message:  "live source extraction was interrupted before bundle publication",
	}, true
}

// adminMigrationLiveSourceFindingCodes freezes live-source extractor findings
// before concrete source PostgreSQL, blob, and HTTP implementations exist.
func adminMigrationLiveSourceFindingCodes() []adminMigrationValidationFindingCode {
	return []adminMigrationValidationFindingCode{
		{Code: adminMigrationFindingSourcePostgresUnavailable, Severity: "error", Family: "source_postgres", Message: "source PostgreSQL could not be reached or queried"},
		{Code: adminMigrationFindingSourceSchemaUnsupported, Severity: "error", Family: "source_schema", Message: "source Chef Server schema is unsupported"},
		{Code: adminMigrationFindingSourceErchefUnavailable, Severity: "error", Family: "source_erchef_postgres", Message: "source Erchef PostgreSQL could not be reached or queried"},
		{Code: adminMigrationFindingSourceErchefSchemaUnsupported, Severity: "error", Family: "source_erchef_schema", Message: "source Erchef schema is unsupported"},
		{Code: adminMigrationFindingSourceBifrostUnavailable, Severity: "error", Family: "source_bifrost_postgres", Message: "source Bifrost PostgreSQL could not be reached or queried"},
		{Code: adminMigrationFindingSourceBifrostSchemaUnsupported, Severity: "error", Family: "source_bifrost_schema", Message: "source Bifrost schema is unsupported"},
		{Code: adminMigrationFindingSourceAuthorizationTargetUnresolved, Severity: "error", Family: "source_authorization", Message: "an Erchef authorization target could not be resolved in Bifrost"},
		{Code: adminMigrationFindingSourceAuthorizationSubjectUnresolved, Severity: "error", Family: "source_authorization", Message: "a Bifrost authorization subject or membership could not be resolved in Erchef"},
		{Code: adminMigrationFindingSourceCrossDatabaseConsistency, Severity: "warning", Family: "live_source", Message: "Erchef and Bifrost snapshots are independently consistent but not atomic across databases"},
		{Code: adminMigrationFindingSourceBifrostUnrelatedRecords, Severity: "warning", Family: "source_bifrost", Message: "Bifrost contains records outside the selected extraction scope"},
		{Code: adminMigrationFindingSourceFamilyUnsupported, Severity: "warning", Family: "source_family", Message: "source family is not supported by OpenCook migration"},
		{Code: adminMigrationFindingSourceBlobUnavailable, Severity: "error", Family: "source_blob", Message: "source blob provider is unavailable"},
		{Code: adminMigrationFindingSourceBlobMissing, Severity: "error", Family: "source_blob", Message: "source blob referenced by metadata is missing"},
		{Code: adminMigrationFindingSourceBlobChecksumMismatch, Severity: "error", Family: "source_blob", Message: "source blob content does not match its Chef checksum"},
		{Code: adminMigrationFindingSourceHTTPReadUnavailable, Severity: "warning", Family: "source_http", Message: "optional source Chef HTTP read probe is unavailable"},
		{Code: adminMigrationFindingSourceExtractionInterrupted, Severity: "error", Family: "live_source", Message: "live source extraction was interrupted before bundle publication"},
	}
}

// adminMigrationLiveSourceOutputDetails summarizes an emitted normalized bundle
// without inspecting private payload bytes.
func adminMigrationLiveSourceOutputDetails(bundle adminMigrationSourceNormalizeBundle) map[string]string {
	return map[string]string{
		"format_version": bundle.Manifest.FormatVersion,
		"source_type":    bundle.Manifest.SourceType,
		"payloads":       fmt.Sprintf("%d", len(bundle.Manifest.Payloads)),
		"artifacts":      fmt.Sprintf("%d", len(bundle.Manifest.Artifacts)),
		"files":          fmt.Sprintf("%d", len(bundle.Files)+1),
	}
}
