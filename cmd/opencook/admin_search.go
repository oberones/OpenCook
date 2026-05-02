package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

func (c *command) runAdminSearch(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin search requires check or repair\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminSearchUsage(c.stdout)
		return exitOK
	}

	switch args[0] {
	case "check":
		return c.runAdminSearchConsistency(ctx, args[1:], false, inheritedJSON)
	case "repair":
		return c.runAdminSearchConsistency(ctx, args[1:], true, inheritedJSON)
	default:
		return c.adminUsageError("unknown admin search command %q\n\n", args[0])
	}
}

func (c *command) runAdminSearchConsistency(ctx context.Context, args []string, repair bool, inheritedJSON bool) int {
	name := "admin search check"
	if repair {
		name = "admin search repair"
	}
	fs := flag.NewFlagSet("opencook "+name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	orgName := fs.String("org", "", "organization to check or repair")
	allOrgs := fs.Bool("all-orgs", false, "check or repair all organizations")
	indexName := fs.String("index", "", "limit check or repair to a built-in or data bag index")
	dryRun := fs.Bool("dry-run", false, "report repair actions without mutating OpenSearch")
	yes := fs.Bool("yes", false, "confirm OpenSearch repair mutation")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	postgresDSN := fs.String("postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
	openSearchURL := fs.String("opensearch-url", "", "OpenSearch URL; defaults to OPENCOOK_OPENSEARCH_URL")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError(name, err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("%s received unexpected arguments: %v\n\n", name, fs.Args())
	}
	_ = *jsonOutput

	if !repair && (*dryRun || *yes) {
		return c.adminUsageError("admin search check does not accept --dry-run or --yes\n\n")
	}
	if *allOrgs && strings.TrimSpace(*orgName) != "" {
		return c.adminUsageError("%s cannot combine --all-orgs with --org\n\n", name)
	}
	if repair && !*dryRun && !*yes {
		return c.adminUsageError("admin search repair requires --dry-run or --yes\n\n")
	}

	plan := search.ConsistencyPlan{
		AllOrganizations: *allOrgs || strings.TrimSpace(*orgName) == "",
		Organization:     *orgName,
		Index:            *indexName,
		Repair:           repair,
		DryRun:           *dryRun,
	}
	cfg, resolvedPostgresDSN, resolvedOpenSearchURL, code, ok := c.loadAdminSearchConsistencyConfig(*postgresDSN, *openSearchURL)
	if !ok {
		return code
	}

	var maintenanceWarnings []string
	if repair && !*dryRun {
		var err error
		maintenanceWarnings, err = c.requireAdminWorkflowMaintenance(ctx, resolvedPostgresDSN, "admin search repair")
		if err != nil {
			result := adminSearchResultFromPlan(plan)
			result.Counts.Failed = 1
			output := adminSearchOutput(result, err, nil, *withTiming)
			if writeErr := writePrettyJSON(c.stdout, output); writeErr != nil {
				fmt.Fprintf(c.stderr, "write search output: %v\n", writeErr)
				return exitDependencyUnavailable
			}
			fmt.Fprintf(c.stderr, "search consistency failed: %s\n", adminReindexFailureMessage(err))
			return adminReindexExitCode(err, search.ReindexResult{})
		}
	}

	_, state, target, code, ok := c.openAdminSearchConsistencyWithConfig(ctx, cfg, resolvedPostgresDSN, resolvedOpenSearchURL)
	if !ok {
		return code
	}

	result, err := search.NewConsistencyService(state, target).Run(ctx, plan)
	output := adminSearchOutput(result, err, appendUniqueAdminWarnings(adminSearchWarnings(plan), maintenanceWarnings...), *withTiming)
	if writeErr := writePrettyJSON(c.stdout, output); writeErr != nil {
		fmt.Fprintf(c.stderr, "write search output: %v\n", writeErr)
		return exitDependencyUnavailable
	}
	if err != nil {
		fmt.Fprintf(c.stderr, "search consistency failed: %s\n", strings.Join(adminSearchErrorMessages(result, err), "; "))
		return adminReindexExitCode(err, search.ReindexResult{})
	}
	if adminSearchHasDrift(result) && (!repair || *dryRun) {
		return exitPartial
	}
	return exitOK
}

// loadAdminSearchConsistencyConfig resolves command flags against environment
// config once so maintenance checks and OpenSearch work use the same targets.
func (c *command) loadAdminSearchConsistencyConfig(postgresDSN, openSearchURL string) (config.Config, string, string, int, bool) {
	cfg, err := c.loadOffline()
	if err != nil {
		fmt.Fprintf(c.stderr, "load search config: %v\n", err)
		return config.Config{}, "", "", exitDependencyUnavailable, false
	}
	if strings.TrimSpace(postgresDSN) == "" {
		postgresDSN = cfg.PostgresDSN
	}
	if strings.TrimSpace(openSearchURL) == "" {
		openSearchURL = cfg.OpenSearchURL
	}
	if strings.TrimSpace(postgresDSN) == "" {
		fmt.Fprintln(c.stderr, "search consistency requires PostgreSQL configuration via --postgres-dsn or OPENCOOK_POSTGRES_DSN")
		return cfg, "", "", exitDependencyUnavailable, false
	}
	if strings.TrimSpace(openSearchURL) == "" {
		fmt.Fprintln(c.stderr, "search consistency requires OpenSearch configuration via --opensearch-url or OPENCOOK_OPENSEARCH_URL")
		return cfg, postgresDSN, "", exitDependencyUnavailable, false
	}
	return cfg, postgresDSN, openSearchURL, exitOK, true
}

func (c *command) openAdminSearchConsistency(ctx context.Context, postgresDSN, openSearchURL string) (config.Config, *bootstrap.Service, search.ConsistencyTarget, int, bool) {
	cfg, postgresDSN, openSearchURL, code, ok := c.loadAdminSearchConsistencyConfig(postgresDSN, openSearchURL)
	if !ok {
		return cfg, nil, nil, code, false
	}
	return c.openAdminSearchConsistencyWithConfig(ctx, cfg, postgresDSN, openSearchURL)
}

// openAdminSearchConsistencyWithConfig opens the offline PostgreSQL snapshot
// and OpenSearch target after caller-level safety gates have already passed.
func (c *command) openAdminSearchConsistencyWithConfig(ctx context.Context, cfg config.Config, postgresDSN, openSearchURL string) (config.Config, *bootstrap.Service, search.ConsistencyTarget, int, bool) {
	store, closeStore, err := c.newOfflineStore(ctx, postgresDSN)
	if err != nil {
		fmt.Fprintf(c.stderr, "open search consistency store: %v\n", err)
		return cfg, nil, nil, exitDependencyUnavailable, false
	}
	defer closeOfflineStore(closeStore)

	state, code, ok := c.loadReindexBootstrapState(store, cfg)
	if !ok {
		return cfg, nil, nil, code, false
	}
	target, err := c.newSearchTarget(openSearchURL)
	if err != nil {
		fmt.Fprintf(c.stderr, "open search consistency target: %s\n", adminReindexFailureMessage(err))
		return cfg, nil, nil, exitDependencyUnavailable, false
	}
	return cfg, state, target, exitOK, true
}

type adminSearchCLIOutput struct {
	OK                bool                            `json:"ok"`
	Command           string                          `json:"command"`
	Target            adminSearchTarget               `json:"target"`
	DryRun            bool                            `json:"dry_run,omitempty"`
	Counts            search.ConsistencyCounts        `json:"counts"`
	ObjectCounts      []search.ConsistencyObjectCount `json:"object_counts,omitempty"`
	MissingDocuments  []string                        `json:"missing_documents,omitempty"`
	StaleDocuments    []string                        `json:"stale_documents,omitempty"`
	UnsupportedScopes []string                        `json:"unsupported_scopes,omitempty"`
	Warnings          []string                        `json:"warnings,omitempty"`
	Errors            []adminCLIError                 `json:"errors,omitempty"`
	Duration          string                          `json:"duration,omitempty"`
	DurationMS        *int64                          `json:"duration_ms,omitempty"`
}

type adminSearchTarget struct {
	AllOrganizations bool   `json:"all_organizations"`
	Organization     string `json:"organization,omitempty"`
	Index            string `json:"index,omitempty"`
}

func adminSearchOutput(result search.ConsistencyResult, err error, warnings []string, withTiming bool) adminSearchCLIOutput {
	command := "search_check"
	if result.Repair {
		command = "search_repair"
	}
	out := adminSearchCLIOutput{
		OK:      err == nil,
		Command: command,
		Target: adminSearchTarget{
			AllOrganizations: result.AllOrganizations,
			Organization:     result.Organization,
			Index:            result.Index,
		},
		DryRun:            result.DryRun,
		Counts:            result.Counts,
		ObjectCounts:      result.ObjectCounts,
		MissingDocuments:  result.MissingDocuments,
		StaleDocuments:    result.StaleDocuments,
		UnsupportedScopes: result.UnsupportedScopes,
		Warnings:          warnings,
	}
	if withTiming {
		durationMS := result.Duration.Milliseconds()
		out.Duration = result.Duration.String()
		out.DurationMS = &durationMS
	}
	if err != nil {
		for _, message := range adminSearchErrorMessages(result, err) {
			out.Errors = append(out.Errors, adminCLIError{
				Code:    adminReindexErrorCode(err),
				Message: message,
			})
		}
	}
	return out
}

// adminSearchResultFromPlan builds an output envelope for safety-gate failures
// detected before PostgreSQL state or OpenSearch consistency checks run.
func adminSearchResultFromPlan(plan search.ConsistencyPlan) search.ConsistencyResult {
	return search.ConsistencyResult{
		AllOrganizations: plan.AllOrganizations,
		Organization:     plan.Organization,
		Index:            plan.Index,
		Repair:           plan.Repair,
		DryRun:           plan.DryRun,
	}
}

func adminSearchWarnings(plan search.ConsistencyPlan) []string {
	if !plan.Repair || plan.DryRun {
		return nil
	}
	return []string{"search repair mutates derived OpenSearch documents; keep maintenance mode active until repair and follow-up search checks finish"}
}

func adminSearchErrorMessages(result search.ConsistencyResult, err error) []string {
	if len(result.Failures) > 0 {
		return append([]string(nil), result.Failures...)
	}
	return []string{adminReindexFailureMessage(err)}
}

func adminSearchHasDrift(result search.ConsistencyResult) bool {
	return result.Counts.Missing > 0 || result.Counts.Stale > 0 || result.Counts.Unsupported > 0
}

func (c *command) printAdminSearchUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin search check [--org ORG|--all-orgs] [--index INDEX] [--with-timing] [--json]
  opencook admin search repair [--org ORG|--all-orgs] [--index INDEX] [--dry-run|--yes] [--with-timing] [--json]

Compare PostgreSQL-derived search documents with OpenSearch-visible document IDs and optionally repair drift.

Flags:
  --org ORG
  --all-orgs
  --index INDEX
  --dry-run
  --yes
  --with-timing
  --json
  --postgres-dsn DSN
  --opensearch-url URL
`)
}
