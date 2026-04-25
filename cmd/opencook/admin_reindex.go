package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

type stringListFlag []string

func (f *stringListFlag) String() string {
	if f == nil {
		return ""
	}
	return strings.Join(*f, ",")
}

func (f *stringListFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func (c *command) runAdminReindex(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminReindexUsage(c.stdout)
		return exitOK
	}

	fs := flag.NewFlagSet("opencook admin reindex", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	orgName := fs.String("org", "", "organization to reindex")
	allOrgs := fs.Bool("all-orgs", false, "reindex all organizations")
	indexName := fs.String("index", "", "limit reindex to a built-in or data bag index")
	var names stringListFlag
	fs.Var(&names, "name", "limit reindex to a named item; may be repeated")
	complete := fs.Bool("complete", false, "drop then reindex the target scope")
	drop := fs.Bool("drop", false, "drop derived OpenSearch documents for the target scope")
	noDrop := fs.Bool("no-drop", false, "upsert documents without dropping existing documents")
	dryRun := fs.Bool("dry-run", false, "load and count PostgreSQL-backed documents without mutating OpenSearch")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	postgresDSN := fs.String("postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
	openSearchURL := fs.String("opensearch-url", "", "OpenSearch URL; defaults to OPENCOOK_OPENSEARCH_URL")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin reindex", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin reindex received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput // Admin commands currently emit JSON as their stable output mode.

	mode, ok := adminReindexMode(*complete, *drop, *noDrop)
	if !ok {
		return c.adminUsageError("admin reindex accepts only one of --complete, --drop, or --no-drop\n\n")
	}

	plan := search.ReindexPlan{
		Mode:             mode,
		AllOrganizations: *allOrgs,
		Organization:     *orgName,
		Index:            *indexName,
		Names:            []string(names),
		DryRun:           *dryRun,
	}
	if code, ok := c.validateAdminReindexPlan(plan); !ok {
		return code
	}

	cfg, err := c.loadOffline()
	if err != nil {
		fmt.Fprintf(c.stderr, "load reindex config: %v\n", err)
		return exitDependencyUnavailable
	}
	if strings.TrimSpace(*postgresDSN) == "" {
		*postgresDSN = cfg.PostgresDSN
	}
	if strings.TrimSpace(*openSearchURL) == "" {
		*openSearchURL = cfg.OpenSearchURL
	}
	if strings.TrimSpace(*postgresDSN) == "" {
		fmt.Fprintln(c.stderr, "reindex requires PostgreSQL configuration via --postgres-dsn or OPENCOOK_POSTGRES_DSN")
		return exitDependencyUnavailable
	}
	if !plan.DryRun && strings.TrimSpace(*openSearchURL) == "" {
		fmt.Fprintln(c.stderr, "active reindex requires OpenSearch configuration via --opensearch-url or OPENCOOK_OPENSEARCH_URL")
		return exitDependencyUnavailable
	}

	store, closeStore, err := c.newOfflineStore(ctx, *postgresDSN)
	if err != nil {
		fmt.Fprintf(c.stderr, "open reindex store: %v\n", err)
		return exitDependencyUnavailable
	}
	defer closeOfflineStore(closeStore)

	state, code, ok := c.loadReindexBootstrapState(store, cfg)
	if !ok {
		return code
	}

	target := search.ReindexTarget(noopReindexTarget{})
	if !plan.DryRun {
		target, err = c.newReindexTarget(*openSearchURL)
		if err != nil {
			fmt.Fprintf(c.stderr, "open reindex target: %s\n", adminReindexFailureMessage(err))
			return exitDependencyUnavailable
		}
	}

	result, err := search.NewReindexService(state, target).Run(ctx, plan)
	output := adminReindexOutput(result, err, adminReindexWarnings(plan), *withTiming)
	if writeErr := writePrettyJSON(c.stdout, output); writeErr != nil {
		fmt.Fprintf(c.stderr, "write reindex output: %v\n", writeErr)
		return exitDependencyUnavailable
	}
	if err != nil {
		fmt.Fprintf(c.stderr, "reindex failed: %s\n", strings.Join(adminReindexErrorMessages(result, err), "; "))
		return adminReindexExitCode(err, result)
	}
	return exitOK
}

func (c *command) validateAdminReindexPlan(plan search.ReindexPlan) (int, bool) {
	if plan.AllOrganizations && strings.TrimSpace(plan.Organization) != "" {
		return c.adminUsageError("admin reindex cannot combine --all-orgs with --org\n\n"), false
	}
	if !plan.AllOrganizations && strings.TrimSpace(plan.Organization) == "" {
		return c.adminUsageError("admin reindex requires --org ORG or --all-orgs\n\n"), false
	}
	if plan.AllOrganizations && strings.TrimSpace(plan.Index) != "" {
		return c.adminUsageError("admin reindex --index requires --org ORG\n\n"), false
	}
	if len(plan.Names) > 0 && (plan.AllOrganizations || strings.TrimSpace(plan.Organization) == "" || strings.TrimSpace(plan.Index) == "") {
		return c.adminUsageError("admin reindex --name requires --org ORG and --index INDEX\n\n"), false
	}
	return exitOK, true
}

func adminReindexMode(complete, drop, noDrop bool) (search.ReindexMode, bool) {
	count := 0
	for _, selected := range []bool{complete, drop, noDrop} {
		if selected {
			count++
		}
	}
	if count > 1 {
		return "", false
	}
	switch {
	case drop:
		return search.ReindexModeDrop, true
	case noDrop:
		return search.ReindexModeReindex, true
	default:
		return search.ReindexModeComplete, true
	}
}

func (c *command) loadReindexBootstrapState(store adminOfflineStore, cfg config.Config) (*bootstrap.Service, int, bool) {
	core, err := store.LoadBootstrapCore()
	if err != nil {
		fmt.Fprintf(c.stderr, "load reindex bootstrap core: %v\n", err)
		return nil, exitDependencyUnavailable, false
	}
	objects, err := store.LoadCoreObjects()
	if err != nil {
		fmt.Fprintf(c.stderr, "load reindex core objects: %v\n", err)
		return nil, exitDependencyUnavailable, false
	}
	state := bootstrap.NewService(nil, bootstrap.Options{
		SuperuserName:             adminReindexSuperuserName(cfg),
		InitialBootstrapCoreState: &core,
		InitialCoreObjectState:    &objects,
	})
	return state, exitOK, true
}

func adminReindexSuperuserName(cfg config.Config) string {
	if strings.TrimSpace(cfg.BootstrapRequestorType) == "" || strings.TrimSpace(cfg.BootstrapRequestorType) == "user" {
		if name := strings.TrimSpace(cfg.BootstrapRequestorName); name != "" {
			return name
		}
	}
	return "pivotal"
}

type noopReindexTarget struct{}

func (noopReindexTarget) Ping(context.Context) error {
	return nil
}

func (noopReindexTarget) EnsureChefIndex(context.Context) error {
	return nil
}

func (noopReindexTarget) DeleteByQuery(context.Context, string, string) error {
	return nil
}

func (noopReindexTarget) DeleteDocument(context.Context, string) error {
	return nil
}

func (noopReindexTarget) BulkUpsert(context.Context, []search.Document) error {
	return nil
}

func (noopReindexTarget) Refresh(context.Context) error {
	return nil
}

type adminReindexCLIOutput struct {
	OK         bool                 `json:"ok"`
	Command    string               `json:"command"`
	Mode       search.ReindexMode   `json:"mode"`
	Target     adminReindexTarget   `json:"target"`
	DryRun     bool                 `json:"dry_run,omitempty"`
	Counts     search.ReindexCounts `json:"counts"`
	Warnings   []string             `json:"warnings,omitempty"`
	Errors     []adminCLIError      `json:"errors,omitempty"`
	Duration   string               `json:"duration,omitempty"`
	DurationMS *int64               `json:"duration_ms,omitempty"`
}

type adminReindexTarget struct {
	AllOrganizations bool     `json:"all_organizations"`
	Organization     string   `json:"organization,omitempty"`
	Index            string   `json:"index,omitempty"`
	Names            []string `json:"names,omitempty"`
}

type adminCLIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func adminReindexOutput(result search.ReindexResult, err error, warnings []string, withTiming bool) adminReindexCLIOutput {
	out := adminReindexCLIOutput{
		OK:      err == nil,
		Command: "reindex",
		Mode:    result.Mode,
		Target: adminReindexTarget{
			AllOrganizations: result.AllOrganizations,
			Organization:     result.Organization,
			Index:            result.Index,
			Names:            result.Names,
		},
		DryRun:   result.DryRun,
		Counts:   result.Counts,
		Warnings: warnings,
	}
	if withTiming {
		durationMS := result.Duration.Milliseconds()
		out.Duration = result.Duration.String()
		out.DurationMS = &durationMS
	}
	if err != nil {
		for _, message := range adminReindexErrorMessages(result, err) {
			out.Errors = append(out.Errors, adminCLIError{
				Code:    adminReindexErrorCode(err),
				Message: message,
			})
		}
	}
	return out
}

func adminReindexWarnings(plan search.ReindexPlan) []string {
	if plan.DryRun {
		return nil
	}
	if plan.Mode == search.ReindexModeDrop || plan.Mode == search.ReindexModeComplete {
		return []string{"drop-and-reindex can race with concurrent Chef object writes until a future maintenance gate exists"}
	}
	return nil
}

func adminReindexErrorMessages(result search.ReindexResult, err error) []string {
	if len(result.Failures) > 0 {
		return append([]string(nil), result.Failures...)
	}
	return []string{adminReindexFailureMessage(err)}
}

func adminReindexFailureMessage(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, search.ErrOrganizationNotFound):
		return search.ErrOrganizationNotFound.Error()
	case errors.Is(err, search.ErrIndexNotFound):
		return search.ErrIndexNotFound.Error()
	case errors.Is(err, search.ErrInvalidConfiguration):
		return search.ErrInvalidConfiguration.Error()
	case errors.Is(err, search.ErrUnavailable):
		return search.ErrUnavailable.Error()
	case errors.Is(err, search.ErrRejected):
		return search.ErrRejected.Error()
	default:
		return "reindex failed"
	}
}

func adminReindexErrorCode(err error) string {
	switch {
	case errors.Is(err, search.ErrOrganizationNotFound), errors.Is(err, search.ErrIndexNotFound):
		return "not_found"
	case errors.Is(err, search.ErrInvalidConfiguration):
		return "usage_error"
	case errors.Is(err, search.ErrUnavailable), errors.Is(err, search.ErrRejected):
		return "dependency_unavailable"
	default:
		return "reindex_failed"
	}
}

func adminReindexExitCode(err error, result search.ReindexResult) int {
	switch {
	case errors.Is(err, search.ErrOrganizationNotFound), errors.Is(err, search.ErrIndexNotFound):
		return exitNotFound
	case errors.Is(err, search.ErrInvalidConfiguration):
		return exitUsage
	case errors.Is(err, search.ErrUnavailable), errors.Is(err, search.ErrRejected):
		return exitDependencyUnavailable
	case result.Counts.Failed > 0:
		return exitPartial
	default:
		return exitDependencyUnavailable
	}
}

func (c *command) printAdminReindexUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin reindex --org ORG [--complete|--drop|--no-drop] [--index INDEX] [--name NAME ...] [--dry-run] [--with-timing] [--json]
  opencook admin reindex --all-orgs [--complete|--drop|--no-drop] [--dry-run] [--with-timing] [--json]

Rebuild derived OpenSearch search documents from PostgreSQL-backed OpenCook state.

Flags:
  --org ORG
  --all-orgs
  --index INDEX
  --name NAME
  --complete
  --drop
  --no-drop
  --dry-run
  --with-timing
  --json
  --postgres-dsn DSN
  --opensearch-url URL
`)
}
