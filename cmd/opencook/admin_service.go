package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
)

type adminServiceOutput struct {
	OK         bool                `json:"ok"`
	Command    string              `json:"command"`
	Offline    bool                `json:"offline,omitempty"`
	Config     map[string]string   `json:"config,omitempty"`
	Summary    adminServiceSummary `json:"summary"`
	Checks     []adminConfigCheck  `json:"checks"`
	Warnings   []string            `json:"warnings,omitempty"`
	Errors     []adminCLIError     `json:"errors,omitempty"`
	Duration   string              `json:"duration,omitempty"`
	DurationMS *int64              `json:"duration_ms,omitempty"`
}

type adminServiceSummary struct {
	ServiceName         string `json:"service_name"`
	Environment         string `json:"environment"`
	ListenAddress       string `json:"listen_address"`
	DefaultOrganization string `json:"default_organization,omitempty"`
	Persistence         string `json:"persistence"`
	Search              string `json:"search"`
	Blob                string `json:"blob"`
}

// runAdminService dispatches service-oriented diagnostics that describe the
// local OpenCook process model rather than calling Chef-facing HTTP routes.
func (c *command) runAdminService(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin service requires status or doctor\n\n")
	}

	switch args[0] {
	case "status":
		return c.runAdminServiceStatus(ctx, args[1:], inheritedJSON)
	case "doctor":
		return c.runAdminServiceDoctor(ctx, args[1:], inheritedJSON)
	case "help", "-h", "--help":
		c.printAdminServiceUsage(c.stdout)
		return exitOK
	default:
		return c.adminUsageError("unknown admin service command %q\n\n", args[0])
	}
}

// runAdminServiceStatus performs a fast local status pass using static config
// and adapter construction only; it never opens PostgreSQL or pings OpenSearch.
func (c *command) runAdminServiceStatus(_ context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin service status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin service status", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin service status received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput

	out, exitCode := c.buildAdminServiceStatusOutput()
	return c.writeAdminServiceResult(out, *withTiming, start, exitCode)
}

// runAdminServiceDoctor adds non-mutating reachability checks on top of the
// static service summary; direct PostgreSQL inspection requires --offline.
func (c *command) runAdminServiceDoctor(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin service doctor", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	offline := fs.Bool("offline", false, "allow direct PostgreSQL inspection while OpenCook servers are stopped")
	jsonOutput := fs.Bool("json", inheritedJSON, "print JSON output")
	withTiming := fs.Bool("with-timing", false, "include duration_ms in output")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin service doctor", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin service doctor received unexpected arguments: %v\n\n", fs.Args())
	}
	_ = *jsonOutput

	out, exitCode := c.buildAdminServiceDoctorOutput(ctx, *offline)
	return c.writeAdminServiceResult(out, *withTiming, start, exitCode)
}

// buildAdminServiceStatusOutput builds the shared JSON envelope for static
// status checks and returns the exit code implied by hard validation errors.
func (c *command) buildAdminServiceStatusOutput() (adminServiceOutput, int) {
	out := adminServiceOutput{
		OK:      true,
		Command: "service_status",
		Checks:  []adminConfigCheck{},
	}
	cfg, err := c.loadOffline()
	if err != nil {
		out.OK = false
		out.Checks = append(out.Checks, adminConfigCheck{
			Name:       "runtime_config",
			Status:     "error",
			Message:    "OPENCOOK_* configuration could not be loaded",
			Configured: false,
		})
		out.Errors = append(out.Errors, adminCLIError{Code: "config_load_failed", Message: err.Error()})
		return out, exitDependencyUnavailable
	}

	out.Config = cfg.Redacted()
	out.Summary = c.adminServiceSummary(cfg)
	acc := c.adminServiceStaticChecks(cfg)
	out.Checks = acc.checks
	out.Warnings = acc.warnings
	out.Errors = acc.errors
	out.OK = len(out.Errors) == 0
	if !out.OK {
		return out, exitDependencyUnavailable
	}
	return out, exitOK
}

// buildAdminServiceDoctorOutput runs static checks plus optional provider
// reachability checks that are safe to execute without mutating runtime state.
func (c *command) buildAdminServiceDoctorOutput(ctx context.Context, offline bool) (adminServiceOutput, int) {
	out, _ := c.buildAdminServiceStatusOutput()
	out.Command = "service_doctor"
	out.Offline = offline
	if !out.OK {
		return out, exitDependencyUnavailable
	}

	cfg, err := c.loadOffline()
	if err != nil {
		out.OK = false
		out.Errors = append(out.Errors, adminCLIError{Code: "config_load_failed", Message: err.Error()})
		return out, exitDependencyUnavailable
	}

	acc := adminConfigCheckAccumulator{checks: out.Checks, warnings: out.Warnings, errors: out.Errors}
	acc.add(c.adminServicePostgresStateCheck(ctx, cfg, offline))
	acc.add(c.adminServiceOpenSearchPingCheck(ctx, cfg))
	acc.add(c.adminServiceBlobInventoryCheck(ctx, cfg))
	out.Checks = acc.checks
	out.Warnings = acc.warnings
	out.Errors = acc.errors
	out.OK = len(out.Errors) == 0
	if !out.OK {
		return out, exitDependencyUnavailable
	}
	return out, exitOK
}

// adminServiceStaticChecks reuses config preflight checks but keeps the service
// command's policy of summarizing warnings without probing external providers.
func (c *command) adminServiceStaticChecks(cfg config.Config) adminConfigCheckAccumulator {
	acc := adminConfigCheckAccumulator{}
	acc.add(adminConfigRuntimeCheck())
	acc.add(adminConfigServerCheck(cfg))
	acc.add(adminConfigPostgresCheck(cfg))
	acc.add(adminConfigOpenSearchCheck(cfg))
	acc.add(c.adminConfigBlobCheck(cfg))
	return acc
}

// adminServiceSummary turns redacted configuration and adapter status into a
// compact operator snapshot suitable for `chef-server-ctl status`-style output.
func (c *command) adminServiceSummary(cfg config.Config) adminServiceSummary {
	persistence := "memory"
	if strings.TrimSpace(cfg.PostgresDSN) != "" {
		persistence = "postgres-configured"
	}

	searchMode := "memory"
	if strings.TrimSpace(cfg.OpenSearchURL) != "" {
		searchMode = "opensearch-configured"
	}

	blobMode := "unavailable"
	if store, err := c.newBlobStore(cfg); err == nil && store != nil {
		blobMode = store.Status().Backend
	}

	return adminServiceSummary{
		ServiceName:         cfg.ServiceName,
		Environment:         cfg.Environment,
		ListenAddress:       cfg.ListenAddress,
		DefaultOrganization: cfg.DefaultOrganization,
		Persistence:         persistence,
		Search:              searchMode,
		Blob:                blobMode,
	}
}

// adminServicePostgresStateCheck loads persisted state only when --offline is
// present, making direct PostgreSQL access an explicit maintenance action.
func (c *command) adminServicePostgresStateCheck(ctx context.Context, cfg config.Config, offline bool) adminConfigCheck {
	if strings.TrimSpace(cfg.PostgresDSN) == "" {
		return adminConfigCheck{
			Name:       "postgres_state",
			Status:     "skipped",
			Message:    "PostgreSQL is not configured; direct persistence inspection skipped",
			Configured: false,
		}
	}
	if !offline {
		return adminConfigCheck{
			Name:       "postgres_state",
			Status:     "skipped",
			Message:    "direct PostgreSQL persistence inspection requires --offline",
			Configured: true,
		}
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return adminConfigCheck{
			Name:       "postgres_state",
			Status:     "error",
			Message:    "could not open PostgreSQL-backed state for offline inspection",
			Configured: true,
		}
	}
	defer closeOfflineStore(closeStore)

	bootstrapState, err := store.LoadBootstrapCore()
	if err != nil {
		return adminConfigCheck{Name: "postgres_state", Status: "error", Message: "could not load bootstrap core state", Configured: true}
	}
	coreState, err := store.LoadCoreObjects()
	if err != nil {
		return adminConfigCheck{Name: "postgres_state", Status: "error", Message: "could not load core object state", Configured: true}
	}
	return adminConfigCheck{
		Name:       "postgres_state",
		Status:     "ok",
		Message:    "PostgreSQL-backed bootstrap core and core object state loaded successfully",
		Configured: true,
		Details:    adminServicePostgresStateDetails(bootstrapState, coreState),
	}
}

// adminServiceOpenSearchPingCheck performs only the provider root ping; it does
// not create indexes, repair documents, or otherwise mutate OpenSearch state.
func (c *command) adminServiceOpenSearchPingCheck(ctx context.Context, cfg config.Config) adminConfigCheck {
	if strings.TrimSpace(cfg.OpenSearchURL) == "" {
		return adminConfigCheck{
			Name:       "opensearch_ping",
			Status:     "skipped",
			Message:    "OpenSearch is not configured; provider reachability check skipped",
			Configured: false,
		}
	}
	target, err := c.newSearchTarget(cfg.OpenSearchURL)
	if err != nil {
		return adminConfigCheck{
			Name:       "opensearch_ping",
			Status:     "error",
			Message:    "OpenSearch target configuration could not be opened",
			Configured: true,
		}
	}
	if err := target.Ping(ctx); err != nil {
		return adminConfigCheck{
			Name:       "opensearch_ping",
			Status:     "error",
			Message:    "OpenSearch provider ping failed",
			Configured: true,
		}
	}
	return adminConfigCheck{
		Name:       "opensearch_ping",
		Status:     "ok",
		Message:    "OpenSearch provider ping succeeded",
		Configured: true,
	}
}

// adminServiceBlobInventoryCheck reports safe adapter inventory information
// when the configured blob backend can list checksum keys without mutation.
func (c *command) adminServiceBlobInventoryCheck(ctx context.Context, cfg config.Config) adminConfigCheck {
	store, err := c.newBlobStore(cfg)
	if err != nil {
		return adminConfigCheck{
			Name:       "blob_inventory",
			Status:     "error",
			Message:    "blob adapter could not be constructed for inventory inspection",
			Configured: strings.TrimSpace(cfg.BlobBackend) != "" || strings.TrimSpace(cfg.BlobStorageURL) != "",
		}
	}
	lister, ok := store.(blob.Lister)
	if !ok {
		return adminConfigCheck{
			Name:       "blob_inventory",
			Status:     "skipped",
			Message:    "configured blob backend does not expose safe local listing",
			Configured: store.Status().Configured,
			Details: map[string]string{
				"backend": store.Status().Backend,
			},
		}
	}
	keys, err := lister.List(ctx)
	if err != nil {
		return adminConfigCheck{
			Name:       "blob_inventory",
			Status:     "error",
			Message:    "blob backend safe listing failed",
			Configured: store.Status().Configured,
			Details: map[string]string{
				"backend": store.Status().Backend,
			},
		}
	}
	return adminConfigCheck{
		Name:       "blob_inventory",
		Status:     "ok",
		Message:    "blob backend safe listing succeeded",
		Configured: store.Status().Configured,
		Details: map[string]string{
			"backend":      store.Status().Backend,
			"object_count": strconv.Itoa(len(keys)),
		},
	}
}

// adminServicePostgresStateDetails returns aggregate counts only, avoiding raw
// Chef object payloads, actor keys, ACL documents, and other sensitive data.
func adminServicePostgresStateDetails(bootstrapState bootstrap.BootstrapCoreState, coreState bootstrap.CoreObjectState) map[string]string {
	clientCount := 0
	clientKeyCount := 0
	groupCount := 0
	containerCount := 0
	bootstrapACLCount := len(bootstrapState.UserACLs)
	for _, org := range bootstrapState.Orgs {
		clientCount += len(org.Clients)
		clientKeyCount += nestedStringMapCount(org.ClientKeys)
		groupCount += len(org.Groups)
		containerCount += len(org.Containers)
		bootstrapACLCount += len(org.ACLs)
	}

	nodeCount := 0
	roleCount := 0
	environmentCount := 0
	dataBagCount := 0
	dataBagItemCount := 0
	sandboxCount := 0
	policyRevisionCount := 0
	policyGroupCount := 0
	coreACLCount := 0
	for _, org := range coreState.Orgs {
		nodeCount += len(org.Nodes)
		roleCount += len(org.Roles)
		environmentCount += len(org.Environments)
		dataBagCount += len(org.DataBags)
		dataBagItemCount += nestedDataBagItemCount(org.DataBagItems)
		sandboxCount += len(org.Sandboxes)
		policyRevisionCount += nestedPolicyRevisionCount(org.Policies)
		policyGroupCount += len(org.PolicyGroups)
		coreACLCount += len(org.ACLs)
	}

	return map[string]string{
		"users":            strconv.Itoa(len(bootstrapState.Users)),
		"user_keys":        strconv.Itoa(nestedStringMapCount(bootstrapState.UserKeys)),
		"organizations":    strconv.Itoa(len(bootstrapState.Orgs)),
		"clients":          strconv.Itoa(clientCount),
		"client_keys":      strconv.Itoa(clientKeyCount),
		"groups":           strconv.Itoa(groupCount),
		"containers":       strconv.Itoa(containerCount),
		"bootstrap_acls":   strconv.Itoa(bootstrapACLCount),
		"nodes":            strconv.Itoa(nodeCount),
		"roles":            strconv.Itoa(roleCount),
		"environments":     strconv.Itoa(environmentCount),
		"data_bags":        strconv.Itoa(dataBagCount),
		"data_bag_items":   strconv.Itoa(dataBagItemCount),
		"sandboxes":        strconv.Itoa(sandboxCount),
		"policy_revisions": strconv.Itoa(policyRevisionCount),
		"policy_groups":    strconv.Itoa(policyGroupCount),
		"core_object_acls": strconv.Itoa(coreACLCount),
		"core_object_orgs": strconv.Itoa(len(coreState.Orgs)),
	}
}

// nestedStringMapCount counts nested map values without exposing their names or
// payloads in service diagnostics.
func nestedStringMapCount[T any](items map[string]map[string]T) int {
	count := 0
	for _, nested := range items {
		count += len(nested)
	}
	return count
}

// nestedDataBagItemCount gives the generic counter a named wrapper so service
// diagnostics read in Chef object-family terms.
func nestedDataBagItemCount(items map[string]map[string]bootstrap.DataBagItem) int {
	return nestedStringMapCount(items)
}

// nestedPolicyRevisionCount gives policy revision inventory its own labeled
// counter while sharing the same non-sensitive aggregate logic.
func nestedPolicyRevisionCount(items map[string]map[string]bootstrap.PolicyRevision) int {
	return nestedStringMapCount(items)
}

// writeAdminServiceResult attaches optional timing data and writes the shared
// service JSON envelope for status and doctor subcommands.
func (c *command) writeAdminServiceResult(out adminServiceOutput, withTiming bool, start time.Time, exitCode int) int {
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write service output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// printAdminServiceUsage documents local service diagnostics separately from
// signed live API admin commands.
func (c *command) printAdminServiceUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin service status [--json] [--with-timing]
  opencook admin service doctor [--offline] [--json] [--with-timing]

Summarize local service configuration and run non-mutating diagnostics. Direct
PostgreSQL-backed state inspection is available only with service doctor --offline.
`)
}
