package main

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

const (
	adminMigrationScaffoldMessage   = "migration command scaffolding is ready; implementation will be filled in by the remaining migration tooling tasks"
	adminMigrationBlobProbeChecksum = "0000000000000000000000000000000000000000000000000000000000000000"
	adminMigrationSourceFormatV1    = "opencook.migration.source_inventory.v1"
)

type adminMigrationFlagValues struct {
	orgName            string
	allOrgs            bool
	jsonOutput         bool
	withTiming         bool
	dryRun             bool
	offline            bool
	yes                bool
	outputPath         string
	manifestPath       string
	serverURL          string
	postgresDSN        string
	openSearchURL      string
	blobBackend        string
	blobStorageURL     string
	blobS3Endpoint     string
	blobS3Region       string
	blobS3AccessKeyID  string
	blobS3SecretKey    string
	blobS3SessionToken string
	requestorName      string
	requestorType      string
	privateKeyPath     string
	defaultOrg         string
	serverAPIVersion   string
}

type adminMigrationCLIOutput struct {
	OK               bool                            `json:"ok"`
	Command          string                          `json:"command"`
	Target           adminMigrationTarget            `json:"target"`
	DryRun           bool                            `json:"dry_run,omitempty"`
	Offline          bool                            `json:"offline,omitempty"`
	Confirmed        bool                            `json:"confirmed,omitempty"`
	Config           map[string]string               `json:"config,omitempty"`
	Dependencies     []adminMigrationDependency      `json:"dependencies"`
	Inventory        adminMigrationInventory         `json:"inventory"`
	Findings         []adminMigrationFinding         `json:"findings"`
	PlannedMutations []adminMigrationPlannedMutation `json:"planned_mutations"`
	Warnings         []string                        `json:"warnings,omitempty"`
	Errors           []adminCLIError                 `json:"errors,omitempty"`
	Duration         string                          `json:"duration,omitempty"`
	DurationMS       *int64                          `json:"duration_ms,omitempty"`
}

type adminMigrationTarget struct {
	AllOrganizations bool   `json:"all_organizations,omitempty"`
	Organization     string `json:"organization,omitempty"`
	BundlePath       string `json:"bundle_path,omitempty"`
	OutputPath       string `json:"output_path,omitempty"`
	ManifestPath     string `json:"manifest_path,omitempty"`
	SourcePath       string `json:"source_path,omitempty"`
	ServerURL        string `json:"server_url,omitempty"`
}

type adminMigrationDependency struct {
	Name       string            `json:"name"`
	Status     string            `json:"status"`
	Backend    string            `json:"backend,omitempty"`
	Configured bool              `json:"configured,omitempty"`
	Message    string            `json:"message,omitempty"`
	Details    map[string]string `json:"details,omitempty"`
}

type adminMigrationInventory struct {
	Families []adminMigrationInventoryFamily `json:"families"`
}

type adminMigrationInventoryFamily struct {
	Organization string `json:"organization,omitempty"`
	Family       string `json:"family"`
	Count        int    `json:"count"`
}

type adminMigrationPostgresRead struct {
	Dependencies      []adminMigrationDependency
	Bootstrap         bootstrap.BootstrapCoreState
	CoreObjects       bootstrap.CoreObjectState
	CookbookInventory map[string]adminMigrationCookbookInventory
	Loaded            bool
}

type adminMigrationBackupRead struct {
	Dependencies      []adminMigrationDependency
	Bootstrap         bootstrap.BootstrapCoreState
	CoreObjects       bootstrap.CoreObjectState
	CookbookInventory map[string]adminMigrationCookbookInventory
	Cookbooks         adminMigrationCookbookExport
	Loaded            bool
}

type adminMigrationCookbookInventory struct {
	Versions           int
	Artifacts          int
	ChecksumReferences int
	Checksums          []string
}

type adminMigrationCookbookInventoryLoader interface {
	LoadCookbookInventory([]string) (map[string]adminMigrationCookbookInventory, error)
}

type adminMigrationCookbookRestoreStore interface {
	RestoreCookbookExport(bootstrap.BootstrapCoreState, adminMigrationCookbookExport) error
}

type adminMigrationBlobReference struct {
	Checksum     string
	Organization string
	Family       string
}

type adminMigrationBlobValidation struct {
	Dependency adminMigrationDependency
	Families   []adminMigrationInventoryFamily
	Findings   []adminMigrationFinding
}

type adminMigrationBackupBlobCopyRead struct {
	Dependency adminMigrationDependency
	Families   []adminMigrationInventoryFamily
	Findings   []adminMigrationFinding
	Copies     []adminMigrationBackupBlobData
}

type adminMigrationOpenSearchValidation struct {
	Dependency       adminMigrationDependency
	Families         []adminMigrationInventoryFamily
	Findings         []adminMigrationFinding
	PlannedMutations []adminMigrationPlannedMutation
}

type adminMigrationSourceManifest struct {
	FormatVersion string                                 `json:"format_version,omitempty"`
	SourceType    string                                 `json:"source_type,omitempty"`
	Families      []adminMigrationInventoryFamily        `json:"families,omitempty"`
	Artifacts     []adminMigrationSourceManifestArtifact `json:"artifacts,omitempty"`
	Notes         []string                               `json:"notes,omitempty"`
}

type adminMigrationSourceManifestArtifact struct {
	Family    string `json:"family"`
	Path      string `json:"path,omitempty"`
	Count     int    `json:"count,omitempty"`
	Supported *bool  `json:"supported,omitempty"`
	Deferred  bool   `json:"deferred,omitempty"`
}

type adminMigrationSourceInventoryRead struct {
	SourceType    string
	FormatVersion string
	Inventory     adminMigrationInventory
	Findings      []adminMigrationFinding
}

type adminMigrationSourceArtifactEntry struct {
	Path  string
	IsDir bool
}

type adminMigrationRehearsalCheck struct {
	Family             string
	Name               string
	Method             string
	Path               string
	DownloadChecksums  []string
	RequireBlobContent bool
}

type adminMigrationRehearsalResult struct {
	Checks    int
	Passed    int
	Failed    int
	Skipped   int
	Downloads int
	Findings  []adminMigrationFinding
}

type adminMigrationUnsignedClient interface {
	DoUnsigned(context.Context, string, string) (admin.RawResponse, error)
}

type adminMigrationFinding struct {
	Severity     string `json:"severity"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	Organization string `json:"organization,omitempty"`
	Family       string `json:"family,omitempty"`
}

type adminMigrationPlannedMutation struct {
	Action       string `json:"action"`
	Family       string `json:"family,omitempty"`
	Organization string `json:"organization,omitempty"`
	Name         string `json:"name,omitempty"`
	Count        int    `json:"count,omitempty"`
	Message      string `json:"message,omitempty"`
}

type adminMigrationOutputOptions struct {
	command          string
	target           adminMigrationTarget
	dryRun           bool
	offline          bool
	confirmed        bool
	withTiming       bool
	config           map[string]string
	plannedMutations []adminMigrationPlannedMutation
}

// runAdminMigration owns the migration command namespace so migration tooling
// can evolve without routing through Chef-facing HTTP compatibility paths.
func (c *command) runAdminMigration(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration requires a subcommand\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}

	switch args[0] {
	case "preflight":
		return c.runAdminMigrationPreflight(ctx, args[1:], inheritedJSON)
	case "backup":
		return c.runAdminMigrationBackup(ctx, args[1:], inheritedJSON)
	case "restore":
		return c.runAdminMigrationRestore(ctx, args[1:], inheritedJSON)
	case "source":
		return c.runAdminMigrationSource(args[1:], inheritedJSON)
	case "cutover":
		return c.runAdminMigrationCutover(ctx, args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration command %q\n\n", args[0])
	}
}

// runAdminMigrationPreflight performs read-only target readiness checks for the
// configured PostgreSQL, blob, OpenSearch, and bootstrap settings.
func (c *command) runAdminMigrationPreflight(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration preflight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	bindAdminMigrationScopeFlags(fs, opts)
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin migration preflight", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin migration preflight received unexpected arguments: %v\n\n", fs.Args())
	}
	if _, ok := validateAdminMigrationScope(opts); !ok {
		return c.adminUsageError("admin migration preflight cannot combine --all-orgs with --org\n\n")
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}
	orgName := strings.TrimSpace(opts.orgName)
	result := c.buildAdminMigrationPreflight(ctx, cfg, opts, orgName)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationPreflight collects dependency and configuration findings
// without mutating Chef-facing state or provider contents.
func (c *command) buildAdminMigrationPreflight(ctx context.Context, cfg config.Config, opts *adminMigrationFlagValues, orgName string) adminMigrationCLIOutput {
	out := adminMigrationCLIOutput{
		OK:      true,
		Command: "migration_preflight",
		Target: adminMigrationTarget{
			AllOrganizations: opts.allOrgs || orgName == "",
			Organization:     orgName,
		},
		DryRun:           opts.dryRun,
		Config:           cfg.Redacted(),
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}

	postgresRead := c.adminMigrationPostgresRead(ctx, cfg)
	out.Dependencies = append(out.Dependencies, postgresRead.Dependencies...)
	var blobReferences []adminMigrationBlobReference
	if postgresRead.Loaded {
		out.Inventory = adminMigrationInventoryFromState(postgresRead.Bootstrap, postgresRead.CoreObjects, postgresRead.CookbookInventory, orgName)
		out.Findings = append(out.Findings, adminMigrationStateFindings(cfg, postgresRead.Bootstrap, postgresRead.CoreObjects, postgresRead.CookbookInventory, orgName)...)
		blobReferences = adminMigrationBlobReferencesFromState(postgresRead.CoreObjects, postgresRead.CookbookInventory, orgName)
	}
	blobValidation := c.adminMigrationBlobValidation(ctx, cfg, blobReferences)
	out.Dependencies = append(out.Dependencies, blobValidation.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobValidation.Families...)
	out.Findings = append(out.Findings, blobValidation.Findings...)
	openSearchDependency := adminMigrationOpenSearchDependency(ctx, cfg)
	out.Dependencies = append(out.Dependencies, openSearchDependency)
	openSearchValidation := c.adminMigrationOpenSearchValidation(ctx, cfg, postgresRead, orgName, openSearchDependency)
	out.Dependencies = append(out.Dependencies, openSearchValidation.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, openSearchValidation.Families...)
	out.Findings = append(out.Findings, openSearchValidation.Findings...)
	out.PlannedMutations = append(out.PlannedMutations, openSearchValidation.PlannedMutations...)
	out.Dependencies = append(out.Dependencies, adminMigrationRuntimeConfigDependency(cfg))
	out.Findings = append(out.Findings, adminMigrationRuntimeConfigFindings(cfg)...)

	for _, dep := range out.Dependencies {
		switch dep.Status {
		case "error":
			out.OK = false
			out.Errors = append(out.Errors, adminCLIError{
				Code:    adminMigrationDependencyErrorCode(dep.Name),
				Message: dep.Message,
			})
		case "warning":
			out.Warnings = append(out.Warnings, dep.Message)
		}
	}
	for _, finding := range out.Findings {
		switch finding.Severity {
		case "error":
			out.OK = false
			out.Errors = append(out.Errors, adminCLIError{
				Code:    finding.Code,
				Message: finding.Message,
			})
		case "warning":
			out.Warnings = append(out.Warnings, finding.Message)
		}
	}
	return out
}

// runAdminMigrationBackup dispatches backup subcommands while preserving the
// safety distinction between local bundle inspection and offline backup writes.
func (c *command) runAdminMigrationBackup(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration backup requires create or inspect\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}

	switch args[0] {
	case "create":
		return c.runAdminMigrationBackupCreate(ctx, args[1:], inheritedJSON)
	case "inspect":
		return c.runAdminMigrationBackupInspect(args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration backup command %q\n\n", args[0])
	}
}

// runAdminMigrationBackupCreate writes a logical backup bundle from offline
// PostgreSQL-backed state after validating referenced blobs and safety gates.
func (c *command) runAdminMigrationBackupCreate(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration backup create", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.outputPath, "output", "", "backup bundle output path")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin migration backup create", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin migration backup create received unexpected arguments: %v\n\n", fs.Args())
	}
	if strings.TrimSpace(opts.outputPath) == "" {
		return c.adminUsageError("admin migration backup create requires --output PATH\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration backup create is offline-only and requires --offline\n\n")
	}
	if !opts.dryRun && !opts.yes {
		return c.adminUsageError("admin migration backup create requires --dry-run or --yes\n\n")
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationBackupCreate(ctx, cfg, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// runAdminMigrationBackupInspect validates the bundle manifest and payload
// hashes without connecting to PostgreSQL, blob, or OpenSearch providers.
func (c *command) runAdminMigrationBackupInspect(args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration backup inspect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	bundlePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration backup inspect", err)
	}
	if bundlePath == "" && fs.NArg() == 1 {
		bundlePath = fs.Arg(0)
	} else if bundlePath != "" && fs.NArg() == 0 {
		// The bundle path was accepted before flags so the documented PATH [flags]
		// shape works with Go's otherwise flag-first parser.
	} else {
		return c.adminUsageError("usage: opencook admin migration backup inspect PATH [--json]\n\n")
	}

	result := buildAdminMigrationBackupInspect(bundlePath, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationBackupCreate prepares the same dependency, inventory, and
// finding envelope operators see in preflight, then writes only when clean and confirmed.
func (c *command) buildAdminMigrationBackupCreate(ctx context.Context, cfg config.Config, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationCLIOutput{
		OK:           true,
		Command:      "migration_backup_create",
		Target:       adminMigrationTarget{OutputPath: opts.outputPath},
		DryRun:       opts.dryRun,
		Offline:      opts.offline,
		Confirmed:    opts.yes,
		Config:       cfg.Redacted(),
		Dependencies: []adminMigrationDependency{},
		Inventory: adminMigrationInventory{
			Families: []adminMigrationInventoryFamily{},
		},
		Findings: []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{{
			Action:  "write_backup_bundle",
			Message: "would write a versioned OpenCook backup bundle",
		}},
	}

	backupRead := c.adminMigrationLoadBackupState(ctx, cfg)
	out.Dependencies = append(out.Dependencies, backupRead.Dependencies...)
	var blobReferences []adminMigrationBlobReference
	if backupRead.Loaded {
		out.Inventory = adminMigrationInventoryFromState(backupRead.Bootstrap, backupRead.CoreObjects, backupRead.CookbookInventory, "")
		out.Findings = append(out.Findings, adminMigrationStateFindings(cfg, backupRead.Bootstrap, backupRead.CoreObjects, backupRead.CookbookInventory, "")...)
		blobReferences = adminMigrationBlobReferencesFromState(backupRead.CoreObjects, backupRead.CookbookInventory, "")
	}

	blobValidation := c.adminMigrationBlobValidation(ctx, cfg, blobReferences)
	out.Dependencies = append(out.Dependencies, blobValidation.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobValidation.Families...)
	out.Findings = append(out.Findings, blobValidation.Findings...)

	var blobCopies []adminMigrationBackupBlobData
	if backupRead.Loaded && blobValidation.Dependency.Status == "ok" && !adminMigrationHasErrorFindings(blobValidation.Findings) {
		copyRead := c.adminMigrationBackupBlobCopies(ctx, cfg, blobReferences)
		out.Dependencies = append(out.Dependencies, copyRead.Dependency)
		out.Inventory.Families = append(out.Inventory.Families, copyRead.Families...)
		out.Findings = append(out.Findings, copyRead.Findings...)
		blobCopies = copyRead.Copies
	}

	adminMigrationCollectOutputStatuses(&out)
	if !out.OK || opts.dryRun {
		return out
	}

	manifest, err := adminMigrationWriteBackupBundle(opts.outputPath, adminMigrationBackupBundleInput{
		Build:       c.build,
		CreatedAt:   time.Now().UTC(),
		Config:      cfg,
		Bootstrap:   backupRead.Bootstrap,
		CoreObjects: backupRead.CoreObjects,
		Cookbooks:   backupRead.Cookbooks,
		BlobCopies:  blobCopies,
		Inventory:   out.Inventory,
		Warnings:    out.Warnings,
	})
	if err != nil {
		out.OK = false
		out.Errors = append(out.Errors, adminCLIError{
			Code:    "backup_write_failed",
			Message: "backup bundle could not be written",
		})
		out.Findings = append(out.Findings, adminMigrationFinding{
			Severity: "error",
			Code:     "backup_write_failed",
			Family:   "backup_bundle",
			Message:  "backup bundle could not be written",
		})
		return out
	}

	out.PlannedMutations = []adminMigrationPlannedMutation{{
		Action:  "write_backup_bundle",
		Family:  "backup_bundle",
		Count:   len(manifest.Payloads) + 1,
		Message: "wrote versioned OpenCook backup bundle",
	}}
	return out
}

// adminMigrationLoadBackupState loads every PostgreSQL-backed state family
// needed for a portable bundle through the offline store/repository seams.
func (c *command) adminMigrationLoadBackupState(ctx context.Context, cfg config.Config) adminMigrationBackupRead {
	if strings.TrimSpace(cfg.PostgresDSN) == "" {
		return adminMigrationBackupRead{Dependencies: []adminMigrationDependency{{
			Name:       "postgres",
			Status:     "error",
			Backend:    "postgres",
			Configured: false,
			Message:    "PostgreSQL is not configured; set OPENCOOK_POSTGRES_DSN or --postgres-dsn",
		}}}
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return adminMigrationBackupRead{Dependencies: []adminMigrationDependency{{
			Name:       "postgres",
			Status:     "error",
			Backend:    "postgres",
			Configured: true,
			Message:    adminMigrationSafeErrorMessage("postgres", err),
		}}}
	}
	defer closeOfflineStore(closeStore)

	result := adminMigrationBackupRead{
		Dependencies: []adminMigrationDependency{{
			Name:       "postgres",
			Status:     "ok",
			Backend:    "postgres",
			Configured: true,
			Message:    "PostgreSQL is configured, reachable, and activated for offline backup reads",
			Details: map[string]string{
				"cookbook_persistence":       "active",
				"bootstrap_core_persistence": "active",
				"core_object_persistence":    "active",
			},
		}},
		CookbookInventory: map[string]adminMigrationCookbookInventory{},
		Cookbooks:         adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}

	bootstrapLoaded := false
	coreObjectsLoaded := false
	cookbooksLoaded := false
	if state, err := store.LoadBootstrapCore(); err != nil {
		result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_bootstrap_core", "error", adminMigrationSafeErrorMessage("postgres_bootstrap_core", err)))
	} else {
		result.Bootstrap = state
		bootstrapLoaded = true
		result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_bootstrap_core", "ok", "bootstrap core state loaded for backup export"))
	}
	if state, err := store.LoadCoreObjects(); err != nil {
		result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_core_objects", "error", adminMigrationSafeErrorMessage("postgres_core_objects", err)))
	} else {
		result.CoreObjects = state
		coreObjectsLoaded = true
		result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_core_objects", "ok", "core object state loaded for backup export"))
	}
	if bootstrapLoaded && coreObjectsLoaded {
		orgNames := adminMigrationOrgNames(result.Bootstrap, result.CoreObjects, nil, "")
		if loader, ok := store.(adminMigrationCookbookExportLoader); ok {
			cookbooks, err := loader.LoadCookbookExport(orgNames)
			if err != nil {
				result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "error", adminMigrationSafeErrorMessage("postgres_cookbooks", err)))
			} else {
				result.Cookbooks = cookbooks
				cookbooksLoaded = true
				result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "ok", "cookbook metadata loaded for backup export"))
			}
		} else {
			result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "error", "cookbook export repository is unavailable for backup creation"))
		}
		if loader, ok := store.(adminMigrationCookbookInventoryLoader); ok {
			inventory, err := loader.LoadCookbookInventory(orgNames)
			if err != nil {
				result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbook_inventory", "error", adminMigrationSafeErrorMessage("postgres_cookbooks", err)))
			} else {
				result.CookbookInventory = inventory
			}
		}
		if len(result.CookbookInventory) == 0 && cookbooksLoaded {
			result.CookbookInventory = adminMigrationCookbookInventoryFromExport(result.Cookbooks)
		}
	}
	result.Loaded = bootstrapLoaded && coreObjectsLoaded && cookbooksLoaded
	return result
}

// adminMigrationCookbookInventoryFromExport derives backup validation counts
// from the full cookbook export when a store does not provide a lighter inventory helper.
func adminMigrationCookbookInventoryFromExport(cookbooks adminMigrationCookbookExport) map[string]adminMigrationCookbookInventory {
	out := map[string]adminMigrationCookbookInventory{}
	for _, orgName := range adminMigrationSortedMapKeys(cookbooks.Orgs) {
		org := cookbooks.Orgs[orgName]
		checksumSet := map[string]struct{}{}
		references := 0
		for _, version := range org.Versions {
			for _, checksum := range adminMigrationCookbookFileChecksums(version.AllFiles) {
				references++
				checksumSet[checksum] = struct{}{}
			}
		}
		for _, artifact := range org.Artifacts {
			for _, checksum := range adminMigrationCookbookFileChecksums(artifact.AllFiles) {
				references++
				checksumSet[checksum] = struct{}{}
			}
		}
		out[orgName] = adminMigrationCookbookInventory{
			Versions:           len(org.Versions),
			Artifacts:          len(org.Artifacts),
			ChecksumReferences: references,
			Checksums:          adminMigrationSortedStringSet(checksumSet),
		}
	}
	return out
}

// adminMigrationBackupBlobCopies copies reachable local blob bytes into the
// backup bundle input when the configured backend supports deterministic reads.
func (c *command) adminMigrationBackupBlobCopies(ctx context.Context, cfg config.Config, references []adminMigrationBlobReference) adminMigrationBackupBlobCopyRead {
	unique := adminMigrationUniqueBlobReferences(references)
	baseFamilies := []adminMigrationInventoryFamily{{Family: "copied_blobs", Count: 0}}
	if len(unique) == 0 {
		return adminMigrationBackupBlobCopyRead{
			Dependency: adminMigrationDependency{Name: "backup_blob_copy", Status: "skipped", Backend: adminMigrationBlobBackendLabel(cfg), Configured: true, Message: "no referenced checksum blobs need byte-copying"},
			Families:   baseFamilies,
		}
	}
	newBlobStore := c.newBlobStore
	if newBlobStore == nil {
		newBlobStore = blob.NewStore
	}
	store, err := newBlobStore(cfg)
	if err != nil {
		return adminMigrationBackupBlobCopyUnavailable(cfg, unique, adminMigrationSafeErrorMessage("blob", err))
	}
	getter, ok := adminMigrationBlobContentGetter(store)
	status := store.Status()
	if !ok {
		return adminMigrationBackupBlobCopyRead{
			Dependency: adminMigrationDependency{
				Name:       "backup_blob_copy",
				Status:     "skipped",
				Backend:    status.Backend,
				Configured: status.Configured,
				Message:    "blob backend does not support deterministic local content reads; backup records checksum references only",
			},
			Families: baseFamilies,
		}
	}

	result := adminMigrationBackupBlobCopyRead{
		Dependency: adminMigrationDependency{
			Name:       "backup_blob_copy",
			Status:     "ok",
			Backend:    status.Backend,
			Configured: status.Configured,
			Message:    "referenced blob bytes copied from deterministic local backend",
		},
		Families: baseFamilies,
		Copies:   []adminMigrationBackupBlobData{},
	}
	for _, ref := range unique {
		body, err := getter.Get(ctx, ref.Checksum)
		if err != nil {
			result.Dependency.Status = "error"
			if errors.Is(err, blob.ErrNotFound) {
				result.Findings = append(result.Findings, adminMigrationBlobFinding("error", "missing_blob", ref, "referenced checksum "+ref.Checksum+" is missing from the blob backend"))
				continue
			}
			result.Findings = append(result.Findings, adminMigrationBlobFinding("error", "blob_content_unavailable", ref, "blob backend could not read checksum "+ref.Checksum+" for backup byte-copying"))
			continue
		}
		if got := adminMigrationMD5Hex(body); got != ref.Checksum {
			result.Dependency.Status = "error"
			result.Findings = append(result.Findings, adminMigrationBlobFinding("error", "blob_checksum_mismatch", ref, "blob content for checksum "+ref.Checksum+" does not match its Chef checksum"))
			continue
		}
		result.Copies = append(result.Copies, adminMigrationBackupBlobData{Checksum: ref.Checksum, Body: body})
	}
	result.Families[0].Count = len(result.Copies)
	if result.Dependency.Status == "error" {
		result.Dependency.Message = "one or more referenced blob bytes could not be copied into the backup bundle"
	}
	return result
}

// adminMigrationBackupBlobCopyUnavailable reports provider construction
// failures using the same redacted wording as preflight blob checks.
func adminMigrationBackupBlobCopyUnavailable(cfg config.Config, references []adminMigrationBlobReference, message string) adminMigrationBackupBlobCopyRead {
	read := adminMigrationBackupBlobCopyRead{
		Dependency: adminMigrationDependency{
			Name:       "backup_blob_copy",
			Status:     "error",
			Backend:    adminMigrationBlobBackendLabel(cfg),
			Configured: true,
			Message:    message,
		},
		Families: []adminMigrationInventoryFamily{{Family: "copied_blobs", Count: 0}},
	}
	if len(references) > 0 {
		read.Findings = append(read.Findings, adminMigrationFinding{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "referenced checksum blobs could not be copied because the blob backend is unavailable",
		})
	}
	return read
}

// buildAdminMigrationBackupInspect verifies manifest integrity using only
// local bundle files so inspect can safely run without external dependencies.
func buildAdminMigrationBackupInspect(bundlePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationCLIOutput{
		OK:        true,
		Command:   "migration_backup_inspect",
		Target:    adminMigrationTarget{BundlePath: bundlePath},
		DryRun:    opts.dryRun,
		Offline:   opts.offline,
		Confirmed: opts.yes,
		Dependencies: []adminMigrationDependency{{
			Name:       "backup_bundle",
			Status:     "ok",
			Backend:    "filesystem",
			Configured: true,
			Message:    "backup manifest and payload hashes are valid",
		}},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
	manifest, findings, err := adminMigrationInspectBackupBundle(bundlePath)
	if err != nil {
		out.OK = false
		out.Dependencies[0].Status = "error"
		out.Dependencies[0].Message = "backup bundle failed integrity inspection"
		out.Errors = append(out.Errors, adminCLIError{Code: "backup_bundle_invalid", Message: "backup bundle failed integrity inspection"})
		out.Findings = append(out.Findings, findings...)
		if len(out.Findings) == 0 {
			out.Findings = append(out.Findings, adminMigrationFinding{Severity: "error", Code: "backup_bundle_invalid", Family: "backup_bundle", Message: "backup bundle failed integrity inspection"})
		}
		return out
	}
	out.Inventory = manifest.Inventory
	out.Warnings = append(out.Warnings, manifest.Warnings...)
	out.Findings = append(out.Findings, findings...)
	out.Dependencies[0].Details = map[string]string{
		"format_version": manifest.FormatVersion,
		"payloads":       fmt.Sprintf("%d", len(manifest.Payloads)),
	}
	return out
}

// adminMigrationInspectBackupBundle loads manifest.json, checks every recorded
// payload size plus SHA-256 digest, and requires the fixed payloads that restore
// consumes later so a trimmed manifest cannot bypass integrity validation.
func adminMigrationInspectBackupBundle(bundlePath string) (adminMigrationBackupManifest, []adminMigrationFinding, error) {
	manifestPath := filepath.Join(bundlePath, adminMigrationBackupManifestPath)
	rawManifest, err := os.ReadFile(manifestPath)
	if err != nil {
		return adminMigrationBackupManifest{}, []adminMigrationFinding{adminMigrationBackupInspectFinding("backup_manifest_missing", "backup manifest.json could not be read")}, err
	}
	var manifest adminMigrationBackupManifest
	if err := json.Unmarshal(rawManifest, &manifest); err != nil {
		return adminMigrationBackupManifest{}, []adminMigrationFinding{adminMigrationBackupInspectFinding("backup_manifest_invalid_json", "backup manifest.json is not valid JSON")}, err
	}
	if manifest.FormatVersion != adminMigrationBackupFormatVersion {
		return manifest, []adminMigrationFinding{adminMigrationBackupInspectFinding("backup_manifest_unsupported_format", "backup manifest format version is not supported")}, fmt.Errorf("unsupported backup format %q", manifest.FormatVersion)
	}
	var findings []adminMigrationFinding
	manifestPayloads := map[string]struct{}{}
	for _, payload := range manifest.Payloads {
		relativePath, err := adminMigrationNormalizeBackupPayloadPath(payload.Path)
		if err == nil {
			manifestPayloads[relativePath] = struct{}{}
		}
		if err := adminMigrationInspectBackupPayload(bundlePath, payload); err != nil {
			findings = append(findings, adminMigrationBackupInspectFinding("backup_payload_integrity_failed", "backup payload "+payload.Path+" failed integrity validation"))
		}
	}
	for _, path := range adminMigrationRequiredRestorePayloadPaths() {
		if _, ok := manifestPayloads[path]; !ok {
			findings = append(findings, adminMigrationBackupInspectFinding("backup_required_payload_missing", "backup manifest is missing required restore payload "+path))
		}
	}
	if len(findings) > 0 {
		return manifest, findings, fmt.Errorf("backup payload integrity failed")
	}
	return manifest, nil, nil
}

// adminMigrationRequiredRestorePayloadPaths lists payloads that restore reads by
// fixed path after the manifest gate succeeds.
func adminMigrationRequiredRestorePayloadPaths() []string {
	return []string{
		adminMigrationBackupBootstrapPath,
		adminMigrationBackupObjectsPath,
		adminMigrationBackupCookbooksPath,
		adminMigrationBackupBlobsPath,
	}
}

// adminMigrationNormalizeBackupPayloadPath canonicalizes manifest paths before
// integrity checks and required-payload matching.
func adminMigrationNormalizeBackupPayloadPath(path string) (string, error) {
	relativePath := filepath.Clean(strings.TrimSpace(path))
	if relativePath == "." || filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("invalid backup payload path %q", path)
	}
	return filepath.ToSlash(relativePath), nil
}

// adminMigrationInspectBackupPayload validates one manifest payload while
// rejecting absolute paths or traversal attempts inside the portable bundle.
func adminMigrationInspectBackupPayload(bundlePath string, payload adminMigrationBackupPayload) error {
	relativePath, err := adminMigrationNormalizeBackupPayloadPath(payload.Path)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(bundlePath, relativePath))
	if err != nil {
		return err
	}
	if int64(len(data)) != payload.Bytes {
		return fmt.Errorf("payload %s byte count mismatch", payload.Path)
	}
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != strings.ToLower(strings.TrimSpace(payload.SHA256)) {
		return fmt.Errorf("payload %s sha256 mismatch", payload.Path)
	}
	return nil
}

// adminMigrationBackupInspectFinding creates provider-free integrity findings
// for backup inspect without leaking filesystem internals.
func adminMigrationBackupInspectFinding(code, message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "error",
		Code:     code,
		Family:   "backup_bundle",
		Message:  message,
	}
}

// runAdminMigrationRestore dispatches restore subcommands and keeps destructive
// apply behavior behind the stricter offline-plus-confirmation gate.
func (c *command) runAdminMigrationRestore(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration restore requires preflight or apply\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}

	switch args[0] {
	case "preflight":
		return c.runAdminMigrationRestorePreflight(ctx, args[1:], inheritedJSON)
	case "apply":
		return c.runAdminMigrationRestoreApply(ctx, args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration restore command %q\n\n", args[0])
	}
}

// runAdminMigrationRestorePreflight validates bundle integrity and target
// emptiness without writing PostgreSQL, blob, or OpenSearch state.
func (c *command) runAdminMigrationRestorePreflight(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration restore preflight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	bundlePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration restore preflight", err)
	}
	if bundlePath == "" && fs.NArg() == 1 {
		bundlePath = fs.Arg(0)
	} else if bundlePath != "" && fs.NArg() == 0 {
		// The bundle path was accepted before flags so the documented PATH [flags]
		// shape works with Go's otherwise flag-first parser.
	} else {
		return c.adminUsageError("usage: opencook admin migration restore preflight PATH --offline [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration restore preflight is offline-only and requires --offline\n\n")
	}
	manifest, blobManifest, inspectResult := buildAdminMigrationRestoreBundlePreflight(bundlePath, opts)
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationRestorePreflight(ctx, cfg, bundlePath, manifest, blobManifest, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationRestoreBundlePreflight performs provider-free bundle
// integrity and blob-manifest parsing before any target configuration is loaded.
func buildAdminMigrationRestoreBundlePreflight(bundlePath string, opts *adminMigrationFlagValues) (adminMigrationBackupManifest, adminMigrationBackupBlobManifest, adminMigrationCLIOutput) {
	out := adminMigrationRestorePreflightOutput(bundlePath, opts)
	manifest, findings, err := adminMigrationInspectBackupBundle(bundlePath)
	if err != nil {
		out.Dependencies = append(out.Dependencies, adminMigrationBackupBundleDependency("error", "backup bundle failed integrity inspection", nil))
		out.Findings = append(out.Findings, findings...)
		if len(out.Findings) == 0 {
			out.Findings = append(out.Findings, adminMigrationBackupInspectFinding("backup_bundle_invalid", "backup bundle failed integrity inspection"))
		}
		adminMigrationCollectOutputStatuses(&out)
		return adminMigrationBackupManifest{}, adminMigrationBackupBlobManifest{}, out
	}
	out.Inventory = manifest.Inventory
	out.Warnings = append(out.Warnings, manifest.Warnings...)
	out.Dependencies = append(out.Dependencies, adminMigrationBackupBundleDependency("ok", "backup manifest and payload hashes are valid", map[string]string{
		"format_version": manifest.FormatVersion,
		"payloads":       fmt.Sprintf("%d", len(manifest.Payloads)),
	}))

	blobManifest, err := adminMigrationReadBackupBlobManifest(bundlePath)
	if err != nil {
		out.Dependencies = append(out.Dependencies, adminMigrationDependency{
			Name:       "backup_blobs",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "backup blob manifest could not be parsed",
		})
		out.Findings = append(out.Findings, adminMigrationBackupInspectFinding("backup_blob_manifest_invalid", "backup blob manifest could not be parsed"))
		adminMigrationCollectOutputStatuses(&out)
		return manifest, adminMigrationBackupBlobManifest{}, out
	}
	out.Dependencies = append(out.Dependencies, adminMigrationDependency{
		Name:       "backup_blobs",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "backup blob manifest is readable",
		Details: map[string]string{
			"referenced_blobs": fmt.Sprintf("%d", len(blobManifest.ReferencedChecksums)),
			"copied_blobs":     fmt.Sprintf("%d", len(blobManifest.Copied)),
		},
	})
	out.PlannedMutations = adminMigrationRestorePlannedMutations(manifest, blobManifest)
	return manifest, blobManifest, out
}

// buildAdminMigrationRestorePreflight checks whether the offline target is safe
// for a future restore and reports provider readiness without mutating anything.
func (c *command) buildAdminMigrationRestorePreflight(ctx context.Context, cfg config.Config, bundlePath string, manifest adminMigrationBackupManifest, blobManifest adminMigrationBackupBlobManifest, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationRestorePreflightOutput(bundlePath, opts)
	out.Config = cfg.Redacted()
	out.Inventory = manifest.Inventory
	out.Warnings = append(out.Warnings, manifest.Warnings...)
	out.Dependencies = append(out.Dependencies,
		adminMigrationBackupBundleDependency("ok", "backup manifest and payload hashes are valid", map[string]string{
			"format_version": manifest.FormatVersion,
			"payloads":       fmt.Sprintf("%d", len(manifest.Payloads)),
		}),
		adminMigrationDependency{
			Name:       "backup_blobs",
			Status:     "ok",
			Backend:    "filesystem",
			Configured: true,
			Message:    "backup blob manifest is readable",
			Details: map[string]string{
				"referenced_blobs": fmt.Sprintf("%d", len(blobManifest.ReferencedChecksums)),
				"copied_blobs":     fmt.Sprintf("%d", len(blobManifest.Copied)),
			},
		},
	)
	out.PlannedMutations = adminMigrationRestorePlannedMutations(manifest, blobManifest)
	if len(blobManifest.ReferencedChecksums) > len(blobManifest.Copied) {
		out.Findings = append(out.Findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "backup_blob_references_without_copied_bytes",
			Family:   "checksum_references",
			Message:  "backup bundle references checksum blobs that are not all included as copied byte payloads",
		})
	}

	postgresRead := c.adminMigrationPostgresRead(ctx, cfg)
	out.Dependencies = append(out.Dependencies, postgresRead.Dependencies...)
	if postgresRead.Loaded {
		targetInventory := adminMigrationInventoryFromState(postgresRead.Bootstrap, postgresRead.CoreObjects, postgresRead.CookbookInventory, "")
		targetDependency, targetFindings := adminMigrationRestoreTargetDependency(targetInventory, manifest.Inventory)
		out.Dependencies = append(out.Dependencies, targetDependency)
		out.Findings = append(out.Findings, targetFindings...)
	} else {
		out.Dependencies = append(out.Dependencies, adminMigrationDependency{
			Name:       "restore_target",
			Status:     "skipped",
			Backend:    "postgres",
			Configured: strings.TrimSpace(cfg.PostgresDSN) != "",
			Message:    "restore target emptiness check skipped because PostgreSQL state did not load",
		})
	}

	blobValidation := c.adminMigrationBlobValidation(ctx, cfg, nil)
	out.Dependencies = append(out.Dependencies, blobValidation.Dependency)
	out.Findings = append(out.Findings, blobValidation.Findings...)
	openSearchDependency := adminMigrationOpenSearchDependency(ctx, cfg)
	out.Dependencies = append(out.Dependencies, openSearchDependency)

	adminMigrationCollectOutputStatuses(&out)
	return out
}

// adminMigrationRestorePreflightOutput initializes the shared JSON envelope for
// restore preflight so bundle-only failures and full target checks stay aligned.
func adminMigrationRestorePreflightOutput(bundlePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_restore_preflight",
		Target:           adminMigrationTarget{BundlePath: bundlePath},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
}

// adminMigrationReadBackupBlobManifest parses the blob manifest after the
// top-level manifest has already verified its payload hash.
func adminMigrationReadBackupBlobManifest(bundlePath string) (adminMigrationBackupBlobManifest, error) {
	data, err := os.ReadFile(filepath.Join(bundlePath, adminMigrationBackupBlobsPath))
	if err != nil {
		return adminMigrationBackupBlobManifest{}, err
	}
	var manifest adminMigrationBackupBlobManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return adminMigrationBackupBlobManifest{}, err
	}
	if manifest.FormatVersion != adminMigrationBackupFormatVersion {
		return adminMigrationBackupBlobManifest{}, fmt.Errorf("unsupported backup blob manifest format %q", manifest.FormatVersion)
	}
	return manifest, nil
}

// adminMigrationBackupBundleDependency keeps restore and inspect bundle
// integrity checks using the same provider-free dependency shape.
func adminMigrationBackupBundleDependency(status, message string, details map[string]string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       "backup_bundle",
		Status:     status,
		Backend:    "filesystem",
		Configured: true,
		Message:    message,
		Details:    details,
	}
}

// adminMigrationRestoreTargetDependency refuses to restore into a target that
// already has PostgreSQL-backed OpenCook state until overwrite semantics exist.
func adminMigrationRestoreTargetDependency(target, expected adminMigrationInventory) (adminMigrationDependency, []adminMigrationFinding) {
	existing := adminMigrationInventoryTotalCount(target)
	dep := adminMigrationDependency{
		Name:       "restore_target",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "restore target PostgreSQL-backed state is empty",
		Details: map[string]string{
			"state":            "empty",
			"existing_objects": fmt.Sprintf("%d", existing),
			"expected_objects": fmt.Sprintf("%d", adminMigrationInventoryTotalCount(expected)),
			"expected_orgs":    strings.Join(adminMigrationInventoryOrgNames(expected), ","),
		},
	}
	if existing == 0 {
		return dep, nil
	}
	dep.Status = "error"
	dep.Message = "restore target PostgreSQL-backed state is not empty"
	dep.Details["state"] = "non_empty"
	return dep, []adminMigrationFinding{{
		Severity: "error",
		Code:     "restore_target_not_empty",
		Family:   "restore_target",
		Message:  "restore preflight refuses non-empty PostgreSQL-backed targets until an explicit overwrite mode exists",
	}}
}

// adminMigrationRestorePlannedMutations summarizes what a later restore apply
// would write if preflight succeeds; it never performs those writes.
func adminMigrationRestorePlannedMutations(manifest adminMigrationBackupManifest, blobManifest adminMigrationBackupBlobManifest) []adminMigrationPlannedMutation {
	return []adminMigrationPlannedMutation{
		{
			Action:  "restore_backup_bundle",
			Family:  "postgres",
			Count:   adminMigrationInventoryTotalCount(manifest.Inventory),
			Message: "would restore PostgreSQL-backed logical state from the backup bundle",
		},
		{
			Action:  "restore_blob_objects",
			Family:  "blobs",
			Count:   len(blobManifest.ReferencedChecksums),
			Message: "would restore or verify referenced checksum blob content, using copied payloads when included",
		},
		{
			Action:  "rebuild_opensearch",
			Family:  "opensearch",
			Message: "would rebuild OpenSearch derived state from restored PostgreSQL state after restore",
		},
	}
}

// adminMigrationInventoryTotalCount totals non-derived inventory rows so
// restore safety checks can compare source and target state compactly.
func adminMigrationInventoryTotalCount(inventory adminMigrationInventory) int {
	total := 0
	for _, family := range inventory.Families {
		if strings.HasPrefix(family.Family, "opensearch_") {
			continue
		}
		total += family.Count
	}
	return total
}

// adminMigrationInventoryOrgNames extracts sorted organization names from an
// inventory payload for human-readable restore target details.
func adminMigrationInventoryOrgNames(inventory adminMigrationInventory) []string {
	seen := map[string]struct{}{}
	for _, family := range inventory.Families {
		if strings.TrimSpace(family.Organization) != "" {
			seen[family.Organization] = struct{}{}
		}
	}
	return adminMigrationSortedStringSet(seen)
}

// runAdminMigrationRestoreApply performs a preflight-equivalent validation
// before restoring blobs first and PostgreSQL-backed state second.
func (c *command) runAdminMigrationRestoreApply(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration restore apply", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	bundlePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration restore apply", err)
	}
	if bundlePath == "" && fs.NArg() == 1 {
		bundlePath = fs.Arg(0)
	} else if bundlePath != "" && fs.NArg() == 0 {
		// The bundle path was accepted before flags so the documented PATH [flags]
		// shape works with Go's otherwise flag-first parser.
	} else {
		return c.adminUsageError("usage: opencook admin migration restore apply PATH --offline --yes [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration restore apply is offline-only and requires --offline\n\n")
	}
	if !opts.dryRun && !opts.yes {
		return c.adminUsageError("admin migration restore apply requires --dry-run or --yes\n\n")
	}
	manifest, blobManifest, inspectResult := buildAdminMigrationRestoreBundlePreflight(bundlePath, opts)
	inspectResult.Command = "migration_restore_apply"
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationRestorePreflight(ctx, cfg, bundlePath, manifest, blobManifest, opts)
	result.Command = "migration_restore_apply"
	result.Confirmed = opts.yes
	if !result.OK || opts.dryRun {
		return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
	}

	result = c.applyAdminMigrationRestore(ctx, cfg, bundlePath, manifest, blobManifest, result)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// applyAdminMigrationRestore writes a validated bundle into an empty offline
// target, rolling back bootstrap/core rows if a later metadata write fails.
func (c *command) applyAdminMigrationRestore(ctx context.Context, cfg config.Config, bundlePath string, manifest adminMigrationBackupManifest, blobManifest adminMigrationBackupBlobManifest, out adminMigrationCLIOutput) adminMigrationCLIOutput {
	bootstrapState, coreObjectState, cookbooks, err := adminMigrationReadBackupStatePayloads(bundlePath)
	if err != nil {
		adminMigrationMarkError(&out, "backup_payload_read_failed", "backup state payloads could not be read", "backup_bundle")
		return out
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "restore_write",
			Status:     "error",
			Backend:    "postgres",
			Configured: strings.TrimSpace(cfg.PostgresDSN) != "",
			Message:    adminMigrationSafeErrorMessage("postgres", err),
		})
		return out
	}
	defer closeOfflineStore(closeStore)

	previousBootstrap, previousCore, ok := adminMigrationLoadRestoreTargetSnapshots(&out, store)
	if !ok {
		return out
	}
	targetCookbookInventory, err := adminMigrationRestoreTargetCookbookInventory(store, previousBootstrap, previousCore, manifest.Inventory)
	if err != nil {
		adminMigrationMarkError(&out, "postgres_cookbooks_unavailable", "restore target cookbook state could not be loaded", "restore_target")
		return out
	}
	targetDependency, targetFindings := adminMigrationRestoreTargetDependency(adminMigrationInventoryFromState(previousBootstrap, previousCore, targetCookbookInventory, ""), manifest.Inventory)
	if targetDependency.Status == "error" {
		adminMigrationMarkDependency(&out, targetDependency)
		for _, finding := range targetFindings {
			adminMigrationMarkFinding(&out, finding)
		}
		return out
	}

	blobRestore := c.adminMigrationRestoreBundleBlobs(ctx, cfg, bundlePath, blobManifest)
	adminMigrationMarkDependency(&out, blobRestore.Dependency)
	for _, finding := range blobRestore.Findings {
		adminMigrationMarkFinding(&out, finding)
	}
	if blobRestore.Dependency.Status == "error" || adminMigrationHasErrorFindings(blobRestore.Findings) {
		return out
	}

	if err := adminMigrationSaveRestoredState(store, previousBootstrap, previousCore, bootstrapState, coreObjectState, cookbooks); err != nil {
		adminMigrationMarkError(&out, "restore_write_failed", "backup state could not be restored; PostgreSQL-backed metadata was rolled back where possible", "restore_target")
		return out
	}

	adminMigrationMarkDependency(&out, adminMigrationDependency{
		Name:       "restore_write",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "PostgreSQL-backed logical state restored from backup bundle",
	})
	out.PlannedMutations = adminMigrationRestoreCompletedMutations(manifest, blobManifest)
	out.Warnings = append(out.Warnings, "OpenSearch documents were not restored as source data; run opencook admin reindex --all-orgs --complete after starting the restored target")
	return out
}

// adminMigrationReadBackupStatePayloads decodes the typed PostgreSQL logical
// state payloads after manifest integrity has already been verified.
func adminMigrationReadBackupStatePayloads(bundlePath string) (bootstrap.BootstrapCoreState, bootstrap.CoreObjectState, adminMigrationCookbookExport, error) {
	var bootstrapState bootstrap.BootstrapCoreState
	if err := adminMigrationReadBackupJSONPayload(bundlePath, adminMigrationBackupBootstrapPath, &bootstrapState); err != nil {
		return bootstrap.BootstrapCoreState{}, bootstrap.CoreObjectState{}, adminMigrationCookbookExport{}, err
	}
	var coreObjectState bootstrap.CoreObjectState
	if err := adminMigrationReadBackupJSONPayload(bundlePath, adminMigrationBackupObjectsPath, &coreObjectState); err != nil {
		return bootstrap.BootstrapCoreState{}, bootstrap.CoreObjectState{}, adminMigrationCookbookExport{}, err
	}
	var cookbooks adminMigrationCookbookExport
	if err := adminMigrationReadBackupJSONPayload(bundlePath, adminMigrationBackupCookbooksPath, &cookbooks); err != nil {
		return bootstrap.BootstrapCoreState{}, bootstrap.CoreObjectState{}, adminMigrationCookbookExport{}, err
	}
	if cookbooks.Orgs == nil {
		cookbooks.Orgs = map[string]adminMigrationCookbookOrgExport{}
	}
	return bootstrapState, coreObjectState, cookbooks, nil
}

// adminMigrationReadBackupJSONPayload reads one path below the bundle root and
// rejects absolute or traversal paths before JSON decoding.
func adminMigrationReadBackupJSONPayload(bundlePath, relativePath string, out any) error {
	relativePath = filepath.Clean(strings.TrimSpace(relativePath))
	if relativePath == "." || filepath.IsAbs(relativePath) || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) {
		return fmt.Errorf("invalid backup payload path %q", relativePath)
	}
	data, err := os.ReadFile(filepath.Join(bundlePath, relativePath))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// adminMigrationLoadRestoreTargetSnapshots loads current target state for the
// final empty-target check and for best-effort rollback if a later write fails.
func adminMigrationLoadRestoreTargetSnapshots(out *adminMigrationCLIOutput, store adminOfflineStore) (bootstrap.BootstrapCoreState, bootstrap.CoreObjectState, bool) {
	bootstrapState, err := store.LoadBootstrapCore()
	if err != nil {
		adminMigrationMarkError(out, "postgres_bootstrap_core_unavailable", "restore target bootstrap core state could not be loaded", "restore_target")
		return bootstrap.BootstrapCoreState{}, bootstrap.CoreObjectState{}, false
	}
	coreObjectState, err := store.LoadCoreObjects()
	if err != nil {
		adminMigrationMarkError(out, "postgres_core_objects_unavailable", "restore target core object state could not be loaded", "restore_target")
		return bootstrap.BootstrapCoreState{}, bootstrap.CoreObjectState{}, false
	}
	return bootstrapState, coreObjectState, true
}

// adminMigrationRestoreTargetCookbookInventory includes cookbook rows in the
// final apply-time emptiness check so restore retries do not start from a
// target that already contains partially imported cookbook metadata.
func adminMigrationRestoreTargetCookbookInventory(store adminOfflineStore, bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, expected adminMigrationInventory) (map[string]adminMigrationCookbookInventory, error) {
	loader, ok := store.(adminMigrationCookbookInventoryLoader)
	if !ok {
		return nil, nil
	}
	orgSet := map[string]struct{}{}
	for _, orgName := range adminMigrationOrgNames(bootstrapState, coreObjectState, nil, "") {
		orgSet[orgName] = struct{}{}
	}
	for _, orgName := range adminMigrationInventoryOrgNames(expected) {
		orgSet[orgName] = struct{}{}
	}
	orgNames := adminMigrationSortedStringSet(orgSet)
	return loader.LoadCookbookInventory(orgNames)
}

// adminMigrationRestoreBundleBlobs restores copied blob bytes and verifies
// referenced external blobs before any PostgreSQL metadata is made visible.
func (c *command) adminMigrationRestoreBundleBlobs(ctx context.Context, cfg config.Config, bundlePath string, blobManifest adminMigrationBackupBlobManifest) adminMigrationBlobValidation {
	newBlobStore := c.newBlobStore
	if newBlobStore == nil {
		newBlobStore = blob.NewStore
	}
	store, err := newBlobStore(cfg)
	if err != nil {
		dep := adminMigrationDependency{Name: "restore_blobs", Status: "error", Backend: adminMigrationBlobBackendLabel(cfg), Configured: false, Message: adminMigrationSafeErrorMessage("blob", err)}
		return adminMigrationBlobValidation{Dependency: dep, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "referenced checksum blobs could not be restored because the blob backend is unavailable",
		}}}
	}
	status := store.Status()
	dep := adminMigrationDependency{Name: "restore_blobs", Status: "ok", Backend: status.Backend, Configured: status.Configured, Message: "referenced blob content restored or verified"}
	if !status.Configured {
		dep.Status = "error"
		dep.Message = status.Message
		return adminMigrationBlobValidation{Dependency: dep}
	}

	var findings []adminMigrationFinding
	copied := map[string]adminMigrationBackupBlobCopy{}
	for _, copy := range blobManifest.Copied {
		copied[strings.ToLower(strings.TrimSpace(copy.Checksum))] = copy
	}
	if len(copied) > 0 {
		putter, ok := store.(blob.Putter)
		if !ok {
			dep.Status = "error"
			dep.Message = "blob backend does not support restoring copied byte payloads"
			return adminMigrationBlobValidation{Dependency: dep, Findings: []adminMigrationFinding{{
				Severity: "error",
				Code:     "blob_put_unavailable",
				Family:   "checksum_references",
				Message:  "backup bundle includes copied blob bytes but the target backend cannot accept blob writes",
			}}}
		}
		for _, checksum := range adminMigrationSortedMapKeys(copied) {
			body, err := adminMigrationReadCopiedBackupBlob(bundlePath, copied[checksum])
			if err != nil {
				dep.Status = "error"
				findings = append(findings, adminMigrationFinding{Severity: "error", Code: "backup_blob_payload_invalid", Family: "checksum_references", Message: "copied blob payload could not be read from the backup bundle"})
				continue
			}
			if _, err := putter.Put(ctx, blob.PutRequest{Key: checksum, Body: body}); err != nil {
				dep.Status = "error"
				findings = append(findings, adminMigrationFinding{Severity: "error", Code: "blob_restore_failed", Family: "checksum_references", Message: "copied blob payload could not be restored into the target backend"})
			}
		}
	}

	checker, ok := store.(blob.Checker)
	if !ok {
		dep.Status = "error"
		findings = append(findings, adminMigrationFinding{Severity: "error", Code: "blob_check_unavailable", Family: "checksum_references", Message: "blob backend cannot verify restored checksum references"})
		return adminMigrationBlobValidation{Dependency: dep, Findings: findings}
	}
	for _, checksum := range blobManifest.ReferencedChecksums {
		checksum = strings.ToLower(strings.TrimSpace(checksum))
		if checksum == "" {
			continue
		}
		exists, err := checker.Exists(ctx, checksum)
		if err != nil {
			dep.Status = "error"
			findings = append(findings, adminMigrationFinding{Severity: "error", Code: "blob_check_unavailable", Family: "checksum_references", Message: "blob backend could not verify restored checksum " + checksum})
			continue
		}
		if !exists {
			dep.Status = "error"
			findings = append(findings, adminMigrationFinding{Severity: "error", Code: "missing_blob", Family: "checksum_references", Message: "referenced checksum " + checksum + " is missing from the target blob backend"})
		}
	}
	if dep.Status == "error" {
		dep.Message = "one or more referenced blob payloads could not be restored or verified"
	}
	return adminMigrationBlobValidation{Dependency: dep, Findings: findings}
}

// adminMigrationReadCopiedBackupBlob validates a copied blob payload against
// both bundle SHA-256 metadata and the Chef MD5 checksum key before restore.
func adminMigrationReadCopiedBackupBlob(bundlePath string, copy adminMigrationBackupBlobCopy) ([]byte, error) {
	payload := adminMigrationBackupPayload{Path: copy.Path, SHA256: copy.SHA256, Bytes: copy.Bytes}
	if err := adminMigrationInspectBackupPayload(bundlePath, payload); err != nil {
		return nil, err
	}
	body, err := os.ReadFile(filepath.Join(bundlePath, filepath.Clean(copy.Path)))
	if err != nil {
		return nil, err
	}
	if got := adminMigrationMD5Hex(body); got != strings.ToLower(strings.TrimSpace(copy.Checksum)) {
		return nil, fmt.Errorf("copied blob checksum mismatch")
	}
	return body, nil
}

// adminMigrationSaveRestoredState writes restored metadata in dependency order
// and rolls bootstrap/core rows back if a later metadata family fails.
func adminMigrationSaveRestoredState(store adminOfflineStore, previousBootstrap bootstrap.BootstrapCoreState, previousCore bootstrap.CoreObjectState, restoredBootstrap bootstrap.BootstrapCoreState, restoredCore bootstrap.CoreObjectState, cookbooks adminMigrationCookbookExport) error {
	if err := store.SaveBootstrapCore(restoredBootstrap); err != nil {
		return err
	}
	if err := store.SaveCoreObjects(restoredCore); err != nil {
		_ = store.SaveBootstrapCore(previousBootstrap)
		return err
	}
	if adminMigrationCookbookExportCount(cookbooks) > 0 {
		importer, ok := store.(adminMigrationCookbookRestoreStore)
		if !ok {
			_ = store.SaveCoreObjects(previousCore)
			_ = store.SaveBootstrapCore(previousBootstrap)
			return fmt.Errorf("cookbook restore is not supported by this offline store")
		}
		if err := importer.RestoreCookbookExport(restoredBootstrap, cookbooks); err != nil {
			_ = store.SaveCoreObjects(previousCore)
			_ = store.SaveBootstrapCore(previousBootstrap)
			return err
		}
	}
	return nil
}

// adminMigrationCookbookExportCount counts cookbook rows so empty bundles do
// not require the optional cookbook restore interface.
func adminMigrationCookbookExportCount(cookbooks adminMigrationCookbookExport) int {
	count := 0
	for _, org := range cookbooks.Orgs {
		count += len(org.Versions) + len(org.Artifacts)
	}
	return count
}

// adminMigrationRestoreCompletedMutations reports completed restore work plus
// the post-restore reindex command operators should run after restart.
func adminMigrationRestoreCompletedMutations(manifest adminMigrationBackupManifest, blobManifest adminMigrationBackupBlobManifest) []adminMigrationPlannedMutation {
	return []adminMigrationPlannedMutation{
		{Action: "restored_backup_bundle", Family: "postgres", Count: adminMigrationInventoryTotalCount(manifest.Inventory), Message: "restored PostgreSQL-backed logical state from the backup bundle"},
		{Action: "restored_blob_objects", Family: "blobs", Count: len(blobManifest.ReferencedChecksums), Message: "restored or verified referenced checksum blob content"},
		{Action: "recommended_command", Family: "opensearch", Message: "opencook admin reindex --all-orgs --complete"},
	}
}

// runAdminMigrationSource dispatches read-only source inventory commands for
// existing Chef Infra Server artifacts before any importer code exists.
func (c *command) runAdminMigrationSource(args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration source requires inventory\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	if args[0] != "inventory" {
		return c.adminUsageError("unknown admin migration source command %q\n\n", args[0])
	}
	return c.runAdminMigrationSourceInventory(args[1:], inheritedJSON)
}

// runAdminMigrationSourceInventory parses a local Chef Server source artifact
// path without loading OpenCook provider configuration or mutating any state.
func (c *command) runAdminMigrationSourceInventory(args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source inventory", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source inventory", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Accept PATH before flags so extracted backup directories and tarballs
		// can be inspected with the same ergonomics as backup inspect.
	} else {
		return c.adminUsageError("usage: opencook admin migration source inventory PATH [--json] [--with-timing]\n\n")
	}

	result := buildAdminMigrationSourceInventory(sourcePath, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationSourceInventory returns a stable migration envelope for a
// source artifact while intentionally leaving planned mutations empty.
func buildAdminMigrationSourceInventory(sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_source_inventory",
		Target:           adminMigrationTarget{SourcePath: sourcePath},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}

	read, err := adminMigrationReadSourceInventory(sourcePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_artifact",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source artifact could not be inventoried",
		})
		for _, finding := range read.Findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(read.Findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationFinding{
				Severity: "error",
				Code:     "source_artifact_unavailable",
				Family:   "source_inventory",
				Message:  "source artifact could not be read or classified",
			})
		}
		return out
	}

	out.Inventory = read.Inventory
	out.Findings = append(out.Findings, read.Findings...)
	out.Dependencies = append(out.Dependencies, adminMigrationDependency{
		Name:       "source_artifact",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "source artifact was inventoried without connecting to live Chef Server databases",
		Details: map[string]string{
			"source_type":    read.SourceType,
			"format_version": read.FormatVersion,
		},
	})
	adminMigrationCollectOutputStatuses(&out)
	return out
}

// adminMigrationReadSourceInventory classifies a supported read-only source
// path: a source manifest, an extracted directory, or a tar/tar.gz archive.
func adminMigrationReadSourceInventory(sourcePath string) (adminMigrationSourceInventoryRead, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source inventory requires a PATH")}}, fmt.Errorf("source path is required")
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source artifact could not be read")}}, err
	}
	if info.IsDir() {
		if manifestPath, ok := adminMigrationFindSourceManifest(sourcePath); ok {
			read, err := adminMigrationReadSourceManifestFile(manifestPath)
			if err != nil {
				return read, err
			}
			read.SourceType = adminMigrationDefaultSourceType(read.SourceType, "source_manifest_directory")
			return read, nil
		}
		entries, err := adminMigrationSourceDirectoryEntries(sourcePath)
		if err != nil {
			return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source artifact could not be scanned")}}, err
		}
		return adminMigrationSourceInventoryFromEntries("extracted_chef_server_artifact", "", entries), nil
	}
	if adminMigrationSourceLooksLikeJSONManifest(sourcePath) {
		read, err := adminMigrationReadSourceManifestFile(sourcePath)
		if err != nil {
			return read, err
		}
		read.SourceType = adminMigrationDefaultSourceType(read.SourceType, "source_manifest_file")
		return read, nil
	}
	if adminMigrationSourceLooksLikeArchive(sourcePath) {
		entries, err := adminMigrationSourceArchiveEntries(sourcePath)
		if err != nil {
			return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_archive_unreadable", "source archive could not be read")}}, err
		}
		return adminMigrationSourceInventoryFromEntries("chef_server_backup_archive", "", entries), nil
	}
	return adminMigrationSourceInventoryRead{SourceType: "unsupported_file", FormatVersion: "unknown", Inventory: adminMigrationInventory{Families: []adminMigrationInventoryFamily{}}, Findings: []adminMigrationFinding{{
		Severity: "warning",
		Code:     "source_artifact_unsupported",
		Family:   "source_inventory",
		Message:  "source artifact exists, but only source manifests, extracted directories, and tar/tar.gz archives are inventoried in this bucket",
	}}}, nil
}

// adminMigrationFindSourceManifest locates the first supported source manifest
// filename in an extracted artifact directory.
func adminMigrationFindSourceManifest(root string) (string, bool) {
	for _, name := range []string{"opencook-source-manifest.json", "source-manifest.json"} {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return path, true
		}
	}
	return "", false
}

// adminMigrationReadSourceManifestFile decodes the explicit source-inventory
// contract used by generated JSON exports and small parser fixtures.
func adminMigrationReadSourceManifestFile(path string) (adminMigrationSourceInventoryRead, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_missing", "source manifest could not be read")}}, err
	}
	var manifest adminMigrationSourceManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return adminMigrationSourceInventoryRead{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_invalid_json", "source manifest is not valid JSON")}}, err
	}
	formatVersion := strings.TrimSpace(manifest.FormatVersion)
	if formatVersion == "" {
		formatVersion = adminMigrationSourceFormatV1
	}
	if formatVersion != adminMigrationSourceFormatV1 {
		return adminMigrationSourceInventoryRead{FormatVersion: formatVersion, Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_unsupported_format", "source manifest format version is not supported")}}, fmt.Errorf("unsupported source manifest format %q", formatVersion)
	}

	read := adminMigrationSourceInventoryRead{
		SourceType:    manifest.SourceType,
		FormatVersion: formatVersion,
		Inventory:     adminMigrationInventory{Families: adminMigrationSortedInventoryFamilies(manifest.Families)},
		Findings:      adminMigrationManifestArtifactFindings(manifest.Artifacts),
	}
	read.Inventory.Families = append(read.Inventory.Families, adminMigrationManifestArtifactFamilies(manifest.Artifacts)...)
	read.Inventory.Families = adminMigrationSortedInventoryFamilies(read.Inventory.Families)
	read.Findings = append(read.Findings, adminMigrationSourceInventoryOnlyFinding())
	return read, nil
}

// adminMigrationSourceDirectoryEntries walks an extracted backup/export tree
// and records only relative path metadata, never file contents.
func adminMigrationSourceDirectoryEntries(root string) ([]adminMigrationSourceArtifactEntry, error) {
	var entries []adminMigrationSourceArtifactEntry
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		entries = append(entries, adminMigrationSourceArtifactEntry{
			Path:  filepath.ToSlash(relative),
			IsDir: d.IsDir(),
		})
		return nil
	})
	return entries, err
}

// adminMigrationSourceArchiveEntries scans a tar or tar.gz backup stream
// without extracting it, giving operators a safe first look at source content.
func adminMigrationSourceArchiveEntries(path string) ([]adminMigrationSourceArtifactEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var reader io.Reader = file
	var gz *gzip.Reader
	if adminMigrationSourceLooksLikeGzipArchive(path) {
		gz, err = gzip.NewReader(file)
		if err != nil {
			return nil, err
		}
		defer gz.Close()
		reader = gz
	}

	var entries []adminMigrationSourceArtifactEntry
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, adminMigrationSourceArtifactEntry{
			Path:  filepath.ToSlash(filepath.Clean(header.Name)),
			IsDir: header.FileInfo().IsDir(),
		})
	}
	return entries, nil
}

// adminMigrationSourceInventoryFromEntries converts discovered source paths
// into compatibility-family counts plus warnings for deferred source behavior.
func adminMigrationSourceInventoryFromEntries(sourceType, formatVersion string, entries []adminMigrationSourceArtifactEntry) adminMigrationSourceInventoryRead {
	counts := map[string]int{}
	orgCounts := map[string]map[string]int{}
	bookshelfChecksums := map[string]struct{}{}
	orgs := map[string]struct{}{}
	searchDetected := false
	databaseDetected := false
	unsupported := map[string]struct{}{}

	for _, entry := range entries {
		parts := adminMigrationSourcePathParts(entry.Path)
		if len(parts) == 0 {
			continue
		}
		if len(parts) >= 2 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
			orgs[parts[1]] = struct{}{}
		}
		if adminMigrationSourcePathContainsAny(parts, "elasticsearch", "opensearch", "opscode-solr4", "solr") {
			searchDetected = true
		}
		if adminMigrationSourcePathContainsAny(parts, "postgresql", "pgsql", "database", "pg_dump") || strings.HasSuffix(strings.ToLower(parts[len(parts)-1]), ".sql") {
			databaseDetected = true
		}
		if family := adminMigrationUnsupportedSourceFamily(parts[0]); family != "" {
			unsupported[family] = struct{}{}
		}
		if checksum, ok := adminMigrationSourceBookshelfChecksum(parts); ok && !entry.IsDir {
			bookshelfChecksums[checksum] = struct{}{}
		}
		if org, family, ok := adminMigrationSourceFamilyForEntry(parts, entry.IsDir); ok {
			if org != "" {
				orgs[org] = struct{}{}
				if orgCounts[org] == nil {
					orgCounts[org] = map[string]int{}
				}
				orgCounts[org][family]++
			} else {
				counts[family]++
			}
		}
	}
	if len(orgs) > counts["organizations"] {
		counts["organizations"] = len(orgs)
	}
	if len(bookshelfChecksums) > 0 {
		counts["cookbook_blob_references"] = len(bookshelfChecksums)
	}

	families := make([]adminMigrationInventoryFamily, 0, len(counts))
	for _, family := range adminMigrationSortedMapKeys(counts) {
		families = append(families, adminMigrationInventoryFamily{Family: family, Count: counts[family]})
	}
	for _, org := range adminMigrationSortedMapKeys(orgCounts) {
		for _, family := range adminMigrationSortedMapKeys(orgCounts[org]) {
			families = append(families, adminMigrationInventoryFamily{Organization: org, Family: family, Count: orgCounts[org][family]})
		}
	}

	findings := []adminMigrationFinding{adminMigrationSourceInventoryOnlyFinding()}
	if searchDetected {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_search_rebuild_required",
			Family:   "opensearch",
			Message:  "search index artifacts were detected; OpenSearch/Elasticsearch content is derived and should be rebuilt from restored PostgreSQL-backed state",
		})
	}
	if databaseDetected {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_database_artifact_deferred",
			Family:   "source_inventory",
			Message:  "raw Chef Server database artifacts were detected; this bucket inventories them but does not connect to or mutate live upstream databases",
		})
	}
	for _, family := range adminMigrationSortedMapKeys(unsupported) {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_artifact_unsupported",
			Family:   family,
			Message:  "source artifact family " + family + " is outside the first OpenCook import contract",
		})
	}
	return adminMigrationSourceInventoryRead{
		SourceType:    sourceType,
		FormatVersion: adminMigrationDefaultSourceType(formatVersion, "path-scan"),
		Inventory:     adminMigrationInventory{Families: adminMigrationSortedInventoryFamilies(families)},
		Findings:      findings,
	}
}

// adminMigrationSourceFamilyForEntry maps exported Chef object paths into the
// same family names used by OpenCook backup and preflight inventory output.
func adminMigrationSourceFamilyForEntry(parts []string, isDir bool) (string, string, bool) {
	orgName := ""
	index := 0
	if len(parts) >= 3 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
		orgName = parts[1]
		index = 2
	}
	segment := parts[index]
	if isDir {
		if adminMigrationSourceFamilyForSegment(segment) == "data_bag_items" && len(parts) > index+1 {
			return orgName, "data_bags", true
		}
		return "", "", false
	}
	if !adminMigrationSourceLooksLikeJSONFile(parts[len(parts)-1]) {
		return "", "", false
	}
	if adminMigrationSourceIsOrganizationsSegment(segment) {
		return "", "organizations", true
	}
	family := adminMigrationSourceFamilyForSegment(segment)
	if family == "" {
		return "", "", false
	}
	return orgName, family, true
}

// adminMigrationSourceFamilyForSegment recognizes common generated JSON and
// pedant-visible object directories used by existing Chef Server exports.
func adminMigrationSourceFamilyForSegment(segment string) string {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "users":
		return "users"
	case "user_acls":
		return "user_acls"
	case "user_keys":
		return "user_keys"
	case "clients":
		return "clients"
	case "client_keys":
		return "client_keys"
	case "groups":
		return "groups"
	case "containers":
		return "containers"
	case "acls":
		return "acls"
	case "nodes":
		return "nodes"
	case "roles":
		return "roles"
	case "environments":
		return "environments"
	case "data_bags", "data-bags":
		return "data_bag_items"
	case "policies", "policy_revisions":
		return "policy_revisions"
	case "policy_groups":
		return "policy_groups"
	case "sandboxes":
		return "sandboxes"
	case "cookbooks":
		return "cookbook_versions"
	case "cookbook_artifacts":
		return "cookbook_artifacts"
	default:
		return ""
	}
}

// adminMigrationManifestArtifactFamilies preserves artifact counts supplied by
// explicit source manifests when they represent blob or search side channels.
func adminMigrationManifestArtifactFamilies(artifacts []adminMigrationSourceManifestArtifact) []adminMigrationInventoryFamily {
	var families []adminMigrationInventoryFamily
	for _, artifact := range artifacts {
		family := strings.TrimSpace(artifact.Family)
		if family == "" || artifact.Count <= 0 {
			continue
		}
		switch family {
		case "bookshelf", "cookbook_blob_references":
			families = append(families, adminMigrationInventoryFamily{Family: "cookbook_blob_references", Count: artifact.Count})
		case "opensearch", "elasticsearch":
			families = append(families, adminMigrationInventoryFamily{Family: "opensearch_source_artifacts", Count: artifact.Count})
		}
	}
	return families
}

// adminMigrationManifestArtifactFindings surfaces unsupported source-family
// notes carried by a source manifest without treating them as parse failures.
func adminMigrationManifestArtifactFindings(artifacts []adminMigrationSourceManifestArtifact) []adminMigrationFinding {
	var findings []adminMigrationFinding
	for _, artifact := range artifacts {
		family := strings.TrimSpace(artifact.Family)
		if family == "" {
			continue
		}
		switch family {
		case "opensearch", "elasticsearch":
			findings = append(findings, adminMigrationFinding{
				Severity: "warning",
				Code:     "source_search_rebuild_required",
				Family:   "opensearch",
				Message:  "search index artifacts were declared; OpenSearch/Elasticsearch content is derived and should be rebuilt from restored PostgreSQL-backed state",
			})
		}
		if (artifact.Supported != nil && !*artifact.Supported) || artifact.Deferred {
			findings = append(findings, adminMigrationFinding{
				Severity: "warning",
				Code:     "source_artifact_unsupported",
				Family:   family,
				Message:  "source artifact family " + family + " is outside the first OpenCook import contract",
			})
		}
	}
	return findings
}

// adminMigrationSourceBookshelfChecksum detects checksum-addressed Bookshelf
// objects by path without opening blob payloads.
func adminMigrationSourceBookshelfChecksum(parts []string) (string, bool) {
	inBookshelf := false
	for _, part := range parts {
		lower := strings.ToLower(part)
		if lower == "bookshelf" || lower == "bookshelf-data" || lower == "bookshelf_data" {
			inBookshelf = true
			continue
		}
		if inBookshelf && bootstrap.ValidSandboxChecksum(part) {
			return strings.ToLower(part), true
		}
	}
	return "", false
}

// adminMigrationSortedInventoryFamilies provides deterministic JSON output for
// both manifest-declared and path-scanned source inventories.
func adminMigrationSortedInventoryFamilies(families []adminMigrationInventoryFamily) []adminMigrationInventoryFamily {
	out := append([]adminMigrationInventoryFamily(nil), families...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Organization != out[j].Organization {
			return out[i].Organization < out[j].Organization
		}
		if out[i].Family != out[j].Family {
			return out[i].Family < out[j].Family
		}
		return out[i].Count < out[j].Count
	})
	return out
}

// adminMigrationSourceInventoryOnlyFinding records the current boundary: this
// command inventories source artifacts, while broad import remains deferred.
func adminMigrationSourceInventoryOnlyFinding() adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "warning",
		Code:     "source_import_not_implemented",
		Family:   "source_inventory",
		Message:  "source artifact import is not implemented in this bucket; this command only inventories read-only Chef Server source content",
	}
}

// adminMigrationSourceErrorFinding standardizes source inventory parser errors
// without leaking local filesystem details.
func adminMigrationSourceErrorFinding(code, message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "error",
		Code:     code,
		Family:   "source_inventory",
		Message:  message,
	}
}

// adminMigrationDefaultSourceType fills optional manifest metadata so JSON
// consumers always receive a useful source_type or format_version detail.
func adminMigrationDefaultSourceType(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

// adminMigrationSourceLooksLikeJSONManifest identifies explicit generated
// source manifests by extension before falling back to path scanning.
func adminMigrationSourceLooksLikeJSONManifest(path string) bool {
	return strings.EqualFold(filepath.Ext(path), ".json")
}

// adminMigrationSourceLooksLikeArchive recognizes the Chef backup archive
// wrappers this inventory command can inspect without extraction.
func adminMigrationSourceLooksLikeArchive(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".tar") || adminMigrationSourceLooksLikeGzipArchive(path)
}

// adminMigrationSourceLooksLikeGzipArchive separates gzip-wrapped tar streams
// from plain tar streams so the reader can wrap them correctly.
func adminMigrationSourceLooksLikeGzipArchive(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasSuffix(lower, ".tgz") || strings.HasSuffix(lower, ".tar.gz")
}

// adminMigrationSourceLooksLikeJSONFile limits object-family counts to files
// that look like generated or API-captured JSON payloads.
func adminMigrationSourceLooksLikeJSONFile(name string) bool {
	return strings.EqualFold(filepath.Ext(name), ".json")
}

// adminMigrationSourcePathParts normalizes directory and archive paths to safe,
// slash-separated segments before family classification.
func adminMigrationSourcePathParts(path string) []string {
	cleaned := strings.Trim(filepath.ToSlash(filepath.Clean(path)), "/")
	if cleaned == "" || cleaned == "." {
		return nil
	}
	raw := strings.Split(cleaned, "/")
	parts := raw[:0]
	for _, part := range raw {
		if part != "" && part != "." && part != ".." {
			parts = append(parts, part)
		}
	}
	return parts
}

// adminMigrationSourcePathContainsAny checks path segments for provider or
// storage families whose import semantics are deferred or derived.
func adminMigrationSourcePathContainsAny(parts []string, names ...string) bool {
	wanted := map[string]struct{}{}
	for _, name := range names {
		wanted[strings.ToLower(name)] = struct{}{}
	}
	for _, part := range parts {
		if _, ok := wanted[strings.ToLower(part)]; ok {
			return true
		}
	}
	return false
}

// adminMigrationSourceIsOrganizationsSegment accepts the two common directory
// names used by generated org-scoped source fixtures.
func adminMigrationSourceIsOrganizationsSegment(segment string) bool {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "orgs", "organizations":
		return true
	default:
		return false
	}
}

// adminMigrationUnsupportedSourceFamily classifies known upstream ancillary
// components that OpenCook intentionally does not import in this contract.
func adminMigrationUnsupportedSourceFamily(segment string) string {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "oc-id", "oc_id":
		return "oc_id"
	case "license", "licenses", "license-server":
		return "licensing"
	case "analytics", "opscode-reporting", "reporting":
		return "reporting"
	case "rabbitmq", "redis", "nginx", "opscode-erchef", "erchef", "oc_bifrost", "oc-bifrost":
		return strings.ReplaceAll(strings.TrimSpace(segment), "-", "_")
	default:
		return ""
	}
}

// runAdminMigrationCutover dispatches cutover rehearsal commands that validate
// a live restored target without mutating Chef-facing state.
func (c *command) runAdminMigrationCutover(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration cutover requires rehearse\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	if args[0] != "rehearse" {
		return c.adminUsageError("unknown admin migration cutover command %q\n\n", args[0])
	}
	return c.runAdminMigrationCutoverRehearse(ctx, args[1:], inheritedJSON)
}

// runAdminMigrationCutoverRehearse validates a restored OpenCook target through
// read-only signed admin/API requests derived from the backup manifest.
func (c *command) runAdminMigrationCutoverRehearse(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration cutover rehearse", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.manifestPath, "manifest", "", "migration manifest path")
	fs.StringVar(&opts.serverURL, "server-url", "", "restored OpenCook server URL")
	fs.StringVar(&opts.requestorName, "requestor-name", "", "Chef requestor name used for signed rehearsal requests")
	fs.StringVar(&opts.requestorType, "requestor-type", "", "Chef requestor type used for signed rehearsal requests")
	fs.StringVar(&opts.privateKeyPath, "private-key", "", "path to the requestor private key PEM")
	fs.StringVar(&opts.defaultOrg, "default-org", "", "default organization for default-org compatibility checks")
	fs.StringVar(&opts.serverAPIVersion, "server-api-version", "", "X-Ops-Server-API-Version value for signed rehearsal requests")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin migration cutover rehearse", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin migration cutover rehearse received unexpected arguments: %v\n\n", fs.Args())
	}
	if strings.TrimSpace(opts.manifestPath) == "" {
		return c.adminUsageError("admin migration cutover rehearse requires --manifest PATH\n\n")
	}

	result := c.buildAdminMigrationCutoverRehearsal(ctx, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationCutoverRehearsal loads the backup bundle and runs
// representative read-only checks against the restored live OpenCook target.
func (c *command) buildAdminMigrationCutoverRehearsal(ctx context.Context, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	bundlePath, manifestPath := adminMigrationResolveManifestBundlePath(opts.manifestPath)
	out := adminMigrationCutoverRehearsalOutput(opts, manifestPath)

	manifest, findings, err := adminMigrationInspectBackupBundle(bundlePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationBackupBundleDependency("error", "backup manifest failed integrity inspection", nil))
		for _, finding := range findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationBackupInspectFinding("backup_bundle_invalid", "backup manifest failed integrity inspection"))
		}
		return out
	}
	out.Inventory = manifest.Inventory
	out.Warnings = append(out.Warnings, manifest.Warnings...)
	adminMigrationMarkDependency(&out, adminMigrationBackupBundleDependency("ok", "backup manifest and payload hashes are valid", map[string]string{
		"format_version": manifest.FormatVersion,
		"payloads":       fmt.Sprintf("%d", len(manifest.Payloads)),
	}))

	bootstrapState, coreObjectState, cookbooks, err := adminMigrationReadBackupStatePayloads(bundlePath)
	if err != nil {
		adminMigrationMarkFinding(&out, adminMigrationFinding{
			Severity: "error",
			Code:     "backup_payload_read_failed",
			Family:   "backup_bundle",
			Message:  "backup state payloads could not be read for cutover rehearsal",
		})
		return out
	}
	blobManifest, err := adminMigrationReadBackupBlobManifest(bundlePath)
	if err != nil {
		adminMigrationMarkFinding(&out, adminMigrationBackupInspectFinding("backup_blob_manifest_invalid", "backup blob manifest could not be parsed for cutover rehearsal"))
		return out
	}

	adminCfg := c.loadAdminMigrationRehearsalConfig(opts)
	out.Target.ServerURL = adminMigrationRedact(adminCfg.ServerURL)
	out.Config = adminMigrationRedactedAdminConfig(adminCfg)
	client, err := c.newAdmin(adminCfg)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "live_opencook",
			Status:     "error",
			Backend:    "http",
			Configured: strings.TrimSpace(adminCfg.ServerURL) != "",
			Message:    "restored OpenCook admin client could not be constructed",
		})
		return out
	}

	checks := adminMigrationCutoverRehearsalChecks(bootstrapState, coreObjectState, cookbooks, blobManifest)
	rehearsal := adminMigrationRunRehearsalChecks(ctx, client, checks)
	out.Inventory.Families = append(out.Inventory.Families, adminMigrationRehearsalInventoryFamilies(rehearsal)...)
	out.PlannedMutations = adminMigrationCutoverRehearsalRecommendations()
	if rehearsal.Failed > 0 {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "cutover_rehearsal",
			Status:     "error",
			Backend:    "http",
			Configured: true,
			Message:    "one or more live cutover rehearsal checks failed",
			Details:    adminMigrationRehearsalDependencyDetails(rehearsal),
		})
	} else {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "cutover_rehearsal",
			Status:     "ok",
			Backend:    "http",
			Configured: true,
			Message:    "live restored target passed read-only cutover rehearsal checks",
			Details:    adminMigrationRehearsalDependencyDetails(rehearsal),
		})
	}
	for _, finding := range rehearsal.Findings {
		adminMigrationMarkFinding(&out, finding)
	}
	adminMigrationMarkFinding(&out, adminMigrationFinding{
		Severity: "warning",
		Code:     "shadow_read_advisory",
		Family:   "shadow_read",
		Message:  "shadow-read comparisons should use read-only source Chef responses, normalize documented compatibility differences, and never proxy writes during cutover rehearsal",
	})
	return out
}

// adminMigrationCutoverRehearsalOutput initializes the shared JSON envelope for
// live cutover validation while keeping all future work read-only by default.
func adminMigrationCutoverRehearsalOutput(opts *adminMigrationFlagValues, manifestPath string) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_cutover_rehearse",
		Target:           adminMigrationTarget{ManifestPath: manifestPath, ServerURL: adminMigrationRedact(opts.serverURL)},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
}

// adminMigrationResolveManifestBundlePath accepts either a bundle directory or
// the manifest.json path operators naturally pass to cutover rehearsal.
func adminMigrationResolveManifestBundlePath(raw string) (string, string) {
	cleaned := filepath.Clean(strings.TrimSpace(raw))
	if filepath.Base(cleaned) == adminMigrationBackupManifestPath {
		return filepath.Dir(cleaned), cleaned
	}
	return cleaned, filepath.Join(cleaned, adminMigrationBackupManifestPath)
}

// loadAdminMigrationRehearsalConfig starts with normal admin configuration and
// applies cutover-specific flags parsed inside the migration namespace.
func (c *command) loadAdminMigrationRehearsalConfig(opts *adminMigrationFlagValues) admin.Config {
	cfg := c.loadAdminConfig()
	if strings.TrimSpace(opts.serverURL) != "" {
		cfg.ServerURL = strings.TrimSpace(opts.serverURL)
	}
	if strings.TrimSpace(opts.requestorName) != "" {
		cfg.RequestorName = strings.TrimSpace(opts.requestorName)
	}
	if strings.TrimSpace(opts.requestorType) != "" {
		cfg.RequestorType = strings.TrimSpace(opts.requestorType)
	}
	if strings.TrimSpace(opts.privateKeyPath) != "" {
		cfg.PrivateKeyPath = strings.TrimSpace(opts.privateKeyPath)
	}
	if strings.TrimSpace(opts.defaultOrg) != "" {
		cfg.DefaultOrg = strings.TrimSpace(opts.defaultOrg)
	}
	if strings.TrimSpace(opts.serverAPIVersion) != "" {
		cfg.ServerAPIVersion = strings.TrimSpace(opts.serverAPIVersion)
	}
	return cfg
}

// adminMigrationRedactedAdminConfig exposes enough rehearsal target context for
// operators without printing private key paths or signed download query strings.
func adminMigrationRedactedAdminConfig(cfg admin.Config) map[string]string {
	return map[string]string{
		"server_url":         adminMigrationRedact(cfg.ServerURL),
		"requestor_name":     cfg.RequestorName,
		"requestor_type":     cfg.RequestorType,
		"private_key":        adminMigrationPresence(cfg.PrivateKeyPath),
		"default_org":        cfg.DefaultOrg,
		"server_api_version": cfg.ServerAPIVersion,
	}
}

// adminMigrationCutoverRehearsalChecks derives a representative read set from
// restored backup state so rehearsal validates the same object families restored.
func adminMigrationCutoverRehearsalChecks(bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, cookbooks adminMigrationCookbookExport, blobManifest adminMigrationBackupBlobManifest) []adminMigrationRehearsalCheck {
	checks := []adminMigrationRehearsalCheck{
		{Family: "status", Name: "_status", Method: http.MethodGet, Path: "/_status"},
		{Family: "status", Name: "readyz", Method: http.MethodGet, Path: "/readyz"},
		{Family: "server_api_version", Name: "server_api_version", Method: http.MethodGet, Path: "/server_api_version"},
	}
	for _, username := range adminMigrationSortedMapKeys(bootstrapState.Users) {
		checks = append(checks,
			adminMigrationReadCheck("users", username, adminPath("users", username)),
			adminMigrationReadCheck("user_keys", username, adminPath("users", username, "keys")),
			adminMigrationReadCheck("user_acls", username, adminPath("users", username, "_acl")),
		)
		break
	}
	copiedChecksums := adminMigrationCopiedBackupBlobChecksums(blobManifest)
	for _, orgName := range adminMigrationOrgNames(bootstrapState, coreObjectState, adminMigrationCookbookInventoryFromExport(cookbooks), "") {
		bootstrapOrg := bootstrapState.Orgs[orgName]
		coreOrg := coreObjectState.Orgs[orgName]
		cookbookOrg := cookbooks.Orgs[orgName]
		checks = append(checks,
			adminMigrationReadCheck("organizations", orgName, adminPath("organizations", orgName)),
			adminMigrationReadCheck("organization_acls", orgName, adminPath("organizations", orgName, "_acl")),
		)
		checks = append(checks, adminMigrationFirstOrgBootstrapChecks(orgName, bootstrapOrg)...)
		checks = append(checks, adminMigrationFirstCoreObjectChecks(orgName, coreOrg)...)
		checks = append(checks, adminMigrationFirstCookbookChecks(orgName, cookbookOrg, copiedChecksums)...)
		if searchIndex, ok := adminMigrationSearchIndexForOrg(bootstrapOrg, coreOrg); ok {
			checks = append(checks, adminMigrationReadCheck("search", searchIndex, "/organizations/"+url.PathEscape(orgName)+"/search/"+url.PathEscape(searchIndex)+"?q=*:*"))
		}
	}
	return checks
}

// adminMigrationFirstOrgBootstrapChecks returns one representative client,
// group, container, and ACL read for an organization when those rows exist.
func adminMigrationFirstOrgBootstrapChecks(orgName string, org bootstrap.BootstrapCoreOrganizationState) []adminMigrationRehearsalCheck {
	var checks []adminMigrationRehearsalCheck
	if clientName, ok := adminMigrationFirstMapKey(org.Clients); ok {
		checks = append(checks,
			adminMigrationReadCheck("clients", clientName, adminPath("organizations", orgName, "clients", clientName)),
			adminMigrationReadCheck("client_keys", clientName, adminPath("organizations", orgName, "clients", clientName, "keys")),
			adminMigrationReadCheck("client_acls", clientName, adminPath("organizations", orgName, "clients", clientName, "_acl")),
		)
	}
	if groupName, ok := adminMigrationFirstMapKey(org.Groups); ok {
		checks = append(checks, adminMigrationReadCheck("groups", groupName, adminPath("organizations", orgName, "groups", groupName)))
	}
	if containerName, ok := adminMigrationFirstMapKey(org.Containers); ok {
		checks = append(checks,
			adminMigrationReadCheck("containers", containerName, adminPath("organizations", orgName, "containers", containerName)),
			adminMigrationReadCheck("container_acls", containerName, adminPath("organizations", orgName, "containers", containerName, "_acl")),
		)
	}
	return checks
}

// adminMigrationFirstCoreObjectChecks selects one route per restored core
// object family so rehearsal remains fast while still covering every surface.
func adminMigrationFirstCoreObjectChecks(orgName string, org bootstrap.CoreObjectOrganizationState) []adminMigrationRehearsalCheck {
	var checks []adminMigrationRehearsalCheck
	if name, ok := adminMigrationFirstMapKey(org.Nodes); ok {
		checks = append(checks, adminMigrationReadCheck("nodes", name, adminPath("organizations", orgName, "nodes", name)))
	}
	if name, ok := adminMigrationFirstMapKey(org.Environments); ok {
		checks = append(checks, adminMigrationReadCheck("environments", name, adminPath("organizations", orgName, "environments", name)))
	}
	if name, ok := adminMigrationFirstMapKey(org.Roles); ok {
		checks = append(checks, adminMigrationReadCheck("roles", name, adminPath("organizations", orgName, "roles", name)))
	}
	if name, ok := adminMigrationFirstMapKey(org.DataBags); ok {
		checks = append(checks, adminMigrationReadCheck("data_bags", name, adminPath("organizations", orgName, "data", name)))
	}
	if bagName, itemName, ok := adminMigrationFirstNestedMapKey(org.DataBagItems); ok {
		checks = append(checks, adminMigrationReadCheck("data_bag_items", bagName+"/"+itemName, adminPath("organizations", orgName, "data", bagName, itemName)))
	}
	if policyName, revisionID, ok := adminMigrationFirstNestedMapKey(org.Policies); ok {
		checks = append(checks, adminMigrationReadCheck("policy_revisions", policyName+"/"+revisionID, adminPath("organizations", orgName, "policies", policyName, "revisions", revisionID)))
	}
	if name, ok := adminMigrationFirstMapKey(org.PolicyGroups); ok {
		checks = append(checks, adminMigrationReadCheck("policy_groups", name, adminPath("organizations", orgName, "policy_groups", name)))
	}
	if id, ok := adminMigrationFirstMapKey(org.Sandboxes); ok {
		checks = append(checks, adminMigrationReadCheck("sandboxes", id, adminPath("organizations", orgName, "sandboxes", id)))
	}
	return checks
}

// adminMigrationFirstCookbookChecks covers cookbook and artifact reads, plus a
// signed blob download when the backup contains copied checksum bytes.
func adminMigrationFirstCookbookChecks(orgName string, org adminMigrationCookbookOrgExport, copiedChecksums map[string]struct{}) []adminMigrationRehearsalCheck {
	var checks []adminMigrationRehearsalCheck
	if len(org.Versions) > 0 {
		version := org.Versions[0]
		cookbookName := adminMigrationCookbookRouteName(version)
		check := adminMigrationReadCheck("cookbook_versions", cookbookName+"/"+version.Version, adminPath("organizations", orgName, "cookbooks", cookbookName, version.Version))
		check.DownloadChecksums = adminMigrationCopiedChecksumsFromFiles(version.AllFiles, copiedChecksums)
		check.RequireBlobContent = len(check.DownloadChecksums) > 0
		checks = append(checks, check)
	}
	if len(org.Artifacts) > 0 {
		artifact := org.Artifacts[0]
		check := adminMigrationReadCheck("cookbook_artifacts", artifact.Name, adminPath("organizations", orgName, "cookbook_artifacts", artifact.Name, artifact.Identifier))
		check.DownloadChecksums = adminMigrationCopiedChecksumsFromFiles(artifact.AllFiles, copiedChecksums)
		check.RequireBlobContent = len(check.DownloadChecksums) > 0
		checks = append(checks, check)
	}
	return checks
}

// adminMigrationCookbookRouteName returns the canonical cookbook route segment
// for a persisted version. PostgreSQL decodes CookbookVersion.Name as Chef's
// full display name (for example "nginx-1.2.3"), while cookbook HTTP routes use
// CookbookName ("nginx").
func adminMigrationCookbookRouteName(version bootstrap.CookbookVersion) string {
	if name := strings.TrimSpace(version.CookbookName); name != "" {
		return name
	}
	name := strings.TrimSpace(version.Name)
	versionSuffix := "-" + strings.TrimSpace(version.Version)
	if versionSuffix != "-" && strings.HasSuffix(name, versionSuffix) {
		return strings.TrimSuffix(name, versionSuffix)
	}
	return name
}

// adminMigrationRunRehearsalChecks executes signed JSON reads and optional
// unsigned blob downloads, returning only stable counts and redacted findings.
func adminMigrationRunRehearsalChecks(ctx context.Context, client adminJSONClient, checks []adminMigrationRehearsalCheck) adminMigrationRehearsalResult {
	result := adminMigrationRehearsalResult{Checks: len(checks)}
	for _, check := range checks {
		if strings.TrimSpace(check.Path) == "" {
			result.Skipped++
			continue
		}
		var payload any
		if err := client.DoJSON(ctx, check.Method, check.Path, nil, &payload); err != nil {
			result.Failed++
			result.Findings = append(result.Findings, adminMigrationRehearsalFailureFinding("cutover_rehearsal_check_failed", check, "live read failed during cutover rehearsal"))
			continue
		}
		result.Passed++
		if check.RequireBlobContent {
			downloaded, findings := adminMigrationValidateRehearsalDownloads(ctx, client, check, payload)
			result.Downloads += downloaded
			if len(findings) > 0 {
				result.Failed += len(findings)
				result.Findings = append(result.Findings, findings...)
			}
		}
	}
	return result
}

// adminMigrationValidateRehearsalDownloads follows signed download URLs from a
// successful cookbook/artifact payload when the backup contains copied bytes.
func adminMigrationValidateRehearsalDownloads(ctx context.Context, client adminJSONClient, check adminMigrationRehearsalCheck, payload any) (int, []adminMigrationFinding) {
	downloader, ok := client.(adminMigrationUnsignedClient)
	if !ok {
		return 0, []adminMigrationFinding{adminMigrationRehearsalFailureFinding("cutover_download_unavailable", check, "admin client cannot follow signed blob download URLs during rehearsal")}
	}
	var findings []adminMigrationFinding
	downloaded := 0
	for _, checksum := range check.DownloadChecksums {
		rawURL := adminMigrationFindDownloadURLForChecksum(payload, checksum)
		if rawURL == "" {
			findings = append(findings, adminMigrationRehearsalFailureFinding("cutover_download_url_missing", check, "restored cookbook payload did not include a signed blob download URL"))
			continue
		}
		response, err := downloader.DoUnsigned(ctx, http.MethodGet, rawURL)
		if err != nil {
			findings = append(findings, adminMigrationRehearsalFailureFinding("cutover_download_failed", check, "signed blob download failed during cutover rehearsal"))
			continue
		}
		if adminMigrationMD5Hex(response.Body) != checksum {
			findings = append(findings, adminMigrationRehearsalFailureFinding("cutover_download_checksum_mismatch", check, "signed blob download body did not match its Chef checksum"))
			continue
		}
		downloaded++
		break
	}
	return downloaded, findings
}

// adminMigrationFindDownloadURLForChecksum walks arbitrary cookbook payloads
// looking for the signed blob URL associated with a copied checksum.
func adminMigrationFindDownloadURLForChecksum(value any, checksum string) string {
	switch typed := value.(type) {
	case map[string]any:
		if rawChecksum, _ := typed["checksum"].(string); strings.EqualFold(strings.TrimSpace(rawChecksum), checksum) {
			if rawURL, _ := typed["url"].(string); strings.TrimSpace(rawURL) != "" {
				return rawURL
			}
		}
		for _, nested := range typed {
			if rawURL := adminMigrationFindDownloadURLForChecksum(nested, checksum); rawURL != "" {
				return rawURL
			}
		}
	case []any:
		for _, nested := range typed {
			if rawURL := adminMigrationFindDownloadURLForChecksum(nested, checksum); rawURL != "" {
				return rawURL
			}
		}
	}
	return ""
}

// adminMigrationRehearsalFailureFinding produces route-scoped but secret-free
// failure details for live cutover rehearsal checks.
func adminMigrationRehearsalFailureFinding(code string, check adminMigrationRehearsalCheck, message string) adminMigrationFinding {
	if strings.TrimSpace(check.Name) != "" {
		message += " for " + check.Family + " " + check.Name
	}
	return adminMigrationFinding{
		Severity: "error",
		Code:     code,
		Family:   check.Family,
		Message:  message,
	}
}

// adminMigrationRehearsalInventoryFamilies appends rehearsal-specific counters
// to the restored object inventory so automation can gate cutover readiness.
func adminMigrationRehearsalInventoryFamilies(result adminMigrationRehearsalResult) []adminMigrationInventoryFamily {
	return []adminMigrationInventoryFamily{
		{Family: "rehearsal_checks", Count: result.Checks},
		{Family: "rehearsal_passed", Count: result.Passed},
		{Family: "rehearsal_failed", Count: result.Failed},
		{Family: "rehearsal_skipped", Count: result.Skipped},
		{Family: "rehearsal_downloads", Count: result.Downloads},
	}
}

// adminMigrationRehearsalDependencyDetails mirrors the rehearsal inventory in
// string form for operators scanning the dependency block.
func adminMigrationRehearsalDependencyDetails(result adminMigrationRehearsalResult) map[string]string {
	return map[string]string{
		"checks":    fmt.Sprintf("%d", result.Checks),
		"passed":    fmt.Sprintf("%d", result.Passed),
		"failed":    fmt.Sprintf("%d", result.Failed),
		"skipped":   fmt.Sprintf("%d", result.Skipped),
		"downloads": fmt.Sprintf("%d", result.Downloads),
	}
}

// adminMigrationCutoverRehearsalRecommendations records the safe next steps
// after live target rehearsal, including the non-proxying shadow-read phase.
func adminMigrationCutoverRehearsalRecommendations() []adminMigrationPlannedMutation {
	return []adminMigrationPlannedMutation{
		{Action: "shadow_read_compare", Family: "source_target", Message: "compare read-only source Chef responses to restored OpenCook responses with documented compatibility normalizers"},
		{Action: "client_cutover", Family: "runbook", Message: "switch Chef/Cinc clients only after rehearsal, reindex checks, and shadow-read comparisons pass"},
		{Action: "rollback_ready", Family: "runbook", Message: "keep source Chef Infra Server read/write path available until post-cutover smoke checks pass"},
	}
}

// adminMigrationReadCheck creates a single signed GET rehearsal check with the
// common family/name/path fields populated consistently.
func adminMigrationReadCheck(family, name, path string) adminMigrationRehearsalCheck {
	return adminMigrationRehearsalCheck{
		Family: family,
		Name:   name,
		Method: http.MethodGet,
		Path:   path,
	}
}

// adminMigrationFirstMapKey returns the first stable key from a map, avoiding
// large rehearsal sweeps while keeping representative reads deterministic.
func adminMigrationFirstMapKey[T any](values map[string]T) (string, bool) {
	keys := adminMigrationSortedMapKeys(values)
	if len(keys) == 0 {
		return "", false
	}
	return keys[0], true
}

// adminMigrationFirstNestedMapKey returns the first stable outer/inner pair for
// nested object maps such as data bag items and policy revisions.
func adminMigrationFirstNestedMapKey[T any](values map[string]map[string]T) (string, string, bool) {
	for _, outer := range adminMigrationSortedMapKeys(values) {
		if inner, ok := adminMigrationFirstMapKey(values[outer]); ok {
			return outer, inner, true
		}
	}
	return "", "", false
}

// adminMigrationCopiedBackupBlobChecksums records the checksum byte payloads
// available in a backup so rehearsal can prove signed download usability.
func adminMigrationCopiedBackupBlobChecksums(blobManifest adminMigrationBackupBlobManifest) map[string]struct{} {
	out := map[string]struct{}{}
	for _, copy := range blobManifest.Copied {
		checksum := strings.ToLower(strings.TrimSpace(copy.Checksum))
		if checksum != "" {
			out[checksum] = struct{}{}
		}
	}
	return out
}

// adminMigrationCopiedChecksumsFromFiles filters cookbook file checksums to
// those whose bytes are actually present in the migration backup bundle.
func adminMigrationCopiedChecksumsFromFiles(files []bootstrap.CookbookFile, copied map[string]struct{}) []string {
	var checksums []string
	seen := map[string]struct{}{}
	for _, file := range files {
		checksum := strings.ToLower(strings.TrimSpace(file.Checksum))
		if checksum == "" {
			continue
		}
		if _, ok := copied[checksum]; !ok {
			continue
		}
		if _, ok := seen[checksum]; ok {
			continue
		}
		seen[checksum] = struct{}{}
		checksums = append(checksums, checksum)
	}
	sort.Strings(checksums)
	return checksums
}

// adminMigrationSearchIndexForOrg selects the first implemented search index
// that should contain documents after the operator has completed reindexing.
func adminMigrationSearchIndexForOrg(bootstrapOrg bootstrap.BootstrapCoreOrganizationState, coreOrg bootstrap.CoreObjectOrganizationState) (string, bool) {
	switch {
	case len(coreOrg.Nodes) > 0:
		return "node", true
	case len(coreOrg.Roles) > 0:
		return "role", true
	case len(coreOrg.Environments) > 0:
		return "environment", true
	case len(bootstrapOrg.Clients) > 0:
		return "client", true
	case len(coreOrg.DataBags) > 0:
		if name, ok := adminMigrationFirstMapKey(coreOrg.DataBags); ok {
			return name, true
		}
	}
	return "", false
}

// bindAdminMigrationCommonFlags attaches the shared migration flags so every
// subcommand accepts a consistent JSON, timing, safety, and provider override vocabulary.
func bindAdminMigrationCommonFlags(fs *flag.FlagSet, inheritedJSON bool) *adminMigrationFlagValues {
	opts := &adminMigrationFlagValues{}
	fs.BoolVar(&opts.jsonOutput, "json", inheritedJSON, "print JSON output")
	fs.BoolVar(&opts.withTiming, "with-timing", false, "include duration_ms in output")
	fs.BoolVar(&opts.dryRun, "dry-run", false, "report planned mutations without writing")
	fs.BoolVar(&opts.offline, "offline", false, "allow direct PostgreSQL access while OpenCook servers are stopped")
	fs.BoolVar(&opts.yes, "yes", false, "confirm the migration mutation")
	fs.StringVar(&opts.postgresDSN, "postgres-dsn", "", "PostgreSQL DSN; defaults to OPENCOOK_POSTGRES_DSN")
	fs.StringVar(&opts.openSearchURL, "opensearch-url", "", "OpenSearch URL; defaults to OPENCOOK_OPENSEARCH_URL")
	fs.StringVar(&opts.blobBackend, "blob-backend", "", "blob backend override; defaults to OPENCOOK_BLOB_BACKEND")
	fs.StringVar(&opts.blobStorageURL, "blob-storage-url", "", "blob storage URL override; defaults to OPENCOOK_BLOB_STORAGE_URL")
	fs.StringVar(&opts.blobS3Endpoint, "blob-s3-endpoint", "", "S3-compatible endpoint override")
	fs.StringVar(&opts.blobS3Region, "blob-s3-region", "", "S3-compatible region override")
	fs.StringVar(&opts.blobS3AccessKeyID, "blob-s3-access-key-id", "", "S3-compatible access key override")
	fs.StringVar(&opts.blobS3SecretKey, "blob-s3-secret-access-key", "", "S3-compatible secret key override")
	fs.StringVar(&opts.blobS3SessionToken, "blob-s3-session-token", "", "S3-compatible session token override")
	return opts
}

// bindAdminMigrationScopeFlags attaches org scoping flags shared by commands
// that can validate either one organization or the whole installation.
func bindAdminMigrationScopeFlags(fs *flag.FlagSet, opts *adminMigrationFlagValues) {
	fs.StringVar(&opts.orgName, "org", "", "organization to validate")
	fs.BoolVar(&opts.allOrgs, "all-orgs", false, "validate all organizations")
}

// validateAdminMigrationScope keeps org scoping compatible with existing admin
// commands by rejecting ambiguous one-org plus all-org requests.
func validateAdminMigrationScope(opts *adminMigrationFlagValues) (int, bool) {
	if opts.allOrgs && strings.TrimSpace(opts.orgName) != "" {
		return exitUsage, false
	}
	return exitOK, true
}

// splitAdminMigrationPathArg accepts the documented PATH-before-flags command
// shape while still allowing flag-first invocations handled directly by flag.Parse.
func splitAdminMigrationPathArg(args []string) (string, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", args
	}
	return args[0], args[1:]
}

// adminMigrationPostgresRead activates the PostgreSQL store through the
// existing offline seam and returns both dependency status and read-only state
// snapshots for inventory validation.
func (c *command) adminMigrationPostgresRead(ctx context.Context, cfg config.Config) adminMigrationPostgresRead {
	if strings.TrimSpace(cfg.PostgresDSN) == "" {
		return adminMigrationPostgresRead{Dependencies: []adminMigrationDependency{{
			Name:       "postgres",
			Status:     "error",
			Backend:    "postgres",
			Configured: false,
			Message:    "PostgreSQL is not configured; set OPENCOOK_POSTGRES_DSN or --postgres-dsn",
		}}}
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return adminMigrationPostgresRead{Dependencies: []adminMigrationDependency{{
			Name:       "postgres",
			Status:     "error",
			Backend:    "postgres",
			Configured: true,
			Message:    adminMigrationSafeErrorMessage("postgres", err),
		}}}
	}
	defer closeOfflineStore(closeStore)

	deps := []adminMigrationDependency{{
		Name:       "postgres",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "PostgreSQL is configured, reachable, and activated for migration reads",
		Details: map[string]string{
			"cookbook_persistence":       "active",
			"bootstrap_core_persistence": "active",
			"core_object_persistence":    "active",
		},
	}}

	var bootstrapState bootstrap.BootstrapCoreState
	var coreObjectState bootstrap.CoreObjectState
	bootstrapLoaded := false
	coreObjectsLoaded := false
	if state, err := store.LoadBootstrapCore(); err != nil {
		deps = append(deps, adminMigrationPostgresFamilyDependency("postgres_bootstrap_core", "error", adminMigrationSafeErrorMessage("postgres_bootstrap_core", err)))
	} else {
		bootstrapState = state
		bootstrapLoaded = true
		deps = append(deps, adminMigrationPostgresFamilyDependency("postgres_bootstrap_core", "ok", "bootstrap core state loaded from PostgreSQL-backed persistence"))
	}
	if state, err := store.LoadCoreObjects(); err != nil {
		deps = append(deps, adminMigrationPostgresFamilyDependency("postgres_core_objects", "error", adminMigrationSafeErrorMessage("postgres_core_objects", err)))
	} else {
		coreObjectState = state
		coreObjectsLoaded = true
		deps = append(deps, adminMigrationPostgresFamilyDependency("postgres_core_objects", "ok", "core object state loaded from PostgreSQL-backed persistence"))
	}

	result := adminMigrationPostgresRead{
		Dependencies: deps,
		Bootstrap:    bootstrapState,
		CoreObjects:  coreObjectState,
		Loaded:       bootstrapLoaded && coreObjectsLoaded,
	}
	if result.Loaded {
		if loader, ok := store.(adminMigrationCookbookInventoryLoader); ok {
			cookbookInventory, err := loader.LoadCookbookInventory(adminMigrationOrgNames(bootstrapState, coreObjectState, nil, ""))
			if err != nil {
				result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "error", adminMigrationSafeErrorMessage("postgres_cookbooks", err)))
			} else {
				result.CookbookInventory = cookbookInventory
				result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "ok", "cookbook inventory loaded from PostgreSQL-backed persistence"))
			}
		} else {
			result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "ok", "cookbook persistence activation completed through the PostgreSQL store startup path"))
		}
	} else {
		result.Dependencies = append(result.Dependencies, adminMigrationPostgresFamilyDependency("postgres_cookbooks", "ok", "cookbook persistence activation completed through the PostgreSQL store startup path"))
	}
	return result
}

// adminMigrationPostgresFamilyDependency records an individual persisted state
// family using the same dependency envelope as external provider checks.
func adminMigrationPostgresFamilyDependency(name, status, message string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       name,
		Status:     status,
		Backend:    "postgres",
		Configured: true,
		Message:    message,
	}
}

// adminMigrationInventoryFromState turns loaded PostgreSQL-backed OpenCook
// state into stable family counts that scripts can compare before a cutover.
func adminMigrationInventoryFromState(bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, cookbookInventory map[string]adminMigrationCookbookInventory, orgFilter string) adminMigrationInventory {
	families := []adminMigrationInventoryFamily{
		{Family: "users", Count: len(bootstrapState.Users)},
		{Family: "user_acls", Count: len(bootstrapState.UserACLs)},
		{Family: "user_keys", Count: adminMigrationNestedKeyCount(bootstrapState.UserKeys)},
		{Family: "organizations", Count: adminMigrationOrganizationCount(bootstrapState, coreObjectState, cookbookInventory, orgFilter)},
		{Family: "server_admin_memberships", Count: len(bootstrap.ListBootstrapServerAdmins(bootstrapState))},
	}

	for _, orgName := range adminMigrationOrgNames(bootstrapState, coreObjectState, cookbookInventory, orgFilter) {
		bootstrapOrg := bootstrapState.Orgs[orgName]
		coreOrg := coreObjectState.Orgs[orgName]
		cookbookOrg := cookbookInventory[orgName]
		checksumReferences := adminMigrationSandboxChecksumReferenceCount(coreOrg.Sandboxes) + adminMigrationCookbookInventoryReferenceCount(cookbookOrg)
		families = append(families,
			adminMigrationInventoryFamily{Organization: orgName, Family: "clients", Count: len(bootstrapOrg.Clients)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "client_keys", Count: adminMigrationNestedKeyCount(bootstrapOrg.ClientKeys)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "groups", Count: len(bootstrapOrg.Groups)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "group_memberships", Count: adminMigrationGroupMembershipCount(bootstrapOrg.Groups)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "containers", Count: len(bootstrapOrg.Containers)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "acls", Count: len(bootstrapOrg.ACLs) + len(coreOrg.ACLs)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "nodes", Count: len(coreOrg.Nodes)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "environments", Count: len(coreOrg.Environments)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "roles", Count: len(coreOrg.Roles)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "data_bags", Count: len(coreOrg.DataBags)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "data_bag_items", Count: adminMigrationDataBagItemCount(coreOrg.DataBagItems)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "policy_revisions", Count: adminMigrationPolicyRevisionCount(coreOrg.Policies)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "policy_groups", Count: len(coreOrg.PolicyGroups)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "policy_assignments", Count: adminMigrationPolicyAssignmentCount(coreOrg.PolicyGroups)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "sandboxes", Count: len(coreOrg.Sandboxes)},
			adminMigrationInventoryFamily{Organization: orgName, Family: "checksum_references", Count: checksumReferences},
			adminMigrationInventoryFamily{Organization: orgName, Family: "cookbook_versions", Count: cookbookOrg.Versions},
			adminMigrationInventoryFamily{Organization: orgName, Family: "cookbook_artifacts", Count: cookbookOrg.Artifacts},
		)
	}

	return adminMigrationInventory{Families: families}
}

// adminMigrationCookbookInventoryReferenceCount preserves repository-provided
// reference occurrence counts while falling back to the loaded checksum list.
func adminMigrationCookbookInventoryReferenceCount(inventory adminMigrationCookbookInventory) int {
	if inventory.ChecksumReferences > 0 {
		return inventory.ChecksumReferences
	}
	return len(inventory.Checksums)
}

// adminMigrationStateFindings validates compatibility invariants that should
// already hold for PostgreSQL-backed OpenCook state before backup/cutover work.
func adminMigrationStateFindings(cfg config.Config, bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, cookbookInventory map[string]adminMigrationCookbookInventory, orgFilter string) []adminMigrationFinding {
	var findings []adminMigrationFinding
	if err := bootstrap.NewService(nil, bootstrap.Options{
		SuperuserName:             cfg.BootstrapRequestorName,
		InitialBootstrapCoreState: &bootstrapState,
		InitialCoreObjectState:    &coreObjectState,
	}).RehydrateKeyStore(); err != nil {
		findings = append(findings, adminMigrationFinding{
			Severity: "error",
			Code:     "actor_keys_unloadable",
			Family:   "keys",
			Message:  "persisted actor keys could not be loaded into the request verifier cache",
		})
	}

	for _, username := range adminMigrationSortedMapKeys(bootstrapState.Users) {
		if _, ok := bootstrapState.UserACLs[username]; !ok {
			findings = append(findings, adminMigrationFinding{
				Severity: "error",
				Code:     "missing_user_acl",
				Family:   "user_acls",
				Message:  "user " + username + " is missing its ACL document",
			})
		}
	}

	for _, orgName := range adminMigrationOrgNames(bootstrapState, coreObjectState, cookbookInventory, orgFilter) {
		bootstrapOrg, hasBootstrapOrg := bootstrapState.Orgs[orgName]
		coreOrg := coreObjectState.Orgs[orgName]
		if !hasBootstrapOrg {
			findings = append(findings, adminMigrationFinding{
				Severity:     "error",
				Code:         "missing_bootstrap_org",
				Organization: orgName,
				Family:       "organizations",
				Message:      "organization " + orgName + " has persisted object state but no bootstrap core row",
			})
			continue
		}
		findings = append(findings, adminMigrationBootstrapOrgFindings(orgName, bootstrapOrg)...)
		findings = append(findings, adminMigrationCoreObjectFindings(orgName, bootstrapOrg, coreOrg)...)
	}
	findings = append(findings, adminMigrationDeferredSourceFindings()...)
	return findings
}

// adminMigrationBootstrapOrgFindings checks the organization-local identity and
// authorization rows that every Chef-compatible organization depends on.
func adminMigrationBootstrapOrgFindings(orgName string, org bootstrap.BootstrapCoreOrganizationState) []adminMigrationFinding {
	var findings []adminMigrationFinding
	for _, groupName := range adminMigrationRequiredDefaultGroups() {
		if _, ok := org.Groups[groupName]; !ok {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_default_group", "groups", "organization "+orgName+" is missing default group "+groupName))
		}
		if !adminMigrationHasACL(org.ACLs, adminMigrationGroupACLKey(groupName)) {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_default_group_acl", "acls", "organization "+orgName+" is missing ACL for default group "+groupName))
		}
	}
	for _, containerName := range adminMigrationRequiredDefaultContainers() {
		if _, ok := org.Containers[containerName]; !ok {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_default_container", "containers", "organization "+orgName+" is missing default container "+containerName))
		}
		if !adminMigrationHasACL(org.ACLs, adminMigrationContainerACLKey(containerName)) {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_default_container_acl", "acls", "organization "+orgName+" is missing ACL for default container "+containerName))
		}
	}
	if !adminMigrationHasACL(org.ACLs, adminMigrationOrganizationACLKey()) {
		findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_organization_acl", "acls", "organization "+orgName+" is missing its organization ACL"))
	}
	for _, clientName := range adminMigrationSortedMapKeys(org.Clients) {
		if !adminMigrationHasACL(org.ACLs, adminMigrationClientACLKey(clientName)) {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_client_acl", "acls", "client "+clientName+" is missing its ACL document"))
		}
	}

	validatorName := orgName + "-validator"
	validator, ok := org.Clients[validatorName]
	if !ok || !validator.Validator {
		findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_validator_client", "clients", "organization "+orgName+" is missing its validator client"))
	}
	if len(org.ClientKeys[validatorName]) == 0 {
		findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_validator_key", "client_keys", "organization "+orgName+" is missing a validator client key"))
	}
	return findings
}

// adminMigrationCoreObjectFindings checks ACL and checksum metadata invariants
// for persisted core objects without attempting provider/blob content reads.
func adminMigrationCoreObjectFindings(orgName string, bootstrapOrg bootstrap.BootstrapCoreOrganizationState, coreOrg bootstrap.CoreObjectOrganizationState) []adminMigrationFinding {
	var findings []adminMigrationFinding
	for _, name := range adminMigrationSortedMapKeys(coreOrg.Environments) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "environment", name, adminMigrationEnvironmentACLKey(name))...)
	}
	for _, name := range adminMigrationSortedMapKeys(coreOrg.Nodes) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "node", name, adminMigrationNodeACLKey(name))...)
	}
	for _, name := range adminMigrationSortedMapKeys(coreOrg.Roles) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "role", name, adminMigrationRoleACLKey(name))...)
	}
	for _, name := range adminMigrationSortedMapKeys(coreOrg.DataBags) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "data_bag", name, adminMigrationDataBagACLKey(name))...)
	}
	for _, name := range adminMigrationSortedMapKeys(coreOrg.Policies) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "policy", name, adminMigrationPolicyACLKey(name))...)
	}
	for _, name := range adminMigrationSortedMapKeys(coreOrg.PolicyGroups) {
		findings = append(findings, adminMigrationMissingObjectACLFindings(orgName, bootstrapOrg, coreOrg, "policy_group", name, adminMigrationPolicyGroupACLKey(name))...)
	}
	for _, id := range adminMigrationSortedMapKeys(coreOrg.Sandboxes) {
		sandbox := coreOrg.Sandboxes[id]
		if strings.TrimSpace(sandbox.ID) == "" {
			findings = append(findings, adminMigrationMissingOrgFinding(orgName, "missing_sandbox_id", "sandboxes", "a persisted sandbox row is missing its sandbox id"))
		}
		for _, checksum := range sandbox.Checksums {
			if !bootstrap.ValidSandboxChecksum(checksum) {
				findings = append(findings, adminMigrationMissingOrgFinding(orgName, "invalid_checksum_reference", "checksum_references", "sandbox "+id+" contains an invalid checksum reference"))
			}
		}
	}
	return findings
}

// adminMigrationMissingObjectACLFindings emits a single finding when a
// persisted core object lacks the ACL row required by existing Chef routes.
func adminMigrationMissingObjectACLFindings(orgName string, bootstrapOrg bootstrap.BootstrapCoreOrganizationState, coreOrg bootstrap.CoreObjectOrganizationState, family, name, aclKey string) []adminMigrationFinding {
	if adminMigrationHasACL(coreOrg.ACLs, aclKey) || adminMigrationHasACL(bootstrapOrg.ACLs, aclKey) {
		return nil
	}
	return []adminMigrationFinding{adminMigrationMissingOrgFinding(orgName, "missing_object_acl", "acls", family+" "+name+" is missing its ACL document")}
}

// adminMigrationDeferredSourceFindings makes unsupported migration source
// families visible so operators do not mistake omission for validated parity.
func adminMigrationDeferredSourceFindings() []adminMigrationFinding {
	return []adminMigrationFinding{{
		Severity: "warning",
		Code:     "unsupported_source_families_deferred",
		Family:   "source_inventory",
		Message:  "live Chef Infra Server source import and unsupported ancillary source families are deferred; this preflight validates the OpenCook PostgreSQL target state",
	}}
}

// adminMigrationMissingOrgFinding builds a consistency error scoped to a single
// organization while keeping provider internals out of CLI output.
func adminMigrationMissingOrgFinding(orgName, code, family, message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity:     "error",
		Code:         code,
		Organization: orgName,
		Family:       family,
		Message:      message,
	}
}

// adminMigrationOrganizationCount returns a scope-aware organization count so
// `--org` output remains focused while `--all-orgs` reports the full target.
func adminMigrationOrganizationCount(bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, cookbookInventory map[string]adminMigrationCookbookInventory, orgFilter string) int {
	return len(adminMigrationOrgNames(bootstrapState, coreObjectState, cookbookInventory, orgFilter))
}

// adminMigrationOrgNames returns deterministic organization names visible in
// any persisted state family, optionally narrowed by the CLI `--org` flag.
func adminMigrationOrgNames(bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState, cookbookInventory map[string]adminMigrationCookbookInventory, orgFilter string) []string {
	orgFilter = strings.TrimSpace(orgFilter)
	if orgFilter != "" {
		return []string{orgFilter}
	}
	seen := map[string]struct{}{}
	for name := range bootstrapState.Orgs {
		seen[name] = struct{}{}
	}
	for name := range coreObjectState.Orgs {
		seen[name] = struct{}{}
	}
	for name := range cookbookInventory {
		seen[name] = struct{}{}
	}
	return adminMigrationSortedStringSet(seen)
}

// adminMigrationNestedKeyCount counts user/client key records across their
// owner maps without assuming every actor has a key.
func adminMigrationNestedKeyCount(records map[string]map[string]bootstrap.KeyRecord) int {
	count := 0
	for _, ownerRecords := range records {
		count += len(ownerRecords)
	}
	return count
}

// adminMigrationGroupMembershipCount counts typed group memberships while
// falling back to legacy Actors-only rows if a store has not split members yet.
func adminMigrationGroupMembershipCount(groups map[string]bootstrap.Group) int {
	count := 0
	for _, group := range groups {
		typedCount := len(group.Users) + len(group.Clients) + len(group.Groups)
		if typedCount == 0 {
			typedCount = len(group.Actors)
		}
		count += typedCount
	}
	return count
}

// adminMigrationDataBagItemCount counts nested data bag items across every bag.
func adminMigrationDataBagItemCount(items map[string]map[string]bootstrap.DataBagItem) int {
	count := 0
	for _, bagItems := range items {
		count += len(bagItems)
	}
	return count
}

// adminMigrationPolicyRevisionCount counts all policy revisions across policy
// names, matching the Chef-facing `/policies/{name}/revisions` shape.
func adminMigrationPolicyRevisionCount(policies map[string]map[string]bootstrap.PolicyRevision) int {
	count := 0
	for _, revisions := range policies {
		count += len(revisions)
	}
	return count
}

// adminMigrationPolicyAssignmentCount counts policy-group assignments because
// groups themselves and revision rows are separate migration families.
func adminMigrationPolicyAssignmentCount(groups map[string]bootstrap.PolicyGroup) int {
	count := 0
	for _, group := range groups {
		count += len(group.Policies)
	}
	return count
}

// adminMigrationSandboxChecksumReferenceCount counts checksum references stored
// in sandbox metadata; provider existence is validated in the next task.
func adminMigrationSandboxChecksumReferenceCount(sandboxes map[string]bootstrap.Sandbox) int {
	count := 0
	for _, sandbox := range sandboxes {
		count += len(sandbox.Checksums)
	}
	return count
}

// adminMigrationCookbookFileChecksums returns normalized non-empty cookbook
// file checksums recorded in cookbook or artifact metadata.
func adminMigrationCookbookFileChecksums(files []bootstrap.CookbookFile) []string {
	checksums := make([]string, 0, len(files))
	for _, file := range files {
		if checksum := strings.ToLower(strings.TrimSpace(file.Checksum)); checksum != "" {
			checksums = append(checksums, checksum)
		}
	}
	return checksums
}

// adminMigrationSortedMapKeys returns sorted string keys for maps whose values
// are irrelevant to deterministic inventory and finding output.
func adminMigrationSortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// adminMigrationSortedStringSet converts a string set to sorted output for
// stable JSON across repeated preflight runs.
func adminMigrationSortedStringSet(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// adminMigrationHasACL checks only for row presence; detailed ACL repair keeps
// ownership of default ACL document contents.
func adminMigrationHasACL[T any](acls map[string]T, key string) bool {
	_, ok := acls[key]
	return ok
}

// adminMigrationRequiredDefaultGroups mirrors the groups seeded during org
// creation without importing bootstrap's private helper names.
func adminMigrationRequiredDefaultGroups() []string {
	return []string{"admins", "billing-admins", "users", "clients"}
}

// adminMigrationRequiredDefaultContainers mirrors the Chef-compatible
// containers seeded for each organization during bootstrap.
func adminMigrationRequiredDefaultContainers() []string {
	return []string{"clients", "containers", "cookbooks", "data", "environments", "groups", "nodes", "roles", "sandboxes", "policies", "policy_groups", "cookbook_artifacts"}
}

// adminMigrationOrganizationACLKey mirrors bootstrap's private organization ACL key.
func adminMigrationOrganizationACLKey() string { return "organization" }

// adminMigrationContainerACLKey mirrors bootstrap's private container ACL key.
func adminMigrationContainerACLKey(name string) string { return "container:" + name }

// adminMigrationGroupACLKey mirrors bootstrap's private group ACL key.
func adminMigrationGroupACLKey(name string) string { return "group:" + name }

// adminMigrationClientACLKey mirrors bootstrap's private client ACL key.
func adminMigrationClientACLKey(name string) string { return "client:" + name }

// adminMigrationEnvironmentACLKey mirrors bootstrap's private environment ACL key.
func adminMigrationEnvironmentACLKey(name string) string { return "environment:" + name }

// adminMigrationNodeACLKey mirrors bootstrap's private node ACL key.
func adminMigrationNodeACLKey(name string) string { return "node:" + name }

// adminMigrationRoleACLKey mirrors bootstrap's private role ACL key.
func adminMigrationRoleACLKey(name string) string { return "role:" + name }

// adminMigrationDataBagACLKey mirrors bootstrap's private data bag ACL key.
func adminMigrationDataBagACLKey(name string) string { return "data_bag:" + name }

// adminMigrationPolicyACLKey mirrors bootstrap's private policy ACL key.
func adminMigrationPolicyACLKey(name string) string { return "policy:" + name }

// adminMigrationPolicyGroupACLKey mirrors bootstrap's private policy group ACL key.
func adminMigrationPolicyGroupACLKey(name string) string { return "policy_group:" + name }

// adminMigrationBlobValidation builds, probes, and checks the selected blob
// backend against checksum references loaded from PostgreSQL-backed metadata.
func (c *command) adminMigrationBlobValidation(ctx context.Context, cfg config.Config, references []adminMigrationBlobReference) adminMigrationBlobValidation {
	newBlobStore := c.newBlobStore
	if newBlobStore == nil {
		newBlobStore = blob.NewStore
	}
	store, err := newBlobStore(cfg)
	if err != nil {
		dep := adminMigrationDependency{
			Name:       "blob",
			Status:     "error",
			Backend:    adminMigrationBlobBackendLabel(cfg),
			Configured: false,
			Message:    adminMigrationSafeErrorMessage("blob", err),
		}
		return adminMigrationUnavailableBlobValidation(dep, references)
	}
	status := store.Status()
	dep := adminMigrationDependency{
		Name:       "blob",
		Status:     "ok",
		Backend:    status.Backend,
		Configured: status.Configured,
		Message:    status.Message,
	}
	if !status.Configured {
		dep.Status = "error"
		return adminMigrationUnavailableBlobValidation(dep, references)
	}
	checker, ok := store.(blob.Checker)
	if !ok {
		dep.Status = "error"
		dep.Message = "blob backend does not expose checksum existence checks"
		return adminMigrationUnavailableBlobValidation(dep, references)
	}
	if _, err := checker.Exists(ctx, adminMigrationBlobProbeChecksum); err != nil {
		dep.Status = "error"
		dep.Message = adminMigrationSafeErrorMessage("blob", err)
		return adminMigrationUnavailableBlobValidation(dep, references)
	}
	dep.Message = status.Message + "; checksum existence probe succeeded"
	families, findings := adminMigrationValidateBlobReferences(ctx, store, checker, references)
	return adminMigrationBlobValidation{
		Dependency: dep,
		Families:   families,
		Findings:   findings,
	}
}

// adminMigrationUnavailableBlobValidation reports that referenced blobs could
// not be checked because provider construction or the initial probe failed.
func adminMigrationUnavailableBlobValidation(dep adminMigrationDependency, references []adminMigrationBlobReference) adminMigrationBlobValidation {
	unique := adminMigrationUniqueBlobReferences(references)
	result := adminMigrationBlobValidation{
		Dependency: dep,
		Families: []adminMigrationInventoryFamily{
			{Family: "referenced_blobs", Count: len(unique)},
			{Family: "reachable_blobs", Count: 0},
			{Family: "missing_blobs", Count: 0},
			{Family: "provider_unavailable_checks", Count: len(unique)},
			{Family: "content_verified_blobs", Count: 0},
			{Family: "checksum_mismatch_blobs", Count: 0},
			{Family: "candidate_orphan_blobs", Count: 0},
		},
	}
	if len(unique) > 0 {
		result.Findings = append(result.Findings, adminMigrationFinding{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "referenced checksum blobs could not be validated because the blob backend is unavailable",
		})
	}
	return result
}

// adminMigrationValidateBlobReferences performs read-only existence checks and,
// for local backends where content reads are safe, verifies checksum content.
func adminMigrationValidateBlobReferences(ctx context.Context, store blob.Store, checker blob.Checker, references []adminMigrationBlobReference) ([]adminMigrationInventoryFamily, []adminMigrationFinding) {
	unique := adminMigrationUniqueBlobReferences(references)
	referenced := adminMigrationBlobReferenceSet(unique)
	reachable := 0
	missing := 0
	unavailable := 0
	verified := 0
	mismatched := 0
	var findings []adminMigrationFinding
	getter, canVerifyContent := adminMigrationBlobContentGetter(store)

	for _, ref := range unique {
		if !bootstrap.ValidSandboxChecksum(ref.Checksum) {
			missing++
			findings = append(findings, adminMigrationBlobFinding("error", "invalid_blob_reference", ref, "checksum reference "+ref.Checksum+" is not a valid Chef checksum"))
			continue
		}
		exists, err := checker.Exists(ctx, ref.Checksum)
		if err != nil {
			unavailable++
			findings = append(findings, adminMigrationBlobFinding("error", "blob_check_unavailable", ref, "blob backend could not verify checksum "+ref.Checksum))
			continue
		}
		if !exists {
			missing++
			findings = append(findings, adminMigrationBlobFinding("error", "missing_blob", ref, "referenced checksum "+ref.Checksum+" is missing from the blob backend"))
			continue
		}
		reachable++
		if !canVerifyContent {
			continue
		}
		body, err := getter.Get(ctx, ref.Checksum)
		if err != nil {
			if errors.Is(err, blob.ErrNotFound) {
				reachable--
				missing++
				findings = append(findings, adminMigrationBlobFinding("error", "missing_blob", ref, "referenced checksum "+ref.Checksum+" disappeared before content validation"))
				continue
			}
			unavailable++
			findings = append(findings, adminMigrationBlobFinding("error", "blob_content_unavailable", ref, "blob backend could not read checksum "+ref.Checksum+" for local content validation"))
			continue
		}
		verified++
		if got := adminMigrationMD5Hex(body); got != ref.Checksum {
			mismatched++
			findings = append(findings, adminMigrationBlobFinding("error", "blob_checksum_mismatch", ref, "blob content for checksum "+ref.Checksum+" does not match its Chef checksum"))
		}
	}

	orphanCount, orphanFindings, orphanUnavailable := adminMigrationCandidateOrphanBlobs(ctx, store, referenced)
	unavailable += orphanUnavailable
	findings = append(findings, orphanFindings...)

	families := []adminMigrationInventoryFamily{
		{Family: "referenced_blobs", Count: len(unique)},
		{Family: "reachable_blobs", Count: reachable},
		{Family: "missing_blobs", Count: missing},
		{Family: "provider_unavailable_checks", Count: unavailable},
		{Family: "content_verified_blobs", Count: verified},
		{Family: "checksum_mismatch_blobs", Count: mismatched},
		{Family: "candidate_orphan_blobs", Count: orphanCount},
	}
	return families, findings
}

// adminMigrationBlobReferencesFromState collects checksum references from
// sandbox rows and cookbook metadata without touching provider blob contents.
func adminMigrationBlobReferencesFromState(coreObjectState bootstrap.CoreObjectState, cookbookInventory map[string]adminMigrationCookbookInventory, orgFilter string) []adminMigrationBlobReference {
	var refs []adminMigrationBlobReference
	for _, orgName := range adminMigrationOrgNames(bootstrap.BootstrapCoreState{}, coreObjectState, cookbookInventory, orgFilter) {
		coreOrg := coreObjectState.Orgs[orgName]
		for _, sandboxID := range adminMigrationSortedMapKeys(coreOrg.Sandboxes) {
			sandbox := coreOrg.Sandboxes[sandboxID]
			for _, checksum := range sandbox.Checksums {
				if checksum = strings.ToLower(strings.TrimSpace(checksum)); checksum != "" {
					refs = append(refs, adminMigrationBlobReference{Checksum: checksum, Organization: orgName, Family: "sandboxes"})
				}
			}
		}
		for _, checksum := range cookbookInventory[orgName].Checksums {
			if checksum = strings.ToLower(strings.TrimSpace(checksum)); checksum != "" {
				refs = append(refs, adminMigrationBlobReference{Checksum: checksum, Organization: orgName, Family: "cookbooks"})
			}
		}
	}
	return refs
}

// adminMigrationUniqueBlobReferences collapses shared checksum references into
// one provider check while preserving org/family context for findings.
func adminMigrationUniqueBlobReferences(references []adminMigrationBlobReference) []adminMigrationBlobReference {
	type aggregate struct {
		ref      adminMigrationBlobReference
		orgs     map[string]struct{}
		families map[string]struct{}
	}
	aggregates := map[string]*aggregate{}
	for _, ref := range references {
		checksum := strings.ToLower(strings.TrimSpace(ref.Checksum))
		if checksum == "" {
			continue
		}
		current := aggregates[checksum]
		if current == nil {
			current = &aggregate{
				ref:      adminMigrationBlobReference{Checksum: checksum},
				orgs:     map[string]struct{}{},
				families: map[string]struct{}{},
			}
			aggregates[checksum] = current
		}
		if ref.Organization != "" {
			current.orgs[ref.Organization] = struct{}{}
		}
		if ref.Family != "" {
			current.families[ref.Family] = struct{}{}
		}
	}
	keys := adminMigrationSortedStringSet(adminMigrationAggregateKeys(aggregates))
	out := make([]adminMigrationBlobReference, 0, len(keys))
	for _, checksum := range keys {
		current := aggregates[checksum]
		orgs := adminMigrationSortedStringSet(current.orgs)
		families := adminMigrationSortedStringSet(current.families)
		ref := current.ref
		if len(orgs) == 1 {
			ref.Organization = orgs[0]
		}
		if len(families) == 1 {
			ref.Family = families[0]
		} else if len(families) > 1 {
			ref.Family = "checksum_references"
		}
		out = append(out, ref)
	}
	return out
}

// adminMigrationAggregateKeys returns aggregate map keys as a set so the
// existing deterministic sorting helper can be reused.
func adminMigrationAggregateKeys[T any](values map[string]T) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for key := range values {
		out[key] = struct{}{}
	}
	return out
}

// adminMigrationBlobReferenceSet builds a checksum set used to compare listed
// provider keys against metadata-backed references.
func adminMigrationBlobReferenceSet(references []adminMigrationBlobReference) map[string]struct{} {
	out := make(map[string]struct{}, len(references))
	for _, ref := range references {
		out[ref.Checksum] = struct{}{}
	}
	return out
}

// adminMigrationBlobContentGetter limits full blob reads to local adapters
// where checksum verification is deterministic and not an expensive S3 scan.
func adminMigrationBlobContentGetter(store blob.Store) (blob.Getter, bool) {
	getter, ok := store.(blob.Getter)
	if !ok {
		return nil, false
	}
	switch store.Status().Backend {
	case "filesystem", "memory-compat":
		return getter, true
	default:
		return nil, false
	}
}

// adminMigrationCandidateOrphanBlobs reports provider keys that are valid Chef
// checksum names but not referenced by the loaded PostgreSQL metadata.
func adminMigrationCandidateOrphanBlobs(ctx context.Context, store blob.Store, referenced map[string]struct{}) (int, []adminMigrationFinding, int) {
	lister, ok := store.(blob.Lister)
	if !ok {
		return 0, nil, 0
	}
	keys, err := lister.List(ctx)
	if err != nil {
		return 0, []adminMigrationFinding{{
			Severity: "warning",
			Code:     "blob_list_unavailable",
			Family:   "candidate_orphan_blobs",
			Message:  "blob backend could not list checksum keys for orphan-candidate reporting",
		}}, 1
	}
	candidates := 0
	for _, key := range keys {
		key = strings.ToLower(strings.TrimSpace(key))
		if !bootstrap.ValidSandboxChecksum(key) {
			continue
		}
		if _, ok := referenced[key]; !ok {
			candidates++
		}
	}
	if candidates == 0 {
		return 0, nil, 0
	}
	return candidates, []adminMigrationFinding{{
		Severity: "warning",
		Code:     "candidate_orphan_blobs",
		Family:   "candidate_orphan_blobs",
		Message:  fmt.Sprintf("%d checksum blob(s) are present in the provider but not referenced by loaded PostgreSQL metadata", candidates),
	}}, 0
}

// adminMigrationBlobFinding scopes blob validation failures to an organization
// when a checksum is only referenced by one org; shared blobs stay global.
func adminMigrationBlobFinding(severity, code string, ref adminMigrationBlobReference, message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity:     severity,
		Code:         code,
		Message:      message,
		Organization: ref.Organization,
		Family:       ref.Family,
	}
}

// adminMigrationMD5Hex computes the legacy Chef checksum for local content
// validation; Chef cookbook and sandbox checksums are MD5 hex strings.
func adminMigrationMD5Hex(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}

// adminMigrationOpenSearchDependency discovers provider capabilities only when
// OpenSearch is configured, keeping memory/unconfigured search a non-fatal state.
func adminMigrationOpenSearchDependency(ctx context.Context, cfg config.Config) adminMigrationDependency {
	if strings.TrimSpace(cfg.OpenSearchURL) == "" {
		return adminMigrationDependency{
			Name:       "opensearch",
			Status:     "unconfigured",
			Backend:    "memory",
			Configured: false,
			Message:    "OpenSearch is not configured; search is derived state and can be rebuilt after configuration",
		}
	}
	client, err := search.NewOpenSearchClient(cfg.OpenSearchURL)
	if err != nil {
		return adminMigrationDependency{
			Name:       "opensearch",
			Status:     "error",
			Backend:    "opensearch",
			Configured: true,
			Message:    adminMigrationSafeErrorMessage("opensearch", err),
		}
	}
	info, err := client.DiscoverProvider(ctx)
	if err != nil {
		return adminMigrationDependency{
			Name:       "opensearch",
			Status:     "error",
			Backend:    "opensearch",
			Configured: true,
			Message:    adminMigrationSafeErrorMessage("opensearch", err),
		}
	}
	status := search.OpenSearchProviderStatus(info, true)
	return adminMigrationDependency{
		Name:       "opensearch",
		Status:     "ok",
		Backend:    status.Backend,
		Configured: status.Configured,
		Message:    status.Message + "; provider is discoverable and capability-compatible for rebuild/check/repair",
		Details: map[string]string{
			"distribution":                info.Distribution,
			"version":                     info.Version,
			"search_after_pagination":     adminMigrationBoolString(info.Capabilities.SearchAfterPagination),
			"delete_by_query":             adminMigrationBoolString(info.Capabilities.DeleteByQuery),
			"delete_by_query_fallback":    adminMigrationBoolString(info.Capabilities.DeleteByQueryFallbackRequired),
			"total_hits_object_responses": adminMigrationBoolString(info.Capabilities.TotalHitsObjectResponses),
		},
	}
}

// adminMigrationOpenSearchValidation compares PostgreSQL-derived search
// documents with OpenSearch-visible document IDs without running repair.
func (c *command) adminMigrationOpenSearchValidation(ctx context.Context, cfg config.Config, postgresRead adminMigrationPostgresRead, orgFilter string, provider adminMigrationDependency) adminMigrationOpenSearchValidation {
	baseFamilies := adminMigrationOpenSearchValidationFamilies(search.ConsistencyResult{})
	if strings.TrimSpace(cfg.OpenSearchURL) == "" {
		return adminMigrationOpenSearchValidation{
			Dependency: adminMigrationOpenSearchValidationDependency("unconfigured", "memory", false, "OpenSearch is not configured; derived search validation skipped", map[string]string{"state": "unconfigured"}),
			Families:   baseFamilies,
		}
	}
	if provider.Status != "ok" {
		return adminMigrationOpenSearchValidation{
			Dependency: adminMigrationOpenSearchValidationDependency("skipped", provider.Backend, provider.Configured, "OpenSearch consistency validation skipped because provider discovery did not succeed", map[string]string{"state": "configured_unavailable"}),
			Families:   baseFamilies,
		}
	}
	if !postgresRead.Loaded {
		return adminMigrationOpenSearchValidation{
			Dependency: adminMigrationOpenSearchValidationDependency("skipped", provider.Backend, provider.Configured, "OpenSearch consistency validation skipped because PostgreSQL state did not load", map[string]string{"state": "postgres_unavailable"}),
			Families:   baseFamilies,
		}
	}

	newSearchTarget := c.newSearchTarget
	if newSearchTarget == nil {
		newSearchTarget = func(raw string) (search.ConsistencyTarget, error) {
			return search.NewOpenSearchClient(raw)
		}
	}
	target, err := newSearchTarget(cfg.OpenSearchURL)
	if err != nil {
		return adminMigrationOpenSearchUnavailableValidation(provider, err, "open")
	}
	state := adminMigrationBootstrapServiceFromState(cfg, postgresRead.Bootstrap, postgresRead.CoreObjects)
	plan := search.ConsistencyPlan{
		AllOrganizations: strings.TrimSpace(orgFilter) == "",
		Organization:     strings.TrimSpace(orgFilter),
	}
	result, err := search.NewConsistencyService(state, target).Run(ctx, plan)
	if err != nil {
		return adminMigrationOpenSearchUnavailableValidation(provider, err, "check")
	}

	depStatus := "ok"
	message := "OpenSearch derived search state is active and consistent with PostgreSQL-backed OpenCook state"
	if adminSearchHasDrift(result) {
		depStatus = "warning"
		message = "OpenSearch derived search state has drift; PostgreSQL remains authoritative"
	}
	return adminMigrationOpenSearchValidation{
		Dependency: adminMigrationOpenSearchValidationDependency(depStatus, provider.Backend, provider.Configured, message, adminMigrationOpenSearchConsistencyDetails(result)),
		Families:   adminMigrationOpenSearchValidationFamilies(result),
		Findings:   adminMigrationOpenSearchConsistencyFindings(result),
		PlannedMutations: adminMigrationOpenSearchRepairRecommendations(
			result,
			adminMigrationOpenSearchRepairScope(strings.TrimSpace(orgFilter)),
		),
	}
}

// adminMigrationOpenSearchUnavailableValidation reports configured OpenSearch
// validation failures with redacted provider wording and no repair suggestion.
func adminMigrationOpenSearchUnavailableValidation(provider adminMigrationDependency, err error, phase string) adminMigrationOpenSearchValidation {
	message := "OpenSearch consistency validation could not run"
	if strings.TrimSpace(phase) != "" {
		message += " during " + strings.TrimSpace(phase)
	}
	message += ": " + adminReindexFailureMessage(err)
	return adminMigrationOpenSearchValidation{
		Dependency: adminMigrationOpenSearchValidationDependency("error", provider.Backend, provider.Configured, message, map[string]string{"state": "configured_unavailable"}),
		Families:   adminMigrationOpenSearchValidationFamilies(search.ConsistencyResult{}),
		Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "opensearch_consistency_unavailable",
			Family:   "opensearch",
			Message:  "OpenSearch derived search state could not be validated; PostgreSQL remains authoritative",
		}},
	}
}

// adminMigrationOpenSearchValidationDependency keeps consistency status separate
// from provider discovery so operators can tell availability from drift.
func adminMigrationOpenSearchValidationDependency(status, backend string, configured bool, message string, details map[string]string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       "opensearch_consistency",
		Status:     status,
		Backend:    backend,
		Configured: configured,
		Message:    message,
		Details:    details,
	}
}

// adminMigrationBootstrapServiceFromState constructs the same in-memory service
// view used by existing reindex/search tooling from loaded PostgreSQL rows.
func adminMigrationBootstrapServiceFromState(cfg config.Config, bootstrapState bootstrap.BootstrapCoreState, coreObjectState bootstrap.CoreObjectState) *bootstrap.Service {
	return bootstrap.NewService(nil, bootstrap.Options{
		SuperuserName:             adminReindexSuperuserName(cfg),
		InitialBootstrapCoreState: &bootstrapState,
		InitialCoreObjectState:    &coreObjectState,
	})
}

// adminMigrationOpenSearchConsistencyDetails serializes read-only consistency
// counters into dependency details for shell-friendly preflight parsing.
func adminMigrationOpenSearchConsistencyDetails(result search.ConsistencyResult) map[string]string {
	state := "clean"
	if adminSearchHasDrift(result) {
		state = "drift"
	}
	return map[string]string{
		"state":              state,
		"expected_documents": fmt.Sprintf("%d", result.Counts.Expected),
		"observed_documents": fmt.Sprintf("%d", result.Counts.Observed),
		"missing_documents":  fmt.Sprintf("%d", result.Counts.Missing),
		"stale_documents":    fmt.Sprintf("%d", result.Counts.Stale),
		"unsupported_scopes": fmt.Sprintf("%d", result.Counts.Unsupported),
	}
}

// adminMigrationOpenSearchValidationFamilies exposes the same consistency
// counters as inventory families without advertising unsupported indexes.
func adminMigrationOpenSearchValidationFamilies(result search.ConsistencyResult) []adminMigrationInventoryFamily {
	return []adminMigrationInventoryFamily{
		{Family: "opensearch_expected_documents", Count: result.Counts.Expected},
		{Family: "opensearch_observed_documents", Count: result.Counts.Observed},
		{Family: "opensearch_missing_documents", Count: result.Counts.Missing},
		{Family: "opensearch_stale_documents", Count: result.Counts.Stale},
		{Family: "opensearch_unsupported_scopes", Count: result.Counts.Unsupported},
	}
}

// adminMigrationOpenSearchConsistencyFindings turns derived-state drift into
// warnings because PostgreSQL state remains the authoritative migration source.
func adminMigrationOpenSearchConsistencyFindings(result search.ConsistencyResult) []adminMigrationFinding {
	var findings []adminMigrationFinding
	if result.Counts.Missing > 0 {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "opensearch_missing_documents",
			Family:   "opensearch",
			Message:  fmt.Sprintf("%d PostgreSQL-backed search document(s) are missing from OpenSearch; PostgreSQL remains authoritative", result.Counts.Missing),
		})
	}
	if result.Counts.Stale > 0 {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "opensearch_stale_documents",
			Family:   "opensearch",
			Message:  fmt.Sprintf("%d OpenSearch document(s) are stale relative to PostgreSQL-backed state", result.Counts.Stale),
		})
	}
	if result.Counts.Unsupported > 0 {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "opensearch_unsupported_documents",
			Family:   "opensearch",
			Message:  fmt.Sprintf("%d unsupported OpenSearch scope(s) were observed; cookbook, artifact, policy, policy-group, sandbox, and checksum indexes remain outside the public search contract", result.Counts.Unsupported),
		})
	}
	return findings
}

// adminMigrationOpenSearchRepairScope returns the exact org scope flags shared
// by recommended read-only and confirmed admin search repair commands.
func adminMigrationOpenSearchRepairScope(orgFilter string) string {
	if strings.TrimSpace(orgFilter) == "" {
		return "--all-orgs"
	}
	return "--org " + strings.TrimSpace(orgFilter)
}

// adminMigrationOpenSearchRepairRecommendations reports exact follow-up commands
// but never runs them from migration preflight.
func adminMigrationOpenSearchRepairRecommendations(result search.ConsistencyResult, scope string) []adminMigrationPlannedMutation {
	if !adminSearchHasDrift(result) {
		return nil
	}
	affected := result.Counts.Missing + result.Counts.Stale
	return []adminMigrationPlannedMutation{
		{
			Action:  "recommended_command",
			Family:  "opensearch",
			Count:   affected,
			Message: "opencook admin search repair " + scope + " --dry-run",
		},
		{
			Action:  "recommended_command",
			Family:  "opensearch",
			Count:   affected,
			Message: "opencook admin search repair " + scope + " --yes",
		},
	}
}

// adminMigrationRuntimeConfigDependency reports operator-facing settings that
// affect migration rehearsal but are not external services themselves.
func adminMigrationRuntimeConfigDependency(cfg config.Config) adminMigrationDependency {
	status := "ok"
	message := "runtime migration settings are present"
	if strings.TrimSpace(cfg.DefaultOrganization) == "" || strings.TrimSpace(cfg.BootstrapRequestorName) == "" {
		status = "warning"
		message = "runtime migration settings are usable but missing optional default organization or bootstrap requestor values"
	}
	return adminMigrationDependency{
		Name:       "runtime_config",
		Status:     status,
		Backend:    "opencook",
		Configured: true,
		Message:    message,
		Details: map[string]string{
			"default_organization":       adminMigrationPresence(cfg.DefaultOrganization),
			"bootstrap_requestor_name":   adminMigrationPresence(cfg.BootstrapRequestorName),
			"bootstrap_requestor_type":   strings.TrimSpace(cfg.BootstrapRequestorType),
			"bootstrap_requestor_org":    adminMigrationPresence(cfg.BootstrapRequestorOrganization),
			"bootstrap_requestor_key_id": adminMigrationPresence(cfg.BootstrapRequestorKeyID),
		},
	}
}

// adminMigrationRuntimeConfigFindings turns optional runtime config gaps into
// explicit warnings rather than hiding assumptions in prose output.
func adminMigrationRuntimeConfigFindings(cfg config.Config) []adminMigrationFinding {
	var findings []adminMigrationFinding
	if strings.TrimSpace(cfg.DefaultOrganization) == "" {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "default_org_unset",
			Message:  "OPENCOOK_DEFAULT_ORGANIZATION is not set; default-org route rehearsal may require explicit org-scoped checks",
		})
	}
	if strings.TrimSpace(cfg.BootstrapRequestorName) == "" {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "bootstrap_requestor_unset",
			Message:  "OPENCOOK_BOOTSTRAP_REQUESTOR_NAME is not set; signed admin rehearsal may require explicit requestor configuration",
		})
	}
	return findings
}

// loadAdminMigrationConfig loads offline configuration and applies CLI
// overrides before any migration output is emitted, keeping redaction centralized.
func (c *command) loadAdminMigrationConfig(opts *adminMigrationFlagValues) (config.Config, int, bool) {
	cfg, err := c.loadOffline()
	if err != nil {
		fmt.Fprintf(c.stderr, "load migration config: %v\n", err)
		return config.Config{}, exitDependencyUnavailable, false
	}
	applyAdminMigrationConfigOverrides(&cfg, opts)
	return cfg, exitOK, true
}

// applyAdminMigrationConfigOverrides maps CLI provider overrides onto the same
// config structure used by server startup and status reporting.
func applyAdminMigrationConfigOverrides(cfg *config.Config, opts *adminMigrationFlagValues) {
	if value := strings.TrimSpace(opts.postgresDSN); value != "" {
		cfg.PostgresDSN = value
	}
	if value := strings.TrimSpace(opts.openSearchURL); value != "" {
		cfg.OpenSearchURL = value
	}
	if value := strings.TrimSpace(opts.blobBackend); value != "" {
		cfg.BlobBackend = value
	}
	if value := strings.TrimSpace(opts.blobStorageURL); value != "" {
		cfg.BlobStorageURL = value
	}
	if value := strings.TrimSpace(opts.blobS3Endpoint); value != "" {
		cfg.BlobS3Endpoint = value
	}
	if value := strings.TrimSpace(opts.blobS3Region); value != "" {
		cfg.BlobS3Region = value
	}
	if value := strings.TrimSpace(opts.blobS3AccessKeyID); value != "" {
		cfg.BlobS3AccessKeyID = value
	}
	if value := strings.TrimSpace(opts.blobS3SecretKey); value != "" {
		cfg.BlobS3SecretKey = value
	}
	if value := strings.TrimSpace(opts.blobS3SessionToken); value != "" {
		cfg.BlobS3SessionToken = value
	}
}

// writeAdminMigrationResult finalizes timing, normalizes empty collections, and
// writes the shared migration JSON envelope for implemented commands.
func (c *command) writeAdminMigrationResult(out adminMigrationCLIOutput, withTiming bool, start time.Time, exitCode int) int {
	adminMigrationNormalizeOutput(&out)
	if withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write migration output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitCode
}

// writeAdminMigrationScaffold emits the shared migration JSON envelope now so
// later task implementations can populate it without changing the top-level shape.
func (c *command) writeAdminMigrationScaffold(opts adminMigrationOutputOptions) int {
	start := time.Now()
	out := adminMigrationCLIOutput{
		OK:               false,
		Command:          opts.command,
		Target:           opts.target,
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.confirmed,
		Config:           opts.config,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: opts.plannedMutations,
		Warnings:         []string{adminMigrationScaffoldMessage},
		Errors: []adminCLIError{{
			Code:    "not_implemented",
			Message: adminMigrationScaffoldMessage,
		}},
	}
	if out.PlannedMutations == nil {
		out.PlannedMutations = []adminMigrationPlannedMutation{}
	}
	if opts.withTiming {
		duration := time.Since(start)
		durationMS := duration.Milliseconds()
		out.Duration = duration.String()
		out.DurationMS = &durationMS
	}
	if err := writePrettyJSON(c.stdout, out); err != nil {
		fmt.Fprintf(c.stderr, "write migration output: %v\n", err)
		return exitDependencyUnavailable
	}
	return exitDependencyUnavailable
}

// adminMigrationNormalizeOutput keeps empty migration collections stable in JSON
// so scripts do not have to distinguish null from an empty list.
func adminMigrationNormalizeOutput(out *adminMigrationCLIOutput) {
	if out.Dependencies == nil {
		out.Dependencies = []adminMigrationDependency{}
	}
	if out.Inventory.Families == nil {
		out.Inventory.Families = []adminMigrationInventoryFamily{}
	}
	if out.Findings == nil {
		out.Findings = []adminMigrationFinding{}
	}
	if out.PlannedMutations == nil {
		out.PlannedMutations = []adminMigrationPlannedMutation{}
	}
}

// adminMigrationCollectOutputStatuses folds dependency and finding severities
// into the top-level OK, warning, and stable error-code fields.
func adminMigrationCollectOutputStatuses(out *adminMigrationCLIOutput) {
	for _, dep := range out.Dependencies {
		switch dep.Status {
		case "error":
			out.OK = false
			out.Errors = append(out.Errors, adminCLIError{
				Code:    adminMigrationDependencyErrorCode(dep.Name),
				Message: dep.Message,
			})
		case "warning":
			out.Warnings = append(out.Warnings, dep.Message)
		}
	}
	for _, finding := range out.Findings {
		switch finding.Severity {
		case "error":
			out.OK = false
			out.Errors = append(out.Errors, adminCLIError{
				Code:    finding.Code,
				Message: finding.Message,
			})
		case "warning":
			out.Warnings = append(out.Warnings, finding.Message)
		}
	}
}

// adminMigrationMarkDependency appends a dependency result and immediately
// reflects blocking dependency failures on the top-level migration result.
func adminMigrationMarkDependency(out *adminMigrationCLIOutput, dep adminMigrationDependency) {
	out.Dependencies = append(out.Dependencies, dep)
	switch dep.Status {
	case "error":
		out.OK = false
		out.Errors = append(out.Errors, adminCLIError{Code: adminMigrationDependencyErrorCode(dep.Name), Message: dep.Message})
	case "warning":
		out.Warnings = append(out.Warnings, dep.Message)
	}
}

// adminMigrationMarkFinding appends a finding and mirrors error/warning
// severity into fields that scripts already inspect.
func adminMigrationMarkFinding(out *adminMigrationCLIOutput, finding adminMigrationFinding) {
	out.Findings = append(out.Findings, finding)
	switch finding.Severity {
	case "error":
		out.OK = false
		out.Errors = append(out.Errors, adminCLIError{Code: finding.Code, Message: finding.Message})
	case "warning":
		out.Warnings = append(out.Warnings, finding.Message)
	}
}

// adminMigrationMarkError records a generic restore failure without exposing
// provider internals or partial rollback details.
func adminMigrationMarkError(out *adminMigrationCLIOutput, code, message, family string) {
	adminMigrationMarkFinding(out, adminMigrationFinding{
		Severity: "error",
		Code:     code,
		Family:   family,
		Message:  message,
	})
}

// adminMigrationHasErrorFindings lets backup creation avoid optional blob copy
// reads when existence/content validation has already found blocking failures.
func adminMigrationHasErrorFindings(findings []adminMigrationFinding) bool {
	for _, finding := range findings {
		if finding.Severity == "error" {
			return true
		}
	}
	return false
}

// adminMigrationExitCode maps preflight dependency failures to the existing
// admin exit-code vocabulary while allowing non-fatal warnings to succeed.
func adminMigrationExitCode(out adminMigrationCLIOutput) int {
	if len(out.Errors) > 0 || !out.OK {
		return exitDependencyUnavailable
	}
	return exitOK
}

// adminMigrationDependencyErrorCode gives automation a stable error code for
// each dependency family without exposing provider-specific internals.
func adminMigrationDependencyErrorCode(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "dependency_unavailable"
	}
	return name + "_unavailable"
}

// adminMigrationSafeErrorMessage classifies provider failures while avoiding
// raw DSNs, URLs, provider bodies, credentials, or transport internals.
func adminMigrationSafeErrorMessage(kind string, err error) string {
	switch strings.TrimSpace(kind) {
	case "postgres", "postgres_bootstrap_core", "postgres_core_objects", "postgres_cookbooks":
		return "PostgreSQL dependency is unavailable"
	case "blob":
		switch {
		case errors.Is(err, blob.ErrUnavailable):
			return "blob backend is unavailable"
		case errors.Is(err, blob.ErrInvalidInput):
			return "blob backend configuration or probe input is invalid"
		default:
			return "blob backend is unavailable or misconfigured"
		}
	case "opensearch":
		switch {
		case errors.Is(err, search.ErrInvalidConfiguration):
			return "OpenSearch provider is not capability-compatible"
		case errors.Is(err, search.ErrRejected):
			return "OpenSearch provider rejected discovery or capability checks"
		case errors.Is(err, search.ErrUnavailable):
			return "OpenSearch provider is unavailable"
		default:
			return "OpenSearch provider is unavailable or misconfigured"
		}
	default:
		return "dependency is unavailable"
	}
}

// adminMigrationBlobBackendLabel reports an intended backend even when store
// construction fails before a provider-specific status is available.
func adminMigrationBlobBackendLabel(cfg config.Config) string {
	if backend := strings.TrimSpace(cfg.BlobBackend); backend != "" {
		return backend
	}
	if target := strings.TrimSpace(cfg.BlobStorageURL); target != "" {
		if strings.HasPrefix(target, "s3://") {
			return "s3-compatible"
		}
		if strings.HasPrefix(target, "file://") || !strings.Contains(target, "://") {
			return "filesystem"
		}
	}
	return "memory"
}

func adminMigrationBoolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func adminMigrationPresence(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unset"
	}
	return "set"
}

// adminMigrationRedact mirrors config redaction for ad-hoc fields that are not
// part of config.Config, such as a rehearsal server URL.
func adminMigrationRedact(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "set"
	}
	return value[:4] + "..."
}

// printAdminMigrationUsage documents the operator-facing migration command
// namespace, including implemented backup, restore, inventory, and rehearsal flows.
func (c *command) printAdminMigrationUsage(w io.Writer) {
	fmt.Fprint(w, `Usage:
  opencook admin migration preflight [--org ORG|--all-orgs] [--json] [--with-timing]
  opencook admin migration backup create --output PATH --offline [--dry-run|--yes] [--json] [--with-timing]
  opencook admin migration backup inspect PATH [--json]
  opencook admin migration restore preflight PATH --offline [--json] [--with-timing]
  opencook admin migration restore apply PATH --offline [--dry-run|--yes] [--json] [--with-timing]
  opencook admin migration source inventory PATH [--json] [--with-timing]
  opencook admin migration cutover rehearse --manifest PATH [--server-url URL] [--json] [--with-timing]

Migration commands validate target readiness, write/inspect OpenCook logical
backup bundles, restore offline targets, and inventory read-only Chef Server
source artifacts. Source inventory does not import or mutate upstream data.

Flags:
  --org ORG
  --all-orgs
  --output PATH
  --manifest PATH
  --server-url URL
  --dry-run
  --offline
  --yes
  --with-timing
  --json
  --requestor-name NAME
  --requestor-type user|client
  --private-key PATH
  --default-org ORG
  --server-api-version VERSION
  --postgres-dsn DSN
  --opensearch-url URL
  --blob-backend memory|filesystem|s3
  --blob-storage-url URL
  --blob-s3-endpoint URL
  --blob-s3-region REGION
  --blob-s3-access-key-id KEY
  --blob-s3-secret-access-key SECRET
  --blob-s3-session-token TOKEN
`)
}
