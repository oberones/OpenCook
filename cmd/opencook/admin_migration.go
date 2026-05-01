package main

import (
	"archive/tar"
	"bytes"
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
	pathpkg "path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/admin"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
)

const (
	adminMigrationScaffoldMessage              = "migration command scaffolding is ready; implementation will be filled in by the remaining migration tooling tasks"
	adminMigrationBlobProbeChecksum            = "0000000000000000000000000000000000000000000000000000000000000000"
	adminMigrationSourceFormatV1               = "opencook.migration.source_inventory.v1"
	adminMigrationChefSourceFormatV1           = "opencook.migration.chef_source.v1"
	adminMigrationSourceImportProgressFormatV1 = "opencook.migration.source_import_progress.v1"
	adminMigrationSourceSyncProgressFormatV1   = "opencook.migration.source_sync_progress.v1"
	adminMigrationSourceManifestPath           = "opencook-source-manifest.json"
	adminMigrationSourceImportProgressPath     = "opencook-source-import-progress.json"
	adminMigrationSourceSyncProgressPath       = "opencook-source-sync-progress.json"
)

var (
	errAdminMigrationNormalizeOutputExists   = errors.New("normalized source output already exists")
	errAdminMigrationNormalizeOutputOverlaps = errors.New("normalized source output overlaps source")
	errAdminMigrationUnsafeSourcePath        = errors.New("unsafe source path")
	adminMigrationChefNamePattern            = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	adminMigrationPolicyRevisionPattern      = regexp.MustCompile(`^[A-Fa-f0-9]{40}$`)
	adminMigrationChecksumPattern            = regexp.MustCompile(`^[A-Fa-f0-9]{32}$`)
	adminMigrationCookbookVersionPattern     = regexp.MustCompile(`^\d+(?:\.\d+){0,2}(?:\.[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*)?$`)
	adminMigrationCookbookArtifactPattern    = regexp.MustCompile(`^[A-Fa-f0-9]{40}$`)
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
	progressPath       string
	manifestPath       string
	sourcePath         string
	importProgressPath string
	syncProgressPath   string
	shadowResultPath   string
	searchResultPath   string
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
	rollbackReady      bool
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
	AllOrganizations   bool   `json:"all_organizations,omitempty"`
	Organization       string `json:"organization,omitempty"`
	BundlePath         string `json:"bundle_path,omitempty"`
	OutputPath         string `json:"output_path,omitempty"`
	ProgressPath       string `json:"progress_path,omitempty"`
	ImportProgressPath string `json:"source_import_progress_path,omitempty"`
	SyncProgressPath   string `json:"source_sync_progress_path,omitempty"`
	ShadowResultPath   string `json:"shadow_result_path,omitempty"`
	SearchResultPath   string `json:"search_check_result_path,omitempty"`
	ManifestPath       string `json:"manifest_path,omitempty"`
	SourcePath         string `json:"source_path,omitempty"`
	ServerURL          string `json:"server_url,omitempty"`
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

type adminMigrationCookbookSyncStore interface {
	SyncCookbookExport(bootstrap.BootstrapCoreState, adminMigrationCookbookExport, map[adminMigrationSourcePayloadKey]bool) error
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
	Payloads      []adminMigrationSourceManifestPayload  `json:"payloads,omitempty"`
	Artifacts     []adminMigrationSourceManifestArtifact `json:"artifacts,omitempty"`
	Notes         []string                               `json:"notes,omitempty"`
}

type adminMigrationSourceManifestPayload struct {
	Organization string `json:"organization,omitempty"`
	Family       string `json:"family"`
	Path         string `json:"path,omitempty"`
	Count        int    `json:"count,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
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

type adminMigrationSourceArtifactFileEntry struct {
	Path  string
	IsDir bool
	Data  []byte
}

type adminMigrationSourceNormalizeBundle struct {
	Manifest      adminMigrationSourceManifest
	Files         map[string][]byte
	Inventory     adminMigrationInventory
	Findings      []adminMigrationFinding
	SourceType    string
	FormatVersion string
}

type adminMigrationSourceImportRead struct {
	Bundle              adminMigrationSourceNormalizeBundle
	PayloadValues       map[adminMigrationSourcePayloadKey][]json.RawMessage
	ReferencedChecksums []adminMigrationBlobReference
	CopiedChecksums     map[string]bool
}

type adminMigrationSourceImportState struct {
	Bootstrap   bootstrap.BootstrapCoreState
	CoreObjects bootstrap.CoreObjectState
	Cookbooks   adminMigrationCookbookExport
}

type adminMigrationSourceImportProgress struct {
	FormatVersion    string   `json:"format_version"`
	SourcePath       string   `json:"source_path,omitempty"`
	UpdatedAt        string   `json:"updated_at,omitempty"`
	CopiedBlobs      []string `json:"copied_blobs,omitempty"`
	VerifiedBlobs    []string `json:"verified_blobs,omitempty"`
	MetadataImported bool     `json:"metadata_imported,omitempty"`
}

type adminMigrationSourceSyncProgress struct {
	FormatVersion  string   `json:"format_version"`
	SourcePath     string   `json:"source_path,omitempty"`
	SourceCursor   string   `json:"source_cursor,omitempty"`
	UpdatedAt      string   `json:"updated_at,omitempty"`
	LastStatus     string   `json:"last_status,omitempty"`
	AppliedCursors []string `json:"applied_cursors,omitempty"`
}

type adminMigrationSourceSyncDiff struct {
	Mutations       []adminMigrationPlannedMutation
	Families        []adminMigrationInventoryFamily
	HasChanges      bool
	CookbookChanges bool
	DeleteCount     int
	Details         []adminMigrationSourceSyncFamilyDiff
}

type adminMigrationSourceSyncFamilyDiff struct {
	Key       adminMigrationSourcePayloadKey
	Creates   int
	Updates   int
	Deletes   int
	Unchanged int
}

type adminMigrationSourcePayloadKey struct {
	Organization string
	Family       string
}

type adminMigrationSourceSemanticError struct {
	Code    string
	Message string
}

// Error returns the stable finding code alongside the redacted operator-facing
// message for semantic source normalization failures.
func (e adminMigrationSourceSemanticError) Error() string {
	return e.Code + ": " + e.Message
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

type adminMigrationShadowComparisonResult struct {
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
		return c.runAdminMigrationSource(ctx, args[1:], inheritedJSON)
	case "cutover":
		return c.runAdminMigrationCutover(ctx, args[1:], inheritedJSON)
	case "shadow":
		return c.runAdminMigrationShadow(ctx, args[1:], inheritedJSON)
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

// adminMigrationSaveSyncedState writes sync metadata in dependency order and
// uses the cookbook sync seam only when cookbook families actually changed.
func adminMigrationSaveSyncedState(store adminOfflineStore, previousBootstrap bootstrap.BootstrapCoreState, previousCore bootstrap.CoreObjectState, syncedBootstrap bootstrap.BootstrapCoreState, syncedCore bootstrap.CoreObjectState, cookbooks adminMigrationCookbookExport, scopes map[adminMigrationSourcePayloadKey]bool, syncCookbooks bool) error {
	if err := store.SaveBootstrapCore(syncedBootstrap); err != nil {
		return err
	}
	if err := store.SaveCoreObjects(syncedCore); err != nil {
		_ = store.SaveBootstrapCore(previousBootstrap)
		return err
	}
	if syncCookbooks {
		syncer, ok := store.(adminMigrationCookbookSyncStore)
		if !ok {
			_ = store.SaveCoreObjects(previousCore)
			_ = store.SaveBootstrapCore(previousBootstrap)
			return fmt.Errorf("cookbook sync is not supported by this offline store")
		}
		if err := syncer.SyncCookbookExport(syncedBootstrap, cookbooks, scopes); err != nil {
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

// runAdminMigrationSource dispatches source artifact commands while keeping
// future apply behavior behind the stricter offline mutation gates.
func (c *command) runAdminMigrationSource(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration source requires inventory, normalize, import, or sync\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	switch args[0] {
	case "inventory":
		return c.runAdminMigrationSourceInventory(args[1:], inheritedJSON)
	case "normalize":
		return c.runAdminMigrationSourceNormalize(args[1:], inheritedJSON)
	case "import":
		return c.runAdminMigrationSourceImport(ctx, args[1:], inheritedJSON)
	case "sync":
		return c.runAdminMigrationSourceSync(ctx, args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration source command %q\n\n", args[0])
	}
}

// runAdminMigrationSourceImport dispatches normalized source import preflight
// and apply flows while keeping both shapes behind explicit offline gates.
func (c *command) runAdminMigrationSourceImport(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration source import requires preflight or apply\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	switch args[0] {
	case "preflight":
		return c.runAdminMigrationSourceImportPreflight(ctx, args[1:], inheritedJSON)
	case "apply":
		return c.runAdminMigrationSourceImportApply(ctx, args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration source import command %q\n\n", args[0])
	}
}

// runAdminMigrationSourceImportPreflight validates a normalized source bundle
// and offline target readiness without mutating PostgreSQL, blobs, or search.
func (c *command) runAdminMigrationSourceImportPreflight(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source import preflight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source import preflight", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Accept PATH before flags so normalized source directories match the
		// ergonomics of backup restore and source normalize.
	} else {
		return c.adminUsageError("usage: opencook admin migration source import preflight PATH --offline [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration source import preflight is offline-only and requires --offline\n\n")
	}

	sourceRead, inspectResult := buildAdminMigrationSourceImportBundlePreflight(sourcePath, opts)
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationSourceImportPreflight(ctx, cfg, sourcePath, sourceRead, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// runAdminMigrationSourceImportApply validates a normalized source bundle,
// copies or verifies blobs, then writes the imported state to an offline target.
func (c *command) runAdminMigrationSourceImportApply(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source import apply", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.progressPath, "progress", "", "source import progress metadata path")
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source import apply", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Accept PATH before flags so apply has the same ergonomics as preflight.
	} else {
		return c.adminUsageError("usage: opencook admin migration source import apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration source import apply is offline-only and requires --offline\n\n")
	}
	if !opts.dryRun && !opts.yes {
		return c.adminUsageError("admin migration source import apply requires --dry-run or --yes\n\n")
	}
	opts.progressPath = adminMigrationSourceImportProgressFile(sourcePath, opts.progressPath)

	sourceRead, inspectResult := buildAdminMigrationSourceImportBundlePreflight(sourcePath, opts)
	inspectResult.Command = "migration_source_import_apply"
	inspectResult.Target.ProgressPath = adminMigrationRedactMigrationPath(opts.progressPath)
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationSourceImportPreflight(ctx, cfg, sourcePath, sourceRead, opts)
	result.Command = "migration_source_import_apply"
	result.Confirmed = opts.yes
	result.Target.ProgressPath = adminMigrationRedactMigrationPath(opts.progressPath)
	if !result.OK || opts.dryRun {
		return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
	}

	result = c.applyAdminMigrationSourceImport(ctx, cfg, sourcePath, sourceRead, opts, result)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// runAdminMigrationSourceSync dispatches repeatable normalized-source
// reconciliation commands while preserving the same offline mutation gates as import.
func (c *command) runAdminMigrationSourceSync(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration source sync requires preflight or apply\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	switch args[0] {
	case "preflight":
		return c.runAdminMigrationSourceSyncPreflight(ctx, args[1:], inheritedJSON)
	case "apply":
		return c.runAdminMigrationSourceSyncApply(ctx, args[1:], inheritedJSON)
	default:
		return c.adminUsageError("unknown admin migration source sync command %q\n\n", args[0])
	}
}

// runAdminMigrationSourceSyncPreflight compares a later normalized source
// snapshot to the offline OpenCook target without writing metadata or blobs.
func (c *command) runAdminMigrationSourceSyncPreflight(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source sync preflight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.progressPath, "progress", "", "source sync progress metadata path")
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source sync preflight", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Keep PATH-before-flags ergonomics identical to source import.
	} else {
		return c.adminUsageError("usage: opencook admin migration source sync preflight PATH --offline [--progress PATH] [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration source sync preflight is offline-only and requires --offline\n\n")
	}
	opts.progressPath = adminMigrationSourceSyncProgressFile(sourcePath, opts.progressPath)

	sourceRead, inspectResult := buildAdminMigrationSourceImportBundlePreflight(sourcePath, opts)
	inspectResult.Command = "migration_source_sync_preflight"
	inspectResult.Target.ProgressPath = adminMigrationRedactMigrationPath(opts.progressPath)
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationSourceSyncPreflight(ctx, cfg, sourcePath, sourceRead, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// runAdminMigrationSourceSyncApply executes the preflight plan only after an
// operator explicitly chooses dry-run or confirmed offline reconciliation.
func (c *command) runAdminMigrationSourceSyncApply(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source sync apply", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.progressPath, "progress", "", "source sync progress metadata path")
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source sync apply", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Keep PATH-before-flags ergonomics identical to source import apply.
	} else {
		return c.adminUsageError("usage: opencook admin migration source sync apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]\n\n")
	}
	if !opts.offline {
		return c.adminUsageError("admin migration source sync apply is offline-only and requires --offline\n\n")
	}
	if !opts.dryRun && !opts.yes {
		return c.adminUsageError("admin migration source sync apply requires --dry-run or --yes\n\n")
	}
	opts.progressPath = adminMigrationSourceSyncProgressFile(sourcePath, opts.progressPath)

	sourceRead, inspectResult := buildAdminMigrationSourceImportBundlePreflight(sourcePath, opts)
	inspectResult.Command = "migration_source_sync_apply"
	inspectResult.Target.ProgressPath = adminMigrationRedactMigrationPath(opts.progressPath)
	if !inspectResult.OK {
		return c.writeAdminMigrationResult(inspectResult, opts.withTiming, start, adminMigrationExitCode(inspectResult))
	}
	cfg, code, ok := c.loadAdminMigrationConfig(opts)
	if !ok {
		return code
	}

	result := c.buildAdminMigrationSourceSyncPreflight(ctx, cfg, sourcePath, sourceRead, opts)
	result.Command = "migration_source_sync_apply"
	result.DryRun = opts.dryRun
	result.Confirmed = opts.yes
	if !result.OK || opts.dryRun {
		return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
	}

	result = c.applyAdminMigrationSourceSync(ctx, cfg, sourcePath, sourceRead, opts, result)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
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

// runAdminMigrationSourceNormalize converts a supported read-only source
// artifact into OpenCook's normalized source-manifest directory layout.
func (c *command) runAdminMigrationSourceNormalize(args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration source normalize", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	fs.StringVar(&opts.outputPath, "output", "", "normalized source output path")
	sourcePath, parseArgs := splitAdminMigrationPathArg(args)
	if err := fs.Parse(parseArgs); err != nil {
		return c.adminFlagError("admin migration source normalize", err)
	}
	if sourcePath == "" && fs.NArg() == 1 {
		sourcePath = fs.Arg(0)
	} else if sourcePath != "" && fs.NArg() == 0 {
		// Preserve PATH-before-flags ergonomics for source directories and
		// archives, matching source inventory and backup inspect.
	} else {
		return c.adminUsageError("usage: opencook admin migration source normalize PATH --output PATH [--yes] [--json] [--with-timing]\n\n")
	}
	if strings.TrimSpace(opts.outputPath) == "" {
		return c.adminUsageError("admin migration source normalize requires --output PATH\n\n")
	}

	result := buildAdminMigrationSourceNormalize(sourcePath, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationSourceNormalize builds and writes the normalized source
// bundle without loading OpenCook runtime configuration or mutating the source.
func buildAdminMigrationSourceNormalize(sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_source_normalize",
		Target:           adminMigrationTarget{SourcePath: adminMigrationRedactMigrationPath(sourcePath), OutputPath: adminMigrationRedactMigrationPath(opts.outputPath)},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}

	bundle, err := adminMigrationBuildNormalizedSourceBundle(sourcePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_artifact",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source artifact could not be normalized",
		})
		for _, finding := range bundle.Findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(bundle.Findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationSourceErrorFinding("source_artifact_unavailable", "source artifact could not be read or classified"))
		}
		return out
	}

	out.Inventory = bundle.Inventory
	out.Findings = append(out.Findings, bundle.Findings...)
	adminMigrationMarkDependency(&out, adminMigrationDependency{
		Name:       "source_artifact",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "source artifact was normalized without connecting to live Chef Server databases",
		Details: map[string]string{
			"source_type":    bundle.SourceType,
			"format_version": bundle.FormatVersion,
		},
	})
	if err := adminMigrationEnsureNormalizeOutputAllowed(sourcePath, opts.outputPath, opts.yes); err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "normalized_source_output",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "normalized source output path is not safe to write",
		})
		adminMigrationMarkFinding(&out, adminMigrationNormalizeOutputFinding(err))
		return out
	}
	if err := adminMigrationWriteNormalizedSourceBundle(opts.outputPath, bundle, opts.yes); err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "normalized_source_output",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "normalized source output could not be written",
		})
		adminMigrationMarkFinding(&out, adminMigrationSourceErrorFinding("source_normalize_write_failed", "normalized source output could not be written"))
		return out
	}
	adminMigrationMarkDependency(&out, adminMigrationDependency{
		Name:       "normalized_source_output",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "normalized source manifest and payload files were written",
		Details: map[string]string{
			"payloads": fmt.Sprintf("%d", len(bundle.Manifest.Payloads)),
			"files":    fmt.Sprintf("%d", len(bundle.Files)+1),
		},
	})
	out.PlannedMutations = append(out.PlannedMutations, adminMigrationPlannedMutation{
		Action:  "write_normalized_source",
		Family:  "source_manifest",
		Count:   len(bundle.Manifest.Payloads),
		Message: "wrote normalized source manifest and deterministic payload files",
	})
	adminMigrationCollectOutputStatuses(&out)
	return out
}

// buildAdminMigrationSourceImportBundlePreflight performs provider-free
// integrity checks before target config is loaded, so invalid source input
// cannot trigger PostgreSQL, blob, or OpenSearch side effects.
func buildAdminMigrationSourceImportBundlePreflight(sourcePath string, opts *adminMigrationFlagValues) (adminMigrationSourceImportRead, adminMigrationCLIOutput) {
	out := adminMigrationSourceImportPreflightOutput(sourcePath, opts)
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationSourceBundleDependency("error", "normalized source bundle failed integrity inspection", nil))
		for _, finding := range read.Bundle.Findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(read.Bundle.Findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationSourceErrorFinding("source_bundle_invalid", "normalized source bundle failed integrity inspection"))
		}
		return adminMigrationSourceImportRead{}, out
	}

	out.Inventory = read.Bundle.Inventory
	out.Findings = append(out.Findings, read.Bundle.Findings...)
	adminMigrationMarkDependency(&out, adminMigrationSourceBundleDependency("ok", "normalized source manifest, payload hashes, and copied blobs are valid", map[string]string{
		"format_version":    read.Bundle.FormatVersion,
		"source_type":       read.Bundle.SourceType,
		"payloads":          fmt.Sprintf("%d", len(read.Bundle.Manifest.Payloads)),
		"referenced_blobs":  fmt.Sprintf("%d", len(adminMigrationUniqueBlobReferences(read.ReferencedChecksums))),
		"copied_blobs":      fmt.Sprintf("%d", len(read.CopiedChecksums)),
		"sidecar_artifacts": fmt.Sprintf("%d", len(read.Bundle.Manifest.Artifacts)),
	}))
	adminMigrationCollectOutputStatuses(&out)
	return read, out
}

// buildAdminMigrationSourceImportPreflight combines source integrity, target
// emptiness, blob readiness, and search rebuild planning without writing state.
func (c *command) buildAdminMigrationSourceImportPreflight(ctx context.Context, cfg config.Config, sourcePath string, read adminMigrationSourceImportRead, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationSourceImportPreflightOutput(sourcePath, opts)
	out.Config = cfg.Redacted()
	out.Inventory = read.Bundle.Inventory
	out.Findings = append(out.Findings, read.Bundle.Findings...)
	out.Dependencies = append(out.Dependencies, adminMigrationSourceBundleDependency("ok", "normalized source manifest, payload hashes, and copied blobs are valid", map[string]string{
		"format_version":    read.Bundle.FormatVersion,
		"source_type":       read.Bundle.SourceType,
		"payloads":          fmt.Sprintf("%d", len(read.Bundle.Manifest.Payloads)),
		"referenced_blobs":  fmt.Sprintf("%d", len(adminMigrationUniqueBlobReferences(read.ReferencedChecksums))),
		"copied_blobs":      fmt.Sprintf("%d", len(read.CopiedChecksums)),
		"sidecar_artifacts": fmt.Sprintf("%d", len(read.Bundle.Manifest.Artifacts)),
	}))

	targetLoaded := false
	targetEmpty := false
	postgresRead := c.adminMigrationPostgresRead(ctx, cfg)
	out.Dependencies = append(out.Dependencies, postgresRead.Dependencies...)
	if postgresRead.Loaded {
		targetLoaded = true
		targetInventory := adminMigrationInventoryFromState(postgresRead.Bootstrap, postgresRead.CoreObjects, postgresRead.CookbookInventory, "")
		targetDependency, targetFindings := adminMigrationSourceImportTargetDependency(targetInventory, read.Bundle.Inventory)
		out.Dependencies = append(out.Dependencies, targetDependency)
		out.Findings = append(out.Findings, targetFindings...)
		targetEmpty = targetDependency.Status == "ok"
	} else {
		out.Dependencies = append(out.Dependencies, adminMigrationDependency{
			Name:       "source_import_target",
			Status:     "skipped",
			Backend:    "postgres",
			Configured: strings.TrimSpace(cfg.PostgresDSN) != "",
			Message:    "source import target emptiness check skipped because PostgreSQL state did not load",
		})
	}

	blobValidation := c.adminMigrationSourceImportBlobPreflight(ctx, cfg, read)
	out.Dependencies = append(out.Dependencies, blobValidation.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobValidation.Families...)
	out.Inventory.Families = adminMigrationSortedInventoryFamilies(out.Inventory.Families)
	out.Findings = append(out.Findings, blobValidation.Findings...)

	openSearchDependency := adminMigrationOpenSearchDependency(ctx, cfg)
	out.Dependencies = append(out.Dependencies, openSearchDependency)
	out.PlannedMutations = adminMigrationSourceImportPlannedMutations(read.Bundle.Inventory, read, targetLoaded, targetEmpty, out.Findings)

	adminMigrationCollectOutputStatuses(&out)
	return out
}

// applyAdminMigrationSourceImport materializes a validated source import into
// the offline target, rechecking empty-target safety just before writes begin.
func (c *command) applyAdminMigrationSourceImport(ctx context.Context, cfg config.Config, sourcePath string, read adminMigrationSourceImportRead, opts *adminMigrationFlagValues, out adminMigrationCLIOutput) adminMigrationCLIOutput {
	importState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		adminMigrationMarkError(&out, "source_import_state_invalid", "normalized source payloads could not be converted into OpenCook state", "source_import")
		return out
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_import_write",
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
	targetCookbookInventory, err := adminMigrationRestoreTargetCookbookInventory(store, previousBootstrap, previousCore, read.Bundle.Inventory)
	if err != nil {
		adminMigrationMarkError(&out, "postgres_cookbooks_unavailable", "source import target cookbook state could not be loaded", "source_import_target")
		return out
	}
	targetDependency, targetFindings := adminMigrationSourceImportTargetDependency(adminMigrationInventoryFromState(previousBootstrap, previousCore, targetCookbookInventory, ""), read.Bundle.Inventory)
	if targetDependency.Status == "error" {
		adminMigrationMarkDependency(&out, targetDependency)
		for _, finding := range targetFindings {
			adminMigrationMarkFinding(&out, finding)
		}
		return out
	}

	blobApply := c.adminMigrationSourceImportApplyBlobs(ctx, cfg, sourcePath, read, opts.progressPath)
	adminMigrationMarkDependency(&out, blobApply.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobApply.Families...)
	out.Inventory.Families = adminMigrationSortedInventoryFamilies(out.Inventory.Families)
	for _, finding := range blobApply.Findings {
		adminMigrationMarkFinding(&out, finding)
	}
	if blobApply.Dependency.Status == "error" || adminMigrationHasErrorFindings(blobApply.Findings) {
		return out
	}

	if err := adminMigrationSaveRestoredState(store, previousBootstrap, previousCore, importState.Bootstrap, importState.CoreObjects, importState.Cookbooks); err != nil {
		adminMigrationMarkError(&out, "source_import_write_failed", "source import metadata could not be written; PostgreSQL-backed metadata was rolled back where possible", "source_import_target")
		return out
	}
	if err := adminMigrationMarkSourceImportMetadataImported(sourcePath, opts.progressPath); err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_import_progress",
			Status:     "warning",
			Backend:    "filesystem",
			Configured: strings.TrimSpace(opts.progressPath) != "",
			Message:    "source import completed, but progress metadata could not be marked complete",
		})
	}

	adminMigrationMarkDependency(&out, adminMigrationDependency{
		Name:       "source_import_write",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "normalized source state imported into PostgreSQL-backed OpenCook metadata",
	})
	out.PlannedMutations = adminMigrationSourceImportCompletedMutations(read)
	out.Warnings = append(out.Warnings, "OpenSearch documents were not imported as source data; run opencook admin reindex --all-orgs --complete and opencook admin search check --all-orgs after starting the imported target")
	return out
}

// buildAdminMigrationSourceSyncPreflight plans a covered-family reconciliation
// from a normalized source snapshot to the current offline OpenCook target.
func (c *command) buildAdminMigrationSourceSyncPreflight(ctx context.Context, cfg config.Config, sourcePath string, read adminMigrationSourceImportRead, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationSourceSyncPreflightOutput(sourcePath, opts)
	out.Config = cfg.Redacted()
	out.Inventory = read.Bundle.Inventory
	out.Findings = append(out.Findings, read.Bundle.Findings...)
	cursor := adminMigrationSourceSyncCursor(read)
	out.Dependencies = append(out.Dependencies, adminMigrationSourceBundleDependency("ok", "normalized source manifest, payload hashes, and copied blobs are valid", map[string]string{
		"format_version":    read.Bundle.FormatVersion,
		"source_type":       read.Bundle.SourceType,
		"payloads":          fmt.Sprintf("%d", len(read.Bundle.Manifest.Payloads)),
		"referenced_blobs":  fmt.Sprintf("%d", len(adminMigrationUniqueBlobReferences(read.ReferencedChecksums))),
		"copied_blobs":      fmt.Sprintf("%d", len(read.CopiedChecksums)),
		"sidecar_artifacts": fmt.Sprintf("%d", len(read.Bundle.Manifest.Artifacts)),
		"source_cursor":     cursor,
	}))

	progress, progressDependency, progressFinding := adminMigrationSourceSyncProgressDependency(opts.progressPath, cursor)
	adminMigrationMarkDependency(&out, progressDependency)
	if progressFinding.Code != "" {
		adminMigrationMarkFinding(&out, progressFinding)
	}
	if !out.OK {
		return out
	}

	importState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		adminMigrationMarkError(&out, "source_sync_state_invalid", "normalized source payloads could not be converted into OpenCook state", "source_sync")
		return out
	}
	targetRead := c.adminMigrationLoadBackupState(ctx, cfg)
	out.Dependencies = append(out.Dependencies, targetRead.Dependencies...)
	if !targetRead.Loaded {
		adminMigrationCollectOutputStatuses(&out)
		return out
	}

	targetState := adminMigrationSourceImportState{
		Bootstrap:   targetRead.Bootstrap,
		CoreObjects: targetRead.CoreObjects,
		Cookbooks:   targetRead.Cookbooks,
	}
	scopes := adminMigrationSourceSyncCoveredScopes(read)
	diff := adminMigrationSourceSyncDiffStates(importState, targetState, scopes)
	out.Inventory.Families = append(out.Inventory.Families, diff.Families...)
	out.Inventory.Families = adminMigrationSortedInventoryFamilies(out.Inventory.Families)
	out.PlannedMutations = adminMigrationSourceSyncPlannedMutations(diff, progress)

	blobValidation := c.adminMigrationSourceImportBlobPreflight(ctx, cfg, read)
	out.Dependencies = append(out.Dependencies, blobValidation.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobValidation.Families...)
	out.Inventory.Families = adminMigrationSortedInventoryFamilies(out.Inventory.Families)
	out.Findings = append(out.Findings, blobValidation.Findings...)

	openSearchDependency := adminMigrationOpenSearchDependency(ctx, cfg)
	out.Dependencies = append(out.Dependencies, openSearchDependency)
	out.PlannedMutations = append(out.PlannedMutations, adminMigrationSourceSearchFollowupMutations(diff.HasChanges)...)
	if diff.DeleteCount > 0 {
		out.Warnings = append(out.Warnings, "source sync planned target-only deletes only for families explicitly present in the normalized source manifest")
	}

	adminMigrationCollectOutputStatuses(&out)
	return out
}

// applyAdminMigrationSourceSync copies required blobs, reconciles covered
// PostgreSQL families, and records a cursor only after metadata writes succeed.
func (c *command) applyAdminMigrationSourceSync(ctx context.Context, cfg config.Config, sourcePath string, read adminMigrationSourceImportRead, opts *adminMigrationFlagValues, out adminMigrationCLIOutput) adminMigrationCLIOutput {
	importState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		adminMigrationMarkError(&out, "source_sync_state_invalid", "normalized source payloads could not be converted into OpenCook state", "source_sync")
		return out
	}

	store, closeStore, err := c.newOfflineStore(ctx, cfg.PostgresDSN)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_sync_write",
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
	currentCookbooks := adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}}
	if loader, ok := store.(adminMigrationCookbookExportLoader); ok {
		cookbooks, err := loader.LoadCookbookExport(adminMigrationOrgNames(previousBootstrap, previousCore, nil, ""))
		if err != nil {
			adminMigrationMarkError(&out, "postgres_cookbooks_unavailable", "source sync target cookbook state could not be loaded", "source_sync_target")
			return out
		}
		currentCookbooks = cookbooks
	} else {
		adminMigrationMarkError(&out, "postgres_cookbooks_unavailable", "source sync requires cookbook export support from the offline store", "source_sync_target")
		return out
	}

	scopes := adminMigrationSourceSyncCoveredScopes(read)
	targetState := adminMigrationSourceImportState{Bootstrap: previousBootstrap, CoreObjects: previousCore, Cookbooks: currentCookbooks}
	diff := adminMigrationSourceSyncDiffStates(importState, targetState, scopes)
	merged := adminMigrationSourceSyncMergedState(targetState, importState, scopes)

	blobApply := c.adminMigrationSourceImportApplyBlobs(ctx, cfg, sourcePath, read, "")
	blobApply.Dependency.Name = "source_sync_blobs"
	if blobApply.Dependency.Message == "source import has no checksum blob references to copy or verify" {
		blobApply.Dependency.Message = "source sync has no checksum blob references to copy or verify"
	}
	adminMigrationMarkDependency(&out, blobApply.Dependency)
	out.Inventory.Families = append(out.Inventory.Families, blobApply.Families...)
	out.Inventory.Families = adminMigrationSortedInventoryFamilies(out.Inventory.Families)
	for _, finding := range blobApply.Findings {
		adminMigrationMarkFinding(&out, finding)
	}
	if blobApply.Dependency.Status == "error" || adminMigrationHasErrorFindings(blobApply.Findings) {
		return out
	}

	if diff.HasChanges {
		cookbookSync := adminMigrationCookbookExport{}
		if diff.CookbookChanges {
			cookbookSync = merged.Cookbooks
		}
		if err := adminMigrationSaveSyncedState(store, previousBootstrap, previousCore, merged.Bootstrap, merged.CoreObjects, cookbookSync, scopes, diff.CookbookChanges); err != nil {
			adminMigrationMarkError(&out, "source_sync_write_failed", "source sync metadata could not be written; PostgreSQL-backed metadata was rolled back where possible", "source_sync_target")
			return out
		}
	}
	if err := adminMigrationWriteSourceSyncProgress(sourcePath, opts.progressPath, adminMigrationSourceSyncProgress{
		SourceCursor:   adminMigrationSourceSyncCursor(read),
		LastStatus:     "applied",
		AppliedCursors: []string{adminMigrationSourceSyncCursor(read)},
	}); err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "source_sync_progress",
			Status:     "warning",
			Backend:    "filesystem",
			Configured: strings.TrimSpace(opts.progressPath) != "",
			Message:    "source sync completed, but cursor progress metadata could not be written",
		})
	}

	adminMigrationMarkDependency(&out, adminMigrationDependency{
		Name:       "source_sync_write",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "covered normalized source families reconciled into PostgreSQL-backed OpenCook metadata",
	})
	out.PlannedMutations = adminMigrationSourceSyncCompletedMutations(diff)
	if diff.HasChanges {
		out.Warnings = append(out.Warnings, "OpenSearch documents were not imported as source data; run opencook admin reindex --all-orgs --complete and opencook admin search check --all-orgs after source sync")
	}
	return out
}

// adminMigrationSourceImportPreflightOutput initializes the shared JSON envelope
// for both source-only failures and full target-readiness checks.
func adminMigrationSourceImportPreflightOutput(sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_source_import_preflight",
		Target:           adminMigrationTarget{SourcePath: adminMigrationRedactMigrationPath(sourcePath)},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
}

// adminMigrationReadSourceImportBundle verifies the normalized source contract
// and rebuilds the semantic payload graph without modifying source files.
func adminMigrationReadSourceImportBundle(sourcePath string) (adminMigrationSourceImportRead, error) {
	sourceRoot, manifestPath, err := adminMigrationResolveSourceImportManifest(sourcePath)
	if err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_missing", "source import requires a normalized source manifest")}}}, err
	}

	manifest, formatVersion, finding, err := adminMigrationLoadSourceManifestForNormalize(manifestPath)
	if err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{finding}}}, err
	}
	if formatVersion != adminMigrationChefSourceFormatV1 {
		finding := adminMigrationSourceErrorFinding("source_manifest_unsupported_format", "source import requires the normalized Chef source manifest format")
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{finding}}}, fmt.Errorf("unsupported source import manifest format %q", formatVersion)
	}
	if len(manifest.Payloads) == 0 {
		finding := adminMigrationSourceErrorFinding("source_manifest_payloads_missing", "source manifest does not declare importable payload files")
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{finding}}}, fmt.Errorf("source manifest has no payloads")
	}

	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	files := map[string][]byte{}
	normalizedPayloads := make([]adminMigrationSourceManifestPayload, 0, len(manifest.Payloads))
	for _, payload := range adminMigrationSortedSourceManifestPayloads(manifest.Payloads) {
		normalized, values, data, finding, err := adminMigrationReadSourceImportPayload(sourceRoot, payload)
		if err != nil {
			return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{finding}}}, err
		}
		key, err := adminMigrationSourcePayloadKeyFromManifest(normalized, values)
		if err != nil {
			return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}}, err
		}
		payloadValues[key] = append(payloadValues[key], values...)
		files[normalized.Path] = data
		normalizedPayloads = append(normalizedPayloads, normalized)
	}
	if err := adminMigrationNormalizeIdentityPayloads(payloadValues); err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}}, err
	}
	if err := adminMigrationNormalizeCoreObjectPayloads(payloadValues); err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}}, err
	}
	if err := adminMigrationNormalizeCookbookPayloads(payloadValues); err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}}, err
	}

	artifacts, artifactFiles, findings, err := adminMigrationCopyManifestArtifacts(sourceRoot, manifest.Artifacts)
	if err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_path_unsafe", "source manifest contains an unsafe artifact path")}}}, err
	}
	if err := adminMigrationValidateCopiedSourceBlobFiles(artifactFiles); err != nil {
		return adminMigrationSourceImportRead{Bundle: adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}}, err
	}
	findings = append(findings, adminMigrationValidateSourceManifestArtifactCounts(artifacts, artifactFiles)...)
	findings = append(findings, adminMigrationUnknownPayloadFamilyFindings(payloadValues)...)
	findings = append(findings, adminMigrationMissingCopiedSourceBlobFindings(payloadValues, artifactFiles)...)
	for path, data := range artifactFiles {
		files[path] = data
	}
	normalizedManifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    adminMigrationDefaultSourceType(manifest.SourceType, "normalized_chef_source"),
		Payloads:      normalizedPayloads,
		Artifacts:     adminMigrationSortedSourceManifestArtifacts(artifacts),
		Notes:         append([]string(nil), manifest.Notes...),
	}
	bundle := adminMigrationSourceNormalizeBundle{
		Manifest:      normalizedManifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(normalizedManifest),
		Findings:      findings,
		SourceType:    normalizedManifest.SourceType,
		FormatVersion: formatVersion,
	}
	return adminMigrationSourceImportRead{
		Bundle:              bundle,
		PayloadValues:       payloadValues,
		ReferencedChecksums: adminMigrationSourceImportBlobReferences(payloadValues),
		CopiedChecksums:     adminMigrationCopiedSourceChecksums(artifactFiles),
	}, nil
}

// adminMigrationResolveSourceImportManifest requires a normalized source
// manifest directory or manifest file instead of accepting raw source archives.
func adminMigrationResolveSourceImportManifest(sourcePath string) (string, string, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return "", "", fmt.Errorf("source path is required")
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return "", "", err
	}
	if info.IsDir() {
		manifestPath := filepath.Join(sourcePath, adminMigrationSourceManifestPath)
		if manifestInfo, err := os.Stat(manifestPath); err == nil && !manifestInfo.IsDir() {
			return sourcePath, manifestPath, nil
		}
		return "", "", fmt.Errorf("normalized source manifest not found")
	}
	if !adminMigrationSourceLooksLikeJSONManifest(sourcePath) {
		return "", "", fmt.Errorf("source import requires a normalized manifest file or directory")
	}
	return filepath.Dir(sourcePath), sourcePath, nil
}

// adminMigrationReadSourceImportPayload verifies one manifest payload hash and
// count before its bytes are admitted into the semantic source graph.
func adminMigrationReadSourceImportPayload(sourceRoot string, payload adminMigrationSourceManifestPayload) (adminMigrationSourceManifestPayload, []json.RawMessage, []byte, adminMigrationFinding, error) {
	relativePath, err := adminMigrationNormalizeSourceRelativePath(payload.Path)
	if err != nil {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_path_unsafe", "source manifest contains an unsafe payload path"), err
	}
	if strings.TrimSpace(payload.SHA256) == "" {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_payload_hash_missing", "source manifest payload is missing SHA-256 integrity metadata"), fmt.Errorf("source payload hash missing")
	}
	data, err := os.ReadFile(filepath.Join(sourceRoot, filepath.FromSlash(relativePath)))
	if err != nil {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_payload_unavailable", "source payload file could not be read"), err
	}
	if got := adminMigrationSHA256Hex(data); !strings.EqualFold(strings.TrimSpace(payload.SHA256), got) {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_payload_hash_mismatch", "source payload SHA-256 does not match manifest metadata"), fmt.Errorf("source payload hash mismatch")
	}
	values, err := adminMigrationCanonicalSourcePayloadValues(data)
	if err != nil {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload JSON could not be parsed"), err
	}
	if payload.Count != len(values) {
		return adminMigrationSourceManifestPayload{}, nil, nil, adminMigrationSourceErrorFinding("source_payload_count_mismatch", "source payload object count does not match manifest metadata"), fmt.Errorf("source payload count mismatch")
	}
	payload.Path = relativePath
	payload.SHA256 = strings.ToLower(strings.TrimSpace(payload.SHA256))
	return payload, values, append([]byte(nil), data...), adminMigrationFinding{}, nil
}

// adminMigrationValidateSourceManifestArtifactCounts compares manifest sidecar
// counts to files copied into memory so omitted or extra artifacts are visible.
func adminMigrationValidateSourceManifestArtifactCounts(artifacts []adminMigrationSourceManifestArtifact, files map[string][]byte) []adminMigrationFinding {
	var findings []adminMigrationFinding
	for _, artifact := range artifacts {
		if artifact.Count <= 0 || strings.TrimSpace(artifact.Path) == "" {
			continue
		}
		prefix := strings.Trim(strings.TrimSpace(artifact.Path), "/")
		count := 0
		for path := range files {
			if path == prefix || strings.HasPrefix(path, prefix+"/") {
				count++
			}
		}
		if count != artifact.Count {
			findings = append(findings, adminMigrationFinding{
				Severity: "error",
				Code:     "source_artifact_count_mismatch",
				Family:   strings.TrimSpace(artifact.Family),
				Message:  "source side-channel artifact count does not match manifest metadata",
			})
		}
	}
	return findings
}

// adminMigrationSourceBundleDependency keeps source import integrity checks in
// the same dependency shape as backup/restore bundle validation.
func adminMigrationSourceBundleDependency(status, message string, details map[string]string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       "source_bundle",
		Status:     status,
		Backend:    "filesystem",
		Configured: true,
		Message:    message,
		Details:    details,
	}
}

// adminMigrationSourceImportTargetDependency refuses non-empty targets until a
// later task defines source-vs-target conflict and update behavior explicitly.
func adminMigrationSourceImportTargetDependency(target, expected adminMigrationInventory) (adminMigrationDependency, []adminMigrationFinding) {
	existing := adminMigrationInventoryTotalCount(target)
	dep := adminMigrationDependency{
		Name:       "source_import_target",
		Status:     "ok",
		Backend:    "postgres",
		Configured: true,
		Message:    "source import target PostgreSQL-backed state is empty",
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
	dep.Message = "source import target PostgreSQL-backed state is not empty"
	dep.Details["state"] = "non_empty"
	return dep, []adminMigrationFinding{{
		Severity: "error",
		Code:     "source_import_target_not_empty",
		Family:   "source_import_target",
		Message:  "source import preflight refuses non-empty PostgreSQL-backed targets until explicit conflict behavior exists",
	}}
}

// adminMigrationSourceImportBlobPreflight checks target blob readiness while
// requiring provider reachability only for source references without copied bytes.
func (c *command) adminMigrationSourceImportBlobPreflight(ctx context.Context, cfg config.Config, read adminMigrationSourceImportRead) adminMigrationBlobValidation {
	allRefs := adminMigrationUniqueBlobReferences(read.ReferencedChecksums)
	providerRefs := adminMigrationSourceImportProviderBlobReferences(allRefs, read.CopiedChecksums)
	families := []adminMigrationInventoryFamily{
		{Family: "referenced_blobs", Count: len(allRefs)},
		{Family: "copied_blobs", Count: len(read.CopiedChecksums)},
		{Family: "provider_verified_blobs", Count: 0},
		{Family: "missing_blobs", Count: 0},
		{Family: "provider_unavailable_checks", Count: 0},
	}
	if len(allRefs) == 0 && len(read.CopiedChecksums) == 0 {
		return adminMigrationBlobValidation{
			Dependency: adminMigrationDependency{Name: "blob", Status: "skipped", Backend: adminMigrationBlobBackendLabel(cfg), Configured: strings.TrimSpace(cfg.BlobBackend) != "", Message: "source import has no checksum blob references to validate"},
			Families:   families,
		}
	}

	newBlobStore := c.newBlobStore
	if newBlobStore == nil {
		newBlobStore = blob.NewStore
	}
	store, err := newBlobStore(cfg)
	if err != nil {
		dep := adminMigrationDependency{Name: "blob", Status: "error", Backend: adminMigrationBlobBackendLabel(cfg), Configured: false, Message: adminMigrationSafeErrorMessage("blob", err)}
		families[4].Count = len(providerRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be validated because the blob backend is unavailable",
		}}}
	}
	status := store.Status()
	dep := adminMigrationDependency{
		Name:       "blob",
		Status:     "ok",
		Backend:    status.Backend,
		Configured: status.Configured,
		Message:    status.Message,
		Details: map[string]string{
			"referenced_blobs":          fmt.Sprintf("%d", len(allRefs)),
			"copied_blobs":              fmt.Sprintf("%d", len(read.CopiedChecksums)),
			"provider_reference_checks": fmt.Sprintf("%d", len(providerRefs)),
		},
	}
	if !status.Configured {
		dep.Status = "error"
		families[4].Count = len(providerRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be validated because the blob backend is unavailable",
		}}}
	}
	checker, ok := store.(blob.Checker)
	if !ok {
		dep.Status = "error"
		dep.Message = "blob backend does not expose checksum existence checks"
		families[4].Count = len(providerRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be validated because the blob backend cannot check checksum existence",
		}}}
	}
	if _, err := checker.Exists(ctx, adminMigrationBlobProbeChecksum); err != nil {
		dep.Status = "error"
		dep.Message = adminMigrationSafeErrorMessage("blob", err)
		families[4].Count = len(providerRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be validated because the blob backend is unavailable",
		}}}
	}
	dep.Message = status.Message + "; checksum existence probe succeeded"

	var findings []adminMigrationFinding
	verified := 0
	missing := 0
	unavailable := 0
	for _, ref := range providerRefs {
		exists, err := checker.Exists(ctx, ref.Checksum)
		if err != nil {
			unavailable++
			findings = append(findings, adminMigrationBlobFinding("error", "blob_check_unavailable", ref, "blob backend could not verify source checksum "+ref.Checksum))
			continue
		}
		if !exists {
			missing++
			findings = append(findings, adminMigrationBlobFinding("error", "source_blob_payload_missing", ref, "source metadata references checksum "+ref.Checksum+" without copied bytes and the configured blob backend does not contain it"))
			continue
		}
		verified++
	}
	families[2].Count = verified
	families[3].Count = missing
	families[4].Count = unavailable
	return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: findings}
}

// adminMigrationSourceImportApplyBlobs copies source-provided blob bytes and
// verifies provider-only references before any imported metadata can point at them.
func (c *command) adminMigrationSourceImportApplyBlobs(ctx context.Context, cfg config.Config, sourcePath string, read adminMigrationSourceImportRead, progressPath string) adminMigrationBlobValidation {
	allRefs := adminMigrationUniqueBlobReferences(read.ReferencedChecksums)
	copiedRefs := adminMigrationSourceImportCopiedBlobReferences(allRefs, read.CopiedChecksums)
	providerRefs := adminMigrationSourceImportProviderBlobReferences(allRefs, read.CopiedChecksums)
	families := []adminMigrationInventoryFamily{
		{Family: "copied_blob_writes", Count: 0},
		{Family: "provider_verified_blobs", Count: 0},
		{Family: "progress_reused_blobs", Count: 0},
		{Family: "missing_blobs", Count: 0},
		{Family: "provider_unavailable_checks", Count: 0},
	}
	if len(allRefs) == 0 {
		return adminMigrationBlobValidation{
			Dependency: adminMigrationDependency{Name: "source_import_blobs", Status: "skipped", Backend: adminMigrationBlobBackendLabel(cfg), Configured: strings.TrimSpace(cfg.BlobBackend) != "", Message: "source import has no checksum blob references to copy or verify"},
			Families:   families,
		}
	}

	newBlobStore := c.newBlobStore
	if newBlobStore == nil {
		newBlobStore = blob.NewStore
	}
	store, err := newBlobStore(cfg)
	if err != nil {
		dep := adminMigrationDependency{Name: "source_import_blobs", Status: "error", Backend: adminMigrationBlobBackendLabel(cfg), Configured: false, Message: adminMigrationSafeErrorMessage("blob", err)}
		families[4].Count = len(allRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be copied or verified because the blob backend is unavailable",
		}}}
	}
	status := store.Status()
	dep := adminMigrationDependency{
		Name:       "source_import_blobs",
		Status:     "ok",
		Backend:    status.Backend,
		Configured: status.Configured,
		Message:    "referenced source blob content copied or verified",
		Details: map[string]string{
			"referenced_blobs":           fmt.Sprintf("%d", len(allRefs)),
			"copied_reference_blobs":     fmt.Sprintf("%d", len(copiedRefs)),
			"provider_reference_checks":  fmt.Sprintf("%d", len(providerRefs)),
			"progress_path":              adminMigrationRedactMigrationPath(progressPath),
			"progress_metadata_imported": "false",
		},
	}
	if !status.Configured {
		dep.Status = "error"
		dep.Message = status.Message
		families[4].Count = len(allRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be copied or verified because the blob backend is unavailable",
		}}}
	}
	checker, ok := store.(blob.Checker)
	if !ok {
		dep.Status = "error"
		dep.Message = "blob backend does not expose checksum existence checks"
		families[4].Count = len(allRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_check_unavailable",
			Family:   "checksum_references",
			Message:  "blob backend cannot verify source import checksum references",
		}}}
	}
	if _, err := checker.Exists(ctx, adminMigrationBlobProbeChecksum); err != nil {
		dep.Status = "error"
		dep.Message = adminMigrationSafeErrorMessage("blob", err)
		families[4].Count = len(allRefs)
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_provider_unavailable",
			Family:   "checksum_references",
			Message:  "source import checksum blobs could not be copied or verified because the blob backend is unavailable",
		}}}
	}
	putter, ok := store.(blob.Putter)
	if len(copiedRefs) > 0 && !ok {
		dep.Status = "error"
		dep.Message = "blob backend does not support copied source blob writes"
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "blob_put_unavailable",
			Family:   "checksum_references",
			Message:  "source import includes copied blob bytes but the target backend cannot accept blob writes",
		}}}
	}

	progress, err := adminMigrationReadSourceImportProgress(progressPath)
	if err != nil {
		dep.Status = "error"
		dep.Message = "source import progress metadata could not be read"
		return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: []adminMigrationFinding{{
			Severity: "error",
			Code:     "source_import_progress_invalid",
			Family:   "source_import_progress",
			Message:  "source import progress metadata could not be read",
		}}}
	}
	if progress.MetadataImported {
		dep.Details["progress_metadata_imported"] = "true"
	}

	copiedBodies := adminMigrationSourceImportCopiedBlobBodies(read)
	copiedProgress := adminMigrationSourceImportProgressSet(progress.CopiedBlobs)
	verifiedProgress := adminMigrationSourceImportProgressSet(progress.VerifiedBlobs)
	var findings []adminMigrationFinding
	for _, ref := range copiedRefs {
		checksum := strings.ToLower(strings.TrimSpace(ref.Checksum))
		if copiedProgress[checksum] {
			if exists, err := checker.Exists(ctx, checksum); err == nil && exists {
				families[2].Count++
				continue
			}
		}
		body, ok := copiedBodies[checksum]
		if !ok {
			dep.Status = "error"
			families[3].Count++
			findings = append(findings, adminMigrationBlobFinding("error", "source_blob_payload_missing", ref, "source metadata references checksum "+checksum+" but the normalized source bundle does not contain copied bytes"))
			continue
		}
		if got := adminMigrationMD5Hex(body); got != checksum {
			dep.Status = "error"
			findings = append(findings, adminMigrationBlobFinding("error", "source_blob_checksum_mismatch", ref, "copied source blob bytes do not match checksum "+checksum))
			continue
		}
		if _, err := putter.Put(ctx, blob.PutRequest{Key: checksum, ContentType: "application/octet-stream", Body: body}); err != nil {
			dep.Status = "error"
			families[4].Count++
			findings = append(findings, adminMigrationBlobFinding("error", "source_import_blob_copy_failed", ref, "source import could not copy checksum "+checksum+" into the target blob backend"))
			continue
		}
		families[0].Count++
		progress.CopiedBlobs = adminMigrationUniqueSortedStrings(append(progress.CopiedBlobs, checksum))
		if err := adminMigrationWriteSourceImportProgress(sourcePath, progressPath, progress); err != nil {
			dep.Status = "error"
			findings = append(findings, adminMigrationFinding{Severity: "error", Code: "source_import_progress_write_failed", Family: "source_import_progress", Message: "source import progress metadata could not be written after copying a blob"})
			break
		}
	}
	for _, ref := range providerRefs {
		checksum := strings.ToLower(strings.TrimSpace(ref.Checksum))
		if verifiedProgress[checksum] {
			if exists, err := checker.Exists(ctx, checksum); err == nil && exists {
				families[2].Count++
				continue
			}
		}
		exists, err := checker.Exists(ctx, checksum)
		if err != nil {
			dep.Status = "error"
			families[4].Count++
			findings = append(findings, adminMigrationBlobFinding("error", "blob_check_unavailable", ref, "blob backend could not verify source checksum "+checksum))
			continue
		}
		if !exists {
			dep.Status = "error"
			families[3].Count++
			findings = append(findings, adminMigrationBlobFinding("error", "source_blob_payload_missing", ref, "source metadata references checksum "+checksum+" without copied bytes and the configured blob backend does not contain it"))
			continue
		}
		families[1].Count++
		progress.VerifiedBlobs = adminMigrationUniqueSortedStrings(append(progress.VerifiedBlobs, checksum))
		if err := adminMigrationWriteSourceImportProgress(sourcePath, progressPath, progress); err != nil {
			dep.Status = "error"
			findings = append(findings, adminMigrationFinding{Severity: "error", Code: "source_import_progress_write_failed", Family: "source_import_progress", Message: "source import progress metadata could not be written after verifying a blob"})
			break
		}
	}
	if dep.Status == "error" {
		dep.Message = "one or more source import blob payloads could not be copied or verified"
	}
	return adminMigrationBlobValidation{Dependency: dep, Families: families, Findings: findings}
}

// adminMigrationSourceImportPlannedMutations summarizes what the later import
// apply command would do if every dependency and conflict gate passes.
func adminMigrationSourceImportPlannedMutations(sourceInventory adminMigrationInventory, read adminMigrationSourceImportRead, targetLoaded, targetEmpty bool, findings []adminMigrationFinding) []adminMigrationPlannedMutation {
	var mutations []adminMigrationPlannedMutation
	if targetLoaded && !targetEmpty {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "conflict",
			Family:  "postgres",
			Count:   adminMigrationInventoryTotalCount(sourceInventory),
			Message: "source import is blocked because the PostgreSQL-backed target is not empty",
		})
	} else {
		for _, family := range sourceInventory.Families {
			switch family.Family {
			case "cookbook_blob_references":
				continue
			case "opensearch_source_artifacts":
				mutations = append(mutations, adminMigrationPlannedMutation{
					Action:  "skip",
					Family:  "opensearch",
					Count:   family.Count,
					Message: "would skip source OpenSearch artifacts because search is derived state",
				})
			default:
				mutations = append(mutations, adminMigrationPlannedMutation{
					Action:       "create",
					Organization: family.Organization,
					Family:       family.Family,
					Count:        family.Count,
					Message:      "would create PostgreSQL-backed " + family.Family + " records from normalized source payloads",
				})
			}
		}
	}
	mutations = append(mutations, adminMigrationSourceImportUnsupportedMutations(findings)...)
	if len(read.CopiedChecksums) > 0 {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "copy_blob_objects",
			Family:  "blobs",
			Count:   len(read.CopiedChecksums),
			Message: "would copy checksum-addressed source blob bytes into the configured OpenCook blob backend before metadata import",
		})
	}
	if providerRefs := adminMigrationSourceImportProviderBlobReferences(adminMigrationUniqueBlobReferences(read.ReferencedChecksums), read.CopiedChecksums); len(providerRefs) > 0 {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "verify_blob_references",
			Family:  "blobs",
			Count:   len(providerRefs),
			Message: "would verify checksum-addressed source blob references already exist in the configured blob backend",
		})
	}
	mutations = append(mutations, adminMigrationPlannedMutation{
		Action:  "rebuild_opensearch",
		Family:  "opensearch",
		Message: "would rebuild OpenSearch derived state from imported PostgreSQL state after import",
	})
	mutations = append(mutations, adminMigrationSourceSearchFollowupMutations(true)...)
	return mutations
}

// adminMigrationSourceImportUnsupportedMutations turns source warnings into
// explicit skip rows so operators can see what will not be imported.
func adminMigrationSourceImportUnsupportedMutations(findings []adminMigrationFinding) []adminMigrationPlannedMutation {
	seen := map[string]bool{}
	var mutations []adminMigrationPlannedMutation
	for _, finding := range findings {
		switch finding.Code {
		case "source_artifact_unsupported", "source_family_unsupported", "source_cookbook_layout_unsupported":
		default:
			continue
		}
		family := strings.TrimSpace(finding.Family)
		if family == "" {
			family = "source_inventory"
		}
		key := finding.Code + "/" + family
		if seen[key] {
			continue
		}
		seen[key] = true
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "unsupported",
			Family:  family,
			Message: "would not import unsupported or deferred source family " + family,
		})
	}
	return mutations
}

// adminMigrationSourceImportBlobReferences preserves checksum context from the
// normalized checksum reference rows for provider diagnostics.
func adminMigrationSourceImportBlobReferences(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) []adminMigrationBlobReference {
	var refs []adminMigrationBlobReference
	for key, values := range payloadValues {
		if key.Family != "checksum_references" {
			continue
		}
		for _, raw := range values {
			object, err := adminMigrationDecodeSourceObject(raw)
			if err != nil {
				continue
			}
			checksum := strings.ToLower(adminMigrationSourceString(object, "checksum", "id"))
			if !adminMigrationChecksumPattern.MatchString(checksum) {
				continue
			}
			family := adminMigrationSourceString(object, "family", "source_family", "type")
			if family == "" {
				family = "checksum_references"
			}
			refs = append(refs, adminMigrationBlobReference{Checksum: checksum, Organization: key.Organization, Family: family})
		}
	}
	return adminMigrationUniqueBlobReferences(refs)
}

// adminMigrationCopiedSourceChecksums extracts checksums whose bytes are present
// in the normalized source artifact and therefore need copy planning, not
// provider existence planning.
func adminMigrationCopiedSourceChecksums(files map[string][]byte) map[string]bool {
	checksums := map[string]bool{}
	for path := range files {
		if checksum := adminMigrationCopiedSourceBlobChecksum(path); checksum != "" {
			checksums[checksum] = true
		}
	}
	return checksums
}

// adminMigrationSourceImportProviderBlobReferences filters out references with
// copied source bytes because those will be copied during the apply phase.
func adminMigrationSourceImportProviderBlobReferences(refs []adminMigrationBlobReference, copied map[string]bool) []adminMigrationBlobReference {
	var providerRefs []adminMigrationBlobReference
	for _, ref := range adminMigrationUniqueBlobReferences(refs) {
		if copied[strings.ToLower(strings.TrimSpace(ref.Checksum))] {
			continue
		}
		providerRefs = append(providerRefs, ref)
	}
	return providerRefs
}

// adminMigrationSourceImportCopiedBlobReferences filters metadata references to
// only the checksum blobs whose byte payloads are present in the source bundle.
func adminMigrationSourceImportCopiedBlobReferences(refs []adminMigrationBlobReference, copied map[string]bool) []adminMigrationBlobReference {
	var copiedRefs []adminMigrationBlobReference
	for _, ref := range adminMigrationUniqueBlobReferences(refs) {
		if !copied[strings.ToLower(strings.TrimSpace(ref.Checksum))] {
			continue
		}
		copiedRefs = append(copiedRefs, ref)
	}
	return copiedRefs
}

// adminMigrationSourceImportCopiedBlobBodies maps checksum-addressed source
// sidecar files to the bytes that must be written into the target blob backend.
func adminMigrationSourceImportCopiedBlobBodies(read adminMigrationSourceImportRead) map[string][]byte {
	out := map[string][]byte{}
	for path, body := range read.Bundle.Files {
		checksum := adminMigrationCopiedSourceBlobChecksum(path)
		if checksum == "" {
			continue
		}
		out[checksum] = append([]byte(nil), body...)
	}
	return out
}

// adminMigrationSourceImportProgressFile picks the retry metadata location,
// defaulting beside the normalized source manifest when the operator omits it.
func adminMigrationSourceImportProgressFile(sourcePath, requested string) string {
	if requested = strings.TrimSpace(requested); requested != "" {
		return requested
	}
	if sourceRoot, _, err := adminMigrationResolveSourceImportManifest(sourcePath); err == nil {
		return filepath.Join(sourceRoot, adminMigrationSourceImportProgressPath)
	}
	if info, err := os.Stat(sourcePath); err == nil && info.IsDir() {
		return filepath.Join(sourcePath, adminMigrationSourceImportProgressPath)
	}
	return filepath.Join(filepath.Dir(sourcePath), adminMigrationSourceImportProgressPath)
}

// adminMigrationReadSourceImportProgress tolerates a missing progress file so
// first attempts and clean retries share one code path.
func adminMigrationReadSourceImportProgress(path string) (adminMigrationSourceImportProgress, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return adminMigrationSourceImportProgress{FormatVersion: adminMigrationSourceImportProgressFormatV1}, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return adminMigrationSourceImportProgress{FormatVersion: adminMigrationSourceImportProgressFormatV1}, nil
	}
	if err != nil {
		return adminMigrationSourceImportProgress{}, err
	}
	var progress adminMigrationSourceImportProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return adminMigrationSourceImportProgress{}, err
	}
	if progress.FormatVersion != "" && progress.FormatVersion != adminMigrationSourceImportProgressFormatV1 {
		return adminMigrationSourceImportProgress{}, fmt.Errorf("unsupported source import progress format %q", progress.FormatVersion)
	}
	progress.FormatVersion = adminMigrationSourceImportProgressFormatV1
	progress.CopiedBlobs = adminMigrationUniqueSortedStrings(progress.CopiedBlobs)
	progress.VerifiedBlobs = adminMigrationUniqueSortedStrings(progress.VerifiedBlobs)
	return progress, nil
}

// adminMigrationWriteSourceImportProgress atomically rewrites retry metadata
// after each non-transactional blob phase so interrupted imports can resume.
func adminMigrationWriteSourceImportProgress(sourcePath, path string, progress adminMigrationSourceImportProgress) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	progress.FormatVersion = adminMigrationSourceImportProgressFormatV1
	progress.SourcePath = adminMigrationRedactMigrationPath(sourcePath)
	progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	progress.CopiedBlobs = adminMigrationUniqueSortedStrings(progress.CopiedBlobs)
	progress.VerifiedBlobs = adminMigrationUniqueSortedStrings(progress.VerifiedBlobs)
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// adminMigrationMarkSourceImportMetadataImported records successful metadata
// publication after PostgreSQL writes complete.
func adminMigrationMarkSourceImportMetadataImported(sourcePath, progressPath string) error {
	progress, err := adminMigrationReadSourceImportProgress(progressPath)
	if err != nil {
		return err
	}
	progress.MetadataImported = true
	return adminMigrationWriteSourceImportProgress(sourcePath, progressPath, progress)
}

// adminMigrationSourceImportProgressSet lets retries skip already completed
// blob work only after the target backend still confirms the checksum exists.
func adminMigrationSourceImportProgressSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = true
		}
	}
	return out
}

// adminMigrationSourceSyncPreflightOutput initializes the sync envelope with
// redacted source/progress paths before provider configuration is reported.
func adminMigrationSourceSyncPreflightOutput(sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:               true,
		Command:          "migration_source_sync_preflight",
		Target:           adminMigrationTarget{SourcePath: adminMigrationRedactMigrationPath(sourcePath), ProgressPath: adminMigrationRedactMigrationPath(opts.progressPath)},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
}

// adminMigrationSourceSyncProgressFile picks the cursor metadata location,
// defaulting beside the normalized source manifest just like import progress.
func adminMigrationSourceSyncProgressFile(sourcePath, requested string) string {
	if requested = strings.TrimSpace(requested); requested != "" {
		return requested
	}
	if sourceRoot, _, err := adminMigrationResolveSourceImportManifest(sourcePath); err == nil {
		return filepath.Join(sourceRoot, adminMigrationSourceSyncProgressPath)
	}
	if info, err := os.Stat(sourcePath); err == nil && info.IsDir() {
		return filepath.Join(sourcePath, adminMigrationSourceSyncProgressPath)
	}
	return filepath.Join(filepath.Dir(sourcePath), adminMigrationSourceSyncProgressPath)
}

// adminMigrationReadSourceSyncProgress tolerates missing cursor metadata so a
// first sync and a retry both go through the same validation path.
func adminMigrationReadSourceSyncProgress(path string) (adminMigrationSourceSyncProgress, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return adminMigrationSourceSyncProgress{FormatVersion: adminMigrationSourceSyncProgressFormatV1}, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return adminMigrationSourceSyncProgress{FormatVersion: adminMigrationSourceSyncProgressFormatV1}, nil
	}
	if err != nil {
		return adminMigrationSourceSyncProgress{}, err
	}
	var progress adminMigrationSourceSyncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return adminMigrationSourceSyncProgress{}, err
	}
	if progress.FormatVersion != "" && progress.FormatVersion != adminMigrationSourceSyncProgressFormatV1 {
		return adminMigrationSourceSyncProgress{}, fmt.Errorf("unsupported source sync progress format %q", progress.FormatVersion)
	}
	progress.FormatVersion = adminMigrationSourceSyncProgressFormatV1
	progress.AppliedCursors = adminMigrationUniqueSortedStrings(progress.AppliedCursors)
	return progress, nil
}

// adminMigrationWriteSourceSyncProgress atomically records the applied source
// cursor after sync metadata writes succeed, avoiding timestamp-only freshness.
func adminMigrationWriteSourceSyncProgress(sourcePath, path string, progress adminMigrationSourceSyncProgress) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	existing, err := adminMigrationReadSourceSyncProgress(path)
	if err != nil {
		return err
	}
	progress.FormatVersion = adminMigrationSourceSyncProgressFormatV1
	progress.SourcePath = adminMigrationRedactMigrationPath(sourcePath)
	progress.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	progress.AppliedCursors = adminMigrationUniqueSortedStrings(append(existing.AppliedCursors, progress.AppliedCursors...))
	if progress.SourceCursor == "" {
		progress.SourceCursor = existing.SourceCursor
	}
	if progress.LastStatus == "" {
		progress.LastStatus = existing.LastStatus
	}
	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.tmp.%d", path, os.Getpid())
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// adminMigrationSourceSyncProgressDependency validates cursor metadata without
// treating a prior successful cursor as proof that the target is still synced.
func adminMigrationSourceSyncProgressDependency(path, cursor string) (adminMigrationSourceSyncProgress, adminMigrationDependency, adminMigrationFinding) {
	progress, err := adminMigrationReadSourceSyncProgress(path)
	dep := adminMigrationDependency{
		Name:       "source_sync_progress",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: strings.TrimSpace(path) != "",
		Message:    "source sync cursor metadata is readable",
		Details: map[string]string{
			"progress_path": adminMigrationRedactMigrationPath(path),
			"source_cursor": cursor,
			"last_status":   strings.TrimSpace(progress.LastStatus),
		},
	}
	if dep.Details["last_status"] == "" {
		dep.Details["last_status"] = "none"
	}
	if err == nil {
		if adminMigrationSourceSyncProgressSet(progress.AppliedCursors)[cursor] {
			dep.Details["cursor_seen"] = "true"
		} else {
			dep.Details["cursor_seen"] = "false"
		}
		return progress, dep, adminMigrationFinding{}
	}
	dep.Status = "error"
	dep.Message = "source sync progress metadata could not be read"
	return adminMigrationSourceSyncProgress{}, dep, adminMigrationFinding{
		Severity: "error",
		Code:     "source_sync_progress_invalid",
		Family:   "source_sync_progress",
		Message:  "source sync progress metadata could not be read",
	}
}

// adminMigrationSourceSyncProgressSet gives cursor lookups stable semantics
// even if a progress file was hand-edited into a non-sorted order.
func adminMigrationSourceSyncProgressSet(values []string) map[string]bool {
	out := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out[value] = true
		}
	}
	return out
}

// adminMigrationSourceSyncCursor hashes the normalized manifest, payloads, and
// copied sidecar bytes so sync freshness does not depend on file modification times.
func adminMigrationSourceSyncCursor(read adminMigrationSourceImportRead) string {
	hash := sha256.New()
	hash.Write([]byte(read.Bundle.FormatVersion))
	hash.Write([]byte{0})
	hash.Write([]byte(read.Bundle.SourceType))
	hash.Write([]byte{0})
	manifest, _ := json.Marshal(read.Bundle.Manifest)
	hash.Write(manifest)
	for _, path := range adminMigrationSortedMapKeys(read.Bundle.Files) {
		hash.Write([]byte{0})
		hash.Write([]byte(path))
		hash.Write([]byte{0})
		hash.Write(read.Bundle.Files[path])
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// adminMigrationSourceSyncCoveredScopes freezes deletion intent to exactly the
// payload families present in the source manifest; absent families are preserved.
func adminMigrationSourceSyncCoveredScopes(read adminMigrationSourceImportRead) map[adminMigrationSourcePayloadKey]bool {
	scopes := map[adminMigrationSourcePayloadKey]bool{}
	for key, values := range read.PayloadValues {
		if len(values) == 0 {
			continue
		}
		if adminMigrationSourceSyncFamilyStored(key.Family) {
			scopes[key] = true
		}
	}
	return scopes
}

// adminMigrationSourceSyncFamilyStored filters out side-channel and advisory
// source families that do not correspond to persisted PostgreSQL rows.
func adminMigrationSourceSyncFamilyStored(family string) bool {
	switch strings.TrimSpace(family) {
	case "users", "user_acls", "user_keys", "server_admin_memberships", "organizations",
		"clients", "client_keys", "groups", "group_memberships", "containers", "acls",
		"nodes", "environments", "roles", "data_bags", "data_bag_items",
		"policy_revisions", "policy_groups", "policy_assignments", "sandboxes",
		"checksum_references", "cookbook_versions", "cookbook_artifacts":
		return true
	default:
		return false
	}
}

// adminMigrationSourceSyncDiffStates compares canonical row digests for each
// manifest-covered family so unchanged rows remain idempotent across reruns.
func adminMigrationSourceSyncDiffStates(source, target adminMigrationSourceImportState, scopes map[adminMigrationSourcePayloadKey]bool) adminMigrationSourceSyncDiff {
	sourceRecords := adminMigrationSourceSyncRecords(source, scopes)
	targetRecords := adminMigrationSourceSyncRecords(target, scopes)
	keys := make([]adminMigrationSourcePayloadKey, 0, len(scopes))
	for key := range scopes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Organization != keys[j].Organization {
			return keys[i].Organization < keys[j].Organization
		}
		return keys[i].Family < keys[j].Family
	})

	diff := adminMigrationSourceSyncDiff{}
	for _, key := range keys {
		familyDiff := adminMigrationSourceSyncFamilyDiff{Key: key}
		for id, sourceDigest := range sourceRecords[key] {
			targetDigest, exists := targetRecords[key][id]
			if !exists {
				familyDiff.Creates++
				continue
			}
			if sourceDigest != targetDigest {
				familyDiff.Updates++
				continue
			}
			familyDiff.Unchanged++
		}
		for id := range targetRecords[key] {
			if _, exists := sourceRecords[key][id]; !exists {
				familyDiff.Deletes++
			}
		}
		diff.Details = append(diff.Details, familyDiff)
		if familyDiff.Creates+familyDiff.Updates+familyDiff.Deletes > 0 {
			diff.HasChanges = true
		}
		if familyDiff.Deletes > 0 {
			diff.DeleteCount += familyDiff.Deletes
		}
		if key.Family == "cookbook_versions" || key.Family == "cookbook_artifacts" {
			if familyDiff.Creates+familyDiff.Updates+familyDiff.Deletes > 0 {
				diff.CookbookChanges = true
			}
		}
		diff.Families = append(diff.Families,
			adminMigrationInventoryFamily{Organization: key.Organization, Family: key.Family + "_creates", Count: familyDiff.Creates},
			adminMigrationInventoryFamily{Organization: key.Organization, Family: key.Family + "_updates", Count: familyDiff.Updates},
			adminMigrationInventoryFamily{Organization: key.Organization, Family: key.Family + "_deletes", Count: familyDiff.Deletes},
			adminMigrationInventoryFamily{Organization: key.Organization, Family: key.Family + "_unchanged", Count: familyDiff.Unchanged},
		)
	}
	diff.Mutations = adminMigrationSourceSyncMutationDetails(diff.Details)
	return diff
}

// adminMigrationSourceSyncMutationDetails turns per-family row counts into
// stable planned mutation records for scripts and migration runbooks.
func adminMigrationSourceSyncMutationDetails(details []adminMigrationSourceSyncFamilyDiff) []adminMigrationPlannedMutation {
	var mutations []adminMigrationPlannedMutation
	for _, detail := range details {
		for _, item := range []struct {
			action  string
			count   int
			message string
		}{
			{action: "create", count: detail.Creates, message: "source sync would create PostgreSQL-backed " + detail.Key.Family + " records from normalized source payloads"},
			{action: "update", count: detail.Updates, message: "source sync would update PostgreSQL-backed " + detail.Key.Family + " records from normalized source payloads"},
			{action: "delete", count: detail.Deletes, message: "source sync would delete target-only " + detail.Key.Family + " records because the source manifest covers that family"},
			{action: "unchanged", count: detail.Unchanged, message: "source sync found unchanged PostgreSQL-backed " + detail.Key.Family + " records"},
		} {
			if item.count == 0 {
				continue
			}
			mutations = append(mutations, adminMigrationPlannedMutation{
				Action:       item.action,
				Organization: detail.Key.Organization,
				Family:       detail.Key.Family,
				Count:        item.count,
				Message:      item.message,
			})
		}
	}
	return mutations
}

// adminMigrationSourceSyncPlannedMutations adds source-sync bookkeeping around
// the row-level diff without mutating target state.
func adminMigrationSourceSyncPlannedMutations(diff adminMigrationSourceSyncDiff, progress adminMigrationSourceSyncProgress) []adminMigrationPlannedMutation {
	mutations := append([]adminMigrationPlannedMutation(nil), diff.Mutations...)
	if !diff.HasChanges {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "noop",
			Family:  "postgres",
			Message: "source sync found no PostgreSQL metadata changes for manifest-covered families",
		})
	}
	if diff.DeleteCount > 0 {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "confirm_destructive_deletes",
			Family:  "postgres",
			Count:   diff.DeleteCount,
			Message: "source sync apply requires --yes before deleting target-only rows from manifest-covered families",
		})
	}
	if strings.TrimSpace(progress.SourceCursor) != "" {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "validate_cursor",
			Family:  "source_sync_progress",
			Message: "source sync will compare target state even when the source cursor was seen before",
		})
	}
	return mutations
}

// adminMigrationSourceSyncCompletedMutations reports actual metadata work after
// a confirmed apply and keeps search validation follow-up visible.
func adminMigrationSourceSyncCompletedMutations(diff adminMigrationSourceSyncDiff) []adminMigrationPlannedMutation {
	mutations := adminMigrationSourceSyncMutationDetails(diff.Details)
	if !diff.HasChanges {
		mutations = append(mutations, adminMigrationPlannedMutation{Action: "noop", Family: "postgres", Message: "source sync found no PostgreSQL metadata changes for manifest-covered families"})
	}
	mutations = append(mutations, adminMigrationSourceSearchFollowupMutations(diff.HasChanges)...)
	return mutations
}

// adminMigrationSourceSearchFollowupMutations keeps source import/sync output
// aligned with the operator sequence that treats OpenSearch as derived state:
// rebuild from PostgreSQL when metadata changed, then validate provider state.
func adminMigrationSourceSearchFollowupMutations(needsReindex bool) []adminMigrationPlannedMutation {
	mutations := []adminMigrationPlannedMutation{}
	if needsReindex {
		mutations = append(mutations, adminMigrationPlannedMutation{
			Action:  "recommended_command",
			Family:  "opensearch",
			Message: "opencook admin reindex --all-orgs --complete",
		})
	}
	mutations = append(mutations, adminMigrationPlannedMutation{
		Action:  "recommended_command",
		Family:  "opensearch",
		Message: "opencook admin search check --all-orgs",
	})
	return mutations
}

// adminMigrationSourceSyncRecords canonicalizes persisted state into row
// digests grouped by the normalized source family scopes being reconciled.
func adminMigrationSourceSyncRecords(state adminMigrationSourceImportState, scopes map[adminMigrationSourcePayloadKey]bool) map[adminMigrationSourcePayloadKey]map[string]string {
	records := map[adminMigrationSourcePayloadKey]map[string]string{}
	add := func(key adminMigrationSourcePayloadKey, id string, value any) {
		if !scopes[key] {
			return
		}
		id = strings.TrimSpace(id)
		if id == "" {
			return
		}
		if records[key] == nil {
			records[key] = map[string]string{}
		}
		records[key][id] = adminMigrationSourceSyncDigest(value)
	}

	for _, username := range adminMigrationSortedMapKeys(state.Bootstrap.Users) {
		add(adminMigrationSourcePayloadKey{Family: "users"}, username, state.Bootstrap.Users[username])
	}
	for _, username := range adminMigrationSortedMapKeys(state.Bootstrap.UserACLs) {
		add(adminMigrationSourcePayloadKey{Family: "user_acls"}, username, adminMigrationSourceSyncCanonicalACL(state.Bootstrap.UserACLs[username]))
	}
	for _, username := range adminMigrationSortedMapKeys(state.Bootstrap.UserKeys) {
		for _, keyName := range adminMigrationSortedMapKeys(state.Bootstrap.UserKeys[username]) {
			add(adminMigrationSourcePayloadKey{Family: "user_keys"}, username+"/"+keyName, state.Bootstrap.UserKeys[username][keyName])
		}
	}
	for _, orgName := range adminMigrationSourceSyncScopeOrgNames(scopes) {
		group := state.Bootstrap.Orgs[orgName].Groups["admins"]
		for _, user := range adminMigrationUniqueSortedStrings(group.Users) {
			add(adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, orgName+"/"+user, map[string]string{"organization": orgName, "type": "user", "actor": user})
		}
	}

	inventory := adminMigrationCookbookInventoryFromExport(state.Cookbooks)
	for _, orgName := range adminMigrationOrgNames(state.Bootstrap, state.CoreObjects, inventory, "") {
		bootstrapOrg := state.Bootstrap.Orgs[orgName]
		coreOrg := state.CoreObjects.Orgs[orgName]
		cookbookOrg := state.Cookbooks.Orgs[orgName]
		add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "organizations"}, orgName, bootstrapOrg.Organization)
		for _, name := range adminMigrationSortedMapKeys(bootstrapOrg.Clients) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "clients"}, name, bootstrapOrg.Clients[name])
		}
		for _, clientName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys) {
			for _, keyName := range adminMigrationSortedMapKeys(bootstrapOrg.ClientKeys[clientName]) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "client_keys"}, clientName+"/"+keyName, bootstrapOrg.ClientKeys[clientName][keyName])
			}
		}
		for _, name := range adminMigrationSortedMapKeys(bootstrapOrg.Groups) {
			group := bootstrapOrg.Groups[name]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "groups"}, name, map[string]any{
				"name":      group.Name,
				"groupname": group.GroupName,
				"orgname":   group.Organization,
			})
			for _, member := range adminMigrationSourceSyncGroupMembershipRecords(group) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "group_memberships"}, member["id"], member)
			}
		}
		for _, name := range adminMigrationSortedMapKeys(bootstrapOrg.Containers) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "containers"}, name, bootstrapOrg.Containers[name])
		}
		for _, name := range adminMigrationSortedMapKeys(bootstrapOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, "bootstrap/"+name, adminMigrationSourceSyncCanonicalACL(bootstrapOrg.ACLs[name]))
		}
		for _, name := range adminMigrationSortedMapKeys(coreOrg.ACLs) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, "core/"+name, adminMigrationSourceSyncCanonicalACL(coreOrg.ACLs[name]))
		}
		for _, name := range adminMigrationSortedMapKeys(coreOrg.Nodes) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "nodes"}, name, coreOrg.Nodes[name])
		}
		for _, name := range adminMigrationSortedMapKeys(coreOrg.Environments) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "environments"}, name, coreOrg.Environments[name])
		}
		for _, name := range adminMigrationSortedMapKeys(coreOrg.Roles) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "roles"}, name, coreOrg.Roles[name])
		}
		for _, name := range adminMigrationSortedMapKeys(coreOrg.DataBags) {
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bags"}, name, coreOrg.DataBags[name])
		}
		for _, bagName := range adminMigrationSortedMapKeys(coreOrg.DataBagItems) {
			for _, itemID := range adminMigrationSortedMapKeys(coreOrg.DataBagItems[bagName]) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bag_items"}, bagName+"/"+itemID, coreOrg.DataBagItems[bagName][itemID])
			}
		}
		for _, policyName := range adminMigrationSortedMapKeys(coreOrg.Policies) {
			for _, revisionID := range adminMigrationSortedMapKeys(coreOrg.Policies[policyName]) {
				revision := coreOrg.Policies[policyName][revisionID]
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_revisions"}, policyName+"/"+revisionID, map[string]any{"name": revision.Name, "revision_id": revision.RevisionID, "payload": revision.Payload})
			}
		}
		for _, groupName := range adminMigrationSortedMapKeys(coreOrg.PolicyGroups) {
			group := coreOrg.PolicyGroups[groupName]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_groups"}, groupName, map[string]any{"name": group.Name})
			for _, policyName := range adminMigrationSortedMapKeys(group.Policies) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_assignments"}, groupName+"/"+policyName, map[string]string{"group": groupName, "policy": policyName, "revision_id": group.Policies[policyName]})
			}
		}
		for _, id := range adminMigrationSortedMapKeys(coreOrg.Sandboxes) {
			sandbox := coreOrg.Sandboxes[id]
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "sandboxes"}, id, map[string]any{"sandbox_id": sandbox.ID, "checksums": adminMigrationUniqueSortedStrings(sandbox.Checksums)})
			for _, checksum := range adminMigrationUniqueSortedStrings(sandbox.Checksums) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}, "sandbox/"+id+"/"+checksum, map[string]string{"family": "sandboxes", "id": id, "checksum": checksum})
			}
		}
		for _, version := range cookbookOrg.Versions {
			id := adminMigrationSourceSyncCookbookVersionID(version)
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_versions"}, id, version)
			for _, checksum := range adminMigrationCookbookFileChecksums(version.AllFiles) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}, "cookbook_version/"+id+"/"+checksum, map[string]string{"family": "cookbook_versions", "id": id, "checksum": checksum})
			}
		}
		for _, artifact := range cookbookOrg.Artifacts {
			id := adminMigrationSourceSyncCookbookArtifactID(artifact)
			add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_artifacts"}, id, artifact)
			for _, checksum := range adminMigrationCookbookFileChecksums(artifact.AllFiles) {
				add(adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}, "cookbook_artifact/"+id+"/"+checksum, map[string]string{"family": "cookbook_artifacts", "id": id, "checksum": checksum})
			}
		}
	}
	for key := range scopes {
		if records[key] == nil {
			records[key] = map[string]string{}
		}
	}
	return records
}

// adminMigrationSourceSyncGroupMembershipRecords splits group actors into rows
// so membership-only source changes can be planned independently of group rows.
func adminMigrationSourceSyncGroupMembershipRecords(group bootstrap.Group) []map[string]string {
	var records []map[string]string
	for _, user := range adminMigrationUniqueSortedStrings(group.Users) {
		records = append(records, map[string]string{"id": group.Name + "/user/" + user, "group": group.Name, "type": "user", "actor": user})
	}
	for _, client := range adminMigrationUniqueSortedStrings(group.Clients) {
		records = append(records, map[string]string{"id": group.Name + "/client/" + client, "group": group.Name, "type": "client", "actor": client})
	}
	for _, nested := range adminMigrationUniqueSortedStrings(group.Groups) {
		records = append(records, map[string]string{"id": group.Name + "/group/" + nested, "group": group.Name, "type": "group", "actor": nested})
	}
	return records
}

// adminMigrationSourceSyncCanonicalACL normalizes nil and sorted ACL actor
// slices so source diffs are not polluted by store clone representation details.
func adminMigrationSourceSyncCanonicalACL(acl authz.ACL) map[string]map[string][]string {
	return map[string]map[string][]string{
		"create": adminMigrationSourceSyncCanonicalPermission(acl.Create),
		"read":   adminMigrationSourceSyncCanonicalPermission(acl.Read),
		"update": adminMigrationSourceSyncCanonicalPermission(acl.Update),
		"delete": adminMigrationSourceSyncCanonicalPermission(acl.Delete),
		"grant":  adminMigrationSourceSyncCanonicalPermission(acl.Grant),
	}
}

// adminMigrationSourceSyncCanonicalPermission preserves Chef ACL membership
// semantics while hiding nil-versus-empty slice differences in diff digests.
func adminMigrationSourceSyncCanonicalPermission(permission authz.Permission) map[string][]string {
	return map[string][]string{
		"actors": adminMigrationUniqueSortedStrings(permission.Actors),
		"groups": adminMigrationUniqueSortedStrings(permission.Groups),
	}
}

// adminMigrationSourceSyncMergedState applies source rows only for manifest-
// covered families, preserving target-only orgs and absent families.
func adminMigrationSourceSyncMergedState(target, source adminMigrationSourceImportState, scopes map[adminMigrationSourcePayloadKey]bool) adminMigrationSourceImportState {
	merged := adminMigrationSourceImportState{
		Bootstrap:   bootstrap.CloneBootstrapCoreState(target.Bootstrap),
		CoreObjects: bootstrap.CloneCoreObjectState(target.CoreObjects),
		Cookbooks:   adminMigrationCloneCookbookExport(target.Cookbooks),
	}
	adminMigrationEnsureSourceSyncStateMaps(&merged)
	source = adminMigrationSourceImportState{
		Bootstrap:   bootstrap.CloneBootstrapCoreState(source.Bootstrap),
		CoreObjects: bootstrap.CloneCoreObjectState(source.CoreObjects),
		Cookbooks:   adminMigrationCloneCookbookExport(source.Cookbooks),
	}
	adminMigrationEnsureSourceSyncStateMaps(&source)

	for key := range scopes {
		switch key.Family {
		case "users":
			merged.Bootstrap.Users = source.Bootstrap.Users
		case "user_acls":
			merged.Bootstrap.UserACLs = source.Bootstrap.UserACLs
		case "user_keys":
			merged.Bootstrap.UserKeys = source.Bootstrap.UserKeys
		case "server_admin_memberships":
			adminMigrationSourceSyncMergeServerAdmins(&merged, source, scopes)
		default:
			adminMigrationSourceSyncMergeOrgFamily(&merged, source, key)
		}
	}
	return merged
}

// adminMigrationSourceSyncMergeOrgFamily replaces only the requested org/family
// map so omitted source families never erase target-only state accidentally.
func adminMigrationSourceSyncMergeOrgFamily(merged *adminMigrationSourceImportState, source adminMigrationSourceImportState, key adminMigrationSourcePayloadKey) {
	orgName := strings.TrimSpace(key.Organization)
	if orgName == "" {
		return
	}
	adminMigrationEnsureSourceSyncOrg(merged, orgName)
	adminMigrationEnsureSourceSyncOrg(&source, orgName)
	bootstrapOrg := merged.Bootstrap.Orgs[orgName]
	sourceBootstrapOrg := source.Bootstrap.Orgs[orgName]
	coreOrg := merged.CoreObjects.Orgs[orgName]
	sourceCoreOrg := source.CoreObjects.Orgs[orgName]
	cookbookOrg := merged.Cookbooks.Orgs[orgName]
	sourceCookbookOrg := source.Cookbooks.Orgs[orgName]

	switch key.Family {
	case "organizations":
		bootstrapOrg.Organization = sourceBootstrapOrg.Organization
	case "clients":
		bootstrapOrg.Clients = sourceBootstrapOrg.Clients
	case "client_keys":
		bootstrapOrg.ClientKeys = sourceBootstrapOrg.ClientKeys
	case "groups", "group_memberships":
		bootstrapOrg.Groups = sourceBootstrapOrg.Groups
	case "containers":
		bootstrapOrg.Containers = sourceBootstrapOrg.Containers
	case "acls":
		bootstrapOrg.ACLs = sourceBootstrapOrg.ACLs
		coreOrg.ACLs = sourceCoreOrg.ACLs
	case "nodes":
		coreOrg.Nodes = sourceCoreOrg.Nodes
	case "environments":
		coreOrg.Environments = sourceCoreOrg.Environments
	case "roles":
		coreOrg.Roles = sourceCoreOrg.Roles
	case "data_bags":
		coreOrg.DataBags = sourceCoreOrg.DataBags
	case "data_bag_items":
		coreOrg.DataBagItems = sourceCoreOrg.DataBagItems
	case "policy_revisions":
		coreOrg.Policies = sourceCoreOrg.Policies
	case "policy_groups", "policy_assignments":
		coreOrg.PolicyGroups = sourceCoreOrg.PolicyGroups
	case "sandboxes":
		coreOrg.Sandboxes = sourceCoreOrg.Sandboxes
	case "cookbook_versions":
		cookbookOrg.Versions = sourceCookbookOrg.Versions
	case "cookbook_artifacts":
		cookbookOrg.Artifacts = sourceCookbookOrg.Artifacts
	}
	merged.Bootstrap.Orgs[orgName] = bootstrapOrg
	merged.CoreObjects.Orgs[orgName] = coreOrg
	merged.Cookbooks.Orgs[orgName] = adminMigrationSortedCookbookExportOrg(cookbookOrg)
}

// adminMigrationSourceSyncMergeServerAdmins updates admins memberships only in
// source-covered orgs so unrelated target-only orgs are not pruned implicitly.
func adminMigrationSourceSyncMergeServerAdmins(merged *adminMigrationSourceImportState, source adminMigrationSourceImportState, scopes map[adminMigrationSourcePayloadKey]bool) {
	for _, orgName := range adminMigrationSourceSyncScopeOrgNames(scopes) {
		adminMigrationEnsureSourceSyncOrg(merged, orgName)
		sourceOrg := source.Bootstrap.Orgs[orgName]
		mergedOrg := merged.Bootstrap.Orgs[orgName]
		group := mergedOrg.Groups["admins"]
		sourceGroup := sourceOrg.Groups["admins"]
		group.Users = adminMigrationUniqueSortedStrings(sourceGroup.Users)
		group.Actors = adminMigrationUniqueSortedStrings(append(append([]string{}, group.Users...), group.Clients...))
		mergedOrg.Groups["admins"] = group
		merged.Bootstrap.Orgs[orgName] = mergedOrg
	}
}

// adminMigrationEnsureSourceSyncStateMaps makes cloned zero states writable
// before sync merge code assigns nested org families.
func adminMigrationEnsureSourceSyncStateMaps(state *adminMigrationSourceImportState) {
	if state.Bootstrap.Users == nil {
		state.Bootstrap.Users = map[string]bootstrap.User{}
	}
	if state.Bootstrap.UserACLs == nil {
		state.Bootstrap.UserACLs = map[string]authz.ACL{}
	}
	if state.Bootstrap.UserKeys == nil {
		state.Bootstrap.UserKeys = map[string]map[string]bootstrap.KeyRecord{}
	}
	if state.Bootstrap.Orgs == nil {
		state.Bootstrap.Orgs = map[string]bootstrap.BootstrapCoreOrganizationState{}
	}
	if state.CoreObjects.Orgs == nil {
		state.CoreObjects.Orgs = map[string]bootstrap.CoreObjectOrganizationState{}
	}
	if state.Cookbooks.Orgs == nil {
		state.Cookbooks.Orgs = map[string]adminMigrationCookbookOrgExport{}
	}
}

// adminMigrationEnsureSourceSyncOrg initializes an org in all three persisted
// state buckets so source sync can add a newly covered org atomically.
func adminMigrationEnsureSourceSyncOrg(state *adminMigrationSourceImportState, orgName string) {
	adminMigrationEnsureSourceSyncStateMaps(state)
	if _, ok := state.Bootstrap.Orgs[orgName]; !ok {
		state.Bootstrap.Orgs[orgName] = adminMigrationNewSourceImportOrgAccumulator(orgName).bootstrap
	}
	if _, ok := state.CoreObjects.Orgs[orgName]; !ok {
		state.CoreObjects.Orgs[orgName] = adminMigrationNewSourceImportOrgAccumulator(orgName).core
	}
	if _, ok := state.Cookbooks.Orgs[orgName]; !ok {
		state.Cookbooks.Orgs[orgName] = adminMigrationCookbookOrgExport{}
	}
}

// adminMigrationSourceSyncScopeOrgNames returns orgs with at least one scoped
// source payload, excluding global user/key/server-admin payloads.
func adminMigrationSourceSyncScopeOrgNames(scopes map[adminMigrationSourcePayloadKey]bool) []string {
	seen := map[string]struct{}{}
	for key := range scopes {
		if strings.TrimSpace(key.Organization) != "" {
			seen[key.Organization] = struct{}{}
		}
	}
	return adminMigrationSortedStringSet(seen)
}

// adminMigrationSourceSyncDigest produces stable row comparisons from JSON
// values while letting Go's encoder sort map keys deterministically.
func adminMigrationSourceSyncDigest(value any) string {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// adminMigrationCloneCookbookExport deep-copies slices and nested JSON-like
// metadata so sync planning cannot mutate loaded target cookbook state.
func adminMigrationCloneCookbookExport(in adminMigrationCookbookExport) adminMigrationCookbookExport {
	out := adminMigrationCookbookExport{Orgs: make(map[string]adminMigrationCookbookOrgExport, len(in.Orgs))}
	for orgName, org := range in.Orgs {
		cloned := adminMigrationCookbookOrgExport{
			Versions:  make([]bootstrap.CookbookVersion, len(org.Versions)),
			Artifacts: make([]bootstrap.CookbookArtifact, len(org.Artifacts)),
		}
		copy(cloned.Versions, org.Versions)
		copy(cloned.Artifacts, org.Artifacts)
		out.Orgs[orgName] = adminMigrationSortedCookbookExportOrg(cloned)
	}
	return out
}

// adminMigrationSourceSyncCookbookVersionID mirrors Chef's cookbook identity:
// cookbook name plus version, with legacy Name as a fallback for old rows.
func adminMigrationSourceSyncCookbookVersionID(version bootstrap.CookbookVersion) string {
	name := strings.TrimSpace(version.CookbookName)
	if name == "" {
		name = adminMigrationCookbookRouteName(version)
	}
	return name + "/" + strings.TrimSpace(version.Version)
}

// adminMigrationSourceSyncCookbookArtifactID identifies artifact metadata by
// cookbook artifact name plus immutable identifier.
func adminMigrationSourceSyncCookbookArtifactID(artifact bootstrap.CookbookArtifact) string {
	return strings.TrimSpace(artifact.Name) + "/" + strings.ToLower(strings.TrimSpace(artifact.Identifier))
}

// adminMigrationSourceImportCompletedMutations summarizes the work that apply
// performed and the derived-state rebuild/validation follow-up operators should run.
func adminMigrationSourceImportCompletedMutations(read adminMigrationSourceImportRead) []adminMigrationPlannedMutation {
	mutations := []adminMigrationPlannedMutation{}
	for _, family := range read.Bundle.Inventory.Families {
		switch family.Family {
		case "cookbook_blob_references":
			continue
		case "opensearch_source_artifacts":
			mutations = append(mutations, adminMigrationPlannedMutation{Action: "skipped", Family: "opensearch", Count: family.Count, Message: "skipped source OpenSearch artifacts because search is derived state"})
		default:
			mutations = append(mutations, adminMigrationPlannedMutation{
				Action:       "imported",
				Organization: family.Organization,
				Family:       family.Family,
				Count:        family.Count,
				Message:      "imported PostgreSQL-backed " + family.Family + " records from normalized source payloads",
			})
		}
	}
	if len(read.CopiedChecksums) > 0 {
		mutations = append(mutations, adminMigrationPlannedMutation{Action: "copied_blob_objects", Family: "blobs", Count: len(read.CopiedChecksums), Message: "copied checksum-addressed source blob bytes before metadata import"})
	}
	if providerRefs := adminMigrationSourceImportProviderBlobReferences(adminMigrationUniqueBlobReferences(read.ReferencedChecksums), read.CopiedChecksums); len(providerRefs) > 0 {
		mutations = append(mutations, adminMigrationPlannedMutation{Action: "verified_blob_references", Family: "blobs", Count: len(providerRefs), Message: "verified checksum-addressed source blob references before metadata import"})
	}
	mutations = append(mutations, adminMigrationSourceSearchFollowupMutations(true)...)
	return mutations
}

type adminMigrationSourceImportOrgAccumulator struct {
	bootstrap bootstrap.BootstrapCoreOrganizationState
	core      bootstrap.CoreObjectOrganizationState
	cookbooks adminMigrationCookbookOrgExport
}

// adminMigrationSourceImportStateFromRead converts the normalized source graph
// into the same logical state shape used by backup restore and PostgreSQL stores.
func adminMigrationSourceImportStateFromRead(read adminMigrationSourceImportRead) (adminMigrationSourceImportState, error) {
	state := adminMigrationSourceImportState{
		Bootstrap: bootstrap.BootstrapCoreState{
			Users:    map[string]bootstrap.User{},
			UserACLs: map[string]authz.ACL{},
			UserKeys: map[string]map[string]bootstrap.KeyRecord{},
			Orgs:     map[string]bootstrap.BootstrapCoreOrganizationState{},
		},
		CoreObjects: bootstrap.CoreObjectState{Orgs: map[string]bootstrap.CoreObjectOrganizationState{}},
		Cookbooks:   adminMigrationCookbookExport{Orgs: map[string]adminMigrationCookbookOrgExport{}},
	}
	orgs := map[string]*adminMigrationSourceImportOrgAccumulator{}
	ensureOrg := func(orgName string) *adminMigrationSourceImportOrgAccumulator {
		orgName = strings.TrimSpace(orgName)
		if orgName == "" {
			return nil
		}
		if acc, ok := orgs[orgName]; ok {
			return acc
		}
		acc := adminMigrationNewSourceImportOrgAccumulator(orgName)
		orgs[orgName] = acc
		return acc
	}

	for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, "", "users") {
		username := adminMigrationSourceString(object, "username", "name")
		state.Bootstrap.Users[username] = bootstrap.User{
			Username:    username,
			DisplayName: adminMigrationSourceString(object, "display_name"),
			Email:       adminMigrationSourceString(object, "email"),
			FirstName:   adminMigrationSourceString(object, "first_name"),
			LastName:    adminMigrationSourceString(object, "last_name"),
		}
	}
	for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, "", "user_acls") {
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "user")
		if err != nil {
			return adminMigrationSourceImportState{}, err
		}
		if resourceType != "user" {
			continue
		}
		acl, err := adminMigrationSourceImportACL(object)
		if err != nil {
			return adminMigrationSourceImportState{}, err
		}
		state.Bootstrap.UserACLs[resourceName] = acl
	}
	for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, "", "user_keys") {
		username := adminMigrationSourceString(object, "username", "user", "name")
		record, err := adminMigrationSourceImportKeyRecord(authn.Principal{Type: "user", Name: username}, object)
		if err != nil {
			return adminMigrationSourceImportState{}, err
		}
		if state.Bootstrap.UserKeys[username] == nil {
			state.Bootstrap.UserKeys[username] = map[string]bootstrap.KeyRecord{}
		}
		state.Bootstrap.UserKeys[username][record.Name] = record
	}

	for _, orgName := range adminMigrationSourceImportOrgNames(read.PayloadValues) {
		ensureOrg(orgName)
	}
	for orgName := range orgs {
		for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, orgName, "organizations") {
			acc := ensureOrg(orgName)
			acc.bootstrap.Organization = bootstrap.Organization{
				Name:     adminMigrationSourceStringDefault(object, orgName, "name", "orgname"),
				FullName: adminMigrationSourceStringDefault(object, orgName, "full_name", "display_name"),
				OrgType:  adminMigrationSourceStringDefault(object, "Business", "org_type"),
				GUID:     adminMigrationSourceStringDefault(object, orgName, "guid"),
			}
		}
	}

	for _, orgName := range adminMigrationSortedMapKeys(orgs) {
		acc := ensureOrg(orgName)
		if err := adminMigrationSourceImportOrgBootstrap(read.PayloadValues, orgName, acc); err != nil {
			return adminMigrationSourceImportState{}, err
		}
		if err := adminMigrationSourceImportOrgCore(read.PayloadValues, orgName, acc); err != nil {
			return adminMigrationSourceImportState{}, err
		}
		if err := adminMigrationSourceImportOrgCookbooks(read.PayloadValues, orgName, acc); err != nil {
			return adminMigrationSourceImportState{}, err
		}
	}
	for _, object := range adminMigrationSourceImportObjects(read.PayloadValues, "", "server_admin_memberships") {
		if adminMigrationSourceStringDefault(object, "user", "type", "actor_type") != "user" {
			continue
		}
		user := adminMigrationSourceString(object, "actor", "username", "name")
		for _, orgName := range adminMigrationSortedMapKeys(orgs) {
			acc := ensureOrg(orgName)
			group, ok := acc.bootstrap.Groups["admins"]
			if !ok {
				continue
			}
			acc.bootstrap.Groups["admins"] = adminMigrationSourceImportAddGroupMember(group, "user", user)
		}
	}

	for _, orgName := range adminMigrationSortedMapKeys(orgs) {
		acc := orgs[orgName]
		adminMigrationFinalizeSourceImportOrg(orgName, acc)
		state.Bootstrap.Orgs[orgName] = acc.bootstrap
		state.CoreObjects.Orgs[orgName] = acc.core
		state.Cookbooks.Orgs[orgName] = adminMigrationSortedCookbookExportOrg(acc.cookbooks)
	}
	return state, nil
}

// adminMigrationNewSourceImportOrgAccumulator initializes every per-org map so
// conversion can assign records directly without nil-map guard noise.
func adminMigrationNewSourceImportOrgAccumulator(orgName string) *adminMigrationSourceImportOrgAccumulator {
	return &adminMigrationSourceImportOrgAccumulator{
		bootstrap: bootstrap.BootstrapCoreOrganizationState{
			Organization: bootstrap.Organization{Name: orgName, FullName: orgName, OrgType: "Business", GUID: orgName},
			Clients:      map[string]bootstrap.Client{},
			ClientKeys:   map[string]map[string]bootstrap.KeyRecord{},
			Groups:       map[string]bootstrap.Group{},
			Containers:   map[string]bootstrap.Container{},
			ACLs:         map[string]authz.ACL{},
		},
		core: bootstrap.CoreObjectOrganizationState{
			DataBags:     map[string]bootstrap.DataBag{},
			DataBagItems: map[string]map[string]bootstrap.DataBagItem{},
			Environments: map[string]bootstrap.Environment{},
			Nodes:        map[string]bootstrap.Node{},
			Roles:        map[string]bootstrap.Role{},
			Sandboxes:    map[string]bootstrap.Sandbox{},
			Policies:     map[string]map[string]bootstrap.PolicyRevision{},
			PolicyGroups: map[string]bootstrap.PolicyGroup{},
			ACLs:         map[string]authz.ACL{},
		},
		cookbooks: adminMigrationCookbookOrgExport{},
	}
}

// adminMigrationSourceImportOrgBootstrap imports identity, key, group,
// container, and bootstrap ACL rows for one organization.
func adminMigrationSourceImportOrgBootstrap(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, acc *adminMigrationSourceImportOrgAccumulator) error {
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "clients") {
		name := adminMigrationSourceString(object, "name", "clientname", "client")
		acc.bootstrap.Clients[name] = bootstrap.Client{
			Name:         name,
			ClientName:   name,
			Organization: orgName,
			Validator:    adminMigrationSourceBool(object, "validator"),
			Admin:        adminMigrationSourceBool(object, "admin"),
			PublicKey:    adminMigrationSourceString(object, "public_key", "public_key_pem"),
			URI:          "/organizations/" + orgName + "/clients/" + name,
		}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "client_keys") {
		clientName := adminMigrationSourceString(object, "client", "clientname")
		record, err := adminMigrationSourceImportKeyRecord(authn.Principal{Type: "client", Name: clientName, Organization: orgName}, object)
		if err != nil {
			return err
		}
		if acc.bootstrap.ClientKeys[clientName] == nil {
			acc.bootstrap.ClientKeys[clientName] = map[string]bootstrap.KeyRecord{}
		}
		acc.bootstrap.ClientKeys[clientName][record.Name] = record
		if client, ok := acc.bootstrap.Clients[clientName]; ok && record.Name == "default" && strings.TrimSpace(client.PublicKey) == "" {
			client.PublicKey = record.PublicKeyPEM
			acc.bootstrap.Clients[clientName] = client
		}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "groups") {
		group, err := adminMigrationSourceImportGroup(orgName, object)
		if err != nil {
			return err
		}
		acc.bootstrap.Groups[group.Name] = group
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "group_memberships") {
		groupName := adminMigrationSourceString(object, "group", "groupname")
		group, ok := acc.bootstrap.Groups[groupName]
		if !ok {
			continue
		}
		acc.bootstrap.Groups[groupName] = adminMigrationSourceImportAddGroupMember(group, adminMigrationSourceStringDefault(object, "user", "type", "actor_type"), adminMigrationSourceString(object, "actor", "name"))
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "containers") {
		name := adminMigrationSourceString(object, "name", "containername", "containerpath")
		acc.bootstrap.Containers[name] = bootstrap.Container{
			Name:          name,
			ContainerName: name,
			ContainerPath: adminMigrationSourceStringDefault(object, name, "containerpath"),
		}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "acls") {
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "organization")
		if err != nil {
			return err
		}
		acl, err := adminMigrationSourceImportACL(object)
		if err != nil {
			return err
		}
		switch resourceType {
		case "organization":
			acc.bootstrap.ACLs[adminMigrationOrganizationACLKey()] = acl
		case "client":
			acc.bootstrap.ACLs[adminMigrationClientACLKey(resourceName)] = acl
		case "group":
			acc.bootstrap.ACLs[adminMigrationGroupACLKey(resourceName)] = acl
		case "container":
			acc.bootstrap.ACLs[adminMigrationContainerACLKey(resourceName)] = acl
		}
	}
	return nil
}

// adminMigrationSourceImportOrgCore imports persisted core object rows while
// leaving Chef-facing validation in the earlier source normalization pass.
func adminMigrationSourceImportOrgCore(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, acc *adminMigrationSourceImportOrgAccumulator) error {
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "nodes") {
		node, err := adminMigrationSourceImportNode(object)
		if err != nil {
			return err
		}
		acc.core.Nodes[node.Name] = node
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "environments") {
		env, err := adminMigrationSourceImportEnvironment(object)
		if err != nil {
			return err
		}
		acc.core.Environments[env.Name] = env
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "roles") {
		role, err := adminMigrationSourceImportRole(object)
		if err != nil {
			return err
		}
		acc.core.Roles[role.Name] = role
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "data_bags") {
		name := adminMigrationSourceString(object, "name", "bag")
		acc.core.DataBags[name] = bootstrap.DataBag{Name: name, JSONClass: "Chef::DataBag", ChefType: "data_bag"}
		if acc.core.DataBagItems[name] == nil {
			acc.core.DataBagItems[name] = map[string]bootstrap.DataBagItem{}
		}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "data_bag_items") {
		bagName := adminMigrationSourceString(object, "bag", "data_bag")
		itemID := adminMigrationSourceString(object, "id", "item", "name")
		payload, _ := object["payload"].(map[string]any)
		if acc.core.DataBagItems[bagName] == nil {
			acc.core.DataBagItems[bagName] = map[string]bootstrap.DataBagItem{}
		}
		acc.core.DataBagItems[bagName][itemID] = bootstrap.DataBagItem{ID: itemID, RawData: adminMigrationSourceImportCloneMap(payload)}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "policy_revisions") {
		name := adminMigrationSourceString(object, "name", "policy", "policy_name")
		revisionID := strings.ToLower(adminMigrationSourceString(object, "revision_id", "revision"))
		if acc.core.Policies[name] == nil {
			acc.core.Policies[name] = map[string]bootstrap.PolicyRevision{}
		}
		acc.core.Policies[name][revisionID] = bootstrap.PolicyRevision{Name: name, RevisionID: revisionID, Payload: adminMigrationSourceImportCloneMap(object)}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "policy_groups") {
		groupName := adminMigrationSourceString(object, "name", "group")
		acc.core.PolicyGroups[groupName] = bootstrap.PolicyGroup{Name: groupName, Policies: adminMigrationSourceImportStringMap(object["policies"])}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "policy_assignments") {
		groupName := adminMigrationSourceString(object, "group", "policy_group")
		policyName := adminMigrationSourceString(object, "policy", "policy_name")
		revisionID := strings.ToLower(adminMigrationSourceString(object, "revision_id", "revision"))
		group := acc.core.PolicyGroups[groupName]
		if group.Name == "" {
			group.Name = groupName
		}
		if group.Policies == nil {
			group.Policies = map[string]string{}
		}
		group.Policies[policyName] = revisionID
		acc.core.PolicyGroups[groupName] = group
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "sandboxes") {
		id := adminMigrationSourceString(object, "id", "sandbox_id", "name")
		checksums, err := adminMigrationSourceStringSlice(object, "checksums")
		if err != nil {
			return err
		}
		acc.core.Sandboxes[id] = bootstrap.Sandbox{ID: id, Organization: orgName, Checksums: checksums, CreatedAt: time.Now().UTC()}
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "acls") {
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "organization")
		if err != nil {
			return err
		}
		acl, err := adminMigrationSourceImportACL(object)
		if err != nil {
			return err
		}
		switch resourceType {
		case "environment":
			acc.core.ACLs[adminMigrationEnvironmentACLKey(resourceName)] = acl
		case "node":
			acc.core.ACLs[adminMigrationNodeACLKey(resourceName)] = acl
		case "role":
			acc.core.ACLs[adminMigrationRoleACLKey(resourceName)] = acl
		case "data_bag":
			acc.core.ACLs[adminMigrationDataBagACLKey(resourceName)] = acl
		case "policy":
			acc.core.ACLs[adminMigrationPolicyACLKey(resourceName)] = acl
		case "policy_group":
			acc.core.ACLs[adminMigrationPolicyGroupACLKey(resourceName)] = acl
		case "sandbox":
			acc.core.ACLs["sandbox:"+resourceName] = acl
		}
	}
	return nil
}

// adminMigrationSourceImportOrgCookbooks imports cookbook metadata only after
// blob apply has made the referenced checksum content available.
func adminMigrationSourceImportOrgCookbooks(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, acc *adminMigrationSourceImportOrgAccumulator) error {
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "cookbook_versions") {
		version, err := adminMigrationSourceImportCookbookVersion(object)
		if err != nil {
			return err
		}
		acc.cookbooks.Versions = append(acc.cookbooks.Versions, version)
	}
	for _, object := range adminMigrationSourceImportObjects(payloadValues, orgName, "cookbook_artifacts") {
		artifact, err := adminMigrationSourceImportCookbookArtifact(object)
		if err != nil {
			return err
		}
		acc.cookbooks.Artifacts = append(acc.cookbooks.Artifacts, artifact)
	}
	return nil
}

// adminMigrationFinalizeSourceImportOrg fills compatibility defaults that
// stores and app startup normally expect after loading persisted state.
func adminMigrationFinalizeSourceImportOrg(orgName string, acc *adminMigrationSourceImportOrgAccumulator) {
	if acc.bootstrap.Organization.Name == "" {
		acc.bootstrap.Organization = bootstrap.Organization{Name: orgName, FullName: orgName, OrgType: "Business", GUID: orgName}
	}
	if acc.bootstrap.Organization.FullName == "" {
		acc.bootstrap.Organization.FullName = acc.bootstrap.Organization.Name
	}
	if acc.bootstrap.Organization.OrgType == "" {
		acc.bootstrap.Organization.OrgType = "Business"
	}
	if acc.bootstrap.Organization.GUID == "" {
		acc.bootstrap.Organization.GUID = acc.bootstrap.Organization.Name
	}
	if _, ok := acc.core.Environments["_default"]; !ok {
		acc.core.Environments["_default"] = adminMigrationSourceImportDefaultEnvironment()
	}
}

// adminMigrationSourceImportOrgNames returns every organization scope present
// in the normalized source payload map, including organization-only rows.
func adminMigrationSourceImportOrgNames(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) []string {
	seen := map[string]bool{}
	for key, values := range payloadValues {
		if strings.TrimSpace(key.Organization) == "" || len(values) == 0 {
			continue
		}
		seen[key.Organization] = true
	}
	return adminMigrationSortedMapKeys(seen)
}

// adminMigrationSourceImportObjects decodes all canonical objects for one
// normalized source family; malformed JSON should already be blocked upstream.
func adminMigrationSourceImportObjects(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName, family string) []map[string]any {
	values := payloadValues[adminMigrationSourcePayloadKey{Organization: orgName, Family: family}]
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			continue
		}
		objects = append(objects, object)
	}
	return objects
}

// adminMigrationSourceImportKeyRecord preserves source key IDs while parsing
// expiration data so authn.MemoryKeyStore can be hydrated after restart.
func adminMigrationSourceImportKeyRecord(principal authn.Principal, object map[string]any) (bootstrap.KeyRecord, error) {
	keyName := adminMigrationSourceStringDefault(object, "default", "key_name", "name")
	publicKey := adminMigrationSourceString(object, "public_key", "public_key_pem")
	if _, err := authn.ParseRSAPublicKeyPEM([]byte(publicKey)); err != nil {
		return bootstrap.KeyRecord{}, err
	}
	expirationDate, expiresAt, expired, err := adminMigrationSourceImportExpiration(adminMigrationSourceStringDefault(object, "infinity", "expiration_date", "expires_at"))
	if err != nil {
		return bootstrap.KeyRecord{}, err
	}
	return bootstrap.KeyRecord{
		Name:           keyName,
		URI:            adminMigrationSourceImportKeyURI(principal, keyName),
		PublicKeyPEM:   publicKey,
		ExpirationDate: expirationDate,
		Expired:        expired,
		ExpiresAt:      expiresAt,
	}, nil
}

// adminMigrationSourceImportExpiration mirrors bootstrap key expiration parsing
// because source import writes records directly through offline store seams.
func adminMigrationSourceImportExpiration(raw string) (string, *time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "infinity" {
		return "infinity", nil, false, nil
	}
	expiresAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "", nil, false, err
	}
	expiresAt = expiresAt.UTC()
	return expiresAt.Format(time.RFC3339), &expiresAt, !expiresAt.After(time.Now().UTC()), nil
}

// adminMigrationSourceImportKeyURI keeps imported key records route-compatible
// with existing user and org-scoped client key APIs.
func adminMigrationSourceImportKeyURI(principal authn.Principal, keyName string) string {
	if principal.Organization != "" {
		return "/organizations/" + principal.Organization + "/clients/" + principal.Name + "/keys/" + keyName
	}
	return "/users/" + principal.Name + "/keys/" + keyName
}

// adminMigrationSourceImportGroup canonicalizes group actor arrays into the
// persisted bootstrap shape used by authorization group lookups.
func adminMigrationSourceImportGroup(orgName string, object map[string]any) (bootstrap.Group, error) {
	users, err := adminMigrationSourceStringSlice(object, "users")
	if err != nil {
		return bootstrap.Group{}, err
	}
	clients, err := adminMigrationSourceStringSlice(object, "clients")
	if err != nil {
		return bootstrap.Group{}, err
	}
	groups, err := adminMigrationSourceStringSlice(object, "groups")
	if err != nil {
		return bootstrap.Group{}, err
	}
	actors := adminMigrationUniqueSortedStrings(append(append([]string{}, users...), clients...))
	name := adminMigrationSourceString(object, "name", "groupname", "group")
	return bootstrap.Group{Name: name, GroupName: name, Organization: orgName, Actors: actors, Users: users, Clients: clients, Groups: groups}, nil
}

// adminMigrationSourceImportAddGroupMember merges relation-table memberships
// into group rows without duplicating users, clients, nested groups, or actors.
func adminMigrationSourceImportAddGroupMember(group bootstrap.Group, actorType, actor string) bootstrap.Group {
	actor = strings.TrimSpace(actor)
	if actor == "" {
		return group
	}
	switch strings.TrimSpace(actorType) {
	case "client":
		group.Clients = adminMigrationUniqueSortedStrings(append(group.Clients, actor))
	case "group":
		group.Groups = adminMigrationUniqueSortedStrings(append(group.Groups, actor))
	default:
		group.Users = adminMigrationUniqueSortedStrings(append(group.Users, actor))
	}
	group.Actors = adminMigrationUniqueSortedStrings(append(append([]string{}, group.Users...), group.Clients...))
	return group
}

// adminMigrationSourceImportACL decodes the five Chef ACL permissions into the
// authz.ACL document shape already persisted by OpenCook.
func adminMigrationSourceImportACL(object map[string]any) (authz.ACL, error) {
	create, err := adminMigrationSourceImportPermission(object["create"])
	if err != nil {
		return authz.ACL{}, err
	}
	read, err := adminMigrationSourceImportPermission(object["read"])
	if err != nil {
		return authz.ACL{}, err
	}
	update, err := adminMigrationSourceImportPermission(object["update"])
	if err != nil {
		return authz.ACL{}, err
	}
	deletePermission, err := adminMigrationSourceImportPermission(object["delete"])
	if err != nil {
		return authz.ACL{}, err
	}
	grant, err := adminMigrationSourceImportPermission(object["grant"])
	if err != nil {
		return authz.ACL{}, err
	}
	return authz.ACL{Create: create, Read: read, Update: update, Delete: deletePermission, Grant: grant}, nil
}

// adminMigrationSourceImportPermission treats omitted source ACL permissions as
// empty actor/group lists, matching the normalized preflight contract.
func adminMigrationSourceImportPermission(value any) (authz.Permission, error) {
	object, _ := value.(map[string]any)
	actors, err := adminMigrationSourceStringSlice(object, "actors")
	if err != nil {
		return authz.Permission{}, err
	}
	groups, err := adminMigrationSourceStringSlice(object, "groups")
	if err != nil {
		return authz.Permission{}, err
	}
	return authz.Permission{Actors: actors, Groups: groups}, nil
}

// adminMigrationSourceImportNode fills the same default fields create/update
// paths produce, while preserving already-normalized source attributes.
func adminMigrationSourceImportNode(object map[string]any) (bootstrap.Node, error) {
	var node bootstrap.Node
	if err := adminMigrationSourceImportDecode(object, &node); err != nil {
		return bootstrap.Node{}, err
	}
	node.Name = adminMigrationSourceStringDefault(object, node.Name, "name", "nodename")
	node.JSONClass = adminMigrationSourceStringDefault(object, "Chef::Node", "json_class")
	node.ChefType = adminMigrationSourceStringDefault(object, "node", "chef_type")
	node.ChefEnvironment = adminMigrationSourceStringDefault(object, "_default", "chef_environment")
	if node.Override == nil {
		node.Override = map[string]any{}
	}
	if node.Normal == nil {
		node.Normal = map[string]any{}
	}
	if node.Default == nil {
		node.Default = map[string]any{}
	}
	if node.Automatic == nil {
		node.Automatic = map[string]any{}
	}
	if node.RunList == nil {
		node.RunList = []string{}
	}
	return node, nil
}

// adminMigrationSourceImportEnvironment preserves cookbook constraints and
// attributes while supplying default-map fields expected after service rehydrate.
func adminMigrationSourceImportEnvironment(object map[string]any) (bootstrap.Environment, error) {
	var env bootstrap.Environment
	if err := adminMigrationSourceImportDecode(object, &env); err != nil {
		return bootstrap.Environment{}, err
	}
	env.Name = adminMigrationSourceStringDefault(object, env.Name, "name")
	if env.Name == "_default" && env.Description == "" {
		env.Description = "The default Chef environment"
	}
	env.JSONClass = adminMigrationSourceStringDefault(object, "Chef::Environment", "json_class")
	env.ChefType = adminMigrationSourceStringDefault(object, "environment", "chef_type")
	if env.CookbookVersions == nil {
		env.CookbookVersions = map[string]string{}
	}
	if env.DefaultAttributes == nil {
		env.DefaultAttributes = map[string]any{}
	}
	if env.OverrideAttributes == nil {
		env.OverrideAttributes = map[string]any{}
	}
	return env, nil
}

// adminMigrationSourceImportRole keeps run-list ordering and env-specific
// overrides stable for depsolver and role-linked read compatibility.
func adminMigrationSourceImportRole(object map[string]any) (bootstrap.Role, error) {
	var role bootstrap.Role
	if err := adminMigrationSourceImportDecode(object, &role); err != nil {
		return bootstrap.Role{}, err
	}
	role.Name = adminMigrationSourceStringDefault(object, role.Name, "name", "rolename")
	role.JSONClass = adminMigrationSourceStringDefault(object, "Chef::Role", "json_class")
	role.ChefType = adminMigrationSourceStringDefault(object, "role", "chef_type")
	if role.DefaultAttributes == nil {
		role.DefaultAttributes = map[string]any{}
	}
	if role.OverrideAttributes == nil {
		role.OverrideAttributes = map[string]any{}
	}
	if role.RunList == nil {
		role.RunList = []string{}
	}
	if role.EnvRunLists == nil {
		role.EnvRunLists = map[string][]string{}
	}
	return role, nil
}

// adminMigrationSourceImportDefaultEnvironment mirrors the bootstrap default
// environment so source imports remain readable even if a source omits it.
func adminMigrationSourceImportDefaultEnvironment() bootstrap.Environment {
	return bootstrap.Environment{
		Name:               "_default",
		Description:        "The default Chef environment",
		CookbookVersions:   map[string]string{},
		JSONClass:          "Chef::Environment",
		ChefType:           "environment",
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	}
}

// adminMigrationSourceImportCookbookVersion decodes cookbook metadata already
// validated by source normalize while preserving Chef's frozen? compatibility field.
func adminMigrationSourceImportCookbookVersion(object map[string]any) (bootstrap.CookbookVersion, error) {
	var version bootstrap.CookbookVersion
	if err := adminMigrationSourceImportDecode(object, &version); err != nil {
		return bootstrap.CookbookVersion{}, err
	}
	version.CookbookName = adminMigrationSourceStringDefault(object, version.CookbookName, "cookbook_name")
	version.Version = adminMigrationSourceStringDefault(object, version.Version, "version")
	version.Name = adminMigrationSourceStringDefault(object, version.CookbookName+"-"+version.Version, "name")
	version.JSONClass = adminMigrationSourceStringDefault(object, "Chef::CookbookVersion", "json_class")
	version.ChefType = adminMigrationSourceStringDefault(object, "cookbook_version", "chef_type")
	if frozen, ok, err := adminMigrationSourceOptionalBool(object, "frozen?"); err != nil {
		return bootstrap.CookbookVersion{}, err
	} else if ok {
		version.Frozen = frozen
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{"name": version.CookbookName, "version": version.Version}
	}
	if version.AllFiles == nil {
		version.AllFiles = []bootstrap.CookbookFile{}
	}
	return version, nil
}

// adminMigrationSourceImportCookbookArtifact decodes cookbook artifact rows in
// the provider-backed shape used by the existing cookbook repository importer.
func adminMigrationSourceImportCookbookArtifact(object map[string]any) (bootstrap.CookbookArtifact, error) {
	var artifact bootstrap.CookbookArtifact
	if err := adminMigrationSourceImportDecode(object, &artifact); err != nil {
		return bootstrap.CookbookArtifact{}, err
	}
	artifact.Name = adminMigrationSourceStringDefault(object, artifact.Name, "name", "cookbook_name")
	artifact.Identifier = strings.ToLower(adminMigrationSourceStringDefault(object, artifact.Identifier, "identifier"))
	artifact.Version = adminMigrationSourceStringDefault(object, artifact.Version, "version")
	artifact.ChefType = adminMigrationSourceStringDefault(object, "cookbook_version", "chef_type")
	if frozen, ok, err := adminMigrationSourceOptionalBool(object, "frozen?"); err != nil {
		return bootstrap.CookbookArtifact{}, err
	} else if ok {
		artifact.Frozen = frozen
	}
	if artifact.Metadata == nil {
		artifact.Metadata = map[string]any{"name": artifact.Name, "version": artifact.Version}
	}
	if artifact.AllFiles == nil {
		artifact.AllFiles = []bootstrap.CookbookFile{}
	}
	return artifact, nil
}

// adminMigrationSourceImportDecode uses JSON tags to convert canonical source
// maps into bootstrap structs without relying on unexported service normalizers.
func adminMigrationSourceImportDecode(object map[string]any, out any) error {
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

// adminMigrationSourceImportCloneMap deep-copies JSON-like maps so imported
// data bag and policy payloads are isolated from temporary source objects.
func adminMigrationSourceImportCloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	data, err := json.Marshal(in)
	if err != nil {
		return adminMigrationCopySourceObject(in)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil || out == nil {
		return adminMigrationCopySourceObject(in)
	}
	return out
}

// adminMigrationSourceImportStringMap accepts optional policy assignment maps
// from source payloads and ignores non-string values already rejected upstream.
func adminMigrationSourceImportStringMap(value any) map[string]string {
	out := map[string]string{}
	raw, ok := value.(map[string]any)
	if !ok {
		return out
	}
	for key, value := range raw {
		if text, ok := value.(string); ok {
			out[key] = text
		}
	}
	return out
}

// adminMigrationBuildNormalizedSourceBundle reads a local source artifact into
// an in-memory bundle so parse failures never leave partial output behind.
func adminMigrationBuildNormalizedSourceBundle(sourcePath string) (adminMigrationSourceNormalizeBundle, error) {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source normalize requires a PATH")}}, fmt.Errorf("source path is required")
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source artifact could not be read")}}, err
	}
	if info.IsDir() {
		if manifestPath, ok := adminMigrationFindSourceManifest(sourcePath); ok {
			return adminMigrationNormalizeSourceManifestDirectory(sourcePath, manifestPath)
		}
		entries, err := adminMigrationSourceDirectoryFileEntries(sourcePath)
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_artifact_unavailable", "source artifact could not be scanned")}}, err
		}
		return adminMigrationNormalizeSourceEntries("extracted_chef_server_artifact", entries)
	}
	if adminMigrationSourceLooksLikeJSONManifest(sourcePath) {
		return adminMigrationNormalizeSourceManifestDirectory(filepath.Dir(sourcePath), sourcePath)
	}
	if adminMigrationSourceLooksLikeArchive(sourcePath) {
		entries, err := adminMigrationSourceArchiveFileEntries(sourcePath)
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationNormalizeArchiveFinding(err)}}, err
		}
		return adminMigrationNormalizeSourceEntries("chef_server_backup_archive", entries)
	}
	return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{{
		Severity: "error",
		Code:     "source_artifact_unsupported",
		Family:   "source_inventory",
		Message:  "source artifact exists, but only source manifests, extracted directories, and tar/tar.gz archives can be normalized",
	}}}, fmt.Errorf("unsupported source artifact")
}

// adminMigrationNormalizeSourceManifestDirectory canonicalizes an existing
// normalized source manifest while adding payload hashes for downstream gates.
func adminMigrationNormalizeSourceManifestDirectory(sourceRoot, manifestPath string) (adminMigrationSourceNormalizeBundle, error) {
	manifest, formatVersion, finding, err := adminMigrationLoadSourceManifestForNormalize(manifestPath)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{finding}}, err
	}
	if len(manifest.Payloads) == 0 {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_payloads_missing", "source manifest does not declare normalized payload files")}}, fmt.Errorf("source manifest has no payloads")
	}

	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	for _, payload := range adminMigrationSortedSourceManifestPayloads(manifest.Payloads) {
		relativePath, err := adminMigrationNormalizeSourceRelativePath(payload.Path)
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_path_unsafe", "source manifest contains an unsafe payload path")}}, err
		}
		values, err := adminMigrationReadSourcePayloadValues(filepath.Join(sourceRoot, filepath.FromSlash(relativePath)))
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload JSON could not be parsed")}}, err
		}
		key, err := adminMigrationSourcePayloadKeyFromManifest(payload, values)
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
		}
		payloadValues[key] = append(payloadValues[key], values...)
	}
	if err := adminMigrationNormalizeIdentityPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	if err := adminMigrationNormalizeCoreObjectPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	if err := adminMigrationNormalizeCookbookPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	files := map[string][]byte{}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(payloadValues, files)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload JSON could not be normalized")}}, err
	}

	artifacts, artifactFiles, findings, err := adminMigrationCopyManifestArtifacts(sourceRoot, manifest.Artifacts)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_path_unsafe", "source manifest contains an unsafe artifact path")}}, err
	}
	if err := adminMigrationValidateCopiedSourceBlobFiles(artifactFiles); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	findings = append(findings, adminMigrationUnknownPayloadFamilyFindings(payloadValues)...)
	findings = append(findings, adminMigrationMissingCopiedSourceBlobFindings(payloadValues, artifactFiles)...)
	for path, data := range artifactFiles {
		files[path] = data
	}

	normalizedManifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    adminMigrationDefaultSourceType(manifest.SourceType, "normalized_chef_source"),
		Payloads:      payloads,
		Artifacts:     adminMigrationSortedSourceManifestArtifacts(artifacts),
		Notes:         append([]string(nil), manifest.Notes...),
	}
	return adminMigrationSourceNormalizeBundle{
		Manifest:      normalizedManifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(normalizedManifest),
		Findings:      findings,
		SourceType:    normalizedManifest.SourceType,
		FormatVersion: formatVersion,
	}, nil
}

// adminMigrationNormalizeSourceEntries groups generated JSON files from an
// extracted directory or archive into deterministic normalized payload files.
func adminMigrationNormalizeSourceEntries(sourceType string, entries []adminMigrationSourceArtifactFileEntry) (adminMigrationSourceNormalizeBundle, error) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Path != entries[j].Path {
			return entries[i].Path < entries[j].Path
		}
		return !entries[i].IsDir && entries[j].IsDir
	})
	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	files := map[string][]byte{}
	orgs := map[string]struct{}{}
	blobChecksums := map[string]struct{}{}
	searchCount := 0
	unsupportedCounts := map[string]int{}
	inventoryEntries := make([]adminMigrationSourceArtifactEntry, 0, len(entries))

	for _, entry := range entries {
		inventoryEntries = append(inventoryEntries, adminMigrationSourceArtifactEntry{Path: entry.Path, IsDir: entry.IsDir})
		parts := adminMigrationSourcePathParts(entry.Path)
		if len(parts) == 0 {
			continue
		}
		if len(parts) >= 2 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
			orgs[parts[1]] = struct{}{}
		}
		if checksum, ok := adminMigrationSourceBookshelfChecksum(parts); ok && !entry.IsDir {
			blobChecksums[checksum] = struct{}{}
			files[pathpkg.Join("blobs", "checksums", checksum)] = append([]byte(nil), entry.Data...)
			continue
		}
		if adminMigrationSourcePathContainsAny(parts, "elasticsearch", "opensearch", "opscode-solr4", "solr") && !entry.IsDir {
			searchCount++
			files[pathpkg.Join("derived", "opensearch", entry.Path)] = append([]byte(nil), entry.Data...)
			continue
		}
		if family := adminMigrationUnsupportedSourceFamily(parts[0]); family != "" {
			if !entry.IsDir {
				unsupportedCounts[family]++
				remainder := strings.Join(parts[1:], "/")
				if remainder == "" {
					remainder = pathpkg.Base(entry.Path)
				}
				files[pathpkg.Join("unsupported", family, remainder)] = append([]byte(nil), entry.Data...)
			}
			continue
		}
		orgName, family, synthetic, ok := adminMigrationSourceNormalizeTargetForEntry(parts, entry.IsDir)
		if !ok {
			continue
		}
		key := adminMigrationSourcePayloadKey{Organization: orgName, Family: family}
		if synthetic != nil {
			payloadValues[key] = append(payloadValues[key], synthetic)
			continue
		}
		values, err := adminMigrationCanonicalSourcePayloadValues(entry.Data)
		if err != nil {
			return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload JSON could not be parsed")}}, err
		}
		if family == "data_bag_items" {
			values, err = adminMigrationAttachSourceDataBagName(parts, values)
			if err != nil {
				return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
			}
		}
		if family == "cookbook_versions" {
			values, err = adminMigrationAttachSourceCookbookVersionRoute(parts, values)
			if err != nil {
				return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
			}
		}
		payloadValues[key] = append(payloadValues[key], values...)
	}
	for _, org := range adminMigrationSortedMapKeys(orgs) {
		key := adminMigrationSourcePayloadKey{Organization: org, Family: "organizations"}
		if len(payloadValues[key]) == 0 {
			payloadValues[key] = append(payloadValues[key], adminMigrationSyntheticSourceObject("name", org))
		}
	}
	if err := adminMigrationNormalizeIdentityPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	if err := adminMigrationNormalizeCoreObjectPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	if err := adminMigrationNormalizeCookbookPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}
	if err := adminMigrationValidateCopiedSourceBlobFiles(files); err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSemanticFinding(err)}}, err
	}

	payloads, err := adminMigrationMaterializeSourcePayloadFiles(payloadValues, files)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload JSON could not be normalized")}}, err
	}
	artifacts := adminMigrationSourceArtifactsFromSideChannels(blobChecksums, searchCount, unsupportedCounts)
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    sourceType,
		Payloads:      payloads,
		Artifacts:     artifacts,
		Notes:         []string{"Generated by opencook admin migration source normalize."},
	}
	inventoryRead := adminMigrationSourceInventoryFromEntries(sourceType, adminMigrationChefSourceFormatV1, inventoryEntries)
	findings := adminMigrationNormalizeFindings(inventoryRead.Findings)
	findings = append(findings, adminMigrationUnknownSourceEntryFamilyFindings(entries)...)
	findings = append(findings, adminMigrationUnsupportedCookbookSourceLayoutFindings(entries)...)
	findings = append(findings, adminMigrationUnknownPayloadFamilyFindings(payloadValues)...)
	findings = append(findings, adminMigrationMissingCopiedSourceBlobFindings(payloadValues, files)...)
	return adminMigrationSourceNormalizeBundle{
		Manifest:      manifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(manifest),
		Findings:      findings,
		SourceType:    sourceType,
		FormatVersion: adminMigrationChefSourceFormatV1,
	}, nil
}

// adminMigrationWriteNormalizedSourceBundle writes through a sibling temporary
// directory so failed writes do not leave a half-normalized source bundle.
func adminMigrationWriteNormalizedSourceBundle(outputPath string, bundle adminMigrationSourceNormalizeBundle, overwrite bool) error {
	outputPath = strings.TrimSpace(outputPath)
	parent := filepath.Dir(outputPath)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return err
	}
	tempDir, err := os.MkdirTemp(parent, "."+filepath.Base(outputPath)+"-normalize-*")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(tempDir)
		}
	}()
	for _, path := range adminMigrationSortedMapKeys(bundle.Files) {
		if err := adminMigrationWriteNormalizedSourceFile(tempDir, path, bundle.Files[path]); err != nil {
			return err
		}
	}
	manifestData, err := json.MarshalIndent(bundle.Manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestData = append(manifestData, '\n')
	if err := adminMigrationWriteNormalizedSourceFile(tempDir, adminMigrationSourceManifestPath, manifestData); err != nil {
		return err
	}
	if overwrite {
		if err := os.RemoveAll(outputPath); err != nil {
			return err
		}
	}
	if err := os.Rename(tempDir, outputPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}

// adminMigrationWriteNormalizedSourceFile writes one already-validated relative
// bundle path and rechecks containment before touching the filesystem.
func adminMigrationWriteNormalizedSourceFile(root, relativePath string, data []byte) error {
	relativePath, err := adminMigrationNormalizeSourceRelativePath(relativePath)
	if err != nil {
		return err
	}
	target := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	return os.WriteFile(target, data, 0o644)
}

// adminMigrationLoadSourceManifestForNormalize decodes a manifest for writing
// output and reports parser failures with stable, provider-free finding codes.
func adminMigrationLoadSourceManifestForNormalize(path string) (adminMigrationSourceManifest, string, adminMigrationFinding, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		finding := adminMigrationSourceErrorFinding("source_manifest_missing", "source manifest could not be read")
		return adminMigrationSourceManifest{}, "", finding, err
	}
	var manifest adminMigrationSourceManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		finding := adminMigrationSourceErrorFinding("source_manifest_invalid_json", "source manifest is not valid JSON")
		return adminMigrationSourceManifest{}, "", finding, err
	}
	formatVersion := strings.TrimSpace(manifest.FormatVersion)
	if formatVersion == "" {
		formatVersion = adminMigrationSourceFormatV1
	}
	if !adminMigrationSourceManifestFormatSupported(formatVersion) {
		finding := adminMigrationSourceErrorFinding("source_manifest_unsupported_format", "source manifest format version is not supported")
		return manifest, formatVersion, finding, fmt.Errorf("unsupported source manifest format %q", formatVersion)
	}
	return manifest, formatVersion, adminMigrationFinding{}, nil
}

// adminMigrationCopyManifestArtifacts preserves side-channel files declared by
// an explicit normalized manifest without treating derived data as authoritative.
func adminMigrationCopyManifestArtifacts(sourceRoot string, artifacts []adminMigrationSourceManifestArtifact) ([]adminMigrationSourceManifestArtifact, map[string][]byte, []adminMigrationFinding, error) {
	files := map[string][]byte{}
	findings := adminMigrationManifestArtifactFindings(artifacts)
	normalized := make([]adminMigrationSourceManifestArtifact, 0, len(artifacts))
	for _, artifact := range adminMigrationSortedSourceManifestArtifacts(artifacts) {
		artifact.Family = strings.TrimSpace(artifact.Family)
		if artifact.Family == "" {
			continue
		}
		if strings.TrimSpace(artifact.Path) != "" {
			relativePath, err := adminMigrationNormalizeSourceRelativePath(artifact.Path)
			if err != nil {
				return nil, nil, nil, err
			}
			artifact.Path = relativePath
			copied, err := adminMigrationCopySourceTreeFiles(sourceRoot, relativePath)
			if err != nil {
				return nil, nil, nil, err
			}
			for path, data := range copied {
				files[path] = data
			}
		}
		normalized = append(normalized, artifact)
	}
	return normalized, files, findings, nil
}

// adminMigrationCopySourceTreeFiles copies declared artifact files into memory
// after validating their manifest-relative paths.
func adminMigrationCopySourceTreeFiles(root, relativePath string) (map[string][]byte, error) {
	files := map[string][]byte{}
	sourcePath := filepath.Join(root, filepath.FromSlash(relativePath))
	info, err := os.Stat(sourcePath)
	if err != nil {
		return files, err
	}
	if !info.IsDir() {
		data, err := os.ReadFile(sourcePath)
		if err != nil {
			return nil, err
		}
		files[relativePath] = data
		return files, nil
	}
	err = filepath.WalkDir(sourcePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == sourcePath || d.IsDir() {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errAdminMigrationUnsafeSourcePath
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative, err = adminMigrationNormalizeSourceRelativePath(relative)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files[relative] = data
		return nil
	})
	return files, err
}

// adminMigrationReadSourcePayloadValues loads one payload file and canonicalizes
// it as a JSON array of objects for deterministic hashing.
func adminMigrationReadSourcePayloadValues(path string) ([]json.RawMessage, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return adminMigrationCanonicalSourcePayloadValues(data)
}

// adminMigrationCanonicalSourcePayloadValues accepts either one JSON object or
// an array of objects and returns canonical object bytes with trailing JSON rejected.
func adminMigrationCanonicalSourcePayloadValues(data []byte) ([]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("payload contains trailing JSON")
	}
	switch value := decoded.(type) {
	case map[string]any:
		canonical, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return []json.RawMessage{canonical}, nil
	case []any:
		values := make([]json.RawMessage, 0, len(value))
		for _, item := range value {
			object, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("payload array contains non-object item")
			}
			canonical, err := json.Marshal(object)
			if err != nil {
				return nil, err
			}
			values = append(values, canonical)
		}
		return values, nil
	default:
		return nil, fmt.Errorf("payload must be a JSON object or array of objects")
	}
}

// adminMigrationMarshalSourcePayloadValues writes canonical object payloads as a
// stable, newline-terminated JSON array so SHA-256 values are reproducible.
func adminMigrationMarshalSourcePayloadValues(values []json.RawMessage) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i, value := range values {
		if !json.Valid(value) {
			return nil, fmt.Errorf("invalid canonical JSON payload")
		}
		buf.WriteString("  ")
		buf.Write(value)
		if i < len(values)-1 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
	}
	buf.WriteString("]\n")
	return buf.Bytes(), nil
}

// adminMigrationMaterializeSourcePayloadFiles turns grouped raw payload objects
// into manifest payload records with byte hashes and deterministic paths.
func adminMigrationMaterializeSourcePayloadFiles(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, files map[string][]byte) ([]adminMigrationSourceManifestPayload, error) {
	keys := make([]adminMigrationSourcePayloadKey, 0, len(payloadValues))
	for key := range payloadValues {
		if key.Family != "" && len(payloadValues[key]) > 0 {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Organization != keys[j].Organization {
			return keys[i].Organization < keys[j].Organization
		}
		return keys[i].Family < keys[j].Family
	})
	payloads := make([]adminMigrationSourceManifestPayload, 0, len(keys))
	for _, key := range keys {
		path := adminMigrationSourcePayloadPath(key.Organization, key.Family)
		data, err := adminMigrationMarshalSourcePayloadValues(payloadValues[key])
		if err != nil {
			return nil, err
		}
		files[path] = data
		payload := adminMigrationSourceManifestPayload{
			Family: key.Family,
			Path:   path,
			Count:  len(payloadValues[key]),
			SHA256: adminMigrationSHA256Hex(data),
		}
		if key.Family != "organizations" {
			payload.Organization = key.Organization
		}
		payloads = append(payloads, payload)
	}
	return payloads, nil
}

// adminMigrationNormalizeIdentityPayloads canonicalizes the first importable
// source families and validates references before any normalized files are written.
func adminMigrationNormalizeIdentityPayloads(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	users, err := adminMigrationNormalizeSourceUsers(payloadValues)
	if err != nil {
		return err
	}
	orgs, err := adminMigrationNormalizeSourceOrganizations(payloadValues)
	if err != nil {
		return err
	}
	if err := adminMigrationNormalizeSourceUserKeys(payloadValues, users); err != nil {
		return err
	}
	if err := adminMigrationNormalizeSourceUserACLs(payloadValues, users); err != nil {
		return err
	}
	if err := adminMigrationNormalizeSourceServerAdminMemberships(payloadValues, users); err != nil {
		return err
	}
	for _, orgName := range adminMigrationSortedMapKeys(orgs) {
		clients, err := adminMigrationNormalizeSourceClients(payloadValues, orgName)
		if err != nil {
			return err
		}
		groups, err := adminMigrationNormalizeSourceGroups(payloadValues, orgName, users, clients)
		if err != nil {
			return err
		}
		containers, err := adminMigrationNormalizeSourceContainers(payloadValues, orgName)
		if err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourceClientKeys(payloadValues, orgName, clients); err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourceGroupMemberships(payloadValues, orgName, users, clients, groups); err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourceOrgACLs(payloadValues, orgName, users, clients, groups, containers); err != nil {
			return err
		}
	}
	for key := range payloadValues {
		if key.Organization != "" && !orgs[key.Organization] {
			return adminMigrationSourceSemanticError{Code: "source_missing_organization", Message: "source payload references an organization that is not present"}
		}
	}
	return nil
}

// adminMigrationNormalizeCoreObjectPayloads canonicalizes org-scoped Chef
// object families after identity state exists, so ownership and ACL links can be
// validated against the same normalized org graph import preflight will read.
func adminMigrationNormalizeCoreObjectPayloads(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	orgs := adminMigrationSourceOrgPayloadNames(payloadValues)
	for _, orgName := range adminMigrationSortedMapKeys(orgs) {
		nodes, err := adminMigrationNormalizeSourceNodes(payloadValues, orgName)
		if err != nil {
			return err
		}
		environments, err := adminMigrationNormalizeSourceEnvironments(payloadValues, orgName)
		if err != nil {
			return err
		}
		roles, err := adminMigrationNormalizeSourceRoles(payloadValues, orgName)
		if err != nil {
			return err
		}
		dataBags, err := adminMigrationNormalizeSourceDataBags(payloadValues, orgName)
		if err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourceDataBagItems(payloadValues, orgName, dataBags); err != nil {
			return err
		}
		policies, policyRevisions, err := adminMigrationNormalizeSourcePolicyRevisions(payloadValues, orgName)
		if err != nil {
			return err
		}
		policyGroups, err := adminMigrationNormalizeSourcePolicyGroups(payloadValues, orgName)
		if err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourcePolicyAssignments(payloadValues, orgName, policyGroups, policyRevisions); err != nil {
			return err
		}
		sandboxes, sandboxChecksums, err := adminMigrationNormalizeSourceSandboxes(payloadValues, orgName)
		if err != nil {
			return err
		}
		if err := adminMigrationNormalizeSourceChecksumReferences(payloadValues, orgName, sandboxChecksums); err != nil {
			return err
		}
		if err := adminMigrationValidateSourceObjectACLs(payloadValues, orgName, nodes, environments, roles, dataBags, policies, policyGroups, sandboxes); err != nil {
			return err
		}
	}
	return nil
}

// adminMigrationSourceOrgPayloadNames returns the activated normalized orgs
// after identity normalization has moved organization rows into per-org keys.
func adminMigrationSourceOrgPayloadNames(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) map[string]bool {
	orgs := map[string]bool{}
	for key, values := range payloadValues {
		if key.Family == "organizations" && key.Organization != "" && len(values) > 0 {
			orgs[key.Organization] = true
		}
	}
	return orgs
}

// adminMigrationNormalizeSourceNodes preserves node payload fields while
// pinning the canonical Chef node identity fields and run-list shape.
func adminMigrationNormalizeSourceNodes(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "nodes"}
	values := payloadValues[key]
	nodes := map[string]bool{}
	if len(values) == 0 {
		return nodes, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "nodename")
		if err := adminMigrationValidateSourceName(name, "source_node_invalid"); err != nil {
			return nil, err
		}
		if nodes[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_node", Message: "source node records contain a duplicate name"}
		}
		nodes[name] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["name"] = name
		canonical["chef_type"] = adminMigrationSourceStringDefault(object, "node", "chef_type")
		canonical["json_class"] = adminMigrationSourceStringDefault(object, "Chef::Node", "json_class")
		if runList, ok, err := adminMigrationSourceOrderedStringSlice(object, "run_list", "source_node_invalid"); err != nil {
			return nil, err
		} else if ok {
			canonical["run_list"] = runList
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return nodes, nil
}

// adminMigrationNormalizeSourceEnvironments keeps environment attributes and
// cookbook constraints intact while validating names and Chef metadata defaults.
func adminMigrationNormalizeSourceEnvironments(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "environments"}
	values := payloadValues[key]
	environments := map[string]bool{}
	if len(values) == 0 {
		return environments, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name")
		if err := adminMigrationValidateSourceName(name, "source_environment_invalid"); err != nil {
			return nil, err
		}
		if environments[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_environment", Message: "source environment records contain a duplicate name"}
		}
		environments[name] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["name"] = name
		canonical["chef_type"] = adminMigrationSourceStringDefault(object, "environment", "chef_type")
		canonical["json_class"] = adminMigrationSourceStringDefault(object, "Chef::Environment", "json_class")
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return environments, nil
}

// adminMigrationNormalizeSourceRoles preserves role attributes and ordered
// run-lists because Chef clients observe run-list order during expansion.
func adminMigrationNormalizeSourceRoles(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "roles"}
	values := payloadValues[key]
	roles := map[string]bool{}
	if len(values) == 0 {
		return roles, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "rolename")
		if err := adminMigrationValidateSourceName(name, "source_role_invalid"); err != nil {
			return nil, err
		}
		if roles[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_role", Message: "source role records contain a duplicate name"}
		}
		roles[name] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["name"] = name
		canonical["chef_type"] = adminMigrationSourceStringDefault(object, "role", "chef_type")
		canonical["json_class"] = adminMigrationSourceStringDefault(object, "Chef::Role", "json_class")
		if runList, ok, err := adminMigrationSourceOrderedStringSlice(object, "run_list", "source_role_invalid"); err != nil {
			return nil, err
		} else if ok {
			canonical["run_list"] = runList
		}
		if envRunLists, ok, err := adminMigrationNormalizeSourceRoleEnvRunLists(object); err != nil {
			return nil, err
		} else if ok {
			canonical["env_run_lists"] = envRunLists
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return roles, nil
}

// adminMigrationNormalizeSourceDataBags canonicalizes bag names separately from
// item payloads so encrypted-looking item JSON can remain untouched.
func adminMigrationNormalizeSourceDataBags(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bags"}
	values := payloadValues[key]
	dataBags := map[string]bool{}
	if len(values) == 0 {
		return dataBags, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "bag")
		if err := adminMigrationValidateSourceName(name, "source_data_bag_invalid"); err != nil {
			return nil, err
		}
		if dataBags[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_data_bag", Message: "source data bag records contain a duplicate name"}
		}
		dataBags[name] = true
		objects = append(objects, map[string]any{"name": name})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return dataBags, nil
}

// adminMigrationNormalizeSourceDataBagItems wraps raw item JSON in the stable
// bag/id/payload source shape while preserving encrypted or plain item bodies.
func adminMigrationNormalizeSourceDataBagItems(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, dataBags map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "data_bag_items"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		bag := adminMigrationSourceString(object, "bag", "data_bag")
		if !dataBags[bag] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_data_bag_item", Message: "source data bag item references a missing data bag"}
		}
		id := adminMigrationSourceString(object, "id", "item", "name")
		if err := adminMigrationValidateSourceName(id, "source_data_bag_item_invalid"); err != nil {
			return err
		}
		identity := bag + "/" + id
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_data_bag_item", Message: "source data bag item records contain a duplicate bag/id"}
		}
		seen[identity] = true
		payload, err := adminMigrationSourceDataBagItemPayload(object, id)
		if err != nil {
			return err
		}
		objects = append(objects, map[string]any{"bag": bag, "id": id, "payload": payload})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "bag", "id")
	return nil
}

// adminMigrationNormalizeSourcePolicyRevisions validates immutable revision IDs
// while retaining policyfile locks, run-lists, and solution metadata verbatim.
func adminMigrationNormalizeSourcePolicyRevisions(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_revisions"}
	values := payloadValues[key]
	policies := map[string]bool{}
	revisions := map[string]bool{}
	if len(values) == 0 {
		return policies, revisions, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, nil, err
		}
		name := adminMigrationSourceString(object, "name", "policy", "policy_name")
		if err := adminMigrationValidateSourceName(name, "source_policy_invalid"); err != nil {
			return nil, nil, err
		}
		revisionID := strings.ToLower(adminMigrationSourceString(object, "revision_id", "revision"))
		if !adminMigrationPolicyRevisionPattern.MatchString(revisionID) {
			return nil, nil, adminMigrationSourceSemanticError{Code: "source_policy_revision_invalid", Message: "source policy revision_id must be a 40-character hexadecimal identifier"}
		}
		identity := name + "/" + revisionID
		if revisions[identity] {
			return nil, nil, adminMigrationSourceSemanticError{Code: "source_duplicate_policy_revision", Message: "source policy revision records contain a duplicate policy/revision_id"}
		}
		policies[name] = true
		revisions[identity] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["name"] = name
		canonical["revision_id"] = revisionID
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name", "revision_id")
	return policies, revisions, nil
}

// adminMigrationNormalizeSourcePolicyGroups validates policy-group names while
// preserving any source-supplied assignment summary fields for later preflight.
func adminMigrationNormalizeSourcePolicyGroups(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_groups"}
	values := payloadValues[key]
	groups := map[string]bool{}
	if len(values) == 0 {
		return groups, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "group")
		if err := adminMigrationValidateSourceName(name, "source_policy_group_invalid"); err != nil {
			return nil, err
		}
		if groups[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_policy_group", Message: "source policy group records contain a duplicate name"}
		}
		groups[name] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["name"] = name
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return groups, nil
}

// adminMigrationNormalizeSourcePolicyAssignments validates the policyfile link
// graph so restored groups cannot point at absent policy revisions.
func adminMigrationNormalizeSourcePolicyAssignments(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, policyGroups, policyRevisions map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "policy_assignments"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		group := adminMigrationSourceString(object, "group", "policy_group")
		if !policyGroups[group] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_policy_assignment", Message: "source policy assignment references a missing policy group"}
		}
		policy := adminMigrationSourceString(object, "policy", "policy_name")
		if err := adminMigrationValidateSourceName(policy, "source_policy_assignment_invalid"); err != nil {
			return err
		}
		revisionID := strings.ToLower(adminMigrationSourceString(object, "revision_id", "revision"))
		if !adminMigrationPolicyRevisionPattern.MatchString(revisionID) {
			return adminMigrationSourceSemanticError{Code: "source_policy_revision_invalid", Message: "source policy revision_id must be a 40-character hexadecimal identifier"}
		}
		if !policyRevisions[policy+"/"+revisionID] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_policy_assignment", Message: "source policy assignment references a missing policy revision"}
		}
		identity := group + "/" + policy
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_policy_assignment", Message: "source policy assignments contain a duplicate group/policy"}
		}
		seen[identity] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["group"] = group
		canonical["policy"] = policy
		canonical["revision_id"] = revisionID
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "group", "policy")
	return nil
}

// adminMigrationNormalizeSourceSandboxes admits only completed sandbox metadata
// and validates every referenced checksum before blob-copy planning sees it.
func adminMigrationNormalizeSourceSandboxes(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "sandboxes"}
	values := payloadValues[key]
	sandboxes := map[string]bool{}
	checksums := map[string]bool{}
	if len(values) == 0 {
		return sandboxes, checksums, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, nil, err
		}
		id := adminMigrationSourceString(object, "id", "sandbox_id", "name")
		if err := adminMigrationValidateSourceName(id, "source_sandbox_invalid"); err != nil {
			return nil, nil, err
		}
		completed, ok := object["completed"].(bool)
		if !ok || !completed {
			return nil, nil, adminMigrationSourceSemanticError{Code: "source_sandbox_incomplete", Message: "source sandbox metadata must be completed before import"}
		}
		if sandboxes[id] {
			return nil, nil, adminMigrationSourceSemanticError{Code: "source_duplicate_sandbox", Message: "source sandbox records contain a duplicate id"}
		}
		sandboxes[id] = true
		sandboxChecksums, ok, err := adminMigrationSourceOrderedStringSlice(object, "checksums", "source_sandbox_invalid")
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			sandboxChecksums = []string{}
		}
		for i, checksum := range sandboxChecksums {
			checksum = strings.ToLower(checksum)
			if !adminMigrationChecksumPattern.MatchString(checksum) {
				return nil, nil, adminMigrationSourceSemanticError{Code: "source_checksum_invalid", Message: "source checksum must be a 32-character hexadecimal identifier"}
			}
			sandboxChecksums[i] = checksum
			checksums[checksum] = true
		}
		canonical := adminMigrationCopySourceObject(object)
		canonical["id"] = id
		canonical["completed"] = true
		canonical["checksums"] = adminMigrationUniqueSortedStrings(sandboxChecksums)
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "id")
	return sandboxes, checksums, nil
}

// adminMigrationNormalizeSourceChecksumReferences deduplicates checksum rows and
// ensures every completed sandbox checksum has an explicit source reference.
func adminMigrationNormalizeSourceChecksumReferences(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, requiredChecksums map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}
	values := payloadValues[key]
	if len(values) == 0 {
		if len(requiredChecksums) > 0 {
			return adminMigrationSourceSemanticError{Code: "source_checksum_reference_missing", Message: "source sandbox checksum is missing a checksum reference row"}
		}
		return nil
	}
	seen := map[string]bool{}
	referenced := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		checksum := strings.ToLower(adminMigrationSourceString(object, "checksum", "id"))
		if !adminMigrationChecksumPattern.MatchString(checksum) {
			return adminMigrationSourceSemanticError{Code: "source_checksum_invalid", Message: "source checksum must be a 32-character hexadecimal identifier"}
		}
		family := adminMigrationSourceString(object, "family", "source_family", "type")
		if family == "" {
			return adminMigrationSourceSemanticError{Code: "source_checksum_reference_invalid", Message: "source checksum reference is missing a family"}
		}
		identity := family + "/" + checksum
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_checksum_reference", Message: "source checksum reference rows contain a duplicate family/checksum"}
		}
		seen[identity] = true
		referenced[checksum] = true
		canonical := adminMigrationCopySourceObject(object)
		canonical["checksum"] = checksum
		canonical["family"] = family
		objects = append(objects, canonical)
	}
	for checksum := range requiredChecksums {
		if !referenced[checksum] {
			return adminMigrationSourceSemanticError{Code: "source_checksum_reference_missing", Message: "source sandbox checksum is missing a checksum reference row"}
		}
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "checksum", "family")
	return nil
}

// adminMigrationValidateSourceObjectACLs performs the second ACL pass for core
// objects whose targets are unavailable during identity normalization.
func adminMigrationValidateSourceObjectACLs(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, nodes, environments, roles, dataBags, policies, policyGroups, sandboxes map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}
	for _, raw := range payloadValues[key] {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "organization")
		if err != nil {
			return err
		}
		switch resourceType {
		case "node":
			if !nodes[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing node"}
			}
		case "environment":
			if !environments[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing environment"}
			}
		case "role":
			if !roles[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing role"}
			}
		case "data_bag":
			if !dataBags[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing data bag"}
			}
		case "policy":
			if !policies[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing policy"}
			}
		case "policy_group":
			if !policyGroups[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing policy group"}
			}
		case "sandbox":
			if !sandboxes[resourceName] {
				return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing sandbox"}
			}
		}
	}
	return nil
}

// adminMigrationNormalizeCookbookPayloads canonicalizes cookbook metadata after
// core object state so checksum references can be merged into the same org graph.
func adminMigrationNormalizeCookbookPayloads(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	orgs := adminMigrationSourceOrgPayloadNames(payloadValues)
	for _, orgName := range adminMigrationSortedMapKeys(orgs) {
		checksums, err := adminMigrationNormalizeSourceCookbookVersions(payloadValues, orgName)
		if err != nil {
			return err
		}
		artifactChecksums, err := adminMigrationNormalizeSourceCookbookArtifacts(payloadValues, orgName)
		if err != nil {
			return err
		}
		for checksum := range artifactChecksums {
			checksums[checksum] = true
		}
		if err := adminMigrationEnsureSourceCookbookChecksumReferences(payloadValues, orgName, checksums); err != nil {
			return err
		}
	}
	return nil
}

// adminMigrationNormalizeSourceCookbookVersions preserves cookbook-version
// metadata while pinning route-compatible name/version and normalized file refs.
func adminMigrationNormalizeSourceCookbookVersions(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_versions"}
	values := payloadValues[key]
	checksums := map[string]bool{}
	if len(values) == 0 {
		return checksums, nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		cookbookName, version, err := adminMigrationSourceCookbookVersionIdentity(object)
		if err != nil {
			return nil, err
		}
		identity := cookbookName + "/" + version
		if seen[identity] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_cookbook_version", Message: "source cookbook version records contain a duplicate cookbook/version"}
		}
		seen[identity] = true
		metadata, err := adminMigrationNormalizeSourceCookbookMetadata(object, cookbookName, version, true)
		if err != nil {
			return nil, err
		}
		allFiles, segments, fileChecksums, err := adminMigrationNormalizeSourceCookbookFileCollections(object)
		if err != nil {
			return nil, err
		}
		for checksum := range fileChecksums {
			checksums[checksum] = true
		}
		canonical := adminMigrationCopySourceObject(object)
		delete(canonical, "_source_cookbook_name")
		delete(canonical, "_source_cookbook_version")
		delete(canonical, "checksums")
		canonical["name"] = cookbookName + "-" + version
		canonical["cookbook_name"] = cookbookName
		canonical["version"] = version
		canonical["json_class"] = adminMigrationSourceStringDefault(object, "Chef::CookbookVersion", "json_class")
		canonical["chef_type"] = adminMigrationSourceStringDefault(object, "cookbook_version", "chef_type")
		canonical["metadata"] = metadata
		canonical["all_files"] = allFiles
		for segment, files := range segments {
			canonical[segment] = files
		}
		if frozen, ok, err := adminMigrationSourceOptionalBool(object, "frozen?"); err != nil {
			return nil, err
		} else if ok {
			canonical["frozen?"] = frozen
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "cookbook_name", "version")
	return checksums, nil
}

// adminMigrationNormalizeSourceCookbookArtifacts validates artifact identifiers
// and keeps artifact file metadata parallel to cookbook-version normalization.
func adminMigrationNormalizeSourceCookbookArtifacts(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "cookbook_artifacts"}
	values := payloadValues[key]
	checksums := map[string]bool{}
	if len(values) == 0 {
		return checksums, nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "cookbook_name")
		if err := adminMigrationValidateSourceName(name, "source_cookbook_artifact_invalid"); err != nil {
			return nil, err
		}
		identifier := strings.ToLower(adminMigrationSourceString(object, "identifier"))
		if !adminMigrationCookbookArtifactPattern.MatchString(identifier) {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_artifact_invalid", Message: "source cookbook artifact identifier must be a 40-character hexadecimal identifier"}
		}
		version := adminMigrationSourceString(object, "version")
		if version == "" {
			if metadataObject, ok := object["metadata"].(map[string]any); ok {
				version = adminMigrationSourceString(metadataObject, "version")
			}
		}
		if !adminMigrationCookbookVersionPattern.MatchString(version) {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_version_invalid", Message: "source cookbook artifact version is invalid"}
		}
		identity := name + "/" + identifier
		if seen[identity] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_cookbook_artifact", Message: "source cookbook artifact records contain a duplicate cookbook/identifier"}
		}
		seen[identity] = true
		metadata, err := adminMigrationNormalizeSourceCookbookMetadata(object, name, version, false)
		if err != nil {
			return nil, err
		}
		allFiles, segments, fileChecksums, err := adminMigrationNormalizeSourceCookbookFileCollections(object)
		if err != nil {
			return nil, err
		}
		for checksum := range fileChecksums {
			checksums[checksum] = true
		}
		canonical := adminMigrationCopySourceObject(object)
		delete(canonical, "checksums")
		canonical["name"] = name
		canonical["identifier"] = identifier
		canonical["version"] = version
		canonical["chef_type"] = adminMigrationSourceStringDefault(object, "cookbook_version", "chef_type")
		canonical["metadata"] = metadata
		canonical["all_files"] = allFiles
		for segment, files := range segments {
			canonical[segment] = files
		}
		if frozen, ok, err := adminMigrationSourceOptionalBool(object, "frozen?"); err != nil {
			return nil, err
		} else if ok {
			canonical["frozen?"] = frozen
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name", "identifier")
	return checksums, nil
}

// adminMigrationSourceCookbookVersionIdentity resolves route-attached and
// payload-declared cookbook version identities and rejects mismatches early.
func adminMigrationSourceCookbookVersionIdentity(object map[string]any) (string, string, error) {
	routeName := adminMigrationSourceString(object, "_source_cookbook_name")
	routeVersion := adminMigrationSourceString(object, "_source_cookbook_version")
	cookbookName := adminMigrationSourceString(object, "cookbook_name")
	version := adminMigrationSourceString(object, "version")
	if version == "" {
		if metadataObject, ok := object["metadata"].(map[string]any); ok {
			version = adminMigrationSourceString(metadataObject, "version")
		}
	}
	payloadName := adminMigrationSourceString(object, "name")
	if cookbookName == "" && routeName != "" {
		cookbookName = routeName
	}
	if version == "" && routeVersion != "" {
		version = routeVersion
	}
	if cookbookName == "" && payloadName != "" && version != "" && strings.HasSuffix(payloadName, "-"+version) {
		cookbookName = strings.TrimSuffix(payloadName, "-"+version)
	}
	if err := adminMigrationValidateSourceName(cookbookName, "source_cookbook_invalid"); err != nil {
		return "", "", err
	}
	if !bootstrap.ValidCookbookRouteVersion(version) {
		return "", "", adminMigrationSourceSemanticError{Code: "source_cookbook_version_invalid", Message: "source cookbook version must use x.y.z route format"}
	}
	expectedName := cookbookName + "-" + version
	if payloadName != "" && payloadName != expectedName {
		return "", "", adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook route and payload name do not match"}
	}
	if routeName != "" && routeName != cookbookName {
		return "", "", adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook route and payload cookbook_name do not match"}
	}
	if routeVersion != "" && routeVersion != version {
		return "", "", adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook route and payload version do not match"}
	}
	return cookbookName, version, nil
}

// adminMigrationNormalizeSourceCookbookMetadata keeps metadata maps intact while
// validating the version anchor required for depsolver and API read parity.
func adminMigrationNormalizeSourceCookbookMetadata(object map[string]any, cookbookName, version string, routeVersion bool) (map[string]any, error) {
	metadata, err := adminMigrationSourceCookbookMetadataObject(object)
	if err != nil {
		return nil, err
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	if rawName := adminMigrationSourceString(metadata, "name"); rawName != "" {
		if err := adminMigrationValidateSourceName(rawName, "source_cookbook_invalid"); err != nil {
			return nil, err
		}
	} else {
		metadata["name"] = cookbookName
	}
	metadataVersion := adminMigrationSourceString(metadata, "version")
	if metadataVersion == "" {
		metadata["version"] = version
		return metadata, nil
	}
	if routeVersion {
		if metadataVersion != version {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook metadata.version does not match route version"}
		}
	} else if !adminMigrationCookbookVersionPattern.MatchString(metadataVersion) {
		return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_version_invalid", Message: "source cookbook metadata.version is invalid"}
	}
	metadata["version"] = metadataVersion
	return metadata, nil
}

// adminMigrationSourceCookbookMetadataObject accepts wrapper payloads and raw
// metadata.json payloads without dropping source-supplied metadata sections.
func adminMigrationSourceCookbookMetadataObject(object map[string]any) (map[string]any, error) {
	if value, ok := object["metadata"]; ok && value != nil {
		metadata, ok := value.(map[string]any)
		if !ok {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_metadata_invalid", Message: "source cookbook metadata must be an object"}
		}
		return adminMigrationCopySourceObject(metadata), nil
	}
	metadata := map[string]any{}
	for key, value := range object {
		if adminMigrationSourceCookbookTopLevelKey(key) {
			continue
		}
		metadata[key] = value
	}
	return metadata, nil
}

// adminMigrationSourceCookbookTopLevelKey excludes wrapper and file-collection
// fields when a source artifact provides a raw cookbook metadata.json object.
func adminMigrationSourceCookbookTopLevelKey(key string) bool {
	switch key {
	case "name", "cookbook_name", "identifier", "version", "json_class", "chef_type", "frozen?", "metadata", "all_files", "checksums", "_source_cookbook_name", "_source_cookbook_version":
		return true
	default:
		for _, segment := range adminMigrationCookbookLegacySegments() {
			if key == segment {
				return true
			}
		}
		return false
	}
}

// adminMigrationNormalizeSourceCookbookFileCollections normalizes all_files and
// legacy segment arrays while preserving the legacy arrays when sources include them.
func adminMigrationNormalizeSourceCookbookFileCollections(object map[string]any) ([]map[string]any, map[string]any, map[string]bool, error) {
	checksums := map[string]bool{}
	segments := map[string]any{}
	var allFiles []map[string]any
	hasRootAllFiles := false
	if raw, ok := object["all_files"]; ok {
		files, err := adminMigrationNormalizeSourceCookbookFileList("all_files", raw)
		if err != nil {
			return nil, nil, nil, err
		}
		allFiles = files
		hasRootAllFiles = true
		for _, checksum := range adminMigrationSourceCookbookFileChecksums(files) {
			checksums[checksum] = true
		}
	}
	for _, segment := range adminMigrationCookbookLegacySegments() {
		raw, ok := object[segment]
		if !ok {
			continue
		}
		files, err := adminMigrationNormalizeSourceCookbookFileList(segment, raw)
		if err != nil {
			return nil, nil, nil, err
		}
		segments[segment] = files
		for _, checksum := range adminMigrationSourceCookbookFileChecksums(files) {
			checksums[checksum] = true
		}
		if !hasRootAllFiles {
			allFiles = append(allFiles, files...)
		}
	}
	if len(allFiles) == 0 {
		synthetic, err := adminMigrationSourceCookbookFilesFromChecksumList(object)
		if err != nil {
			return nil, nil, nil, err
		}
		allFiles = synthetic
		for _, checksum := range adminMigrationSourceCookbookFileChecksums(synthetic) {
			checksums[checksum] = true
		}
	}
	adminMigrationSortSourceCookbookFiles(allFiles)
	for segment, value := range segments {
		files := value.([]map[string]any)
		adminMigrationSortSourceCookbookFiles(files)
		segments[segment] = files
	}
	return allFiles, segments, checksums, nil
}

// adminMigrationNormalizeSourceCookbookFileList validates Chef file entries and
// lowercases checksum keys without altering path/name compatibility metadata.
func adminMigrationNormalizeSourceCookbookFileList(segment string, value any) ([]map[string]any, error) {
	rawList, ok := value.([]any)
	if !ok {
		return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_file_invalid", Message: "source cookbook file collection must be an array"}
	}
	files := make([]map[string]any, 0, len(rawList))
	for _, item := range rawList {
		rawFile, ok := item.(map[string]any)
		if !ok {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_file_invalid", Message: "source cookbook file collection entries must be objects"}
		}
		file, err := adminMigrationNormalizeSourceCookbookFile(segment, rawFile)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, nil
}

// adminMigrationNormalizeSourceCookbookFile validates one cookbook file record
// and keeps the four fields Chef clients observe in cookbook read responses.
func adminMigrationNormalizeSourceCookbookFile(segment string, raw map[string]any) (map[string]any, error) {
	name := adminMigrationSourceString(raw, "name")
	path := adminMigrationSourceString(raw, "path")
	checksum := strings.ToLower(adminMigrationSourceString(raw, "checksum"))
	specificity := adminMigrationSourceStringDefault(raw, "default", "specificity")
	if name == "" || path == "" || specificity == "" || !adminMigrationChecksumPattern.MatchString(checksum) {
		return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_file_invalid", Message: "source cookbook file record has invalid name, path, checksum, or specificity"}
	}
	if segment == "all_files" {
		name = adminMigrationNormalizeSourceCookbookAllFilesName(name)
	}
	return map[string]any{
		"name":        name,
		"path":        path,
		"checksum":    checksum,
		"specificity": specificity,
	}, nil
}

// adminMigrationSourceCookbookFilesFromChecksumList supports generated source
// summaries that know checksum references but did not retain file path details.
func adminMigrationSourceCookbookFilesFromChecksumList(object map[string]any) ([]map[string]any, error) {
	checksums, err := adminMigrationSourceStringSlice(object, "checksums")
	if err != nil {
		return nil, err
	}
	files := make([]map[string]any, 0, len(checksums))
	for _, checksum := range checksums {
		checksum = strings.ToLower(checksum)
		if !adminMigrationChecksumPattern.MatchString(checksum) {
			return nil, adminMigrationSourceSemanticError{Code: "source_checksum_invalid", Message: "source checksum must be a 32-character hexadecimal identifier"}
		}
		files = append(files, map[string]any{
			"name":        checksum,
			"path":        checksum,
			"checksum":    checksum,
			"specificity": "default",
		})
	}
	return files, nil
}

// adminMigrationEnsureSourceCookbookChecksumReferences merges cookbook blob
// references into checksum_references so later import stages see one graph.
func adminMigrationEnsureSourceCookbookChecksumReferences(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, checksums map[string]bool) error {
	if len(checksums) == 0 {
		return nil
	}
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "checksum_references"}
	seen := map[string]bool{}
	for _, raw := range payloadValues[key] {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		checksum := strings.ToLower(adminMigrationSourceString(object, "checksum", "id"))
		family := adminMigrationSourceString(object, "family", "source_family", "type")
		if checksum != "" && family != "" {
			seen[family+"/"+checksum] = true
		}
	}
	for checksum := range checksums {
		if !seen["cookbook/"+checksum] {
			payloadValues[key] = append(payloadValues[key], adminMigrationMarshalSourceObject(map[string]any{"checksum": checksum, "family": "cookbook"}))
		}
	}
	return adminMigrationNormalizeSourceChecksumReferences(payloadValues, orgName, map[string]bool{})
}

// adminMigrationReferencedSourceChecksums collects checksum references from the
// normalized source graph for copied-blob completeness warnings.
func adminMigrationReferencedSourceChecksums(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) map[string]bool {
	checksums := map[string]bool{}
	for key, values := range payloadValues {
		if key.Family != "checksum_references" {
			continue
		}
		for _, raw := range values {
			object, err := adminMigrationDecodeSourceObject(raw)
			if err != nil {
				continue
			}
			checksum := strings.ToLower(adminMigrationSourceString(object, "checksum", "id"))
			if adminMigrationChecksumPattern.MatchString(checksum) {
				checksums[checksum] = true
			}
		}
	}
	return checksums
}

// adminMigrationNormalizeSourceUsers converts user records to the canonical
// import payload shape and rejects duplicate or invalid usernames.
func adminMigrationNormalizeSourceUsers(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Family: "users"}
	values := payloadValues[key]
	users := map[string]bool{}
	if len(values) == 0 {
		return users, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		username := adminMigrationSourceString(object, "username", "name")
		if err := adminMigrationValidateSourceName(username, "source_user_invalid"); err != nil {
			return nil, err
		}
		if users[username] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_user", Message: "source user records contain a duplicate username"}
		}
		users[username] = true
		canonical := map[string]any{"username": username}
		adminMigrationCopySourceString(canonical, object, "display_name", "display_name")
		adminMigrationCopySourceString(canonical, object, "email", "email")
		adminMigrationCopySourceString(canonical, object, "first_name", "first_name")
		adminMigrationCopySourceString(canonical, object, "last_name", "last_name")
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "username")
	return users, nil
}

// adminMigrationNormalizeSourceOrganizations canonicalizes organization records
// and moves each one to the frozen per-organization payload path.
func adminMigrationNormalizeSourceOrganizations(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) (map[string]bool, error) {
	var values []json.RawMessage
	for key, current := range payloadValues {
		if key.Family == "organizations" {
			values = append(values, current...)
			delete(payloadValues, key)
		}
	}
	orgs := map[string]bool{}
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "orgname")
		if err := adminMigrationValidateSourceName(name, "source_organization_invalid"); err != nil {
			return nil, err
		}
		if orgs[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_organization", Message: "source organization records contain a duplicate name"}
		}
		orgs[name] = true
		canonical := map[string]any{
			"name":      name,
			"full_name": adminMigrationSourceStringDefault(object, name, "full_name", "display_name"),
		}
		adminMigrationCopySourceString(canonical, object, "org_type", "org_type")
		adminMigrationCopySourceString(canonical, object, "guid", "guid")
		payloadValues[adminMigrationSourcePayloadKey{Organization: name, Family: "organizations"}] = []json.RawMessage{adminMigrationMarshalSourceObject(canonical)}
	}
	return orgs, nil
}

// adminMigrationNormalizeSourceUserKeys preserves user key IDs, public key PEMs,
// and expiration metadata while rejecting non-RSA or duplicate keys.
func adminMigrationNormalizeSourceUserKeys(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, users map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Family: "user_keys"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		username := adminMigrationSourceString(object, "username", "user", "name")
		if !users[username] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_user_key", Message: "source user key references a missing user"}
		}
		keyName := adminMigrationSourceStringDefault(object, "default", "key_name", "name")
		if err := adminMigrationValidateSourceName(keyName, "source_key_invalid"); err != nil {
			return err
		}
		identity := username + "/" + keyName
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_user_key", Message: "source user key records contain a duplicate key name"}
		}
		seen[identity] = true
		publicKey := adminMigrationSourceString(object, "public_key", "public_key_pem")
		if err := adminMigrationValidateSourcePublicKey(publicKey); err != nil {
			return err
		}
		objects = append(objects, map[string]any{
			"username":        username,
			"key_name":        keyName,
			"public_key":      publicKey,
			"expiration_date": adminMigrationSourceStringDefault(object, "infinity", "expiration_date", "expires_at"),
		})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "username", "key_name")
	return nil
}

// adminMigrationNormalizeSourceUserACLs validates user ACL resources and
// canonicalizes permission arrays without requiring source defaults to match OpenCook.
func adminMigrationNormalizeSourceUserACLs(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, users map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Family: "user_acls"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "user")
		if err != nil {
			return err
		}
		if resourceType != "user" || !users[resourceName] {
			return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source user ACL references a missing user"}
		}
		resource := resourceType + ":" + resourceName
		if seen[resource] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_acl", Message: "source ACL records contain a duplicate resource"}
		}
		seen[resource] = true
		canonical, err := adminMigrationNormalizeSourceACLObject(object, resource)
		if err != nil {
			return err
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "resource")
	return nil
}

// adminMigrationNormalizeSourceServerAdminMemberships validates global admin
// membership rows against existing users and keeps the actor type explicit.
func adminMigrationNormalizeSourceServerAdminMemberships(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, users map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		actorType := adminMigrationSourceStringDefault(object, "user", "type", "actor_type")
		actor := adminMigrationSourceString(object, "actor", "username", "name")
		if actorType != "user" || !users[actor] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_server_admin", Message: "source server admin membership references a missing user"}
		}
		identity := actorType + ":" + actor
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_server_admin", Message: "source server admin memberships contain a duplicate actor"}
		}
		seen[identity] = true
		objects = append(objects, map[string]any{"actor": actor, "type": actorType})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "type", "actor")
	return nil
}

// adminMigrationNormalizeSourceClients canonicalizes client rows for one org
// and validates validator/admin flags plus optional default public keys.
func adminMigrationNormalizeSourceClients(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "clients"}
	values := payloadValues[key]
	clients := map[string]bool{}
	if len(values) == 0 {
		return clients, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "clientname", "client")
		if err := adminMigrationValidateSourceName(name, "source_client_invalid"); err != nil {
			return nil, err
		}
		if clients[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_client", Message: "source client records contain a duplicate name"}
		}
		clients[name] = true
		canonical := map[string]any{
			"name":       name,
			"clientname": name,
			"orgname":    orgName,
			"validator":  adminMigrationSourceBool(object, "validator"),
			"admin":      adminMigrationSourceBool(object, "admin"),
		}
		if publicKey := adminMigrationSourceString(object, "public_key", "public_key_pem"); publicKey != "" {
			if err := adminMigrationValidateSourcePublicKey(publicKey); err != nil {
				return nil, err
			}
			canonical["public_key"] = publicKey
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return clients, nil
}

// adminMigrationNormalizeSourceClientKeys preserves client key IDs and key
// expiration metadata while ensuring every key belongs to a known client.
func adminMigrationNormalizeSourceClientKeys(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, clients map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "client_keys"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		clientName := adminMigrationSourceString(object, "client", "clientname")
		if !clients[clientName] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_client_key", Message: "source client key references a missing client"}
		}
		keyName := adminMigrationSourceStringDefault(object, "default", "key_name", "name")
		if err := adminMigrationValidateSourceName(keyName, "source_key_invalid"); err != nil {
			return err
		}
		identity := clientName + "/" + keyName
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_client_key", Message: "source client key records contain a duplicate key name"}
		}
		seen[identity] = true
		publicKey := adminMigrationSourceString(object, "public_key", "public_key_pem")
		if err := adminMigrationValidateSourcePublicKey(publicKey); err != nil {
			return err
		}
		objects = append(objects, map[string]any{
			"client":          clientName,
			"key_name":        keyName,
			"public_key":      publicKey,
			"expiration_date": adminMigrationSourceStringDefault(object, "infinity", "expiration_date", "expires_at"),
		})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "client", "key_name")
	return nil
}

// adminMigrationNormalizeSourceGroups normalizes group membership arrays and
// rejects actors that cannot be resolved within the source users or org clients.
func adminMigrationNormalizeSourceGroups(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, users, clients map[string]bool) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "groups"}
	values := payloadValues[key]
	groups := map[string]bool{}
	if len(values) == 0 {
		return groups, nil
	}
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "groupname", "group")
		if err := adminMigrationValidateSourceName(name, "source_group_invalid"); err != nil {
			return nil, err
		}
		if groups[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_group", Message: "source group records contain a duplicate name"}
		}
		groups[name] = true
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, _ := adminMigrationDecodeSourceObject(raw)
		name := adminMigrationSourceString(object, "name", "groupname", "group")
		groupUsers, groupClients, groupGroups, err := adminMigrationNormalizeSourceGroupMembers(object, users, clients, groups)
		if err != nil {
			return nil, err
		}
		objects = append(objects, map[string]any{
			"name":      name,
			"groupname": name,
			"orgname":   orgName,
			"actors":    adminMigrationUniqueSortedStrings(append(append([]string{}, groupUsers...), groupClients...)),
			"users":     groupUsers,
			"clients":   groupClients,
			"groups":    groupGroups,
		})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return groups, nil
}

// adminMigrationNormalizeSourceGroupMemberships validates relation rows for
// group membership tables without expanding or inventing additional actors.
func adminMigrationNormalizeSourceGroupMemberships(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, users, clients, groups map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "group_memberships"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		groupName := adminMigrationSourceString(object, "group", "groupname")
		if !groups[groupName] {
			return adminMigrationSourceSemanticError{Code: "source_orphan_group_membership", Message: "source group membership references a missing group"}
		}
		actorType := adminMigrationSourceStringDefault(object, "user", "type", "actor_type")
		actor := adminMigrationSourceString(object, "actor", "name")
		if err := adminMigrationValidateSourceMembershipActor(actorType, actor, users, clients, groups); err != nil {
			return err
		}
		identity := groupName + "/" + actorType + "/" + actor
		if seen[identity] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_group_membership", Message: "source group memberships contain a duplicate actor"}
		}
		seen[identity] = true
		objects = append(objects, map[string]any{"group": groupName, "actor": actor, "type": actorType})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "group", "type", "actor")
	return nil
}

// adminMigrationNormalizeSourceContainers canonicalizes container names and
// container paths while preserving default container rows supplied by the source.
func adminMigrationNormalizeSourceContainers(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string) (map[string]bool, error) {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "containers"}
	values := payloadValues[key]
	containers := map[string]bool{}
	if len(values) == 0 {
		return containers, nil
	}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		name := adminMigrationSourceString(object, "name", "containername", "containerpath")
		if err := adminMigrationValidateSourceName(name, "source_container_invalid"); err != nil {
			return nil, err
		}
		if containers[name] {
			return nil, adminMigrationSourceSemanticError{Code: "source_duplicate_container", Message: "source container records contain a duplicate name"}
		}
		containers[name] = true
		objects = append(objects, map[string]any{
			"name":          name,
			"containername": name,
			"containerpath": adminMigrationSourceStringDefault(object, name, "containerpath"),
		})
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "name")
	return containers, nil
}

// adminMigrationNormalizeSourceOrgACLs validates ACL resources for organization,
// client, group, and container rows within one source organization.
func adminMigrationNormalizeSourceOrgACLs(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgName string, users, clients, groups, containers map[string]bool) error {
	key := adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}
	values := payloadValues[key]
	if len(values) == 0 {
		return nil
	}
	seen := map[string]bool{}
	objects := make([]map[string]any, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return err
		}
		resourceType, resourceName, err := adminMigrationSourceACLResource(object, "organization")
		if err != nil {
			return err
		}
		if err := adminMigrationValidateSourceOrgACLTarget(orgName, resourceType, resourceName, users, clients, groups, containers); err != nil {
			return err
		}
		resource := resourceType + ":" + resourceName
		if seen[resource] {
			return adminMigrationSourceSemanticError{Code: "source_duplicate_acl", Message: "source ACL records contain a duplicate resource"}
		}
		seen[resource] = true
		canonical, err := adminMigrationNormalizeSourceACLObject(object, resource)
		if err != nil {
			return err
		}
		objects = append(objects, canonical)
	}
	payloadValues[key] = adminMigrationMarshalSortedSourceObjects(objects, "resource")
	return nil
}

// adminMigrationSourcePayloadKeyFromManifest restores the internal org scope
// needed to materialize per-org manifest payloads into the frozen layout.
func adminMigrationSourcePayloadKeyFromManifest(payload adminMigrationSourceManifestPayload, values []json.RawMessage) (adminMigrationSourcePayloadKey, error) {
	family := strings.TrimSpace(payload.Family)
	if family == "" {
		return adminMigrationSourcePayloadKey{}, adminMigrationSourceSemanticError{Code: "source_manifest_payload_invalid", Message: "source manifest payload is missing a family"}
	}
	orgName := strings.TrimSpace(payload.Organization)
	if family == "organizations" && orgName == "" {
		if parsed := adminMigrationOrgNameFromSourcePayloadPath(payload.Path); parsed != "" {
			orgName = parsed
		} else if len(values) > 0 {
			object, err := adminMigrationDecodeSourceObject(values[0])
			if err == nil {
				orgName = adminMigrationSourceString(object, "name", "orgname")
			}
		}
	}
	return adminMigrationSourcePayloadKey{Organization: orgName, Family: family}, nil
}

// adminMigrationOrgNameFromSourcePayloadPath extracts the org segment from the
// normalized organization payload path when manifests omit the optional field.
func adminMigrationOrgNameFromSourcePayloadPath(path string) string {
	parts := adminMigrationSourcePathParts(path)
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "organizations" && parts[i+2] == "organization.json" {
			return parts[i+1]
		}
	}
	return ""
}

// adminMigrationNormalizeSourceGroupMembers expands users, clients, nested
// groups, and legacy actors fields into explicit typed membership arrays.
func adminMigrationNormalizeSourceGroupMembers(object map[string]any, users, clients, groups map[string]bool) ([]string, []string, []string, error) {
	groupUsers, err := adminMigrationSourceStringSlice(object, "users")
	if err != nil {
		return nil, nil, nil, err
	}
	groupClients, err := adminMigrationSourceStringSlice(object, "clients")
	if err != nil {
		return nil, nil, nil, err
	}
	groupGroups, err := adminMigrationSourceStringSlice(object, "groups")
	if err != nil {
		return nil, nil, nil, err
	}
	actors, err := adminMigrationSourceStringSlice(object, "actors")
	if err != nil {
		return nil, nil, nil, err
	}
	for _, actor := range actors {
		switch {
		case users[actor]:
			groupUsers = append(groupUsers, actor)
		case clients[actor]:
			groupClients = append(groupClients, actor)
		case groups[actor]:
			groupGroups = append(groupGroups, actor)
		default:
			return nil, nil, nil, adminMigrationSourceSemanticError{Code: "source_orphan_group_member", Message: "source group references an unknown actor"}
		}
	}
	groupUsers = adminMigrationUniqueSortedStrings(groupUsers)
	groupClients = adminMigrationUniqueSortedStrings(groupClients)
	groupGroups = adminMigrationUniqueSortedStrings(groupGroups)
	for _, actor := range groupUsers {
		if !users[actor] {
			return nil, nil, nil, adminMigrationSourceSemanticError{Code: "source_orphan_group_member", Message: "source group references a missing user"}
		}
	}
	for _, actor := range groupClients {
		if !clients[actor] {
			return nil, nil, nil, adminMigrationSourceSemanticError{Code: "source_orphan_group_member", Message: "source group references a missing client"}
		}
	}
	for _, actor := range groupGroups {
		if !groups[actor] {
			return nil, nil, nil, adminMigrationSourceSemanticError{Code: "source_orphan_group_member", Message: "source group references a missing nested group"}
		}
	}
	return groupUsers, groupClients, groupGroups, nil
}

// adminMigrationValidateSourceMembershipActor checks typed membership rows
// against the actors already normalized from the source.
func adminMigrationValidateSourceMembershipActor(actorType, actor string, users, clients, groups map[string]bool) error {
	switch actorType {
	case "user":
		if users[actor] {
			return nil
		}
	case "client":
		if clients[actor] {
			return nil
		}
	case "group":
		if groups[actor] {
			return nil
		}
	default:
		return adminMigrationSourceSemanticError{Code: "source_group_membership_invalid", Message: "source group membership has an unsupported actor type"}
	}
	return adminMigrationSourceSemanticError{Code: "source_orphan_group_membership", Message: "source group membership references a missing actor"}
}

// adminMigrationSourceACLResource resolves resource identifiers from either a
// Chef-style "type:name" string or explicit type/name fields.
func adminMigrationSourceACLResource(object map[string]any, fallbackType string) (string, string, error) {
	resource := adminMigrationSourceString(object, "resource")
	if resource != "" {
		parts := strings.SplitN(resource, ":", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return "", "", adminMigrationSourceSemanticError{Code: "source_acl_invalid", Message: "source ACL resource must use type:name format"}
		}
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), nil
	}
	resourceType := adminMigrationSourceStringDefault(object, fallbackType, "resource_type", "type")
	resourceName := adminMigrationSourceString(object, "name", "resource_name", "username")
	if resourceType == "" || resourceName == "" {
		return "", "", adminMigrationSourceSemanticError{Code: "source_acl_invalid", Message: "source ACL resource is missing a type or name"}
	}
	return resourceType, resourceName, nil
}

// adminMigrationNormalizeSourceACLObject canonicalizes the five Chef ACL
// permissions and rejects malformed actors/groups arrays.
func adminMigrationNormalizeSourceACLObject(object map[string]any, resource string) (map[string]any, error) {
	canonical := map[string]any{"resource": resource}
	for _, action := range []string{"create", "read", "update", "delete", "grant"} {
		permission, err := adminMigrationNormalizeSourceACLPermission(object[action])
		if err != nil {
			return nil, err
		}
		canonical[action] = permission
	}
	return canonical, nil
}

// adminMigrationNormalizeSourceACLPermission accepts omitted permissions as
// empty ACL entries but requires present permissions to be object-shaped.
func adminMigrationNormalizeSourceACLPermission(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{"actors": []string{}, "groups": []string{}}, nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, adminMigrationSourceSemanticError{Code: "source_acl_invalid", Message: "source ACL permission must be an object"}
	}
	actors, err := adminMigrationSourceStringSlice(object, "actors")
	if err != nil {
		return nil, adminMigrationSourceSemanticError{Code: "source_acl_invalid", Message: "source ACL actors must be an array of strings"}
	}
	groups, err := adminMigrationSourceStringSlice(object, "groups")
	if err != nil {
		return nil, adminMigrationSourceSemanticError{Code: "source_acl_invalid", Message: "source ACL groups must be an array of strings"}
	}
	return map[string]any{"actors": actors, "groups": groups}, nil
}

// adminMigrationValidateSourceOrgACLTarget ensures ACL rows point at identity
// resources present in the same normalized organization payload; core object
// ACL targets are accepted here and resolved in the later core-object pass.
func adminMigrationValidateSourceOrgACLTarget(orgName, resourceType, resourceName string, users, clients, groups, containers map[string]bool) error {
	switch resourceType {
	case "organization":
		if resourceName == orgName {
			return nil
		}
	case "client":
		if clients[resourceName] {
			return nil
		}
	case "group":
		if groups[resourceName] {
			return nil
		}
	case "container":
		if containers[resourceName] {
			return nil
		}
	case "user":
		if users[resourceName] {
			return nil
		}
	case "node", "environment", "role", "data_bag", "policy", "policy_group", "sandbox":
		return nil
	}
	return adminMigrationSourceSemanticError{Code: "source_acl_target_missing", Message: "source ACL references a missing resource"}
}

// adminMigrationDecodeSourceObject unmarshals a canonical raw message into the
// object form required by semantic source normalization.
func adminMigrationDecodeSourceObject(raw json.RawMessage) (map[string]any, error) {
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, adminMigrationSourceSemanticError{Code: "source_payload_invalid_json", Message: "source payload JSON object could not be decoded"}
	}
	if object == nil {
		return nil, adminMigrationSourceSemanticError{Code: "source_payload_invalid_json", Message: "source payload item must be an object"}
	}
	return object, nil
}

// adminMigrationSourceString returns the first non-empty string field from a
// source object, trimming whitespace but not otherwise rewriting Chef names.
func adminMigrationSourceString(object map[string]any, names ...string) string {
	for _, name := range names {
		if value, ok := object[name].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// adminMigrationSourceStringDefault returns a source string or a fallback when
// older generated fixtures omit optional Chef metadata.
func adminMigrationSourceStringDefault(object map[string]any, fallback string, names ...string) string {
	if value := adminMigrationSourceString(object, names...); value != "" {
		return value
	}
	return fallback
}

// adminMigrationCopySourceString copies optional string fields into canonical
// output only when the source supplied a non-empty value.
func adminMigrationCopySourceString(out, in map[string]any, target string, names ...string) {
	if value := adminMigrationSourceString(in, names...); value != "" {
		out[target] = value
	}
}

// adminMigrationCopySourceObject makes a shallow copy before canonical fields
// are overlaid, preserving compatibility-sensitive nested Chef payloads.
func adminMigrationCopySourceObject(object map[string]any) map[string]any {
	out := make(map[string]any, len(object))
	for key, value := range object {
		out[key] = value
	}
	return out
}

// adminMigrationSourceBool reads boolean source fields while treating absent
// values as false, matching Chef payload defaults for clients.
func adminMigrationSourceBool(object map[string]any, names ...string) bool {
	for _, name := range names {
		if value, ok := object[name].(bool); ok {
			return value
		}
	}
	return false
}

// adminMigrationSourceOptionalBool distinguishes an omitted boolean from a
// malformed one, which matters for Chef fields such as frozen?.
func adminMigrationSourceOptionalBool(object map[string]any, name string) (bool, bool, error) {
	value, ok := object[name]
	if !ok || value == nil {
		return false, false, nil
	}
	typed, ok := value.(bool)
	if !ok {
		return false, false, adminMigrationSourceSemanticError{Code: "source_payload_invalid_json", Message: "source field " + name + " must be a boolean"}
	}
	return typed, true, nil
}

// adminMigrationSourceStringSlice canonicalizes optional string arrays and
// rejects mixed-type arrays before they can reach import preflight.
func adminMigrationSourceStringSlice(object map[string]any, name string) ([]string, error) {
	value, ok := object[name]
	if !ok || value == nil {
		return []string{}, nil
	}
	items, ok := value.([]any)
	if !ok {
		return nil, adminMigrationSourceSemanticError{Code: "source_payload_invalid_json", Message: "source field " + name + " must be an array of strings"}
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return nil, adminMigrationSourceSemanticError{Code: "source_payload_invalid_json", Message: "source field " + name + " must contain only non-empty strings"}
		}
		out = append(out, strings.TrimSpace(text))
	}
	return adminMigrationUniqueSortedStrings(out), nil
}

// adminMigrationSourceOrderedStringSlice validates an optional string array
// without sorting it, which preserves Chef-observable run-list order.
func adminMigrationSourceOrderedStringSlice(object map[string]any, name, code string) ([]string, bool, error) {
	value, ok := object[name]
	if !ok || value == nil {
		return nil, false, nil
	}
	out, err := adminMigrationSourceOrderedStringSliceValue(value, code, "source field "+name)
	return out, true, err
}

// adminMigrationSourceOrderedStringSliceValue validates arbitrary array values
// for nested structures such as role env_run_lists.
func adminMigrationSourceOrderedStringSliceValue(value any, code, label string) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		return nil, adminMigrationSourceSemanticError{Code: code, Message: label + " must be an array of strings"}
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		text, ok := item.(string)
		if !ok || strings.TrimSpace(text) == "" {
			return nil, adminMigrationSourceSemanticError{Code: code, Message: label + " must contain only non-empty strings"}
		}
		out = append(out, strings.TrimSpace(text))
	}
	return out, nil
}

// adminMigrationNormalizeSourceRoleEnvRunLists validates environment-specific
// role run-lists while preserving both environment keys and item ordering.
func adminMigrationNormalizeSourceRoleEnvRunLists(object map[string]any) (map[string]any, bool, error) {
	value, ok := object["env_run_lists"]
	if !ok || value == nil {
		return nil, false, nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, false, adminMigrationSourceSemanticError{Code: "source_role_invalid", Message: "source role env_run_lists must be an object"}
	}
	out := map[string]any{}
	for envName, listValue := range raw {
		if err := adminMigrationValidateSourceName(envName, "source_role_invalid"); err != nil {
			return nil, false, err
		}
		runList, err := adminMigrationSourceOrderedStringSliceValue(listValue, "source_role_invalid", "source role env_run_lists."+envName)
		if err != nil {
			return nil, false, err
		}
		out[envName] = runList
	}
	return out, true, nil
}

// adminMigrationSourceDataBagItemPayload returns the item body in the canonical
// wrapper while keeping encrypted and plain Chef data bag JSON byte-equivalent in shape.
func adminMigrationSourceDataBagItemPayload(object map[string]any, id string) (map[string]any, error) {
	if value, ok := object["payload"]; ok && value != nil {
		payload, ok := value.(map[string]any)
		if !ok {
			return nil, adminMigrationSourceSemanticError{Code: "source_data_bag_item_invalid", Message: "source data bag item payload must be an object"}
		}
		canonical := adminMigrationCopySourceObject(payload)
		if payloadID := adminMigrationSourceString(canonical, "id"); payloadID != "" && payloadID != id {
			return nil, adminMigrationSourceSemanticError{Code: "source_data_bag_item_invalid", Message: "source data bag item payload id does not match item id"}
		}
		canonical["id"] = id
		return canonical, nil
	}
	canonical := adminMigrationCopySourceObject(object)
	delete(canonical, "bag")
	delete(canonical, "data_bag")
	canonical["id"] = id
	return canonical, nil
}

// adminMigrationCookbookLegacySegments mirrors Chef's legacy cookbook file
// collection keys without importing internal bootstrap package variables.
func adminMigrationCookbookLegacySegments() []string {
	return []string{"recipes", "definitions", "libraries", "attributes", "files", "templates", "resources", "providers", "root_files"}
}

// adminMigrationNormalizeSourceCookbookAllFilesName matches OpenCook's stored
// all_files name canonicalization while preserving the separate path field.
func adminMigrationNormalizeSourceCookbookAllFilesName(name string) string {
	name = strings.TrimSpace(name)
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		return name[idx+1:]
	}
	return name
}

// adminMigrationSourceCookbookFileChecksums extracts valid checksum keys from
// normalized source file maps for reference and blob completeness checks.
func adminMigrationSourceCookbookFileChecksums(files []map[string]any) []string {
	checksums := make([]string, 0, len(files))
	for _, file := range files {
		checksum := strings.ToLower(adminMigrationSourceString(file, "checksum"))
		if checksum != "" {
			checksums = append(checksums, checksum)
		}
	}
	return checksums
}

// adminMigrationSortSourceCookbookFiles gives normalized cookbook file arrays a
// deterministic order independent of the source export traversal order.
func adminMigrationSortSourceCookbookFiles(files []map[string]any) {
	sort.Slice(files, func(i, j int) bool {
		for _, field := range []string{"path", "name", "checksum", "specificity"} {
			left := fmt.Sprint(files[i][field])
			right := fmt.Sprint(files[j][field])
			if left != right {
				return left < right
			}
		}
		return false
	})
}

// adminMigrationValidateSourceName mirrors the conservative Chef/OpenCook name
// character set for identity and authorization source records.
func adminMigrationValidateSourceName(name, code string) error {
	if strings.TrimSpace(name) == "" || !adminMigrationChefNamePattern.MatchString(name) {
		return adminMigrationSourceSemanticError{Code: code, Message: "source record contains an invalid or empty name"}
	}
	return nil
}

// adminMigrationValidateSourcePublicKey rejects missing, non-PEM, non-RSA, or
// otherwise unsupported public keys without generating replacement key material.
func adminMigrationValidateSourcePublicKey(publicKey string) error {
	if strings.TrimSpace(publicKey) == "" {
		return adminMigrationSourceSemanticError{Code: "source_key_public_key_missing", Message: "source key record is missing public_key"}
	}
	if _, err := authn.ParseRSAPublicKeyPEM([]byte(publicKey)); err != nil {
		return adminMigrationSourceSemanticError{Code: "source_key_invalid", Message: "source key public_key must contain an RSA public key PEM"}
	}
	return nil
}

// adminMigrationMarshalSortedSourceObjects returns canonical raw messages sorted
// by stable source fields, which keeps generated payload hashes reproducible.
func adminMigrationMarshalSortedSourceObjects(objects []map[string]any, sortFields ...string) []json.RawMessage {
	sort.Slice(objects, func(i, j int) bool {
		for _, field := range sortFields {
			left := fmt.Sprint(objects[i][field])
			right := fmt.Sprint(objects[j][field])
			if left != right {
				return left < right
			}
		}
		return false
	})
	out := make([]json.RawMessage, 0, len(objects))
	for _, object := range objects {
		out = append(out, adminMigrationMarshalSourceObject(object))
	}
	return out
}

// adminMigrationMarshalSourceObject converts a validated source object back
// into raw JSON for deterministic payload emission.
func adminMigrationMarshalSourceObject(object map[string]any) json.RawMessage {
	data, _ := json.Marshal(object)
	return data
}

// adminMigrationUniqueSortedStrings deduplicates actor lists so normalized
// group and ACL payloads remain stable across source ordering differences.
func adminMigrationUniqueSortedStrings(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// adminMigrationSemanticFinding maps semantic normalizer errors to the shared
// migration finding envelope without exposing source file paths or record bytes.
func adminMigrationSemanticFinding(err error) adminMigrationFinding {
	var semantic adminMigrationSourceSemanticError
	if errors.As(err, &semantic) {
		return adminMigrationSourceErrorFinding(semantic.Code, semantic.Message)
	}
	return adminMigrationSourceErrorFinding("source_payload_invalid_json", "source payload could not be semantically normalized")
}

// adminMigrationSourceNormalizeTargetForEntry maps a source path to the
// normalized payload family and synthesizes folder-only records where needed.
func adminMigrationSourceNormalizeTargetForEntry(parts []string, isDir bool) (string, string, json.RawMessage, bool) {
	orgName := ""
	index := 0
	if len(parts) >= 2 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
		orgName = parts[1]
		index = 2
	}
	if orgName != "" && len(parts) == index+1 && adminMigrationSourceLooksLikeJSONFile(parts[index]) {
		base := strings.TrimSuffix(parts[index], ".json")
		if base == "organization" {
			return orgName, "organizations", nil, true
		}
	}
	if isDir {
		if index < len(parts) && adminMigrationSourceFamilyForSegment(parts[index]) == "data_bag_items" && len(parts) > index+1 {
			return orgName, "data_bags", adminMigrationSyntheticSourceObject("name", parts[index+1]), true
		}
		return "", "", nil, false
	}
	org, family, ok := adminMigrationSourceFamilyForEntry(parts, false)
	return org, family, nil, ok
}

// adminMigrationAttachSourceDataBagName restores the bag name carried by Chef
// export paths when item files themselves only contain the item JSON body.
func adminMigrationAttachSourceDataBagName(parts []string, values []json.RawMessage) ([]json.RawMessage, error) {
	bagName := adminMigrationSourceDataBagNameFromEntry(parts)
	if bagName == "" {
		return values, nil
	}
	out := make([]json.RawMessage, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		if existing := adminMigrationSourceString(object, "bag", "data_bag"); existing != "" && existing != bagName {
			return nil, adminMigrationSourceSemanticError{Code: "source_data_bag_item_invalid", Message: "source data bag item path and payload bag do not match"}
		}
		object["bag"] = bagName
		out = append(out, adminMigrationMarshalSourceObject(object))
	}
	return out, nil
}

// adminMigrationSourceDataBagNameFromEntry extracts the data bag directory name
// from org-scoped data_bags/<bag>/<item>.json source paths.
func adminMigrationSourceDataBagNameFromEntry(parts []string) string {
	index := 0
	if len(parts) >= 3 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
		index = 2
	}
	if len(parts) > index+2 && adminMigrationSourceFamilyForSegment(parts[index]) == "data_bag_items" {
		return parts[index+1]
	}
	return ""
}

// adminMigrationAttachSourceCookbookVersionRoute carries route-derived cookbook
// names and versions into semantic normalization without persisting helper keys.
func adminMigrationAttachSourceCookbookVersionRoute(parts []string, values []json.RawMessage) ([]json.RawMessage, error) {
	cookbookName, version := adminMigrationSourceCookbookVersionRouteFromEntry(parts)
	if cookbookName == "" || version == "" {
		return values, nil
	}
	out := make([]json.RawMessage, 0, len(values))
	for _, raw := range values {
		object, err := adminMigrationDecodeSourceObject(raw)
		if err != nil {
			return nil, err
		}
		if existing := adminMigrationSourceString(object, "cookbook_name"); existing != "" && existing != cookbookName {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook path and payload cookbook_name do not match"}
		}
		if existing := adminMigrationSourceString(object, "version"); existing != "" && existing != version {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook path and payload version do not match"}
		}
		if existing := adminMigrationSourceString(object, "name"); existing != "" && existing != cookbookName+"-"+version {
			return nil, adminMigrationSourceSemanticError{Code: "source_cookbook_route_mismatch", Message: "source cookbook path and payload name do not match"}
		}
		object["_source_cookbook_name"] = cookbookName
		object["_source_cookbook_version"] = version
		if object["cookbook_name"] == nil {
			object["cookbook_name"] = cookbookName
		}
		if object["version"] == nil {
			object["version"] = version
		}
		if object["name"] == nil {
			object["name"] = cookbookName + "-" + version
		}
		out = append(out, adminMigrationMarshalSourceObject(object))
	}
	return out, nil
}

// adminMigrationSourceCookbookVersionRouteFromEntry recognizes generated source
// paths such as cookbooks/apache2-1.2.3/metadata.json.
func adminMigrationSourceCookbookVersionRouteFromEntry(parts []string) (string, string) {
	index := 0
	if len(parts) >= 3 && adminMigrationSourceIsOrganizationsSegment(parts[0]) {
		index = 2
	}
	if len(parts) <= index+1 || adminMigrationSourceFamilyForSegment(parts[index]) != "cookbook_versions" {
		return "", ""
	}
	candidate := strings.TrimSuffix(parts[index+1], ".json")
	if len(parts) > index+2 {
		candidate = parts[index+1]
	}
	return adminMigrationSplitSourceCookbookVersionSegment(candidate)
}

// adminMigrationSplitSourceCookbookVersionSegment splits the final x.y.z suffix
// from cookbook directories while allowing hyphens in cookbook names.
func adminMigrationSplitSourceCookbookVersionSegment(segment string) (string, string) {
	for i := strings.LastIndex(segment, "-"); i > 0; i = strings.LastIndex(segment[:i], "-") {
		name := segment[:i]
		version := segment[i+1:]
		if bootstrap.ValidCookbookRouteVersion(version) && adminMigrationChefNamePattern.MatchString(name) {
			return name, version
		}
	}
	return "", ""
}

// adminMigrationSyntheticSourceObject builds tiny records for source concepts
// represented by directory names rather than standalone JSON files.
func adminMigrationSyntheticSourceObject(key, value string) json.RawMessage {
	data, _ := json.Marshal(map[string]string{key: value})
	return data
}

// adminMigrationSourcePayloadPath returns the frozen normalized payload layout
// for global, organization, and org-scoped source families.
func adminMigrationSourcePayloadPath(orgName, family string) string {
	if family == "organizations" {
		return pathpkg.Join("payloads", "organizations", orgName, "organization.json")
	}
	if strings.TrimSpace(orgName) == "" {
		return pathpkg.Join("payloads", "bootstrap", family+".json")
	}
	return pathpkg.Join("payloads", "organizations", orgName, family+".json")
}

// adminMigrationSourceArtifactsFromSideChannels records copied blobs, derived
// search files, and unsupported ancillary material in the manifest.
func adminMigrationSourceArtifactsFromSideChannels(blobChecksums map[string]struct{}, searchCount int, unsupportedCounts map[string]int) []adminMigrationSourceManifestArtifact {
	var artifacts []adminMigrationSourceManifestArtifact
	if len(blobChecksums) > 0 {
		artifacts = append(artifacts, adminMigrationSourceManifestArtifact{Family: "bookshelf", Path: "blobs/checksums", Count: len(blobChecksums), Supported: adminMigrationBoolPtr(true)})
	}
	if searchCount > 0 {
		artifacts = append(artifacts, adminMigrationSourceManifestArtifact{Family: "opensearch", Path: "derived/opensearch", Count: searchCount, Deferred: true})
	}
	for _, family := range adminMigrationSortedMapKeys(unsupportedCounts) {
		artifacts = append(artifacts, adminMigrationSourceManifestArtifact{Family: family, Path: pathpkg.Join("unsupported", family), Count: unsupportedCounts[family], Supported: adminMigrationBoolPtr(false)})
	}
	return artifacts
}

// adminMigrationInventoryFromSourceManifest derives the command inventory from
// the same normalized payload and side-channel records written to disk.
func adminMigrationInventoryFromSourceManifest(manifest adminMigrationSourceManifest) adminMigrationInventory {
	families := adminMigrationManifestPayloadFamilies(manifest.Payloads)
	families = append(families, adminMigrationManifestArtifactFamilies(manifest.Artifacts)...)
	return adminMigrationInventory{Families: adminMigrationSortedInventoryFamilies(families)}
}

// adminMigrationNormalizeFindings keeps parser warnings from inventory but
// drops the old inventory-only advisory once normalize has produced output.
func adminMigrationNormalizeFindings(findings []adminMigrationFinding) []adminMigrationFinding {
	var out []adminMigrationFinding
	for _, finding := range findings {
		if finding.Code == "source_import_not_implemented" {
			continue
		}
		out = append(out, finding)
	}
	return out
}

// adminMigrationValidateCopiedSourceBlobFiles verifies checksum-addressed blob
// payloads when normalize can see the local bytes.
func adminMigrationValidateCopiedSourceBlobFiles(files map[string][]byte) error {
	for path, data := range files {
		checksum := adminMigrationCopiedSourceBlobChecksum(path)
		if checksum == "" {
			continue
		}
		sum := md5.Sum(data)
		if hex.EncodeToString(sum[:]) != checksum {
			return adminMigrationSourceSemanticError{Code: "source_blob_checksum_mismatch", Message: "copied source blob bytes do not match their checksum path"}
		}
	}
	return nil
}

// adminMigrationCopiedSourceBlobChecksum extracts checksum keys from normalized
// copied-blob paths and ignores unrelated side-channel files.
func adminMigrationCopiedSourceBlobChecksum(path string) string {
	parts := adminMigrationSourcePathParts(path)
	if len(parts) == 3 && parts[0] == "blobs" && parts[1] == "checksums" {
		checksum := strings.ToLower(parts[2])
		if adminMigrationChecksumPattern.MatchString(checksum) {
			return checksum
		}
	}
	return ""
}

// adminMigrationMissingCopiedSourceBlobFindings warns when metadata references
// blobs whose bytes were not packaged into the normalized source artifact.
func adminMigrationMissingCopiedSourceBlobFindings(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, files map[string][]byte) []adminMigrationFinding {
	referenced := adminMigrationReferencedSourceChecksums(payloadValues)
	if len(referenced) == 0 {
		return nil
	}
	copied := map[string]bool{}
	for path := range files {
		if checksum := adminMigrationCopiedSourceBlobChecksum(path); checksum != "" {
			copied[checksum] = true
		}
	}
	var findings []adminMigrationFinding
	for _, checksum := range adminMigrationSortedMapKeys(referenced) {
		if copied[checksum] {
			continue
		}
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_blob_payload_missing",
			Family:   "checksum_references",
			Message:  "source metadata references checksum " + checksum + " but the normalized bundle does not include copied blob bytes; import preflight must verify the target/source blob provider",
		})
	}
	return findings
}

// adminMigrationUnknownPayloadFamilyFindings reports manifest-declared payload
// families that this import contract does not yet understand.
func adminMigrationUnknownPayloadFamilyFindings(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) []adminMigrationFinding {
	unknown := map[string]struct{}{}
	for key := range payloadValues {
		if !adminMigrationKnownSourcePayloadFamily(key.Family) {
			unknown[key.Family] = struct{}{}
		}
	}
	var findings []adminMigrationFinding
	for _, family := range adminMigrationSortedMapKeys(unknown) {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_family_unsupported",
			Family:   family,
			Message:  "source payload family " + family + " is not imported by the current OpenCook source contract",
		})
	}
	return findings
}

// adminMigrationUnsupportedCookbookSourceLayoutFindings flags cookbook source
// trees that contain raw files instead of metadata JSON or blob-store objects.
func adminMigrationUnsupportedCookbookSourceLayoutFindings(entries []adminMigrationSourceArtifactFileEntry) []adminMigrationFinding {
	seen := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir || adminMigrationSourceLooksLikeJSONFile(entry.Path) {
			continue
		}
		parts := adminMigrationSourcePathParts(entry.Path)
		if len(parts) < 4 || !adminMigrationSourceIsOrganizationsSegment(parts[0]) {
			continue
		}
		family := adminMigrationSourceFamilyForSegment(parts[2])
		if family == "cookbook_versions" || family == "cookbook_artifacts" {
			seen[family] = struct{}{}
		}
	}
	var findings []adminMigrationFinding
	for _, family := range adminMigrationSortedMapKeys(seen) {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_cookbook_layout_unsupported",
			Family:   family,
			Message:  "raw cookbook source files were detected; this bucket imports cookbook metadata and checksum-addressed blob payloads, not unpacked cookbook source trees",
		})
	}
	return findings
}

// adminMigrationUnknownSourceEntryFamilyFindings makes skipped org-scoped JSON
// families visible to operators instead of silently omitting them.
func adminMigrationUnknownSourceEntryFamilyFindings(entries []adminMigrationSourceArtifactFileEntry) []adminMigrationFinding {
	unknown := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir || !adminMigrationSourceLooksLikeJSONFile(entry.Path) {
			continue
		}
		parts := adminMigrationSourcePathParts(entry.Path)
		if len(parts) < 4 || !adminMigrationSourceIsOrganizationsSegment(parts[0]) {
			continue
		}
		segment := parts[2]
		if adminMigrationSourceFamilyForSegment(segment) == "" && adminMigrationUnsupportedSourceFamily(segment) == "" {
			unknown[segment] = struct{}{}
		}
	}
	var findings []adminMigrationFinding
	for _, family := range adminMigrationSortedMapKeys(unknown) {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_family_unsupported",
			Family:   family,
			Message:  "source object family " + family + " was detected but is not imported by the current OpenCook source contract",
		})
	}
	return findings
}

// adminMigrationSourceDirectoryFileEntries scans an extracted source directory
// and reads file bytes only after rejecting symlinks.
func adminMigrationSourceDirectoryFileEntries(root string) ([]adminMigrationSourceArtifactFileEntry, error) {
	var entries []adminMigrationSourceArtifactFileEntry
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errAdminMigrationUnsafeSourcePath
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative, err = adminMigrationNormalizeSourceRelativePath(relative)
		if err != nil {
			return err
		}
		entry := adminMigrationSourceArtifactFileEntry{Path: relative, IsDir: d.IsDir()}
		if !d.IsDir() {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			entry.Data = data
		}
		entries = append(entries, entry)
		return nil
	})
	return entries, err
}

// adminMigrationSourceArchiveFileEntries scans tar streams without extracting
// them and rejects traversal paths before they can influence output paths.
func adminMigrationSourceArchiveFileEntries(path string) ([]adminMigrationSourceArtifactFileEntry, error) {
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

	var entries []adminMigrationSourceArtifactFileEntry
	tarReader := tar.NewReader(reader)
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		relativePath, err := adminMigrationNormalizeSourceRelativePath(header.Name)
		if err != nil {
			return nil, err
		}
		entry := adminMigrationSourceArtifactFileEntry{Path: relativePath, IsDir: header.FileInfo().IsDir()}
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA || header.Typeflag == 0 {
			data, err := io.ReadAll(tarReader)
			if err != nil {
				return nil, err
			}
			entry.Data = data
		} else if !entry.IsDir {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// adminMigrationNormalizeSourceRelativePath validates portable manifest and
// archive paths before they are used for reads or writes.
func adminMigrationNormalizeSourceRelativePath(value string) (string, error) {
	trimmed := strings.TrimSpace(filepath.ToSlash(value))
	if trimmed == "" || strings.HasPrefix(trimmed, "/") {
		return "", errAdminMigrationUnsafeSourcePath
	}
	cleaned := pathpkg.Clean(trimmed)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", errAdminMigrationUnsafeSourcePath
	}
	for _, part := range strings.Split(cleaned, "/") {
		if part == "" || part == "." || part == ".." {
			return "", errAdminMigrationUnsafeSourcePath
		}
	}
	return cleaned, nil
}

// adminMigrationEnsureNormalizeOutputAllowed enforces overwrite confirmation
// and prevents writing the normalized bundle inside the source tree.
func adminMigrationEnsureNormalizeOutputAllowed(sourcePath, outputPath string, overwrite bool) error {
	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "" || filepath.Clean(outputPath) == "." || filepath.Clean(outputPath) == string(os.PathSeparator) {
		return errAdminMigrationUnsafeSourcePath
	}
	sourceAbs, sourceErr := filepath.Abs(sourcePath)
	outputAbs, outputErr := filepath.Abs(outputPath)
	if sourceErr == nil && outputErr == nil {
		sourceClean := filepath.Clean(sourceAbs)
		outputClean := filepath.Clean(outputAbs)
		if sourceClean == outputClean {
			return errAdminMigrationNormalizeOutputOverlaps
		}
		if info, err := os.Stat(sourcePath); err == nil && info.IsDir() {
			if rel, err := filepath.Rel(sourceClean, outputClean); err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)) && rel != ".." {
				return errAdminMigrationNormalizeOutputOverlaps
			}
		}
	}
	if _, err := os.Stat(outputPath); err == nil && !overwrite {
		return errAdminMigrationNormalizeOutputExists
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// adminMigrationNormalizeOutputFinding maps local output failures to stable
// codes without embedding filesystem paths in operator output.
func adminMigrationNormalizeOutputFinding(err error) adminMigrationFinding {
	switch {
	case errors.Is(err, errAdminMigrationNormalizeOutputExists):
		return adminMigrationSourceErrorFinding("source_normalize_output_exists", "normalized source output already exists; pass --yes to replace it")
	case errors.Is(err, errAdminMigrationNormalizeOutputOverlaps):
		return adminMigrationSourceErrorFinding("source_normalize_output_overlaps_source", "normalized source output must not be written inside the source artifact")
	default:
		return adminMigrationSourceErrorFinding("source_normalize_output_unsafe", "normalized source output path is not safe to write")
	}
}

// adminMigrationNormalizeArchiveFinding distinguishes unsafe archive paths from
// generic unreadable archive failures without exposing raw header names.
func adminMigrationNormalizeArchiveFinding(err error) adminMigrationFinding {
	if errors.Is(err, errAdminMigrationUnsafeSourcePath) {
		return adminMigrationSourceErrorFinding("source_path_unsafe", "source archive contains an unsafe relative path")
	}
	return adminMigrationSourceErrorFinding("source_archive_unreadable", "source archive could not be read")
}

// adminMigrationRedactMigrationPath hides URL credentials and secret-looking
// local path components while leaving ordinary local paths useful in output.
func adminMigrationRedactMigrationPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.Contains(value, "://") {
		return adminMigrationRedact(value)
	}
	lower := strings.ToLower(value)
	for _, marker := range []string{"secret", "password", "token", "credential", "access_key", "secret_key", ".pem"} {
		if strings.Contains(lower, marker) {
			return "redacted_path"
		}
	}
	return value
}

// adminMigrationSHA256Hex returns the lowercase digest used by normalized
// source manifests for payload integrity metadata.
func adminMigrationSHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// adminMigrationBoolPtr builds manifest booleans without leaking mutable local
// variables across artifact records.
func adminMigrationBoolPtr(value bool) *bool {
	v := value
	return &v
}

// adminMigrationSortedSourceManifestPayloads gives manifest payload processing
// a deterministic order independent of JSON input order.
func adminMigrationSortedSourceManifestPayloads(payloads []adminMigrationSourceManifestPayload) []adminMigrationSourceManifestPayload {
	out := append([]adminMigrationSourceManifestPayload(nil), payloads...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Organization != out[j].Organization {
			return out[i].Organization < out[j].Organization
		}
		if out[i].Family != out[j].Family {
			return out[i].Family < out[j].Family
		}
		return out[i].Path < out[j].Path
	})
	return out
}

// adminMigrationSortedSourceManifestArtifacts gives side-channel artifact
// records a deterministic order in normalized manifests.
func adminMigrationSortedSourceManifestArtifacts(artifacts []adminMigrationSourceManifestArtifact) []adminMigrationSourceManifestArtifact {
	out := append([]adminMigrationSourceManifestArtifact(nil), artifacts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Family != out[j].Family {
			return out[i].Family < out[j].Family
		}
		return out[i].Path < out[j].Path
	})
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
	for _, name := range []string{adminMigrationSourceManifestPath, "source-manifest.json"} {
		path := filepath.Join(root, name)
		info, err := os.Stat(path)
		if err == nil && !info.IsDir() {
			return path, true
		}
	}
	return "", false
}

// adminMigrationReadSourceManifestFile decodes both the legacy advisory
// inventory manifest and the normalized Chef-source manifest introduced for
// source import planning.
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
	if !adminMigrationSourceManifestFormatSupported(formatVersion) {
		return adminMigrationSourceInventoryRead{FormatVersion: formatVersion, Findings: []adminMigrationFinding{adminMigrationSourceErrorFinding("source_manifest_unsupported_format", "source manifest format version is not supported")}}, fmt.Errorf("unsupported source manifest format %q", formatVersion)
	}

	read := adminMigrationSourceInventoryRead{
		SourceType:    manifest.SourceType,
		FormatVersion: formatVersion,
		Inventory:     adminMigrationInventory{Families: adminMigrationSortedInventoryFamilies(manifest.Families)},
		Findings:      adminMigrationManifestArtifactFindings(manifest.Artifacts),
	}
	read.Inventory.Families = append(read.Inventory.Families, adminMigrationManifestPayloadFamilies(manifest.Payloads)...)
	read.Inventory.Families = append(read.Inventory.Families, adminMigrationManifestArtifactFamilies(manifest.Artifacts)...)
	read.Inventory.Families = adminMigrationSortedInventoryFamilies(read.Inventory.Families)
	read.Findings = append(read.Findings, adminMigrationSourceInventoryOnlyFinding())
	return read, nil
}

// adminMigrationSourceManifestFormatSupported keeps the old inventory manifest
// readable while freezing the normalized Chef-source contract for this bucket.
func adminMigrationSourceManifestFormatSupported(formatVersion string) bool {
	switch strings.TrimSpace(formatVersion) {
	case adminMigrationSourceFormatV1, adminMigrationChefSourceFormatV1:
		return true
	default:
		return false
	}
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

// adminMigrationSourceFamilyForSegment recognizes common generated JSON,
// normalized source payload, and pedant-visible object directories used by
// existing Chef Server exports.
func adminMigrationSourceFamilyForSegment(segment string) string {
	switch strings.ToLower(strings.TrimSpace(segment)) {
	case "users":
		return "users"
	case "user_acls":
		return "user_acls"
	case "user_keys":
		return "user_keys"
	case "server_admin_memberships":
		return "server_admin_memberships"
	case "clients":
		return "clients"
	case "client_keys":
		return "client_keys"
	case "groups":
		return "groups"
	case "group_memberships":
		return "group_memberships"
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
	case "policy_assignments":
		return "policy_assignments"
	case "sandboxes":
		return "sandboxes"
	case "checksum_references":
		return "checksum_references"
	case "cookbooks":
		return "cookbook_versions"
	case "cookbook_artifacts":
		return "cookbook_artifacts"
	default:
		return ""
	}
}

// adminMigrationKnownSourcePayloadFamily enumerates the normalized manifest
// families this staged source contract recognizes, even when later tasks import
// some of them more deeply.
func adminMigrationKnownSourcePayloadFamily(family string) bool {
	switch strings.TrimSpace(family) {
	case "users",
		"user_acls",
		"user_keys",
		"server_admin_memberships",
		"organizations",
		"clients",
		"client_keys",
		"groups",
		"group_memberships",
		"containers",
		"acls",
		"nodes",
		"environments",
		"roles",
		"data_bags",
		"data_bag_items",
		"policy_revisions",
		"policy_groups",
		"policy_assignments",
		"sandboxes",
		"checksum_references",
		"cookbook_versions",
		"cookbook_artifacts":
		return true
	default:
		return false
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

// adminMigrationManifestPayloadFamilies aggregates normalized manifest payloads
// into inventory families before later tasks implement actual import.
func adminMigrationManifestPayloadFamilies(payloads []adminMigrationSourceManifestPayload) []adminMigrationInventoryFamily {
	counts := map[adminMigrationSourcePayloadKey]int{}
	for _, payload := range payloads {
		family := strings.TrimSpace(payload.Family)
		if family == "" || payload.Count <= 0 {
			continue
		}
		key := adminMigrationSourcePayloadKey{Organization: strings.TrimSpace(payload.Organization), Family: family}
		counts[key] += payload.Count
	}
	keys := make([]adminMigrationSourcePayloadKey, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Organization != keys[j].Organization {
			return keys[i].Organization < keys[j].Organization
		}
		return keys[i].Family < keys[j].Family
	})
	families := make([]adminMigrationInventoryFamily, 0, len(keys))
	for _, key := range keys {
		families = append(families, adminMigrationInventoryFamily{Organization: key.Organization, Family: key.Family, Count: counts[key]})
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

// runAdminMigrationShadow dispatches shadow-read comparison commands that stay
// read-only while comparing normalized source artifacts with a restored target.
func (c *command) runAdminMigrationShadow(ctx context.Context, args []string, inheritedJSON bool) int {
	if len(args) == 0 {
		return c.adminUsageError("admin migration shadow requires compare\n\n")
	}
	if len(args) == 1 && (args[0] == "help" || args[0] == "-h" || args[0] == "--help") {
		c.printAdminMigrationUsage(c.stdout)
		return exitOK
	}
	if args[0] != "compare" {
		return c.adminUsageError("unknown admin migration shadow command %q\n\n", args[0])
	}
	return c.runAdminMigrationShadowCompare(ctx, args[1:], inheritedJSON)
}

// runAdminMigrationShadowCompare compares read-only source-derived responses
// with a live restored target using compatibility normalizers, never writes.
func (c *command) runAdminMigrationShadowCompare(ctx context.Context, args []string, inheritedJSON bool) int {
	start := time.Now()
	fs := flag.NewFlagSet("opencook admin migration shadow compare", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	opts := bindAdminMigrationCommonFlags(fs, inheritedJSON)
	var sourcePath string
	fs.StringVar(&sourcePath, "source", "", "normalized Chef source manifest directory or file")
	fs.StringVar(&opts.serverURL, "target-server-url", "", "restored OpenCook target server URL")
	fs.StringVar(&opts.serverURL, "server-url", "", "alias for --target-server-url")
	fs.StringVar(&opts.manifestPath, "manifest", "", "optional restored target backup manifest path")
	fs.StringVar(&opts.requestorName, "requestor-name", "", "Chef requestor name used for signed shadow reads")
	fs.StringVar(&opts.requestorType, "requestor-type", "", "Chef requestor type used for signed shadow reads")
	fs.StringVar(&opts.privateKeyPath, "private-key", "", "path to the requestor private key PEM")
	fs.StringVar(&opts.defaultOrg, "default-org", "", "default organization for default-org compatibility checks")
	fs.StringVar(&opts.serverAPIVersion, "server-api-version", "", "X-Ops-Server-API-Version value for signed shadow reads")
	if err := fs.Parse(args); err != nil {
		return c.adminFlagError("admin migration shadow compare", err)
	}
	if fs.NArg() != 0 {
		return c.adminUsageError("admin migration shadow compare received unexpected arguments: %v\n\n", fs.Args())
	}
	if strings.TrimSpace(sourcePath) == "" {
		return c.adminUsageError("admin migration shadow compare requires --source PATH\n\n")
	}
	if strings.TrimSpace(opts.serverURL) == "" {
		return c.adminUsageError("admin migration shadow compare requires --target-server-url URL\n\n")
	}

	result := c.buildAdminMigrationShadowCompare(ctx, sourcePath, opts)
	return c.writeAdminMigrationResult(result, opts.withTiming, start, adminMigrationExitCode(result))
}

// buildAdminMigrationShadowCompare validates source integrity, constructs a
// read-only target client, and compares representative Chef-facing reads.
func (c *command) buildAdminMigrationShadowCompare(ctx context.Context, sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	out := adminMigrationShadowCompareOutput(sourcePath, opts)
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationSourceBundleDependency("error", "normalized source bundle failed integrity inspection", nil))
		for _, finding := range read.Bundle.Findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(read.Bundle.Findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationSourceErrorFinding("source_bundle_invalid", "normalized source bundle failed integrity inspection"))
		}
		return out
	}

	out.Inventory = read.Bundle.Inventory
	adminMigrationMarkDependency(&out, adminMigrationSourceBundleDependency("ok", "normalized source manifest, payload hashes, and copied blobs are valid", map[string]string{
		"format_version":    read.Bundle.FormatVersion,
		"source_type":       read.Bundle.SourceType,
		"payloads":          fmt.Sprintf("%d", len(read.Bundle.Manifest.Payloads)),
		"referenced_blobs":  fmt.Sprintf("%d", len(adminMigrationUniqueBlobReferences(read.ReferencedChecksums))),
		"copied_blobs":      fmt.Sprintf("%d", len(read.CopiedChecksums)),
		"sidecar_artifacts": fmt.Sprintf("%d", len(read.Bundle.Manifest.Artifacts)),
	}))
	for _, finding := range read.Bundle.Findings {
		if adminMigrationShadowSourceFindingBlocks(finding) {
			adminMigrationMarkFinding(&out, adminMigrationFinding{Severity: "error", Code: "shadow_source_unsupported_family", Family: finding.Family, Message: "shadow compare cannot prove unsupported source family " + finding.Family})
			continue
		}
		adminMigrationMarkFinding(&out, finding)
	}
	if !out.OK {
		return out
	}

	sourceState, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		adminMigrationMarkError(&out, "shadow_source_state_invalid", "normalized source payloads could not be converted into comparable OpenCook state", "shadow_read")
		return out
	}
	if strings.TrimSpace(opts.manifestPath) != "" {
		out = adminMigrationShadowInspectOptionalManifest(out, opts.manifestPath)
		if !out.OK {
			return out
		}
	}

	adminCfg := c.loadAdminMigrationRehearsalConfig(opts)
	out.Target.ServerURL = adminMigrationRedact(adminCfg.ServerURL)
	out.Config = adminMigrationRedactedAdminConfig(adminCfg)
	client, err := c.newAdmin(adminCfg)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "shadow_target",
			Status:     "error",
			Backend:    "http",
			Configured: strings.TrimSpace(adminCfg.ServerURL) != "",
			Message:    "restored target admin client could not be constructed",
		})
		return out
	}

	blobManifest := adminMigrationSourceReadBlobManifest(sourceState, read)
	checks, skipped := adminMigrationShadowComparableChecks(adminMigrationCutoverRehearsalChecks(sourceState.Bootstrap, sourceState.CoreObjects, sourceState.Cookbooks, blobManifest))
	shadow := adminMigrationRunShadowComparison(ctx, client, sourceState, checks, skipped)
	out.Inventory.Families = append(out.Inventory.Families, adminMigrationShadowInventoryFamilies(shadow)...)
	if shadow.Failed > 0 {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "shadow_read_compare",
			Status:     "error",
			Backend:    "http",
			Configured: true,
			Message:    "one or more read-only source-to-target shadow comparisons failed",
			Details:    adminMigrationShadowDependencyDetails(shadow),
		})
	} else {
		adminMigrationMarkDependency(&out, adminMigrationDependency{
			Name:       "shadow_read_compare",
			Status:     "ok",
			Backend:    "http",
			Configured: true,
			Message:    "read-only source-to-target shadow comparisons matched after compatibility normalization",
			Details:    adminMigrationShadowDependencyDetails(shadow),
		})
	}
	for _, finding := range shadow.Findings {
		adminMigrationMarkFinding(&out, finding)
	}
	out.PlannedMutations = adminMigrationShadowCompareRecommendations()
	out.Warnings = append(out.Warnings, "shadow compare issued only read-only GET requests and signed blob downloads; writes remain blocked until cutover")
	return out
}

// adminMigrationShadowCompareOutput initializes the shared JSON envelope for
// source-to-target comparison while redacting local source and target paths.
func adminMigrationShadowCompareOutput(sourcePath string, opts *adminMigrationFlagValues) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:      true,
		Command: "migration_shadow_compare",
		Target: adminMigrationTarget{
			SourcePath:   adminMigrationRedactMigrationPath(sourcePath),
			ManifestPath: adminMigrationRedactMigrationPath(opts.manifestPath),
			ServerURL:    adminMigrationRedact(opts.serverURL),
		},
		DryRun:           opts.dryRun,
		Offline:          opts.offline,
		Confirmed:        opts.yes,
		Dependencies:     []adminMigrationDependency{},
		Inventory:        adminMigrationInventory{Families: []adminMigrationInventoryFamily{}},
		Findings:         []adminMigrationFinding{},
		PlannedMutations: []adminMigrationPlannedMutation{},
	}
}

// adminMigrationShadowInspectOptionalManifest validates a restored-target
// manifest when provided, making shadow comparison composable with rehearsal.
func adminMigrationShadowInspectOptionalManifest(out adminMigrationCLIOutput, manifestPath string) adminMigrationCLIOutput {
	bundlePath, _ := adminMigrationResolveManifestBundlePath(manifestPath)
	manifest, findings, err := adminMigrationInspectBackupBundle(bundlePath)
	if err != nil {
		adminMigrationMarkDependency(&out, adminMigrationBackupBundleDependency("error", "optional backup manifest failed integrity inspection", nil))
		for _, finding := range findings {
			adminMigrationMarkFinding(&out, finding)
		}
		if len(findings) == 0 {
			adminMigrationMarkFinding(&out, adminMigrationBackupInspectFinding("backup_bundle_invalid", "optional backup manifest failed integrity inspection"))
		}
		return out
	}
	adminMigrationMarkDependency(&out, adminMigrationBackupBundleDependency("ok", "optional backup manifest and payload hashes are valid", map[string]string{
		"format_version": manifest.FormatVersion,
		"payloads":       fmt.Sprintf("%d", len(manifest.Payloads)),
	}))
	return out
}

// adminMigrationShadowSourceFindingBlocks promotes truly unsupported source
// families to blockers while keeping deferred derived artifacts advisory-only.
func adminMigrationShadowSourceFindingBlocks(finding adminMigrationFinding) bool {
	switch finding.Code {
	case "source_family_unsupported", "source_cookbook_layout_unsupported":
		return true
	default:
		return false
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
	fs.StringVar(&opts.sourcePath, "source", "", "normalized source manifest path used to validate sync freshness")
	fs.StringVar(&opts.importProgressPath, "source-import-progress", "", "source import progress metadata path")
	fs.StringVar(&opts.syncProgressPath, "source-sync-progress", "", "source sync progress metadata path")
	fs.StringVar(&opts.shadowResultPath, "shadow-result", "", "migration shadow compare JSON result path")
	fs.StringVar(&opts.searchResultPath, "search-check-result", "", "admin search check JSON result path")
	fs.StringVar(&opts.serverURL, "server-url", "", "restored OpenCook server URL")
	fs.StringVar(&opts.requestorName, "requestor-name", "", "Chef requestor name used for signed rehearsal requests")
	fs.StringVar(&opts.requestorType, "requestor-type", "", "Chef requestor type used for signed rehearsal requests")
	fs.StringVar(&opts.privateKeyPath, "private-key", "", "path to the requestor private key PEM")
	fs.StringVar(&opts.defaultOrg, "default-org", "", "default organization for default-org compatibility checks")
	fs.StringVar(&opts.serverAPIVersion, "server-api-version", "", "X-Ops-Server-API-Version value for signed rehearsal requests")
	fs.BoolVar(&opts.rollbackReady, "rollback-ready", false, "confirm the source Chef path remains available for rollback")
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
	adminMigrationApplyCutoverEvidenceGates(&out, opts)
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
	adminMigrationApplyCutoverLiveGates(&out, rehearsal, checks)
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
	adminMigrationAppendCutoverReadinessSummary(&out)
	return out
}

// adminMigrationCutoverRehearsalOutput initializes the shared JSON envelope for
// live cutover validation while keeping all future work read-only by default.
func adminMigrationCutoverRehearsalOutput(opts *adminMigrationFlagValues, manifestPath string) adminMigrationCLIOutput {
	return adminMigrationCLIOutput{
		OK:      true,
		Command: "migration_cutover_rehearse",
		Target: adminMigrationTarget{
			ManifestPath:       manifestPath,
			SourcePath:         adminMigrationRedactMigrationPath(opts.sourcePath),
			ImportProgressPath: adminMigrationRedactMigrationPath(opts.importProgressPath),
			SyncProgressPath:   adminMigrationRedactMigrationPath(opts.syncProgressPath),
			ShadowResultPath:   adminMigrationRedactMigrationPath(opts.shadowResultPath),
			SearchResultPath:   adminMigrationRedactMigrationPath(opts.searchResultPath),
			ServerURL:          adminMigrationRedact(opts.serverURL),
		},
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

// adminMigrationApplyCutoverEvidenceGates folds prior import, sync, search, and
// shadow-read outputs into cutover rehearsal without making old invocations fail.
func adminMigrationApplyCutoverEvidenceGates(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) {
	sourceRead, sourceLoaded := adminMigrationCutoverSourceBundleGate(out, opts)
	adminMigrationCutoverImportProgressGate(out, opts)
	adminMigrationCutoverSyncFreshnessGate(out, opts, sourceRead, sourceLoaded)
	adminMigrationCutoverSearchCleanlinessGate(out, opts)
	adminMigrationCutoverShadowEvidenceGate(out, opts)
	adminMigrationCutoverRollbackReadinessGate(out, opts)
}

// adminMigrationCutoverSourceBundleGate validates optional normalized-source
// evidence and returns the read graph needed for cursor freshness checks.
func adminMigrationCutoverSourceBundleGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) (adminMigrationSourceImportRead, bool) {
	sourcePath := strings.TrimSpace(opts.sourcePath)
	if sourcePath == "" {
		return adminMigrationSourceImportRead{}, false
	}
	out.Target.SourcePath = adminMigrationRedactMigrationPath(sourcePath)
	read, err := adminMigrationReadSourceImportBundle(sourcePath)
	if err != nil {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "cutover_source_bundle",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "normalized source evidence could not be read for cutover freshness checks",
			Details: map[string]string{
				"source_path": adminMigrationRedactMigrationPath(sourcePath),
			},
		})
		for _, finding := range read.Bundle.Findings {
			adminMigrationMarkFinding(out, finding)
		}
		if len(read.Bundle.Findings) == 0 {
			adminMigrationMarkFinding(out, adminMigrationFinding{
				Severity: "error",
				Code:     "cutover_source_bundle_invalid",
				Family:   "source_bundle",
				Message:  "normalized source evidence could not be read for cutover freshness checks",
			})
		}
		return adminMigrationSourceImportRead{}, false
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "cutover_source_bundle",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "normalized source evidence is readable for cutover freshness checks",
		Details: map[string]string{
			"source_path":   adminMigrationRedactMigrationPath(sourcePath),
			"source_cursor": adminMigrationSourceSyncCursor(read),
			"payloads":      fmt.Sprintf("%d", len(read.Bundle.Manifest.Payloads)),
		},
	})
	return read, true
}

// adminMigrationCutoverImportProgressGate proves the offline import reached the
// metadata publication phase before operators cut clients over to the target.
func adminMigrationCutoverImportProgressGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) {
	path := adminMigrationCutoverImportProgressPath(opts)
	out.Target.ImportProgressPath = adminMigrationRedactMigrationPath(path)
	if strings.TrimSpace(path) == "" {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_import_progress",
			Status:     "warning",
			Backend:    "filesystem",
			Configured: false,
			Message:    "source import progress evidence was not provided; treat import completion as an operator checklist item",
		})
		return
	}
	progress, err := adminMigrationReadSourceImportProgress(path)
	if err != nil {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_import_progress",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source import progress evidence could not be read",
			Details:    map[string]string{"progress_path": adminMigrationRedactMigrationPath(path)},
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "source_import_progress_invalid",
			Family:   "source_import_progress",
			Message:  "source import progress evidence could not be read",
		})
		return
	}
	details := map[string]string{
		"progress_path":     adminMigrationRedactMigrationPath(path),
		"metadata_imported": fmt.Sprintf("%t", progress.MetadataImported),
		"copied_blobs":      fmt.Sprintf("%d", len(progress.CopiedBlobs)),
		"verified_blobs":    fmt.Sprintf("%d", len(progress.VerifiedBlobs)),
	}
	if !progress.MetadataImported {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_import_progress",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source import progress does not prove metadata was imported",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "source_import_incomplete",
			Family:   "source_import_progress",
			Message:  "source import progress does not prove metadata was imported",
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "source_import_progress",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "source import progress confirms metadata import completed",
		Details:    details,
	})
}

// adminMigrationCutoverSyncFreshnessGate checks that the last source-sync apply
// covered the exact normalized source cursor operators intend to cut over.
func adminMigrationCutoverSyncFreshnessGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues, read adminMigrationSourceImportRead, sourceLoaded bool) {
	path := adminMigrationCutoverSyncProgressPath(opts)
	out.Target.SyncProgressPath = adminMigrationRedactMigrationPath(path)
	if strings.TrimSpace(path) == "" {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_sync_freshness",
			Status:     "warning",
			Backend:    "filesystem",
			Configured: false,
			Message:    "source sync progress evidence was not provided; perform a final sync after the source write freeze",
		})
		return
	}
	progress, err := adminMigrationReadSourceSyncProgress(path)
	if err != nil {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_sync_freshness",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source sync progress evidence could not be read",
			Details:    map[string]string{"progress_path": adminMigrationRedactMigrationPath(path)},
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "source_sync_progress_invalid",
			Family:   "source_sync_freshness",
			Message:  "source sync progress evidence could not be read",
		})
		return
	}
	details := map[string]string{
		"progress_path": adminMigrationRedactMigrationPath(path),
		"last_status":   strings.TrimSpace(progress.LastStatus),
	}
	if details["last_status"] == "" {
		details["last_status"] = "none"
	}
	if progress.LastStatus != "applied" {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_sync_freshness",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source sync progress does not show a successful final apply",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "source_sync_not_applied",
			Family:   "source_sync_freshness",
			Message:  "source sync progress does not show a successful final apply",
		})
		return
	}
	if !sourceLoaded {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_sync_freshness",
			Status:     "warning",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source sync progress is applied, but no normalized source was provided to verify cursor freshness",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "warning",
			Code:     "source_sync_cursor_unverified",
			Family:   "source_sync_freshness",
			Message:  "source sync progress is applied, but no normalized source was provided to verify cursor freshness",
		})
		return
	}
	cursor := adminMigrationSourceSyncCursor(read)
	details["source_cursor"] = cursor
	details["cursor_seen"] = fmt.Sprintf("%t", adminMigrationSourceSyncProgressSet(progress.AppliedCursors)[cursor])
	if !adminMigrationSourceSyncProgressSet(progress.AppliedCursors)[cursor] {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "source_sync_freshness",
			Status:     "error",
			Backend:    "filesystem",
			Configured: true,
			Message:    "source sync progress did not apply the current normalized source cursor",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "source_sync_stale",
			Family:   "source_sync_freshness",
			Message:  "source sync progress did not apply the current normalized source cursor",
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "source_sync_freshness",
		Status:     "ok",
		Backend:    "filesystem",
		Configured: true,
		Message:    "source sync progress confirms the current normalized source cursor was applied",
		Details:    details,
	})
}

// adminMigrationCutoverSearchCleanlinessGate consumes admin search-check JSON so
// cutover rehearsal can prove OpenSearch is clean instead of merely reindexed.
func adminMigrationCutoverSearchCleanlinessGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) {
	path := strings.TrimSpace(opts.searchResultPath)
	out.Target.SearchResultPath = adminMigrationRedactMigrationPath(path)
	if path == "" {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "search_cleanliness",
			Status:     "warning",
			Backend:    "opensearch",
			Configured: false,
			Message:    "search check evidence was not provided; run opencook admin search check before client cutover",
		})
		return
	}
	result, err := adminMigrationReadCutoverJSONEvidence(path)
	if err != nil {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "search_cleanliness",
			Status:     "error",
			Backend:    "opensearch",
			Configured: true,
			Message:    "search check evidence could not be read",
			Details:    map[string]string{"result_path": adminMigrationRedactMigrationPath(path)},
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "cutover_search_check_unreadable",
			Family:   "opensearch",
			Message:  "search check evidence could not be read",
		})
		return
	}
	counts := adminMigrationJSONMap(result, "counts")
	details := map[string]string{
		"result_path":        adminMigrationRedactMigrationPath(path),
		"missing_documents":  fmt.Sprintf("%d", adminMigrationJSONInt(counts, "missing")),
		"stale_documents":    fmt.Sprintf("%d", adminMigrationJSONInt(counts, "stale")),
		"unsupported_scopes": fmt.Sprintf("%d", adminMigrationJSONInt(counts, "unsupported")),
		"failed":             fmt.Sprintf("%d", adminMigrationJSONInt(counts, "failed")),
		"clean":              fmt.Sprintf("%d", adminMigrationJSONInt(counts, "clean")),
	}
	if adminMigrationJSONString(result, "command") != "search_check" || !adminMigrationJSONBool(result, "ok") || adminMigrationJSONInt(counts, "missing") > 0 || adminMigrationJSONInt(counts, "stale") > 0 || adminMigrationJSONInt(counts, "unsupported") > 0 || adminMigrationJSONInt(counts, "failed") > 0 {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "search_cleanliness",
			Status:     "error",
			Backend:    "opensearch",
			Configured: true,
			Message:    "search check evidence does not show a clean restored index",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "cutover_search_not_clean",
			Family:   "opensearch",
			Message:  "search check evidence does not show a clean restored index",
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "search_cleanliness",
		Status:     "ok",
		Backend:    "opensearch",
		Configured: true,
		Message:    "search check evidence shows the restored index is clean",
		Details:    details,
	})
}

// adminMigrationCutoverShadowEvidenceGate reads prior shadow-compare JSON and
// promotes any mismatch, auth failure, or download failure to a cutover blocker.
func adminMigrationCutoverShadowEvidenceGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) {
	path := strings.TrimSpace(opts.shadowResultPath)
	out.Target.ShadowResultPath = adminMigrationRedactMigrationPath(path)
	if path == "" {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "shadow_read_evidence",
			Status:     "warning",
			Backend:    "http",
			Configured: false,
			Message:    "shadow-read comparison evidence was not provided; compare source and target reads before client cutover",
		})
		return
	}
	result, err := adminMigrationReadCutoverJSONEvidence(path)
	if err != nil {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "shadow_read_evidence",
			Status:     "error",
			Backend:    "http",
			Configured: true,
			Message:    "shadow-read comparison evidence could not be read",
			Details:    map[string]string{"result_path": adminMigrationRedactMigrationPath(path)},
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "cutover_shadow_result_unreadable",
			Family:   "shadow_read",
			Message:  "shadow-read comparison evidence could not be read",
		})
		return
	}
	shadowFailed, _ := adminMigrationJSONInventoryFamilyCount(result, "", "shadow_failed")
	details := map[string]string{
		"result_path":   adminMigrationRedactMigrationPath(path),
		"shadow_failed": fmt.Sprintf("%d", shadowFailed),
		"errors":        fmt.Sprintf("%d", adminMigrationJSONArrayLength(result, "errors")),
	}
	if adminMigrationJSONString(result, "command") != "migration_shadow_compare" || !adminMigrationJSONBool(result, "ok") || shadowFailed > 0 || adminMigrationJSONArrayLength(result, "errors") > 0 {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "shadow_read_evidence",
			Status:     "error",
			Backend:    "http",
			Configured: true,
			Message:    "shadow-read comparison evidence contains cutover blockers",
			Details:    details,
		})
		adminMigrationMarkFinding(out, adminMigrationFinding{
			Severity: "error",
			Code:     "cutover_shadow_result_failed",
			Family:   "shadow_read",
			Message:  "shadow-read comparison evidence contains cutover blockers",
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "shadow_read_evidence",
		Status:     "ok",
		Backend:    "http",
		Configured: true,
		Message:    "shadow-read comparison evidence passed",
		Details:    details,
	})
}

// adminMigrationCutoverRollbackReadinessGate keeps rollback as an explicit
// operator acknowledgement instead of inferring it from target health.
func adminMigrationCutoverRollbackReadinessGate(out *adminMigrationCLIOutput, opts *adminMigrationFlagValues) {
	if !opts.rollbackReady {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "rollback_readiness",
			Status:     "warning",
			Backend:    "runbook",
			Configured: false,
			Message:    "rollback readiness was not confirmed; keep the source Chef path available until target smoke checks pass",
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "rollback_readiness",
		Status:     "ok",
		Backend:    "runbook",
		Configured: true,
		Message:    "rollback readiness confirmed for the source Chef path",
	})
}

// adminMigrationApplyCutoverLiveGates turns signed rehearsal counters into
// explicit cutover dependencies for authentication and blob reachability.
func adminMigrationApplyCutoverLiveGates(out *adminMigrationCLIOutput, rehearsal adminMigrationRehearsalResult, checks []adminMigrationRehearsalCheck) {
	adminMigrationCutoverSignedAuthGate(out, rehearsal)
	adminMigrationCutoverBlobReachabilityGate(out, rehearsal, checks)
}

// adminMigrationCutoverSignedAuthGate separates request-signing health from
// object-read failures so operators can quickly distinguish auth blockers.
func adminMigrationCutoverSignedAuthGate(out *adminMigrationCLIOutput, rehearsal adminMigrationRehearsalResult) {
	details := map[string]string{
		"checks": fmt.Sprintf("%d", rehearsal.Checks),
		"passed": fmt.Sprintf("%d", rehearsal.Passed),
		"failed": fmt.Sprintf("%d", rehearsal.Failed),
	}
	if rehearsal.Passed > 0 {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "signed_auth",
			Status:     "ok",
			Backend:    "http",
			Configured: true,
			Message:    "signed admin authentication succeeded for representative target reads",
			Details:    details,
		})
		return
	}
	status := "warning"
	message := "no representative signed reads were available to prove request authentication"
	if rehearsal.Checks > 0 {
		status = "error"
		message = "signed admin authentication did not succeed for representative target reads"
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "signed_auth",
		Status:     status,
		Backend:    "http",
		Configured: true,
		Message:    message,
		Details:    details,
	})
}

// adminMigrationCutoverBlobReachabilityGate confirms at least one signed
// cookbook/artifact blob route works whenever the backup restored blob bytes.
func adminMigrationCutoverBlobReachabilityGate(out *adminMigrationCLIOutput, rehearsal adminMigrationRehearsalResult, checks []adminMigrationRehearsalCheck) {
	expected := adminMigrationExpectedRehearsalDownloads(checks)
	details := map[string]string{
		"expected_download_checks": fmt.Sprintf("%d", expected),
		"downloads":                fmt.Sprintf("%d", rehearsal.Downloads),
	}
	if expected == 0 {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "blob_reachability",
			Status:     "warning",
			Backend:    "blob",
			Configured: false,
			Message:    "no copied cookbook blob downloads were available to validate during cutover rehearsal",
			Details:    details,
		})
		return
	}
	if rehearsal.Downloads < expected || adminMigrationRehearsalHasDownloadFailure(rehearsal) {
		adminMigrationMarkDependency(out, adminMigrationDependency{
			Name:       "blob_reachability",
			Status:     "error",
			Backend:    "blob",
			Configured: true,
			Message:    "one or more signed cookbook blob downloads failed during cutover rehearsal",
			Details:    details,
		})
		return
	}
	adminMigrationMarkDependency(out, adminMigrationDependency{
		Name:       "blob_reachability",
		Status:     "ok",
		Backend:    "blob",
		Configured: true,
		Message:    "signed cookbook blob downloads succeeded during cutover rehearsal",
		Details:    details,
	})
}

// adminMigrationRehearsalHasDownloadFailure keeps blob gate status aligned with
// detailed cutover_download_* findings from representative cookbook reads.
func adminMigrationRehearsalHasDownloadFailure(rehearsal adminMigrationRehearsalResult) bool {
	for _, finding := range rehearsal.Findings {
		if strings.HasPrefix(finding.Code, "cutover_download_") {
			return true
		}
	}
	return false
}

// adminMigrationExpectedRehearsalDownloads counts representative routes that
// should yield a signed blob URL, not every checksum in a large cookbook.
func adminMigrationExpectedRehearsalDownloads(checks []adminMigrationRehearsalCheck) int {
	count := 0
	for _, check := range checks {
		if check.RequireBlobContent {
			count++
		}
	}
	return count
}

// adminMigrationCutoverImportProgressPath shares source-import retry defaults
// while allowing cutover rehearsal to accept a standalone progress path.
func adminMigrationCutoverImportProgressPath(opts *adminMigrationFlagValues) string {
	if strings.TrimSpace(opts.importProgressPath) != "" {
		return strings.TrimSpace(opts.importProgressPath)
	}
	if strings.TrimSpace(opts.sourcePath) != "" {
		return adminMigrationSourceImportProgressFile(opts.sourcePath, "")
	}
	return ""
}

// adminMigrationCutoverSyncProgressPath shares source-sync retry defaults while
// still letting operators point rehearsal at archived evidence files.
func adminMigrationCutoverSyncProgressPath(opts *adminMigrationFlagValues) string {
	if strings.TrimSpace(opts.syncProgressPath) != "" {
		return strings.TrimSpace(opts.syncProgressPath)
	}
	if strings.TrimSpace(opts.sourcePath) != "" {
		return adminMigrationSourceSyncProgressFile(opts.sourcePath, "")
	}
	return ""
}

// adminMigrationReadCutoverJSONEvidence decodes prior JSON command output using
// json.Number so count gates do not depend on float64 conversions.
func adminMigrationReadCutoverJSONEvidence(path string) (map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// adminMigrationJSONMap returns a nested map from generic JSON output while
// making absent evidence fields safe for count-based gates.
func adminMigrationJSONMap(object map[string]any, key string) map[string]any {
	value, _ := object[key].(map[string]any)
	if value == nil {
		return map[string]any{}
	}
	return value
}

// adminMigrationJSONBool extracts a boolean field from prior command JSON.
func adminMigrationJSONBool(object map[string]any, key string) bool {
	value, _ := object[key].(bool)
	return value
}

// adminMigrationJSONString extracts a string field from prior command JSON.
func adminMigrationJSONString(object map[string]any, key string) string {
	value, _ := object[key].(string)
	return strings.TrimSpace(value)
}

// adminMigrationJSONInt converts the number shapes used by stdlib JSON decoders
// and hand-built tests into one integer for gate comparisons.
func adminMigrationJSONInt(object map[string]any, key string) int {
	switch value := object[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, err := value.Int64()
		if err == nil {
			return int(parsed)
		}
	}
	return 0
}

// adminMigrationJSONArrayLength returns the array length for generic JSON
// fields such as top-level errors without exposing those errors in findings.
func adminMigrationJSONArrayLength(object map[string]any, key string) int {
	values, _ := object[key].([]any)
	return len(values)
}

// adminMigrationJSONInventoryFamilyCount finds a family count in previous
// migration output so rehearsal can gate on prior shadow-read results.
func adminMigrationJSONInventoryFamilyCount(object map[string]any, organization, family string) (int, bool) {
	inventory := adminMigrationJSONMap(object, "inventory")
	values, _ := inventory["families"].([]any)
	for _, value := range values {
		entry, ok := value.(map[string]any)
		if !ok {
			continue
		}
		entryOrg, _ := entry["organization"].(string)
		if entryOrg == organization && adminMigrationJSONString(entry, "family") == family {
			return adminMigrationJSONInt(entry, "count"), true
		}
	}
	return 0, false
}

// adminMigrationAppendCutoverReadinessSummary records compact blocker/advisory
// counts after all gates run so humans and shell scripts can read one summary.
func adminMigrationAppendCutoverReadinessSummary(out *adminMigrationCLIOutput) {
	blockers, advisories := adminMigrationCutoverReadinessCounts(out)
	out.Inventory.Families = append(out.Inventory.Families,
		adminMigrationInventoryFamily{Family: "cutover_blockers", Count: blockers},
		adminMigrationInventoryFamily{Family: "cutover_advisories", Count: advisories},
	)
}

// adminMigrationCutoverReadinessCounts treats failed gates as blockers and
// warning gates/findings as advisories without double-counting error details.
func adminMigrationCutoverReadinessCounts(out *adminMigrationCLIOutput) (int, int) {
	blockers := 0
	advisories := 0
	for _, dep := range out.Dependencies {
		switch dep.Status {
		case "error":
			blockers++
		case "warning":
			advisories++
		}
	}
	for _, finding := range out.Findings {
		if finding.Severity == "warning" {
			advisories++
		}
	}
	return blockers, advisories
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

// adminMigrationSourceReadBlobManifest adapts copied source checksums to the
// backup blob manifest shape reused by rehearsal and shadow download checks.
func adminMigrationSourceReadBlobManifest(state adminMigrationSourceImportState, read adminMigrationSourceImportRead) adminMigrationBackupBlobManifest {
	copies := make([]adminMigrationBackupBlobCopy, 0, len(read.CopiedChecksums))
	for checksum := range read.CopiedChecksums {
		checksum = strings.ToLower(strings.TrimSpace(checksum))
		if checksum != "" {
			copies = append(copies, adminMigrationBackupBlobCopy{Checksum: checksum})
		}
	}
	return adminMigrationBackupBlobManifestFromState(state.CoreObjects, state.Cookbooks, copies)
}

// adminMigrationShadowComparableChecks removes routes that cannot be exercised
// safely through GET shadow reads, preserving the skipped count for reporting.
func adminMigrationShadowComparableChecks(checks []adminMigrationRehearsalCheck) ([]adminMigrationRehearsalCheck, int) {
	var comparable []adminMigrationRehearsalCheck
	skipped := 0
	for _, check := range checks {
		switch check.Family {
		case "status", "server_api_version", "sandboxes":
			skipped++
			continue
		default:
			comparable = append(comparable, check)
		}
	}
	return comparable, skipped
}

// adminMigrationRunShadowComparison executes target GETs, compares normalized
// payloads to source-derived expectations, and follows signed blob downloads.
func adminMigrationRunShadowComparison(ctx context.Context, client adminJSONClient, source adminMigrationSourceImportState, checks []adminMigrationRehearsalCheck, skipped int) adminMigrationShadowComparisonResult {
	result := adminMigrationShadowComparisonResult{Checks: len(checks) + skipped, Skipped: skipped}
	if skipped > 0 {
		result.Findings = append(result.Findings, adminMigrationFinding{
			Severity: "warning",
			Code:     "shadow_read_scope_skipped",
			Family:   "shadow_read",
			Message:  "one or more source families do not have safe GET shadow-read routes and were skipped instead of issuing writes",
		})
	}
	for _, check := range checks {
		var targetPayload any
		if err := client.DoJSON(ctx, http.MethodGet, check.Path, nil, &targetPayload); err != nil {
			result.Failed++
			result.Findings = append(result.Findings, adminMigrationShadowFailureFinding(adminMigrationShadowReadErrorCode(err), check, "target read failed during shadow comparison"))
			continue
		}
		if check.Family == "search" {
			if !adminMigrationShadowSearchCountMatches(check, source, targetPayload) {
				result.Failed++
				result.Findings = append(result.Findings, adminMigrationShadowFailureFinding("shadow_search_count_mismatch", check, "target search result count differed from source-derived state"))
				continue
			}
			result.Passed++
			continue
		}
		sourcePayload, ok := adminMigrationShadowSourcePayload(check, source)
		if !ok {
			result.Failed++
			result.Findings = append(result.Findings, adminMigrationShadowFailureFinding("shadow_source_payload_missing", check, "source state did not contain the expected comparison payload"))
			continue
		}
		sourceCanonical, sourceErr := adminMigrationShadowCanonicalPayload(check.Family, sourcePayload)
		targetCanonical, targetErr := adminMigrationShadowCanonicalPayload(check.Family, targetPayload)
		if sourceErr != nil || targetErr != nil || sourceCanonical != targetCanonical {
			result.Failed++
			result.Findings = append(result.Findings, adminMigrationShadowFailureFinding("shadow_payload_mismatch", check, "target payload differed from source after compatibility normalization"))
			continue
		}
		result.Passed++
		if check.RequireBlobContent {
			downloaded, findings := adminMigrationValidateShadowDownloads(ctx, client, check, targetPayload)
			result.Downloads += downloaded
			if len(findings) > 0 {
				result.Failed += len(findings)
				result.Findings = append(result.Findings, findings...)
			}
		}
	}
	return result
}

// adminMigrationShadowSourcePayload renders the source import state into the
// minimal response shape that the target route should expose for comparison.
func adminMigrationShadowSourcePayload(check adminMigrationRehearsalCheck, source adminMigrationSourceImportState) (any, bool) {
	orgName := adminMigrationCheckOrganization(check.Path)
	switch check.Family {
	case "users":
		value, ok := source.Bootstrap.Users[check.Name]
		return adminMigrationShadowUserPayload(value), ok
	case "user_keys":
		keys, ok := source.Bootstrap.UserKeys[check.Name]
		return adminMigrationShadowKeyListPayload(keys), ok
	case "user_acls":
		value, ok := source.Bootstrap.UserACLs[check.Name]
		return value, ok
	case "organizations":
		org, ok := source.Bootstrap.Orgs[check.Name]
		return org.Organization, ok
	case "organization_acls":
		org, ok := source.Bootstrap.Orgs[check.Name]
		if !ok {
			return nil, false
		}
		value, ok := org.ACLs[adminMigrationOrganizationACLKey()]
		return value, ok
	case "clients":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		value, ok := org.Clients[check.Name]
		return adminMigrationShadowClientPayload(value), ok
	case "client_keys":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		keys, ok := org.ClientKeys[check.Name]
		return adminMigrationShadowKeyListPayload(keys), ok
	case "client_acls":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		value, ok := org.ACLs[adminMigrationClientACLKey(check.Name)]
		return value, ok
	case "groups":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		value, ok := org.Groups[check.Name]
		return value, ok
	case "containers":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		value, ok := org.Containers[check.Name]
		return value, ok
	case "container_acls":
		org, ok := source.Bootstrap.Orgs[orgName]
		if !ok {
			return nil, false
		}
		value, ok := org.ACLs[adminMigrationContainerACLKey(check.Name)]
		return value, ok
	}

	coreOrg, ok := source.CoreObjects.Orgs[orgName]
	if !ok {
		return nil, false
	}
	switch check.Family {
	case "nodes":
		value, ok := coreOrg.Nodes[check.Name]
		return value, ok
	case "environments":
		value, ok := coreOrg.Environments[check.Name]
		return value, ok
	case "roles":
		value, ok := coreOrg.Roles[check.Name]
		return value, ok
	case "data_bags":
		_, ok := coreOrg.DataBags[check.Name]
		return adminMigrationShadowDataBagPayload(orgName, check.Name, coreOrg.DataBagItems[check.Name]), ok
	case "data_bag_items":
		bagName, itemName, ok := strings.Cut(check.Name, "/")
		if !ok {
			return nil, false
		}
		item, ok := coreOrg.DataBagItems[bagName][itemName]
		if !ok {
			return nil, false
		}
		return item.RawData, true
	case "policy_revisions":
		policyName, revisionID, ok := strings.Cut(check.Name, "/")
		if !ok {
			return nil, false
		}
		revision, ok := coreOrg.Policies[policyName][revisionID]
		if !ok {
			return nil, false
		}
		return adminMigrationShadowPolicyRevisionPayload(orgName, revision, source), revision.Payload != nil
	case "policy_groups":
		value, ok := coreOrg.PolicyGroups[check.Name]
		return adminMigrationShadowPolicyGroupPayload(orgName, value), ok
	case "cookbook_versions":
		version, ok := adminMigrationShadowCookbookVersion(source.Cookbooks.Orgs[orgName], check.Name)
		if !ok {
			return nil, false
		}
		return adminMigrationShadowCookbookVersionPayload(version), true
	case "cookbook_artifacts":
		artifact, ok := adminMigrationShadowCookbookArtifact(source.Cookbooks.Orgs[orgName], check.Name, adminMigrationLastPathSegment(check.Path))
		if !ok {
			return nil, false
		}
		return adminMigrationShadowCookbookArtifactPayload(artifact), true
	default:
		return nil, false
	}
}

// adminMigrationShadowUserPayload mirrors the user GET shape while leaving
// OpenCook-only request metadata for the generic normalizer to strip.
func adminMigrationShadowUserPayload(user bootstrap.User) map[string]any {
	return map[string]any{
		"username":     user.Username,
		"display_name": user.DisplayName,
		"email":        user.Email,
		"first_name":   user.FirstName,
		"last_name":    user.LastName,
	}
}

// adminMigrationShadowClientPayload renders the API v1 client read shape so
// imported key material does not falsely mismatch against public_key omission.
func adminMigrationShadowClientPayload(client bootstrap.Client) map[string]any {
	return map[string]any{
		"name":       client.Name,
		"clientname": client.ClientName,
		"json_class": "Chef::ApiClient",
		"chef_type":  "client",
		"orgname":    client.Organization,
		"validator":  client.Validator,
	}
}

// adminMigrationShadowKeyListPayload mirrors Chef key-list responses and
// intentionally excludes public/private key material from list comparisons.
func adminMigrationShadowKeyListPayload(keys map[string]bootstrap.KeyRecord) []map[string]any {
	names := adminMigrationSortedMapKeys(keys)
	out := make([]map[string]any, 0, len(names))
	for _, name := range names {
		key := keys[name]
		out = append(out, map[string]any{
			"name":    key.Name,
			"uri":     key.URI,
			"expired": key.Expired,
		})
	}
	return out
}

// adminMigrationShadowDataBagPayload mirrors named data-bag GET responses,
// which list item URLs rather than returning the stored data-bag metadata row.
func adminMigrationShadowDataBagPayload(orgName, bagName string, items map[string]bootstrap.DataBagItem) map[string]string {
	out := make(map[string]string, len(items))
	for _, itemID := range adminMigrationSortedMapKeys(items) {
		out[itemID] = adminPath("organizations", orgName, "data", bagName, itemID)
	}
	return out
}

// adminMigrationShadowPolicyRevisionPayload adds the route-computed
// policy_group_list field to the stored policy revision payload.
func adminMigrationShadowPolicyRevisionPayload(orgName string, revision bootstrap.PolicyRevision, source adminMigrationSourceImportState) map[string]any {
	out := adminMigrationShadowCloneMap(revision.Payload)
	var groups []string
	for _, groupName := range adminMigrationSortedMapKeys(source.CoreObjects.Orgs[orgName].PolicyGroups) {
		group := source.CoreObjects.Orgs[orgName].PolicyGroups[groupName]
		if group.Policies[revision.Name] == revision.RevisionID {
			groups = append(groups, groupName)
		}
	}
	out["policy_group_list"] = adminMigrationStringSliceToAny(groups)
	return out
}

// adminMigrationShadowPolicyGroupPayload renders named policy-group responses
// with stable policy assignment maps and explicit route URIs.
func adminMigrationShadowPolicyGroupPayload(orgName string, group bootstrap.PolicyGroup) map[string]any {
	return map[string]any{
		"uri":      adminPath("organizations", orgName, "policy_groups", group.Name),
		"policies": adminMigrationShadowPolicyAssignments(group.Policies),
	}
}

// adminMigrationShadowPolicyAssignments mirrors the Chef-facing policy group
// assignment map where each policy points at its pinned revision ID.
func adminMigrationShadowPolicyAssignments(policies map[string]string) map[string]any {
	out := make(map[string]any, len(policies))
	for _, name := range adminMigrationSortedMapKeys(policies) {
		out[name] = map[string]any{"revision_id": policies[name]}
	}
	return out
}

// adminMigrationStringSliceToAny produces the JSON array shape used by policy
// revision responses without relying on the API package internals.
func adminMigrationStringSliceToAny(values []string) []any {
	if len(values) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

// adminMigrationShadowCookbookVersion finds a cookbook version by the
// route-style "cookbook/version" label used by representative checks.
func adminMigrationShadowCookbookVersion(org adminMigrationCookbookOrgExport, label string) (bootstrap.CookbookVersion, bool) {
	name, version, ok := strings.Cut(label, "/")
	if !ok {
		return bootstrap.CookbookVersion{}, false
	}
	for _, candidate := range org.Versions {
		if adminMigrationCookbookRouteName(candidate) == name && candidate.Version == version {
			return candidate, true
		}
	}
	return bootstrap.CookbookVersion{}, false
}

// adminMigrationShadowCookbookArtifact finds an artifact by name and immutable
// identifier, falling back to name when older tests omit the identifier path.
func adminMigrationShadowCookbookArtifact(org adminMigrationCookbookOrgExport, name, identifier string) (bootstrap.CookbookArtifact, bool) {
	for _, candidate := range org.Artifacts {
		if candidate.Name != name {
			continue
		}
		if identifier == "" || strings.EqualFold(candidate.Identifier, identifier) {
			return candidate, true
		}
	}
	return bootstrap.CookbookArtifact{}, false
}

// adminMigrationShadowCookbookVersionPayload mirrors API v1 cookbook-version
// reads, including legacy segment buckets and checksum download URLs.
func adminMigrationShadowCookbookVersionPayload(version bootstrap.CookbookVersion) any {
	value := map[string]any{
		"name":          version.Name,
		"cookbook_name": version.CookbookName,
		"version":       version.Version,
		"json_class":    version.JSONClass,
		"chef_type":     version.ChefType,
		"frozen?":       version.Frozen,
		"metadata":      adminMigrationShadowCookbookReadMetadata(version.CookbookName, version.Version, version.Metadata),
	}
	adminMigrationShadowAddLegacyCookbookFiles(value, version.AllFiles)
	return value
}

// adminMigrationShadowCookbookArtifactPayload mirrors API v1 artifact reads so
// immutable cookbook artifacts compare by their Chef-facing legacy file buckets.
func adminMigrationShadowCookbookArtifactPayload(artifact bootstrap.CookbookArtifact) any {
	value := map[string]any{
		"name":       artifact.Name,
		"identifier": artifact.Identifier,
		"version":    artifact.Version,
		"chef_type":  artifact.ChefType,
		"frozen?":    artifact.Frozen,
		"metadata":   adminMigrationShadowCloneMap(artifact.Metadata),
	}
	adminMigrationShadowAddLegacyCookbookFiles(value, artifact.AllFiles)
	return value
}

var adminMigrationShadowCookbookSegments = []string{"recipes", "definitions", "libraries", "attributes", "files", "templates", "resources", "providers", "root_files"}

// adminMigrationShadowAddLegacyCookbookFiles expands all_files into Chef's
// legacy per-segment response buckets and attaches checksum-addressed URLs.
func adminMigrationShadowAddLegacyCookbookFiles(value map[string]any, files []bootstrap.CookbookFile) {
	segments := map[string][]map[string]any{}
	for _, segment := range adminMigrationShadowCookbookSegments {
		segments[segment] = []map[string]any{}
	}
	for _, file := range files {
		segment := adminMigrationShadowCookbookFileSegment(file.Path)
		segments[segment] = append(segments[segment], map[string]any{
			"name":        adminMigrationShadowCookbookLegacyFileName(segment, file.Path),
			"path":        file.Path,
			"checksum":    file.Checksum,
			"specificity": file.Specificity,
			"url":         "/_blob/checksums/" + file.Checksum,
		})
	}
	for _, segment := range adminMigrationShadowCookbookSegments {
		value[segment] = segments[segment]
	}
}

// adminMigrationShadowCookbookReadMetadata applies the default metadata fields
// the cookbook GET route synthesizes for version reads.
func adminMigrationShadowCookbookReadMetadata(name, version string, metadata map[string]any) map[string]any {
	raw := adminMigrationShadowCloneMap(metadata)
	out := map[string]any{
		"attributes":       map[string]any{},
		"dependencies":     map[string]any{},
		"description":      "A fabulous new cookbook",
		"license":          "Apache v2.0",
		"long_description": "",
		"maintainer":       "Your Name",
		"maintainer_email": "youremail@example.com",
		"name":             name,
		"recipes":          map[string]any{},
		"version":          version,
	}
	for _, field := range []string{"attributes", "dependencies", "recipes", "description", "license", "long_description", "maintainer", "maintainer_email"} {
		if value, ok := raw[field]; ok {
			out[field] = value
		}
	}
	return out
}

// adminMigrationShadowCookbookFileSegment follows the API's legacy segment
// bucketing so source fixtures compare with v0/v1 cookbook reads.
func adminMigrationShadowCookbookFileSegment(path string) string {
	if !strings.Contains(path, "/") {
		return "root_files"
	}
	segment, _, _ := strings.Cut(path, "/")
	for _, allowed := range adminMigrationShadowCookbookSegments {
		if segment == allowed {
			return segment
		}
	}
	return "files"
}

// adminMigrationShadowCookbookLegacyFileName returns the basename that legacy
// cookbook segment arrays expose for non-root files.
func adminMigrationShadowCookbookLegacyFileName(segment, path string) string {
	if segment == "root_files" {
		return path
	}
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}

// adminMigrationShadowCloneMap copies JSON object maps before adding
// route-computed fields for source-vs-target comparisons.
func adminMigrationShadowCloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = adminMigrationShadowJSONValue(value)
	}
	return out
}

// adminMigrationShadowSearchCountMatches compares only result counts for search
// because row order and query scoring are provider-derived compatibility noise.
func adminMigrationShadowSearchCountMatches(check adminMigrationRehearsalCheck, source adminMigrationSourceImportState, targetPayload any) bool {
	targetCount, ok := adminMigrationShadowSearchResultCount(targetPayload)
	if !ok {
		return false
	}
	return targetCount == adminMigrationShadowExpectedSearchCount(check, source)
}

// adminMigrationShadowExpectedSearchCount derives the number of indexed rows
// from source PostgreSQL families rather than trusting source search artifacts.
func adminMigrationShadowExpectedSearchCount(check adminMigrationRehearsalCheck, source adminMigrationSourceImportState) int {
	orgName := adminMigrationCheckOrganization(check.Path)
	bootstrapOrg := source.Bootstrap.Orgs[orgName]
	coreOrg := source.CoreObjects.Orgs[orgName]
	switch check.Name {
	case "client":
		return len(bootstrapOrg.Clients)
	case "environment":
		return len(coreOrg.Environments)
	case "node":
		return len(coreOrg.Nodes)
	case "role":
		return len(coreOrg.Roles)
	default:
		return len(coreOrg.DataBagItems[check.Name])
	}
}

// adminMigrationShadowSearchResultCount extracts Chef search totals while
// tolerating JSON number decoding from fake and real admin clients.
func adminMigrationShadowSearchResultCount(value any) (int, bool) {
	object, ok := value.(map[string]any)
	if !ok {
		return 0, false
	}
	switch total := object["total"].(type) {
	case int:
		return total, true
	case float64:
		return int(total), total == float64(int(total))
	case json.Number:
		value, err := total.Int64()
		return int(value), err == nil
	default:
		return 0, false
	}
}

// adminMigrationShadowCanonicalPayload serializes a normalized payload so
// comparisons are deterministic without exposing payload bodies in findings.
func adminMigrationShadowCanonicalPayload(family string, value any) (string, error) {
	normalized := adminMigrationShadowNormalizeValue(family, "", adminMigrationShadowJSONValue(value))
	data, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// adminMigrationShadowJSONValue round-trips structs into generic JSON values so
// the normalizer handles source and target payloads identically.
func adminMigrationShadowJSONValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return value
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return value
	}
	return out
}

// adminMigrationShadowNormalizeValue removes documented compatibility noise but
// preserves fields whose differences should block a Chef cutover.
func adminMigrationShadowNormalizeValue(family, key string, value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := map[string]any{}
		for childKey, childValue := range typed {
			if adminMigrationShadowSkipKey(family, childKey) {
				continue
			}
			out[childKey] = adminMigrationShadowNormalizeValue(family, childKey, childValue)
		}
		return out
	case []any:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, adminMigrationShadowNormalizeValue(family, key, item))
		}
		if adminMigrationShadowUnorderedArrayKey(key) {
			sort.Slice(out, func(i, j int) bool {
				left, _ := json.Marshal(out[i])
				right, _ := json.Marshal(out[j])
				return string(left) < string(right)
			})
		}
		return out
	case string:
		if key == "url" {
			return adminMigrationShadowNormalizeURLString(typed)
		}
		return typed
	case nil:
		if key == "actors" || key == "groups" {
			return []any{}
		}
		return nil
	default:
		return typed
	}
}

// adminMigrationShadowSkipKey captures volatile or version-specific fields that
// should not decide source-to-target compatibility during read-only shadowing.
func adminMigrationShadowSkipKey(family, key string) bool {
	switch key {
	case "private_key", "created_at", "updated_at", "last_updated", "last_modified", "create_time", "requestor", "authn_status", "storage_status":
		return true
	case "uri":
		return family == "users" || family == "clients" || family == "organizations"
	case "public_key":
		return family == "user_keys" || family == "client_keys"
	default:
		return false
	}
}

// adminMigrationShadowUnorderedArrayKey sorts membership and file arrays where
// Chef compatibility does not depend on response ordering.
func adminMigrationShadowUnorderedArrayKey(key string) bool {
	switch key {
	case "actors", "groups", "users", "clients", "checksums", "all_files", "definitions", "files", "libraries", "recipes", "templates", "root_files":
		return true
	default:
		return false
	}
}

// adminMigrationShadowNormalizeURLString strips signed blob query strings and
// hosts so source and target compare by checksum-addressed download path.
func adminMigrationShadowNormalizeURLString(raw string) string {
	trimmed := strings.TrimSpace(raw)
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return trimmed
	}
	if strings.Contains(parsed.Path, "/_blob/checksums/") {
		return parsed.EscapedPath()
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

// adminMigrationValidateShadowDownloads follows target-signed blob URLs and
// treats missing or mismatched bytes as shadow-read blockers.
func adminMigrationValidateShadowDownloads(ctx context.Context, client adminJSONClient, check adminMigrationRehearsalCheck, payload any) (int, []adminMigrationFinding) {
	downloader, ok := client.(adminMigrationUnsignedClient)
	if !ok {
		return 0, []adminMigrationFinding{adminMigrationShadowFailureFinding("shadow_download_unavailable", check, "admin client cannot follow signed blob download URLs during shadow comparison")}
	}
	var findings []adminMigrationFinding
	downloaded := 0
	for _, checksum := range check.DownloadChecksums {
		rawURL := adminMigrationFindDownloadURLForChecksum(payload, checksum)
		if rawURL == "" {
			findings = append(findings, adminMigrationShadowFailureFinding("shadow_download_url_missing", check, "target cookbook payload did not include a signed blob download URL"))
			continue
		}
		response, err := downloader.DoUnsigned(ctx, http.MethodGet, rawURL)
		if err != nil {
			findings = append(findings, adminMigrationShadowFailureFinding("shadow_download_failed", check, "signed blob download failed during shadow comparison"))
			continue
		}
		if adminMigrationMD5Hex(response.Body) != checksum {
			findings = append(findings, adminMigrationShadowFailureFinding("shadow_download_checksum_mismatch", check, "signed blob download body did not match its Chef checksum"))
			continue
		}
		downloaded++
		break
	}
	return downloaded, findings
}

// adminMigrationShadowReadErrorCode classifies read failures without leaking
// provider response bodies, credentials, signed URLs, or private key paths.
func adminMigrationShadowReadErrorCode(err error) string {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "401"), strings.Contains(text, "403"), strings.Contains(text, "unauthorized"), strings.Contains(text, "forbidden"):
		return "shadow_auth_failed"
	case strings.Contains(text, "404"), strings.Contains(text, "not found"):
		return "shadow_object_missing"
	default:
		return "shadow_read_failed"
	}
}

// adminMigrationShadowFailureFinding produces route-scoped comparison findings
// while avoiding raw payload, credential, or signed URL details.
func adminMigrationShadowFailureFinding(code string, check adminMigrationRehearsalCheck, message string) adminMigrationFinding {
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

// adminMigrationShadowInventoryFamilies appends shadow counters to the source
// inventory so automation can gate cutover on compare outcomes.
func adminMigrationShadowInventoryFamilies(result adminMigrationShadowComparisonResult) []adminMigrationInventoryFamily {
	return []adminMigrationInventoryFamily{
		{Family: "shadow_checks", Count: result.Checks},
		{Family: "shadow_passed", Count: result.Passed},
		{Family: "shadow_failed", Count: result.Failed},
		{Family: "shadow_skipped", Count: result.Skipped},
		{Family: "shadow_downloads", Count: result.Downloads},
	}
}

// adminMigrationShadowDependencyDetails mirrors shadow counters in string form
// for operators scanning dependency blocks.
func adminMigrationShadowDependencyDetails(result adminMigrationShadowComparisonResult) map[string]string {
	return map[string]string{
		"checks":    fmt.Sprintf("%d", result.Checks),
		"passed":    fmt.Sprintf("%d", result.Passed),
		"failed":    fmt.Sprintf("%d", result.Failed),
		"skipped":   fmt.Sprintf("%d", result.Skipped),
		"downloads": fmt.Sprintf("%d", result.Downloads),
	}
}

// adminMigrationShadowCompareRecommendations records the remaining runbook
// gates while making it explicit that shadow compare never proxies writes.
func adminMigrationShadowCompareRecommendations() []adminMigrationPlannedMutation {
	return []adminMigrationPlannedMutation{
		{Action: "shadow_read_gate", Family: "runbook", Message: "treat any shadow-read mismatch, auth failure, missing object, or blob download failure as a cutover blocker"},
		{Action: "client_cutover", Family: "runbook", Message: "switch Chef/Cinc clients only after shadow-read comparisons and cutover rehearsal pass"},
		{Action: "no_write_proxy", Family: "source_target", Message: "do not proxy writes, validator registration, cookbook upload, sandbox commit, or key/client mutations during shadow comparison"},
	}
}

// adminMigrationCheckOrganization extracts the explicit organization segment
// from representative org-scoped routes.
func adminMigrationCheckOrganization(rawPath string) string {
	parsed, err := url.Parse(rawPath)
	if err != nil {
		return ""
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(segments) >= 2 && segments[0] == "organizations" {
		org, _ := url.PathUnescape(segments[1])
		return org
	}
	return ""
}

// adminMigrationLastPathSegment returns the final unescaped path segment for
// routes whose check name omits immutable IDs, such as cookbook artifacts.
func adminMigrationLastPathSegment(rawPath string) string {
	parsed, err := url.Parse(rawPath)
	if err != nil {
		return ""
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(segments) == 0 {
		return ""
	}
	value, _ := url.PathUnescape(segments[len(segments)-1])
	return value
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
		{Action: "blocker_gate", Family: "runbook", Message: "treat source-import, sync freshness, search cleanliness, shadow-read, signed-auth, representative-read, and blob-download errors as cutover blockers"},
		{Action: "freeze_source_writes", Family: "runbook", Message: "freeze source Chef writes, run one final source sync, and keep the freeze through target smoke checks"},
		{Action: "shadow_read_compare", Family: "source_target", Message: "compare read-only source Chef responses to restored OpenCook responses with documented compatibility normalizers"},
		{Action: "client_cutover", Family: "runbook", Message: "switch DNS/load balancers or Chef/Cinc chef_server_url only after rehearsal, search checks, and shadow-read comparisons pass"},
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
	for name, inventory := range cookbookInventory {
		if adminMigrationCookbookInventoryHasRows(inventory) {
			seen[name] = struct{}{}
		}
	}
	return adminMigrationSortedStringSet(seen)
}

// adminMigrationCookbookInventoryHasRows distinguishes real cookbook metadata
// from zero-value placeholders requested during empty-target safety checks.
func adminMigrationCookbookInventoryHasRows(inventory adminMigrationCookbookInventory) bool {
	return inventory.Versions > 0 || inventory.Artifacts > 0 || inventory.ChecksumReferences > 0 || len(inventory.Checksums) > 0
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
  opencook admin migration source normalize PATH --output PATH [--yes] [--json] [--with-timing]
  opencook admin migration source import preflight PATH --offline [--json] [--with-timing]
  opencook admin migration source import apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
  opencook admin migration source sync preflight PATH --offline [--progress PATH] [--json] [--with-timing]
  opencook admin migration source sync apply PATH --offline [--dry-run|--yes] [--progress PATH] [--json] [--with-timing]
  opencook admin migration shadow compare --source PATH --target-server-url URL [--manifest PATH] [--json] [--with-timing]
  opencook admin migration cutover rehearse --manifest PATH [--source PATH] [--source-import-progress PATH] [--source-sync-progress PATH] [--search-check-result PATH] [--shadow-result PATH] [--rollback-ready] [--server-url URL] [--json] [--with-timing]

Migration commands validate target readiness, write/inspect OpenCook logical
backup bundles, restore offline targets, and inventory or normalize read-only
Chef Server source artifacts. Source import preflight validates a normalized
bundle and target readiness without writing OpenCook, blob, or search state;
source import apply copies or verifies blobs before offline PostgreSQL metadata
writes and records retry progress for non-transactional blob phases. Source
sync preflight/apply compares later normalized snapshots to manifest-covered
target families and stores cursor metadata after confirmed reconciliation.
Shadow compare reads a normalized source artifact and a restored target through
signed GETs, applies compatibility normalizers, and never proxies writes.
Cutover rehearsal remains read-only but can also consume import, sync, search,
shadow-read, and rollback evidence to separate blockers from advisories before
DNS/load-balancer or Chef/Cinc client configuration changes.

Flags:
  --org ORG
  --all-orgs
  --output PATH
  --manifest PATH
  --source PATH
  --source-import-progress PATH
  --source-sync-progress PATH
  --search-check-result PATH
  --shadow-result PATH
  --server-url URL
  --target-server-url URL
  --rollback-ready
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
