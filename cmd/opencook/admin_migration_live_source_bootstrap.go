package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type adminMigrationPGXLiveSourceBootstrapExtractor struct{}

var (
	errAdminMigrationLiveSourceBookshelfBlobNotFound         = errors.New("source Bookshelf blob not found")
	errAdminMigrationLiveSourceBookshelfBlobChecksumMismatch = errors.New("source Bookshelf blob checksum mismatch")
)

// ExtractBootstrap reads implemented source PostgreSQL families through
// independent Erchef and Bifrost read-only transactions and emits the existing
// normalized source bundle format that import/sync already validate.
func (adminMigrationPGXLiveSourceBootstrapExtractor) ExtractBootstrap(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	read, err := adminMigrationReadLiveSourceBootstrapPayloads(ctx, cfg)
	if err != nil {
		if finding, ok := adminMigrationLiveSourceContextFinding(ctx); ok {
			read.Findings = append(read.Findings, finding)
		}
		return adminMigrationLiveSourceExtractorResult{
			Dependencies: read.Dependencies,
			Findings:     read.Findings,
		}
	}
	bundle, blobExtraction, err := adminMigrationLiveSourceBundleFromPayloadValues(ctx, cfg, readPayloadValues(read))
	dependencies := append([]adminMigrationDependency{}, read.Dependencies...)
	if blobExtraction.Dependency.Name != "" {
		dependencies = append(dependencies, blobExtraction.Dependency)
	}
	dependencies = append(dependencies, adminMigrationLiveSourceNonBlobDependencies(cfg)...)
	if err != nil {
		findings := append([]adminMigrationFinding{}, read.Findings...)
		findings = append(findings, blobExtraction.Findings...)
		if blobExtraction.Dependency.Status == "error" {
			return adminMigrationLiveSourceExtractorResult{
				Dependencies: dependencies,
				Findings:     findings,
			}
		}
		findings = append(findings, adminMigrationSemanticFinding(err))
		return adminMigrationLiveSourceExtractorResult{
			Dependencies: dependencies,
			Findings:     findings,
		}
	}
	return adminMigrationLiveSourceExtractorResult{
		Dependencies: dependencies,
		Inventory:    bundle.Inventory,
		Findings: append(append(append(read.Findings, blobExtraction.Findings...), bundle.Findings...), adminMigrationFinding{
			Severity: "warning",
			Code:     adminMigrationFindingSourceFamilyUnsupported,
			Family:   "live_source",
			Message:  "live extraction currently emits PostgreSQL-backed source families and checksum blob evidence; optional HTTP read probes are added by a later task",
		}),
		PlannedMutations: []adminMigrationPlannedMutation{
			{
				Action:  "read_source_postgres_payloads",
				Family:  "source_payloads",
				Count:   len(bundle.Manifest.Payloads),
				Message: "extracted Erchef objects and Bifrost authorization payloads through independent read-only repeatable-read transactions",
			},
			{
				Action:  "write_live_source_bundle",
				Family:  "source_manifest",
				Count:   len(bundle.Manifest.Payloads),
				Message: "would write a local normalized live Chef source bundle",
			},
		},
		Bundle: &bundle,
	}
}

type adminMigrationLiveSourceBootstrapRead struct {
	Dependencies  []adminMigrationDependency
	Findings      []adminMigrationFinding
	PayloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage
}

type adminMigrationLiveSourceBlobExtraction struct {
	Dependency adminMigrationDependency
	Findings   []adminMigrationFinding
	Files      map[string][]byte
}

func adminMigrationLiveSourceNonBlobDependencies(cfg adminMigrationLiveSourceConfig) []adminMigrationDependency {
	var deps []adminMigrationDependency
	for _, dep := range adminMigrationLiveSourceNonPostgresDependencies(cfg) {
		if dep.Name == "source_blob" {
			continue
		}
		deps = append(deps, dep)
	}
	return deps
}

func readPayloadValues(read adminMigrationLiveSourceBootstrapRead) map[adminMigrationSourcePayloadKey][]json.RawMessage {
	if read.PayloadValues == nil {
		return map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	}
	return read.PayloadValues
}

// adminMigrationReadLiveSourceBootstrapPayloads owns the PostgreSQL connection
// lifecycle for live extraction and rolls back every transaction path.
func adminMigrationReadLiveSourceBootstrapPayloads(ctx context.Context, cfg adminMigrationLiveSourceConfig) (adminMigrationLiveSourceBootstrapRead, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	targets, err := adminMigrationLiveSourceDerivePostgresTargets(cfg)
	if err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL cluster seed DSN is invalid")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL cluster seed connection string could not be parsed")},
		}, err
	}
	probeCtx, cancel := context.WithTimeout(ctx, adminMigrationLiveSourcePostgresExtractionTimeout)
	defer cancel()

	erchefConn, err := pgx.ConnectConfig(probeCtx, targets.Erchef)
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("erchef", targets.Erchef.Database, "source Erchef PostgreSQL could not be reached or authenticated", false), err
	}
	defer adminMigrationLiveSourceCloseConnection(erchefConn)

	bifrostConn, err := pgx.ConnectConfig(probeCtx, targets.Bifrost)
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("bifrost", targets.Bifrost.Database, "source Bifrost PostgreSQL could not be reached or authenticated", false), err
	}
	defer adminMigrationLiveSourceCloseConnection(bifrostConn)

	erchefTx, err := erchefConn.BeginTx(probeCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("erchef", targets.Erchef.Database, "source Erchef PostgreSQL did not allow a read-only repeatable-read extraction transaction", false), err
	}
	defer adminMigrationLiveSourceRollback(erchefTx)
	bifrostTx, err := bifrostConn.BeginTx(probeCtx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("bifrost", targets.Bifrost.Database, "source Bifrost PostgreSQL did not allow a read-only repeatable-read extraction transaction", false), err
	}
	defer adminMigrationLiveSourceRollback(bifrostTx)

	erchefDatabase, erchefUser, err := adminMigrationLiveSourceEstablishSnapshot(probeCtx, erchefTx)
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("erchef", targets.Erchef.Database, "source Erchef PostgreSQL read-only snapshot could not be established", false), err
	}
	bifrostDatabase, bifrostUser, err := adminMigrationLiveSourceEstablishSnapshot(probeCtx, bifrostTx)
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("bifrost", targets.Bifrost.Database, "source Bifrost PostgreSQL read-only snapshot could not be established", false), err
	}

	if missing, schemaErr := adminMigrationLiveSourceMissingTables(probeCtx, erchefTx, adminMigrationLiveSourceRequiredErchefTables()); schemaErr != nil || len(missing) > 0 {
		return adminMigrationLiveSourceExtractionSchemaFailure("erchef", erchefDatabase, erchefUser, missing), firstAdminMigrationLiveSourceError(schemaErr, fmt.Errorf("source Erchef schema missing required tables"))
	}
	if missing, schemaErr := adminMigrationLiveSourceMissingTables(probeCtx, bifrostTx, adminMigrationLiveSourceRequiredBifrostTables()); schemaErr != nil || len(missing) > 0 {
		return adminMigrationLiveSourceExtractionSchemaFailure("bifrost", bifrostDatabase, bifrostUser, missing), firstAdminMigrationLiveSourceError(schemaErr, fmt.Errorf("source Bifrost schema missing required tables"))
	}

	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	orgs, err := adminMigrationLiveSourceReadBootstrapOrganizations(probeCtx, erchefTx, cfg)
	if err != nil {
		return adminMigrationLiveSourceExtractionDatabaseFailure("erchef", erchefDatabase, "source Erchef organizations could not be read by the configured read-only role", true), err
	}
	adminMigrationLiveSourceInitializeBootstrapPayloads(payloadValues, orgs)
	if len(orgs) == 0 {
		return adminMigrationLiveSourceExtractionDatabaseFailure("erchef", erchefDatabase, "source Erchef PostgreSQL did not expose any organizations to the configured read-only role", true), fmt.Errorf("source organizations missing")
	}
	catalog, err := adminMigrationLiveSourceReadAuthorizationCatalog(probeCtx, erchefTx, cfg)
	if err != nil {
		return adminMigrationLiveSourceExtractionErrorResult("erchef", erchefDatabase, err), err
	}
	if err := adminMigrationLiveSourceReadBootstrapRows(probeCtx, erchefTx, cfg, orgs, payloadValues); err != nil {
		failure := adminMigrationLiveSourceExtractionDatabaseFailure("erchef", erchefDatabase, "source Erchef identity and object rows could not be read by the configured read-only role", true)
		var payloadRead *adminMigrationLiveSourcePayloadReadError
		if errors.As(err, &payloadRead) {
			failure.Dependencies[0].Details["payload_family"] = payloadRead.Family
			var malformed *adminMigrationLiveSourcePayloadDecodeError
			if errors.As(err, &malformed) {
				failure.Findings = []adminMigrationFinding{{Severity: "error", Code: "source_payload_malformed", Family: payloadRead.Family, Message: "serialized source payload is malformed; extraction stopped before publication", Details: map[string]string{"identity_sha256": malformed.Identity}}}
			}

		}
		return failure, err
	}
	unrelatedBifrostRecords, err := adminMigrationLiveSourceReadBifrostAuthorization(probeCtx, bifrostTx, catalog, payloadValues)
	if err != nil {
		return adminMigrationLiveSourceExtractionErrorResult("bifrost", bifrostDatabase, err), err
	}

	findings := []adminMigrationFinding{adminMigrationLiveSourceCrossDatabaseConsistencyFinding()}
	if unrelatedBifrostRecords > 0 {
		findings = append(findings, adminMigrationFinding{
			Severity: "warning",
			Code:     adminMigrationFindingSourceBifrostUnrelatedRecords,
			Family:   "source_bifrost",
			Message:  fmt.Sprintf("source Bifrost contains %d authorization records outside the selected extraction scope; these may include valid excluded organizations or stale records and were not extracted", unrelatedBifrostRecords),
		})
	}
	return adminMigrationLiveSourceBootstrapRead{
		Dependencies: []adminMigrationDependency{
			adminMigrationLiveSourceConnectionDependency("erchef", erchefDatabase, erchefUser, "extraction snapshot passed"),
			adminMigrationLiveSourceExtractionSchemaDependency("erchef", erchefDatabase, len(adminMigrationLiveSourceRequiredErchefTables())),
			adminMigrationLiveSourceConnectionDependency("bifrost", bifrostDatabase, bifrostUser, "extraction snapshot passed"),
			adminMigrationLiveSourceExtractionSchemaDependency("bifrost", bifrostDatabase, len(adminMigrationLiveSourceRequiredBifrostTables())),
			{
				Name:       "source_bootstrap",
				Status:     "ok",
				Backend:    "postgres",
				Configured: true,
				Message:    "source bootstrap identity and authorization rows were extracted",
				Details: map[string]string{
					"payload_families":          fmt.Sprintf("%d", len(payloadValues)),
					"visible_organizations":     fmt.Sprintf("%d", len(orgs)),
					"unrelated_bifrost_records": fmt.Sprintf("%d", unrelatedBifrostRecords),
				},
			},
		},
		Findings:      findings,
		PayloadValues: payloadValues,
	}, nil
}

func adminMigrationLiveSourceEstablishSnapshot(ctx context.Context, tx pgx.Tx) (string, string, error) {
	var readOnly, databaseName, databaseUser, snapshot string
	if err := tx.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readOnly); err != nil {
		return "", "", err
	}
	if !adminMigrationLiveSourcePostgresReadOnlyEnabled(readOnly) {
		return "", "", fmt.Errorf("source PostgreSQL extraction transaction is not read-only")
	}
	if err := tx.QueryRow(ctx, "SELECT current_database(), current_user, txid_current_snapshot()::text").Scan(&databaseName, &databaseUser, &snapshot); err != nil {
		return "", "", err
	}
	if strings.TrimSpace(snapshot) == "" {
		return "", "", fmt.Errorf("source PostgreSQL snapshot identity is empty")
	}
	return databaseName, databaseUser, nil
}

func firstAdminMigrationLiveSourceError(primary, fallback error) error {
	if primary != nil {
		return primary
	}
	return fallback
}

func adminMigrationLiveSourceExtractionDatabaseFailure(component, database, message string, schema bool) adminMigrationLiveSourceBootstrapRead {
	code := adminMigrationFindingSourceErchefUnavailable
	name := "source_erchef_postgres"
	if component == "bifrost" {
		code = adminMigrationFindingSourceBifrostUnavailable
		name = "source_bifrost_postgres"
	}
	if schema {
		name = "source_" + component + "_schema"
		if component == "erchef" {
			code = adminMigrationFindingSourceErchefSchemaUnsupported
		} else {
			code = adminMigrationFindingSourceBifrostSchemaUnsupported
		}
	}
	return adminMigrationLiveSourceBootstrapRead{
		Dependencies: []adminMigrationDependency{{Name: name, Status: "error", Backend: "postgres", Configured: true, Message: message, Details: map[string]string{"database": database, "read_only_required": "true"}}},
		Findings:     []adminMigrationFinding{{Severity: "error", Code: code, Family: name, Message: message}},
	}
}

func adminMigrationLiveSourceExtractionSchemaFailure(component, database, user string, missing []string) adminMigrationLiveSourceBootstrapRead {
	result := adminMigrationLiveSourceExtractionDatabaseFailure(component, database, "source "+component+" PostgreSQL schema is missing or cannot inspect required Chef Server tables", true)
	result.Dependencies = append([]adminMigrationDependency{adminMigrationLiveSourceConnectionDependency(component, database, user, "extraction snapshot passed")}, result.Dependencies...)
	result.Dependencies[1].Details["missing_tables"] = adminMigrationLiveSourceBoundedList(missing, 8)
	result.Dependencies[1].Details["missing_count"] = fmt.Sprintf("%d", len(missing))
	return result
}

func adminMigrationLiveSourceExtractionSchemaDependency(component, database string, requiredTables int) adminMigrationDependency {
	return adminMigrationDependency{Name: "source_" + component + "_schema", Status: "ok", Backend: "postgres", Configured: true, Message: "source " + component + " PostgreSQL schema was read through its dedicated connection", Details: map[string]string{"database": database, "required_tables": fmt.Sprintf("%d", requiredTables)}}
}

func adminMigrationLiveSourceExtractionErrorResult(component, database string, err error) adminMigrationLiveSourceBootstrapRead {
	var integrity adminMigrationLiveSourceAuthorizationIntegrityError
	if errors.As(err, &integrity) {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{{Name: "source_authorization", Status: "error", Backend: "postgres", Configured: true, Message: "source authorization data could not be resolved without ambiguity", Details: map[string]string{"database": database}}},
			Findings:     []adminMigrationFinding{{Severity: "error", Code: integrity.Code, Family: "source_authorization", Message: integrity.Error()}},
		}
	}
	return adminMigrationLiveSourceExtractionDatabaseFailure(component, database, "source "+component+" authorization rows could not be read by the configured read-only role", true)
}

func adminMigrationLiveSourcePostgresErrorDependency(message string) adminMigrationDependency {
	return adminMigrationDependency{
		Name:       "source_postgres",
		Status:     "error",
		Backend:    "postgres",
		Configured: true,
		Message:    message,
		Details: map[string]string{
			"read_only_required": "true",
		},
	}
}

func adminMigrationLiveSourcePostgresErrorFinding(message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "error",
		Code:     adminMigrationFindingSourcePostgresUnavailable,
		Family:   "source_postgres",
		Message:  message,
	}
}

// adminMigrationLiveSourceBootstrapBundleFromPayloadValues validates and
// materializes already-read source rows without copying blob sidecars. Tests and
// adapters that need source blob behavior should call
// adminMigrationLiveSourceBundleFromPayloadValues with the real live config.
func adminMigrationLiveSourceBootstrapBundleFromPayloadValues(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) (adminMigrationSourceNormalizeBundle, error) {
	bundle, _, err := adminMigrationLiveSourceBundleFromPayloadValues(context.Background(), adminMigrationLiveSourceConfig{}, payloadValues)
	return bundle, err
}

// adminMigrationLiveSourceBundleFromPayloadValues normalizes live PostgreSQL
// rows into the existing source bundle contract and optionally packages copied
// checksum blobs as sidecar files.
func adminMigrationLiveSourceBundleFromPayloadValues(ctx context.Context, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) (adminMigrationSourceNormalizeBundle, adminMigrationLiveSourceBlobExtraction, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := adminMigrationNormalizeIdentityPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{}, adminMigrationLiveSourceBlobExtraction{}, err
	}
	if err := adminMigrationNormalizeCoreObjectPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{}, adminMigrationLiveSourceBlobExtraction{}, err
	}
	if err := adminMigrationNormalizeCookbookPayloads(payloadValues); err != nil {
		return adminMigrationSourceNormalizeBundle{}, adminMigrationLiveSourceBlobExtraction{}, err
	}
	blobExtraction := adminMigrationLiveSourceExtractBlobSidecars(ctx, cfg, payloadValues)
	if blobExtraction.Dependency.Status == "error" {
		return adminMigrationSourceNormalizeBundle{}, blobExtraction, adminMigrationSourceSemanticError{Code: adminMigrationFindingSourceBlobUnavailable, Message: blobExtraction.Dependency.Message}
	}
	files := make(map[string][]byte, len(blobExtraction.Files))
	for path, data := range blobExtraction.Files {
		files[path] = append([]byte(nil), data...)
	}
	if err := adminMigrationValidateCopiedSourceBlobFiles(files); err != nil {
		return adminMigrationSourceNormalizeBundle{}, blobExtraction, err
	}
	payloads, err := adminMigrationMaterializeSourcePayloadFiles(payloadValues, files)
	if err != nil {
		return adminMigrationSourceNormalizeBundle{}, blobExtraction, err
	}
	artifacts := adminMigrationSourceArtifactsFromSideChannels(adminMigrationLiveSourceCopiedBlobChecksums(files), 0, nil)
	findings := append([]adminMigrationFinding{}, blobExtraction.Findings...)
	findings = append(findings, adminMigrationMissingCopiedSourceBlobFindings(payloadValues, files)...)
	manifest := adminMigrationSourceManifest{
		FormatVersion: adminMigrationChefSourceFormatV1,
		SourceType:    "live_chef_infra_server",
		Payloads:      payloads,
		Artifacts:     artifacts,
		Notes: []string{
			"Generated by live source PostgreSQL extraction.",
			"Contains identity, authorization, core object, cookbook, sandbox, and checksum reference families.",
			"Checksum blob bytes are copied only when the selected source blob adapter can read deterministic local content; otherwise references remain for import/sync validation.",
		},
	}
	return adminMigrationSourceNormalizeBundle{
		Manifest:      manifest,
		Files:         files,
		Inventory:     adminMigrationInventoryFromSourceManifest(manifest),
		Findings:      findings,
		SourceType:    manifest.SourceType,
		FormatVersion: manifest.FormatVersion,
	}, blobExtraction, nil
}

// adminMigrationLiveSourceExtractBlobSidecars copies checksum-addressed source
// blob bytes when a deterministic local filesystem adapter is configured, or
// records reference-only evidence for import/sync validation.
func adminMigrationLiveSourceExtractBlobSidecars(ctx context.Context, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) adminMigrationLiveSourceBlobExtraction {
	if ctx == nil {
		ctx = context.Background()
	}
	referenced := adminMigrationReferencedSourceChecksums(payloadValues)
	backend := adminMigrationLiveSourceBlobMode(cfg)
	configured := backend != "unconfigured"
	details := map[string]string{
		"copy_mode":            cfg.ExtractionMode,
		"referenced_checksums": fmt.Sprintf("%d", len(referenced)),
	}
	if len(referenced) == 0 {
		return adminMigrationLiveSourceBlobExtraction{
			Dependency: adminMigrationDependency{
				Name:       "source_blob",
				Status:     "skipped",
				Backend:    backend,
				Configured: configured,
				Message:    "live source metadata did not reference checksum blobs",
				Details:    details,
			},
			Files: map[string][]byte{},
		}
	}
	if !cfg.CopyBlobs {
		message := "source checksum blobs are recorded by reference; import or sync must validate provider reachability before cutover"
		if cfg.ReferenceBlobs {
			message = "source checksum blobs are intentionally reference-only; import or sync must validate provider reachability before cutover"
		}
		return adminMigrationLiveSourceBlobExtraction{
			Dependency: adminMigrationDependency{
				Name:       "source_blob",
				Status:     "warning",
				Backend:    backend,
				Configured: configured,
				Message:    message,
				Details:    details,
			},
			Files: map[string][]byte{},
		}
	}
	if strings.TrimSpace(cfg.BookshelfRoot) == "" {
		return adminMigrationLiveSourceBlobExtraction{
			Dependency: adminMigrationDependency{
				Name:       "source_blob",
				Status:     "error",
				Backend:    backend,
				Configured: configured,
				Message:    "source blob bytes could not be copied because no deterministic local Bookshelf root was configured",
				Details:    details,
			},
			Findings: []adminMigrationFinding{adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobUnavailable, "source blob provider cannot provide deterministic local checksum bytes")},
			Files:    map[string][]byte{},
		}
	}
	files := map[string][]byte{}
	var findings []adminMigrationFinding
	for _, reference := range adminMigrationLiveSourceBlobReferences(payloadValues) {
		checksum := reference.Checksum
		if err := ctx.Err(); err != nil {
			findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceExtractionInterrupted, "live source extraction was interrupted while copying checksum blobs"))
			break
		}
		body, err := adminMigrationLiveSourceReadBookshelfBlob(cfg.BookshelfRoot, reference.OrganizationID, checksum)
		if err != nil {
			if errors.Is(err, errAdminMigrationLiveSourceBookshelfBlobNotFound) {
				findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobMissing, "source metadata references checksum "+checksum+" but the source blob provider did not contain it"))
				continue
			}
			if errors.Is(err, errAdminMigrationLiveSourceBookshelfBlobChecksumMismatch) {
				findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobChecksumMismatch, "source blob content did not match checksum "+checksum))
				continue
			}
			findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobUnavailable, "source blob provider could not read referenced checksum content"))
			continue
		}
		if adminMigrationMD5Hex(body) != checksum {
			findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobChecksumMismatch, "source blob content did not match checksum "+checksum))
			continue
		}
		files[pathpkg.Join("blobs", "checksums", checksum)] = append([]byte(nil), body...)
	}
	if len(findings) > 0 {
		return adminMigrationLiveSourceBlobExtraction{
			Dependency: adminMigrationDependency{
				Name:       "source_blob",
				Status:     "error",
				Backend:    backend,
				Configured: true,
				Message:    "one or more referenced source checksum blobs could not be copied safely",
				Details:    details,
			},
			Findings: findings,
			Files:    files,
		}
	}
	details["copied_checksums"] = fmt.Sprintf("%d", len(files))
	return adminMigrationLiveSourceBlobExtraction{
		Dependency: adminMigrationDependency{
			Name:       "source_blob",
			Status:     "ok",
			Backend:    backend,
			Configured: true,
			Message:    "referenced source checksum blobs were copied into the normalized source bundle",
			Details:    details,
		},
		Files: files,
	}
}

type adminMigrationLiveSourceBlobReference struct {
	OrganizationID string
	Checksum       string
}

// Preserve the source orgs.id (normalized as guid) for every org/checksum pair.
// Native Bookshelf objects are scoped by organization even when their content
// is shared; the resulting bundle still stores only one sidecar per checksum.
func adminMigrationLiveSourceBlobReferences(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) []adminMigrationLiveSourceBlobReference {
	keys := map[adminMigrationSourcePayloadKey]struct{}{}
	for key := range payloadValues {
		if key.Family == "checksum_references" {
			keys[key] = struct{}{}
		}
	}
	var references []adminMigrationLiveSourceBlobReference
	for _, key := range adminMigrationSortedSourcePayloadKeys(keys) {
		orgID := ""
		for _, raw := range payloadValues[adminMigrationSourcePayloadKey{Organization: key.Organization, Family: "organizations"}] {
			if object, err := adminMigrationDecodeSourceObject(raw); err == nil {
				orgID = adminMigrationSourceString(object, "guid")
			}
		}
		checksums := adminMigrationReferencedSourceChecksums(map[adminMigrationSourcePayloadKey][]json.RawMessage{key: payloadValues[key]})
		for _, checksum := range adminMigrationSortedMapKeys(checksums) {
			references = append(references, adminMigrationLiveSourceBlobReference{OrganizationID: orgID, Checksum: checksum})
		}
	}
	return references
}

// adminMigrationLiveSourceReadBookshelfBlob reads native Bookshelf filesystem
// layouts without treating the source as OpenCook's flat filesystem backend.
// chef_s3:make_key/2 scopes the key by orgs.id; bksw_io_names:entry_path_sha/1
// hashes the percent-encoded key into four two-character directory levels and
// uses that encoded key as the filename. bksw_io prefixes bytes with magic + MD5.
func adminMigrationLiveSourceReadBookshelfBlob(root, orgID, checksum string) ([]byte, error) {
	root = strings.TrimSpace(root)
	checksum = strings.ToLower(strings.TrimSpace(checksum))
	digest, err := hex.DecodeString(checksum)
	if root == "" || err != nil || len(digest) != 16 {
		return nil, fmt.Errorf("%w: invalid checksum key", errAdminMigrationLiveSourceBookshelfBlobChecksumMismatch)
	}
	var candidates [][]string
	if orgID = strings.TrimSpace(orgID); orgID != "" {
		key := url.PathEscape("organization-" + orgID + "/checksum-" + checksum)
		shardDigest := sha1.Sum([]byte(key)) // #nosec G505 -- native Bookshelf path compatibility requires SHA-1.
		shard := hex.EncodeToString(shardDigest[:])
		shardedParts := []string{shard[0:2], shard[2:4], shard[4:6], shard[6:8], key}
		candidates = append(candidates,
			append([]string{root, "bookshelf"}, shardedParts...),
			append([]string{root}, shardedParts...),
		)
	}
	candidates = append(candidates, [][]string{
		{root, "bookshelf", checksum},
		{root, checksum},
	}...)
	seen := map[string]struct{}{}
	for _, parts := range candidates {
		candidate := filepath.Join(parts...)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		data, err := os.ReadFile(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		return adminMigrationLiveSourceDecodeBookshelfBlob(data, digest)
	}
	return nil, errAdminMigrationLiveSourceBookshelfBlobNotFound
}

func adminMigrationLiveSourceDecodeBookshelfBlob(data, expectedDigest []byte) ([]byte, error) {
	const bookshelfHeaderSize = 18
	if len(data) < 2 || data[0] != 0xb0 || data[1] != 0x0c {
		return data, nil
	}
	// Flat checksum files can contain arbitrary binary data, including the
	// Bookshelf magic. A matching full-file digest takes precedence over decoding.
	if adminMigrationMD5Hex(data) == hex.EncodeToString(expectedDigest) {
		return data, nil
	}
	if len(data) < bookshelfHeaderSize || len(expectedDigest) != 16 || !bytes.Equal(data[2:bookshelfHeaderSize], expectedDigest) {
		return nil, errAdminMigrationLiveSourceBookshelfBlobChecksumMismatch
	}
	return data[bookshelfHeaderSize:], nil
}

func adminMigrationLiveSourceBlobFinding(severity, code, message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity: severity,
		Code:     code,
		Family:   "source_blob",
		Message:  message,
	}
}

func adminMigrationLiveSourceCopiedBlobChecksums(files map[string][]byte) map[string]struct{} {
	checksums := map[string]struct{}{}
	for path := range files {
		if checksum := adminMigrationCopiedSourceBlobChecksum(path); checksum != "" {
			checksums[checksum] = struct{}{}
		}
	}
	return checksums
}

// adminMigrationLiveSourceInitializeBootstrapPayloads creates empty covered
// families up front so import/sync can distinguish "zero rows" from "not read."
func adminMigrationLiveSourceInitializeBootstrapPayloads(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, orgs []string) {
	for _, family := range []string{"users", "user_acls", "user_keys", "server_admin_memberships"} {
		payloadValues[adminMigrationSourcePayloadKey{Family: family}] = []json.RawMessage{}
	}
	for _, orgName := range orgs {
		for _, family := range adminMigrationLiveSourceCoveredOrgFamilies() {
			payloadValues[adminMigrationSourcePayloadKey{Organization: orgName, Family: family}] = []json.RawMessage{}
		}
	}
}

// adminMigrationLiveSourceCoveredOrgFamilies records the payload families this
// extractor intentionally covers so empty families remain explicit in bundles.
func adminMigrationLiveSourceCoveredOrgFamilies() []string {
	return []string{
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
		"cookbook_artifacts",
	}
}

func adminMigrationLiveSourceReadBootstrapOrganizations(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig) ([]string, error) {
	rows, err := tx.Query(ctx, `
SELECT name
FROM orgs
WHERE name <> '' AND ($1::text = '' OR name = $1)
ORDER BY name`, strings.TrimSpace(cfg.Organization))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orgs []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		orgs = append(orgs, name)
	}
	return orgs, rows.Err()
}

// adminMigrationLiveSourceReadBootstrapRows keeps every query read-only and
// appends raw rows into the normalized payload family buckets.
func adminMigrationLiveSourceReadBootstrapRows(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, orgs []string, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	if err := adminMigrationLiveSourceReadUsers(ctx, tx, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadUserKeys(ctx, tx, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadServerAdmins(ctx, tx, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadOrganizationPayloads(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadClients(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadClientKeys(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadGroups(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadContainers(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadCoreObjects(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadCookbooks(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	for _, orgName := range orgs {
		if len(payloadValues[adminMigrationSourcePayloadKey{Organization: orgName, Family: "organizations"}]) == 0 {
			return fmt.Errorf("source organization %q disappeared during bootstrap extraction", orgName)
		}
	}
	return nil
}

func adminMigrationLiveSourceReadUsers(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendQuery(ctx, tx, payloadValues, adminMigrationSourcePayloadKey{Family: "users"}, `
SELECT username, username AS name, COALESCE(email, '') AS email, serialized_object
FROM users
ORDER BY username`)
}

func adminMigrationLiveSourceReadUserKeys(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendQuery(ctx, tx, payloadValues, adminMigrationSourcePayloadKey{Family: "user_keys"}, `
SELECT u.username, k.key_name, k.public_key,
       CASE WHEN k.expires_at::text = 'infinity' THEN 'infinity'
            ELSE to_char(k.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
       END AS expiration_date
FROM users u
JOIN keys k ON k.id = u.id
ORDER BY u.username, k.key_name`)
}

func adminMigrationLiveSourceReadServerAdmins(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendQuery(ctx, tx, payloadValues, adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}, `
SELECT username AS actor, 'user' AS type
FROM users
WHERE admin = true
ORDER BY username`)
}

func adminMigrationLiveSourceReadOrganizationPayloads(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "organizations", `
SELECT name AS orgname, name, full_name, 'Business' AS org_type, id AS guid
FROM orgs
WHERE name <> '' AND ($1::text = '' OR name = $1)
ORDER BY name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadClients(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "clients", `
SELECT o.name AS orgname, c.name, c.name AS clientname, c.validator, c.admin, COALESCE(c.public_key, '') AS public_key
FROM clients c
JOIN orgs o ON o.id = c.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, c.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadClientKeys(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "client_keys", `
SELECT o.name AS orgname, c.name AS client, c.name AS clientname, k.key_name, k.public_key,
       CASE WHEN k.expires_at::text = 'infinity' THEN 'infinity'
            ELSE to_char(k.expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
       END AS expiration_date
FROM clients c
JOIN orgs o ON o.id = c.org_id
JOIN keys k ON k.id = c.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, c.name, k.key_name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadGroups(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "groups", `
SELECT o.name AS orgname, g.name, g.name AS groupname,
       ARRAY[]::text[] AS users, ARRAY[]::text[] AS clients, ARRAY[]::text[] AS groups, ARRAY[]::text[] AS actors
FROM groups g
JOIN orgs o ON o.id = g.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, g.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadContainers(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "containers", `
SELECT o.name AS orgname, c.name, c.name AS containername, c.name AS containerpath
FROM containers c
JOIN orgs o ON o.id = c.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, c.name`, strings.TrimSpace(cfg.Organization))
}

// adminMigrationLiveSourceReadCoreObjects extracts Chef object metadata from
// the source SQL tables that already back Chef-facing APIs. Serialized payloads
// are merged as compatibility JSON, while selected canonical columns override
// identity fields so OpenCook import receives stable names and relationships.
func adminMigrationLiveSourceReadCoreObjects(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	if err := adminMigrationLiveSourceReadNodes(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadEnvironments(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadRoles(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadDataBags(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadDataBagItems(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadPolicyRevisions(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadPolicyGroups(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadPolicyAssignments(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadSandboxes(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadChecksumReferences(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	return nil
}

func adminMigrationLiveSourceReadNodes(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "nodes", `
SELECT o.name AS orgname, n.serialized_object AS raw_payload, n.name, n.environment AS chef_environment,
       COALESCE(n.policy_name, '') AS policy_name, COALESCE(n.policy_group, '') AS policy_group
FROM nodes n
JOIN orgs o ON o.id = n.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, n.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadEnvironments(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "environments", `
SELECT o.name AS orgname, e.serialized_object AS raw_payload, e.name
FROM environments e
JOIN orgs o ON o.id = e.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, e.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadRoles(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "roles", `
SELECT o.name AS orgname, r.serialized_object AS raw_payload, r.name
FROM roles r
JOIN orgs o ON o.id = r.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, r.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadDataBags(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "data_bags", `
SELECT o.name AS orgname, d.name
FROM data_bags d
JOIN orgs o ON o.id = d.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, d.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadDataBagItems(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "data_bag_items", `
SELECT o.name AS orgname, i.serialized_object AS raw_payload, i.data_bag_name AS bag, i.item_name AS id
FROM data_bag_items i
JOIN orgs o ON o.id = i.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, i.data_bag_name, i.item_name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadPolicyRevisions(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "policy_revisions", `
SELECT o.name AS orgname, pr.serialized_object AS raw_payload, pr.name, lower(pr.revision_id) AS revision_id
FROM policy_revisions pr
JOIN orgs o ON o.id = pr.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, pr.name, pr.revision_id`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadPolicyGroups(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "policy_groups", `
SELECT o.name AS orgname, pg.serialized_object AS raw_payload, pg.name
FROM policy_groups pg
JOIN orgs o ON o.id = pg.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, pg.name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadPolicyAssignments(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "policy_assignments", `
SELECT o.name AS orgname, a.policy_group_name AS "group", a.policy_revision_name AS policy,
       lower(a.policy_revision_revision_id) AS revision_id
FROM policy_revisions_policy_groups_association a
JOIN orgs o ON o.id = a.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, a.policy_group_name, a.policy_revision_name`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadSandboxes(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "sandboxes", `
SELECT o.name AS orgname, s.sandbox_id AS id, s.sandbox_id AS sandbox_id, true AS completed,
       array_agg(lower(s.checksum) ORDER BY lower(s.checksum)) AS checksums
FROM sandboxed_checksums s
JOIN orgs o ON o.id = s.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
GROUP BY o.name, s.sandbox_id
ORDER BY o.name, s.sandbox_id`, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadChecksumReferences(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	if err := adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "checksum_references", `
SELECT o.name AS orgname, lower(c.checksum) AS checksum, 'checksums' AS family
FROM checksums c
JOIN orgs o ON o.id = c.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, c.checksum`, strings.TrimSpace(cfg.Organization)); err != nil {
		return err
	}
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "checksum_references", `
SELECT o.name AS orgname, lower(s.checksum) AS checksum, 'sandboxes' AS family, s.sandbox_id AS id
FROM sandboxed_checksums s
JOIN orgs o ON o.id = s.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, s.sandbox_id, s.checksum`, strings.TrimSpace(cfg.Organization))
}

// adminMigrationLiveSourceReadCookbooks extracts cookbook metadata and checksum
// references from erchef without reading blob bytes from PostgreSQL.
func adminMigrationLiveSourceReadCookbooks(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	if err := adminMigrationLiveSourceReadCookbookVersions(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	return adminMigrationLiveSourceReadCookbookArtifacts(ctx, tx, cfg, payloadValues)
}

const adminMigrationLiveSourceCookbookVersionsQuery = `
SELECT o.name AS orgname,
       cv.serialized_object AS raw_payload,
       cv.metadata AS metadata_payload,
       cb.name AS _source_cookbook_name,
       cb.name AS cookbook_name,
       (cv.major::text || '.' || cv.minor::text || '.' || cv.patch::text) AS _source_cookbook_version,
       (cv.major::text || '.' || cv.minor::text || '.' || cv.patch::text) AS version,
       COALESCE(ARRAY_REMOVE(ARRAY_AGG(lower(cvc.checksum) ORDER BY lower(cvc.checksum)), NULL), ARRAY[]::text[]) AS checksums
FROM cookbook_versions cv
JOIN cookbooks cb ON cb.id = cv.cookbook_id
JOIN orgs o ON o.id = cb.org_id
LEFT JOIN cookbook_version_checksums cvc ON cvc.cookbook_version_id = cv.id AND cvc.org_id = cb.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
GROUP BY o.name, cv.id, cv.serialized_object, cv.metadata, cb.name, cv.major, cv.minor, cv.patch
ORDER BY o.name, cb.name, cv.major, cv.minor, cv.patch`

func adminMigrationLiveSourceReadCookbookVersions(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "cookbook_versions", adminMigrationLiveSourceCookbookVersionsQuery, strings.TrimSpace(cfg.Organization))
}

func adminMigrationLiveSourceReadCookbookArtifacts(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "cookbook_artifacts", `
SELECT o.name AS orgname,
       cav.serialized_object AS raw_payload,
       cav.metadata AS metadata_payload,
       ca.name AS name,
       lower(cav.identifier) AS identifier,
       COALESCE(ARRAY_REMOVE(ARRAY_AGG(lower(cavc.checksum) ORDER BY lower(cavc.checksum)), NULL), ARRAY[]::text[]) AS checksums
FROM cookbook_artifact_versions cav
JOIN cookbook_artifacts ca ON ca.id = cav.cookbook_artifact_id
JOIN orgs o ON o.id = ca.org_id
LEFT JOIN cookbook_artifact_version_checksums cavc ON cavc.cookbook_artifact_version_id = cav.id AND cavc.org_id = ca.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
GROUP BY o.name, ca.name, cav.id, cav.serialized_object, cav.metadata, cav.identifier
ORDER BY o.name, ca.name, cav.identifier`, strings.TrimSpace(cfg.Organization))
}

type adminMigrationLiveSourceACLRow struct {
	OrgName        string
	Resource       string
	Permission     string
	AuthorizeeType string
	Authorizee     string
}

func adminMigrationLiveSourceACLObjects(rows []adminMigrationLiveSourceACLRow) []map[string]any {
	byIdentity := map[string]map[string]map[string][]string{}
	resourceByIdentity := map[string]string{}
	orgByIdentity := map[string]string{}
	for _, row := range rows {
		resource := strings.TrimSpace(row.Resource)
		permission := strings.TrimSpace(row.Permission)
		authorizee := strings.TrimSpace(row.Authorizee)
		if resource == "" {
			continue
		}
		identity := strings.TrimSpace(row.OrgName) + "|" + resource
		if byIdentity[identity] == nil {
			byIdentity[identity] = map[string]map[string][]string{}
		}
		if byIdentity[identity][permission] == nil {
			byIdentity[identity][permission] = map[string][]string{"actors": []string{}, "groups": []string{}}
		}
		resourceByIdentity[identity] = resource
		if row.OrgName != "" {
			orgByIdentity[identity] = row.OrgName
		}
		if permission == "" || authorizee == "" {
			continue
		}
		key := "actors"
		if row.AuthorizeeType == "group" {
			key = "groups"
		}
		byIdentity[identity][permission][key] = append(byIdentity[identity][permission][key], authorizee)
	}
	objects := make([]map[string]any, 0, len(byIdentity))
	for _, identity := range adminMigrationSortedMapKeys(byIdentity) {
		object := map[string]any{"resource": resourceByIdentity[identity]}
		if orgName := orgByIdentity[identity]; orgName != "" {
			object["orgname"] = orgName
		}
		for _, permission := range []string{"create", "read", "update", "delete", "grant"} {
			actors := adminMigrationUniqueSortedStrings(byIdentity[identity][permission]["actors"])
			groups := adminMigrationUniqueSortedStrings(byIdentity[identity][permission]["groups"])
			object[permission] = map[string]any{"actors": actors, "groups": groups}
		}
		objects = append(objects, object)
	}
	return objects
}

func adminMigrationLiveSourceAppendQuery(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, key adminMigrationSourcePayloadKey, query string, args ...any) error {
	rows, err := adminMigrationLiveSourceQueryObjects(ctx, tx, query, args...)
	if err != nil {
		return adminMigrationLiveSourceWrapPayloadReadError(key.Family, err)
	}
	for _, row := range rows {
		adminMigrationLiveSourceAppendObject(payloadValues, key, row)
	}
	return nil
}

func adminMigrationLiveSourceAppendOrgQuery(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, family, query string, args ...any) error {
	rows, err := adminMigrationLiveSourceQueryObjects(ctx, tx, query, args...)
	if err != nil {
		return adminMigrationLiveSourceWrapPayloadReadError(family, err)
	}
	for _, row := range rows {
		orgName := adminMigrationSourceString(row, "orgname")
		if orgName == "" {
			continue
		}
		delete(row, "orgname")
		adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Organization: orgName, Family: family}, row)
	}
	return nil
}

// adminMigrationLiveSourcePayloadReadError identifies the safe normalized
// family that failed without exposing source SQL, driver text, or credentials.
type adminMigrationLiveSourcePayloadReadError struct {
	Family string
	Err    error
}

func (e *adminMigrationLiveSourcePayloadReadError) Error() string {
	return "source payload family " + e.Family + " could not be read"
}

func (e *adminMigrationLiveSourcePayloadReadError) Unwrap() error {
	return e.Err
}

func adminMigrationLiveSourceWrapPayloadReadError(family string, err error) error {
	if err == nil {
		return nil
	}
	return &adminMigrationLiveSourcePayloadReadError{Family: strings.TrimSpace(family), Err: err}
}

func adminMigrationLiveSourceQueryObjects(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]map[string]any, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	out := []map[string]any{}
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		// JSON/JSONB values from native columns must not pass through pgx's
		// default interface decoder, which converts arbitrary numbers to float64.
		rawValues := rows.RawValues()
		for i, field := range fields {
			if field.DataTypeOID == pgtype.JSONOID || field.DataTypeOID == pgtype.JSONBOID {
				raw := rawValues[i]
				if field.DataTypeOID == pgtype.JSONBOID && field.Format == pgx.BinaryFormatCode && len(raw) > 0 {
					raw = raw[1:]
				}
				values[i] = string(raw)
			}
		}
		object, err := adminMigrationLiveSourceRowMap(fields, values)
		if err != nil {
			return nil, err
		}
		out = append(out, object)
	}
	return out, rows.Err()
}

type adminMigrationLiveSourcePayloadDecodeError struct{ Identity string }

func (e *adminMigrationLiveSourcePayloadDecodeError) Error() string {
	return "malformed serialized source object"
}

func adminMigrationLiveSourceRowMap(fields []pgconn.FieldDescription, values []any) (map[string]any, error) {
	object := map[string]any{}
	identity := map[string]any{}
	for i, field := range fields {
		if i >= len(values) {
			return nil, &adminMigrationLiveSourcePayloadDecodeError{}
		}
		switch field.Name {
		case "orgname", "name", "username", "id", "cookbook_name", "version", "identifier", "revision_id":
			identity[field.Name] = values[i]
		}
	}
	identityBytes, _ := json.Marshal(identity)
	digest := sha256.Sum256(identityBytes)
	malformed := func() (map[string]any, error) {
		return nil, &adminMigrationLiveSourcePayloadDecodeError{Identity: hex.EncodeToString(digest[:])}
	}
	// SQL identity columns are authoritative regardless of their column order.
	for i, field := range fields {
		switch field.Name {
		case "serialized_object", "raw_payload", "metadata_payload":
			continue
		}
		value, err := adminMigrationLiveSourceJSONValue(values[i])
		if err != nil {
			return malformed()
		}
		object[field.Name] = value
	}
	for i, field := range fields {
		switch field.Name {
		case "serialized_object", "raw_payload", "metadata_payload":
			value, err := adminMigrationLiveSourceJSONValue(values[i])
			if err != nil {
				return malformed()
			}
			switch field.Name {
			case "serialized_object":
				err = adminMigrationLiveSourceMergeSerializedObject(object, value)
			case "raw_payload":
				err = adminMigrationLiveSourceMergeRawPayload(object, value)
			case "metadata_payload":
				err = adminMigrationLiveSourceMergeMetadataPayload(object, value)
			}
			if err != nil {
				return malformed()
			}
		}
	}
	return object, nil
}

func adminMigrationLiveSourceJSONValue(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return "", nil
	case []byte:
		return adminMigrationLiveSourceStringFromBytes(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339), nil
	case []string:
		return append([]string(nil), typed...), nil
	default:
		return typed, nil
	}
}

// Decode compressed payloads strictly: gzip headers, trailers, CRCs, and
// concatenated members are checked before parsing exactly one JSON object.
func adminMigrationLiveSourceStringFromBytes(data []byte) (string, error) {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return "", err
		}
		decoded, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return "", err
		}
		if closeErr != nil {
			return "", closeErr
		}
		return string(decoded), nil
	}
	return string(data), nil
}

func adminMigrationLiveSourcePayloadObject(value any, optional bool) (map[string]any, error) {
	raw, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("serialized payload must be JSON text")
	}
	if optional && raw == "" {
		return nil, nil
	}
	return adminMigrationDecodeSourceObject(json.RawMessage(raw))
}

// Only absent user-profile data is optional; malformed nonempty data blocks extraction.
func adminMigrationLiveSourceMergeSerializedObject(object map[string]any, value any) error {
	decoded, err := adminMigrationLiveSourcePayloadObject(value, true)
	if err != nil {
		return err
	}
	for _, field := range []string{"display_name", "first_name", "last_name"} {
		if _, exists := object[field]; exists {
			continue
		}
		if value, ok := decoded[field].(string); ok && strings.TrimSpace(value) != "" {
			object[field] = strings.TrimSpace(value)
		}
	}
	return nil
}
func adminMigrationLiveSourceMergeRawPayload(object map[string]any, value any) error {
	decoded, err := adminMigrationLiveSourcePayloadObject(value, false)
	if err != nil {
		return err
	}
	for key, value := range decoded {
		if _, exists := object[key]; !exists {
			object[key] = value
		}
	}
	return nil
}
func adminMigrationLiveSourceMergeMetadataPayload(object map[string]any, value any) error {
	decoded, err := adminMigrationLiveSourcePayloadObject(value, false)
	if err != nil {
		return err
	}
	object["metadata"] = decoded
	return nil
}

func adminMigrationLiveSourceAppendObject(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, key adminMigrationSourcePayloadKey, object map[string]any) {
	data, err := json.Marshal(object)
	if err != nil {
		return
	}
	payloadValues[key] = append(payloadValues[key], json.RawMessage(data))
}
