package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	pathpkg "path"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/blob"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type adminMigrationPGXLiveSourceBootstrapExtractor struct{}

// ExtractBootstrap reads implemented source PostgreSQL families in a read-only
// transaction and emits the existing normalized source bundle format that
// import/sync already validate.
func (adminMigrationPGXLiveSourceBootstrapExtractor) ExtractBootstrap(ctx context.Context, cfg adminMigrationLiveSourceConfig) adminMigrationLiveSourceExtractorResult {
	read, err := adminMigrationReadLiveSourceBootstrapPayloads(ctx, cfg)
	if err != nil {
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
				Message: "extracted identity, authorization, core object, cookbook, and checksum-reference payloads from source PostgreSQL using read-only queries",
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
	probeCtx, cancel := context.WithTimeout(ctx, adminMigrationLiveSourcePostgresProbeTimeout)
	defer cancel()

	conn, err := pgx.Connect(probeCtx, cfg.PostgresDSN)
	if err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL could not be reached")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL could not be reached or authenticated")},
		}, err
	}
	defer conn.Close(probeCtx)

	tx, err := conn.BeginTx(probeCtx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL read-only transaction could not be opened")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL did not allow a read-only migration extraction transaction")},
		}, err
	}
	defer tx.Rollback(probeCtx)

	var readOnly string
	if err := tx.QueryRow(probeCtx, "SHOW transaction_read_only").Scan(&readOnly); err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL read-only posture could not be verified")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL read-only posture could not be verified")},
		}, err
	}
	if !adminMigrationLiveSourcePostgresReadOnlyEnabled(readOnly) {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL extraction was not running in a read-only transaction")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL extraction was not running in a read-only transaction")},
		}, fmt.Errorf("source PostgreSQL extraction was not read-only")
	}

	var databaseName, databaseUser string
	if err := tx.QueryRow(probeCtx, "SELECT current_database(), current_user").Scan(&databaseName, &databaseUser); err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL identity could not be read")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourcePostgresErrorFinding("source PostgreSQL identity could not be read")},
		}, err
	}

	payloadValues := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
	orgs, err := adminMigrationLiveSourceReadBootstrapOrganizations(probeCtx, tx, cfg)
	if err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source organizations could not be read")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourceSchemaErrorFinding("source organizations could not be read by the configured read-only role")},
		}, err
	}
	adminMigrationLiveSourceInitializeBootstrapPayloads(payloadValues, orgs)
	if len(orgs) == 0 {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source PostgreSQL exposes no organizations")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourceSchemaErrorFinding("source PostgreSQL did not expose any organizations to the configured read-only role")},
		}, fmt.Errorf("source organizations missing")
	}
	if err := adminMigrationLiveSourceReadBootstrapRows(probeCtx, tx, cfg, orgs, payloadValues); err != nil {
		return adminMigrationLiveSourceBootstrapRead{
			Dependencies: []adminMigrationDependency{adminMigrationLiveSourcePostgresErrorDependency("source bootstrap rows could not be read")},
			Findings:     []adminMigrationFinding{adminMigrationLiveSourceSchemaErrorFinding("source bootstrap identity and authorization rows could not be read by the configured read-only role")},
		}, err
	}

	return adminMigrationLiveSourceBootstrapRead{
		Dependencies: []adminMigrationDependency{
			{
				Name:       "source_postgres",
				Status:     "ok",
				Backend:    "postgres",
				Configured: true,
				Message:    "source PostgreSQL bootstrap extraction used a read-only transaction",
				Details: map[string]string{
					"database":              adminMigrationLiveSourceSafeDetail(databaseName),
					"user":                  adminMigrationLiveSourceSafeDetail(databaseUser),
					"read_only":             "true",
					"visible_organizations": fmt.Sprintf("%d", len(orgs)),
				},
			},
			{
				Name:       "source_bootstrap",
				Status:     "ok",
				Backend:    "postgres",
				Configured: true,
				Message:    "source bootstrap identity and authorization rows were extracted",
				Details: map[string]string{
					"payload_families": fmt.Sprintf("%d", len(payloadValues)),
				},
			},
		},
		PayloadValues: payloadValues,
	}, nil
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

func adminMigrationLiveSourceSchemaErrorFinding(message string) adminMigrationFinding {
	return adminMigrationFinding{
		Severity: "error",
		Code:     adminMigrationFindingSourceSchemaUnsupported,
		Family:   "source_bootstrap",
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
	store, err := blob.NewFileStore(cfg.BookshelfRoot)
	if err != nil {
		return adminMigrationLiveSourceBlobExtraction{
			Dependency: adminMigrationDependency{
				Name:       "source_blob",
				Status:     "error",
				Backend:    backend,
				Configured: true,
				Message:    "source blob bytes could not be copied from the configured local Bookshelf root",
				Details:    details,
			},
			Findings: []adminMigrationFinding{adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobUnavailable, "source blob provider could not be opened for deterministic checksum reads")},
			Files:    map[string][]byte{},
		}
	}
	files := map[string][]byte{}
	var findings []adminMigrationFinding
	for _, checksum := range adminMigrationSortedMapKeys(referenced) {
		if err := ctx.Err(); err != nil {
			findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceExtractionInterrupted, "live source extraction was interrupted while copying checksum blobs"))
			break
		}
		body, err := store.Get(ctx, checksum)
		if err != nil {
			if err == blob.ErrNotFound {
				findings = append(findings, adminMigrationLiveSourceBlobFinding("error", adminMigrationFindingSourceBlobMissing, "source metadata references checksum "+checksum+" but the source blob provider did not contain it"))
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
	if err := adminMigrationLiveSourceReadUserACLs(ctx, tx, payloadValues); err != nil {
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
	if err := adminMigrationLiveSourceReadGroupMemberships(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadContainers(ctx, tx, cfg, payloadValues); err != nil {
		return err
	}
	if err := adminMigrationLiveSourceReadOrgACLs(ctx, tx, cfg, payloadValues); err != nil {
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
            ELSE to_char(k.expires_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
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
            ELSE to_char(k.expires_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
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

func adminMigrationLiveSourceReadGroupMemberships(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	if err := adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "group_memberships", `
SELECT o.name AS orgname, g.name AS "group",
       CASE WHEN u.username IS NOT NULL THEN 'user' ELSE 'client' END AS type,
       COALESCE(u.username, c.name, child.authz_id) AS actor
FROM groups g
JOIN orgs o ON o.id = g.org_id
JOIN auth_group parent_group ON parent_group.authz_id = g.authz_id
JOIN group_actor_relations rel ON rel.parent = parent_group.id
JOIN auth_actor child ON child.id = rel.child
LEFT JOIN users u ON u.authz_id = child.authz_id
LEFT JOIN clients c ON c.authz_id = child.authz_id AND c.org_id = g.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, g.name, type, actor`, strings.TrimSpace(cfg.Organization)); err != nil {
		return err
	}
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "group_memberships", `
SELECT o.name AS orgname, parent_group.name AS "group", 'group' AS type, child_group.name AS actor
FROM groups parent_group
JOIN orgs o ON o.id = parent_group.org_id
JOIN auth_group parent_auth ON parent_auth.authz_id = parent_group.authz_id
JOIN group_group_relations rel ON rel.parent = parent_auth.id
JOIN auth_group child_auth ON child_auth.id = rel.child
JOIN groups child_group ON child_group.authz_id = child_auth.authz_id AND child_group.org_id = parent_group.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY o.name, parent_group.name, child_group.name`, strings.TrimSpace(cfg.Organization))
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
	return adminMigrationLiveSourceReadCoreObjectACLs(ctx, tx, cfg, payloadValues)
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

func adminMigrationLiveSourceReadCookbookVersions(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	return adminMigrationLiveSourceAppendOrgQuery(ctx, tx, payloadValues, "cookbook_versions", `
SELECT o.name AS orgname,
       cv.serialized_object AS raw_payload,
       cv.metadata AS metadata_payload,
       cv.name AS _source_cookbook_name,
       cv.name AS cookbook_name,
       (cv.major::text || '.' || cv.minor::text || '.' || cv.patch::text) AS _source_cookbook_version,
       (cv.major::text || '.' || cv.minor::text || '.' || cv.patch::text) AS version,
       COALESCE(ARRAY_REMOVE(ARRAY_AGG(lower(cvc.checksum) ORDER BY lower(cvc.checksum)), NULL), ARRAY[]::text[]) AS checksums
FROM cookbook_versions cv
JOIN orgs o ON o.id = cv.org_id
LEFT JOIN cookbook_version_checksums cvc ON cvc.cookbook_version_id = cv.id AND cvc.org_id = cv.org_id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
GROUP BY o.name, cv.id, cv.serialized_object, cv.metadata, cv.name, cv.major, cv.minor, cv.patch
ORDER BY o.name, cv.name, cv.major, cv.minor, cv.patch`, strings.TrimSpace(cfg.Organization))
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

func adminMigrationLiveSourceReadCoreObjectACLs(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	rows, err := adminMigrationLiveSourceQueryACLRows(ctx, tx, `
WITH core_objects AS (
	SELECT o.name AS orgname, o.id AS org_id, n.authz_id, 'node:' || n.name AS resource
	FROM nodes n JOIN orgs o ON o.id = n.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
	UNION ALL
	SELECT o.name AS orgname, o.id AS org_id, e.authz_id, 'environment:' || e.name AS resource
	FROM environments e JOIN orgs o ON o.id = e.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
	UNION ALL
	SELECT o.name AS orgname, o.id AS org_id, r.authz_id, 'role:' || r.name AS resource
	FROM roles r JOIN orgs o ON o.id = r.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
	UNION ALL
	SELECT o.name AS orgname, o.id AS org_id, d.authz_id, 'data_bag:' || d.name AS resource
	FROM data_bags d JOIN orgs o ON o.id = d.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
	UNION ALL
	SELECT o.name AS orgname, o.id AS org_id, p.authz_id, 'policy:' || p.name AS resource
	FROM policies p JOIN orgs o ON o.id = p.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
	UNION ALL
	SELECT o.name AS orgname, o.id AS org_id, pg.authz_id, 'policy_group:' || pg.name AS resource
	FROM policy_groups pg JOIN orgs o ON o.id = pg.org_id
	WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
)
SELECT target.orgname, target.resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(u.username, c.name, authorizee.authz_id) AS authorizee
FROM core_objects target
JOIN auth_object object_target ON object_target.authz_id = target.authz_id
JOIN object_acl_actor acl ON acl.target = object_target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users u ON u.authz_id = authorizee.authz_id
LEFT JOIN clients c ON c.authz_id = authorizee.authz_id AND c.org_id = target.org_id
UNION ALL
SELECT target.orgname, target.resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(g.name, authorizee.authz_id) AS authorizee
FROM core_objects target
JOIN auth_object object_target ON object_target.authz_id = target.authz_id
JOIN object_acl_group acl ON acl.target = object_target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups g ON g.authz_id = authorizee.authz_id AND g.org_id = target.org_id
ORDER BY orgname, resource, permission, authorizee_type, authorizee`, strings.TrimSpace(cfg.Organization))
	if err != nil {
		return err
	}
	byOrg := map[string][]map[string]any{}
	for _, acl := range adminMigrationLiveSourceACLObjects(rows) {
		orgName := adminMigrationSourceString(acl, "orgname")
		delete(acl, "orgname")
		byOrg[orgName] = append(byOrg[orgName], acl)
	}
	for _, orgName := range adminMigrationSortedMapKeys(byOrg) {
		for _, acl := range byOrg[orgName] {
			adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, acl)
		}
	}
	return nil
}

func adminMigrationLiveSourceReadUserACLs(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	rows, err := adminMigrationLiveSourceQueryACLRows(ctx, tx, `
SELECT 'user:' || u.username AS resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(au.username, ac.name, authorizee.authz_id) AS authorizee
FROM users u
JOIN auth_actor target ON target.authz_id = u.authz_id
JOIN actor_acl_actor acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users au ON au.authz_id = authorizee.authz_id
LEFT JOIN clients ac ON ac.authz_id = authorizee.authz_id
UNION ALL
SELECT 'user:' || u.username AS resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(g.name, authorizee.authz_id) AS authorizee
FROM users u
JOIN auth_actor target ON target.authz_id = u.authz_id
JOIN actor_acl_group acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups g ON g.authz_id = authorizee.authz_id
ORDER BY resource, permission, authorizee_type, authorizee`)
	if err != nil {
		return err
	}
	for _, acl := range adminMigrationLiveSourceACLObjects(rows) {
		adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Family: "user_acls"}, acl)
	}
	return nil
}

func adminMigrationLiveSourceReadOrgACLs(ctx context.Context, tx pgx.Tx, cfg adminMigrationLiveSourceConfig, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage) error {
	rows, err := adminMigrationLiveSourceQueryACLRows(ctx, tx, `
SELECT o.name AS orgname, 'organization:' || o.name AS resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(u.username, c.name, authorizee.authz_id) AS authorizee
FROM orgs o
JOIN auth_object target ON target.authz_id = o.authz_id
JOIN object_acl_actor acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users u ON u.authz_id = authorizee.authz_id
LEFT JOIN clients c ON c.authz_id = authorizee.authz_id AND c.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'organization:' || o.name AS resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(g.name, authorizee.authz_id) AS authorizee
FROM orgs o
JOIN auth_object target ON target.authz_id = o.authz_id
JOIN object_acl_group acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups g ON g.authz_id = authorizee.authz_id AND g.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'client:' || c.name AS resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(u.username, ac.name, authorizee.authz_id) AS authorizee
FROM clients c
JOIN orgs o ON o.id = c.org_id
JOIN auth_actor target ON target.authz_id = c.authz_id
JOIN actor_acl_actor acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users u ON u.authz_id = authorizee.authz_id
LEFT JOIN clients ac ON ac.authz_id = authorizee.authz_id AND ac.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'client:' || c.name AS resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(g.name, authorizee.authz_id) AS authorizee
FROM clients c
JOIN orgs o ON o.id = c.org_id
JOIN auth_actor target ON target.authz_id = c.authz_id
JOIN actor_acl_group acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups g ON g.authz_id = authorizee.authz_id AND g.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'group:' || g.name AS resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(u.username, c.name, authorizee.authz_id) AS authorizee
FROM groups g
JOIN orgs o ON o.id = g.org_id
JOIN auth_group target ON target.authz_id = g.authz_id
JOIN group_acl_actor acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users u ON u.authz_id = authorizee.authz_id
LEFT JOIN clients c ON c.authz_id = authorizee.authz_id AND c.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'group:' || g.name AS resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(ag.name, authorizee.authz_id) AS authorizee
FROM groups g
JOIN orgs o ON o.id = g.org_id
JOIN auth_group target ON target.authz_id = g.authz_id
JOIN group_acl_group acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups ag ON ag.authz_id = authorizee.authz_id AND ag.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'container:' || c.name AS resource, acl.permission::text AS permission, 'actor' AS authorizee_type,
       COALESCE(u.username, ac.name, authorizee.authz_id) AS authorizee
FROM containers c
JOIN orgs o ON o.id = c.org_id
JOIN auth_container target ON target.authz_id = c.authz_id
JOIN container_acl_actor acl ON acl.target = target.id
JOIN auth_actor authorizee ON authorizee.id = acl.authorizee
LEFT JOIN users u ON u.authz_id = authorizee.authz_id
LEFT JOIN clients ac ON ac.authz_id = authorizee.authz_id AND ac.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
UNION ALL
SELECT o.name AS orgname, 'container:' || c.name AS resource, acl.permission::text AS permission, 'group' AS authorizee_type,
       COALESCE(g.name, authorizee.authz_id) AS authorizee
FROM containers c
JOIN orgs o ON o.id = c.org_id
JOIN auth_container target ON target.authz_id = c.authz_id
JOIN container_acl_group acl ON acl.target = target.id
JOIN auth_group authorizee ON authorizee.id = acl.authorizee
LEFT JOIN groups g ON g.authz_id = authorizee.authz_id AND g.org_id = o.id
WHERE o.name <> '' AND ($1::text = '' OR o.name = $1)
ORDER BY orgname, resource, permission, authorizee_type, authorizee`, strings.TrimSpace(cfg.Organization))
	if err != nil {
		return err
	}
	byOrg := map[string][]map[string]any{}
	for _, acl := range adminMigrationLiveSourceACLObjects(rows) {
		orgName := adminMigrationSourceString(acl, "orgname")
		delete(acl, "orgname")
		byOrg[orgName] = append(byOrg[orgName], acl)
	}
	for _, orgName := range adminMigrationSortedMapKeys(byOrg) {
		for _, acl := range byOrg[orgName] {
			adminMigrationLiveSourceAppendObject(payloadValues, adminMigrationSourcePayloadKey{Organization: orgName, Family: "acls"}, acl)
		}
	}
	return nil
}

type adminMigrationLiveSourceACLRow struct {
	OrgName        string
	Resource       string
	Permission     string
	AuthorizeeType string
	Authorizee     string
}

func adminMigrationLiveSourceQueryACLRows(ctx context.Context, tx pgx.Tx, query string, args ...any) ([]adminMigrationLiveSourceACLRow, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	fields := rows.FieldDescriptions()
	var out []adminMigrationLiveSourceACLRow
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		object := adminMigrationLiveSourceRowMap(fields, values)
		out = append(out, adminMigrationLiveSourceACLRow{
			OrgName:        adminMigrationSourceString(object, "orgname"),
			Resource:       adminMigrationSourceString(object, "resource"),
			Permission:     adminMigrationSourceString(object, "permission"),
			AuthorizeeType: adminMigrationSourceString(object, "authorizee_type"),
			Authorizee:     adminMigrationSourceString(object, "authorizee"),
		})
	}
	return out, rows.Err()
}

func adminMigrationLiveSourceACLObjects(rows []adminMigrationLiveSourceACLRow) []map[string]any {
	byIdentity := map[string]map[string]map[string][]string{}
	resourceByIdentity := map[string]string{}
	orgByIdentity := map[string]string{}
	for _, row := range rows {
		resource := strings.TrimSpace(row.Resource)
		permission := strings.TrimSpace(row.Permission)
		authorizee := strings.TrimSpace(row.Authorizee)
		if resource == "" || permission == "" || authorizee == "" {
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
		return err
	}
	for _, row := range rows {
		adminMigrationLiveSourceAppendObject(payloadValues, key, row)
	}
	return nil
}

func adminMigrationLiveSourceAppendOrgQuery(ctx context.Context, tx pgx.Tx, payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, family, query string, args ...any) error {
	rows, err := adminMigrationLiveSourceQueryObjects(ctx, tx, query, args...)
	if err != nil {
		return err
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
		out = append(out, adminMigrationLiveSourceRowMap(fields, values))
	}
	return out, rows.Err()
}

func adminMigrationLiveSourceRowMap(fields []pgconn.FieldDescription, values []any) map[string]any {
	object := map[string]any{}
	for i, field := range fields {
		if i >= len(values) {
			continue
		}
		name := string(field.Name)
		value := adminMigrationLiveSourceJSONValue(values[i])
		if name == "serialized_object" {
			adminMigrationLiveSourceMergeSerializedObject(object, value)
			continue
		}
		if name == "raw_payload" {
			adminMigrationLiveSourceMergeRawPayload(object, value)
			continue
		}
		if name == "metadata_payload" {
			adminMigrationLiveSourceMergeMetadataPayload(object, value)
			continue
		}
		object[name] = value
	}
	return object
}

func adminMigrationLiveSourceJSONValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return adminMigrationLiveSourceStringFromBytes(typed)
	case time.Time:
		return typed.UTC().Format(time.RFC3339)
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return typed
	}
}

// adminMigrationLiveSourceStringFromBytes accepts both plain JSON bytea and
// Chef's gzip-compressed serialized_object blobs so source extraction can stay
// read-only without relying on database-side extensions.
func adminMigrationLiveSourceStringFromBytes(data []byte) string {
	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return string(data)
		}
		defer reader.Close()
		decoded, err := io.ReadAll(reader)
		if err != nil {
			return string(data)
		}
		return string(decoded)
	}
	return string(data)
}

// adminMigrationLiveSourceMergeSerializedObject salvages optional Chef user
// profile fields without trusting serialized JSON to override canonical names.
func adminMigrationLiveSourceMergeSerializedObject(object map[string]any, value any) {
	raw, _ := value.(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return
	}
	for _, field := range []string{"display_name", "first_name", "last_name"} {
		if _, exists := object[field]; exists {
			continue
		}
		if value, ok := decoded[field].(string); ok && strings.TrimSpace(value) != "" {
			object[field] = strings.TrimSpace(value)
		}
	}
}

// adminMigrationLiveSourceMergeRawPayload preserves Chef object JSON exactly
// enough for OpenCook's normalized importer while keeping SQL-selected identity
// columns authoritative for names, org routing, and relationship fields.
func adminMigrationLiveSourceMergeRawPayload(object map[string]any, value any) {
	raw, _ := value.(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return
	}
	for key, value := range decoded {
		if _, exists := object[key]; exists {
			continue
		}
		object[key] = value
	}
}

// adminMigrationLiveSourceMergeMetadataPayload preserves cookbook metadata bytea
// as the nested metadata object expected by OpenCook's cookbook importer.
func adminMigrationLiveSourceMergeMetadataPayload(object map[string]any, value any) {
	raw, _ := value.(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return
	}
	object["metadata"] = decoded
}

func adminMigrationLiveSourceAppendObject(payloadValues map[adminMigrationSourcePayloadKey][]json.RawMessage, key adminMigrationSourcePayloadKey, object map[string]any) {
	data, err := json.Marshal(object)
	if err != nil {
		return
	}
	payloadValues[key] = append(payloadValues[key], json.RawMessage(data))
}
