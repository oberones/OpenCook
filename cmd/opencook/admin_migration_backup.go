package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/version"
)

const (
	adminMigrationBackupFormatVersion = "opencook.migration.backup.v1"
	adminMigrationBackupManifestPath  = "manifest.json"
	adminMigrationBackupBootstrapPath = "postgres/bootstrap_core.json"
	adminMigrationBackupObjectsPath   = "postgres/core_objects.json"
	adminMigrationBackupCookbooksPath = "postgres/cookbooks.json"
	adminMigrationBackupBlobsPath     = "blobs/manifest.json"
	adminMigrationBackupBlobObjectDir = "blobs/objects"
	adminMigrationBackupRunbookPath   = "runbook-notes.md"
)

type adminMigrationCookbookExportLoader interface {
	LoadCookbookExport([]string) (adminMigrationCookbookExport, error)
}

type adminMigrationCookbookExport struct {
	Orgs map[string]adminMigrationCookbookOrgExport `json:"orgs"`
}

type adminMigrationCookbookOrgExport struct {
	Versions  []bootstrap.CookbookVersion  `json:"versions"`
	Artifacts []bootstrap.CookbookArtifact `json:"artifacts"`
}

type adminMigrationBackupBundleInput struct {
	Build             version.Info
	CreatedAt         time.Time
	Config            config.Config
	Bootstrap         bootstrap.BootstrapCoreState
	CoreObjects       bootstrap.CoreObjectState
	Cookbooks         adminMigrationCookbookExport
	BlobCopies        []adminMigrationBackupBlobData
	Inventory         adminMigrationInventory
	Warnings          []string
	CompatibilityNote string
}

type adminMigrationBackupManifest struct {
	FormatVersion string                          `json:"format_version"`
	CreatedAt     string                          `json:"created_at"`
	OpenCook      version.Info                    `json:"opencook"`
	SourceConfig  map[string]string               `json:"source_config"`
	Inventory     adminMigrationInventory         `json:"inventory"`
	Payloads      []adminMigrationBackupPayload   `json:"payloads"`
	Excluded      []adminMigrationBackupExclusion `json:"excluded,omitempty"`
	Warnings      []string                        `json:"warnings,omitempty"`
	Notes         []string                        `json:"notes,omitempty"`
}

type adminMigrationBackupPayload struct {
	Path   string `json:"path"`
	Family string `json:"family"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

type adminMigrationBackupBlobData struct {
	Checksum string
	Body     []byte
}

type adminMigrationBackupBlobCopy struct {
	Checksum string `json:"checksum"`
	Path     string `json:"path"`
	SHA256   string `json:"sha256"`
	Bytes    int64  `json:"bytes"`
}

type adminMigrationBackupExclusion struct {
	Family string `json:"family"`
	Reason string `json:"reason"`
}

type adminMigrationBackupBlobManifest struct {
	FormatVersion       string                         `json:"format_version"`
	ReferencedChecksums []string                       `json:"referenced_checksums"`
	Copied              []adminMigrationBackupBlobCopy `json:"copied,omitempty"`
	Notes               []string                       `json:"notes,omitempty"`
}

// adminMigrationWriteBackupBundle writes the versioned logical backup directory
// layout and returns the manifest that was persisted to manifest.json.
func adminMigrationWriteBackupBundle(outputPath string, input adminMigrationBackupBundleInput) (adminMigrationBackupManifest, error) {
	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "" {
		return adminMigrationBackupManifest{}, fmt.Errorf("backup output path is required")
	}
	if input.CreatedAt.IsZero() {
		input.CreatedAt = time.Now().UTC()
	} else {
		input.CreatedAt = input.CreatedAt.UTC()
	}

	payloads := make([]adminMigrationBackupPayload, 0, 5+len(input.BlobCopies))
	writes := []struct {
		path   string
		family string
		value  any
	}{
		{path: adminMigrationBackupBootstrapPath, family: "bootstrap_core", value: input.Bootstrap},
		{path: adminMigrationBackupObjectsPath, family: "core_objects", value: input.CoreObjects},
		{path: adminMigrationBackupCookbooksPath, family: "cookbooks", value: input.Cookbooks},
	}
	for _, write := range writes {
		payload, err := adminMigrationWriteJSONPayload(outputPath, write.path, write.family, write.value)
		if err != nil {
			return adminMigrationBackupManifest{}, err
		}
		payloads = append(payloads, payload)
	}

	copiedBlobs, copiedPayloads, err := adminMigrationWriteBackupBlobCopies(outputPath, input.BlobCopies)
	if err != nil {
		return adminMigrationBackupManifest{}, err
	}
	payloads = append(payloads, copiedPayloads...)

	blobManifest := adminMigrationBackupBlobManifestFromState(input.CoreObjects, input.Cookbooks, copiedBlobs)
	blobPayload, err := adminMigrationWriteJSONPayload(outputPath, adminMigrationBackupBlobsPath, "blobs", blobManifest)
	if err != nil {
		return adminMigrationBackupManifest{}, err
	}
	payloads = append(payloads, blobPayload)

	runbookPayload, err := adminMigrationWriteTextPayload(outputPath, adminMigrationBackupRunbookPath, "runbook", adminMigrationBackupRunbookNotes())
	if err != nil {
		return adminMigrationBackupManifest{}, err
	}
	payloads = append(payloads, runbookPayload)

	manifest := adminMigrationBackupManifest{
		FormatVersion: adminMigrationBackupFormatVersion,
		CreatedAt:     input.CreatedAt.Format(time.RFC3339Nano),
		OpenCook:      input.Build,
		SourceConfig:  input.Config.Redacted(),
		Inventory:     input.Inventory,
		Payloads:      payloads,
		Excluded: []adminMigrationBackupExclusion{{
			Family: "opensearch",
			Reason: "OpenSearch documents are derived from PostgreSQL-backed OpenCook state and must be rebuilt after restore.",
		}},
		Warnings: adminMigrationSortedStrings(input.Warnings),
		Notes: []string{
			"Logical OpenCook state export; not a raw PostgreSQL dump.",
			adminMigrationBackupBlobCopyNote(len(copiedBlobs)),
		},
	}
	if input.CompatibilityNote != "" {
		manifest.Notes = append(manifest.Notes, input.CompatibilityNote)
	}
	if _, err := adminMigrationWriteJSONPayload(outputPath, adminMigrationBackupManifestPath, "manifest", manifest); err != nil {
		return adminMigrationBackupManifest{}, err
	}
	return manifest, nil
}

// adminMigrationWriteBackupBlobCopies writes local deterministic blob content
// into the bundle while preserving the checksum-addressed object layout.
func adminMigrationWriteBackupBlobCopies(root string, copies []adminMigrationBackupBlobData) ([]adminMigrationBackupBlobCopy, []adminMigrationBackupPayload, error) {
	byChecksum := map[string][]byte{}
	for _, copy := range copies {
		checksum := strings.ToLower(strings.TrimSpace(copy.Checksum))
		if checksum == "" {
			continue
		}
		if !bootstrap.ValidSandboxChecksum(checksum) {
			return nil, nil, fmt.Errorf("invalid backup blob checksum %q", copy.Checksum)
		}
		if _, ok := byChecksum[checksum]; !ok {
			byChecksum[checksum] = append([]byte(nil), copy.Body...)
		}
	}

	checksums := adminMigrationSortedMapKeys(byChecksum)
	copied := make([]adminMigrationBackupBlobCopy, 0, len(checksums))
	payloads := make([]adminMigrationBackupPayload, 0, len(checksums))
	for _, checksum := range checksums {
		relativePath := filepath.ToSlash(filepath.Join(adminMigrationBackupBlobObjectDir, checksum))
		payload, err := adminMigrationWritePayloadBytes(root, relativePath, "blob_object", byChecksum[checksum])
		if err != nil {
			return nil, nil, err
		}
		copied = append(copied, adminMigrationBackupBlobCopy{
			Checksum: checksum,
			Path:     payload.Path,
			SHA256:   payload.SHA256,
			Bytes:    payload.Bytes,
		})
		payloads = append(payloads, payload)
	}
	return copied, payloads, nil
}

// adminMigrationWriteJSONPayload serializes a payload with stable indentation
// and records its path, size, and SHA-256 digest for manifest integrity checks.
func adminMigrationWriteJSONPayload(root, relativePath, family string, value any) (adminMigrationBackupPayload, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return adminMigrationBackupPayload{}, err
	}
	data = append(data, '\n')
	return adminMigrationWritePayloadBytes(root, relativePath, family, data)
}

// adminMigrationWriteTextPayload writes a text payload and records the same
// integrity metadata as JSON payloads.
func adminMigrationWriteTextPayload(root, relativePath, family, body string) (adminMigrationBackupPayload, error) {
	if !strings.HasSuffix(body, "\n") {
		body += "\n"
	}
	return adminMigrationWritePayloadBytes(root, relativePath, family, []byte(body))
}

// adminMigrationWritePayloadBytes writes one bundle payload below the output
// root, creating parent directories but refusing path traversal.
func adminMigrationWritePayloadBytes(root, relativePath, family string, data []byte) (adminMigrationBackupPayload, error) {
	relativePath = filepath.Clean(strings.TrimSpace(relativePath))
	if relativePath == "." || filepath.IsAbs(relativePath) || strings.HasPrefix(relativePath, ".."+string(os.PathSeparator)) || relativePath == ".." {
		return adminMigrationBackupPayload{}, fmt.Errorf("invalid backup payload path %q", relativePath)
	}
	path := filepath.Join(root, relativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return adminMigrationBackupPayload{}, err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return adminMigrationBackupPayload{}, err
	}
	sum := sha256.Sum256(data)
	return adminMigrationBackupPayload{
		Path:   filepath.ToSlash(relativePath),
		Family: strings.TrimSpace(family),
		SHA256: hex.EncodeToString(sum[:]),
		Bytes:  int64(len(data)),
	}, nil
}

// adminMigrationBackupBlobManifestFromState records every unique checksum
// referenced by metadata plus any local blob bytes copied into the bundle.
func adminMigrationBackupBlobManifestFromState(coreObjectState bootstrap.CoreObjectState, cookbooks adminMigrationCookbookExport, copied []adminMigrationBackupBlobCopy) adminMigrationBackupBlobManifest {
	set := map[string]struct{}{}
	for _, orgName := range adminMigrationSortedMapKeys(coreObjectState.Orgs) {
		for _, sandbox := range coreObjectState.Orgs[orgName].Sandboxes {
			for _, checksum := range sandbox.Checksums {
				checksum = strings.ToLower(strings.TrimSpace(checksum))
				if checksum != "" {
					set[checksum] = struct{}{}
				}
			}
		}
	}
	for _, orgName := range adminMigrationSortedMapKeys(cookbooks.Orgs) {
		org := cookbooks.Orgs[orgName]
		for _, cookbookVersion := range org.Versions {
			for _, checksum := range adminMigrationCookbookFileChecksums(cookbookVersion.AllFiles) {
				set[checksum] = struct{}{}
			}
		}
		for _, artifact := range org.Artifacts {
			for _, checksum := range adminMigrationCookbookFileChecksums(artifact.AllFiles) {
				set[checksum] = struct{}{}
			}
		}
	}
	return adminMigrationBackupBlobManifest{
		FormatVersion:       adminMigrationBackupFormatVersion,
		ReferencedChecksums: adminMigrationSortedStringSet(set),
		Copied:              adminMigrationSortedBackupBlobCopies(copied),
		Notes: []string{
			"Checksums identify blob content referenced by PostgreSQL-backed metadata.",
			adminMigrationBackupBlobCopyNote(len(copied)),
		},
	}
}

// adminMigrationSortedBackupBlobCopies stabilizes copied blob metadata before
// it is embedded in blobs/manifest.json.
func adminMigrationSortedBackupBlobCopies(copies []adminMigrationBackupBlobCopy) []adminMigrationBackupBlobCopy {
	out := append([]adminMigrationBackupBlobCopy(nil), copies...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].Checksum < out[j].Checksum
	})
	return out
}

// adminMigrationBackupBlobCopyNote explains whether this bundle contains
// portable blob bytes or only checksum references.
func adminMigrationBackupBlobCopyNote(count int) string {
	if count > 0 {
		return fmt.Sprintf("%d referenced blob byte payload(s) were copied from a deterministic local backend.", count)
	}
	return "Blob bytes were not copied; blobs/manifest.json records referenced checksums for restore planning."
}

// adminMigrationSortedCookbookExportOrg stabilizes cookbook export ordering so
// bundle hashes remain deterministic across map iteration order.
func adminMigrationSortedCookbookExportOrg(org adminMigrationCookbookOrgExport) adminMigrationCookbookOrgExport {
	sort.Slice(org.Versions, func(i, j int) bool {
		if org.Versions[i].Name == org.Versions[j].Name {
			return org.Versions[i].Version < org.Versions[j].Version
		}
		return org.Versions[i].Name < org.Versions[j].Name
	})
	sort.Slice(org.Artifacts, func(i, j int) bool {
		if org.Artifacts[i].Name == org.Artifacts[j].Name {
			return org.Artifacts[i].Identifier < org.Artifacts[j].Identifier
		}
		return org.Artifacts[i].Name < org.Artifacts[j].Name
	})
	return org
}

// adminMigrationBackupRunbookNotes explains restore-time responsibilities that
// intentionally stay outside the portable logical state payloads.
func adminMigrationBackupRunbookNotes() string {
	return strings.TrimSpace(`# OpenCook Backup Bundle

This bundle stores logical OpenCook state, not a raw PostgreSQL dump.

OpenSearch provider documents are intentionally excluded because OpenSearch is derived state. After restoring PostgreSQL-backed state and referenced blob content, rebuild search with:

    opencook admin reindex --all-orgs --complete

The blob manifest records checksum references and includes blob byte payloads when the configured backend supports deterministic local reads.
`) + "\n"
}

// adminMigrationSortedStrings returns deterministic warning output while
// preserving duplicates as separate operator-visible messages.
func adminMigrationSortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}
