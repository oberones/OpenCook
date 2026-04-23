package pg

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

//go:embed schema/0001_cookbook_persistence.sql
var cookbookPersistenceSchemaSQL string

type Migration struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

type CookbookRepository struct {
	store     *Store
	mu        sync.RWMutex
	db        *sql.DB
	orgs      map[string]CookbookOrganizationRecord
	versions  map[string]map[string]map[string]CookbookVersionBundle
	artifacts map[string]map[string]map[string]CookbookArtifactBundle
}

type CookbookOrganizationRecord struct {
	Name     string
	FullName string
}

type CookbookVersionRecord struct {
	Organization string
	CookbookName string
	Version      string
	FullName     string
	JSONClass    string
	ChefType     string
	Frozen       bool
	MetadataJSON []byte
}

type CookbookVersionFileRecord struct {
	Organization string
	CookbookName string
	Version      string
	Ordinal      int
	Name         string
	Path         string
	Checksum     string
	Specificity  string
}

type CookbookVersionBundle struct {
	Version CookbookVersionRecord
	Files   []CookbookVersionFileRecord
}

type CookbookArtifactRecord struct {
	Organization string
	Name         string
	Identifier   string
	Version      string
	ChefType     string
	Frozen       bool
	MetadataJSON []byte
}

type CookbookArtifactFileRecord struct {
	Organization string
	Name         string
	Identifier   string
	Ordinal      int
	FileName     string
	FilePath     string
	Checksum     string
	Specificity  string
}

type CookbookArtifactBundle struct {
	Artifact CookbookArtifactRecord
	Files    []CookbookArtifactFileRecord
}

func newCookbookRepository(store *Store) *CookbookRepository {
	return &CookbookRepository{
		store:     store,
		orgs:      make(map[string]CookbookOrganizationRecord),
		versions:  make(map[string]map[string]map[string]CookbookVersionBundle),
		artifacts: make(map[string]map[string]map[string]CookbookArtifactBundle),
	}
}

func (s *Store) Cookbooks() *CookbookRepository {
	if s == nil {
		return nil
	}
	return s.cookbooks
}

func (r *CookbookRepository) Migrations() []Migration {
	if r == nil {
		return nil
	}

	return []Migration{
		{
			Name: "0001_cookbook_persistence.sql",
			SQL:  cookbookPersistenceSchemaSQL,
		},
	}
}

func (r *CookbookRepository) OrganizationRecords() []CookbookOrganizationRecord {
	if r == nil {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.orgs) == 0 {
		return nil
	}

	out := make([]CookbookOrganizationRecord, 0, len(r.orgs))
	for _, org := range r.orgs {
		out = append(out, org)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func (r *CookbookRepository) activate(ctx context.Context, db *sql.DB) error {
	if r == nil {
		return fmt.Errorf("cookbook repository is required")
	}

	for _, migration := range r.Migrations() {
		if _, err := db.ExecContext(ctx, migration.SQL); err != nil {
			return fmt.Errorf("apply %s: %w", migration.Name, err)
		}
	}

	orgs, err := loadCookbookOrganizations(ctx, db)
	if err != nil {
		return err
	}
	versions, err := loadCookbookVersions(ctx, db)
	if err != nil {
		return err
	}
	artifacts, err := loadCookbookArtifacts(ctx, db)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.db = db
	r.orgs = orgs
	r.versions = versions
	r.artifacts = artifacts
	return nil
}

func (r *CookbookRepository) EncodeCookbookVersion(orgName string, version bootstrap.CookbookVersion) (CookbookVersionBundle, error) {
	orgName = strings.TrimSpace(orgName)
	if orgName == "" {
		return CookbookVersionBundle{}, fmt.Errorf("organization is required")
	}

	metadataJSON, err := json.Marshal(version.Metadata)
	if err != nil {
		return CookbookVersionBundle{}, fmt.Errorf("marshal cookbook metadata: %w", err)
	}

	files := make([]CookbookVersionFileRecord, 0, len(version.AllFiles))
	for idx, file := range version.AllFiles {
		files = append(files, CookbookVersionFileRecord{
			Organization: orgName,
			CookbookName: version.CookbookName,
			Version:      version.Version,
			Ordinal:      idx,
			Name:         file.Name,
			Path:         file.Path,
			Checksum:     file.Checksum,
			Specificity:  file.Specificity,
		})
	}

	return CookbookVersionBundle{
		Version: CookbookVersionRecord{
			Organization: orgName,
			CookbookName: version.CookbookName,
			Version:      version.Version,
			FullName:     version.Name,
			JSONClass:    version.JSONClass,
			ChefType:     version.ChefType,
			Frozen:       version.Frozen,
			MetadataJSON: metadataJSON,
		},
		Files: files,
	}, nil
}

func (r *CookbookRepository) DecodeCookbookVersion(bundle CookbookVersionBundle) (bootstrap.CookbookVersion, error) {
	var metadata map[string]any
	if err := json.Unmarshal(bundle.Version.MetadataJSON, &metadata); err != nil {
		return bootstrap.CookbookVersion{}, fmt.Errorf("unmarshal cookbook metadata: %w", err)
	}

	files := append([]CookbookVersionFileRecord(nil), bundle.Files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Ordinal != files[j].Ordinal {
			return files[i].Ordinal < files[j].Ordinal
		}
		return files[i].Name < files[j].Name
	})

	outFiles := make([]bootstrap.CookbookFile, 0, len(files))
	for _, file := range files {
		if err := validateCookbookVersionFileParent(bundle.Version, file); err != nil {
			return bootstrap.CookbookVersion{}, err
		}
		outFiles = append(outFiles, bootstrap.CookbookFile{
			Name:        file.Name,
			Path:        file.Path,
			Checksum:    file.Checksum,
			Specificity: file.Specificity,
		})
	}

	return bootstrap.CookbookVersion{
		Name:         bundle.Version.FullName,
		CookbookName: bundle.Version.CookbookName,
		Version:      bundle.Version.Version,
		JSONClass:    bundle.Version.JSONClass,
		ChefType:     bundle.Version.CookbookType(),
		Frozen:       bundle.Version.Frozen,
		Metadata:     metadata,
		AllFiles:     outFiles,
	}, nil
}

func (r *CookbookRepository) EncodeCookbookArtifact(orgName string, artifact bootstrap.CookbookArtifact) (CookbookArtifactBundle, error) {
	orgName = strings.TrimSpace(orgName)
	if orgName == "" {
		return CookbookArtifactBundle{}, fmt.Errorf("organization is required")
	}

	metadataJSON, err := json.Marshal(artifact.Metadata)
	if err != nil {
		return CookbookArtifactBundle{}, fmt.Errorf("marshal cookbook artifact metadata: %w", err)
	}

	files := make([]CookbookArtifactFileRecord, 0, len(artifact.AllFiles))
	for idx, file := range artifact.AllFiles {
		files = append(files, CookbookArtifactFileRecord{
			Organization: orgName,
			Name:         artifact.Name,
			Identifier:   artifact.Identifier,
			Ordinal:      idx,
			FileName:     file.Name,
			FilePath:     file.Path,
			Checksum:     file.Checksum,
			Specificity:  file.Specificity,
		})
	}

	return CookbookArtifactBundle{
		Artifact: CookbookArtifactRecord{
			Organization: orgName,
			Name:         artifact.Name,
			Identifier:   artifact.Identifier,
			Version:      artifact.Version,
			ChefType:     artifact.ChefType,
			Frozen:       artifact.Frozen,
			MetadataJSON: metadataJSON,
		},
		Files: files,
	}, nil
}

func (r *CookbookRepository) DecodeCookbookArtifact(bundle CookbookArtifactBundle) (bootstrap.CookbookArtifact, error) {
	var metadata map[string]any
	if err := json.Unmarshal(bundle.Artifact.MetadataJSON, &metadata); err != nil {
		return bootstrap.CookbookArtifact{}, fmt.Errorf("unmarshal cookbook artifact metadata: %w", err)
	}

	files := append([]CookbookArtifactFileRecord(nil), bundle.Files...)
	sort.Slice(files, func(i, j int) bool {
		if files[i].Ordinal != files[j].Ordinal {
			return files[i].Ordinal < files[j].Ordinal
		}
		return files[i].FileName < files[j].FileName
	})

	outFiles := make([]bootstrap.CookbookFile, 0, len(files))
	for _, file := range files {
		if err := validateCookbookArtifactFileParent(bundle.Artifact, file); err != nil {
			return bootstrap.CookbookArtifact{}, err
		}
		outFiles = append(outFiles, bootstrap.CookbookFile{
			Name:        file.FileName,
			Path:        file.FilePath,
			Checksum:    file.Checksum,
			Specificity: file.Specificity,
		})
	}

	return bootstrap.CookbookArtifact{
		Name:       bundle.Artifact.Name,
		Identifier: bundle.Artifact.Identifier,
		Version:    bundle.Artifact.Version,
		ChefType:   bundle.Artifact.ChefType,
		Frozen:     bundle.Artifact.Frozen,
		Metadata:   metadata,
		AllFiles:   outFiles,
	}, nil
}

func (r CookbookVersionRecord) CookbookType() string {
	if strings.TrimSpace(r.ChefType) == "" {
		return "cookbook_version"
	}
	return r.ChefType
}

func validateCookbookVersionFileParent(version CookbookVersionRecord, file CookbookVersionFileRecord) error {
	if version.Organization != file.Organization || version.CookbookName != file.CookbookName || version.Version != file.Version {
		return fmt.Errorf("cookbook version file parent mismatch")
	}
	return nil
}

func validateCookbookArtifactFileParent(artifact CookbookArtifactRecord, file CookbookArtifactFileRecord) error {
	if artifact.Organization != file.Organization || artifact.Name != file.Name || artifact.Identifier != file.Identifier {
		return fmt.Errorf("cookbook artifact file parent mismatch")
	}
	return nil
}

func normalizedOrganizationRecord(orgName, fullName string) CookbookOrganizationRecord {
	orgName = strings.TrimSpace(orgName)
	fullName = strings.TrimSpace(fullName)
	if fullName == "" {
		fullName = orgName
	}
	return CookbookOrganizationRecord{
		Name:     orgName,
		FullName: fullName,
	}
}

func mergeOrganizationRecord(existing, incoming CookbookOrganizationRecord) CookbookOrganizationRecord {
	if strings.TrimSpace(existing.Name) == "" {
		return incoming
	}

	// Preserve a non-placeholder display name when a later write only knows the org slug.
	if strings.TrimSpace(existing.FullName) != "" &&
		existing.FullName != existing.Name &&
		incoming.FullName == incoming.Name {
		return CookbookOrganizationRecord{
			Name:     incoming.Name,
			FullName: existing.FullName,
		}
	}

	return incoming
}

func (r *CookbookRepository) ensureOrganization(orgName, fullName string) {
	if r == nil {
		return
	}

	record := normalizedOrganizationRecord(orgName, fullName)
	if record.Name == "" {
		return
	}

	if r.db != nil {
		if _, err := r.db.ExecContext(context.Background(),
			`INSERT INTO oc_cookbook_orgs (org_name, full_name) VALUES ($1, $2)
			 ON CONFLICT (org_name) DO UPDATE
			 SET full_name = CASE
			 	WHEN oc_cookbook_orgs.full_name = oc_cookbook_orgs.org_name THEN EXCLUDED.full_name
			 	ELSE oc_cookbook_orgs.full_name
			 END,
			 updated_at = NOW()`,
			record.Name,
			record.FullName,
		); err != nil {
			// Keep the in-process state usable; write-path errors are surfaced by the CRUD methods.
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.orgs[record.Name] = mergeOrganizationRecord(r.orgs[record.Name], record)
}

func (r *CookbookRepository) organizationExists(orgName string) bool {
	if r == nil {
		return false
	}

	orgName = strings.TrimSpace(orgName)
	if orgName == "" {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.orgs[orgName]
	return ok
}

func (r *CookbookRepository) listCookbookVersions(orgName string) map[string][]bootstrap.CookbookVersion {
	if r == nil {
		return nil
	}

	orgName = strings.TrimSpace(orgName)
	r.mu.RLock()
	defer r.mu.RUnlock()

	versionSets := r.versions[orgName]
	if len(versionSets) == 0 {
		return map[string][]bootstrap.CookbookVersion{}
	}

	out := make(map[string][]bootstrap.CookbookVersion, len(versionSets))
	for name, byVersion := range versionSets {
		decoded := make([]bootstrap.CookbookVersion, 0, len(byVersion))
		for _, bundle := range byVersion {
			version, err := r.DecodeCookbookVersion(bundle)
			if err != nil {
				continue
			}
			decoded = append(decoded, version)
		}
		sortCookbookVersions(decoded)
		out[name] = decoded
	}
	return out
}

func (r *CookbookRepository) listCookbookVersionsByName(orgName, name string) []bootstrap.CookbookVersion {
	if r == nil {
		return nil
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	r.mu.RLock()
	defer r.mu.RUnlock()

	byVersion := r.versions[orgName][name]
	if len(byVersion) == 0 {
		return nil
	}

	out := make([]bootstrap.CookbookVersion, 0, len(byVersion))
	for _, bundle := range byVersion {
		version, err := r.DecodeCookbookVersion(bundle)
		if err != nil {
			continue
		}
		out = append(out, version)
	}
	sortCookbookVersions(out)
	return out
}

func (r *CookbookRepository) getCookbookVersion(orgName, name, version string) (bootstrap.CookbookVersion, bool) {
	if r == nil {
		return bootstrap.CookbookVersion{}, false
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)

	r.mu.RLock()
	defer r.mu.RUnlock()

	bundle, ok := r.versions[orgName][name][version]
	if !ok {
		return bootstrap.CookbookVersion{}, false
	}

	decoded, err := r.DecodeCookbookVersion(bundle)
	if err != nil {
		return bootstrap.CookbookVersion{}, false
	}
	return decoded, true
}

func (r *CookbookRepository) putCookbookVersion(orgName string, version bootstrap.CookbookVersion) error {
	if r == nil {
		return fmt.Errorf("cookbook repository is required")
	}

	bundle, err := r.EncodeCookbookVersion(orgName, version)
	if err != nil {
		return err
	}

	orgName = strings.TrimSpace(orgName)
	name := strings.TrimSpace(version.CookbookName)
	revision := strings.TrimSpace(version.Version)

	if r.db != nil {
		tx, err := r.db.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("begin cookbook version transaction: %w", err)
		}
		defer tx.Rollback()

		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO oc_cookbook_versions (org_name, cookbook_name, version, full_name, json_class, chef_type, frozen, metadata_json)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (org_name, cookbook_name, version)
			 DO UPDATE SET full_name = EXCLUDED.full_name, json_class = EXCLUDED.json_class, chef_type = EXCLUDED.chef_type, frozen = EXCLUDED.frozen, metadata_json = EXCLUDED.metadata_json, updated_at = NOW()`,
			bundle.Version.Organization,
			bundle.Version.CookbookName,
			bundle.Version.Version,
			bundle.Version.FullName,
			bundle.Version.JSONClass,
			bundle.Version.ChefType,
			bundle.Version.Frozen,
			bundle.Version.MetadataJSON,
		); err != nil {
			return fmt.Errorf("upsert cookbook version row: %w", err)
		}

		if _, err := tx.ExecContext(context.Background(),
			`DELETE FROM oc_cookbook_version_files WHERE org_name = $1 AND cookbook_name = $2 AND version = $3`,
			bundle.Version.Organization,
			bundle.Version.CookbookName,
			bundle.Version.Version,
		); err != nil {
			return fmt.Errorf("delete cookbook version files: %w", err)
		}

		for _, file := range bundle.Files {
			if _, err := tx.ExecContext(context.Background(),
				`INSERT INTO oc_cookbook_version_files (org_name, cookbook_name, version, ordinal, file_name, file_path, checksum, specificity)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				file.Organization,
				file.CookbookName,
				file.Version,
				file.Ordinal,
				file.Name,
				file.Path,
				file.Checksum,
				file.Specificity,
			); err != nil {
				return fmt.Errorf("insert cookbook version file row: %w", err)
			}
		}

		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO oc_cookbook_orgs (org_name, full_name) VALUES ($1, $2)
			 ON CONFLICT (org_name) DO NOTHING`,
			orgName,
			orgName,
		); err != nil {
			return fmt.Errorf("upsert cookbook organization row: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit cookbook version transaction: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.orgs[orgName] = mergeOrganizationRecord(r.orgs[orgName], normalizedOrganizationRecord(orgName, ""))
	if r.versions[orgName] == nil {
		r.versions[orgName] = make(map[string]map[string]CookbookVersionBundle)
	}
	if r.versions[orgName][name] == nil {
		r.versions[orgName][name] = make(map[string]CookbookVersionBundle)
	}
	r.versions[orgName][name][revision] = bundle
	return nil
}

func (r *CookbookRepository) deleteCookbookVersion(orgName, name, version string) (bootstrap.CookbookVersion, bool, error) {
	if r == nil {
		return bootstrap.CookbookVersion{}, false, fmt.Errorf("cookbook repository is required")
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)

	r.mu.Lock()
	bundle, ok := r.versions[orgName][name][version]
	r.mu.Unlock()
	if !ok {
		return bootstrap.CookbookVersion{}, false, nil
	}

	if r.db != nil {
		if _, err := r.db.ExecContext(context.Background(),
			`DELETE FROM oc_cookbook_versions WHERE org_name = $1 AND cookbook_name = $2 AND version = $3`,
			orgName,
			name,
			version,
		); err != nil {
			return bootstrap.CookbookVersion{}, false, fmt.Errorf("delete cookbook version row: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.versions[orgName][name], version)
	if len(r.versions[orgName][name]) == 0 {
		delete(r.versions[orgName], name)
	}

	decoded, err := r.DecodeCookbookVersion(bundle)
	if err != nil {
		return bootstrap.CookbookVersion{}, false, err
	}
	return decoded, true, nil
}

func (r *CookbookRepository) listCookbookArtifacts(orgName string) map[string][]bootstrap.CookbookArtifact {
	if r == nil {
		return nil
	}

	orgName = strings.TrimSpace(orgName)
	r.mu.RLock()
	defer r.mu.RUnlock()

	artifactSets := r.artifacts[orgName]
	if len(artifactSets) == 0 {
		return map[string][]bootstrap.CookbookArtifact{}
	}

	out := make(map[string][]bootstrap.CookbookArtifact, len(artifactSets))
	for name, byIdentifier := range artifactSets {
		decoded := make([]bootstrap.CookbookArtifact, 0, len(byIdentifier))
		for _, bundle := range byIdentifier {
			artifact, err := r.DecodeCookbookArtifact(bundle)
			if err != nil {
				continue
			}
			decoded = append(decoded, artifact)
		}
		sortCookbookArtifacts(decoded)
		out[name] = decoded
	}
	return out
}

func (r *CookbookRepository) listCookbookArtifactsByName(orgName, name string) []bootstrap.CookbookArtifact {
	if r == nil {
		return nil
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	r.mu.RLock()
	defer r.mu.RUnlock()

	byIdentifier := r.artifacts[orgName][name]
	if len(byIdentifier) == 0 {
		return nil
	}

	out := make([]bootstrap.CookbookArtifact, 0, len(byIdentifier))
	for _, bundle := range byIdentifier {
		artifact, err := r.DecodeCookbookArtifact(bundle)
		if err != nil {
			continue
		}
		out = append(out, artifact)
	}
	sortCookbookArtifacts(out)
	return out
}

func (r *CookbookRepository) getCookbookArtifact(orgName, name, identifier string) (bootstrap.CookbookArtifact, bool) {
	if r == nil {
		return bootstrap.CookbookArtifact{}, false
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	identifier = strings.TrimSpace(identifier)

	r.mu.RLock()
	defer r.mu.RUnlock()

	bundle, ok := r.artifacts[orgName][name][identifier]
	if !ok {
		return bootstrap.CookbookArtifact{}, false
	}

	decoded, err := r.DecodeCookbookArtifact(bundle)
	if err != nil {
		return bootstrap.CookbookArtifact{}, false
	}
	return decoded, true
}

func (r *CookbookRepository) putCookbookArtifact(orgName string, artifact bootstrap.CookbookArtifact) error {
	if r == nil {
		return fmt.Errorf("cookbook repository is required")
	}

	bundle, err := r.EncodeCookbookArtifact(orgName, artifact)
	if err != nil {
		return err
	}

	orgName = strings.TrimSpace(orgName)
	name := strings.TrimSpace(artifact.Name)
	identifier := strings.TrimSpace(artifact.Identifier)

	if r.db != nil {
		tx, err := r.db.BeginTx(context.Background(), nil)
		if err != nil {
			return fmt.Errorf("begin cookbook artifact transaction: %w", err)
		}
		defer tx.Rollback()

		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO oc_cookbook_artifacts (org_name, name, identifier, version, chef_type, frozen, metadata_json)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (org_name, name, identifier)
			 DO UPDATE SET version = EXCLUDED.version, chef_type = EXCLUDED.chef_type, frozen = EXCLUDED.frozen, metadata_json = EXCLUDED.metadata_json, updated_at = NOW()`,
			bundle.Artifact.Organization,
			bundle.Artifact.Name,
			bundle.Artifact.Identifier,
			bundle.Artifact.Version,
			bundle.Artifact.ChefType,
			bundle.Artifact.Frozen,
			bundle.Artifact.MetadataJSON,
		); err != nil {
			return fmt.Errorf("upsert cookbook artifact row: %w", err)
		}

		if _, err := tx.ExecContext(context.Background(),
			`DELETE FROM oc_cookbook_artifact_files WHERE org_name = $1 AND name = $2 AND identifier = $3`,
			bundle.Artifact.Organization,
			bundle.Artifact.Name,
			bundle.Artifact.Identifier,
		); err != nil {
			return fmt.Errorf("delete cookbook artifact files: %w", err)
		}

		for _, file := range bundle.Files {
			if _, err := tx.ExecContext(context.Background(),
				`INSERT INTO oc_cookbook_artifact_files (org_name, name, identifier, ordinal, file_name, file_path, checksum, specificity)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				file.Organization,
				file.Name,
				file.Identifier,
				file.Ordinal,
				file.FileName,
				file.FilePath,
				file.Checksum,
				file.Specificity,
			); err != nil {
				return fmt.Errorf("insert cookbook artifact file row: %w", err)
			}
		}

		if _, err := tx.ExecContext(context.Background(),
			`INSERT INTO oc_cookbook_orgs (org_name, full_name) VALUES ($1, $2)
			 ON CONFLICT (org_name) DO NOTHING`,
			orgName,
			orgName,
		); err != nil {
			return fmt.Errorf("upsert cookbook organization row: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit cookbook artifact transaction: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.orgs[orgName] = mergeOrganizationRecord(r.orgs[orgName], normalizedOrganizationRecord(orgName, ""))
	if r.artifacts[orgName] == nil {
		r.artifacts[orgName] = make(map[string]map[string]CookbookArtifactBundle)
	}
	if r.artifacts[orgName][name] == nil {
		r.artifacts[orgName][name] = make(map[string]CookbookArtifactBundle)
	}
	r.artifacts[orgName][name][identifier] = bundle
	return nil
}

func (r *CookbookRepository) deleteCookbookArtifact(orgName, name, identifier string) (bootstrap.CookbookArtifact, bool, error) {
	if r == nil {
		return bootstrap.CookbookArtifact{}, false, fmt.Errorf("cookbook repository is required")
	}

	orgName = strings.TrimSpace(orgName)
	name = strings.TrimSpace(name)
	identifier = strings.TrimSpace(identifier)

	r.mu.Lock()
	bundle, ok := r.artifacts[orgName][name][identifier]
	r.mu.Unlock()
	if !ok {
		return bootstrap.CookbookArtifact{}, false, nil
	}

	if r.db != nil {
		if _, err := r.db.ExecContext(context.Background(),
			`DELETE FROM oc_cookbook_artifacts WHERE org_name = $1 AND name = $2 AND identifier = $3`,
			orgName,
			name,
			identifier,
		); err != nil {
			return bootstrap.CookbookArtifact{}, false, fmt.Errorf("delete cookbook artifact row: %w", err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.artifacts[orgName][name], identifier)
	if len(r.artifacts[orgName][name]) == 0 {
		delete(r.artifacts[orgName], name)
	}

	decoded, err := r.DecodeCookbookArtifact(bundle)
	if err != nil {
		return bootstrap.CookbookArtifact{}, false, err
	}
	return decoded, true, nil
}

func loadCookbookOrganizations(ctx context.Context, db *sql.DB) (map[string]CookbookOrganizationRecord, error) {
	rows, err := db.QueryContext(ctx, `SELECT org_name, full_name FROM oc_cookbook_orgs`)
	if err != nil {
		return nil, fmt.Errorf("load cookbook organizations: %w", err)
	}
	defer rows.Close()

	out := make(map[string]CookbookOrganizationRecord)
	for rows.Next() {
		var org CookbookOrganizationRecord
		if err := rows.Scan(&org.Name, &org.FullName); err != nil {
			return nil, fmt.Errorf("scan cookbook organization: %w", err)
		}
		out[org.Name] = org
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cookbook organizations: %w", err)
	}
	return out, nil
}

func loadCookbookVersions(ctx context.Context, db *sql.DB) (map[string]map[string]map[string]CookbookVersionBundle, error) {
	versionRows, err := db.QueryContext(ctx, `SELECT org_name, cookbook_name, version, full_name, json_class, chef_type, frozen, metadata_json FROM oc_cookbook_versions`)
	if err != nil {
		return nil, fmt.Errorf("load cookbook version rows: %w", err)
	}
	defer versionRows.Close()

	versions := make(map[string]map[string]map[string]CookbookVersionBundle)
	for versionRows.Next() {
		var record CookbookVersionRecord
		if err := versionRows.Scan(&record.Organization, &record.CookbookName, &record.Version, &record.FullName, &record.JSONClass, &record.ChefType, &record.Frozen, &record.MetadataJSON); err != nil {
			return nil, fmt.Errorf("scan cookbook version row: %w", err)
		}
		if versions[record.Organization] == nil {
			versions[record.Organization] = make(map[string]map[string]CookbookVersionBundle)
		}
		if versions[record.Organization][record.CookbookName] == nil {
			versions[record.Organization][record.CookbookName] = make(map[string]CookbookVersionBundle)
		}
		versions[record.Organization][record.CookbookName][record.Version] = CookbookVersionBundle{Version: record}
	}
	if err := versionRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cookbook version rows: %w", err)
	}

	fileRows, err := db.QueryContext(ctx, `SELECT org_name, cookbook_name, version, ordinal, file_name, file_path, checksum, specificity FROM oc_cookbook_version_files`)
	if err != nil {
		return nil, fmt.Errorf("load cookbook version file rows: %w", err)
	}
	defer fileRows.Close()

	for fileRows.Next() {
		var file CookbookVersionFileRecord
		if err := fileRows.Scan(&file.Organization, &file.CookbookName, &file.Version, &file.Ordinal, &file.Name, &file.Path, &file.Checksum, &file.Specificity); err != nil {
			return nil, fmt.Errorf("scan cookbook version file row: %w", err)
		}
		bundle := versions[file.Organization][file.CookbookName][file.Version]
		bundle.Files = append(bundle.Files, file)
		versions[file.Organization][file.CookbookName][file.Version] = bundle
	}
	if err := fileRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cookbook version file rows: %w", err)
	}

	return versions, nil
}

func loadCookbookArtifacts(ctx context.Context, db *sql.DB) (map[string]map[string]map[string]CookbookArtifactBundle, error) {
	artifactRows, err := db.QueryContext(ctx, `SELECT org_name, name, identifier, version, chef_type, frozen, metadata_json FROM oc_cookbook_artifacts`)
	if err != nil {
		return nil, fmt.Errorf("load cookbook artifact rows: %w", err)
	}
	defer artifactRows.Close()

	artifacts := make(map[string]map[string]map[string]CookbookArtifactBundle)
	for artifactRows.Next() {
		var record CookbookArtifactRecord
		if err := artifactRows.Scan(&record.Organization, &record.Name, &record.Identifier, &record.Version, &record.ChefType, &record.Frozen, &record.MetadataJSON); err != nil {
			return nil, fmt.Errorf("scan cookbook artifact row: %w", err)
		}
		if artifacts[record.Organization] == nil {
			artifacts[record.Organization] = make(map[string]map[string]CookbookArtifactBundle)
		}
		if artifacts[record.Organization][record.Name] == nil {
			artifacts[record.Organization][record.Name] = make(map[string]CookbookArtifactBundle)
		}
		artifacts[record.Organization][record.Name][record.Identifier] = CookbookArtifactBundle{Artifact: record}
	}
	if err := artifactRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cookbook artifact rows: %w", err)
	}

	fileRows, err := db.QueryContext(ctx, `SELECT org_name, name, identifier, ordinal, file_name, file_path, checksum, specificity FROM oc_cookbook_artifact_files`)
	if err != nil {
		return nil, fmt.Errorf("load cookbook artifact file rows: %w", err)
	}
	defer fileRows.Close()

	for fileRows.Next() {
		var file CookbookArtifactFileRecord
		if err := fileRows.Scan(&file.Organization, &file.Name, &file.Identifier, &file.Ordinal, &file.FileName, &file.FilePath, &file.Checksum, &file.Specificity); err != nil {
			return nil, fmt.Errorf("scan cookbook artifact file row: %w", err)
		}
		bundle := artifacts[file.Organization][file.Name][file.Identifier]
		bundle.Files = append(bundle.Files, file)
		artifacts[file.Organization][file.Name][file.Identifier] = bundle
	}
	if err := fileRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cookbook artifact file rows: %w", err)
	}

	return artifacts, nil
}

func compareCookbookVersions(left, right string) int {
	if left == right {
		return 0
	}

	leftParts := strings.Split(strings.TrimSpace(left), ".")
	rightParts := strings.Split(strings.TrimSpace(right), ".")

	for len(leftParts) < 3 {
		leftParts = append(leftParts, "0")
	}
	for len(rightParts) < 3 {
		rightParts = append(rightParts, "0")
	}

	for idx := 0; idx < 3; idx++ {
		if cmp := compareCookbookVersionPart(leftParts[idx], rightParts[idx]); cmp != 0 {
			return cmp
		}
	}

	leftTail := leftParts[3:]
	rightTail := rightParts[3:]
	switch {
	case len(leftTail) == 0 && len(rightTail) == 0:
		return 0
	case len(leftTail) == 0:
		return 1
	case len(rightTail) == 0:
		return -1
	}

	for idx := 0; idx < len(leftTail) && idx < len(rightTail); idx++ {
		if cmp := compareCookbookVersionPart(leftTail[idx], rightTail[idx]); cmp != 0 {
			return cmp
		}
	}

	switch {
	case len(leftTail) < len(rightTail):
		return -1
	case len(leftTail) > len(rightTail):
		return 1
	default:
		return 0
	}
}

func compareCookbookVersionPart(left, right string) int {
	leftInt, leftErr := strconv.ParseInt(left, 10, 64)
	rightInt, rightErr := strconv.ParseInt(right, 10, 64)
	switch {
	case leftErr == nil && rightErr == nil:
		switch {
		case leftInt < rightInt:
			return -1
		case leftInt > rightInt:
			return 1
		default:
			return 0
		}
	case leftErr == nil:
		return -1
	case rightErr == nil:
		return 1
	default:
		switch {
		case left < right:
			return -1
		case left > right:
			return 1
		default:
			return 0
		}
	}
}

func sortCookbookVersions(versions []bootstrap.CookbookVersion) {
	sort.Slice(versions, func(i, j int) bool {
		return compareCookbookVersions(versions[i].Version, versions[j].Version) > 0
	})
}

func sortCookbookArtifacts(artifacts []bootstrap.CookbookArtifact) {
	sort.Slice(artifacts, func(i, j int) bool {
		if artifacts[i].Name != artifacts[j].Name {
			return artifacts[i].Name < artifacts[j].Name
		}
		if cmp := compareCookbookVersions(artifacts[i].Version, artifacts[j].Version); cmp != 0 {
			return cmp > 0
		}
		return artifacts[i].Identifier < artifacts[j].Identifier
	})
}
