package pg

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

type cookbookStore struct {
	repo *CookbookRepository
}

func (s *Store) CookbookStore() bootstrap.CookbookStore {
	if s == nil {
		return nil
	}
	return &cookbookStore{repo: s.Cookbooks()}
}

func (s *cookbookStore) EnsureOrganization(orgName string) {
	if s == nil || s.repo == nil {
		return
	}
	s.repo.ensureOrganization(orgName)
}

func (s *cookbookStore) HasCookbookVersion(orgName, name, version string) (bool, bool) {
	if !s.organizationExists(orgName) {
		return false, false
	}

	versions := s.repo.listCookbookVersionsByName(orgName, name)
	if len(versions) == 0 {
		return false, true
	}

	version = strings.TrimSpace(version)
	for _, current := range versions {
		if current.Version == version {
			return true, true
		}
	}
	return false, true
}

func (s *cookbookStore) ListCookbookArtifacts(orgName string) (map[string][]bootstrap.CookbookArtifact, bool) {
	if !s.organizationExists(orgName) {
		return nil, false
	}
	return s.repo.listCookbookArtifacts(orgName), true
}

func (s *cookbookStore) ListCookbookArtifactsByName(orgName, name string) ([]bootstrap.CookbookArtifact, bool, bool) {
	if !s.organizationExists(orgName) {
		return nil, false, false
	}

	artifacts := s.repo.listCookbookArtifactsByName(orgName, name)
	if len(artifacts) == 0 {
		return nil, true, false
	}
	return artifacts, true, true
}

func (s *cookbookStore) GetCookbookArtifact(orgName, name, identifier string) (bootstrap.CookbookArtifact, bool, bool) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookArtifact{}, false, false
	}

	artifact, ok := s.repo.getCookbookArtifact(orgName, name, identifier)
	if !ok {
		return bootstrap.CookbookArtifact{}, true, false
	}
	return artifact, true, true
}

func (s *cookbookStore) CreateCookbookArtifact(orgName string, artifact bootstrap.CookbookArtifact) (bootstrap.CookbookArtifact, error) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookArtifact{}, bootstrap.ErrNotFound
	}

	if _, ok := s.repo.getCookbookArtifact(orgName, artifact.Name, artifact.Identifier); ok {
		return bootstrap.CookbookArtifact{}, bootstrap.ErrConflict
	}

	if err := s.repo.putCookbookArtifact(orgName, artifact); err != nil {
		return bootstrap.CookbookArtifact{}, fmt.Errorf("persist cookbook artifact: %w", err)
	}
	return artifact, nil
}

func (s *cookbookStore) DeleteCookbookArtifactWithReleasedChecksums(orgName, name, identifier string) (bootstrap.CookbookArtifact, []string, error) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookArtifact{}, nil, bootstrap.ErrNotFound
	}

	artifact, ok, err := s.repo.deleteCookbookArtifact(orgName, name, identifier)
	if err != nil {
		return bootstrap.CookbookArtifact{}, nil, err
	}
	if !ok {
		return bootstrap.CookbookArtifact{}, nil, bootstrap.ErrNotFound
	}

	return artifact, s.unreferencedChecksums(cookbookFileChecksums(artifact.AllFiles)), nil
}

func (s *cookbookStore) ListCookbookVersions(orgName string) (map[string][]bootstrap.CookbookVersionRef, bool) {
	if !s.organizationExists(orgName) {
		return nil, false
	}

	models := s.repo.listCookbookVersions(orgName)
	out := make(map[string][]bootstrap.CookbookVersionRef, len(models))
	for name, versions := range models {
		out[name] = cookbookVersionRefs(versions)
	}
	return out, true
}

func (s *cookbookStore) ListCookbookVersionsByName(orgName, name string) ([]bootstrap.CookbookVersionRef, bool, bool) {
	if !s.organizationExists(orgName) {
		return nil, false, false
	}

	versions := s.repo.listCookbookVersionsByName(orgName, name)
	if len(versions) == 0 {
		return nil, true, false
	}
	return cookbookVersionRefs(versions), true, true
}

func (s *cookbookStore) ListCookbookVersionModelsByName(orgName, name string) ([]bootstrap.CookbookVersion, bool, bool) {
	if !s.organizationExists(orgName) {
		return nil, false, false
	}

	versions := s.repo.listCookbookVersionsByName(orgName, name)
	if len(versions) == 0 {
		return nil, true, false
	}
	return versions, true, true
}

func (s *cookbookStore) GetCookbookVersion(orgName, name, version string) (bootstrap.CookbookVersion, bool, bool) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookVersion{}, false, false
	}

	version = strings.TrimSpace(version)
	if version == "_latest" || version == "latest" {
		versions := s.repo.listCookbookVersionsByName(orgName, name)
		if len(versions) == 0 {
			return bootstrap.CookbookVersion{}, true, false
		}
		version = versions[0].Version
	}

	cookbookVersion, ok := s.repo.getCookbookVersion(orgName, name, version)
	if !ok {
		return bootstrap.CookbookVersion{}, true, false
	}
	return cookbookVersion, true, true
}

func (s *cookbookStore) UpsertCookbookVersionWithReleasedChecksums(orgName string, version bootstrap.CookbookVersion, force bool) (bootstrap.CookbookVersion, []string, bool, error) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookVersion{}, nil, false, bootstrap.ErrNotFound
	}

	existing, exists := s.repo.getCookbookVersion(orgName, version.CookbookName, version.Version)
	if exists && existing.Frozen && !force {
		return bootstrap.CookbookVersion{}, nil, false, &bootstrap.FrozenCookbookError{
			Name:    existing.CookbookName,
			Version: existing.Version,
		}
	}
	if exists && existing.Frozen {
		version.Frozen = true
	}

	if err := s.repo.putCookbookVersion(orgName, version); err != nil {
		return bootstrap.CookbookVersion{}, nil, false, fmt.Errorf("persist cookbook version: %w", err)
	}

	released := []string(nil)
	if exists {
		released = s.unreferencedChecksums(cookbookChecksumDifference(existing.AllFiles, version.AllFiles))
	}
	return version, released, !exists, nil
}

func (s *cookbookStore) DeleteCookbookVersionWithReleasedChecksums(orgName, name, version string) (bootstrap.CookbookVersion, []string, error) {
	if !s.organizationExists(orgName) {
		return bootstrap.CookbookVersion{}, nil, bootstrap.ErrNotFound
	}

	version = strings.TrimSpace(version)
	if version == "_latest" || version == "latest" {
		versions := s.repo.listCookbookVersionsByName(orgName, name)
		if len(versions) == 0 {
			return bootstrap.CookbookVersion{}, nil, bootstrap.ErrNotFound
		}
		version = versions[0].Version
	}

	cookbookVersion, ok, err := s.repo.deleteCookbookVersion(orgName, name, version)
	if err != nil {
		return bootstrap.CookbookVersion{}, nil, err
	}
	if !ok {
		return bootstrap.CookbookVersion{}, nil, bootstrap.ErrNotFound
	}
	return cookbookVersion, s.unreferencedChecksums(cookbookFileChecksums(cookbookVersion.AllFiles)), nil
}

func (s *cookbookStore) DeleteCookbookChecksumReferencesFromRemaining(remaining map[string]struct{}) {
	if s == nil || s.repo == nil || len(remaining) == 0 {
		return
	}

	s.repo.mu.RLock()
	defer s.repo.mu.RUnlock()

	for _, byName := range s.repo.versions {
		for _, byVersion := range byName {
			for _, bundle := range byVersion {
				version, err := s.repo.DecodeCookbookVersion(bundle)
				if err != nil {
					continue
				}
				deleteCookbookFileReferencesFromRemaining(remaining, version.AllFiles)
			}
		}
	}

	for _, byName := range s.repo.artifacts {
		for _, byIdentifier := range byName {
			for _, bundle := range byIdentifier {
				artifact, err := s.repo.DecodeCookbookArtifact(bundle)
				if err != nil {
					continue
				}
				deleteCookbookFileReferencesFromRemaining(remaining, artifact.AllFiles)
			}
		}
	}
}

func (s *cookbookStore) CookbookChecksumReferenced(checksum string) bool {
	remaining := normalizedChecksumCandidateSet([]string{checksum})
	if len(remaining) == 0 {
		return false
	}
	s.DeleteCookbookChecksumReferencesFromRemaining(remaining)
	return len(remaining) == 0
}

func (s *cookbookStore) organizationExists(orgName string) bool {
	if s == nil || s.repo == nil {
		return false
	}
	return s.repo.organizationExists(orgName)
}

func (s *cookbookStore) unreferencedChecksums(candidates []string) []string {
	if len(candidates) == 0 {
		return nil
	}

	remaining := normalizedChecksumCandidateSet(candidates)
	if len(remaining) == 0 {
		return nil
	}

	s.DeleteCookbookChecksumReferencesFromRemaining(remaining)
	if len(remaining) == 0 {
		return nil
	}

	out := make([]string, 0, len(remaining))
	for checksum := range remaining {
		out = append(out, checksum)
	}
	sort.Strings(out)
	return out
}

func cookbookVersionRefs(versions []bootstrap.CookbookVersion) []bootstrap.CookbookVersionRef {
	if len(versions) == 0 {
		return nil
	}

	out := make([]bootstrap.CookbookVersionRef, 0, len(versions))
	for _, version := range versions {
		out = append(out, bootstrap.CookbookVersionRef{
			Name:    version.CookbookName,
			Version: version.Version,
		})
	}
	return out
}

func cookbookFileChecksums(files []bootstrap.CookbookFile) []string {
	if len(files) == 0 {
		return nil
	}

	set := make(map[string]struct{}, len(files))
	for _, file := range files {
		checksum := strings.ToLower(strings.TrimSpace(file.Checksum))
		if checksum == "" {
			continue
		}
		set[checksum] = struct{}{}
	}
	return checksumSetKeys(set)
}

func cookbookChecksumDifference(existing, updated []bootstrap.CookbookFile) []string {
	updatedSet := make(map[string]struct{}, len(updated))
	for _, checksum := range cookbookFileChecksums(updated) {
		updatedSet[checksum] = struct{}{}
	}

	released := make(map[string]struct{})
	for _, checksum := range cookbookFileChecksums(existing) {
		if _, ok := updatedSet[checksum]; ok {
			continue
		}
		released[checksum] = struct{}{}
	}
	return checksumSetKeys(released)
}

func checksumSetKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}

	out := make([]string, 0, len(set))
	for checksum := range set {
		out = append(out, checksum)
	}
	sort.Strings(out)
	return out
}

func normalizedChecksumCandidateSet(candidates []string) map[string]struct{} {
	remaining := make(map[string]struct{}, len(candidates))
	for _, checksum := range candidates {
		checksum = strings.ToLower(strings.TrimSpace(checksum))
		if checksum == "" {
			continue
		}
		remaining[checksum] = struct{}{}
	}
	if len(remaining) == 0 {
		return nil
	}
	return remaining
}

func deleteCookbookFileReferencesFromRemaining(remaining map[string]struct{}, files []bootstrap.CookbookFile) {
	for _, file := range files {
		delete(remaining, strings.ToLower(strings.TrimSpace(file.Checksum)))
	}
}
