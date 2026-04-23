package bootstrap

import "strings"

type CookbookStore interface {
	HasCookbookVersion(orgName, name, version string) (bool, bool)
	ListCookbookArtifacts(orgName string) (map[string][]CookbookArtifact, bool)
	ListCookbookArtifactsByName(orgName, name string) ([]CookbookArtifact, bool, bool)
	GetCookbookArtifact(orgName, name, identifier string) (CookbookArtifact, bool, bool)
	CreateCookbookArtifact(orgName string, artifact CookbookArtifact) (CookbookArtifact, error)
	DeleteCookbookArtifactWithReleasedChecksums(orgName, name, identifier string) (CookbookArtifact, []string, error)
	ListCookbookVersions(orgName string) (map[string][]CookbookVersionRef, bool)
	ListCookbookVersionsByName(orgName, name string) ([]CookbookVersionRef, bool, bool)
	ListCookbookVersionModelsByName(orgName, name string) ([]CookbookVersion, bool, bool)
	GetCookbookVersion(orgName, name, version string) (CookbookVersion, bool, bool)
	UpsertCookbookVersionWithReleasedChecksums(orgName string, version CookbookVersion, force bool) (CookbookVersion, []string, bool, error)
	DeleteCookbookVersionWithReleasedChecksums(orgName, name, version string) (CookbookVersion, []string, error)
	DeleteCookbookChecksumReferencesFromRemaining(remaining map[string]struct{})
	CookbookChecksumReferenced(checksum string) bool
}

type cookbookStoreOrganizationRegistrar interface {
	EnsureOrganization(org Organization)
}

type memoryCookbookStore struct {
	service *Service
}

func newMemoryCookbookStore(service *Service) CookbookStore {
	return &memoryCookbookStore{service: service}
}

func (s *memoryCookbookStore) HasCookbookVersion(orgName, name, version string) (bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return false, false
	}

	versions, ok := org.cookbooks[strings.TrimSpace(name)]
	if !ok {
		return false, true
	}

	_, exists := versions[strings.TrimSpace(version)]
	return exists, true
}

func (s *memoryCookbookStore) ListCookbookArtifacts(orgName string) (map[string][]CookbookArtifact, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]CookbookArtifact, len(org.cookbookArtifacts))
	for name, versions := range org.cookbookArtifacts {
		out[name] = sortedCookbookArtifacts(versions)
	}
	return out, true
}

func (s *memoryCookbookStore) ListCookbookArtifactsByName(orgName, name string) ([]CookbookArtifact, bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}

	return sortedCookbookArtifacts(versions), true, true
}

func (s *memoryCookbookStore) GetCookbookArtifact(orgName, name, identifier string) (CookbookArtifact, bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookArtifact{}, false, false
	}

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return CookbookArtifact{}, true, false
	}

	artifact, ok := versions[strings.TrimSpace(identifier)]
	if !ok {
		return CookbookArtifact{}, true, false
	}

	return copyCookbookArtifact(artifact), true, true
}

func (s *memoryCookbookStore) CreateCookbookArtifact(orgName string, artifact CookbookArtifact) (CookbookArtifact, error) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookArtifact{}, ErrNotFound
	}

	versions := org.cookbookArtifacts[artifact.Name]
	if versions == nil {
		versions = make(map[string]CookbookArtifact)
		org.cookbookArtifacts[artifact.Name] = versions
	}
	if _, exists := versions[artifact.Identifier]; exists {
		return CookbookArtifact{}, ErrConflict
	}

	versions[artifact.Identifier] = artifact
	return copyCookbookArtifact(artifact), nil
}

func (s *memoryCookbookStore) DeleteCookbookArtifactWithReleasedChecksums(orgName, name, identifier string) (CookbookArtifact, []string, error) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookArtifact{}, nil, ErrNotFound
	}

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return CookbookArtifact{}, nil, ErrNotFound
	}

	artifact, ok := versions[strings.TrimSpace(identifier)]
	if !ok {
		return CookbookArtifact{}, nil, ErrNotFound
	}

	delete(versions, artifact.Identifier)
	if len(versions) == 0 {
		delete(org.cookbookArtifacts, artifact.Name)
	}

	return copyCookbookArtifact(artifact), s.service.unreferencedChecksumsLocked(cookbookFileChecksums(artifact.AllFiles)), nil
}

func (s *memoryCookbookStore) ListCookbookVersions(orgName string) (map[string][]CookbookVersionRef, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]CookbookVersionRef, len(org.cookbooks))
	for name, versions := range org.cookbooks {
		out[name] = cookbookVersionRefs(versions)
	}
	return out, true
}

func (s *memoryCookbookStore) ListCookbookVersionsByName(orgName, name string) ([]CookbookVersionRef, bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	versions, ok := org.cookbooks[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}

	return cookbookVersionRefs(versions), true, true
}

func (s *memoryCookbookStore) ListCookbookVersionModelsByName(orgName, name string) ([]CookbookVersion, bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	versions, ok := org.cookbooks[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}

	return sortedCookbookVersions(versions), true, true
}

func (s *memoryCookbookStore) GetCookbookVersion(orgName, name, version string) (CookbookVersion, bool, bool) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookVersion{}, false, false
	}

	versions, ok := org.cookbooks[strings.TrimSpace(name)]
	if !ok {
		return CookbookVersion{}, true, false
	}

	version = strings.TrimSpace(version)
	if version == "_latest" || version == "latest" {
		refs := cookbookVersionRefs(versions)
		if len(refs) == 0 {
			return CookbookVersion{}, true, false
		}
		version = refs[0].Version
	}

	cookbookVersion, ok := versions[version]
	if !ok {
		return CookbookVersion{}, true, false
	}

	return copyCookbookVersion(cookbookVersion), true, true
}

func (s *memoryCookbookStore) UpsertCookbookVersionWithReleasedChecksums(orgName string, version CookbookVersion, force bool) (CookbookVersion, []string, bool, error) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookVersion{}, nil, false, ErrNotFound
	}

	versions := org.cookbooks[version.CookbookName]
	if versions == nil {
		versions = make(map[string]CookbookVersion)
		org.cookbooks[version.CookbookName] = versions
	}
	existing, exists := versions[version.Version]
	if exists && existing.Frozen && !force {
		return CookbookVersion{}, nil, false, &FrozenCookbookError{
			Name:    existing.CookbookName,
			Version: existing.Version,
		}
	}
	if exists && existing.Frozen {
		version.Frozen = true
	}
	versions[version.Version] = version
	released := []string(nil)
	if exists {
		released = s.service.unreferencedChecksumsLocked(cookbookChecksumDifference(existing.AllFiles, version.AllFiles))
	}
	return copyCookbookVersion(version), released, !exists, nil
}

func (s *memoryCookbookStore) DeleteCookbookVersionWithReleasedChecksums(orgName, name, version string) (CookbookVersion, []string, error) {
	org, ok := s.service.orgs[orgName]
	if !ok {
		return CookbookVersion{}, nil, ErrNotFound
	}

	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	versions, ok := org.cookbooks[name]
	if !ok {
		return CookbookVersion{}, nil, ErrNotFound
	}

	if version == "_latest" || version == "latest" {
		refs := cookbookVersionRefs(versions)
		if len(refs) == 0 {
			return CookbookVersion{}, nil, ErrNotFound
		}
		version = refs[0].Version
	}

	cookbookVersion, ok := versions[version]
	if !ok {
		return CookbookVersion{}, nil, ErrNotFound
	}

	delete(versions, version)
	if len(versions) == 0 {
		delete(org.cookbooks, name)
	}

	return copyCookbookVersion(cookbookVersion), s.service.unreferencedChecksumsLocked(cookbookFileChecksums(cookbookVersion.AllFiles)), nil
}

func (s *memoryCookbookStore) DeleteCookbookChecksumReferencesFromRemaining(remaining map[string]struct{}) {
	for _, org := range s.service.orgs {
		for _, versions := range org.cookbooks {
			for _, version := range versions {
				deleteCookbookFileReferencesFromRemaining(remaining, version.AllFiles)
			}
		}
		for _, artifacts := range org.cookbookArtifacts {
			for _, artifact := range artifacts {
				deleteCookbookFileReferencesFromRemaining(remaining, artifact.AllFiles)
			}
		}
		if len(remaining) == 0 {
			return
		}
	}
}

func (s *memoryCookbookStore) CookbookChecksumReferenced(checksum string) bool {
	for _, org := range s.service.orgs {
		for _, versions := range org.cookbooks {
			for _, version := range versions {
				if cookbookFilesContainChecksum(version.AllFiles, checksum) {
					return true
				}
			}
		}
		for _, artifacts := range org.cookbookArtifacts {
			for _, artifact := range artifacts {
				if cookbookFilesContainChecksum(artifact.AllFiles, checksum) {
					return true
				}
			}
		}
	}
	return false
}
