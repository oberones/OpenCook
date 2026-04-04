package bootstrap

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var (
	validCookbookArtifactIdentifierPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
	validCookbookVersionPattern            = regexp.MustCompile(`^\d+(?:\.\d+){0,2}(?:\.[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*)?$`)
	validCookbookRouteVersionPattern       = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
)

var allowedCookbookArtifactKeys = map[string]struct{}{
	"name":        {},
	"identifier":  {},
	"version":     {},
	"chef_type":   {},
	"frozen?":     {},
	"metadata":    {},
	"all_files":   {},
	"recipes":     {},
	"definitions": {},
	"libraries":   {},
	"attributes":  {},
	"files":       {},
	"templates":   {},
	"resources":   {},
	"providers":   {},
	"root_files":  {},
}

var allowedCookbookKeys = map[string]struct{}{
	"name":          {},
	"cookbook_name": {},
	"version":       {},
	"json_class":    {},
	"chef_type":     {},
	"frozen?":       {},
	"metadata":      {},
	"all_files":     {},
	"recipes":       {},
	"definitions":   {},
	"libraries":     {},
	"attributes":    {},
	"files":         {},
	"templates":     {},
	"resources":     {},
	"providers":     {},
	"root_files":    {},
}

var cookbookLegacySegments = []string{
	"recipes",
	"definitions",
	"libraries",
	"attributes",
	"files",
	"templates",
	"resources",
	"providers",
	"root_files",
}

type CookbookFile struct {
	Name        string `json:"name"`
	Path        string `json:"path"`
	Checksum    string `json:"checksum"`
	Specificity string `json:"specificity"`
}

type CookbookArtifact struct {
	Name       string         `json:"name"`
	Identifier string         `json:"identifier"`
	Version    string         `json:"version"`
	ChefType   string         `json:"chef_type"`
	Frozen     bool           `json:"frozen"`
	Metadata   map[string]any `json:"metadata"`
	AllFiles   []CookbookFile `json:"all_files"`
}

type CookbookVersion struct {
	Name         string         `json:"name"`
	CookbookName string         `json:"cookbook_name"`
	Version      string         `json:"version"`
	JSONClass    string         `json:"json_class"`
	ChefType     string         `json:"chef_type"`
	Frozen       bool           `json:"frozen"`
	Metadata     map[string]any `json:"metadata"`
	AllFiles     []CookbookFile `json:"all_files"`
}

type CookbookVersionRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type UniverseEntry struct {
	Version      string
	Dependencies map[string]string
}

type CreateCookbookArtifactInput struct {
	Name           string
	Identifier     string
	Payload        map[string]any
	ChecksumExists func(string) (bool, error)
}

type UpsertCookbookVersionInput struct {
	Name           string
	Version        string
	Payload        map[string]any
	Force          bool
	ChecksumExists func(string) (bool, error)
}

type FrozenCookbookError struct {
	Name    string
	Version string
}

func (e *FrozenCookbookError) Error() string {
	return fmt.Sprintf("The cookbook %s at version %s is frozen. Use the 'force' option to override.", e.Name, e.Version)
}

func (e *FrozenCookbookError) Unwrap() error {
	return ErrConflict
}

type MissingChecksumError struct {
	Checksum string
}

func (e *MissingChecksumError) Error() string {
	if strings.TrimSpace(e.Checksum) == "" {
		return "Manifest has a checksum that hasn't been uploaded."
	}
	return fmt.Sprintf("Manifest has checksum %s but it hasn't yet been uploaded", e.Checksum)
}

func (s *Service) ListCookbookArtifacts(orgName string) (map[string][]CookbookArtifact, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]CookbookArtifact, len(org.cookbookArtifacts))
	for name, versions := range org.cookbookArtifacts {
		out[name] = sortedCookbookArtifacts(versions)
	}
	return out, true
}

func (s *Service) ListCookbookArtifactsByName(orgName, name string) ([]CookbookArtifact, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}

	return sortedCookbookArtifacts(versions), true, true
}

func (s *Service) GetCookbookArtifact(orgName, name, identifier string) (CookbookArtifact, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
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

func (s *Service) CreateCookbookArtifact(orgName string, input CreateCookbookArtifactInput) (CookbookArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return CookbookArtifact{}, ErrNotFound
	}

	artifact, err := normalizeCookbookArtifactPayload(input.Name, input.Identifier, input.Payload, input.ChecksumExists)
	if err != nil {
		return CookbookArtifact{}, err
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

func (s *Service) DeleteCookbookArtifact(orgName, name, identifier string) (CookbookArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return CookbookArtifact{}, ErrNotFound
	}

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return CookbookArtifact{}, ErrNotFound
	}

	artifact, ok := versions[strings.TrimSpace(identifier)]
	if !ok {
		return CookbookArtifact{}, ErrNotFound
	}

	delete(versions, artifact.Identifier)
	if len(versions) == 0 {
		delete(org.cookbookArtifacts, artifact.Name)
	}

	return copyCookbookArtifact(artifact), nil
}

func (s *Service) ListCookbookVersions(orgName string) (map[string][]CookbookVersionRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]CookbookVersionRef, len(org.cookbooks))
	for name, versions := range org.cookbooks {
		out[name] = cookbookVersionRefs(versions)
	}
	return out, true
}

func (s *Service) ListCookbookVersionsByName(orgName, name string) ([]CookbookVersionRef, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	versions, ok := org.cookbooks[strings.TrimSpace(name)]
	if !ok {
		return nil, true, false
	}

	return cookbookVersionRefs(versions), true, true
}

func (s *Service) GetCookbookVersion(orgName, name, version string) (CookbookVersion, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
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
	if ok {
		return copyCookbookVersion(cookbookVersion), true, true
	}

	return CookbookVersion{}, true, false
}

func (s *Service) CookbookUniverse(orgName string) (map[string][]UniverseEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]UniverseEntry, len(org.cookbooks))
	for name, versions := range org.cookbooks {
		refs := cookbookVersionRefs(versions)
		entries := make([]UniverseEntry, 0, len(refs))
		for _, ref := range refs {
			version, ok := versions[ref.Version]
			if !ok {
				continue
			}
			entries = append(entries, UniverseEntry{
				Version:      version.Version,
				Dependencies: cookbookMetadataDependencies(version.Metadata),
			})
		}
		out[name] = entries
	}
	return out, true
}

func (s *Service) UpsertCookbookVersion(orgName string, input UpsertCookbookVersionInput) (CookbookVersion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return CookbookVersion{}, false, ErrNotFound
	}

	version, err := normalizeCookbookVersionPayload(input.Name, input.Version, input.Payload, input.ChecksumExists)
	if err != nil {
		return CookbookVersion{}, false, err
	}

	versions := org.cookbooks[version.CookbookName]
	if versions == nil {
		versions = make(map[string]CookbookVersion)
		org.cookbooks[version.CookbookName] = versions
	}
	existing, exists := versions[version.Version]
	if exists && existing.Frozen && !input.Force {
		return CookbookVersion{}, false, &FrozenCookbookError{
			Name:    existing.CookbookName,
			Version: existing.Version,
		}
	}
	if exists && existing.Frozen {
		version.Frozen = true
	}
	versions[version.Version] = version
	return copyCookbookVersion(version), !exists, nil
}

func (s *Service) DeleteCookbookVersion(orgName, name, version string) (CookbookVersion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return CookbookVersion{}, ErrNotFound
	}

	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	versions, ok := org.cookbooks[name]
	if !ok {
		return CookbookVersion{}, ErrNotFound
	}

	if version == "_latest" || version == "latest" {
		refs := cookbookVersionRefs(versions)
		if len(refs) == 0 {
			return CookbookVersion{}, ErrNotFound
		}
		version = refs[0].Version
	}

	cookbookVersion, ok := versions[version]
	if !ok {
		return CookbookVersion{}, ErrNotFound
	}

	delete(versions, version)
	if len(versions) == 0 {
		delete(org.cookbooks, name)
	}

	return copyCookbookVersion(cookbookVersion), nil
}

func normalizeCookbookVersionPayload(name, version string, payload map[string]any, checksumExists func(string) (bool, error)) (CookbookVersion, error) {
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	if !validNamePattern.MatchString(name) {
		return CookbookVersion{}, &ValidationError{Messages: []string{invalidCookbookNameMessage(name)}}
	}
	if !validCookbookRouteVersion(version) {
		return CookbookVersion{}, &ValidationError{Messages: []string{invalidCookbookVersionMessage(version)}}
	}

	for key := range payload {
		if _, ok := allowedCookbookKeys[key]; !ok {
			return CookbookVersion{}, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key %s in request body", key)}}
		}
	}

	expectedName := name + "-" + version
	rawName, ok := payload["name"]
	if !ok {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	payloadName, ok := rawName.(string)
	if !ok || strings.TrimSpace(payloadName) != expectedName {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}

	rawCookbookName, ok := payload["cookbook_name"]
	if !ok {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'cookbook_name' missing"}}
	}
	payloadCookbookName, ok := rawCookbookName.(string)
	if !ok {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'cookbook_name' invalid"}}
	}
	payloadCookbookName = strings.TrimSpace(payloadCookbookName)
	if !validNamePattern.MatchString(payloadCookbookName) || payloadCookbookName != name {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'cookbook_name' invalid"}}
	}

	if rawVersion, ok := payload["version"]; ok {
		payloadVersion, ok := rawVersion.(string)
		if !ok {
			return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'version' invalid"}}
		}
		payloadVersion = strings.TrimSpace(payloadVersion)
		if !validCookbookRouteVersion(payloadVersion) || payloadVersion != version {
			return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'version' invalid"}}
		}
	}

	jsonClass := "Chef::CookbookVersion"
	if rawJSONClass, ok := payload["json_class"]; ok {
		text, ok := rawJSONClass.(string)
		if !ok || strings.TrimSpace(text) != jsonClass {
			return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'json_class' invalid"}}
		}
	}
	if rawChefType, ok := payload["chef_type"]; ok {
		text, ok := rawChefType.(string)
		if !ok || strings.TrimSpace(text) != "cookbook_version" {
			return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'chef_type' invalid"}}
		}
	}

	metadataValue, ok := payload["metadata"]
	if !ok {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'metadata.version' missing"}}
	}
	metadata, err := normalizeCookbookMetadata(metadataValue)
	if err != nil {
		return CookbookVersion{}, err
	}

	metadataVersion, ok := metadata["version"].(string)
	if !ok || strings.TrimSpace(metadataVersion) != version {
		return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'metadata.version' invalid"}}
	}

	allFiles, err := normalizeCookbookFiles(payload, checksumExists)
	if err != nil {
		return CookbookVersion{}, err
	}

	cookbookVersion := CookbookVersion{
		Name:         expectedName,
		CookbookName: name,
		Version:      version,
		JSONClass:    jsonClass,
		ChefType:     "cookbook_version",
		Frozen:       false,
		Metadata:     metadata,
		AllFiles:     allFiles,
	}
	if frozenValue, ok := payload["frozen?"]; ok {
		frozen, ok := frozenValue.(bool)
		if !ok {
			return CookbookVersion{}, &ValidationError{Messages: []string{"Field 'frozen?' invalid"}}
		}
		cookbookVersion.Frozen = frozen
	}

	return cookbookVersion, nil
}

func normalizeCookbookArtifactPayload(name, identifier string, payload map[string]any, checksumExists func(string) (bool, error)) (CookbookArtifact, error) {
	name = strings.TrimSpace(name)
	identifier = strings.ToLower(strings.TrimSpace(identifier))
	if !validNamePattern.MatchString(name) {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	if !validCookbookArtifactIdentifierPattern.MatchString(identifier) {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'identifier' invalid"}}
	}

	for key := range payload {
		if _, ok := allowedCookbookArtifactKeys[key]; !ok {
			return CookbookArtifact{}, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key %s in request body", key)}}
		}
	}

	rawName, ok := payload["name"]
	if !ok {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	}
	payloadName, ok := rawName.(string)
	if !ok || !validNamePattern.MatchString(strings.TrimSpace(payloadName)) {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	payloadName = strings.TrimSpace(payloadName)
	if payloadName != name {
		return CookbookArtifact{}, &ValidationError{Messages: []string{fmt.Sprintf("Field 'name' invalid : %s does not match %s", name, payloadName)}}
	}

	rawIdentifier, ok := payload["identifier"]
	if !ok {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'identifier' missing"}}
	}
	payloadIdentifier, ok := rawIdentifier.(string)
	if !ok || !validCookbookArtifactIdentifierPattern.MatchString(strings.ToLower(strings.TrimSpace(payloadIdentifier))) {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'identifier' invalid"}}
	}
	payloadIdentifier = strings.ToLower(strings.TrimSpace(payloadIdentifier))
	if payloadIdentifier != identifier {
		return CookbookArtifact{}, &ValidationError{Messages: []string{fmt.Sprintf("Field 'identifier' invalid : %s does not match %s", identifier, payloadIdentifier)}}
	}

	metadataValue, ok := payload["metadata"]
	if !ok {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'metadata' missing"}}
	}
	metadata, err := normalizeCookbookMetadata(metadataValue)
	if err != nil {
		return CookbookArtifact{}, err
	}

	version, ok := metadata["version"].(string)
	if !ok || !validCookbookVersion(version) {
		return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'metadata.version' missing"}}
	}
	version = strings.TrimSpace(version)

	if rawVersion, ok := payload["version"]; ok {
		payloadVersion, ok := rawVersion.(string)
		if !ok || !validCookbookVersion(payloadVersion) {
			return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'version' invalid"}}
		}
		if strings.TrimSpace(payloadVersion) != version {
			return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'version' invalid"}}
		}
	}

	allFiles, err := normalizeCookbookFiles(payload, checksumExists)
	if err != nil {
		return CookbookArtifact{}, err
	}

	artifact := CookbookArtifact{
		Name:       name,
		Identifier: identifier,
		Version:    version,
		ChefType:   "cookbook_version",
		Frozen:     false,
		Metadata:   metadata,
		AllFiles:   allFiles,
	}
	if frozenValue, ok := payload["frozen?"]; ok {
		frozen, ok := frozenValue.(bool)
		if !ok {
			return CookbookArtifact{}, &ValidationError{Messages: []string{"Field 'frozen?' invalid"}}
		}
		artifact.Frozen = frozen
	}

	return artifact, nil
}

func normalizeCookbookMetadata(value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{"Field 'metadata' invalid"}}
	}

	metadata := cloneMap(raw)
	if version, ok := metadata["version"]; !ok {
		return nil, &ValidationError{Messages: []string{"Field 'metadata.version' missing"}}
	} else if versionString, ok := version.(string); !ok || !validCookbookVersion(versionString) {
		return nil, &ValidationError{Messages: []string{"Field 'metadata.version' invalid"}}
	} else {
		metadata["version"] = strings.TrimSpace(versionString)
	}
	if rawName, ok := metadata["name"]; ok {
		name, ok := rawName.(string)
		if !ok || !validNamePattern.MatchString(strings.TrimSpace(name)) {
			return nil, &ValidationError{Messages: []string{"Field 'metadata.name' invalid"}}
		}
		metadata["name"] = strings.TrimSpace(name)
	}
	for _, field := range []string{"description", "long_description", "maintainer", "maintainer_email", "license"} {
		if rawField, ok := metadata[field]; ok {
			text, ok := rawField.(string)
			if !ok {
				return nil, &ValidationError{Messages: []string{fmt.Sprintf("Field 'metadata.%s' invalid", field)}}
			}
			metadata[field] = text
		}
	}

	for _, section := range []string{"attributes", "recipes"} {
		if rawSection, ok := metadata[section]; ok {
			normalized, err := normalizeCookbookMetadataMap(section, rawSection)
			if err != nil {
				return nil, err
			}
			metadata[section] = normalized
		}
	}
	for _, section := range []string{"platforms", "dependencies", "recommendations", "suggestions", "conflicting", "replacing"} {
		if rawSection, ok := metadata[section]; ok {
			normalized, err := normalizeCookbookConstraintMap(section, rawSection)
			if err != nil {
				return nil, err
			}
			metadata[section] = normalized
		}
	}
	if rawProviding, ok := metadata["providing"]; ok {
		metadata["providing"] = cloneValue(rawProviding)
	}

	return metadata, nil
}

func normalizeCookbookMetadataMap(section string, value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{fmt.Sprintf("Field 'metadata.%s' invalid", section)}}
	}

	out := make(map[string]any, len(raw))
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = cloneValue(raw[key])
	}
	return out, nil
}

func normalizeCookbookConstraintMap(section string, value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{fmt.Sprintf("Field 'metadata.%s' invalid", section)}}
	}

	out := make(map[string]any, len(raw))
	keys := make([]string, 0, len(raw))
	for key := range raw {
		if !validNamePattern.MatchString(key) {
			return nil, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key '%s' for metadata.%s", key, section)}}
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		text, ok := raw[key].(string)
		if !ok || !validCookbookConstraintPattern.MatchString(text) {
			return nil, &ValidationError{Messages: []string{fmt.Sprintf("Invalid value '%s' for metadata.%s", cookbookMetadataValueString(raw[key]), section)}}
		}
		out[key] = text
	}
	return out, nil
}

func normalizeCookbookFiles(payload map[string]any, checksumExists func(string) (bool, error)) ([]CookbookFile, error) {
	if allFilesRaw, ok := payload["all_files"]; ok {
		files, err := normalizeCookbookFileList("all_files", allFilesRaw, checksumExists)
		if err != nil {
			return nil, err
		}
		sortCookbookFiles(files)
		return files, nil
	}

	files := make([]CookbookFile, 0)
	for _, segment := range cookbookLegacySegments {
		rawSegment, ok := payload[segment]
		if !ok {
			continue
		}
		segmentFiles, err := normalizeCookbookFileList(segment, rawSegment, checksumExists)
		if err != nil {
			return nil, err
		}
		files = append(files, segmentFiles...)
	}
	sortCookbookFiles(files)
	return files, nil
}

func normalizeCookbookFileList(segment string, value any, checksumExists func(string) (bool, error)) ([]CookbookFile, error) {
	rawList, ok := value.([]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", segment)}}
	}

	files := make([]CookbookFile, 0, len(rawList))
	for _, item := range rawList {
		rawFile, ok := item.(map[string]any)
		if !ok {
			return nil, &ValidationError{Messages: []string{fmt.Sprintf("Invalid element in array value of '%s'.", segment)}}
		}

		file, err := normalizeCookbookFile(segment, rawFile, checksumExists)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}

	return files, nil
}

func normalizeCookbookFile(segment string, raw map[string]any, checksumExists func(string) (bool, error)) (CookbookFile, error) {
	name, err := normalizeCookbookFileString(raw, "name", segment)
	if err != nil {
		return CookbookFile{}, err
	}
	path, err := normalizeCookbookFileString(raw, "path", segment)
	if err != nil {
		return CookbookFile{}, err
	}
	checksum, err := normalizeCookbookFileString(raw, "checksum", segment)
	if err != nil {
		return CookbookFile{}, err
	}
	checksum = strings.ToLower(checksum)
	if !ValidSandboxChecksum(checksum) {
		return CookbookFile{}, &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", segment)}}
	}
	specificity, err := normalizeCookbookFileString(raw, "specificity", segment)
	if err != nil {
		return CookbookFile{}, err
	}

	if !strings.Contains(path, "/") {
		if segment != "all_files" {
			path = segmentPath(segment, name)
		}
	}
	if segment == "all_files" {
		name = normalizeCookbookAllFilesStoredName(name)
	}

	if checksumExists != nil {
		exists, err := checksumExists(checksum)
		if err != nil {
			return CookbookFile{}, err
		}
		if !exists {
			return CookbookFile{}, &MissingChecksumError{Checksum: checksum}
		}
	}

	return CookbookFile{
		Name:        name,
		Path:        path,
		Checksum:    checksum,
		Specificity: specificity,
	}, nil
}

func normalizeCookbookFileString(raw map[string]any, field, segment string) (string, error) {
	value, ok := raw[field]
	if !ok {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", segment)}}
	}
	text, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", segment)}}
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", segment)}}
	}
	return text, nil
}

func cookbookVersionRefs(versions map[string]CookbookVersion) []CookbookVersionRef {
	out := make([]CookbookVersionRef, 0, len(versions))
	for _, version := range versions {
		out = append(out, CookbookVersionRef{
			Name:    version.CookbookName,
			Version: version.Version,
		})
	}

	sort.Slice(out, func(i, j int) bool {
		return compareCookbookVersions(out[i].Version, out[j].Version) > 0
	})
	return out
}

func sortedCookbookArtifacts(in map[string]CookbookArtifact) []CookbookArtifact {
	if len(in) == 0 {
		return nil
	}

	out := make([]CookbookArtifact, 0, len(in))
	for _, artifact := range in {
		out = append(out, copyCookbookArtifact(artifact))
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		if cmp := compareCookbookVersions(out[i].Version, out[j].Version); cmp != 0 {
			return cmp > 0
		}
		return out[i].Identifier < out[j].Identifier
	})
	return out
}

func copyCookbookArtifact(artifact CookbookArtifact) CookbookArtifact {
	out := CookbookArtifact{
		Name:       artifact.Name,
		Identifier: artifact.Identifier,
		Version:    artifact.Version,
		ChefType:   artifact.ChefType,
		Frozen:     artifact.Frozen,
		Metadata:   cloneMap(artifact.Metadata),
	}
	if len(artifact.AllFiles) > 0 {
		out.AllFiles = append([]CookbookFile(nil), artifact.AllFiles...)
	}
	return out
}

func copyCookbookVersion(version CookbookVersion) CookbookVersion {
	out := CookbookVersion{
		Name:         version.Name,
		CookbookName: version.CookbookName,
		Version:      version.Version,
		JSONClass:    version.JSONClass,
		ChefType:     version.ChefType,
		Frozen:       version.Frozen,
		Metadata:     cloneMap(version.Metadata),
	}
	if len(version.AllFiles) > 0 {
		out.AllFiles = append([]CookbookFile(nil), version.AllFiles...)
	}
	return out
}

func cookbookMetadataDependencies(metadata map[string]any) map[string]string {
	raw, ok := metadata["dependencies"].(map[string]any)
	if !ok || len(raw) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(raw))
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value, _ := raw[key].(string)
		out[key] = strings.TrimSpace(value)
	}
	return out
}

func sortCookbookFiles(files []CookbookFile) {
	sort.Slice(files, func(i, j int) bool {
		if files[i].Path != files[j].Path {
			return files[i].Path < files[j].Path
		}
		if files[i].Name != files[j].Name {
			return files[i].Name < files[j].Name
		}
		return files[i].Checksum < files[j].Checksum
	})
}

func segmentPath(segment, name string) string {
	if segment == "root_files" {
		return name
	}
	return segment + "/" + name
}

func validCookbookVersion(value string) bool {
	return validCookbookVersionPattern.MatchString(strings.TrimSpace(value))
}

func validCookbookRouteVersion(value string) bool {
	value = strings.TrimSpace(value)
	if !validCookbookRouteVersionPattern.MatchString(value) {
		return false
	}

	for _, part := range strings.Split(value, ".") {
		if _, err := strconv.ParseInt(part, 10, 64); err != nil {
			return false
		}
	}
	return true
}

func invalidCookbookNameMessage(name string) string {
	return fmt.Sprintf("Invalid cookbook name '%s' using regex: 'Malformed cookbook name. Must only contain A-Z, a-z, 0-9, _, . or -'.", name)
}

func invalidCookbookVersionMessage(version string) string {
	return fmt.Sprintf("Invalid cookbook version '%s'.", version)
}

func cookbookMetadataValueString(value any) string {
	switch value.(type) {
	case map[string]any, map[string]string:
		return "{[]}"
	default:
		return fmt.Sprintf("%v", value)
	}
}

func normalizeCookbookAllFilesStoredName(name string) string {
	parts := strings.Split(strings.TrimSpace(name), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
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
