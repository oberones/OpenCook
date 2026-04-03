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
	Name     string         `json:"name"`
	Version  string         `json:"version"`
	ChefType string         `json:"chef_type"`
	Frozen   bool           `json:"frozen"`
	Metadata map[string]any `json:"metadata"`
	AllFiles []CookbookFile `json:"all_files"`
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

	out := make(map[string][]CookbookVersionRef, len(org.cookbookArtifacts))
	for name, versions := range org.cookbookArtifacts {
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

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
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

	versions, ok := org.cookbookArtifacts[strings.TrimSpace(name)]
	if !ok {
		return CookbookVersion{}, true, false
	}

	version = strings.TrimSpace(version)
	if version == "_latest" {
		refs := cookbookVersionRefs(versions)
		if len(refs) == 0 {
			return CookbookVersion{}, true, false
		}
		version = refs[0].Version
	}

	for _, artifact := range versions {
		if artifact.Version == version {
			return cookbookVersionFromArtifact(artifact), true, true
		}
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

	out := make(map[string][]UniverseEntry, len(org.cookbookArtifacts))
	for name, versions := range org.cookbookArtifacts {
		refs := cookbookVersionRefs(versions)
		entries := make([]UniverseEntry, 0, len(refs))
		for _, ref := range refs {
			for _, artifact := range versions {
				if artifact.Version != ref.Version {
					continue
				}
				entries = append(entries, UniverseEntry{
					Version:      artifact.Version,
					Dependencies: cookbookMetadataDependencies(artifact.Metadata),
				})
				break
			}
		}
		out[name] = entries
	}
	return out, true
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
		return nil, &ValidationError{Messages: []string{"Field 'metadata.version' missing"}}
	} else {
		metadata["version"] = strings.TrimSpace(versionString)
	}

	for _, section := range []string{"dependencies", "attributes", "recipes", "providing", "platforms"} {
		if rawSection, ok := metadata[section]; ok {
			normalized, err := normalizeCookbookMetadataMap(section, rawSection)
			if err != nil {
				return nil, err
			}
			metadata[section] = normalized
		}
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
		if segment == "all_files" {
			return CookbookFile{}, &ValidationError{Messages: []string{"Field 'all_files' invalid"}}
		}
		path = segmentPath(segment, name)
	}
	if segment == "all_files" && name == path {
		name = path
	}

	if checksumExists != nil {
		exists, err := checksumExists(checksum)
		if err != nil {
			return CookbookFile{}, err
		}
		if !exists {
			return CookbookFile{}, &ValidationError{Messages: []string{"Manifest has a checksum that hasn't been uploaded."}}
		}
	}

	if segment == "all_files" && name == "" {
		name = path
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

func cookbookVersionRefs(versions map[string]CookbookArtifact) []CookbookVersionRef {
	seen := make(map[string]struct{}, len(versions))
	out := make([]CookbookVersionRef, 0, len(versions))
	for _, artifact := range versions {
		if _, ok := seen[artifact.Version]; ok {
			continue
		}
		seen[artifact.Version] = struct{}{}
		out = append(out, CookbookVersionRef{
			Name:    artifact.Name,
			Version: artifact.Version,
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

func cookbookVersionFromArtifact(artifact CookbookArtifact) CookbookVersion {
	out := CookbookVersion{
		Name:     artifact.Name,
		Version:  artifact.Version,
		ChefType: artifact.ChefType,
		Frozen:   artifact.Frozen,
		Metadata: cloneMap(artifact.Metadata),
	}
	if len(artifact.AllFiles) > 0 {
		out.AllFiles = append([]CookbookFile(nil), artifact.AllFiles...)
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
