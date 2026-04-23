package bootstrap

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var (
	validEnvironmentNamePattern    = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	validCookbookNamePattern       = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
	validCookbookConstraintPattern = regexp.MustCompile(`^(?:(?:>=|<=|>|<|=|~>) )?\d+(?:\.\d+){0,2}$`)
	allowedEnvironmentTopLevelKeys = map[string]struct{}{"name": {}, "json_class": {}, "chef_type": {}, "description": {}, "cookbook_versions": {}, "default_attributes": {}, "override_attributes": {}}
	defaultEnvironmentName         = "_default"
	defaultEnvironmentDescription  = "The default Chef environment"
	defaultEnvironmentChefType     = "environment"
	defaultEnvironmentJSONClass    = "Chef::Environment"
)

type Environment struct {
	Name               string            `json:"name"`
	Description        string            `json:"description"`
	CookbookVersions   map[string]string `json:"cookbook_versions"`
	JSONClass          string            `json:"json_class"`
	ChefType           string            `json:"chef_type"`
	DefaultAttributes  map[string]any    `json:"default_attributes"`
	OverrideAttributes map[string]any    `json:"override_attributes"`
}

type CreateEnvironmentInput struct {
	Payload map[string]any
	Creator authn.Principal
}

type UpdateEnvironmentInput struct {
	Payload map[string]any
}

type UpdateEnvironmentResult struct {
	Environment Environment
	Renamed     bool
}

func (s *Service) ListEnvironments(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.envs))
	for name := range org.envs {
		out[name] = environmentURI(orgName, name)
	}
	return out, true
}

func (s *Service) GetEnvironment(orgName, name string) (Environment, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Environment{}, false, false
	}

	env, ok := org.envs[name]
	if !ok {
		return Environment{}, true, false
	}

	return copyEnvironment(env), true, true
}

func (s *Service) CreateEnvironment(orgName string, input CreateEnvironmentInput) (Environment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Environment{}, ErrNotFound
	}

	env, err := normalizeEnvironmentPayload(input.Payload)
	if err != nil {
		return Environment{}, err
	}
	if _, exists := org.envs[env.Name]; exists {
		return Environment{}, ErrConflict
	}

	org.envs[env.Name] = env
	org.acls[environmentACLKey(env.Name)] = defaultEnvironmentACL(s.superuserName, input.Creator)
	return copyEnvironment(env), nil
}

func (s *Service) UpdateEnvironment(orgName, currentName string, input UpdateEnvironmentInput) (UpdateEnvironmentResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return UpdateEnvironmentResult{}, ErrNotFound
	}
	if currentName == defaultEnvironmentName {
		return UpdateEnvironmentResult{}, ErrImmutable
	}

	if _, ok := org.envs[currentName]; !ok {
		return UpdateEnvironmentResult{}, ErrNotFound
	}

	env, err := normalizeEnvironmentPayload(input.Payload)
	if err != nil {
		return UpdateEnvironmentResult{}, err
	}
	if env.Name != currentName {
		if _, exists := org.envs[env.Name]; exists {
			return UpdateEnvironmentResult{}, ErrConflict
		}
	}

	delete(org.envs, currentName)
	org.envs[env.Name] = env
	if env.Name != currentName {
		if acl, ok := org.acls[environmentACLKey(currentName)]; ok {
			delete(org.acls, environmentACLKey(currentName))
			org.acls[environmentACLKey(env.Name)] = acl
		}
	}
	if _, ok := org.acls[environmentACLKey(env.Name)]; !ok {
		org.acls[environmentACLKey(env.Name)] = defaultEnvironmentACL(s.superuserName, authn.Principal{
			Type: "user",
			Name: s.superuserName,
		})
	}

	return UpdateEnvironmentResult{
		Environment: copyEnvironment(env),
		Renamed:     env.Name != currentName,
	}, nil
}

func (s *Service) DeleteEnvironment(orgName, name string) (Environment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Environment{}, ErrNotFound
	}
	if name == defaultEnvironmentName {
		return Environment{}, ErrImmutable
	}

	env, ok := org.envs[name]
	if !ok {
		return Environment{}, ErrNotFound
	}

	delete(org.envs, name)
	delete(org.acls, environmentACLKey(name))
	return copyEnvironment(env), nil
}

func (s *Service) ListEnvironmentNodes(orgName, environmentName string) (map[string]string, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}
	if _, ok := org.envs[environmentName]; !ok {
		return nil, true, false
	}

	out := make(map[string]string)
	for name, node := range org.nodes {
		if node.ChefEnvironment != environmentName {
			continue
		}
		out[name] = nodeURI(orgName, name)
	}
	return out, true, true
}

func (s *Service) ListEnvironmentCookbookVersions(orgName, environmentName string, numVersions int, allVersions bool) (map[string][]CookbookVersionRef, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	env, ok := org.envs[environmentName]
	if !ok {
		return nil, true, false
	}

	versions, orgExists := s.cookbookStore.ListCookbookVersions(orgName)
	if !orgExists {
		return nil, false, false
	}

	return environmentCookbookVersionsFromRefs(versions, env, numVersions, allVersions), true, true
}

func (s *Service) GetEnvironmentCookbookVersions(orgName, environmentName, cookbookName string, numVersions int, allVersions bool) ([]CookbookVersionRef, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false, false
	}

	env, ok := org.envs[environmentName]
	if !ok {
		return nil, true, false, false
	}

	cookbookName = strings.TrimSpace(cookbookName)
	refs, _, cookbookExists := s.cookbookStore.ListCookbookVersionsByName(orgName, cookbookName)
	if !cookbookExists {
		return nil, true, true, false
	}

	refs = filterEnvironmentCookbookRefs(refs, env.CookbookVersions[cookbookName])
	return limitEnvironmentCookbookRefs(refs, numVersions, allVersions), true, true, true
}

func environmentCookbookVersionsFromRefs(cookbooks map[string][]CookbookVersionRef, env Environment, numVersions int, allVersions bool) map[string][]CookbookVersionRef {
	if len(cookbooks) == 0 {
		return map[string][]CookbookVersionRef{}
	}

	if !allVersions && numVersions == 0 {
		out := make(map[string][]CookbookVersionRef, len(cookbooks))
		for name := range cookbooks {
			out[name] = []CookbookVersionRef{}
		}
		return out
	}

	out := make(map[string][]CookbookVersionRef, len(cookbooks))
	anyAllowed := false
	for name, refs := range cookbooks {
		refs = filterEnvironmentCookbookRefs(refs, env.CookbookVersions[name])
		refs = limitEnvironmentCookbookRefs(refs, numVersions, allVersions)
		if len(refs) == 0 {
			continue
		}
		out[name] = refs
		anyAllowed = true
	}

	if anyAllowed {
		return out
	}

	out = make(map[string][]CookbookVersionRef, len(cookbooks))
	for name := range cookbooks {
		out[name] = []CookbookVersionRef{}
	}
	return out
}

func environmentCookbookVersions(cookbooks map[string]map[string]CookbookVersion, env Environment, numVersions int, allVersions bool) map[string][]CookbookVersionRef {
	if len(cookbooks) == 0 {
		return map[string][]CookbookVersionRef{}
	}

	refs := make(map[string][]CookbookVersionRef, len(cookbooks))
	for name, versions := range cookbooks {
		refs[name] = cookbookVersionRefs(versions)
	}
	return environmentCookbookVersionsFromRefs(refs, env, numVersions, allVersions)
}

func filterEnvironmentCookbookRefs(refs []CookbookVersionRef, constraint string) []CookbookVersionRef {
	constraint = strings.TrimSpace(constraint)
	if constraint == "" || len(refs) == 0 {
		return refs
	}

	out := make([]CookbookVersionRef, 0, len(refs))
	for _, ref := range refs {
		if cookbookConstraintMatches(ref.Version, constraint) {
			out = append(out, ref)
		}
	}
	return out
}

func limitEnvironmentCookbookRefs(refs []CookbookVersionRef, numVersions int, allVersions bool) []CookbookVersionRef {
	if len(refs) == 0 {
		return []CookbookVersionRef{}
	}
	if allVersions {
		return append([]CookbookVersionRef(nil), refs...)
	}
	if numVersions <= 0 {
		return []CookbookVersionRef{}
	}
	if len(refs) <= numVersions {
		return append([]CookbookVersionRef(nil), refs...)
	}
	return append([]CookbookVersionRef(nil), refs[:numVersions]...)
}

func cookbookConstraintMatches(version, constraint string) bool {
	op, target := parseCookbookConstraint(constraint)
	switch op {
	case ">":
		return compareCookbookVersions(version, target) > 0
	case ">=":
		return compareCookbookVersions(version, target) >= 0
	case "<":
		return compareCookbookVersions(version, target) < 0
	case "<=":
		return compareCookbookVersions(version, target) <= 0
	case "~>":
		lowerOK := compareCookbookVersions(version, target) >= 0
		if !lowerOK {
			return false
		}
		return compareCookbookVersions(version, cookbookConstraintUpperBound(target)) < 0
	default:
		return compareCookbookVersions(version, target) == 0
	}
}

func parseCookbookConstraint(constraint string) (string, string) {
	parts := strings.Fields(strings.TrimSpace(constraint))
	switch len(parts) {
	case 0:
		return "=", ""
	case 1:
		return "=", parts[0]
	default:
		return parts[0], parts[1]
	}
}

func cookbookConstraintUpperBound(version string) string {
	parts := strings.Split(strings.TrimSpace(version), ".")
	ints := make([]int, 0, len(parts))
	for _, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil {
			return version
		}
		ints = append(ints, value)
	}
	for len(ints) < 3 {
		ints = append(ints, 0)
	}

	switch len(parts) {
	case 0:
		return version
	case 1, 2:
		ints[0]++
		ints[1] = 0
		ints[2] = 0
	default:
		ints[1]++
		ints[2] = 0
	}

	return fmt.Sprintf("%d.%d.%d", ints[0], ints[1], ints[2])
}

func normalizeEnvironmentPayload(payload map[string]any) (Environment, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	for key := range payload {
		if _, ok := allowedEnvironmentTopLevelKeys[key]; ok {
			continue
		}
		return Environment{}, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key %s in request body", key)}}
	}

	nameValue, ok := payload["name"]
	if !ok {
		return Environment{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	}

	name, err := validateEnvironmentName(nameValue)
	if err != nil {
		return Environment{}, err
	}

	env := Environment{
		Name:               name,
		Description:        "",
		CookbookVersions:   map[string]string{},
		JSONClass:          defaultEnvironmentJSONClass,
		ChefType:           defaultEnvironmentChefType,
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	}

	if value, ok := payload["description"]; ok {
		description, err := validateOptionalStringField("description", value)
		if err != nil {
			return Environment{}, err
		}
		env.Description = description
	} else if name == defaultEnvironmentName {
		env.Description = defaultEnvironmentDescription
	}

	if value, ok := payload["json_class"]; ok {
		jsonClass, err := validateExactStringField("json_class", value, defaultEnvironmentJSONClass)
		if err != nil {
			return Environment{}, err
		}
		env.JSONClass = jsonClass
	}

	if value, ok := payload["chef_type"]; ok {
		chefType, err := validateExactStringField("chef_type", value, defaultEnvironmentChefType)
		if err != nil {
			return Environment{}, err
		}
		env.ChefType = chefType
	}

	if value, ok := payload["cookbook_versions"]; ok {
		cookbookVersions, err := validateCookbookVersions(value)
		if err != nil {
			return Environment{}, err
		}
		env.CookbookVersions = cookbookVersions
	}

	if value, ok := payload["default_attributes"]; ok {
		defaultAttrs, err := validateNodeMapField("default_attributes", value)
		if err != nil {
			return Environment{}, err
		}
		env.DefaultAttributes = defaultAttrs
	}

	if value, ok := payload["override_attributes"]; ok {
		overrideAttrs, err := validateNodeMapField("override_attributes", value)
		if err != nil {
			return Environment{}, err
		}
		env.OverrideAttributes = overrideAttrs
	}

	return env, nil
}

func validateEnvironmentName(value any) (string, error) {
	name, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	name = strings.TrimSpace(name)
	if name == "" || !validEnvironmentNamePattern.MatchString(name) {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	return name, nil
}

func validateOptionalStringField(field string, value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	return text, nil
}

func validateCookbookVersions(value any) (map[string]string, error) {
	switch raw := value.(type) {
	case map[string]string:
		out := make(map[string]string, len(raw))
		for key, val := range raw {
			if err := validateCookbookVersionPair(key, val); err != nil {
				return nil, err
			}
			out[key] = val
		}
		return out, nil
	case map[string]any:
		out := make(map[string]string, len(raw))
		for key, val := range raw {
			text, ok := val.(string)
			if !ok {
				return nil, &ValidationError{Messages: []string{fmt.Sprintf("Invalid value '%v' for cookbook_versions", val)}}
			}
			if err := validateCookbookVersionPair(key, text); err != nil {
				return nil, err
			}
			out[key] = text
		}
		return out, nil
	default:
		return nil, &ValidationError{Messages: []string{"Field 'cookbook_versions' is not a hash"}}
	}
}

func validateCookbookVersionPair(key, value string) error {
	if !validCookbookNamePattern.MatchString(key) {
		return &ValidationError{Messages: []string{fmt.Sprintf("Invalid key '%s' for cookbook_versions", key)}}
	}
	if !validCookbookConstraintPattern.MatchString(value) {
		return &ValidationError{Messages: []string{fmt.Sprintf("Invalid value '%s' for cookbook_versions", value)}}
	}
	return nil
}

func copyEnvironment(env Environment) Environment {
	return Environment{
		Name:               env.Name,
		Description:        env.Description,
		CookbookVersions:   cloneStringMap(env.CookbookVersions),
		JSONClass:          env.JSONClass,
		ChefType:           env.ChefType,
		DefaultAttributes:  cloneMap(env.DefaultAttributes),
		OverrideAttributes: cloneMap(env.OverrideAttributes),
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func defaultEnvironment() Environment {
	return Environment{
		Name:               defaultEnvironmentName,
		Description:        defaultEnvironmentDescription,
		CookbookVersions:   map[string]string{},
		JSONClass:          defaultEnvironmentJSONClass,
		ChefType:           defaultEnvironmentChefType,
		DefaultAttributes:  map[string]any{},
		OverrideAttributes: map[string]any{},
	}
}

func defaultEnvironmentACL(superuserName string, creator authn.Principal) authz.ACL {
	creatorActors := []string{superuserName}
	if creator.Name != "" {
		creatorActors = uniqueSorted(append(creatorActors, creator.Name))
	}

	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Read:   authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users", "clients"}},
		Update: authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users"}},
		Delete: authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users"}},
		Grant:  authz.Permission{Actors: creatorActors, Groups: []string{"admins"}},
	}
}

func environmentACLKey(name string) string {
	return "environment:" + name
}

func environmentURI(orgName, name string) string {
	return "/organizations/" + orgName + "/environments/" + name
}
