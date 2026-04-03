package bootstrap

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var allowedRoleKeys = map[string]struct{}{
	"name":                {},
	"description":         {},
	"json_class":          {},
	"chef_type":           {},
	"default_attributes":  {},
	"override_attributes": {},
	"run_list":            {},
	"env_run_lists":       {},
}

type Role struct {
	Name               string              `json:"name"`
	Description        string              `json:"description"`
	JSONClass          string              `json:"json_class"`
	ChefType           string              `json:"chef_type"`
	DefaultAttributes  map[string]any      `json:"default_attributes"`
	OverrideAttributes map[string]any      `json:"override_attributes"`
	RunList            []string            `json:"run_list"`
	EnvRunLists        map[string][]string `json:"env_run_lists"`
}

type CreateRoleInput struct {
	Payload map[string]any
	Creator authn.Principal
}

type UpdateRoleInput struct {
	Payload map[string]any
}

func (s *Service) ListRoles(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.roles))
	for name := range org.roles {
		out[name] = roleURI(orgName, name)
	}
	return out, true
}

func (s *Service) GetRole(orgName, roleName string) (Role, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Role{}, false, false
	}

	role, ok := org.roles[roleName]
	if !ok {
		return Role{}, true, false
	}

	return copyRole(role), true, true
}

func (s *Service) CreateRole(orgName string, input CreateRoleInput) (Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Role{}, ErrNotFound
	}

	role, err := normalizeRolePayload(input.Payload, "", Role{}, true)
	if err != nil {
		return Role{}, err
	}
	if _, exists := org.roles[role.Name]; exists {
		return Role{}, ErrConflict
	}

	org.roles[role.Name] = role
	org.acls[roleACLKey(role.Name)] = defaultRoleACL(s.superuserName, input.Creator)
	return copyRole(role), nil
}

func (s *Service) UpdateRole(orgName, roleName string, input UpdateRoleInput) (Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Role{}, ErrNotFound
	}

	current, ok := org.roles[roleName]
	if !ok {
		return Role{}, ErrNotFound
	}

	role, err := normalizeRolePayload(input.Payload, roleName, current, false)
	if err != nil {
		return Role{}, err
	}

	org.roles[roleName] = role
	return copyRole(role), nil
}

func (s *Service) DeleteRole(orgName, roleName string) (Role, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Role{}, ErrNotFound
	}

	role, ok := org.roles[roleName]
	if !ok {
		return Role{}, ErrNotFound
	}

	delete(org.roles, roleName)
	delete(org.acls, roleACLKey(roleName))
	return copyRole(role), nil
}

func normalizeRolePayload(payload map[string]any, targetName string, current Role, create bool) (Role, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	for key := range payload {
		if _, ok := allowedRoleKeys[key]; ok {
			continue
		}
		return Role{}, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key %s in request body", key)}}
	}

	role := Role{
		Name:               current.Name,
		Description:        current.Description,
		JSONClass:          fallback(current.JSONClass, "Chef::Role"),
		ChefType:           fallback(current.ChefType, "role"),
		DefaultAttributes:  cloneMap(current.DefaultAttributes),
		OverrideAttributes: cloneMap(current.OverrideAttributes),
		RunList:            append([]string(nil), current.RunList...),
		EnvRunLists:        cloneStringSliceMap(current.EnvRunLists),
	}

	if create {
		role.Description = ""
		role.JSONClass = "Chef::Role"
		role.ChefType = "role"
		role.DefaultAttributes = map[string]any{}
		role.OverrideAttributes = map[string]any{}
		role.RunList = []string{}
		role.EnvRunLists = map[string][]string{}
	}

	nameValue, ok := payload["name"]
	switch {
	case create && !ok:
		return Role{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	case ok:
		name, err := validateRoleName(nameValue)
		if err != nil {
			return Role{}, err
		}
		if !create && targetName != "" && name != targetName {
			return Role{}, &ValidationError{Messages: []string{"Role name mismatch."}}
		}
		role.Name = name
	case !create && role.Name == "" && targetName != "":
		role.Name = targetName
	}

	if value, ok := payload["description"]; ok {
		description, err := validateOptionalStringField("description", value)
		if err != nil {
			return Role{}, err
		}
		role.Description = description
	}

	if value, ok := payload["json_class"]; ok {
		jsonClass, err := validateExactStringField("json_class", value, "Chef::Role")
		if err != nil {
			return Role{}, err
		}
		role.JSONClass = jsonClass
	}

	if value, ok := payload["chef_type"]; ok {
		chefType, err := validateExactStringField("chef_type", value, "role")
		if err != nil {
			return Role{}, err
		}
		role.ChefType = chefType
	}

	if value, ok := payload["default_attributes"]; ok {
		defaults, err := validateNodeMapField("default_attributes", value)
		if err != nil {
			return Role{}, err
		}
		role.DefaultAttributes = defaults
	}

	if value, ok := payload["override_attributes"]; ok {
		overrides, err := validateNodeMapField("override_attributes", value)
		if err != nil {
			return Role{}, err
		}
		role.OverrideAttributes = overrides
	}

	if value, ok := payload["run_list"]; ok {
		runList, err := validateRunList(value)
		if err != nil {
			return Role{}, err
		}
		role.RunList = runList
	}

	if value, ok := payload["env_run_lists"]; ok {
		envRunLists, err := validateEnvironmentRunLists(value)
		if err != nil {
			return Role{}, err
		}
		role.EnvRunLists = envRunLists
	}

	return role, nil
}

func validateRoleName(value any) (string, error) {
	name, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	name = strings.TrimSpace(name)
	if name == "" || !validNodeNamePattern.MatchString(name) {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	return name, nil
}

func validateEnvironmentRunLists(value any) (map[string][]string, error) {
	switch raw := value.(type) {
	case map[string][]string:
		out := make(map[string][]string, len(raw))
		for envName, runList := range raw {
			if !validEnvironmentNamePattern.MatchString(envName) {
				return nil, &ValidationError{Messages: []string{"Field 'env_run_lists' contains invalid run lists"}}
			}
			normalized, err := validateRunList(runList)
			if err != nil {
				return nil, &ValidationError{Messages: []string{"Field 'env_run_lists' contains invalid run lists"}}
			}
			out[envName] = normalized
		}
		return out, nil
	case map[string]any:
		out := make(map[string][]string, len(raw))
		for envName, runList := range raw {
			if !validEnvironmentNamePattern.MatchString(envName) {
				return nil, &ValidationError{Messages: []string{"Field 'env_run_lists' contains invalid run lists"}}
			}
			normalized, err := validateRunList(runList)
			if err != nil {
				return nil, &ValidationError{Messages: []string{"Field 'env_run_lists' contains invalid run lists"}}
			}
			out[envName] = normalized
		}
		return out, nil
	default:
		return nil, &ValidationError{Messages: []string{"Field 'env_run_lists' contains invalid run lists"}}
	}
}

func copyRole(role Role) Role {
	return Role{
		Name:               role.Name,
		Description:        role.Description,
		JSONClass:          role.JSONClass,
		ChefType:           role.ChefType,
		DefaultAttributes:  cloneMap(role.DefaultAttributes),
		OverrideAttributes: cloneMap(role.OverrideAttributes),
		RunList:            append([]string(nil), role.RunList...),
		EnvRunLists:        cloneStringSliceMap(role.EnvRunLists),
	}
}

func cloneStringSliceMap(in map[string][]string) map[string][]string {
	if len(in) == 0 {
		return map[string][]string{}
	}

	out := make(map[string][]string, len(in))
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = append([]string(nil), in[key]...)
	}
	return out
}

func defaultRoleACL(superuserName string, creator authn.Principal) authz.ACL {
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

func roleACLKey(name string) string {
	return "role:" + name
}

func roleURI(orgName, name string) string {
	return "/organizations/" + orgName + "/roles/" + name
}
