package bootstrap

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

const (
	runListNameRegex    = `[A-Za-z0-9_.-]+`
	runListVersionRegex = `\d+(?:\.\d+){1,2}`
	runListRecipeRegex  = `(?:` + runListNameRegex + `::)?` + runListNameRegex
)

var (
	validNodeNamePattern        = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	validNodeEnvironmentPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	validPolicyNamePattern      = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	validRunListItemPattern     = regexp.MustCompile(`^` + runListRecipeRegex + `(?:@` + runListVersionRegex + `)?$`)
	validRecipeRunListPattern   = regexp.MustCompile(`^recipe\[` + runListRecipeRegex + `(?:@` + runListVersionRegex + `)?\]$`)
	validRoleRunListPattern     = regexp.MustCompile(`^role\[` + runListNameRegex + `\]$`)
)

var allowedNodeKeys = map[string]struct{}{
	"name":             {},
	"json_class":       {},
	"chef_type":        {},
	"chef_environment": {},
	"override":         {},
	"normal":           {},
	"default":          {},
	"automatic":        {},
	"run_list":         {},
	"policy_name":      {},
	"policy_group":     {},
}

type ValidationError struct {
	Messages []string
}

func (e *ValidationError) Error() string {
	return strings.Join(e.Messages, "; ")
}

type Node struct {
	Name            string         `json:"name"`
	JSONClass       string         `json:"json_class"`
	ChefType        string         `json:"chef_type"`
	ChefEnvironment string         `json:"chef_environment"`
	Override        map[string]any `json:"override"`
	Normal          map[string]any `json:"normal"`
	Default         map[string]any `json:"default"`
	Automatic       map[string]any `json:"automatic"`
	RunList         []string       `json:"run_list"`
	PolicyName      string         `json:"policy_name,omitempty"`
	PolicyGroup     string         `json:"policy_group,omitempty"`
}

type CreateNodeInput struct {
	Payload map[string]any
	Creator authn.Principal
}

type UpdateNodeInput struct {
	Payload map[string]any
}

func (s *Service) ListNodes(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.nodes))
	for name := range org.nodes {
		out[name] = nodeURI(orgName, name)
	}
	return out, true
}

func (s *Service) GetNode(orgName, nodeName string) (Node, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Node{}, false, false
	}

	node, ok := org.nodes[nodeName]
	if !ok {
		return Node{}, true, false
	}

	return copyNode(node), true, true
}

func (s *Service) CreateNode(orgName string, input CreateNodeInput) (Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Node{}, ErrNotFound
	}

	node, err := normalizeNodePayload(input.Payload, "", Node{}, true)
	if err != nil {
		return Node{}, err
	}
	if _, exists := org.nodes[node.Name]; exists {
		return Node{}, ErrConflict
	}

	previous := s.snapshotCoreObjectsLocked()
	org.nodes[node.Name] = node
	org.acls[nodeACLKey(node.Name)] = defaultNodeACL(s.superuserName, input.Creator)
	if err := s.finishCoreObjectMutationLocked(previous); err != nil {
		return Node{}, err
	}
	return copyNode(node), nil
}

func (s *Service) UpdateNode(orgName, nodeName string, input UpdateNodeInput) (Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Node{}, ErrNotFound
	}

	current, ok := org.nodes[nodeName]
	if !ok {
		return Node{}, ErrNotFound
	}

	node, err := normalizeNodePayload(input.Payload, nodeName, current, false)
	if err != nil {
		return Node{}, err
	}

	previous := s.snapshotCoreObjectsLocked()
	org.nodes[nodeName] = node
	if err := s.finishCoreObjectMutationLocked(previous); err != nil {
		return Node{}, err
	}
	return copyNode(node), nil
}

func (s *Service) DeleteNode(orgName, nodeName string) (Node, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Node{}, ErrNotFound
	}

	node, ok := org.nodes[nodeName]
	if !ok {
		return Node{}, ErrNotFound
	}

	previous := s.snapshotCoreObjectsLocked()
	delete(org.nodes, nodeName)
	delete(org.acls, nodeACLKey(nodeName))
	if err := s.finishCoreObjectMutationLocked(previous); err != nil {
		return Node{}, err
	}
	return copyNode(node), nil
}

func normalizeNodePayload(payload map[string]any, targetName string, current Node, create bool) (Node, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	for key := range payload {
		if _, ok := allowedNodeKeys[key]; ok {
			continue
		}
		return Node{}, &ValidationError{Messages: []string{fmt.Sprintf("Invalid key %s in request body", key)}}
	}

	node := Node{
		Name:            current.Name,
		JSONClass:       fallback(current.JSONClass, "Chef::Node"),
		ChefType:        fallback(current.ChefType, "node"),
		ChefEnvironment: fallback(current.ChefEnvironment, "_default"),
		Override:        cloneMap(current.Override),
		Normal:          cloneMap(current.Normal),
		Default:         cloneMap(current.Default),
		Automatic:       cloneMap(current.Automatic),
		RunList:         append([]string(nil), current.RunList...),
		PolicyName:      current.PolicyName,
		PolicyGroup:     current.PolicyGroup,
	}

	if create {
		node.JSONClass = "Chef::Node"
		node.ChefType = "node"
		node.ChefEnvironment = "_default"
		node.Override = map[string]any{}
		node.Normal = map[string]any{}
		node.Default = map[string]any{}
		node.Automatic = map[string]any{}
		node.RunList = []string{}
	}

	nameValue, ok := payload["name"]
	switch {
	case create && !ok:
		return Node{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	case ok:
		name, err := validateNodeName(nameValue)
		if err != nil {
			return Node{}, err
		}
		if !create && targetName != "" && name != targetName {
			return Node{}, &ValidationError{Messages: []string{"Node name mismatch."}}
		}
		node.Name = name
	}

	if value, ok := payload["json_class"]; ok {
		jsonClass, err := validateExactStringField("json_class", value, "Chef::Node")
		if err != nil {
			return Node{}, err
		}
		node.JSONClass = jsonClass
	}

	if value, ok := payload["chef_type"]; ok {
		chefType, err := validateExactStringField("chef_type", value, "node")
		if err != nil {
			return Node{}, err
		}
		node.ChefType = chefType
	}

	if value, ok := payload["chef_environment"]; ok {
		chefEnvironment, err := validateNodeEnvironment(value)
		if err != nil {
			return Node{}, err
		}
		node.ChefEnvironment = chefEnvironment
	}

	if value, ok := payload["override"]; ok {
		override, err := validateNodeMapField("override", value)
		if err != nil {
			return Node{}, err
		}
		node.Override = override
	}

	if value, ok := payload["normal"]; ok {
		normal, err := validateNodeMapField("normal", value)
		if err != nil {
			return Node{}, err
		}
		node.Normal = normal
	}

	if value, ok := payload["default"]; ok {
		defaults, err := validateNodeMapField("default", value)
		if err != nil {
			return Node{}, err
		}
		node.Default = defaults
	}

	if value, ok := payload["automatic"]; ok {
		automatic, err := validateNodeMapField("automatic", value)
		if err != nil {
			return Node{}, err
		}
		node.Automatic = automatic
	}

	if value, ok := payload["run_list"]; ok {
		runList, err := validateRunList(value)
		if err != nil {
			return Node{}, err
		}
		node.RunList = runList
	}

	if value, ok := payload["policy_name"]; ok {
		policyName, err := validatePolicyField("policy_name", value)
		if err != nil {
			return Node{}, err
		}
		node.PolicyName = policyName
	}

	if value, ok := payload["policy_group"]; ok {
		policyGroup, err := validatePolicyField("policy_group", value)
		if err != nil {
			return Node{}, err
		}
		node.PolicyGroup = policyGroup
	}

	return node, nil
}

func validateNodeName(value any) (string, error) {
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

func validateExactStringField(field string, value any, expected string) (string, error) {
	text, ok := value.(string)
	if !ok || strings.TrimSpace(text) != expected {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	return expected, nil
}

func validateNodeEnvironment(value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{"Field 'chef_environment' invalid"}}
	}
	text = strings.TrimSpace(text)
	if text == "" || !validNodeEnvironmentPattern.MatchString(text) {
		return "", &ValidationError{Messages: []string{"Field 'chef_environment' invalid"}}
	}
	return text, nil
}

func validateNodeMapField(field string, value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' is not a hash", field)}}
	}
	return cloneMap(raw), nil
}

func validateRunList(value any) ([]string, error) {
	switch raw := value.(type) {
	case []string:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			if !isValidRunListItem(item) {
				return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
			}
			out = append(out, item)
		}
		return out, nil
	case []any:
		out := make([]string, 0, len(raw))
		for _, item := range raw {
			text, ok := item.(string)
			if !ok || !isValidRunListItem(text) {
				return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
			}
			out = append(out, text)
		}
		return out, nil
	default:
		return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
	}
}

func isValidRunListItem(value string) bool {
	if value == "" {
		return false
	}
	return validRunListItemPattern.MatchString(value) ||
		validRecipeRunListPattern.MatchString(value) ||
		validRoleRunListPattern.MatchString(value)
}

func validatePolicyField(field string, value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	text = strings.TrimSpace(text)
	if text == "" || len(text) > 255 || !validPolicyNamePattern.MatchString(text) {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	return text, nil
}

func cloneMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(in))
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		out[key] = cloneValue(in[key])
	}
	return out
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneValue(typed[idx])
		}
		return out
	case []string:
		out := make([]string, len(typed))
		copy(out, typed)
		return out
	default:
		return typed
	}
}

func copyNode(node Node) Node {
	return Node{
		Name:            node.Name,
		JSONClass:       node.JSONClass,
		ChefType:        node.ChefType,
		ChefEnvironment: node.ChefEnvironment,
		Override:        cloneMap(node.Override),
		Normal:          cloneMap(node.Normal),
		Default:         cloneMap(node.Default),
		Automatic:       cloneMap(node.Automatic),
		RunList:         append([]string(nil), node.RunList...),
		PolicyName:      node.PolicyName,
		PolicyGroup:     node.PolicyGroup,
	}
}

func defaultNodeACL(superuserName string, creator authn.Principal) authz.ACL {
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

func nodeACLKey(name string) string {
	return "node:" + name
}

func nodeURI(orgName, nodeName string) string {
	return "/organizations/" + orgName + "/nodes/" + nodeName
}
