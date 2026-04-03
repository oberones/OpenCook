package bootstrap

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var validPolicyTokenPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)

type PolicyRevision struct {
	Name       string         `json:"name"`
	RevisionID string         `json:"revision_id"`
	Payload    map[string]any `json:"-"`
}

type PolicyGroup struct {
	Name     string            `json:"name"`
	Policies map[string]string `json:"policies"`
}

type PolicyAssignmentPlan struct {
	Revision        PolicyRevision
	CreatesPolicy   bool
	CreatesRevision bool
}

type CreatePolicyRevisionInput struct {
	Payload map[string]any
	Creator authn.Principal
}

type UpdatePolicyGroupAssignmentInput struct {
	Payload map[string]any
	Creator authn.Principal
}

func (s *Service) ListPolicies(orgName string) (map[string][]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string][]string, len(org.policies))
	for name, revisions := range org.policies {
		keys := make([]string, 0, len(revisions))
		for revisionID := range revisions {
			keys = append(keys, revisionID)
		}
		sort.Strings(keys)
		out[name] = keys
	}
	return out, true
}

func (s *Service) GetPolicy(orgName, policyName string) (map[string]PolicyRevision, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}

	revisions, ok := org.policies[policyName]
	if !ok {
		return nil, true, false
	}

	return copyPolicyRevisions(revisions), true, true
}

func (s *Service) GetPolicyRevision(orgName, policyName, revisionID string) (PolicyRevision, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, false, false, false
	}

	revisions, ok := org.policies[policyName]
	if !ok {
		return PolicyRevision{}, true, false, false
	}

	revision, ok := revisions[revisionID]
	if !ok {
		return PolicyRevision{}, true, true, false
	}

	return copyPolicyRevision(revision), true, true, true
}

func (s *Service) PolicyGroupsForRevision(orgName, policyName, revisionID string) ([]string, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false, false
	}

	revisions, ok := org.policies[policyName]
	if !ok {
		return nil, true, false, false
	}
	if _, ok := revisions[revisionID]; !ok {
		return nil, true, true, false
	}

	groups := make([]string, 0, len(org.policyGroups))
	for name, group := range org.policyGroups {
		if group.Policies[policyName] == revisionID {
			groups = append(groups, name)
		}
	}
	sort.Strings(groups)
	return groups, true, true, true
}

func (s *Service) CreatePolicyRevision(orgName, targetName string, input CreatePolicyRevisionInput) (PolicyRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	revision, err := normalizePolicyPayload(input.Payload, targetName)
	if err != nil {
		return PolicyRevision{}, err
	}

	revisions := ensurePolicyRevisions(org.policies, revision.Name)
	if _, exists := revisions[revision.RevisionID]; exists {
		return PolicyRevision{}, ErrConflict
	}

	revisions[revision.RevisionID] = revision
	if _, ok := org.acls[policyACLKey(revision.Name)]; !ok {
		org.acls[policyACLKey(revision.Name)] = defaultPolicyACL(s.superuserName, input.Creator)
	}
	return copyPolicyRevision(revision), nil
}

func (s *Service) DeletePolicy(orgName, policyName string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, ErrNotFound
	}

	revisions, ok := org.policies[policyName]
	if !ok {
		return nil, ErrNotFound
	}

	revisionIDs := make([]string, 0, len(revisions))
	for revisionID := range revisions {
		revisionIDs = append(revisionIDs, revisionID)
	}
	sort.Strings(revisionIDs)

	delete(org.policies, policyName)
	delete(org.acls, policyACLKey(policyName))
	for groupName, group := range org.policyGroups {
		if _, ok := group.Policies[policyName]; !ok {
			continue
		}
		delete(group.Policies, policyName)
		org.policyGroups[groupName] = group
	}

	return revisionIDs, nil
}

func (s *Service) DeletePolicyRevision(orgName, policyName, revisionID string) (PolicyRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	revisions, ok := org.policies[policyName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	revision, ok := revisions[revisionID]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	delete(revisions, revisionID)
	for groupName, group := range org.policyGroups {
		if group.Policies[policyName] != revisionID {
			continue
		}
		delete(group.Policies, policyName)
		org.policyGroups[groupName] = group
	}

	return copyPolicyRevision(revision), nil
}

func (s *Service) ListPolicyGroups(orgName string) (map[string]PolicyGroup, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]PolicyGroup, len(org.policyGroups))
	for name, group := range org.policyGroups {
		out[name] = copyPolicyGroup(group)
	}
	return out, true
}

func (s *Service) GetPolicyGroup(orgName, groupName string) (PolicyGroup, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyGroup{}, false, false
	}

	group, ok := org.policyGroups[groupName]
	if !ok {
		return PolicyGroup{}, true, false
	}

	return copyPolicyGroup(group), true, true
}

func (s *Service) DeletePolicyGroup(orgName, groupName string) (PolicyGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyGroup{}, ErrNotFound
	}

	group, ok := org.policyGroups[groupName]
	if !ok {
		return PolicyGroup{}, ErrNotFound
	}

	delete(org.policyGroups, groupName)
	delete(org.acls, policyGroupACLKey(groupName))
	return copyPolicyGroup(group), nil
}

func (s *Service) GetPolicyGroupAssignment(orgName, groupName, policyName string) (PolicyRevision, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, false, false, false
	}

	group, ok := org.policyGroups[groupName]
	if !ok {
		return PolicyRevision{}, true, false, false
	}

	revisionID, ok := group.Policies[policyName]
	if !ok {
		return PolicyRevision{}, true, true, false
	}

	revisions := org.policies[policyName]
	revision, ok := revisions[revisionID]
	if !ok {
		return PolicyRevision{}, true, true, false
	}

	return copyPolicyRevision(revision), true, true, true
}

func (s *Service) PreviewPolicyGroupAssignment(orgName, targetPolicyName string, payload map[string]any) (PolicyAssignmentPlan, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyAssignmentPlan{}, ErrNotFound
	}

	return previewPolicyGroupAssignmentLocked(org, targetPolicyName, payload)
}

func (s *Service) UpsertPolicyGroupAssignment(orgName, groupName, targetPolicyName string, input UpdatePolicyGroupAssignmentInput) (PolicyRevision, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, false, ErrNotFound
	}

	plan, err := previewPolicyGroupAssignmentLocked(org, targetPolicyName, input.Payload)
	if err != nil {
		return PolicyRevision{}, false, err
	}
	revision := plan.Revision

	revisions := ensurePolicyRevisions(org.policies, revision.Name)
	if plan.CreatesRevision {
		revisions[revision.RevisionID] = revision
	}
	if _, ok := org.acls[policyACLKey(revision.Name)]; !ok {
		org.acls[policyACLKey(revision.Name)] = defaultPolicyACL(s.superuserName, input.Creator)
	}

	group := ensurePolicyGroup(org.policyGroups, groupName)
	if _, ok := org.acls[policyGroupACLKey(groupName)]; !ok {
		org.acls[policyGroupACLKey(groupName)] = defaultPolicyGroupACL(s.superuserName, input.Creator)
	}

	_, existed := group.Policies[revision.Name]
	group.Policies[revision.Name] = revision.RevisionID
	org.policyGroups[groupName] = group

	return copyPolicyRevision(revision), !existed, nil
}

func (s *Service) DeletePolicyGroupAssignment(orgName, groupName, policyName string) (PolicyRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	group, ok := org.policyGroups[groupName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	revisionID, ok := group.Policies[policyName]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	revisions := org.policies[policyName]
	revision, ok := revisions[revisionID]
	if !ok {
		return PolicyRevision{}, ErrNotFound
	}

	delete(group.Policies, policyName)
	org.policyGroups[groupName] = group
	return copyPolicyRevision(revision), nil
}

func normalizePolicyPayload(payload map[string]any, targetName string) (PolicyRevision, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	rawRevisionID, ok := payload["revision_id"]
	if !ok {
		return PolicyRevision{}, &ValidationError{Messages: []string{"Field 'revision_id' missing"}}
	}
	revisionID, err := validatePolicyToken("revision_id", rawRevisionID)
	if err != nil {
		return PolicyRevision{}, err
	}

	rawName, ok := payload["name"]
	if !ok {
		return PolicyRevision{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	}
	name, err := validatePolicyToken("name", rawName)
	if err != nil {
		return PolicyRevision{}, err
	}
	if targetName != "" && name != targetName {
		return PolicyRevision{}, &ValidationError{Messages: []string{fmt.Sprintf("Field 'name' invalid : %s does not match %s", targetName, name)}}
	}

	rawRunList, ok := payload["run_list"]
	if !ok {
		return PolicyRevision{}, &ValidationError{Messages: []string{"Field 'run_list' missing"}}
	}
	runList, err := validatePolicyRunList(rawRunList)
	if err != nil {
		return PolicyRevision{}, err
	}

	rawCookbookLocks, ok := payload["cookbook_locks"]
	if !ok {
		return PolicyRevision{}, &ValidationError{Messages: []string{"Field 'cookbook_locks' missing"}}
	}
	cookbookLocks, err := validatePolicyCookbookLocks(rawCookbookLocks)
	if err != nil {
		return PolicyRevision{}, err
	}

	normalized := cloneMap(payload)
	delete(normalized, "policy_group_list")
	delete(normalized, "policy_group")
	normalized["revision_id"] = revisionID
	normalized["name"] = name
	normalized["run_list"] = stringSliceToAny(runList)
	normalized["cookbook_locks"] = cookbookLocks

	if value, ok := payload["named_run_lists"]; ok {
		namedRunLists, err := validatePolicyNamedRunLists(value)
		if err != nil {
			return PolicyRevision{}, err
		}
		normalized["named_run_lists"] = stringSliceMapToAny(namedRunLists)
	}

	if value, ok := payload["solution_dependencies"]; ok {
		deps, err := validatePolicySolutionDependencies(value)
		if err != nil {
			return PolicyRevision{}, err
		}
		normalized["solution_dependencies"] = deps
	}

	return PolicyRevision{
		Name:       name,
		RevisionID: revisionID,
		Payload:    normalized,
	}, nil
}

func previewPolicyGroupAssignmentLocked(org *organizationState, targetPolicyName string, payload map[string]any) (PolicyAssignmentPlan, error) {
	revision, err := normalizePolicyPayload(payload, targetPolicyName)
	if err != nil {
		return PolicyAssignmentPlan{}, err
	}

	revisions, policyExists := org.policies[revision.Name]
	if !policyExists {
		return PolicyAssignmentPlan{
			Revision:        revision,
			CreatesPolicy:   true,
			CreatesRevision: true,
		}, nil
	}

	stored, revisionExists := revisions[revision.RevisionID]
	if !revisionExists {
		return PolicyAssignmentPlan{
			Revision:        revision,
			CreatesPolicy:   false,
			CreatesRevision: true,
		}, nil
	}

	if !reflect.DeepEqual(stored.Payload, revision.Payload) {
		return PolicyAssignmentPlan{}, fmt.Errorf("%w: policy revision payload differs from stored revision", ErrConflict)
	}

	return PolicyAssignmentPlan{
		Revision:        copyPolicyRevision(stored),
		CreatesPolicy:   false,
		CreatesRevision: false,
	}, nil
}

func validatePolicyToken(field string, value any) (string, error) {
	text, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	text = strings.TrimSpace(text)
	if text == "" || len(text) > 255 || !validPolicyTokenPattern.MatchString(text) {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' invalid", field)}}
	}
	return text, nil
}

func validatePolicyRunList(value any) ([]string, error) {
	runList, err := validateRunList(value)
	if err != nil {
		return nil, err
	}
	for _, item := range runList {
		if !isValidPolicyRunListItem(item) {
			return nil, &ValidationError{Messages: []string{"Field 'run_list' is not a valid run list"}}
		}
	}
	return runList, nil
}

func validatePolicyNamedRunLists(value any) (map[string][]string, error) {
	switch raw := value.(type) {
	case map[string][]string:
		out := make(map[string][]string, len(raw))
		for name, entry := range raw {
			validName, err := validatePolicyToken("named_run_lists", name)
			if err != nil {
				return nil, &ValidationError{Messages: []string{"Field 'named_run_lists' invalid"}}
			}
			runList, err := validatePolicyRunList(entry)
			if err != nil {
				return nil, err
			}
			out[validName] = runList
		}
		return out, nil
	case map[string]any:
		out := make(map[string][]string, len(raw))
		for name, entry := range raw {
			validName, err := validatePolicyToken("named_run_lists", name)
			if err != nil {
				return nil, &ValidationError{Messages: []string{"Field 'named_run_lists' invalid"}}
			}
			runList, err := validatePolicyRunList(entry)
			if err != nil {
				return nil, err
			}
			out[validName] = runList
		}
		return out, nil
	default:
		return nil, &ValidationError{Messages: []string{"Field 'named_run_lists' invalid"}}
	}
}

func validatePolicyCookbookLocks(value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{"Field 'cookbook_locks' invalid"}}
	}

	out := make(map[string]any, len(raw))
	keys := make([]string, 0, len(raw))
	for key := range raw {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, name := range keys {
		if !validCookbookNamePattern.MatchString(name) {
			return nil, &ValidationError{Messages: []string{"Field 'cookbook_locks' invalid"}}
		}

		lockRaw, ok := raw[name].(map[string]any)
		if !ok {
			return nil, &ValidationError{Messages: []string{"Field 'cookbook_locks' invalid"}}
		}

		identifierValue, ok := lockRaw["identifier"]
		if !ok {
			return nil, &ValidationError{Messages: []string{"Field 'identifier' missing"}}
		}
		if _, err := validatePolicyToken("identifier", identifierValue); err != nil {
			return nil, err
		}
		versionValue, ok := lockRaw["version"]
		if !ok {
			return nil, &ValidationError{Messages: []string{"Field 'version' missing"}}
		}
		if _, err := validatePolicyCookbookVersion("version", versionValue); err != nil {
			return nil, err
		}
		if dotted, ok := lockRaw["dotted_decimal_identifier"]; ok {
			if _, err := validatePolicyCookbookVersion("dotted_decimal_identifier", dotted); err != nil {
				return nil, err
			}
		}
		if scmInfo, ok := lockRaw["scm_info"]; ok {
			if _, ok := scmInfo.(map[string]any); !ok {
				return nil, &ValidationError{Messages: []string{"Field 'scm_info' invalid"}}
			}
		}
		if sourceOptions, ok := lockRaw["source_options"]; ok {
			if _, ok := sourceOptions.(map[string]any); !ok {
				return nil, &ValidationError{Messages: []string{"Field 'source_options' invalid"}}
			}
		}

		out[name] = cloneMap(lockRaw)
	}

	return out, nil
}

func validatePolicySolutionDependencies(value any) (map[string]any, error) {
	raw, ok := value.(map[string]any)
	if !ok {
		return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
	}

	if policyfileValue, ok := raw["Policyfile"]; ok {
		pairs, ok := policyfileValue.([]any)
		if !ok {
			return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
		}
		for _, pairValue := range pairs {
			pair, ok := pairValue.([]any)
			if !ok || len(pair) != 2 {
				return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
			}
			for _, item := range pair {
				text, ok := item.(string)
				if !ok || strings.TrimSpace(text) == "" {
					return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
				}
			}
		}
	}

	if dependenciesValue, ok := raw["dependencies"]; ok {
		dependencies, ok := dependenciesValue.(map[string]any)
		if !ok {
			return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
		}
		for _, entry := range dependencies {
			if _, ok := entry.([]any); !ok {
				return nil, &ValidationError{Messages: []string{"Field 'solution_dependencies' invalid"}}
			}
		}
	}

	return cloneMap(raw), nil
}

func validatePolicyCookbookVersion(field string, value any) (string, error) {
	text, ok := value.(string)
	if !ok || !validPolicyDottedDecimalIdentifier(text) {
		return "", &ValidationError{Messages: []string{fmt.Sprintf("Field '%s' is not a valid version", field)}}
	}
	return strings.TrimSpace(text), nil
}

func validPolicyDottedDecimalIdentifier(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func isValidPolicyRunListItem(value string) bool {
	if !validRecipeRunListPattern.MatchString(value) {
		return false
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(value, "recipe["), "]")
	return strings.Contains(inner, "::")
}

func ensurePolicyRevisions(policies map[string]map[string]PolicyRevision, policyName string) map[string]PolicyRevision {
	revisions, ok := policies[policyName]
	if ok {
		return revisions
	}
	revisions = make(map[string]PolicyRevision)
	policies[policyName] = revisions
	return revisions
}

func ensurePolicyGroup(groups map[string]PolicyGroup, name string) PolicyGroup {
	group, ok := groups[name]
	if ok {
		if group.Policies == nil {
			group.Policies = map[string]string{}
		}
		return group
	}
	group = PolicyGroup{
		Name:     name,
		Policies: map[string]string{},
	}
	groups[name] = group
	return group
}

func copyPolicyRevisions(in map[string]PolicyRevision) map[string]PolicyRevision {
	out := make(map[string]PolicyRevision, len(in))
	for revisionID, revision := range in {
		out[revisionID] = copyPolicyRevision(revision)
	}
	return out
}

func copyPolicyRevision(in PolicyRevision) PolicyRevision {
	return PolicyRevision{
		Name:       in.Name,
		RevisionID: in.RevisionID,
		Payload:    cloneMap(in.Payload),
	}
}

func copyPolicyGroup(in PolicyGroup) PolicyGroup {
	out := PolicyGroup{
		Name:     in.Name,
		Policies: make(map[string]string, len(in.Policies)),
	}
	for name, revisionID := range in.Policies {
		out.Policies[name] = revisionID
	}
	return out
}

func stringSliceToAny(in []string) []any {
	if len(in) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(in))
	for _, item := range in {
		out = append(out, item)
	}
	return out
}

func stringSliceMapToAny(in map[string][]string) map[string]any {
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
		out[key] = stringSliceToAny(in[key])
	}
	return out
}

func defaultPolicyACL(superuserName string, creator authn.Principal) authz.ACL {
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

func defaultPolicyGroupACL(superuserName string, creator authn.Principal) authz.ACL {
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

func policyACLKey(name string) string {
	return "policy:" + name
}

func policyGroupACLKey(name string) string {
	return "policy_group:" + name
}
