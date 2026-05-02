package bootstrap

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

type AdminRepairResult struct {
	Changed  bool
	Repaired []string
}

// RepairDefaultACLsInput scopes a default-ACL repair to one organization when
// requested. DryRun is used by tests and future previews to reuse the same
// normalization path without persisting changes.
type RepairDefaultACLsInput struct {
	Organization string
	DryRun       bool
}

// RepairDefaultACLsResult reports the two ACL families repaired by the live
// service seam so operators can distinguish bootstrap ACLs from object ACLs.
type RepairDefaultACLsResult struct {
	Changed            bool
	BootstrapRepaired  []string
	CoreObjectRepaired []string
}

// RepairDefaultACLs repairs missing default ACL documents through the live
// service state and its configured persistence stores. Online maintenance
// repair uses this seam instead of writing PostgreSQL directly so process-local
// authorization caches cannot stay stale after the command reports success.
func (s *Service) RepairDefaultACLs(input RepairDefaultACLsInput) (RepairDefaultACLsResult, error) {
	if s == nil {
		return RepairDefaultACLsResult{}, fmt.Errorf("%w: bootstrap service is required", ErrInvalidInput)
	}

	orgFilter := strings.TrimSpace(input.Organization)

	s.mu.Lock()
	defer s.mu.Unlock()

	previousBootstrap := s.snapshotBootstrapCoreLocked()
	previousCoreObjects := s.snapshotCoreObjectsLocked()
	if orgFilter != "" {
		if _, ok := previousBootstrap.Orgs[orgFilter]; !ok {
			return RepairDefaultACLsResult{}, fmt.Errorf("%w: organization %s not found", ErrNotFound, orgFilter)
		}
	}

	nextBootstrap, bootstrapRepair := RepairBootstrapCoreDefaultACLs(previousBootstrap, orgFilter, s.superuserName)
	nextCoreObjects, coreObjectRepair := RepairCoreObjectDefaultACLs(previousCoreObjects, orgFilter, s.superuserName)
	result := RepairDefaultACLsResult{
		Changed:            bootstrapRepair.Changed || coreObjectRepair.Changed,
		BootstrapRepaired:  append([]string(nil), bootstrapRepair.Repaired...),
		CoreObjectRepaired: append([]string(nil), coreObjectRepair.Repaired...),
	}
	if input.DryRun || !result.Changed {
		return result, nil
	}

	s.restoreBootstrapCoreLocked(nextBootstrap)
	s.restoreCoreObjectsLocked(nextCoreObjects)
	if err := s.persistBootstrapCoreLocked(); err != nil {
		s.restoreBootstrapCoreLocked(previousBootstrap)
		s.restoreCoreObjectsLocked(previousCoreObjects)
		return RepairDefaultACLsResult{}, fmt.Errorf("save repaired bootstrap ACLs: %w", err)
	}
	if err := s.persistCoreObjectsLocked(); err != nil {
		s.restoreBootstrapCoreLocked(previousBootstrap)
		s.restoreCoreObjectsLocked(previousCoreObjects)
		if s.bootstrapCoreStore != nil {
			if rollbackErr := s.bootstrapCoreStore.SaveBootstrapCore(previousBootstrap); rollbackErr != nil {
				return RepairDefaultACLsResult{}, fmt.Errorf("save repaired core object ACLs: %w; additionally failed to roll back bootstrap ACLs: %v", err, rollbackErr)
			}
		}
		return RepairDefaultACLsResult{}, fmt.Errorf("save repaired core object ACLs: %w", err)
	}
	return result, nil
}

func AddUserToBootstrapCoreOrg(state BootstrapCoreState, orgName, username string, admin bool) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	org, ok := state.Orgs[strings.TrimSpace(orgName)]
	if !ok {
		return state, nil, fmt.Errorf("%w: organization not found", ErrNotFound)
	}
	username = strings.TrimSpace(username)
	if _, ok := state.Users[username]; !ok {
		return state, nil, fmt.Errorf("%w: user not found", ErrNotFound)
	}
	if org.Groups == nil {
		return state, nil, fmt.Errorf("%w: organization groups not found", ErrNotFound)
	}

	var changed []string
	var err error
	org.Groups, changed, err = addUserToOrgGroups(org.Groups, username, admin)
	if err != nil {
		return state, nil, err
	}
	state.Orgs[strings.TrimSpace(orgName)] = org
	return state, changed, nil
}

func RemoveUserFromBootstrapCoreOrg(state BootstrapCoreState, orgName, username string, force bool) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	org, ok := state.Orgs[strings.TrimSpace(orgName)]
	if !ok {
		return state, nil, fmt.Errorf("%w: organization not found", ErrNotFound)
	}
	username = strings.TrimSpace(username)
	if _, ok := state.Users[username]; !ok && !force {
		return state, nil, fmt.Errorf("%w: user not found", ErrNotFound)
	}

	changed := removeUserFromAllGroups(org.Groups, username)
	org.Groups = normalizeGroupActors(org.Groups)
	state.Orgs[strings.TrimSpace(orgName)] = org
	return state, changed, nil
}

func AddActorToBootstrapCoreGroup(state BootstrapCoreState, orgName, groupName, actorType, actorName string) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	org, group, err := bootstrapCoreGroup(state, orgName, groupName)
	if err != nil {
		return state, nil, err
	}
	if err := validateBootstrapCoreActor(state, org, actorType, actorName); err != nil {
		return state, nil, err
	}

	var changed []string
	if !groupHasMember(group, actorType, actorName) {
		changed = append(changed, groupMembershipLabel(strings.TrimSpace(actorType), strings.TrimSpace(actorName)))
	}
	group = addGroupMember(group, actorType, actorName)
	org.Groups[strings.TrimSpace(groupName)] = group
	state.Orgs[strings.TrimSpace(orgName)] = org
	return state, changed, nil
}

func RemoveActorFromBootstrapCoreGroup(state BootstrapCoreState, orgName, groupName, actorType, actorName string) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	org, group, err := bootstrapCoreGroup(state, orgName, groupName)
	if err != nil {
		return state, nil, err
	}

	var changed []string
	if groupHasMember(group, actorType, actorName) {
		changed = append(changed, groupMembershipLabel(strings.TrimSpace(actorType), strings.TrimSpace(actorName)))
	}
	group, err = removeGroupMember(group, actorType, actorName)
	if err != nil {
		return state, nil, err
	}
	org.Groups[strings.TrimSpace(groupName)] = group
	state.Orgs[strings.TrimSpace(orgName)] = org
	return state, changed, nil
}

func ListBootstrapServerAdmins(state BootstrapCoreState) []string {
	seen := map[string]struct{}{}
	for _, org := range state.Orgs {
		group, ok := org.Groups["admins"]
		if !ok {
			continue
		}
		for _, user := range group.Users {
			if user != "" {
				seen[user] = struct{}{}
			}
		}
	}

	out := make([]string, 0, len(seen))
	for user := range seen {
		out = append(out, user)
	}
	sort.Strings(out)
	return out
}

func GrantBootstrapServerAdmin(state BootstrapCoreState, username string) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	username = strings.TrimSpace(username)
	if _, ok := state.Users[username]; !ok {
		return state, nil, fmt.Errorf("%w: user not found", ErrNotFound)
	}

	var changed []string
	for orgName, org := range state.Orgs {
		group, ok := org.Groups["admins"]
		if !ok {
			continue
		}
		before := len(group.Users)
		group.Users = uniqueSorted(append(group.Users, username))
		group.Actors = uniqueSorted(append(group.Users, group.Clients...))
		org.Groups["admins"] = group
		state.Orgs[orgName] = org
		if len(group.Users) != before {
			changed = append(changed, orgName+"/admins/user:"+username)
		}
	}
	sort.Strings(changed)
	return state, changed, nil
}

func RevokeBootstrapServerAdmin(state BootstrapCoreState, username string) (BootstrapCoreState, []string, error) {
	state = cloneBootstrapCoreState(state)
	username = strings.TrimSpace(username)
	if _, ok := state.Users[username]; !ok {
		return state, nil, fmt.Errorf("%w: user not found", ErrNotFound)
	}

	var changed []string
	for orgName, org := range state.Orgs {
		group, ok := org.Groups["admins"]
		if !ok {
			continue
		}
		before := len(group.Users)
		group.Users = withoutString(group.Users, username)
		group.Actors = uniqueSorted(append(group.Users, group.Clients...))
		org.Groups["admins"] = group
		state.Orgs[orgName] = org
		if len(group.Users) != before {
			changed = append(changed, orgName+"/admins/user:"+username)
		}
	}
	sort.Strings(changed)
	return state, changed, nil
}

func RepairBootstrapCoreDefaultACLs(state BootstrapCoreState, orgFilter, superuserName string) (BootstrapCoreState, AdminRepairResult) {
	state = cloneBootstrapCoreState(state)
	superuserName = fallback(superuserName, "pivotal")
	result := AdminRepairResult{}

	if state.UserACLs == nil {
		state.UserACLs = make(map[string]authz.ACL)
	}
	for _, username := range sortedStringKeys(state.Users) {
		if _, ok := state.UserACLs[username]; ok {
			continue
		}
		state.UserACLs[username] = defaultUserACL(superuserName, username)
		result.Repaired = append(result.Repaired, "user:"+username)
	}

	for _, orgName := range sortedStringKeys(state.Orgs) {
		if orgFilter != "" && orgName != orgFilter {
			continue
		}
		org := state.Orgs[orgName]
		if org.ACLs == nil {
			org.ACLs = make(map[string]authz.ACL)
		}
		addACLIfMissing(org.ACLs, organizationACLKey(), defaultOrganizationACL(superuserName), orgName, &result)
		for _, name := range sortedStringKeys(org.Containers) {
			addACLIfMissing(org.ACLs, containerACLKey(name), defaultContainerACL(superuserName, name), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.Groups) {
			addACLIfMissing(org.ACLs, groupACLKey(name), defaultGroupACL(superuserName, name), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.Clients) {
			client := org.Clients[name]
			actors := []string{}
			if !client.Validator {
				actors = append(actors, client.Name)
			}
			addACLIfMissing(org.ACLs, clientACLKey(name), defaultClientACL(superuserName, actors...), orgName, &result)
		}
		state.Orgs[orgName] = org
	}

	result.Changed = len(result.Repaired) > 0
	return state, result
}

func RepairCoreObjectDefaultACLs(state CoreObjectState, orgFilter, superuserName string) (CoreObjectState, AdminRepairResult) {
	state = cloneCoreObjectState(state)
	superuserName = fallback(superuserName, "pivotal")
	result := AdminRepairResult{}
	creator := authn.Principal{}

	for _, orgName := range sortedStringKeys(state.Orgs) {
		if orgFilter != "" && orgName != orgFilter {
			continue
		}
		org := state.Orgs[orgName]
		if org.ACLs == nil {
			org.ACLs = make(map[string]authz.ACL)
		}
		for _, name := range sortedStringKeys(org.Environments) {
			addACLIfMissing(org.ACLs, environmentACLKey(name), defaultEnvironmentACL(superuserName, creator), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.Nodes) {
			addACLIfMissing(org.ACLs, nodeACLKey(name), defaultNodeACL(superuserName, creator), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.Roles) {
			addACLIfMissing(org.ACLs, roleACLKey(name), defaultRoleACL(superuserName, creator), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.DataBags) {
			addACLIfMissing(org.ACLs, dataBagACLKey(name), defaultDataBagACL(superuserName, creator), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.Policies) {
			addACLIfMissing(org.ACLs, policyACLKey(name), defaultPolicyACL(superuserName, creator), orgName, &result)
		}
		for _, name := range sortedStringKeys(org.PolicyGroups) {
			addACLIfMissing(org.ACLs, policyGroupACLKey(name), defaultPolicyGroupACL(superuserName, creator), orgName, &result)
		}
		state.Orgs[orgName] = org
	}

	result.Changed = len(result.Repaired) > 0
	return state, result
}

func addUserToOrgGroups(groups map[string]Group, username string, admin bool) (map[string]Group, []string, error) {
	var targets []string
	if admin {
		targets = append(targets, "admins")
	}
	targets = append(targets, "users")

	changed := make([]string, 0, len(targets))
	for _, groupName := range targets {
		group, ok := groups[groupName]
		if !ok {
			return groups, nil, fmt.Errorf("%w: group %s not found", ErrNotFound, groupName)
		}
		before := len(group.Users)
		group.Users = uniqueSorted(append(group.Users, username))
		group.Actors = uniqueSorted(append(group.Users, group.Clients...))
		groups[groupName] = group
		if len(group.Users) != before {
			changed = append(changed, groupName+"/user:"+username)
		}
	}
	return groups, changed, nil
}

func removeUserFromAllGroups(groups map[string]Group, username string) []string {
	var changed []string
	for groupName, group := range groups {
		before := len(group.Users)
		group.Users = withoutString(group.Users, username)
		if len(group.Users) != before {
			changed = append(changed, groupName+"/user:"+username)
		}
		groups[groupName] = group
	}
	sort.Strings(changed)
	return changed
}

func normalizeGroupActors(groups map[string]Group) map[string]Group {
	for name, group := range groups {
		group.Users = uniqueSorted(group.Users)
		group.Clients = uniqueSorted(group.Clients)
		group.Groups = uniqueSorted(group.Groups)
		group.Actors = uniqueSorted(append(group.Users, group.Clients...))
		groups[name] = group
	}
	return groups
}

func bootstrapCoreGroup(state BootstrapCoreState, orgName, groupName string) (BootstrapCoreOrganizationState, Group, error) {
	orgName = strings.TrimSpace(orgName)
	groupName = strings.TrimSpace(groupName)
	org, ok := state.Orgs[orgName]
	if !ok {
		return BootstrapCoreOrganizationState{}, Group{}, fmt.Errorf("%w: organization not found", ErrNotFound)
	}
	group, ok := org.Groups[groupName]
	if !ok {
		return BootstrapCoreOrganizationState{}, Group{}, fmt.Errorf("%w: group not found", ErrNotFound)
	}
	return org, group, nil
}

func validateBootstrapCoreActor(state BootstrapCoreState, org BootstrapCoreOrganizationState, actorType, actorName string) error {
	actorType = strings.TrimSpace(actorType)
	actorName = strings.TrimSpace(actorName)
	switch actorType {
	case "user":
		if _, ok := state.Users[actorName]; !ok {
			return fmt.Errorf("%w: user not found", ErrNotFound)
		}
	case "client":
		if _, ok := org.Clients[actorName]; !ok {
			return fmt.Errorf("%w: client not found", ErrNotFound)
		}
	case "group":
		if _, ok := org.Groups[actorName]; !ok {
			return fmt.Errorf("%w: member group not found", ErrNotFound)
		}
	default:
		return fmt.Errorf("%w: actor-type must be user, client, or group", ErrInvalidInput)
	}
	return nil
}

func addGroupMember(group Group, actorType, actorName string) Group {
	actorType = strings.TrimSpace(actorType)
	actorName = strings.TrimSpace(actorName)
	switch actorType {
	case "user":
		group.Users = uniqueSorted(append(group.Users, actorName))
	case "client":
		group.Clients = uniqueSorted(append(group.Clients, actorName))
	case "group":
		group.Groups = uniqueSorted(append(group.Groups, actorName))
	}
	group.Actors = uniqueSorted(append(group.Users, group.Clients...))
	return group
}

func removeGroupMember(group Group, actorType, actorName string) (Group, error) {
	actorType = strings.TrimSpace(actorType)
	actorName = strings.TrimSpace(actorName)
	switch actorType {
	case "user":
		group.Users = withoutString(group.Users, actorName)
	case "client":
		group.Clients = withoutString(group.Clients, actorName)
	case "group":
		group.Groups = withoutString(group.Groups, actorName)
	default:
		return group, fmt.Errorf("%w: actor-type must be user, client, or group", ErrInvalidInput)
	}
	group.Actors = uniqueSorted(append(group.Users, group.Clients...))
	return group, nil
}

func groupHasMember(group Group, actorType, actorName string) bool {
	actorType = strings.TrimSpace(actorType)
	actorName = strings.TrimSpace(actorName)
	switch actorType {
	case "user":
		return contains(group.Users, actorName)
	case "client":
		return contains(group.Clients, actorName)
	case "group":
		return contains(group.Groups, actorName)
	default:
		return false
	}
}

func groupMembershipLabel(actorType, actorName string) string {
	return strings.TrimSpace(actorType) + ":" + strings.TrimSpace(actorName)
}

func addACLIfMissing(acls map[string]authz.ACL, key string, acl authz.ACL, orgName string, result *AdminRepairResult) {
	if _, ok := acls[key]; ok {
		return
	}
	acls[key] = acl
	if orgName == "" {
		result.Repaired = append(result.Repaired, key)
		return
	}
	result.Repaired = append(result.Repaired, orgName+"/"+key)
}

func sortedStringKeys[V any](in map[string]V) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
