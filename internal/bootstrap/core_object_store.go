package bootstrap

import (
	"strings"
	"sync"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

type CoreObjectStore interface {
	LoadCoreObjects() (CoreObjectState, error)
	SaveCoreObjects(CoreObjectState) error
}

type CoreObjectState struct {
	Orgs map[string]CoreObjectOrganizationState
}

type CoreObjectOrganizationState struct {
	DataBags     map[string]DataBag
	DataBagItems map[string]map[string]DataBagItem
	Environments map[string]Environment
	Nodes        map[string]Node
	Roles        map[string]Role
	Sandboxes    map[string]Sandbox
	Policies     map[string]map[string]PolicyRevision
	PolicyGroups map[string]PolicyGroup
	ACLs         map[string]authz.ACL
}

type MemoryCoreObjectStore struct {
	mu    sync.RWMutex
	state CoreObjectState
}

func NewMemoryCoreObjectStore(initial CoreObjectState) *MemoryCoreObjectStore {
	return &MemoryCoreObjectStore{state: cloneCoreObjectState(initial)}
}

func CloneCoreObjectState(state CoreObjectState) CoreObjectState {
	return cloneCoreObjectState(state)
}

func (s *MemoryCoreObjectStore) LoadCoreObjects() (CoreObjectState, error) {
	if s == nil {
		return CoreObjectState{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneCoreObjectState(s.state), nil
}

func (s *MemoryCoreObjectStore) SaveCoreObjects(state CoreObjectState) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = cloneCoreObjectState(state)
	return nil
}

func (s *Service) snapshotCoreObjectsLocked() CoreObjectState {
	state := CoreObjectState{
		Orgs: make(map[string]CoreObjectOrganizationState, len(s.orgs)),
	}

	for name, org := range s.orgs {
		if org == nil {
			continue
		}
		state.Orgs[name] = CoreObjectOrganizationState{
			DataBags:     cloneDataBags(org.dataBags),
			DataBagItems: cloneDataBagItems(org.dataBagItems),
			Environments: cloneEnvironments(org.envs),
			Nodes:        cloneNodes(org.nodes),
			Roles:        cloneRoles(org.roles),
			Sandboxes:    cloneSandboxes(org.sandboxes),
			Policies:     clonePolicies(org.policies),
			PolicyGroups: clonePolicyGroups(org.policyGroups),
			ACLs:         cloneCoreObjectACLs(org.acls),
		}
	}

	return state
}

func (s *Service) restoreCoreObjectsLocked(state CoreObjectState) {
	state = cloneCoreObjectState(state)
	defaultEnvironmentACLs := s.defaultEnvironmentACLsLocked()

	for name, org := range s.orgs {
		if org == nil {
			continue
		}
		org.dataBags = make(map[string]DataBag)
		org.dataBagItems = make(map[string]map[string]DataBagItem)
		org.envs = map[string]Environment{defaultEnvironmentName: defaultEnvironment()}
		org.nodes = make(map[string]Node)
		org.roles = make(map[string]Role)
		org.sandboxes = make(map[string]Sandbox)
		org.policies = make(map[string]map[string]PolicyRevision)
		org.policyGroups = make(map[string]PolicyGroup)
		removeCoreObjectACLs(org.acls)
		ensureOrganizationStateMaps(org)
		if _, hasPersistedCoreObjects := state.Orgs[name]; !hasPersistedCoreObjects {
			s.ensureDefaultEnvironmentACLLocked(name, org, defaultEnvironmentACLs)
		}
	}

	for name, orgState := range state.Orgs {
		org := s.orgs[name]
		if org == nil {
			org = newOrganizationState(Organization{
				Name:     name,
				FullName: name,
				OrgType:  "Business",
				GUID:     name,
			})
			s.orgs[name] = org
		}
		org.dataBags = cloneDataBags(orgState.DataBags)
		org.dataBagItems = cloneDataBagItems(orgState.DataBagItems)
		org.envs = cloneEnvironments(orgState.Environments)
		org.nodes = cloneNodes(orgState.Nodes)
		org.roles = cloneRoles(orgState.Roles)
		org.sandboxes = cloneSandboxes(orgState.Sandboxes)
		org.policies = clonePolicies(orgState.Policies)
		org.policyGroups = clonePolicyGroups(orgState.PolicyGroups)
		removeCoreObjectACLs(org.acls)
		for key, acl := range orgState.ACLs {
			org.acls[key] = cloneACL(acl)
		}
		ensureOrganizationStateMaps(org)
		s.ensureDefaultEnvironmentACLLocked(name, org, defaultEnvironmentACLs)
	}
}

func (s *Service) defaultEnvironmentACLsLocked() map[string]authz.ACL {
	key := environmentACLKey(defaultEnvironmentName)
	out := make(map[string]authz.ACL, len(s.orgs))
	for name, org := range s.orgs {
		if org == nil || org.acls == nil {
			continue
		}
		if acl, ok := org.acls[key]; ok {
			out[name] = cloneACL(acl)
		}
	}
	return out
}

func (s *Service) ensureDefaultEnvironmentACLLocked(orgName string, org *organizationState, preserved map[string]authz.ACL) {
	if org == nil {
		return
	}
	if org.acls == nil {
		org.acls = make(map[string]authz.ACL)
	}
	key := environmentACLKey(defaultEnvironmentName)
	if _, ok := org.acls[key]; ok {
		return
	}
	if acl, ok := preserved[orgName]; ok {
		org.acls[key] = cloneACL(acl)
		return
	}
	org.acls[key] = defaultEnvironmentACL(s.superuserName, authn.Principal{
		Type: "user",
		Name: s.superuserName,
	})
}

func (s *Service) persistCoreObjectsLocked() error {
	if s.coreObjectStore == nil {
		return nil
	}
	return s.coreObjectStore.SaveCoreObjects(s.snapshotCoreObjectsLocked())
}

func (s *Service) finishCoreObjectMutationLocked(previous CoreObjectState) error {
	if err := s.persistCoreObjectsLocked(); err != nil {
		s.restoreCoreObjectsLocked(previous)
		return err
	}
	return nil
}

func cloneCoreObjectState(state CoreObjectState) CoreObjectState {
	out := CoreObjectState{
		Orgs: make(map[string]CoreObjectOrganizationState, len(state.Orgs)),
	}
	for name, org := range state.Orgs {
		out.Orgs[name] = CoreObjectOrganizationState{
			DataBags:     cloneDataBags(org.DataBags),
			DataBagItems: cloneDataBagItems(org.DataBagItems),
			Environments: cloneEnvironments(org.Environments),
			Nodes:        cloneNodes(org.Nodes),
			Roles:        cloneRoles(org.Roles),
			Sandboxes:    cloneSandboxes(org.Sandboxes),
			Policies:     clonePolicies(org.Policies),
			PolicyGroups: clonePolicyGroups(org.PolicyGroups),
			ACLs:         cloneACLs(org.ACLs),
		}
	}
	return out
}

func cloneDataBags(in map[string]DataBag) map[string]DataBag {
	out := make(map[string]DataBag, len(in))
	for name, bag := range in {
		out[name] = bag
	}
	return out
}

func cloneDataBagItems(in map[string]map[string]DataBagItem) map[string]map[string]DataBagItem {
	out := make(map[string]map[string]DataBagItem, len(in))
	for bagName, items := range in {
		out[bagName] = make(map[string]DataBagItem, len(items))
		for itemID, item := range items {
			out[bagName][itemID] = copyDataBagItem(item)
		}
	}
	return out
}

func cloneEnvironments(in map[string]Environment) map[string]Environment {
	out := make(map[string]Environment, len(in))
	for name, env := range in {
		out[name] = copyEnvironment(env)
	}
	return out
}

func cloneNodes(in map[string]Node) map[string]Node {
	out := make(map[string]Node, len(in))
	for name, node := range in {
		out[name] = copyNode(node)
	}
	return out
}

func cloneRoles(in map[string]Role) map[string]Role {
	out := make(map[string]Role, len(in))
	for name, role := range in {
		out[name] = copyRole(role)
	}
	return out
}

func cloneSandboxes(in map[string]Sandbox) map[string]Sandbox {
	out := make(map[string]Sandbox, len(in))
	for id, sandbox := range in {
		out[id] = copySandbox(sandbox)
	}
	return out
}

func clonePolicies(in map[string]map[string]PolicyRevision) map[string]map[string]PolicyRevision {
	out := make(map[string]map[string]PolicyRevision, len(in))
	for name, revisions := range in {
		out[name] = copyPolicyRevisions(revisions)
	}
	return out
}

func clonePolicyGroups(in map[string]PolicyGroup) map[string]PolicyGroup {
	out := make(map[string]PolicyGroup, len(in))
	for name, group := range in {
		out[name] = copyPolicyGroup(group)
	}
	return out
}

func cloneCoreObjectACLs(in map[string]authz.ACL) map[string]authz.ACL {
	out := make(map[string]authz.ACL)
	for key, acl := range in {
		if !isCoreObjectACLKey(key) {
			continue
		}
		out[key] = cloneACL(acl)
	}
	return out
}

func removeCoreObjectACLs(in map[string]authz.ACL) {
	for key := range in {
		if isCoreObjectACLKey(key) {
			delete(in, key)
		}
	}
}

func isCoreObjectACLKey(key string) bool {
	switch {
	case strings.HasPrefix(key, "data_bag:"):
		return true
	case strings.HasPrefix(key, "environment:"):
		return true
	case strings.HasPrefix(key, "node:"):
		return true
	case strings.HasPrefix(key, "policy:"):
		return true
	case strings.HasPrefix(key, "policy_group:"):
		return true
	case strings.HasPrefix(key, "role:"):
		return true
	default:
		return false
	}
}
