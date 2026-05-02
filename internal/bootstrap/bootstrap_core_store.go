package bootstrap

import (
	"fmt"
	"sync"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

type BootstrapCoreStore interface {
	LoadBootstrapCore() (BootstrapCoreState, error)
	SaveBootstrapCore(BootstrapCoreState) error
}

type BootstrapCoreState struct {
	Users    map[string]User
	UserACLs map[string]authz.ACL
	UserKeys map[string]map[string]KeyRecord
	Orgs     map[string]BootstrapCoreOrganizationState
}

type BootstrapCoreOrganizationState struct {
	Organization Organization
	Clients      map[string]Client
	ClientKeys   map[string]map[string]KeyRecord
	Groups       map[string]Group
	Containers   map[string]Container
	ACLs         map[string]authz.ACL
}

type MemoryBootstrapCoreStore struct {
	mu    sync.RWMutex
	state BootstrapCoreState
}

func NewMemoryBootstrapCoreStore(initial BootstrapCoreState) *MemoryBootstrapCoreStore {
	return &MemoryBootstrapCoreStore{state: cloneBootstrapCoreState(initial)}
}

func CloneBootstrapCoreState(state BootstrapCoreState) BootstrapCoreState {
	return cloneBootstrapCoreState(state)
}

func (s *MemoryBootstrapCoreStore) LoadBootstrapCore() (BootstrapCoreState, error) {
	if s == nil {
		return BootstrapCoreState{}, nil
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return cloneBootstrapCoreState(s.state), nil
}

func (s *MemoryBootstrapCoreStore) SaveBootstrapCore(state BootstrapCoreState) error {
	if s == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = cloneBootstrapCoreState(state)
	return nil
}

func (s *Service) RehydrateKeyStore() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.rehydrateKeyStoreLocked()
}

// ReloadPersistedState refreshes the service's in-memory bootstrap/core object
// maps from its configured stores and rebuilds the request verifier cache. It
// is intentionally all-or-nothing so future online repair workflows cannot
// leave live reads using partially refreshed state.
func (s *Service) ReloadPersistedState() error {
	if s == nil {
		return fmt.Errorf("bootstrap service is required")
	}

	var bootstrapState BootstrapCoreState
	if s.bootstrapCoreStore != nil {
		loaded, err := s.bootstrapCoreStore.LoadBootstrapCore()
		if err != nil {
			return fmt.Errorf("load bootstrap core state: %w", err)
		}
		bootstrapState = loaded
	}
	var coreObjectState CoreObjectState
	if s.coreObjectStore != nil {
		loaded, err := s.coreObjectStore.LoadCoreObjects()
		if err != nil {
			return fmt.Errorf("load core object state: %w", err)
		}
		coreObjectState = loaded
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	previousBootstrap := s.snapshotBootstrapCoreLocked()
	previousCoreObjects := s.snapshotCoreObjectsLocked()
	if s.bootstrapCoreStore != nil {
		s.restoreBootstrapCoreLocked(bootstrapState)
	}
	if s.coreObjectStore != nil {
		s.restoreCoreObjectsLocked(coreObjectState)
	}
	s.ensureUserLocked(s.superuserName)
	if err := s.rehydrateKeyStoreLocked(); err != nil {
		s.restoreBootstrapCoreLocked(previousBootstrap)
		s.restoreCoreObjectsLocked(previousCoreObjects)
		if hydrateErr := s.rehydrateKeyStoreLocked(); hydrateErr != nil {
			return fmt.Errorf("reload persisted state: %w; additionally failed to restore key verifier cache: %v", err, hydrateErr)
		}
		return fmt.Errorf("reload persisted state: %w", err)
	}
	return nil
}

func (s *Service) snapshotBootstrapCoreLocked() BootstrapCoreState {
	state := BootstrapCoreState{
		Users:    cloneUsers(s.users),
		UserACLs: cloneACLs(s.userACLs),
		UserKeys: cloneNestedKeys(s.userKeys),
		Orgs:     make(map[string]BootstrapCoreOrganizationState, len(s.orgs)),
	}

	for name, org := range s.orgs {
		if org == nil {
			continue
		}
		state.Orgs[name] = BootstrapCoreOrganizationState{
			Organization: org.org,
			Clients:      cloneClients(org.clients),
			ClientKeys:   cloneNestedKeys(org.clientKeys),
			Groups:       cloneGroups(org.groups),
			Containers:   cloneContainers(org.containers),
			ACLs:         cloneACLs(org.acls),
		}
	}

	return state
}

func (s *Service) restoreBootstrapCoreLocked(state BootstrapCoreState) {
	state = cloneBootstrapCoreState(state)

	s.users = state.Users
	if s.users == nil {
		s.users = make(map[string]User)
	}
	s.userACLs = state.UserACLs
	if s.userACLs == nil {
		s.userACLs = make(map[string]authz.ACL)
	}
	s.userKeys = state.UserKeys
	if s.userKeys == nil {
		s.userKeys = make(map[string]map[string]KeyRecord)
	}

	for name := range s.users {
		if _, ok := s.userACLs[name]; !ok {
			s.userACLs[name] = defaultUserACL(s.superuserName, name)
		}
	}

	for name := range s.orgs {
		if _, ok := state.Orgs[name]; !ok {
			delete(s.orgs, name)
		}
	}

	for name, orgState := range state.Orgs {
		org := s.orgs[name]
		if org == nil {
			org = newOrganizationState(orgState.Organization)
			s.orgs[name] = org
		}
		org.org = orgState.Organization
		org.clients = cloneClients(orgState.Clients)
		org.clientKeys = cloneNestedKeys(orgState.ClientKeys)
		org.groups = cloneGroups(orgState.Groups)
		org.containers = cloneContainers(orgState.Containers)
		org.acls = cloneACLs(orgState.ACLs)
		ensureOrganizationStateMaps(org)
	}
}

func (s *Service) persistBootstrapCoreLocked() error {
	if s.bootstrapCoreStore == nil {
		return nil
	}
	return s.bootstrapCoreStore.SaveBootstrapCore(s.snapshotBootstrapCoreLocked())
}

func (s *Service) finishBootstrapCoreMutationLocked(previous BootstrapCoreState) error {
	if err := s.persistBootstrapCoreLocked(); err != nil {
		s.restoreBootstrapCoreLocked(previous)
		if hydrateErr := s.rehydrateKeyStoreLocked(); hydrateErr != nil {
			return fmt.Errorf("%w; additionally failed to restore key verifier cache: %v", err, hydrateErr)
		}
		return err
	}
	return nil
}

func (s *Service) rehydrateKeyStoreLocked() error {
	var keys []authn.Key
	for username, records := range s.userKeys {
		if _, ok := s.users[username]; !ok {
			continue
		}
		for _, record := range records {
			key, err := authnKeyFromRecord(authn.Principal{
				Type: "user",
				Name: username,
			}, record)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
	}

	for orgName, org := range s.orgs {
		for clientName, records := range org.clientKeys {
			if _, ok := org.clients[clientName]; !ok {
				continue
			}
			for _, record := range records {
				key, err := authnKeyFromRecord(authn.Principal{
					Type:         "client",
					Name:         clientName,
					Organization: orgName,
				}, record)
				if err != nil {
					return err
				}
				keys = append(keys, key)
			}
		}
	}

	return s.keyStore.Replace(keys)
}

func authnKeyFromRecord(principal authn.Principal, record KeyRecord) (authn.Key, error) {
	publicKey, err := authn.ParseRSAPublicKeyPEM([]byte(record.PublicKeyPEM))
	if err != nil {
		return authn.Key{}, fmt.Errorf("parse public key for %s/%s key %s: %w", principal.Organization, principal.Name, record.Name, err)
	}
	return authn.Key{
		ID:        record.Name,
		Principal: principal,
		PublicKey: publicKey,
		ExpiresAt: record.ExpiresAt,
	}, nil
}

func newOrganizationState(org Organization) *organizationState {
	state := &organizationState{
		org:               org,
		clients:           make(map[string]Client),
		clientKeys:        make(map[string]map[string]KeyRecord),
		cookbooks:         make(map[string]map[string]CookbookVersion),
		cookbookArtifacts: make(map[string]map[string]CookbookArtifact),
		dataBags:          make(map[string]DataBag),
		dataBagItems:      make(map[string]map[string]DataBagItem),
		envs:              make(map[string]Environment),
		nodes:             make(map[string]Node),
		roles:             make(map[string]Role),
		sandboxes:         make(map[string]Sandbox),
		policies:          make(map[string]map[string]PolicyRevision),
		policyGroups:      make(map[string]PolicyGroup),
		groups:            make(map[string]Group),
		containers:        make(map[string]Container),
		acls:              make(map[string]authz.ACL),
	}
	ensureOrganizationStateMaps(state)
	return state
}

func ensureOrganizationStateMaps(org *organizationState) {
	if org.clients == nil {
		org.clients = make(map[string]Client)
	}
	if org.clientKeys == nil {
		org.clientKeys = make(map[string]map[string]KeyRecord)
	}
	if org.cookbooks == nil {
		org.cookbooks = make(map[string]map[string]CookbookVersion)
	}
	if org.cookbookArtifacts == nil {
		org.cookbookArtifacts = make(map[string]map[string]CookbookArtifact)
	}
	if org.dataBags == nil {
		org.dataBags = make(map[string]DataBag)
	}
	if org.dataBagItems == nil {
		org.dataBagItems = make(map[string]map[string]DataBagItem)
	}
	if org.envs == nil {
		org.envs = make(map[string]Environment)
	}
	if _, ok := org.envs[defaultEnvironmentName]; !ok {
		org.envs[defaultEnvironmentName] = defaultEnvironment()
	}
	if org.nodes == nil {
		org.nodes = make(map[string]Node)
	}
	if org.roles == nil {
		org.roles = make(map[string]Role)
	}
	if org.sandboxes == nil {
		org.sandboxes = make(map[string]Sandbox)
	}
	if org.policies == nil {
		org.policies = make(map[string]map[string]PolicyRevision)
	}
	if org.policyGroups == nil {
		org.policyGroups = make(map[string]PolicyGroup)
	}
	if org.groups == nil {
		org.groups = make(map[string]Group)
	}
	if org.containers == nil {
		org.containers = make(map[string]Container)
	}
	if org.acls == nil {
		org.acls = make(map[string]authz.ACL)
	}
}

func cloneBootstrapCoreState(state BootstrapCoreState) BootstrapCoreState {
	out := BootstrapCoreState{
		Users:    cloneUsers(state.Users),
		UserACLs: cloneACLs(state.UserACLs),
		UserKeys: cloneNestedKeys(state.UserKeys),
		Orgs:     make(map[string]BootstrapCoreOrganizationState, len(state.Orgs)),
	}
	for name, org := range state.Orgs {
		out.Orgs[name] = BootstrapCoreOrganizationState{
			Organization: org.Organization,
			Clients:      cloneClients(org.Clients),
			ClientKeys:   cloneNestedKeys(org.ClientKeys),
			Groups:       cloneGroups(org.Groups),
			Containers:   cloneContainers(org.Containers),
			ACLs:         cloneACLs(org.ACLs),
		}
	}
	return out
}

func cloneUsers(in map[string]User) map[string]User {
	out := make(map[string]User, len(in))
	for name, user := range in {
		out[name] = user
	}
	return out
}

func cloneClients(in map[string]Client) map[string]Client {
	out := make(map[string]Client, len(in))
	for name, client := range in {
		out[name] = client
	}
	return out
}

func cloneGroups(in map[string]Group) map[string]Group {
	out := make(map[string]Group, len(in))
	for name, group := range in {
		out[name] = Group{
			Name:         group.Name,
			GroupName:    group.GroupName,
			Organization: group.Organization,
			Actors:       append([]string{}, group.Actors...),
			Users:        append([]string{}, group.Users...),
			Clients:      append([]string{}, group.Clients...),
			Groups:       append([]string{}, group.Groups...),
		}
	}
	return out
}

func cloneContainers(in map[string]Container) map[string]Container {
	out := make(map[string]Container, len(in))
	for name, container := range in {
		out[name] = container
	}
	return out
}

func cloneACLs(in map[string]authz.ACL) map[string]authz.ACL {
	out := make(map[string]authz.ACL, len(in))
	for name, acl := range in {
		out[name] = cloneACL(acl)
	}
	return out
}

func cloneACL(acl authz.ACL) authz.ACL {
	return authz.ACL{
		Create: clonePermission(acl.Create),
		Read:   clonePermission(acl.Read),
		Update: clonePermission(acl.Update),
		Delete: clonePermission(acl.Delete),
		Grant:  clonePermission(acl.Grant),
	}
}

func clonePermission(permission authz.Permission) authz.Permission {
	return authz.Permission{
		Actors: append([]string(nil), permission.Actors...),
		Groups: append([]string(nil), permission.Groups...),
	}
}

func cloneNestedKeys(in map[string]map[string]KeyRecord) map[string]map[string]KeyRecord {
	out := make(map[string]map[string]KeyRecord, len(in))
	for principal, records := range in {
		out[principal] = cloneKeyRecords(records)
	}
	return out
}

func cloneKeyRecords(in map[string]KeyRecord) map[string]KeyRecord {
	out := make(map[string]KeyRecord, len(in))
	for name, record := range in {
		out[name] = cloneKeyRecord(record)
	}
	return out
}

func cloneKeyRecord(record KeyRecord) KeyRecord {
	if record.ExpiresAt != nil {
		expiresAt := *record.ExpiresAt
		record.ExpiresAt = &expiresAt
	}
	return record
}
