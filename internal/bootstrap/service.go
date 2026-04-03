package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var (
	ErrConflict     = errors.New("resource already exists")
	ErrImmutable    = errors.New("resource cannot be modified")
	ErrInvalidInput = errors.New("invalid input")
	ErrNotFound     = errors.New("resource not found")
)

var validNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)

var defaultContainers = []string{
	"clients",
	"containers",
	"cookbooks",
	"data",
	"environments",
	"groups",
	"nodes",
	"roles",
	"sandboxes",
	"policies",
	"policy_groups",
	"cookbook_artifacts",
}

type Options struct {
	SuperuserName string
}

type Service struct {
	mu            sync.RWMutex
	keyStore      *authn.MemoryKeyStore
	superuserName string
	users         map[string]User
	userACLs      map[string]authz.ACL
	userKeys      map[string]map[string]KeyRecord
	orgs          map[string]*organizationState
}

type User struct {
	Username    string `json:"username"`
	DisplayName string `json:"display_name,omitempty"`
	Email       string `json:"email,omitempty"`
	FirstName   string `json:"first_name,omitempty"`
	LastName    string `json:"last_name,omitempty"`
}

type Organization struct {
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	OrgType  string `json:"org_type,omitempty"`
	GUID     string `json:"guid"`
}

type Client struct {
	Name         string `json:"name"`
	ClientName   string `json:"clientname"`
	Organization string `json:"orgname"`
	Validator    bool   `json:"validator"`
	Admin        bool   `json:"admin"`
	PublicKey    string `json:"public_key,omitempty"`
	URI          string `json:"uri,omitempty"`
}

type Group struct {
	Name         string   `json:"name"`
	GroupName    string   `json:"groupname"`
	Organization string   `json:"orgname"`
	Actors       []string `json:"actors"`
	Users        []string `json:"users"`
	Clients      []string `json:"clients"`
	Groups       []string `json:"groups,omitempty"`
}

type Container struct {
	Name          string `json:"name,omitempty"`
	ContainerName string `json:"containername"`
	ContainerPath string `json:"containerpath"`
}

type KeyMaterial struct {
	Name           string `json:"name"`
	URI            string `json:"uri"`
	PrivateKeyPEM  string `json:"private_key,omitempty"`
	PublicKeyPEM   string `json:"public_key,omitempty"`
	ExpirationDate string `json:"expiration_date"`
}

type KeyRecord struct {
	Name           string     `json:"name"`
	URI            string     `json:"uri"`
	PublicKeyPEM   string     `json:"public_key"`
	ExpirationDate string     `json:"expiration_date"`
	Expired        bool       `json:"expired"`
	ExpiresAt      *time.Time `json:"-"`
}

type CreateUserInput struct {
	Username    string
	DisplayName string
	Email       string
	FirstName   string
	LastName    string
	PublicKey   string
}

type CreateOrganizationInput struct {
	Name      string
	FullName  string
	OrgType   string
	OwnerName string
}

type CreateClientInput struct {
	Name      string
	Validator bool
	Admin     bool
	PublicKey string
}

type CreateKeyInput struct {
	Name           string
	PublicKey      string
	CreateKey      bool
	ExpirationDate string
}

type UpdateKeyInput struct {
	Name           *string
	PublicKey      *string
	CreateKey      *bool
	ExpirationDate *string
}

type UpdateKeyResult struct {
	KeyMaterial KeyMaterial
	Renamed     bool
}

type organizationState struct {
	org        Organization
	clients    map[string]Client
	clientKeys map[string]map[string]KeyRecord
	envs       map[string]Environment
	nodes      map[string]Node
	roles      map[string]Role
	groups     map[string]Group
	containers map[string]Container
	acls       map[string]authz.ACL
}

func NewService(keyStore *authn.MemoryKeyStore, opts Options) *Service {
	if keyStore == nil {
		keyStore = authn.NewMemoryKeyStore()
	}

	superuser := strings.TrimSpace(opts.SuperuserName)
	if superuser == "" {
		superuser = "pivotal"
	}

	s := &Service{
		keyStore:      keyStore,
		superuserName: superuser,
		users:         make(map[string]User),
		userACLs:      make(map[string]authz.ACL),
		userKeys:      make(map[string]map[string]KeyRecord),
		orgs:          make(map[string]*organizationState),
	}
	s.ensureUserLocked(superuser)
	return s
}

func (s *Service) SuperuserName() string {
	return s.superuserName
}

func (s *Service) SeedPrincipal(principal authn.Principal) {
	if principal.Type != "user" {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.ensureUserLocked(principal.Name)
}

func (s *Service) SeedPublicKey(principal authn.Principal, name, publicKeyPEM string) error {
	principal, err := normalizeKeyPrincipal(principal)
	if err != nil {
		return err
	}

	name = strings.TrimSpace(name)
	if name == "" {
		name = "default"
	}
	publicKeyPEM = strings.TrimSpace(publicKeyPEM)
	if publicKeyPEM == "" {
		return fmt.Errorf("%w: public key is required", ErrInvalidInput)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if principal.Type == "user" {
		s.ensureUserLocked(principal.Name)
	} else {
		org, ok := s.orgs[principal.Organization]
		if !ok {
			return ErrNotFound
		}
		client, ok := org.clients[principal.Name]
		if !ok {
			return ErrNotFound
		}
		client.PublicKey = publicKeyPEM
		org.clients[principal.Name] = client
	}

	s.recordKeyLocked(principal, KeyRecord{
		Name:           name,
		URI:            keyURI(principal, name),
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: "infinity",
		Expired:        false,
		ExpiresAt:      nil,
	})
	return nil
}

func (s *Service) ListUsers() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.users))
	for name := range s.users {
		out[name] = "/users/" + name
	}
	return out
}

func (s *Service) GetUser(name string) (User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	user, ok := s.users[name]
	return user, ok
}

func (s *Service) ListUserKeys(name string) ([]KeyRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.users[name]; !ok {
		return nil, false
	}

	return sortedKeyRecords(s.userKeys[name]), true
}

func (s *Service) GetUserKey(name, keyName string) (KeyRecord, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.users[name]; !ok {
		return KeyRecord{}, false, false
	}

	record, ok := s.userKeys[name][keyName]
	return effectiveKeyRecord(record), true, ok
}

func (s *Service) CreateUserKey(name string, input CreateKeyInput) (*KeyMaterial, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[name]; !ok {
		return nil, ErrNotFound
	}

	return s.createNamedKeyLocked(authn.Principal{
		Type: "user",
		Name: name,
	}, s.userKeys[name], input)
}

func (s *Service) DeleteUserKey(name, keyName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[name]; !ok {
		return ErrNotFound
	}
	if _, ok := s.userKeys[name][keyName]; !ok {
		return ErrNotFound
	}

	delete(s.userKeys[name], keyName)
	if len(s.userKeys[name]) == 0 {
		delete(s.userKeys, name)
	}

	return s.keyStore.Delete(authn.Principal{
		Type: "user",
		Name: name,
	}, keyName)
}

func (s *Service) UpdateUserKey(name, keyName string, input UpdateKeyInput) (UpdateKeyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.users[name]; !ok {
		return UpdateKeyResult{}, ErrNotFound
	}

	return s.updateNamedKeyLocked(authn.Principal{
		Type: "user",
		Name: name,
	}, keyName, s.userKeys[name], input)
}

func (s *Service) CreateUser(input CreateUserInput) (User, *KeyMaterial, error) {
	username := strings.TrimSpace(input.Username)
	if username == "" {
		return User{}, nil, fmt.Errorf("%w: username is required", ErrInvalidInput)
	}
	if !validNamePattern.MatchString(username) {
		return User{}, nil, fmt.Errorf("%w: username contains invalid characters", ErrInvalidInput)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.users[username]; exists {
		return User{}, nil, ErrConflict
	}

	keyMaterial, err := s.keyMaterialForPrincipalLocked(authn.Principal{
		Type: "user",
		Name: username,
	}, input.PublicKey)
	if err != nil {
		return User{}, nil, err
	}

	user := User{
		Username:    username,
		DisplayName: fallback(input.DisplayName, username),
		Email:       strings.TrimSpace(input.Email),
		FirstName:   fallback(input.FirstName, username),
		LastName:    fallback(input.LastName, username),
	}
	s.users[username] = user
	s.userACLs[username] = defaultUserACL(s.superuserName, username)

	return user, keyMaterial, nil
}

func (s *Service) ListOrganizations() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]string, len(s.orgs))
	for name := range s.orgs {
		out[name] = "/organizations/" + name
	}
	return out
}

func (s *Service) GetOrganization(name string) (Organization, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[name]
	if !ok {
		return Organization{}, false
	}
	return org.org, true
}

func (s *Service) CreateOrganization(input CreateOrganizationInput) (Organization, Client, *KeyMaterial, error) {
	name := strings.TrimSpace(input.Name)
	fullName := strings.TrimSpace(input.FullName)
	if name == "" {
		return Organization{}, Client{}, nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if fullName == "" {
		return Organization{}, Client{}, nil, fmt.Errorf("%w: full_name is required", ErrInvalidInput)
	}
	if !validNamePattern.MatchString(name) {
		return Organization{}, Client{}, nil, fmt.Errorf("%w: name contains invalid characters", ErrInvalidInput)
	}

	ownerName := strings.TrimSpace(input.OwnerName)
	if ownerName == "" {
		ownerName = s.superuserName
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.orgs[name]; exists {
		return Organization{}, Client{}, nil, ErrConflict
	}

	s.ensureUserLocked(ownerName)
	s.ensureUserLocked(s.superuserName)

	org := Organization{
		Name:     name,
		FullName: fullName,
		OrgType:  fallback(input.OrgType, "Business"),
		GUID:     newGUID(),
	}

	state := &organizationState{
		org:        org,
		clients:    make(map[string]Client),
		clientKeys: make(map[string]map[string]KeyRecord),
		envs:       make(map[string]Environment),
		nodes:      make(map[string]Node),
		roles:      make(map[string]Role),
		groups:     make(map[string]Group),
		containers: make(map[string]Container),
		acls:       make(map[string]authz.ACL),
	}

	s.orgs[org.Name] = state

	for _, container := range defaultContainers {
		state.containers[container] = Container{
			Name:          container,
			ContainerName: container,
			ContainerPath: container,
		}
		state.acls[containerACLKey(container)] = defaultContainerACL(s.superuserName, container)
	}

	for name, group := range defaultGroups(s.superuserName, ownerName, org.Name) {
		state.groups[name] = group
		state.acls[groupACLKey(name)] = defaultGroupACL(s.superuserName, name)
	}

	state.acls[organizationACLKey()] = defaultOrganizationACL(s.superuserName)
	state.envs["_default"] = defaultEnvironment()
	state.acls[environmentACLKey("_default")] = defaultEnvironmentACL(s.superuserName, authn.Principal{
		Type: "user",
		Name: ownerName,
	})

	validatorName := org.Name + "-validator"
	keyMaterial, err := s.keyMaterialForPrincipalLocked(authn.Principal{
		Type:         "client",
		Name:         validatorName,
		Organization: org.Name,
	}, "")
	if err != nil {
		delete(s.orgs, org.Name)
		return Organization{}, Client{}, nil, err
	}

	validator := Client{
		Name:         validatorName,
		ClientName:   validatorName,
		Organization: org.Name,
		Validator:    true,
		Admin:        false,
		PublicKey:    keyMaterial.PublicKeyPEM,
		URI:          "/organizations/" + org.Name + "/clients/" + validatorName,
	}
	state.clients[validatorName] = validator
	state.acls[clientACLKey(validatorName)] = defaultClientACL(s.superuserName)

	clientsGroup := state.groups["clients"]
	clientsGroup.Clients = uniqueSorted(append(clientsGroup.Clients, validatorName))
	clientsGroup.Actors = uniqueSorted(append(clientsGroup.Users, clientsGroup.Clients...))
	state.groups["clients"] = clientsGroup

	return org, validator, keyMaterial, nil
}

func (s *Service) ListClients(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.clients))
	for name := range org.clients {
		out[name] = "/organizations/" + orgName + "/clients/" + name
	}
	return out, true
}

func (s *Service) GetClient(orgName, clientName string) (Client, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Client{}, false
	}

	client, ok := org.clients[clientName]
	return client, ok
}

func (s *Service) ListClientKeys(orgName, clientName string) ([]KeyRecord, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}
	if _, ok := org.clients[clientName]; !ok {
		return nil, true, false
	}

	return sortedKeyRecords(org.clientKeys[clientName]), true, true
}

func (s *Service) GetClientKey(orgName, clientName, keyName string) (KeyRecord, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return KeyRecord{}, false, false, false
	}
	if _, ok := org.clients[clientName]; !ok {
		return KeyRecord{}, true, false, false
	}

	record, ok := org.clientKeys[clientName][keyName]
	return effectiveKeyRecord(record), true, true, ok
}

func (s *Service) CreateClientKey(orgName, clientName string, input CreateKeyInput) (*KeyMaterial, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, ErrNotFound
	}
	if _, ok := org.clients[clientName]; !ok {
		return nil, ErrNotFound
	}

	keyMaterial, err := s.createNamedKeyLocked(authn.Principal{
		Type:         "client",
		Name:         clientName,
		Organization: orgName,
	}, org.clientKeys[clientName], input)
	if err != nil {
		return nil, err
	}

	if keyMaterial.Name == "default" {
		client := org.clients[clientName]
		client.PublicKey = keyMaterial.PublicKeyPEM
		org.clients[clientName] = client
	}

	return keyMaterial, nil
}

func (s *Service) DeleteClientKey(orgName, clientName, keyName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return ErrNotFound
	}
	if _, ok := org.clients[clientName]; !ok {
		return ErrNotFound
	}
	if _, ok := org.clientKeys[clientName][keyName]; !ok {
		return ErrNotFound
	}

	delete(org.clientKeys[clientName], keyName)
	if len(org.clientKeys[clientName]) == 0 {
		delete(org.clientKeys, clientName)
	}

	if keyName == "default" {
		client := org.clients[clientName]
		client.PublicKey = ""
		org.clients[clientName] = client
	}

	return s.keyStore.Delete(authn.Principal{
		Type:         "client",
		Name:         clientName,
		Organization: orgName,
	}, keyName)
}

func (s *Service) UpdateClientKey(orgName, clientName, keyName string, input UpdateKeyInput) (UpdateKeyResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return UpdateKeyResult{}, ErrNotFound
	}
	if _, ok := org.clients[clientName]; !ok {
		return UpdateKeyResult{}, ErrNotFound
	}

	result, err := s.updateNamedKeyLocked(authn.Principal{
		Type:         "client",
		Name:         clientName,
		Organization: orgName,
	}, keyName, org.clientKeys[clientName], input)
	if err != nil {
		return UpdateKeyResult{}, err
	}

	client := org.clients[clientName]
	switch {
	case keyName == "default" && result.KeyMaterial.Name != "default":
		client.PublicKey = ""
	case result.KeyMaterial.Name == "default":
		client.PublicKey = result.KeyMaterial.PublicKeyPEM
	}
	org.clients[clientName] = client

	return result, nil
}

func (s *Service) CreateClient(orgName string, input CreateClientInput) (Client, *KeyMaterial, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return Client{}, nil, fmt.Errorf("%w: name is required", ErrInvalidInput)
	}
	if !validNamePattern.MatchString(name) {
		return Client{}, nil, fmt.Errorf("%w: name contains invalid characters", ErrInvalidInput)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Client{}, nil, ErrNotFound
	}
	if _, exists := org.clients[name]; exists {
		return Client{}, nil, ErrConflict
	}

	keyMaterial, err := s.keyMaterialForPrincipalLocked(authn.Principal{
		Type:         "client",
		Name:         name,
		Organization: orgName,
	}, input.PublicKey)
	if err != nil {
		return Client{}, nil, err
	}

	client := Client{
		Name:         name,
		ClientName:   name,
		Organization: orgName,
		Validator:    input.Validator,
		Admin:        input.Admin,
		PublicKey:    keyMaterial.PublicKeyPEM,
		URI:          "/organizations/" + orgName + "/clients/" + name,
	}
	org.clients[name] = client
	org.acls[clientACLKey(name)] = defaultClientACL(s.superuserName)

	group := org.groups["clients"]
	group.Clients = uniqueSorted(append(group.Clients, name))
	group.Actors = uniqueSorted(append(group.Users, group.Clients...))
	org.groups["clients"] = group

	return client, keyMaterial, nil
}

func (s *Service) ListGroups(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.groups))
	for name := range org.groups {
		out[name] = "/organizations/" + orgName + "/groups/" + name
	}
	return out, true
}

func (s *Service) GetGroup(orgName, groupName string) (Group, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Group{}, false
	}

	group, ok := org.groups[groupName]
	return group, ok
}

func (s *Service) ListContainers(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.containers))
	for name := range org.containers {
		out[name] = "/organizations/" + orgName + "/containers/" + name
	}
	return out, true
}

func (s *Service) GetContainer(orgName, containerName string) (Container, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return Container{}, false
	}

	container, ok := org.containers[containerName]
	return container, ok
}

func (s *Service) ResolveACL(_ context.Context, resource authz.Resource) (authz.ACL, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch resource.Type {
	case "users":
		return collectionACL(s.superuserName), true, nil
	case "user":
		acl, ok := s.userACLs[resource.Name]
		return acl, ok, nil
	case "organizations":
		return collectionACL(s.superuserName), true, nil
	case "organization":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[organizationACLKey()]
		return acl, ok, nil
	case "container":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[containerACLKey(resource.Name)]
		return acl, ok, nil
	case "group":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[groupACLKey(resource.Name)]
		return acl, ok, nil
	case "client":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[clientACLKey(resource.Name)]
		return acl, ok, nil
	case "environment":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[environmentACLKey(resource.Name)]
		return acl, ok, nil
	case "node":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[nodeACLKey(resource.Name)]
		return acl, ok, nil
	case "role":
		org, ok := s.orgs[resource.Organization]
		if !ok {
			return authz.ACL{}, false, nil
		}
		acl, ok := org.acls[roleACLKey(resource.Name)]
		return acl, ok, nil
	default:
		return authz.ACL{}, false, nil
	}
}

func (s *Service) GroupsFor(_ context.Context, subject authz.Subject) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if subject.Organization == "" {
		return nil, nil
	}

	org, ok := s.orgs[subject.Organization]
	if !ok {
		return nil, nil
	}

	var groups []string
	for _, group := range org.groups {
		if subject.Type == "client" && contains(group.Clients, subject.Name) {
			groups = append(groups, group.Name)
			continue
		}
		if subject.Type == "user" && contains(group.Users, subject.Name) {
			groups = append(groups, group.Name)
		}
	}

	return uniqueSorted(groups), nil
}

func (s *Service) ensureUserLocked(username string) {
	if username == "" {
		return
	}
	if _, exists := s.users[username]; exists {
		return
	}
	s.users[username] = User{
		Username:    username,
		DisplayName: username,
		FirstName:   username,
		LastName:    username,
	}
	s.userACLs[username] = defaultUserACL(s.superuserName, username)
}

func (s *Service) keyMaterialForPrincipalLocked(principal authn.Principal, providedPublicKey string) (*KeyMaterial, error) {
	principal, err := normalizeKeyPrincipal(principal)
	if err != nil {
		return nil, err
	}

	publicKeyPEM := strings.TrimSpace(providedPublicKey)
	var (
		privateKeyPEM string
		publicKey     *rsa.PublicKey
	)

	if publicKeyPEM != "" {
		publicKey, err = authn.ParseRSAPublicKeyPEM([]byte(publicKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("%w: parse public key: %v", ErrInvalidInput, err)
		}
	} else {
		privateKeyPEM, publicKeyPEM, publicKey, err = generateRSAKeyPair()
		if err != nil {
			return nil, err
		}
	}

	if err := s.keyStore.Put(authn.Key{
		ID:        "default",
		Principal: principal,
		PublicKey: publicKey,
	}); err != nil {
		return nil, err
	}

	uri := keyURI(principal, "default")

	record := KeyRecord{
		Name:           "default",
		URI:            uri,
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: "infinity",
		Expired:        false,
		ExpiresAt:      nil,
	}
	s.recordKeyLocked(principal, record)

	return &KeyMaterial{
		Name:           "default",
		URI:            uri,
		PrivateKeyPEM:  privateKeyPEM,
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: "infinity",
	}, nil
}

func (s *Service) createNamedKeyLocked(principal authn.Principal, existing map[string]KeyRecord, input CreateKeyInput) (*KeyMaterial, error) {
	principal, err := normalizeKeyPrincipal(principal)
	if err != nil {
		return nil, err
	}

	name := strings.TrimSpace(input.Name)
	if name == "" {
		return nil, fmt.Errorf("%w: key name is required", ErrInvalidInput)
	}
	if !validNamePattern.MatchString(name) {
		return nil, fmt.Errorf("%w: key name contains invalid characters", ErrInvalidInput)
	}
	if _, exists := existing[name]; exists {
		return nil, ErrConflict
	}

	expirationDate, expiresAt, expired, err := parseExpirationDate(input.ExpirationDate)
	if err != nil {
		return nil, err
	}

	publicKeyPEM := strings.TrimSpace(input.PublicKey)
	if input.CreateKey && publicKeyPEM != "" {
		return nil, fmt.Errorf("%w: public_key and create_key cannot both be set", ErrInvalidInput)
	}
	if !input.CreateKey && publicKeyPEM == "" {
		return nil, fmt.Errorf("%w: public_key is required unless create_key is true", ErrInvalidInput)
	}

	var (
		privateKeyPEM string
		publicKey     *rsa.PublicKey
	)

	if input.CreateKey {
		privateKeyPEM, publicKeyPEM, publicKey, err = generateRSAKeyPair()
		if err != nil {
			return nil, err
		}
	} else {
		publicKey, err = authn.ParseRSAPublicKeyPEM([]byte(publicKeyPEM))
		if err != nil {
			return nil, fmt.Errorf("%w: parse public key: %v", ErrInvalidInput, err)
		}
	}

	if err := s.keyStore.Put(authn.Key{
		ID:        name,
		Principal: principal,
		PublicKey: publicKey,
		ExpiresAt: expiresAt,
	}); err != nil {
		return nil, err
	}

	uri := keyURI(principal, name)
	s.recordKeyLocked(principal, KeyRecord{
		Name:           name,
		URI:            uri,
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: expirationDate,
		Expired:        expired,
		ExpiresAt:      expiresAt,
	})

	return &KeyMaterial{
		Name:           name,
		URI:            uri,
		PrivateKeyPEM:  privateKeyPEM,
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: expirationDate,
	}, nil
}

func (s *Service) updateNamedKeyLocked(principal authn.Principal, currentName string, existing map[string]KeyRecord, input UpdateKeyInput) (UpdateKeyResult, error) {
	principal, err := normalizeKeyPrincipal(principal)
	if err != nil {
		return UpdateKeyResult{}, err
	}

	current, ok := existing[currentName]
	if !ok {
		return UpdateKeyResult{}, ErrNotFound
	}
	if input.Name == nil && input.PublicKey == nil && input.CreateKey == nil && input.ExpirationDate == nil {
		return UpdateKeyResult{}, fmt.Errorf("%w: update payload must include at least one field", ErrInvalidInput)
	}

	targetName := currentName
	if input.Name != nil {
		targetName = strings.TrimSpace(*input.Name)
		if targetName == "" {
			return UpdateKeyResult{}, fmt.Errorf("%w: key name is required", ErrInvalidInput)
		}
		if !validNamePattern.MatchString(targetName) {
			return UpdateKeyResult{}, fmt.Errorf("%w: key name contains invalid characters", ErrInvalidInput)
		}
		if targetName != currentName {
			if _, exists := existing[targetName]; exists {
				return UpdateKeyResult{}, ErrConflict
			}
		}
	}

	createKey := input.CreateKey != nil && *input.CreateKey
	if createKey && input.PublicKey != nil {
		return UpdateKeyResult{}, fmt.Errorf("%w: public_key and create_key cannot both be set", ErrInvalidInput)
	}
	if input.CreateKey != nil && !*input.CreateKey && input.PublicKey == nil && input.Name == nil && input.ExpirationDate == nil {
		return UpdateKeyResult{}, fmt.Errorf("%w: update payload must include at least one field", ErrInvalidInput)
	}

	expirationDate := current.ExpirationDate
	expiresAt := current.ExpiresAt
	if input.ExpirationDate != nil {
		expirationDate, expiresAt, _, err = parseExpirationDate(*input.ExpirationDate)
		if err != nil {
			return UpdateKeyResult{}, err
		}
	}

	publicKeyPEM := current.PublicKeyPEM
	var (
		privateKeyPEM string
		publicKey     *rsa.PublicKey
	)

	switch {
	case createKey:
		privateKeyPEM, publicKeyPEM, publicKey, err = generateRSAKeyPair()
		if err != nil {
			return UpdateKeyResult{}, err
		}
	case input.PublicKey != nil:
		publicKeyPEM = strings.TrimSpace(*input.PublicKey)
		if publicKeyPEM == "" {
			return UpdateKeyResult{}, fmt.Errorf("%w: public key is required", ErrInvalidInput)
		}
		publicKey, err = authn.ParseRSAPublicKeyPEM([]byte(publicKeyPEM))
		if err != nil {
			return UpdateKeyResult{}, fmt.Errorf("%w: parse public key: %v", ErrInvalidInput, err)
		}
	default:
		publicKey, err = authn.ParseRSAPublicKeyPEM([]byte(publicKeyPEM))
		if err != nil {
			return UpdateKeyResult{}, fmt.Errorf("parse stored public key: %w", err)
		}
	}

	if targetName != currentName {
		if err := s.keyStore.Put(authn.Key{
			ID:        targetName,
			Principal: principal,
			PublicKey: publicKey,
			ExpiresAt: expiresAt,
		}); err != nil {
			return UpdateKeyResult{}, err
		}
		if err := s.keyStore.Delete(principal, currentName); err != nil {
			return UpdateKeyResult{}, err
		}
		delete(existing, currentName)
	} else {
		if err := s.keyStore.Put(authn.Key{
			ID:        currentName,
			Principal: principal,
			PublicKey: publicKey,
			ExpiresAt: expiresAt,
		}); err != nil {
			return UpdateKeyResult{}, err
		}
	}

	record := KeyRecord{
		Name:           targetName,
		URI:            keyURI(principal, targetName),
		PublicKeyPEM:   publicKeyPEM,
		ExpirationDate: expirationDate,
		Expired:        isExpiredTime(expiresAt),
		ExpiresAt:      expiresAt,
	}
	s.recordKeyLocked(principal, record)

	return UpdateKeyResult{
		KeyMaterial: KeyMaterial{
			Name:           targetName,
			URI:            record.URI,
			PrivateKeyPEM:  privateKeyPEM,
			PublicKeyPEM:   publicKeyPEM,
			ExpirationDate: expirationDate,
		},
		Renamed: targetName != currentName,
	}, nil
}

func (s *Service) recordKeyLocked(principal authn.Principal, record KeyRecord) {
	if principal.Organization == "" {
		if _, ok := s.userKeys[principal.Name]; !ok {
			s.userKeys[principal.Name] = make(map[string]KeyRecord)
		}
		s.userKeys[principal.Name][record.Name] = record
		return
	}

	org, ok := s.orgs[principal.Organization]
	if !ok {
		return
	}
	if _, ok := org.clientKeys[principal.Name]; !ok {
		org.clientKeys[principal.Name] = make(map[string]KeyRecord)
	}
	org.clientKeys[principal.Name][record.Name] = record
}

func parseExpirationDate(raw string) (string, *time.Time, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil, false, fmt.Errorf("%w: expiration_date is required", ErrInvalidInput)
	}
	if raw == "infinity" {
		return "infinity", nil, false, nil
	}

	expiresAt, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return "", nil, false, fmt.Errorf("%w: expiration_date must be RFC3339 or infinity", ErrInvalidInput)
	}
	expiresAt = expiresAt.UTC()
	expired := !expiresAt.After(time.Now().UTC())
	return expiresAt.Format(time.RFC3339), &expiresAt, expired, nil
}

func effectiveKeyRecord(record KeyRecord) KeyRecord {
	record.Expired = isExpiredTime(record.ExpiresAt)
	return record
}

func isExpiredTime(expiresAt *time.Time) bool {
	return expiresAt != nil && !expiresAt.After(time.Now().UTC())
}

func defaultGroups(superuserName, ownerName, orgName string) map[string]Group {
	adminUsers := uniqueSorted([]string{superuserName, ownerName})
	usersUsers := uniqueSorted([]string{superuserName, ownerName})
	return map[string]Group{
		"admins": {
			Name:         "admins",
			GroupName:    "admins",
			Organization: orgName,
			Actors:       adminUsers,
			Users:        adminUsers,
			Clients:      []string{},
			Groups:       []string{},
		},
		"billing-admins": {
			Name:         "billing-admins",
			GroupName:    "billing-admins",
			Organization: orgName,
			Actors:       uniqueSorted([]string{ownerName}),
			Users:        uniqueSorted([]string{ownerName}),
			Clients:      []string{},
			Groups:       []string{},
		},
		"users": {
			Name:         "users",
			GroupName:    "users",
			Organization: orgName,
			Actors:       usersUsers,
			Users:        usersUsers,
			Clients:      []string{},
			Groups:       []string{usagName(orgName, ownerName)},
		},
		"clients": {
			Name:         "clients",
			GroupName:    "clients",
			Organization: orgName,
			Actors:       []string{},
			Users:        []string{},
			Clients:      []string{},
			Groups:       []string{},
		},
	}
}

func collectionACL(superuserName string) authz.ACL {
	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}},
		Read:   authz.Permission{Actors: []string{superuserName}},
		Update: authz.Permission{Actors: []string{superuserName}},
		Delete: authz.Permission{Actors: []string{superuserName}},
		Grant:  authz.Permission{Actors: []string{superuserName}},
	}
}

func defaultUserACL(superuserName, username string) authz.ACL {
	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}},
		Read:   authz.Permission{Actors: uniqueSorted([]string{superuserName, username})},
		Update: authz.Permission{Actors: uniqueSorted([]string{superuserName, username})},
		Delete: authz.Permission{Actors: []string{superuserName}},
		Grant:  authz.Permission{Actors: []string{superuserName}},
	}
}

func defaultOrganizationACL(superuserName string) authz.ACL {
	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
		Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
	}
}

func defaultContainerACL(superuserName, container string) authz.ACL {
	switch container {
	case "clients":
		return authz.ACL{
			Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
			Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
			Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		}
	case "data", "roles", "environments", "cookbooks":
		return authz.ACL{
			Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users", "clients"}},
			Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		}
	case "nodes":
		return authz.ACL{
			Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users", "clients"}},
			Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users", "clients"}},
			Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		}
	default:
		return authz.ACL{
			Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
			Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
			Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
			Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
			Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		}
	}
}

func defaultGroupACL(superuserName, group string) authz.ACL {
	if group == "billing-admins" {
		return authz.ACL{
			Create: authz.Permission{Actors: []string{superuserName}},
			Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"billing-admins"}},
			Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"billing-admins"}},
			Delete: authz.Permission{Actors: []string{superuserName}},
			Grant:  authz.Permission{Actors: []string{superuserName}},
		}
	}

	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
	}
}

func defaultClientACL(superuserName string) authz.ACL {
	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Read:   authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
		Update: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
		Delete: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
		Grant:  authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins"}},
	}
}

func generateRSAKeyPair() (string, string, *rsa.PublicKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", "", nil, err
	}

	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	publicKeyDER, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return "", "", nil, err
	}
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyDER,
	})

	return string(privateKeyPEM), string(publicKeyPEM), &privateKey.PublicKey, nil
}

func normalizeKeyPrincipal(principal authn.Principal) (authn.Principal, error) {
	principal.Type = strings.TrimSpace(principal.Type)
	principal.Name = strings.TrimSpace(principal.Name)
	principal.Organization = strings.TrimSpace(principal.Organization)

	if principal.Name == "" {
		return authn.Principal{}, fmt.Errorf("%w: principal name is required", ErrInvalidInput)
	}

	switch principal.Type {
	case "user":
		if principal.Organization != "" {
			return authn.Principal{}, fmt.Errorf("%w: user principals must not be organization-scoped", ErrInvalidInput)
		}
	case "client":
		if principal.Organization == "" {
			return authn.Principal{}, fmt.Errorf("%w: client principals require an organization", ErrInvalidInput)
		}
	default:
		return authn.Principal{}, fmt.Errorf("%w: unsupported principal type %q", ErrInvalidInput, principal.Type)
	}

	return principal, nil
}

func keyURI(principal authn.Principal, keyName string) string {
	if principal.Organization != "" {
		return "/organizations/" + principal.Organization + "/clients/" + principal.Name + "/keys/" + keyName
	}
	return "/users/" + principal.Name + "/keys/" + keyName
}

func organizationACLKey() string {
	return "organization"
}

func containerACLKey(name string) string {
	return "container:" + name
}

func groupACLKey(name string) string {
	return "group:" + name
}

func clientACLKey(name string) string {
	return "client:" + name
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func sortedKeyRecords(in map[string]KeyRecord) []KeyRecord {
	if len(in) == 0 {
		return nil
	}

	keys := make([]string, 0, len(in))
	for name := range in {
		keys = append(keys, name)
	}
	sort.Strings(keys)

	out := make([]KeyRecord, 0, len(keys))
	for _, name := range keys {
		out = append(out, effectiveKeyRecord(in[name]))
	}
	return out
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func fallback(value, fallback string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	return value
}

func newGUID() string {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		hash := sha256.Sum256([]byte("opencook-guid-fallback"))
		copy(data[:], hash[:16])
	}
	return hex.EncodeToString(data[:])
}

func usagName(orgName, ownerName string) string {
	sum := sha256.Sum256([]byte(orgName + ":" + ownerName))
	return hex.EncodeToString(sum[:16])
}
