package bootstrap

import (
	"regexp"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
)

var (
	validDataBagNamePattern   = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
	validDataBagItemIDPattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]+$`)
)

type DataBag struct {
	Name      string `json:"name"`
	JSONClass string `json:"json_class"`
	ChefType  string `json:"chef_type"`
}

type DataBagItem struct {
	ID      string         `json:"id"`
	RawData map[string]any `json:"raw_data"`
}

type CreateDataBagInput struct {
	Payload map[string]any
	Creator authn.Principal
}

type CreateDataBagItemInput struct {
	Payload map[string]any
}

type UpdateDataBagItemInput struct {
	Payload map[string]any
}

func (s *Service) ListDataBags(orgName string) (map[string]string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false
	}

	out := make(map[string]string, len(org.dataBags))
	for name := range org.dataBags {
		out[name] = dataBagURI(orgName, name)
	}
	return out, true
}

func (s *Service) GetDataBag(orgName, bagName string) (DataBag, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBag{}, false, false
	}

	bag, ok := org.dataBags[bagName]
	if !ok {
		return DataBag{}, true, false
	}

	return bag, true, true
}

func (s *Service) CreateDataBag(orgName string, input CreateDataBagInput) (DataBag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBag{}, ErrNotFound
	}

	bag, err := normalizeDataBagPayload(input.Payload)
	if err != nil {
		return DataBag{}, err
	}
	if _, exists := org.dataBags[bag.Name]; exists {
		return DataBag{}, ErrConflict
	}

	org.dataBags[bag.Name] = bag
	ensureDataBagItems(org.dataBagItems, bag.Name)
	org.acls[dataBagACLKey(bag.Name)] = defaultDataBagACL(s.superuserName, input.Creator)
	return bag, nil
}

func (s *Service) DeleteDataBag(orgName, bagName string) (DataBag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBag{}, ErrNotFound
	}

	bag, ok := org.dataBags[bagName]
	if !ok {
		return DataBag{}, ErrNotFound
	}

	delete(org.dataBags, bagName)
	delete(org.dataBagItems, bagName)
	delete(org.acls, dataBagACLKey(bagName))
	return bag, nil
}

func (s *Service) ListDataBagItems(orgName, bagName string) (map[string]string, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return nil, false, false
	}
	if _, ok := org.dataBags[bagName]; !ok {
		return nil, true, false
	}

	items := org.dataBagItems[bagName]
	out := make(map[string]string, len(items))
	for name := range items {
		out[name] = dataBagItemURI(orgName, bagName, name)
	}
	return out, true, true
}

func (s *Service) GetDataBagItem(orgName, bagName, itemID string) (DataBagItem, bool, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBagItem{}, false, false, false
	}
	if _, ok := org.dataBags[bagName]; !ok {
		return DataBagItem{}, true, false, false
	}

	item, ok := org.dataBagItems[bagName][itemID]
	if !ok {
		return DataBagItem{}, true, true, false
	}

	return copyDataBagItem(item), true, true, true
}

func (s *Service) CreateDataBagItem(orgName, bagName string, input CreateDataBagItemInput) (DataBagItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBagItem{}, ErrNotFound
	}
	if _, ok := org.dataBags[bagName]; !ok {
		return DataBagItem{}, ErrNotFound
	}

	item, err := normalizeDataBagItemPayload(input.Payload, "", true)
	if err != nil {
		return DataBagItem{}, err
	}
	items := ensureDataBagItems(org.dataBagItems, bagName)
	if _, exists := items[item.ID]; exists {
		return DataBagItem{}, ErrConflict
	}

	items[item.ID] = item
	return copyDataBagItem(item), nil
}

func (s *Service) UpdateDataBagItem(orgName, bagName, itemID string, input UpdateDataBagItemInput) (DataBagItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBagItem{}, ErrNotFound
	}
	if _, ok := org.dataBags[bagName]; !ok {
		return DataBagItem{}, ErrNotFound
	}
	items := ensureDataBagItems(org.dataBagItems, bagName)
	if _, ok := items[itemID]; !ok {
		return DataBagItem{}, ErrNotFound
	}

	item, err := normalizeDataBagItemPayload(input.Payload, itemID, false)
	if err != nil {
		return DataBagItem{}, err
	}

	items[itemID] = item
	return copyDataBagItem(item), nil
}

func (s *Service) DeleteDataBagItem(orgName, bagName, itemID string) (DataBagItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	org, ok := s.orgs[orgName]
	if !ok {
		return DataBagItem{}, ErrNotFound
	}
	if _, ok := org.dataBags[bagName]; !ok {
		return DataBagItem{}, ErrNotFound
	}

	items := ensureDataBagItems(org.dataBagItems, bagName)
	item, ok := items[itemID]
	if !ok {
		return DataBagItem{}, ErrNotFound
	}

	delete(items, itemID)
	return copyDataBagItem(item), nil
}

func normalizeDataBagPayload(payload map[string]any) (DataBag, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	rawName, ok := payload["name"]
	if !ok {
		return DataBag{}, &ValidationError{Messages: []string{"Field 'name' missing"}}
	}

	name, err := validateDataBagName(rawName)
	if err != nil {
		return DataBag{}, err
	}

	return DataBag{
		Name:      name,
		JSONClass: "Chef::DataBag",
		ChefType:  "data_bag",
	}, nil
}

func normalizeDataBagItemPayload(payload map[string]any, targetID string, create bool) (DataBagItem, error) {
	if payload == nil {
		payload = map[string]any{}
	}

	raw := cloneDataBagMap(payload)

	if rawID, ok := payload["id"]; ok {
		id, err := validateDataBagItemID(rawID)
		if err != nil {
			return DataBagItem{}, err
		}
		if !create && targetID != "" && id != targetID {
			return DataBagItem{}, &ValidationError{Messages: []string{"DataBagItem name mismatch."}}
		}
		raw["id"] = id
		return DataBagItem{ID: id, RawData: raw}, nil
	}

	if create {
		return DataBagItem{}, &ValidationError{Messages: []string{"Field 'id' missing"}}
	}
	if targetID == "" {
		return DataBagItem{}, &ValidationError{Messages: []string{"Field 'id' missing"}}
	}

	raw["id"] = targetID
	return DataBagItem{ID: targetID, RawData: raw}, nil
}

func validateDataBagName(value any) (string, error) {
	name, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	name = strings.TrimSpace(name)
	if name == "" || !validDataBagNamePattern.MatchString(name) {
		return "", &ValidationError{Messages: []string{"Field 'name' invalid"}}
	}
	return name, nil
}

func validateDataBagItemID(value any) (string, error) {
	id, ok := value.(string)
	if !ok {
		return "", &ValidationError{Messages: []string{"Field 'id' invalid"}}
	}
	id = strings.TrimSpace(id)
	if id == "" || !validDataBagItemIDPattern.MatchString(id) {
		return "", &ValidationError{Messages: []string{"Field 'id' invalid"}}
	}
	return id, nil
}

func cloneDataBagMap(in map[string]any) map[string]any {
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
		out[key] = cloneDataBagValue(in[key])
	}
	return out
}

func cloneDataBagValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneDataBagMap(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneDataBagValue(typed[idx])
		}
		return out
	case []string:
		return append([]string(nil), typed...)
	default:
		return typed
	}
}

func copyDataBagItem(item DataBagItem) DataBagItem {
	return DataBagItem{
		ID:      item.ID,
		RawData: cloneDataBagMap(item.RawData),
	}
}

func defaultDataBagACL(superuserName string, creator authn.Principal) authz.ACL {
	creatorActors := []string{superuserName}
	if creator.Name != "" {
		creatorActors = uniqueSorted(append(creatorActors, creator.Name))
	}

	return authz.ACL{
		Create: authz.Permission{Actors: []string{superuserName}, Groups: []string{"admins", "users"}},
		Read:   authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users", "clients"}},
		Update: authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users"}},
		Delete: authz.Permission{Actors: creatorActors, Groups: []string{"admins", "users"}},
		Grant:  authz.Permission{Actors: creatorActors, Groups: []string{"admins"}},
	}
}

func dataBagACLKey(name string) string {
	return "data_bag:" + name
}

func dataBagURI(orgName, bagName string) string {
	return "/organizations/" + orgName + "/data/" + bagName
}

func dataBagItemURI(orgName, bagName, itemID string) string {
	return "/organizations/" + orgName + "/data/" + bagName + "/" + itemID
}

func ensureDataBagItems(items map[string]map[string]DataBagItem, bagName string) map[string]DataBagItem {
	if existing, ok := items[bagName]; ok {
		return existing
	}
	created := make(map[string]DataBagItem)
	items[bagName] = created
	return created
}
