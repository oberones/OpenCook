package search

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

var builtInIndexes = []string{"client", "environment", "node", "role"}

type MemoryIndex struct {
	state  *bootstrap.Service
	target string
}

func NewMemoryIndex(state *bootstrap.Service, target string) MemoryIndex {
	return MemoryIndex{
		state:  state,
		target: strings.TrimSpace(target),
	}
}

func (i MemoryIndex) Name() string {
	return "memory-compat-search"
}

func (i MemoryIndex) Status() Status {
	message := "search compatibility routes are backed by in-memory state; OpenSearch adapter is still pending"
	if i.target != "" {
		message = "OPENCOOK_OPENSEARCH_URL is configured, but search compatibility still uses the in-memory adapter for this phase"
	}

	return Status{
		Backend:    "memory-compat",
		Configured: i.state != nil,
		Message:    message,
	}
}

func (i MemoryIndex) Indexes(_ context.Context, org string) ([]string, error) {
	if i.state == nil {
		return nil, ErrUnavailable
	}
	if err := i.ensureOrganization(org); err != nil {
		return nil, err
	}

	indexes := append([]string(nil), builtInIndexes...)
	seen := make(map[string]struct{}, len(indexes))
	for _, name := range indexes {
		seen[name] = struct{}{}
	}
	dataBags, ok := i.state.ListDataBags(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}
	for _, name := range sortedKeys(dataBags) {
		if _, exists := seen[name]; exists {
			continue
		}
		seen[name] = struct{}{}
		indexes = append(indexes, name)
	}

	return indexes, nil
}

func (i MemoryIndex) Search(_ context.Context, query Query) (Result, error) {
	if i.state == nil {
		return Result{}, ErrUnavailable
	}
	if err := i.ensureOrganization(query.Organization); err != nil {
		return Result{}, err
	}

	docs, err := i.documentsForQuery(query)
	if err != nil {
		return Result{}, err
	}

	matched := make([]Document, 0, len(docs))
	for _, doc := range docs {
		if !matchesQuery(doc, query.Q) {
			continue
		}
		matched = append(matched, doc)
	}

	return Result{Documents: matched}, nil
}

func (i MemoryIndex) documentsForQuery(query Query) ([]Document, error) {
	switch strings.TrimSpace(query.Index) {
	case "client":
		return i.clientDocuments(query.Organization)
	case "environment":
		return i.environmentDocuments(query.Organization)
	case "node":
		return i.nodeDocuments(query.Organization)
	case "role":
		return i.roleDocuments(query.Organization)
	default:
		return i.dataBagDocuments(query.Organization, query.Index)
	}
}

func (i MemoryIndex) clientDocuments(org string) ([]Document, error) {
	clients, ok := i.state.ListClients(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(clients)
	out := make([]Document, 0, len(names))
	for _, name := range names {
		client, exists := i.state.GetClient(org, name)
		if !exists {
			continue
		}
		object := clientObject(client)
		out = append(out, Document{
			Index:   "client",
			Name:    client.Name,
			Object:  object,
			Partial: cloneSearchMap(object),
			Fields:  clientFields(client),
			Resource: authz.Resource{
				Type:         "client",
				Name:         client.Name,
				Organization: org,
			},
		})
	}
	return out, nil
}

func (i MemoryIndex) environmentDocuments(org string) ([]Document, error) {
	environments, ok := i.state.ListEnvironments(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(environments)
	out := make([]Document, 0, len(names))
	for _, name := range names {
		env, _, exists := i.state.GetEnvironment(org, name)
		if !exists {
			continue
		}
		object := environmentObject(env)
		out = append(out, Document{
			Index:   "environment",
			Name:    env.Name,
			Object:  object,
			Partial: cloneSearchMap(object),
			Fields:  environmentFields(env),
			Resource: authz.Resource{
				Type:         "environment",
				Name:         env.Name,
				Organization: org,
			},
		})
	}
	return out, nil
}

func (i MemoryIndex) nodeDocuments(org string) ([]Document, error) {
	nodes, ok := i.state.ListNodes(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(nodes)
	out := make([]Document, 0, len(names))
	for _, name := range names {
		node, _, exists := i.state.GetNode(org, name)
		if !exists {
			continue
		}
		object := nodeObject(node)
		out = append(out, Document{
			Index:   "node",
			Name:    node.Name,
			Object:  object,
			Partial: nodePartialObject(node),
			Fields:  nodeFields(node),
			Resource: authz.Resource{
				Type:         "node",
				Name:         node.Name,
				Organization: org,
			},
		})
	}
	return out, nil
}

func (i MemoryIndex) roleDocuments(org string) ([]Document, error) {
	roles, ok := i.state.ListRoles(org)
	if !ok {
		return nil, ErrOrganizationNotFound
	}

	names := sortedKeys(roles)
	out := make([]Document, 0, len(names))
	for _, name := range names {
		role, _, exists := i.state.GetRole(org, name)
		if !exists {
			continue
		}
		object := roleObject(role)
		out = append(out, Document{
			Index:   "role",
			Name:    role.Name,
			Object:  object,
			Partial: cloneSearchMap(object),
			Fields:  roleFields(role),
			Resource: authz.Resource{
				Type:         "role",
				Name:         role.Name,
				Organization: org,
			},
		})
	}
	return out, nil
}

func (i MemoryIndex) dataBagDocuments(org, bagName string) ([]Document, error) {
	items, orgExists, bagExists := i.state.ListDataBagItems(org, bagName)
	switch {
	case !orgExists:
		return nil, ErrOrganizationNotFound
	case !bagExists:
		return nil, ErrIndexNotFound
	}

	names := sortedKeys(items)
	out := make([]Document, 0, len(names))
	for _, name := range names {
		item, itemOrgExists, itemBagExists, itemExists := i.state.GetDataBagItem(org, bagName, name)
		switch {
		case !itemOrgExists:
			return nil, ErrOrganizationNotFound
		case !itemBagExists:
			return nil, ErrIndexNotFound
		case !itemExists:
			continue
		}

		object := dataBagItemObject(bagName, item)
		out = append(out, Document{
			Index:   bagName,
			Name:    item.ID,
			Object:  object,
			Partial: cloneSearchMap(item.RawData),
			Fields:  dataBagItemFields(item),
			Resource: authz.Resource{
				Type:         "data_bag",
				Name:         bagName,
				Organization: org,
			},
		})
	}
	return out, nil
}

func clientObject(client bootstrap.Client) map[string]any {
	object := map[string]any{
		"name":       client.Name,
		"clientname": client.ClientName,
		"json_class": "Chef::ApiClient",
		"chef_type":  "client",
		"orgname":    client.Organization,
		"validator":  client.Validator,
	}
	if strings.TrimSpace(client.PublicKey) != "" {
		object["public_key"] = client.PublicKey
	}
	return object
}

func environmentObject(env bootstrap.Environment) map[string]any {
	return map[string]any{
		"name":                env.Name,
		"description":         env.Description,
		"json_class":          env.JSONClass,
		"chef_type":           env.ChefType,
		"cookbook_versions":   cloneStringMapToAny(env.CookbookVersions),
		"default_attributes":  cloneSearchMap(env.DefaultAttributes),
		"override_attributes": cloneSearchMap(env.OverrideAttributes),
	}
}

func nodeObject(node bootstrap.Node) map[string]any {
	object := map[string]any{
		"name":             node.Name,
		"json_class":       node.JSONClass,
		"chef_type":        node.ChefType,
		"chef_environment": node.ChefEnvironment,
		"override":         cloneSearchMap(node.Override),
		"normal":           cloneSearchMap(node.Normal),
		"default":          cloneSearchMap(node.Default),
		"automatic":        cloneSearchMap(node.Automatic),
		"run_list":         runListAny(normalizeRunList(node.RunList)),
	}
	if node.PolicyName != "" {
		object["policy_name"] = node.PolicyName
	}
	if node.PolicyGroup != "" {
		object["policy_group"] = node.PolicyGroup
	}
	return object
}

func nodePartialObject(node bootstrap.Node) map[string]any {
	partial := map[string]any{
		"name":             node.Name,
		"json_class":       node.JSONClass,
		"chef_type":        node.ChefType,
		"chef_environment": node.ChefEnvironment,
		"run_list":         runListAny(normalizeRunList(node.RunList)),
	}
	if node.PolicyName != "" {
		partial["policy_name"] = node.PolicyName
	}
	if node.PolicyGroup != "" {
		partial["policy_group"] = node.PolicyGroup
	}
	merged := mergeNodeAttributes(node)
	return deepMergeSearchMaps(partial, merged)
}

func roleObject(role bootstrap.Role) map[string]any {
	return map[string]any{
		"name":                role.Name,
		"description":         role.Description,
		"json_class":          role.JSONClass,
		"chef_type":           role.ChefType,
		"default_attributes":  cloneSearchMap(role.DefaultAttributes),
		"override_attributes": cloneSearchMap(role.OverrideAttributes),
		"run_list":            runListAny(normalizeRunList(role.RunList)),
		"env_run_lists":       stringSliceMapToAny(normalizeEnvRunLists(role.EnvRunLists)),
	}
}

func dataBagItemObject(bagName string, item bootstrap.DataBagItem) map[string]any {
	return map[string]any{
		"name":       "data_bag_item_" + bagName + "_" + item.ID,
		"json_class": "Chef::DataBagItem",
		"chef_type":  "data_bag_item",
		"data_bag":   bagName,
		"raw_data":   cloneSearchMap(item.RawData),
	}
}

func clientFields(client bootstrap.Client) map[string][]string {
	fields := make(map[string][]string)
	addField(fields, "name", client.Name)
	addField(fields, "clientname", client.ClientName)
	addField(fields, "json_class", "Chef::ApiClient")
	addField(fields, "chef_type", "client")
	addField(fields, "orgname", client.Organization)
	addFlattenedFields(fields, nil, clientObject(client))
	return fields
}

func environmentFields(env bootstrap.Environment) map[string][]string {
	fields := make(map[string][]string)
	addField(fields, "name", env.Name)
	addField(fields, "description", env.Description)
	addField(fields, "json_class", env.JSONClass)
	addField(fields, "chef_type", env.ChefType)
	addFlattenedFields(fields, nil, env.DefaultAttributes)
	addFlattenedFields(fields, nil, env.OverrideAttributes)
	addCookbookVersionFields(fields, env.CookbookVersions)
	return fields
}

func nodeFields(node bootstrap.Node) map[string][]string {
	fields := make(map[string][]string)
	addField(fields, "name", node.Name)
	addField(fields, "json_class", node.JSONClass)
	addField(fields, "chef_type", node.ChefType)
	addField(fields, "chef_environment", node.ChefEnvironment)
	addField(fields, "policy_name", node.PolicyName)
	addField(fields, "policy_group", node.PolicyGroup)

	normalizedRunList := normalizeRunList(node.RunList)
	for _, item := range normalizedRunList {
		addField(fields, "run_list", item)
		switch {
		case strings.HasPrefix(item, "recipe[") && strings.HasSuffix(item, "]"):
			addField(fields, "recipe", strings.TrimSuffix(strings.TrimPrefix(item, "recipe["), "]"))
		case strings.HasPrefix(item, "role[") && strings.HasSuffix(item, "]"):
			addField(fields, "role", strings.TrimSuffix(strings.TrimPrefix(item, "role["), "]"))
		}
	}

	addFlattenedFields(fields, nil, mergeNodeAttributes(node))
	return fields
}

func roleFields(role bootstrap.Role) map[string][]string {
	fields := make(map[string][]string)
	addField(fields, "name", role.Name)
	addField(fields, "description", role.Description)
	addField(fields, "json_class", role.JSONClass)
	addField(fields, "chef_type", role.ChefType)
	for _, item := range normalizeRunList(role.RunList) {
		addField(fields, "run_list", item)
		switch {
		case strings.HasPrefix(item, "recipe[") && strings.HasSuffix(item, "]"):
			addField(fields, "recipe", strings.TrimSuffix(strings.TrimPrefix(item, "recipe["), "]"))
		case strings.HasPrefix(item, "role[") && strings.HasSuffix(item, "]"):
			addField(fields, "role", strings.TrimSuffix(strings.TrimPrefix(item, "role["), "]"))
		}
	}
	addFlattenedFields(fields, nil, role.DefaultAttributes)
	addFlattenedFields(fields, nil, role.OverrideAttributes)
	return fields
}

func dataBagItemFields(item bootstrap.DataBagItem) map[string][]string {
	fields := make(map[string][]string)
	addFlattenedFields(fields, nil, item.RawData)
	return fields
}

func matchesQuery(doc Document, query string) bool {
	query = strings.TrimSpace(query)
	if query == "" || query == "*:*" {
		return true
	}

	for _, clause := range strings.Split(query, " OR ") {
		if matchesAndExpression(doc.Fields, clause) {
			return true
		}
	}
	return false
}

func matchesAndExpression(fields map[string][]string, expression string) bool {
	if strings.TrimSpace(expression) == "" {
		return false
	}

	terms := strings.Split(expression, " AND ")
	positiveSeen := false
	processedTerm := false
	for _, rawTerm := range terms {
		rawTerm = strings.TrimSpace(rawTerm)
		if rawTerm == "" {
			continue
		}

		negated := false
		switch {
		case strings.HasPrefix(rawTerm, "NOT "):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "NOT "))
		case strings.HasPrefix(rawTerm, "-"):
			negated = true
			rawTerm = strings.TrimSpace(strings.TrimPrefix(rawTerm, "-"))
		}
		if rawTerm == "" {
			continue
		}

		processedTerm = true
		matched := matchesTerm(fields, rawTerm)
		if negated {
			if matched {
				return false
			}
			continue
		}

		positiveSeen = true
		if !matched {
			return false
		}
	}

	return positiveSeen || processedTerm
}

func matchesTerm(fields map[string][]string, term string) bool {
	parts := strings.SplitN(term, ":", 2)
	if len(parts) != 2 {
		return matchesAnyField(fields, unescapeQueryToken(strings.TrimSpace(term)))
	}

	field := unescapeQueryToken(strings.TrimSpace(parts[0]))
	value := unescapeQueryToken(strings.TrimSpace(parts[1]))
	candidates := fields[field]
	if len(candidates) == 0 {
		return false
	}
	if value == "*" {
		return true
	}

	wildcard := strings.HasSuffix(value, "*")
	if wildcard {
		value = strings.TrimSuffix(value, "*")
	}

	for _, candidate := range candidates {
		if wildcard {
			if strings.HasPrefix(candidate, value) {
				return true
			}
			continue
		}
		if candidate == value {
			return true
		}
	}
	return false
}

func matchesAnyField(fields map[string][]string, value string) bool {
	if value == "" {
		return false
	}

	wildcard := strings.HasSuffix(value, "*")
	if wildcard {
		value = strings.TrimSuffix(value, "*")
	}

	for _, candidates := range fields {
		for _, candidate := range candidates {
			if wildcard {
				if strings.HasPrefix(candidate, value) {
					return true
				}
				continue
			}
			if candidate == value {
				return true
			}
		}
	}

	return false
}

func unescapeQueryToken(value string) string {
	replacer := strings.NewReplacer(`\:`, ":", `\[`, "[", `\]`, "]", `\@`, "@", `\/`, "/")
	return replacer.Replace(value)
}

func addField(fields map[string][]string, key, value string) {
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" || value == "" {
		return
	}
	fields[key] = append(fields[key], value)
}

func addFlattenedFields(fields map[string][]string, path []string, value any) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			addFlattenedFields(fields, append(path, key), typed[key])
		}
	case []any:
		for _, item := range typed {
			addFlattenedFields(fields, path, item)
		}
	case []string:
		for _, item := range typed {
			addFlattenedFields(fields, path, item)
		}
	case string:
		addFlattenedLeaf(fields, path, typed)
	case bool:
		addFlattenedLeaf(fields, path, fmt.Sprintf("%t", typed))
	case int:
		addFlattenedLeaf(fields, path, fmt.Sprintf("%d", typed))
	case int64:
		addFlattenedLeaf(fields, path, fmt.Sprintf("%d", typed))
	case float64:
		addFlattenedLeaf(fields, path, fmt.Sprintf("%v", typed))
	}
}

func addFlattenedLeaf(fields map[string][]string, path []string, value string) {
	if len(path) == 0 {
		return
	}
	full := strings.Join(path, "_")
	addField(fields, full, value)
	addField(fields, path[len(path)-1], value)
}

func addCookbookVersionFields(fields map[string][]string, versions map[string]string) {
	keys := make([]string, 0, len(versions))
	for key := range versions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		addField(fields, "cookbook_versions", key)
		addField(fields, key, versions[key])
	}
}

func mergeNodeAttributes(node bootstrap.Node) map[string]any {
	merged := map[string]any{}
	for _, layer := range []map[string]any{node.Default, node.Normal, node.Override, node.Automatic} {
		merged = deepMergeSearchMaps(merged, cloneSearchMap(layer))
	}
	return merged
}

func deepMergeSearchMaps(base, overlay map[string]any) map[string]any {
	if len(base) == 0 {
		return cloneSearchMap(overlay)
	}
	out := cloneSearchMap(base)
	for key, value := range overlay {
		if existing, ok := out[key]; ok {
			existingMap, existingOK := existing.(map[string]any)
			valueMap, valueOK := value.(map[string]any)
			if existingOK && valueOK {
				out[key] = deepMergeSearchMaps(existingMap, valueMap)
				continue
			}
		}
		out[key] = cloneSearchValue(value)
	}
	return out
}

func cloneSearchMap(in map[string]any) map[string]any {
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
		out[key] = cloneSearchValue(in[key])
	}
	return out
}

func cloneSearchValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneSearchMap(typed)
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneSearchValue(typed[idx])
		}
		return out
	case []string:
		out := make([]any, 0, len(typed))
		for _, item := range typed {
			out = append(out, item)
		}
		return out
	default:
		return typed
	}
}

func cloneStringMapToAny(in map[string]string) map[string]any {
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
		out[key] = in[key]
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
		out[key] = runListAny(in[key])
	}
	return out
}

func normalizeRunList(in []string) []string {
	if len(in) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, item := range in {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		switch {
		case strings.HasPrefix(item, "recipe[") && strings.HasSuffix(item, "]"):
		case strings.HasPrefix(item, "role[") && strings.HasSuffix(item, "]"):
		default:
			item = "recipe[" + item + "]"
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func normalizeEnvRunLists(in map[string][]string) map[string][]string {
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
		out[key] = normalizeRunList(in[key])
	}
	return out
}

func runListAny(in []string) []any {
	if len(in) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(in))
	for _, item := range in {
		out = append(out, item)
	}
	return out
}

func sortedKeys(in map[string]string) []string {
	keys := make([]string, 0, len(in))
	for key := range in {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (i MemoryIndex) ensureOrganization(org string) error {
	if i.state == nil {
		return ErrUnavailable
	}
	if strings.TrimSpace(org) == "" {
		return ErrOrganizationNotFound
	}
	if _, ok := i.state.GetOrganization(org); !ok {
		return ErrOrganizationNotFound
	}
	return nil
}
