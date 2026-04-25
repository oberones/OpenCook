package search

import (
	"fmt"
	"sort"
	"strings"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
)

// DocumentBuilder centralizes Chef-style search document expansion so the
// memory and OpenSearch-backed indexes cannot drift in field semantics.
type DocumentBuilder struct{}

func NewDocumentBuilder() DocumentBuilder {
	return DocumentBuilder{}
}

func (b DocumentBuilder) Client(org string, client bootstrap.Client) Document {
	object := clientObject(client)
	return Document{
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
	}
}

func (b DocumentBuilder) Environment(org string, env bootstrap.Environment) Document {
	object := environmentObject(env)
	return Document{
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
	}
}

func (b DocumentBuilder) Node(org string, node bootstrap.Node) Document {
	return Document{
		Index:   "node",
		Name:    node.Name,
		Object:  nodeObject(node),
		Partial: nodePartialObject(node),
		Fields:  nodeFields(node),
		Resource: authz.Resource{
			Type:         "node",
			Name:         node.Name,
			Organization: org,
		},
	}
}

func (b DocumentBuilder) Role(org string, role bootstrap.Role) Document {
	object := roleObject(role)
	return Document{
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
	}
}

func (b DocumentBuilder) DataBagItem(org, bagName string, item bootstrap.DataBagItem) Document {
	return Document{
		Index:   bagName,
		Name:    item.ID,
		Object:  dataBagItemObject(bagName, item),
		Partial: cloneSearchMap(item.RawData),
		Fields:  dataBagItemFields(item),
		Resource: authz.Resource{
			Type:         "data_bag",
			Name:         bagName,
			Organization: org,
		},
	}
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
