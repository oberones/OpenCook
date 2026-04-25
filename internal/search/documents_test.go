package search

import (
	"testing"

	"github.com/oberones/OpenCook/internal/bootstrap"
)

func TestDocumentBuilderExpandsSearchDocuments(t *testing.T) {
	builder := NewDocumentBuilder()

	tests := []struct {
		name   string
		doc    Document
		assert func(*testing.T, Document)
	}{
		{
			name: "client",
			doc: builder.Client("ponyville", bootstrap.Client{
				Name:         "fluttershy",
				ClientName:   "fluttershy",
				Organization: "ponyville",
				PublicKey:    "-----BEGIN PUBLIC KEY-----",
			}),
			assert: func(t *testing.T, doc Document) {
				requireDocumentIdentity(t, doc, "client", "fluttershy", "client", "fluttershy")
				requireFieldContains(t, doc.Fields, "name", "fluttershy")
				requireFieldContains(t, doc.Fields, "orgname", "ponyville")
				requireFieldContains(t, doc.Fields, "public_key", "-----BEGIN PUBLIC KEY-----")
				if doc.Object["chef_type"] != "client" {
					t.Fatalf("client chef_type = %v, want client", doc.Object["chef_type"])
				}
			},
		},
		{
			name: "environment",
			doc: builder.Environment("ponyville", bootstrap.Environment{
				Name:             "production",
				Description:      "Production",
				JSONClass:        "Chef::Environment",
				ChefType:         "environment",
				CookbookVersions: map[string]string{"nginx": "~> 1.2"},
				DefaultAttributes: map[string]any{
					"apache": map[string]any{"port": 80},
				},
				OverrideAttributes: map[string]any{
					"feature": map[string]any{"enabled": true},
				},
			}),
			assert: func(t *testing.T, doc Document) {
				requireDocumentIdentity(t, doc, "environment", "production", "environment", "production")
				requireFieldContains(t, doc.Fields, "apache_port", "80")
				requireFieldContains(t, doc.Fields, "port", "80")
				requireFieldContains(t, doc.Fields, "feature_enabled", "true")
				requireFieldContains(t, doc.Fields, "enabled", "true")
				requireFieldContains(t, doc.Fields, "cookbook_versions", "nginx")
				requireFieldContains(t, doc.Fields, "nginx", "~> 1.2")
			},
		},
		{
			name: "node",
			doc: builder.Node("ponyville", bootstrap.Node{
				Name:            "twilight",
				JSONClass:       "Chef::Node",
				ChefType:        "node",
				ChefEnvironment: "production",
				Default: map[string]any{
					"levels": map[string]any{
						"magic":  "default",
						"shared": "default",
					},
				},
				Normal: map[string]any{
					"levels": map[string]any{"shared": "normal"},
				},
				Override: map[string]any{
					"levels": map[string]any{"shared": "override"},
				},
				Automatic: map[string]any{
					"levels": map[string]any{"automatic": "yes"},
				},
				RunList:     []string{"base", "recipe[app::default]", "role[web]"},
				PolicyName:  "delivery",
				PolicyGroup: "production",
			}),
			assert: func(t *testing.T, doc Document) {
				requireDocumentIdentity(t, doc, "node", "twilight", "node", "twilight")
				requireFieldContains(t, doc.Fields, "recipe", "base")
				requireFieldContains(t, doc.Fields, "recipe", "app::default")
				requireFieldContains(t, doc.Fields, "role", "web")
				requireFieldContains(t, doc.Fields, "policy_name", "delivery")
				requireFieldContains(t, doc.Fields, "policy_group", "production")
				requireFieldContains(t, doc.Fields, "levels_shared", "override")
				requireFieldContains(t, doc.Fields, "shared", "override")
				requirePartialNestedValue(t, doc.Partial, "levels", "magic", "default")
				requirePartialNestedValue(t, doc.Partial, "levels", "shared", "override")
				requirePartialNestedValue(t, doc.Partial, "levels", "automatic", "yes")
			},
		},
		{
			name: "role",
			doc: builder.Role("ponyville", bootstrap.Role{
				Name:      "web",
				JSONClass: "Chef::Role",
				ChefType:  "role",
				DefaultAttributes: map[string]any{
					"app": map[string]any{"tier": "frontend"},
				},
				OverrideAttributes: map[string]any{
					"feature": true,
				},
				RunList:     []string{"base", "role[db]"},
				EnvRunLists: map[string][]string{"production": {"app::prod"}},
			}),
			assert: func(t *testing.T, doc Document) {
				requireDocumentIdentity(t, doc, "role", "web", "role", "web")
				requireFieldContains(t, doc.Fields, "recipe", "base")
				requireFieldContains(t, doc.Fields, "role", "db")
				requireFieldContains(t, doc.Fields, "app_tier", "frontend")
				requireFieldContains(t, doc.Fields, "tier", "frontend")
				requireFieldContains(t, doc.Fields, "feature", "true")
			},
		},
		{
			name: "data bag item",
			doc: builder.DataBagItem("ponyville", "ponies", bootstrap.DataBagItem{
				ID: "twilight",
				RawData: map[string]any{
					"id": "twilight",
					"ssh": map[string]any{
						"public_key": "ssh-rsa AAAA twilight",
					},
					"age": 7,
				},
			}),
			assert: func(t *testing.T, doc Document) {
				requireDocumentIdentity(t, doc, "ponies", "twilight", "data_bag", "ponies")
				if doc.Object["name"] != "data_bag_item_ponies_twilight" {
					t.Fatalf("data bag object name = %v, want data_bag_item_ponies_twilight", doc.Object["name"])
				}
				if doc.Partial["id"] != "twilight" {
					t.Fatalf("data bag partial id = %v, want twilight", doc.Partial["id"])
				}
				requireFieldContains(t, doc.Fields, "ssh_public_key", "ssh-rsa AAAA twilight")
				requireFieldContains(t, doc.Fields, "public_key", "ssh-rsa AAAA twilight")
				requireFieldContains(t, doc.Fields, "age", "7")
				if _, ok := doc.Fields["raw_data_ssh_public_key"]; ok {
					t.Fatalf("data bag fields unexpectedly included raw_data-prefixed key: %v", doc.Fields)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.assert(t, tt.doc)
		})
	}
}

func requireDocumentIdentity(t *testing.T, doc Document, index, name, resourceType, resourceName string) {
	t.Helper()

	if doc.Index != index {
		t.Fatalf("doc.Index = %q, want %q", doc.Index, index)
	}
	if doc.Name != name {
		t.Fatalf("doc.Name = %q, want %q", doc.Name, name)
	}
	if doc.Resource.Type != resourceType || doc.Resource.Name != resourceName || doc.Resource.Organization != "ponyville" {
		t.Fatalf("doc.Resource = %+v, want %s/%s in ponyville", doc.Resource, resourceType, resourceName)
	}
}

func requireFieldContains(t *testing.T, fields map[string][]string, key, want string) {
	t.Helper()

	for _, got := range fields[key] {
		if got == want {
			return
		}
	}
	t.Fatalf("fields[%q] = %v, want to include %q", key, fields[key], want)
}

func requirePartialNestedValue(t *testing.T, partial map[string]any, key, nestedKey, want string) {
	t.Helper()

	nested, ok := partial[key].(map[string]any)
	if !ok {
		t.Fatalf("partial[%q] = %T(%v), want nested map", key, partial[key], partial[key])
	}
	if nested[nestedKey] != want {
		t.Fatalf("partial[%q][%q] = %v, want %q", key, nestedKey, nested[nestedKey], want)
	}
}
