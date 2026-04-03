package bootstrap

import (
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateNodeAcceptsStringRunList(t *testing.T) {
	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	node, err := service.CreateNode("ponyville", CreateNodeInput{
		Payload: map[string]any{
			"name":             "twilight",
			"json_class":       "Chef::Node",
			"chef_type":        "node",
			"chef_environment": "_default",
			"override":         map[string]any{},
			"normal":           map[string]any{},
			"default":          map[string]any{},
			"automatic":        map[string]any{},
			"run_list":         []string{"recipe[base]", "role[web]"},
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateNode() error = %v", err)
	}

	if len(node.RunList) != 2 {
		t.Fatalf("len(RunList) = %d, want 2", len(node.RunList))
	}
	if node.RunList[0] != "recipe[base]" || node.RunList[1] != "role[web]" {
		t.Fatalf("RunList = %v, want [recipe[base] role[web]]", node.RunList)
	}
}
