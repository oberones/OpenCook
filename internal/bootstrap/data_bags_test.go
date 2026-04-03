package bootstrap

import (
	"errors"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestCreateDataBagNormalizesCompatibilityFields(t *testing.T) {
	service := newDataBagTestService(t)

	bag, err := service.CreateDataBag("ponyville", CreateDataBagInput{
		Payload: map[string]any{
			"name":       "ponies",
			"json_class": "Chef::Node",
			"chef_type":  "node",
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	})
	if err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}

	if bag.Name != "ponies" {
		t.Fatalf("Name = %q, want %q", bag.Name, "ponies")
	}
	if bag.JSONClass != "Chef::DataBag" {
		t.Fatalf("JSONClass = %q, want %q", bag.JSONClass, "Chef::DataBag")
	}
	if bag.ChefType != "data_bag" {
		t.Fatalf("ChefType = %q, want %q", bag.ChefType, "data_bag")
	}
}

func TestUpdateDataBagItemUsesURLIDWhenPayloadOmitsID(t *testing.T) {
	service := newDataBagTestService(t)
	createDataBagFixture(t, service)
	createDataBagItemFixture(t, service, "ponies", map[string]any{
		"id":   "twilight",
		"kind": "unicorn",
	})

	item, err := service.UpdateDataBagItem("ponyville", "ponies", "twilight", UpdateDataBagItemInput{
		Payload: map[string]any{
			"kind": "alicorn",
		},
	})
	if err != nil {
		t.Fatalf("UpdateDataBagItem() error = %v", err)
	}

	if item.ID != "twilight" {
		t.Fatalf("ID = %q, want %q", item.ID, "twilight")
	}
	if item.RawData["id"] != "twilight" {
		t.Fatalf("RawData[id] = %v, want %q", item.RawData["id"], "twilight")
	}
	if item.RawData["kind"] != "alicorn" {
		t.Fatalf("RawData[kind] = %v, want %q", item.RawData["kind"], "alicorn")
	}
}

func TestUpdateDataBagItemRejectsMismatchedID(t *testing.T) {
	service := newDataBagTestService(t)
	createDataBagFixture(t, service)
	createDataBagItemFixture(t, service, "ponies", map[string]any{
		"id":   "twilight",
		"kind": "unicorn",
	})

	_, err := service.UpdateDataBagItem("ponyville", "ponies", "twilight", UpdateDataBagItemInput{
		Payload: map[string]any{
			"id":   "rainbow",
			"kind": "pegasus",
		},
	})
	if err == nil {
		t.Fatal("UpdateDataBagItem() error = nil, want mismatch validation error")
	}

	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("UpdateDataBagItem() error = %T, want *ValidationError", err)
	}
	if len(validationErr.Messages) != 1 || validationErr.Messages[0] != "DataBagItem name mismatch." {
		t.Fatalf("Validation messages = %v, want [DataBagItem name mismatch.]", validationErr.Messages)
	}
}

func newDataBagTestService(t *testing.T) *Service {
	t.Helper()

	service := NewService(authn.NewMemoryKeyStore(), Options{SuperuserName: "pivotal"})
	if _, _, _, err := service.CreateOrganization(CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "pivotal",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}

	return service
}

func createDataBagFixture(t *testing.T, service *Service) {
	t.Helper()

	if _, err := service.CreateDataBag("ponyville", CreateDataBagInput{
		Payload: map[string]any{
			"name": "ponies",
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag() error = %v", err)
	}
}

func createDataBagItemFixture(t *testing.T, service *Service, bagName string, payload map[string]any) {
	t.Helper()

	if _, err := service.CreateDataBagItem("ponyville", bagName, CreateDataBagItemInput{Payload: payload}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
}
