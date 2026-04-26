package bootstrap

import (
	"errors"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/testfixtures"
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

// TestEncryptedDataBagItemPayloadsAreClonedAndOpaque proves encrypted-looking
// envelope fields are stored as ordinary JSON while every service boundary
// returns defensive copies instead of sharing nested map or array state.
func TestEncryptedDataBagItemPayloadsAreClonedAndOpaque(t *testing.T) {
	service := newDataBagTestService(t)
	bagName := testfixtures.EncryptedDataBagName()
	createNamedDataBagFixture(t, service, bagName)

	want := testfixtures.NestedEncryptedDataBagItem()
	input := testfixtures.CloneDataBagPayload(want)
	itemID := want["id"].(string)
	created, err := service.CreateDataBagItem("ponyville", bagName, CreateDataBagItemInput{Payload: input})
	if err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
	assertDataBagItemRawData(t, created, want)

	mutateEncryptedDataBagPayload(input)
	fetched := mustGetDataBagItem(t, service, bagName, itemID)
	assertDataBagItemRawData(t, fetched, want)

	mutateEncryptedDataBagPayload(created.RawData)
	fetchedAfterReturnedCreateMutation := mustGetDataBagItem(t, service, bagName, itemID)
	assertDataBagItemRawData(t, fetchedAfterReturnedCreateMutation, want)

	mutateEncryptedDataBagPayload(fetched.RawData)
	fetchedAgain := mustGetDataBagItem(t, service, bagName, itemID)
	assertDataBagItemRawData(t, fetchedAgain, want)
}

// TestEncryptedDataBagItemUpdateStoresOpaquePayload pins update semantics for
// encrypted-looking values, including URL-derived item IDs when the body omits
// id and clone safety for the caller-owned update payload.
func TestEncryptedDataBagItemUpdateStoresOpaquePayload(t *testing.T) {
	service := newDataBagTestService(t)
	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	createNamedDataBagFixture(t, service, bagName)
	createDataBagItemFixture(t, service, bagName, testfixtures.EncryptedDataBagItem())

	updateInput := testfixtures.UpdatedEncryptedDataBagItem()
	updated, err := service.UpdateDataBagItem("ponyville", bagName, itemID, UpdateDataBagItemInput{Payload: updateInput})
	if err != nil {
		t.Fatalf("UpdateDataBagItem() error = %v", err)
	}
	wantUpdated := testfixtures.CloneDataBagPayload(updateInput)
	wantUpdated["id"] = itemID
	assertDataBagItemRawData(t, updated, wantUpdated)

	mutateEncryptedDataBagPayload(updateInput)
	fetched := mustGetDataBagItem(t, service, bagName, itemID)
	assertDataBagItemRawData(t, fetched, wantUpdated)
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

// createNamedDataBagFixture creates a specific data bag name for tests that
// need the shared encrypted-data-bag fixture instead of the ordinary baseline.
func createNamedDataBagFixture(t *testing.T, service *Service, name string) {
	t.Helper()

	if _, err := service.CreateDataBag("ponyville", CreateDataBagInput{
		Payload: map[string]any{
			"name": name,
		},
		Creator: authn.Principal{Type: "user", Name: "pivotal"},
	}); err != nil {
		t.Fatalf("CreateDataBag(%q) error = %v", name, err)
	}
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

// mustGetDataBagItem returns the current stored item or fails with full
// existence detail so clone-safety tests stay focused on payload assertions.
func mustGetDataBagItem(t *testing.T, service *Service, bagName, itemID string) DataBagItem {
	t.Helper()

	item, orgExists, bagExists, itemExists := service.GetDataBagItem("ponyville", bagName, itemID)
	if !orgExists || !bagExists || !itemExists {
		t.Fatalf("GetDataBagItem(%q/%q) existence = %t/%t/%t, want true/true/true", bagName, itemID, orgExists, bagExists, itemExists)
	}
	return item
}

// assertDataBagItemRawData compares the complete raw JSON object so encrypted
// envelope internals, arrays, booleans, numbers, nulls, and unknown keys stay
// opaque to bootstrap storage.
func assertDataBagItemRawData(t *testing.T, item DataBagItem, want map[string]any) {
	t.Helper()

	if !reflect.DeepEqual(item.RawData, want) {
		t.Fatalf("DataBagItem.RawData = %#v, want %#v", item.RawData, want)
	}
}

// mutateEncryptedDataBagPayload changes nested encrypted-looking fields in
// place; tests use it after writes/reads to catch accidental shared map state.
func mutateEncryptedDataBagPayload(payload map[string]any) {
	payload["kind"] = "mutated"
	if credentials, ok := payload["credentials"].([]any); ok && len(credentials) > 0 {
		if first, ok := credentials[0].(map[string]any); ok {
			first["name"] = "mutated-primary"
			if value, ok := first["value"].(map[string]any); ok {
				value["encrypted_data"] = "mutated-ciphertext"
				value["unknown_mutation_marker"] = true
			}
		}
	}
	if metadata, ok := payload["metadata"].(map[string]any); ok {
		metadata["enabled"] = false
		metadata["note"] = "mutated"
	}
	if password, ok := payload["password"].(map[string]any); ok {
		password["encrypted_data"] = "mutated-password-ciphertext"
	}
}

func createDataBagItemFixture(t *testing.T, service *Service, bagName string, payload map[string]any) {
	t.Helper()

	if _, err := service.CreateDataBagItem("ponyville", bagName, CreateDataBagItemInput{Payload: payload}); err != nil {
		t.Fatalf("CreateDataBagItem() error = %v", err)
	}
}
