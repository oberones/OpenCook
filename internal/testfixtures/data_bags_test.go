package testfixtures

import (
	"reflect"
	"testing"
)

func TestEncryptedDataBagFixturesFreezeCanonicalShape(t *testing.T) {
	// Keep this fixture stable so every layer tests the same encrypted-looking
	// payload shape as the bucket moves from contract freezing to route coverage.
	item := EncryptedDataBagItem()

	if item["id"] != EncryptedDataBagItemID() {
		t.Fatalf("encrypted fixture id = %v, want %q", item["id"], EncryptedDataBagItemID())
	}
	if item["kind"] != "database" {
		t.Fatalf("encrypted fixture kind = %v, want database", item["kind"])
	}
	password, ok := item["password"].(map[string]any)
	if !ok {
		t.Fatalf("password fixture = %T, want map[string]any", item["password"])
	}
	for _, key := range []string{"encrypted_data", "iv", "version", "cipher"} {
		if _, ok := password[key]; !ok {
			t.Fatalf("password fixture missing %q: %v", key, password)
		}
	}
	apiKey, ok := item["api_key"].(map[string]any)
	if !ok {
		t.Fatalf("api_key fixture = %T, want map[string]any", item["api_key"])
	}
	if _, ok := apiKey["auth_tag"]; !ok {
		t.Fatalf("api_key fixture missing auth_tag: %v", apiKey)
	}
}

func TestOrdinaryDataBagFixtureFreezesBaselineShape(t *testing.T) {
	// The ordinary fixture documents the current data bag contract before any
	// encrypted-looking envelopes are involved.
	item := OrdinaryDataBagItem()

	if item["id"] != "plain" {
		t.Fatalf("ordinary fixture id = %v, want plain", item["id"])
	}
	if _, ok := item["password"]; ok {
		t.Fatalf("ordinary fixture unexpectedly contains encrypted field: %v", item)
	}
	if _, ok := item["chef_type"]; ok {
		t.Fatalf("ordinary fixture unexpectedly contains route-added chef_type: %v", item)
	}
	if _, ok := item["data_bag"]; ok {
		t.Fatalf("ordinary fixture unexpectedly contains route-added data_bag: %v", item)
	}
}

func TestCloneDataBagPayloadIsIndependent(t *testing.T) {
	// Later route/search tests mutate fixture payloads to model updates, so the
	// shared helper must return independent nested maps.
	original := EncryptedDataBagItem()
	cloned := CloneDataBagPayload(original)

	cloned["kind"] = "changed"
	clonedPassword := cloned["password"].(map[string]any)
	clonedPassword["encrypted_data"] = "changed-ciphertext"

	if original["kind"] != "database" {
		t.Fatalf("original kind changed to %v", original["kind"])
	}
	originalPassword := original["password"].(map[string]any)
	if originalPassword["encrypted_data"] == "changed-ciphertext" {
		t.Fatalf("original password map was mutated: %v", originalPassword)
	}
	if reflect.DeepEqual(original, cloned) {
		t.Fatalf("clone unexpectedly still equals mutated copy")
	}
}
