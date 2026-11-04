package functional

import (
	"database/sql"
	"encoding/json"
	_ "github.com/jackc/pgx/v5/stdlib"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/jsonvalue"
)

// Invoked by the Docker migration harness against the imported/restored service,
// including distinct process starts. Ordinary go test runs have no live target.
func TestMigrationFidelity(t *testing.T) {
	if os.Getenv("OPENCOOK_MIGRATION_FIDELITY") != "1" {
		t.Skip("requires Docker migration fixture")
	}
	cfg := loadFunctionalConfig(t)
	client := newFunctionalClient(t, cfg)
	response := client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/nodes/web01", nil, http.StatusOK)
	var node struct {
		Normal map[string]any `json:"normal"`
	}
	if err := jsonvalue.Unmarshal(response.Body, &node); err != nil {
		t.Fatal(err)
	}
	expected := map[string]json.Number{"large": "9007199254740993", "negative": "-9007199254740997", "decimal": "0.1234567890123456789012345", "exponent": "1.234567890123456789e40"}
	for key, want := range expected {
		got, ok := node.Normal[key].(json.Number)
		if !ok || jsonvalue.CanonicalNumber(got) != jsonvalue.CanonicalNumber(want) {
			t.Fatalf("numeric attribute %s changed: got %v want %s", key, node.Normal[key], want)
		}
	}
	if node.Normal["private_key"] != "application-value" || node.Normal["updated_at"] != "application-time" || node.Normal["url"] != "https://application.test/path?value=original" || !reflect.DeepEqual(node.Normal["users"], []any{"b", "a"}) {
		t.Fatal("application attributes changed")
	}
	db, err := sql.Open("pgx", os.Getenv("OPENCOOK_MIGRATION_TARGET_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{"SELECT acl_json FROM oc_bootstrap_user_acls WHERE username='zz-denied-user'", "SELECT acl_json FROM oc_core_object_acls WHERE acl_key='policy_group:zz-denied-group'"} {
		var raw []byte
		if err := db.QueryRow(query).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var acl map[string]struct{ Actors, Groups []string }
		if err := json.Unmarshal(raw, &acl); err != nil {
			t.Fatal(err)
		}
		if len(acl) != 5 {
			t.Fatal("deny-all ACL document missing permissions")
		}
		for _, permission := range []string{"create", "read", "update", "delete", "grant"} {
			value, ok := acl[permission]
			if !ok || len(value.Actors)+len(value.Groups) != 0 {
				t.Fatalf("deny-all ACL changed at %s", permission)
			}
		}
	}
	observer := newFunctionalClientFromPrivateKey(cfg.baseURL, "migration-observer", client.privateKey)
	observer.expectJSON(t, http.MethodGet, "/users/zz-denied-user", nil, http.StatusForbidden)
	observer.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/policy_groups/zz-denied-group", nil, http.StatusForbidden)
	// A logical restore must retain the entire policy payload, not just its ID.
	response = client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/policies/base/revisions/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil, http.StatusOK)
	var policy map[string]any
	if err := jsonvalue.Unmarshal(response.Body, &policy); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(policy["run_list"], []any{"recipe[base]"}) {
		t.Fatal("policy payload lost in backup/restore")
	}
}
