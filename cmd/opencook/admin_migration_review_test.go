package main

import (
	"context"
	"encoding/json"
	"github.com/oberones/OpenCook/internal/config"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReviewPublicationPaths(t *testing.T) {
	t.Run("parent traversal", func(t *testing.T) {
		root := t.TempDir()
		protected := filepath.Join(root, "protected")
		if err := os.MkdirAll(filepath.Join(protected, "child"), 0755); err != nil {
			t.Fatal(err)
		}
		key := filepath.Join(protected, "key")
		if err := os.WriteFile(key, []byte("secret sentinel"), 0600); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(root, "alias")
		if err := os.Symlink(filepath.Join(protected, "child"), alias); err != nil {
			t.Fatal(err)
		}
		traversal := alias + "/../key"
		for _, paths := range [][2]string{{traversal, key}, {key, traversal}} {
			before := migrationTreeBytes(t, root)
			called := false
			err := adminMigrationPublishBundle(paths[0], "normalize", true, []string{paths[1]}, func(string) error { called = true; return nil })
			if err == nil || called {
				t.Errorf("unsafe paths accepted: %v, writer=%v", err, called)
			}
			if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
				t.Fatal("protected files changed")
			}
		}
	})
	t.Run("trailing slash", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "backup")
		err := adminMigrationPublishBundle(output+"/", "backup", false, nil, func(stage string) error {
			return os.WriteFile(filepath.Join(stage, "manifest.json"), []byte("{}"), 0600)
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(output, "manifest.json")); err != nil {
			t.Fatal(err)
		}
	})
}

func reviewRelationshipRead(t *testing.T, values map[adminMigrationSourcePayloadKey][]json.RawMessage) adminMigrationSourceImportRead {
	t.Helper()
	bundle, err := adminMigrationLiveSourceBootstrapBundleFromPayloadValues(values)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "source")
	if err := adminMigrationWriteNormalizedSourceBundle(path, bundle, false); err != nil {
		t.Fatal(err)
	}
	read, err := adminMigrationReadSourceImportBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	return read
}

func TestReviewServerAdminsSurviveSync(t *testing.T) {
	read := reviewRelationshipRead(t, map[adminMigrationSourcePayloadKey][]json.RawMessage{
		{Family: "users"}: {json.RawMessage(`{"username":"global"}`), json.RawMessage(`{"username":"local"}`)},
		{Organization: "ponyville", Family: "organizations"}:     {json.RawMessage(`{"name":"ponyville"}`)},
		{Organization: "ponyville", Family: "groups"}:            {json.RawMessage(`{"name":"admins"}`)},
		{Organization: "ponyville", Family: "group_memberships"}: {json.RawMessage(`{"group":"admins","actor":"local","type":"user"}`)},
		{Family: "server_admin_memberships"}:                     {json.RawMessage(`{"actor":"global","type":"user"}`)},
	})
	imported, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatal(err)
	}
	source, err := adminMigrationSourceSyncStateFromRead(read)
	if err != nil {
		t.Fatal(err)
	}
	merged := adminMigrationSourceSyncMergedState(imported, source, adminMigrationSourceSyncCoveredScopes(read))
	group := merged.Bootstrap.Orgs["ponyville"].Groups["admins"]
	if !reflect.DeepEqual(group.Users, []string{"global", "local"}) || !reflect.DeepEqual(group.Actors, group.Users) {
		t.Fatalf("admins lost or duplicated: %+v", group)
	}
}

func TestReviewInitialImportRequiresPolicyGroupDefinitions(t *testing.T) {
	for _, definition := range []string{"absent", "empty", "incomplete", "explicit"} {
		t.Run(definition, func(t *testing.T) {
			values := map[adminMigrationSourcePayloadKey][]json.RawMessage{
				{Organization: "ponyville", Family: "organizations"}:      {json.RawMessage(`{"name":"ponyville"}`)},
				{Organization: "ponyville", Family: "policy_revisions"}:   {json.RawMessage(`{"name":"base","revision_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)},
				{Organization: "ponyville", Family: "policy_assignments"}: {json.RawMessage(`{"group":"prod","policy":"base","revision_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)},
			}
			key := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "policy_groups"}
			switch definition {
			case "empty":
				values[key] = []json.RawMessage{}
			case "incomplete":
				values[key] = []json.RawMessage{json.RawMessage(`{"name":"other"}`)}
			case "explicit":
				values[key] = []json.RawMessage{json.RawMessage(`{"name":"prod"}`)}
			}
			read := adminMigrationSourceImportRead{PayloadValues: values}
			_, err := adminMigrationSourceImportStateFromRead(read)
			if definition != "explicit" {
				cmd, _, _ := newTestCommand(t)
				cmd.newOfflineStore = func(context.Context, string) (adminOfflineStore, func() error, error) {
					t.Fatal("invalid import reached target store")
					return nil, nil, nil
				}
				out := cmd.applyAdminMigrationSourceImport(context.Background(), config.Config{}, "", read, &adminMigrationFlagValues{}, adminMigrationCLIOutput{OK: true})
				if out.OK {
					t.Fatal("invalid import apply succeeded")
				}
			}
			if (err == nil) != (definition == "explicit") {
				t.Fatalf("import error=%v", err)
			}
		})
	}
}

func TestReviewServerAdminScopes(t *testing.T) {
	base := map[adminMigrationSourcePayloadKey][]json.RawMessage{
		{Family: "users"}: {json.RawMessage(`{"username":"global"}`), json.RawMessage(`{"username":"local"}`)},
		{Organization: "ponyville", Family: "organizations"}:     {json.RawMessage(`{"name":"ponyville"}`)},
		{Organization: "ponyville", Family: "groups"}:            {json.RawMessage(`{"name":"admins"}`)},
		{Organization: "ponyville", Family: "group_memberships"}: {json.RawMessage(`{"group":"admins","actor":"local","type":"user"}`)},
		{Family: "server_admin_memberships"}:                     {json.RawMessage(`{"actor":"global","type":"user"}`)},
	}
	imported, err := adminMigrationSourceImportStateFromRead(reviewRelationshipRead(t, base))
	if err != nil {
		t.Fatal(err)
	}
	imported.Bootstrap.Orgs["target-only"] = imported.Bootstrap.Orgs["ponyville"]
	local := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "group_memberships"}
	global := adminMigrationSourcePayloadKey{Family: "server_admin_memberships"}
	for _, tc := range []struct {
		name string
		want []string
	}{
		{"omitted", []string{"global", "local"}}, {"empty", nil}, {"global-only", []string{"global"}}, {"local-only", []string{"local"}}, {"duplicate", []string{"global", "local"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := map[adminMigrationSourcePayloadKey][]json.RawMessage{}
			for k, v := range base {
				values[k] = append([]json.RawMessage{}, v...)
			}
			switch tc.name {
			case "omitted":
				delete(values, local)
				delete(values, global)
			case "empty":
				values[local] = nil
				values[global] = nil
			case "global-only":
				values[local] = nil
			case "local-only":
				values[global] = nil
			case "duplicate":
				values[local] = append(values[local], json.RawMessage(`{"group":"admins","actor":"global","type":"user"}`))
			}
			read := reviewRelationshipRead(t, values)
			source, err := adminMigrationSourceSyncStateFromRead(read)
			if err != nil {
				t.Fatal(err)
			}
			merged := adminMigrationSourceSyncMergedState(imported, source, adminMigrationSourceSyncCoveredScopes(read))
			got := merged.Bootstrap.Orgs["ponyville"].Groups["admins"]
			if len(got.Users) != len(tc.want) {
				t.Fatalf("users=%v want %v", got.Users, tc.want)
			}
			for i, want := range tc.want {
				if got.Users[i] != want {
					t.Fatalf("users=%v want %v", got.Users, tc.want)
				}
			}
			if !reflect.DeepEqual(got.Users, got.Actors) {
				t.Fatalf("actor list stale: %+v", got)
			}
			if !reflect.DeepEqual(imported.Bootstrap.Orgs["target-only"], merged.Bootstrap.Orgs["target-only"]) {
				t.Fatal("target-only organization changed")
			}
		})
	}
}

func TestReviewLiveExtractionRejectsParentTraversalBeforeReadingKey(t *testing.T) {
	for _, yes := range []bool{false, true} {
		root := t.TempDir()
		protected := filepath.Join(root, "protected")
		if err := os.MkdirAll(filepath.Join(protected, "child"), 0755); err != nil {
			t.Fatal(err)
		}
		key := filepath.Join(protected, "key")
		if err := os.WriteFile(key, []byte("key sentinel"), 0600); err != nil {
			t.Fatal(err)
		}
		alias := filepath.Join(root, "alias")
		if err := os.Symlink(filepath.Join(protected, "child"), alias); err != nil {
			t.Fatal(err)
		}
		for _, paths := range [][2]string{{key, alias + "/../key"}, {alias + "/../key", key}} {
			before := migrationTreeBytes(t, root)
			cmd, stdout, _ := newTestCommand(t)
			fake := &fakeAdminMigrationLiveSourceExtractor{}
			cmd.newLiveSource = fake.factory(t)
			args := []string{"admin", "migration", "source", "live", "extract", "--source-postgres-dsn", "postgres://source", "--output", paths[0], "--source-private-key", paths[1], "--source-server-url", "https://source.example", "--source-requestor-name", "pivotal", "--json"}
			if yes {
				args = append(args, "--yes")
			}
			if cmd.Run(context.Background(), args) == exitOK {
				t.Fatalf("unsafe extraction accepted: %s", stdout.String())
			}
			if fake.extractCalls != 0 {
				t.Fatal("source read before traversal rejection")
			}
			if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
				t.Fatal("protected key changed")
			}
		}
	}
}
