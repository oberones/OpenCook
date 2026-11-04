package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/jsonvalue"
	"github.com/oberones/OpenCook/internal/store/pg"
)

func migrationTreeBytes(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		if entry.IsDir() {
			out[rel] = "directory"
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			target, err := os.Readlink(path)
			out[rel] = "symlink:" + target
			return err
		}
		data, err := os.ReadFile(path)
		out[rel] = string(data)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestMigrationRemediationProtectedPaths(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "nested"), 0755); err != nil {
		t.Fatal(err)
	}
	manifest := filepath.Join(source, "manifest.json")
	if err := os.WriteFile(manifest, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(root, "alias")
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, source, output string }{
		{"equal", source, source}, {"descendant", source, filepath.Join(source, "new", "output")},
		{"ancestor", source, root}, {"manifest ancestor", manifest, source}, {"manifest equality", manifest, manifest},
		{"symlink equal", source, alias}, {"symlink missing ancestors", source, filepath.Join(alias, "new", "nested", "out")},
		{"source alias", alias, source},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := migrationTreeBytes(t, root)
			err := adminMigrationEnsureNormalizeOutputAllowed(tc.source, tc.output, true)
			if !errors.Is(err, errAdminMigrationNormalizeOutputOverlaps) {
				t.Fatalf("overlap error=%v", err)
			}
			if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
				t.Fatal("path check mutated files")
			}
		})
	}
	if err := adminMigrationProtectPaths(filepath.Join(root, "new", "output"), source); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationRemediationLivePathsBeforeExtraction(t *testing.T) {
	for _, kind := range []string{"bookshelf", "private-key"} {
		for _, direction := range []string{"equal", "ancestor", "descendant", "alias"} {
			t.Run(kind+"/"+direction, func(t *testing.T) {
				root := t.TempDir()
				protected := filepath.Join(root, "protected")
				if kind == "bookshelf" {
					if err := os.Mkdir(protected, 0755); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := os.WriteFile(protected, []byte("key sentinel"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				output := protected
				switch direction {
				case "ancestor":
					output = root
				case "descendant":
					output = filepath.Join(protected, "new", "out")
				case "alias":
					output = filepath.Join(root, "alias")
					if err := os.Symlink(protected, output); err != nil {
						t.Fatal(err)
					}
				}
				before := migrationTreeBytes(t, root)
				cmd, stdout, _ := newTestCommand(t)
				fake := &fakeAdminMigrationLiveSourceExtractor{}
				cmd.newLiveSource = fake.factory(t)
				args := []string{"admin", "migration", "source", "live", "extract", "--source-postgres-dsn", "postgres://source", "--output", output, "--yes", "--json"}
				if kind == "bookshelf" {
					args = append(args, "--source-bookshelf-root", protected)
				} else {
					args = append(args, "--source-private-key", protected, "--source-server-url", "https://source.example", "--source-requestor-name", "pivotal")
				}
				if code := cmd.Run(context.Background(), args); code == exitOK {
					t.Fatalf("overlap accepted: %s", stdout.String())
				}
				if fake.extractCalls != 0 {
					t.Fatal("read source before output safety check")
				}
				if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
					t.Fatal("source changed")
				}
			})
		}
	}
}

func TestMigrationRemediationNormalizeDryRun(t *testing.T) {
	source := writeAdminMigrationNormalizedSourceFixture(t)
	for _, manifestInput := range []bool{false, true} {
		for _, exists := range []bool{false, true} {
			for _, yes := range []bool{false, true} {
				root := t.TempDir()
				output := filepath.Join(root, "nested", "output")
				if exists {
					if err := os.MkdirAll(output, 0755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(output, "sentinel"), []byte("preserve"), 0600); err != nil {
						t.Fatal(err)
					}
				}
				input := source
				if manifestInput {
					input = filepath.Join(source, adminMigrationSourceManifestPath)
				}
				sourceBefore := migrationTreeBytes(t, source)
				outputBefore := migrationTreeBytes(t, root)
				out := buildAdminMigrationSourceNormalize(input, &adminMigrationFlagValues{outputPath: output, dryRun: true, yes: yes})
				if !out.OK || len(out.PlannedMutations) == 0 {
					t.Fatalf("dry-run failed: %+v", out)
				}
				if !reflect.DeepEqual(sourceBefore, migrationTreeBytes(t, source)) || !reflect.DeepEqual(outputBefore, migrationTreeBytes(t, root)) {
					t.Fatal("dry-run wrote files")
				}
			}
		}
	}
}

func TestMigrationRemediationBackupPublication(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink", "dangling"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			output := filepath.Join(root, "backup")
			switch kind {
			case "file":
				os.WriteFile(output, []byte("sentinel"), 0600)
			case "directory":
				os.Mkdir(output, 0755)
			case "symlink":
				os.Symlink(root, output)
			case "dangling":
				os.Symlink(filepath.Join(root, "absent"), output)
			}
			before := migrationTreeBytes(t, root)
			_, err := adminMigrationWriteBackupBundle(output, adminMigrationBackupBundleInput{})
			if !errors.Is(err, errAdminMigrationBackupOutputExists) {
				t.Fatalf("existing destination error=%v", err)
			}
			if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
				t.Fatal("existing backup modified")
			}
		})
	}
	t.Run("late write failure", func(t *testing.T) {
		root := t.TempDir()
		output := filepath.Join(root, "backup")
		_, err := adminMigrationWriteBackupBundle(output, adminMigrationBackupBundleInput{BlobCopies: []adminMigrationBackupBlobData{{Checksum: "invalid", Body: []byte("bad")}}})
		if err == nil {
			t.Fatal("invalid blob accepted")
		}
		entries, _ := os.ReadDir(root)
		if len(entries) != 0 {
			t.Fatalf("ordinary failure left staging: %v", entries)
		}
	})
	t.Run("integrity verification", func(t *testing.T) {
		output := filepath.Join(t.TempDir(), "backup")
		_, err := adminMigrationWriteBackupBundle(output, adminMigrationBackupBundleInput{BlobCopies: []adminMigrationBackupBlobData{{Checksum: strings.Repeat("a", 32), Body: []byte("wrong checksum")}}})
		if err == nil {
			t.Fatal("published corrupt copied blob")
		}
		if _, err := os.Lstat(output); !os.IsNotExist(err) {
			t.Fatal("invalid bundle published")
		}
	})
	for _, kind := range []string{"file", "empty directory", "symlink"} {
		t.Run("destination race/"+kind, func(t *testing.T) {
			root := t.TempDir()
			output := filepath.Join(root, "backup")
			err := adminMigrationPublishBundle(output, "backup", false, nil, func(stage string) error {
				if err := os.WriteFile(filepath.Join(stage, "recovery"), []byte("bundle"), 0600); err != nil {
					return err
				}
				switch kind {
				case "file":
					return os.WriteFile(output, []byte("other writer"), 0600)
				case "empty directory":
					return os.Mkdir(output, 0755)
				default:
					return os.Symlink(root, output)
				}
			})
			var publication *adminMigrationPublicationError
			if !errors.As(err, &publication) || publication.Code != "bundle_publication_failed" {
				t.Fatalf("race error=%v", err)
			}
			if data, err := os.ReadFile(filepath.Join(publication.Stage, "recovery")); err != nil || string(data) != "bundle" {
				t.Fatal("recoverable staging lost")
			}
			if _, err := os.Lstat(output); err != nil {
				t.Fatal("racing destination lost")
			}
			if _, err := os.Stat(publication.Lock); err != nil {
				t.Fatal("recovery lock missing")
			}
		})
	}
	t.Run("cooperating writer", func(t *testing.T) {
		root := t.TempDir()
		output := filepath.Join(root, "backup")
		err := adminMigrationPublishBundle(output, "backup", false, nil, func(stage string) error {
			err := adminMigrationPublishBundle(output, "backup", false, nil, func(string) error { t.Fatal("second writer entered"); return nil })
			var publication *adminMigrationPublicationError
			if !errors.As(err, &publication) || publication.Code != "bundle_destination_locked" {
				t.Fatalf("lock error=%v", err)
			}
			return os.WriteFile(filepath.Join(stage, "manifest"), []byte("owner"), 0600)
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("interrupted recovery", func(t *testing.T) {
		root := t.TempDir()
		output := filepath.Join(root, "backup")
		stale := filepath.Join(root, ".backup-backup-interrupted")
		os.Mkdir(stale, 0700)
		os.WriteFile(filepath.Join(stale, "sentinel"), []byte("recover"), 0600)
		before := migrationTreeBytes(t, root)
		_, err := adminMigrationWriteBackupBundle(output, adminMigrationBackupBundleInput{})
		if err == nil {
			t.Fatal("interrupted staging ignored")
		}
		if !reflect.DeepEqual(before, migrationTreeBytes(t, root)) {
			t.Fatal("another invocation staging changed")
		}
	})
}

func TestMigrationRemediationSyncRelationships(t *testing.T) {
	b, c := adminMigrationHealthyStates(t)
	org := b.Orgs["ponyville"]
	org.Groups["retained"] = bootstrap.Group{Name: "retained", GroupName: "retained", Organization: "ponyville", Users: []string{"pivotal"}, Actors: []string{"pivotal"}}
	org.Groups["deleted"] = org.Groups["retained"]
	b.Orgs["ponyville"] = org
	co := c.Orgs["ponyville"]
	co.Policies = map[string]map[string]bootstrap.PolicyRevision{"base": {"rev": {Name: "base", RevisionID: "rev", Payload: map[string]any{}}}}
	co.PolicyGroups = map[string]bootstrap.PolicyGroup{"retained": {Name: "retained", Policies: map[string]string{"base": "rev"}}, "deleted": {Name: "deleted", Policies: map[string]string{"base": "rev"}}}
	c.Orgs["ponyville"] = co
	target := adminMigrationSourceImportState{Bootstrap: b, CoreObjects: c}
	for _, family := range []string{"groups", "policy_groups"} {
		t.Run(family, func(t *testing.T) {
			relation := "group_memberships"
			if family == "policy_groups" {
				relation = "policy_assignments"
			}
			source := adminMigrationSourceImportState{Bootstrap: bootstrap.CloneBootstrapCoreState(b), CoreObjects: bootstrap.CloneCoreObjectState(c)}
			bo := source.Bootstrap.Orgs["ponyville"]
			bo.Groups = map[string]bootstrap.Group{"retained": {Name: "retained", GroupName: "new definition", Organization: "ponyville"}, "new": {Name: "new", Users: []string{"pivotal"}}}
			source.Bootstrap.Orgs["ponyville"] = bo
			co := source.CoreObjects.Orgs["ponyville"]
			co.PolicyGroups = map[string]bootstrap.PolicyGroup{"retained": {Name: "changed"}, "new": {Name: "new", Policies: map[string]string{"base": "rev"}}}
			source.CoreObjects.Orgs["ponyville"] = co
			key := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: family}
			scope := map[adminMigrationSourcePayloadKey]bool{key: true}
			merged := adminMigrationSourceSyncMergedState(target, source, scope)
			if family == "groups" {
				g := merged.Bootstrap.Orgs["ponyville"].Groups
				if len(g["retained"].Users) != 1 || g["retained"].GroupName != "new definition" || len(g["new"].Users) != 0 || len(g) != 2 {
					t.Fatalf("definition merge=%v", g)
				}
			} else {
				g := merged.CoreObjects.Orgs["ponyville"].PolicyGroups
				if len(g["retained"].Policies) != 1 || len(g["new"].Policies) != 0 || len(g) != 2 {
					t.Fatalf("definition merge=%v", g)
				}
			}
			relationScope := map[adminMigrationSourcePayloadKey]bool{{Organization: "ponyville", Family: relation}: true}
			empty := adminMigrationSourceSyncMergedState(target, adminMigrationSourceImportState{}, relationScope)
			if family == "groups" {
				if len(empty.Bootstrap.Orgs["ponyville"].Groups) != len(org.Groups) || len(empty.Bootstrap.Orgs["ponyville"].Groups["retained"].Users) != 0 {
					t.Fatal("empty relationships removed parents")
				}
			} else {
				if len(empty.CoreObjects.Orgs["ponyville"].PolicyGroups) != 2 || len(empty.CoreObjects.Orgs["ponyville"].PolicyGroups["retained"].Policies) != 0 {
					t.Fatal("empty assignments removed parents")
				}
			}
			scope[adminMigrationSourcePayloadKey{Organization: "ponyville", Family: relation}] = true
			first := adminMigrationSourceSyncMergedState(target, source, scope)
			for i := 0; i < 30; i++ {
				got := adminMigrationSourceSyncMergedState(target, source, scope)
				if !reflect.DeepEqual(first, got) {
					t.Fatal("nondeterministic merge")
				}
			}
			repeated := adminMigrationSourceSyncMergedState(first, source, scope)
			if diff := adminMigrationSourceSyncDiffStates(repeated, first, adminMigrationSourceSyncDiffScopes(scope)); diff.HasChanges {
				t.Fatalf("repeat sync not idempotent: %+v", diff)
			}
		})
	}
	// Deleting a referenced subject fails validation of the final graph.
	broken := target
	broken.Bootstrap = bootstrap.CloneBootstrapCoreState(target.Bootstrap)
	broken.Bootstrap.Users = map[string]bootstrap.User{}
	if adminMigrationValidateSyncGraph(broken, adminMigrationSourceImportState{}, nil) == nil {
		t.Fatal("dangling retained relationship accepted")
	}
}

func TestMigrationRemediationMalformedLivePayloads(t *testing.T) {
	compress := func(raw []byte) []byte {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(raw)
		w.Close()
		return b.Bytes()
	}
	valid := []byte(`{"normal":{"large":9007199254740993}}`)
	truncated := compress(valid)
	truncated = truncated[:len(truncated)-4]
	for _, column := range []string{"raw_payload", "metadata_payload", "serialized_object"} {
		for _, tc := range []struct {
			name  string
			data  []byte
			valid bool
		}{
			{"plain", valid, true}, {"gzip", compress(valid), true}, {"invalid", []byte(`{"private_key":"secret"`), false}, {"trailing", append(append([]byte{}, valid...), []byte(` {}`)...), false}, {"gzip trailing", compress([]byte(`{} {}`)), false}, {"bad gzip", []byte{0x1f, 0x8b, 0}, false}, {"truncated", truncated, false}, {"array", []byte(`[]`), false}, {"null", []byte(`null`), false}, {"number", []byte(`42`), false}, {"empty", nil, column == "serialized_object"}, {"whitespace", []byte(" \n "), false},
		} {
			t.Run(column+"/"+tc.name, func(t *testing.T) {
				fields := []pgconn.FieldDescription{{Name: "name"}, {Name: column}}
				_, err := adminMigrationLiveSourceRowMap(fields, []any{"safe-name", tc.data})
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%v error=%v", tc.valid, err)
				}
				if err != nil {
					var malformed *adminMigrationLiveSourcePayloadDecodeError
					if !errors.As(err, &malformed) || len(malformed.Identity) != 64 || strings.Contains(err.Error(), "secret") {
						t.Fatalf("unsafe or unclassified error %v", err)
					}
				}
			})
		}
	}
}

func TestMigrationRemediationEmptyACLImportAndRehydrate(t *testing.T) {
	values := testAdminMigrationLiveSourceBootstrapPayloadValues(t)
	values[adminMigrationSourcePayloadKey{Family: "user_acls"}] = nil
	aclKey := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "acls"}
	var retained []json.RawMessage
	for _, raw := range values[aclKey] {
		object, _ := adminMigrationDecodeSourceObject(raw)
		if adminMigrationSourceString(object, "resource") != "node:web01" {
			retained = append(retained, raw)
		}
	}
	values[aclKey] = retained

	for _, target := range []adminMigrationLiveSourceACLRow{{Resource: "user:pivotal"}, {OrgName: "ponyville", Resource: "node:web01"}} {
		objects := adminMigrationLiveSourceACLObjects([]adminMigrationLiveSourceACLRow{target})
		if len(objects) != 1 {
			t.Fatal("deny-all target dropped")
		}
		key := adminMigrationSourcePayloadKey{Organization: target.OrgName, Family: "acls"}
		if target.OrgName == "" {
			key.Family = "user_acls"
		}
		delete(objects[0], "orgname")
		adminMigrationLiveSourceAppendObject(values, key, objects[0])
	}
	bundle, err := adminMigrationLiveSourceBootstrapBundleFromPayloadValues(values)
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "source")
	if err := adminMigrationWriteNormalizedSourceBundle(output, bundle, false); err != nil {
		t.Fatal(err)
	}
	read, err := adminMigrationReadSourceImportBundle(output)
	if err != nil {
		t.Fatal(err)
	}
	state, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatal(err)
	}
	repo := pg.New("postgres://unused").CoreObjects()
	rows, err := repo.EncodeCoreObjects(state.CoreObjects)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := repo.DecodeCoreObjects(rows)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		service := bootstrap.NewService(authn.NewMemoryKeyStore(), bootstrap.Options{SuperuserName: "separate-superuser", InitialBootstrapCoreState: &state.Bootstrap, InitialCoreObjectState: &restored})
		for _, resource := range []authz.Resource{{Type: "user", Name: "pivotal"}, {Type: "node", Name: "web01", Organization: "ponyville"}} {
			decision, err := authz.NewACLAuthorizer(service).Authorize(context.Background(), authz.Subject{Type: "user", Name: "pivotal", Organization: resource.Organization}, authz.ActionRead, resource)
			if err != nil || decision.Allowed {
				t.Fatalf("empty ACL allowed read after rehydrate: %+v %v", decision, err)
			}
			acl, ok, err := service.ResolveACL(context.Background(), resource)
			if err != nil || !ok {
				t.Fatalf("missing persisted ACL: %v", err)
			}
			for _, p := range []authz.Permission{acl.Create, acl.Read, acl.Update, acl.Delete, acl.Grant} {
				if len(p.Actors)+len(p.Groups) != 0 {
					t.Fatalf("deny-all ACL replaced: %+v", acl)
				}
			}
		}
	}
}

func TestMigrationRemediationExactNumbers(t *testing.T) {
	attributes := `{"large":9007199254740993,"negative":-9007199254740997,"decimal":0.1234567890123456789012345,"exponent":1.234567890123456789e+40}`
	var want map[string]any
	if err := jsonvalue.Unmarshal([]byte(attributes), &want); err != nil {
		t.Fatal(err)
	}
	values := testAdminMigrationLiveSourceBootstrapPayloadValues(t)
	key := adminMigrationSourcePayloadKey{Organization: "ponyville", Family: "nodes"}
	values[key] = []json.RawMessage{json.RawMessage(`{"name":"web01","normal":` + attributes + `}`)}
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
	state, err := adminMigrationSourceImportStateFromRead(read)
	if err != nil {
		t.Fatal(err)
	}
	repo := pg.New("postgres://unused").CoreObjects()
	rows, err := repo.EncodeCoreObjects(bootstrap.CloneCoreObjectState(state.CoreObjects))
	if err != nil {
		t.Fatal(err)
	}
	state.CoreObjects, err = repo.DecodeCoreObjects(rows)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup")
	if _, err := adminMigrationWriteBackupBundle(backup, adminMigrationBackupBundleInput{Bootstrap: state.Bootstrap, CoreObjects: state.CoreObjects, Cookbooks: state.Cookbooks}); err != nil {
		t.Fatal(err)
	}
	data := readAdminMigrationTestFile(t, backup, adminMigrationBackupObjectsPath)
	var restored bootstrap.CoreObjectState
	if err := jsonvalue.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	service := adminMigrationBootstrapServiceFromState(config.Config{}, state.Bootstrap, restored)
	node, _, ok := service.GetNode("ponyville", "web01")
	if !ok {
		t.Fatal("node missing after rehydrate")
	}
	if !reflect.DeepEqual(node.Normal, want) {
		t.Fatalf("numeric fidelity lost: got=%v want=%v", node.Normal, want)
	}
	a, _ := adminMigrationShadowCanonicalPayload("nodes", node)
	node.Normal["large"] = json.Number("9007199254740992")
	b, _ := adminMigrationShadowCanonicalPayload("nodes", node)
	if a == b {
		t.Fatal("shadow lost adjacent large integer difference")
	}
}

func TestMigrationRemediationCutoverLatestCursor(t *testing.T) {
	read, err := adminMigrationReadSourceImportBundle(writeAdminMigrationNormalizedSourceFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	a := adminMigrationSourceSyncCursor(read)
	path := filepath.Join(t.TempDir(), "progress.json")
	check := func(progress adminMigrationSourceSyncProgress, want bool) {
		t.Helper()
		writeAdminMigrationSourceJSON(t, path, progress)
		out := adminMigrationCLIOutput{OK: true}
		adminMigrationCutoverSyncFreshnessGate(&out, &adminMigrationFlagValues{syncProgressPath: path}, read, true)
		if out.OK != want {
			t.Fatalf("freshness=%v want=%v progress=%+v findings=%v", out.OK, want, progress, out.Findings)
		}
	}
	check(adminMigrationSourceSyncProgress{LastStatus: "applied", SourceCursor: a, AppliedCursors: []string{a}}, true)
	check(adminMigrationSourceSyncProgress{LastStatus: "applied", SourceCursor: "B", AppliedCursors: []string{a, "B"}}, false)
	check(adminMigrationSourceSyncProgress{LastStatus: "applied", SourceCursor: a, AppliedCursors: []string{a, "B"}}, true)
	check(adminMigrationSourceSyncProgress{LastStatus: "applied", AppliedCursors: []string{a}}, false)
	check(adminMigrationSourceSyncProgress{LastStatus: "applied", SourceCursor: a, AppliedCursors: []string{"unrelated"}}, true)
	check(adminMigrationSourceSyncProgress{LastStatus: "failed", SourceCursor: a, AppliedCursors: []string{a}}, false)
}

func TestMigrationRemediationShadowOpaqueData(t *testing.T) {
	for _, family := range []string{"nodes", "environments", "roles", "data_bag_items", "policy_revisions", "cookbook_versions", "cookbook_artifacts"} {
		path := ""
		switch family {
		case "nodes":
			path = "normal"
		case "environments", "roles":
			path = "default_attributes"
		case "cookbook_versions", "cookbook_artifacts":
			path = "metadata"
		}
		for _, key := range []string{"private_key", "updated_at", "users", "url", "large"} {
			t.Run(family+"/"+key, func(t *testing.T) {
				source := map[string]any{key: "source-secret"}
				target := map[string]any{key: "target-secret"}
				switch key {
				case "users":
					source[key] = []any{"a", "b"}
					target[key] = []any{"b", "a"}
				case "url":
					source[key] = "https://app.test/path?a=1"
					target[key] = "https://app.test/path?a=2"
				case "large":
					source[key] = json.Number("9007199254740992")
					target[key] = json.Number("9007199254740993")
				}
				if path != "" {
					source = map[string]any{path: source}
					target = map[string]any{path: target}
				}
				a, _ := adminMigrationShadowCanonicalPayload(family, source)
				b, _ := adminMigrationShadowCanonicalPayload(family, target)
				if a == b {
					t.Fatal("application difference erased")
				}
			})
		}
	}
	for _, tc := range []struct {
		family string
		a, b   any
	}{
		{"users", map[string]any{"username": "a"}, map[string]any{"username": "a", "requestor": map[string]any{"name": "b"}, "uri": "/users/a", "authn_status": "verified", "storage_status": "postgres"}},
		{"groups", map[string]any{"users": []string{"b", "a"}}, map[string]any{"users": []string{"a", "b"}}},
		{"user_acls", map[string]any{"read": map[string]any{"actors": nil, "groups": []string{"b", "a"}}}, map[string]any{"read": map[string]any{"actors": []any{}, "groups": []string{"a", "b"}}}},
		{"cookbook_versions", map[string]any{"recipes": []any{map[string]any{"url": "/_blob/checksums/abc?source=1"}}}, map[string]any{"recipes": []any{map[string]any{"url": "https://target.test/_blob/checksums/abc?signature=secret"}}}},
	} {
		a, _ := adminMigrationShadowCanonicalPayload(tc.family, tc.a)
		b, _ := adminMigrationShadowCanonicalPayload(tc.family, tc.b)
		if a != b {
			t.Fatalf("protocol noise in %s not normalized", tc.family)
		}
	}
}

func TestMigrationRemediationPublicationRechecksProtectedPaths(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	output := filepath.Join(root, "output")
	alias := filepath.Join(root, "source-alias")
	if err := os.Mkdir(source, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "sentinel"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(source, alias); err != nil {
		t.Fatal(err)
	}
	before := migrationTreeBytes(t, output)
	err := adminMigrationPublishBundle(output, "normalize", true, []string{alias}, func(stage string) error {
		if err := os.Remove(alias); err != nil {
			return err
		}
		return os.Symlink(output, alias)
	})
	if !errors.Is(err, errAdminMigrationNormalizeOutputOverlaps) {
		t.Fatalf("replaced protected alias not caught: %v", err)
	}
	if !reflect.DeepEqual(before, migrationTreeBytes(t, output)) {
		t.Fatal("protected output replaced")
	}
}

func TestMigrationRemediationCleanupFailurePreservesRecovery(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("permission failure requires an unprivileged process")
	}
	root := t.TempDir()
	output := filepath.Join(root, "backup")
	owned := ""
	err := adminMigrationPublishBundle(output, "backup", false, nil, func(stage string) error {
		owned = stage
		if err := os.WriteFile(filepath.Join(stage, "recover"), []byte("bundle"), 0600); err != nil {
			return err
		}
		if err := os.Chmod(stage, 0500); err != nil {
			return err
		}
		return errors.New("late write failure")
	})
	t.Cleanup(func() { _ = os.Chmod(owned, 0700) })
	var publication *adminMigrationPublicationError
	if !errors.As(err, &publication) || publication.Code != "bundle_cleanup_failed" {
		t.Fatalf("cleanup failure not reported: %v", err)
	}
	if publication.Stage != owned {
		t.Fatal("recovery path missing")
	}
	if _, err := os.Stat(publication.Lock); err != nil {
		t.Fatal("recovery lock missing")
	}
	if _, err := os.Stat(filepath.Join(owned, "recover")); err != nil {
		t.Fatal("recovery bytes lost")
	}
}

func TestMigrationRemediationRelationshipOnlyBundle(t *testing.T) {
	b, c := adminMigrationHealthyStates(t)
	org := b.Orgs["ponyville"]
	org.Groups["kept"] = bootstrap.Group{Name: "kept", GroupName: "kept", Organization: "ponyville"}
	b.Orgs["ponyville"] = org
	co := c.Orgs["ponyville"]
	co.Policies = map[string]map[string]bootstrap.PolicyRevision{"base": {strings.Repeat("a", 40): {Name: "base", RevisionID: strings.Repeat("a", 40), Payload: map[string]any{"name": "base"}}}}
	co.PolicyGroups = map[string]bootstrap.PolicyGroup{"kept": {Name: "kept"}}
	c.Orgs["ponyville"] = co
	values := map[adminMigrationSourcePayloadKey][]json.RawMessage{
		{Organization: "ponyville", Family: "organizations"}:      {json.RawMessage(`{"name":"ponyville"}`)},
		{Organization: "ponyville", Family: "group_memberships"}:  {json.RawMessage(`{"group":"kept","type":"user","actor":"pivotal"}`)},
		{Organization: "ponyville", Family: "policy_assignments"}: {json.RawMessage(`{"group":"kept","policy":"base","revision_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`)},
	}
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
	source, err := adminMigrationSourceSyncStateFromRead(read)
	if err != nil {
		t.Fatal(err)
	}
	scopes := adminMigrationSourceSyncCoveredScopes(read)
	target := adminMigrationSourceImportState{Bootstrap: b, CoreObjects: c}
	merged := adminMigrationSourceSyncMergedState(target, source, scopes)
	if err := adminMigrationValidateSyncGraph(merged, source, scopes); err != nil {
		t.Fatal(err)
	}
	if len(merged.Bootstrap.Orgs["ponyville"].Groups) != len(org.Groups) || len(merged.Bootstrap.Orgs["ponyville"].Groups["kept"].Users) != 1 || merged.CoreObjects.Orgs["ponyville"].PolicyGroups["kept"].Policies["base"] != strings.Repeat("a", 40) {
		t.Fatal("relationship-only sync lost parents or relationships")
	}
	if _, err := adminMigrationSourceImportStateFromRead(read); err == nil {
		t.Fatal("initial import accepted unresolved relationship-only source")
	}
}

func TestMigrationRemediationShadowSecretDiagnostic(t *testing.T) {
	b, c := adminMigrationHealthyStates(t)
	org := c.Orgs["ponyville"]
	org.DataBags = map[string]bootstrap.DataBag{"secrets": {Name: "secrets"}}
	org.DataBagItems = map[string]map[string]bootstrap.DataBagItem{"secrets": {"item": {ID: "item", RawData: map[string]any{"id": "item", "private_key": "source-private-secret"}}}}
	c.Orgs["ponyville"] = org
	source := adminMigrationSourceImportState{Bootstrap: b, CoreObjects: c}
	path := "/organizations/ponyville/data/secrets/item"
	check := adminMigrationReadCheck("data_bag_items", "secrets/item", path)
	client := &fakeMigrationRehearsalClient{responses: map[string]any{path: map[string]any{"id": "item", "private_key": "target-private-secret"}}}
	result := adminMigrationRunShadowComparison(context.Background(), client, source, []adminMigrationRehearsalCheck{check}, 0)
	if result.Failed != 1 {
		t.Fatalf("private-key change did not fail comparison: %+v", result)
	}
	data, err := json.Marshal(result.Findings)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "source-private-secret") || strings.Contains(string(data), "target-private-secret") {
		t.Fatal("comparison leaked secrets")
	}
}
