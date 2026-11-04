package main

import (
	"reflect"
	"testing"
)

func TestReviewGlobalACLExpansion(t *testing.T) {
	catalog := adminMigrationLiveSourceAuthorizationCatalog{}
	for _, ref := range []adminMigrationLiveSourceAuthorizationRef{
		{RecordType: "group", AuthzID: "global", Name: "::server-admins", SubjectOnly: true, SubjectType: "group"},
		{RecordType: "group", AuthzID: "nested", Name: "::nested", SubjectOnly: true, SubjectType: "group"},
		{RecordType: "actor", AuthzID: "alice", Name: "alice", SubjectType: "user"},
		{RecordType: "actor", AuthzID: "client", Name: "client", SubjectType: "client", Organization: "other"},
	} {
		if err := catalog.add(ref); err != nil {
			t.Fatal(err)
		}
	}
	graph := map[string][]adminMigrationLiveSourceBifrostGroupMembershipRow{
		"global": {{ChildType: "group", ChildAuthzID: "nested"}, {ChildType: "actor", ChildAuthzID: "alice"}},
		"nested": {{ChildType: "group", ChildAuthzID: "global"}, {ChildType: "actor", ChildAuthzID: "alice"}},
	}
	for _, org := range []string{"", "ponyville"} {
		target := adminMigrationLiveSourceAuthorizationRef{Organization: org, Resource: "user:orphan"}
		grant := adminMigrationLiveSourceBifrostACLRow{Permission: "read", AuthorizeeType: "group", AuthorizeeAuthzID: "global"}
		rows, err := adminMigrationExpandGlobalACL(catalog, graph, target, grant)
		want := []adminMigrationLiveSourceACLRow{{OrgName: org, Resource: target.Resource, Permission: "read", AuthorizeeType: "actor", Authorizee: "alice"}}
		if err != nil || !reflect.DeepEqual(rows, want) {
			t.Errorf("expanded rows=%+v err=%v", rows, err)
		}
		for _, member := range []string{"missing", "client"} {
			bad := map[string][]adminMigrationLiveSourceBifrostGroupMembershipRow{"global": {{ChildType: "actor", ChildAuthzID: member}}}
			if _, err := adminMigrationExpandGlobalACL(catalog, bad, target, grant); err == nil {
				t.Errorf("accepted unrepresentable member %s", member)
			}
		}
		empty := map[string][]adminMigrationLiveSourceBifrostGroupMembershipRow{"global": {}}
		rows, err = adminMigrationExpandGlobalACL(catalog, empty, target, grant)
		if err != nil || len(rows) != 0 {
			t.Errorf("empty grant expanded: %+v %v", rows, err)
		}
	}
}
