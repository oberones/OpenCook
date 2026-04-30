package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
	"github.com/oberones/OpenCook/internal/testfixtures"
)

func TestActivePostgresOpenSearchRoutesHydratePersistedSearchResults(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	seedActivePostgresOpenSearchState(t, persisted)
	secretBag := testfixtures.EncryptedDataBagName()
	secretItemName := "data_bag_item_" + secretBag + "_" + testfixtures.EncryptedDataBagItemID()

	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "client", "*:*"): {
			"ponyville/client/ponyville-validator",
		},
		apiOpenSearchQueryKey("ponyville", "client", "name:ponyville-*"): {
			"ponyville/client/ponyville-validator",
		},
		apiOpenSearchQueryKey("ponyville", "environment", "name:_default OR name:production"): {
			"ponyville/environment/_default",
			"ponyville/environment/production",
		},
		apiOpenSearchQueryKey("ponyville", "environment", "description:*search-target"): {
			"ponyville/environment/production",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:twi*"): {
			"ponyville/node/twilight",
			"ponyville/node/stale",
		},
		apiOpenSearchQueryKey("ponyville", "node", "rainbow"): {
			"ponyville/node/rainbow",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:*"): {
			"ponyville/node/rainbow",
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:*light"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:twi*ght"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "te*:friend*"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "run_list:*web*"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "build:[001 TO 099]"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "team:friendship OR recipe:missing AND name:twilight"): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", `path:foo\/*`): {
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "role", "name:* AND NOT name:db"): {
			"ponyville/role/web",
		},
		apiOpenSearchQueryKey("ponyville", "role", "description:*search-target"): {
			"ponyville/role/web",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `path:foo\/*`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `badge:primary\[blue\]`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", "ba*:primary*"): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `note:"hello world"`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", `punctuation:*!*`): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", "ponies", "ssh_public_key:*"): {
			"ponyville/ponies/alice",
		},
		apiOpenSearchQueryKey("ponyville", secretBag, "*_encrypted_data:*"): {
			"ponyville/" + secretBag + "/" + testfixtures.EncryptedDataBagItemID(),
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, nil)

	assertActiveOpenSearchIndexURLs(t, restarted.router, "/search", map[string]string{
		"client":      "/search/client",
		"environment": "/search/environment",
		"node":        "/search/node",
		"role":        "/search/role",
		"ponies":      "/search/ponies",
		secretBag:     "/search/" + secretBag,
	})
	assertActiveOpenSearchIndexURLs(t, restarted.router, "/organizations/ponyville/search", map[string]string{
		"client":      "/organizations/ponyville/search/client",
		"environment": "/organizations/ponyville/search/environment",
		"node":        "/organizations/ponyville/search/node",
		"role":        "/organizations/ponyville/search/role",
		"ponies":      "/organizations/ponyville/search/ponies",
		secretBag:     "/organizations/ponyville/search/" + secretBag,
	})

	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/client", "*:*"), "/search/client", []string{"ponyville-validator"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/client", "name:ponyville-*"), "/organizations/ponyville/search/client", []string{"ponyville-validator"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/environment", "name:_default OR name:production"), "/organizations/ponyville/search/environment", []string{"_default", "production"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/environment", "description:*search-target"), "/search/environment", []string{"production"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "name:twi*"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/node", "rainbow"), "/organizations/ponyville/search/node", []string{"rainbow"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "name:*light"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "name:twi*ght"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "te*:friend*"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "run_list:*web*"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "build:[001 TO 099]"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "(team:friendship OR recipe:missing) AND name:twilight"), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", `path:foo\/*`), "/search/node", []string{"twilight"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "name:* AND NOT name:db"), "/search/role", []string{"web"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/role", "description:*search-target"), "/organizations/ponyville/search/role", []string{"web"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/ponies", `path:foo\/*`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/ponies", `badge:primary\[blue\]`), "/organizations/ponyville/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/ponies", "ba*:primary*"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/ponies", `note:"hello world"`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/ponies", `punctuation:*\!*`), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/"+secretBag, "*_encrypted_data:*"), "/search/"+secretBag, []string{secretItemName})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/organizations/ponyville/search/"+secretBag, "*_encrypted_data:*"), "/organizations/ponyville/search/"+secretBag, []string{secretItemName})

	pagedRec := performActiveOpenSearchRequest(t, restarted.router, http.MethodGet, searchPath("/search/node", "name:*")+"&rows=1&start=1", "/search/node", nil)
	pagedPayload := decodeSearchPayload(t, pagedRec)
	if pagedPayload["total"] != float64(2) {
		t.Fatalf("paged total = %v, want 2", pagedPayload["total"])
	}
	pagedRows := pagedPayload["rows"].([]any)
	if len(pagedRows) != 1 || pagedRows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("paged rows = %v, want second sorted row twilight", pagedRows)
	}

	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/organizations/ponyville/search/client", "name:ponyville-*"), "/organizations/ponyville/search/client", []byte(`{"org":["orgname"]}`), "/organizations/ponyville/clients/ponyville-validator", map[string]any{"org": "ponyville"})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/search/environment", "description:*search-target"), "/search/environment", []byte(`{"desc":["description"]}`), "/environments/production", map[string]any{"desc": "prod-search-target"})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/search/node", "(team:friendship OR recipe:missing) AND name:twilight"), "/search/node", []byte(`{"team":["team"],"build":["build"]}`), "/nodes/twilight", map[string]any{"team": "friendship", "build": "010"})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/organizations/ponyville/search/role", "description:*search-target"), "/organizations/ponyville/search/role", []byte(`{"desc":["description"]}`), "/organizations/ponyville/roles/web", map[string]any{"desc": "role-search-target"})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/organizations/ponyville/search/ponies", "ba*:primary*"), "/organizations/ponyville/search/ponies", []byte(`{"badge":["badge"],"note":["note"]}`), "/organizations/ponyville/data/ponies/alice", map[string]any{"badge": "primary[blue]", "note": "hello world"})

	encryptedPartialRec := performActiveOpenSearchRequest(t, restarted.router, http.MethodPost, searchPath("/organizations/ponyville/search/"+secretBag, "*_encrypted_data:*"), "/organizations/ponyville/search/"+secretBag, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, encryptedPartialRec), "/organizations/ponyville/data/"+secretBag+"/"+testfixtures.EncryptedDataBagItemID())
}

// TestActivePostgresOpenSearchRoutesIgnoreProviderSources proves the provider
// remains an ID index: route payloads are hydrated from PostgreSQL, not from
// arbitrary provider-side _source documents returned with matching hits.
func TestActivePostgresOpenSearchRoutesIgnoreProviderSources(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	seedActivePostgresOpenSearchState(t, persisted)

	transport := newAPIOpenSearchSearchTransportWithSources(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "node", "name:twi*"): {
			"ponyville/node/twilight",
		},
	}, map[string]map[string]any{
		"ponyville/node/twilight": {
			"name":          []string{"provider-spoof"},
			"chef_type":     "provider_doc",
			"provider_only": "must-not-leak",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, nil)

	rec := performActiveOpenSearchRequest(t, restarted.router, http.MethodGet, searchPath("/search/node", "name:twi*"), "/search/node", nil)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("provider-source search payload = %v, want one hydrated row", payload)
	}
	row := rows[0].(map[string]any)
	if row["name"] != "twilight" || row["chef_type"] != "node" {
		t.Fatalf("provider-source search row = %v, want hydrated PostgreSQL node", row)
	}
	if _, ok := row["provider_only"]; ok {
		t.Fatalf("provider-source search row leaked provider-only field: %v", row)
	}
}

// TestActivePostgresOpenSearchUnsupportedObjectFamilyIndexesStayUnsupportedWithObjectsPresent
// pins the same unsupported-index contract after persisted state is rehydrated
// behind the active OpenSearch route adapter.
func TestActivePostgresOpenSearchUnsupportedObjectFamilyIndexesStayUnsupportedWithObjectsPresent(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	persisted.createOrganizationWithValidator("ponyville")
	seedUnsupportedSearchObjectFamilies(t, persisted.router)

	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, newAPIOpenSearchSearchTransport(t, map[string][]string{}), nil)
	assertSearchIndexListingOnlyIncludesSupportedFamilies(t, restarted.router, "/search")
	assertSearchIndexListingOnlyIncludesSupportedFamilies(t, restarted.router, "/organizations/ponyville/search")

	for _, index := range unsupportedObjectFamilySearchIndexes() {
		t.Run(index, func(t *testing.T) {
			assertUnsupportedSearchIndex(t, restarted.router, http.MethodGet, searchPath("/search/"+index, "*:*"), "/search/"+index, nil, index)
			assertUnsupportedSearchIndex(t, restarted.router, http.MethodPost, searchPath("/search/"+index, "name:*"), "/search/"+index, []byte(`{"name":["name"]}`), index)
			assertUnsupportedSearchIndex(t, restarted.router, http.MethodGet, searchPath("/organizations/ponyville/search/"+index, "*:*"), "/organizations/ponyville/search/"+index, nil, index)
			assertUnsupportedSearchIndex(t, restarted.router, http.MethodPost, searchPath("/organizations/ponyville/search/"+index, "name:*"), "/organizations/ponyville/search/"+index, []byte(`{"name":["name"]}`), index)
		})
	}
}

// TestActivePostgresOpenSearchIndirectFieldsRemainSearchableWithUnsupportedObjectsPresent
// proves provider-backed search still treats cookbook/policy concepts as fields
// on supported indexes after PostgreSQL rehydration, not as searchable joins.
func TestActivePostgresOpenSearchIndirectFieldsRemainSearchableWithUnsupportedObjectsPresent(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	persisted.createOrganizationWithValidator("ponyville")
	seedUnsupportedSearchObjectFamilies(t, persisted.router)
	seedIndirectSearchFields(t, persisted.router)

	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "node", "policy_name:delivery-app AND policy_group:prod-blue"): {
			"ponyville/node/policy-node",
		},
		apiOpenSearchQueryKey("ponyville", "node", "recipe:*default AND role:indirect-web"): {
			"ponyville/node/policy-node",
		},
		apiOpenSearchQueryKey("ponyville", "node", "policy_name:delivery-app OR policy_name:hidden-app"): {
			"ponyville/node/policy-node",
			"ponyville/node/hidden-node",
		},
		apiOpenSearchQueryKey("ponyville", "node", "policy_name:delivery-app"): {
			"ponyville/node/policy-node",
		},
		apiOpenSearchQueryKey("ponyville", "environment", `searchcookbook:"= 1.0.0"`): {
			"ponyville/environment/production",
		},
		apiOpenSearchQueryKey("ponyville", "role", "recipe:*default"): {
			"ponyville/role/indirect-web",
		},
		apiOpenSearchQueryKey("ponyville", "searchbag", "marker:indirect"): {
			"ponyville/searchbag/visible",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:hidden-node": {},
			},
		}
	})

	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "policy_name:delivery-app AND policy_group:prod-blue"), "/search/node", []string{"policy-node"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "recipe:*default AND role:indirect-web"), "/search/node", []string{"policy-node"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/environment", `searchcookbook:"= 1.0.0"`), "/search/environment", []string{"production"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/role", "recipe:*default"), "/search/role", []string{"indirect-web"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/searchbag", "marker:indirect"), "/search/searchbag", []string{"data_bag_item_searchbag_visible"})
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/node", "policy_name:delivery-app OR policy_name:hidden-app"), "/search/node", []string{"policy-node"})

	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/search/node", "policy_name:delivery-app"), "/search/node", []byte(`{"policy_name":["policy_name"],"policy_group":["policy_group"],"run_list":["run_list"]}`), "/nodes/policy-node", map[string]any{
		"policy_name":  "delivery-app",
		"policy_group": "prod-blue",
		"run_list":     []any{"recipe[searchcookbook::default]", "role[indirect-web]"},
	})
	assertActiveOpenSearchPartialData(t, restarted.router, searchPath("/search/searchbag", "marker:indirect"), "/search/searchbag", []byte(`{"marker":["marker"],"kind":["details","kind"]}`), "/data/searchbag/visible", map[string]any{
		"marker": "indirect",
		"kind":   "retained",
	})
}

func TestActivePostgresOpenSearchFiltersDeniedIDsAfterHydration(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	seedActivePostgresOpenSearchState(t, persisted)

	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "node", "name:*"): {
			"ponyville/node/rainbow",
			"ponyville/node/twilight",
		},
		apiOpenSearchQueryKey("ponyville", "node", "name:rainbow OR name:twilight AND name:*"): {
			"ponyville/node/rainbow",
			"ponyville/node/twilight",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:rainbow": {},
			},
		}
	})

	rec := performActiveOpenSearchRequestAs(t, restarted.router, "silent-bob", http.MethodGet, searchPath("/search/node", "name:*"), "/search/node", nil)
	payload := decodeSearchPayload(t, rec)
	if payload["total"] != float64(1) {
		t.Fatalf("filtered total = %v, want 1", payload["total"])
	}
	rows := payload["rows"].([]any)
	if len(rows) != 1 || rows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("filtered rows = %v, want only twilight", rows)
	}

	groupedRec := performActiveOpenSearchRequestAs(t, restarted.router, "silent-bob", http.MethodGet, searchPath("/search/node", "(name:rainbow OR name:twilight) AND name:*"), "/search/node", nil)
	groupedPayload := decodeSearchPayload(t, groupedRec)
	groupedRows := groupedPayload["rows"].([]any)
	if groupedPayload["total"] != float64(1) || len(groupedRows) != 1 || groupedRows[0].(map[string]any)["name"] != "twilight" {
		t.Fatalf("grouped non-admin filtered payload = %v, want only twilight", groupedPayload)
	}
}

func TestActivePostgresOpenSearchRoutesPinPagingTotalsAndOrderingAfterHydration(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	persisted.createOrganizationWithValidator("ponyville")
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := persisted.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "silent-bob",
		DisplayName: "Silent Bob",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(silent-bob) error = %v", err)
	}
	if err := persisted.state.AddUserToGroup("ponyville", "users", "silent-bob"); err != nil {
		t.Fatalf("AddUserToGroup(silent-bob) error = %v", err)
	}
	for _, name := range []string{"zeta", "alpha", "kilo", "bravo", "hotel", "charlie", "india", "delta", "juliet", "echo", "foxtrot", "golf"} {
		sendActivePostgresPivotalJSON(t, persisted.router, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalSearchNodePayload(t, name, map[string]any{
			"sequence": "050",
		}, map[string]any{
			"team": "fleet",
		}, []string{"base"}), http.StatusCreated)
	}

	query := "(team:fleet OR recipe:missing) AND sequence:[001 TO 999]"
	transport := newAPIOpenSearchSearchTransport(t, map[string][]string{
		apiOpenSearchQueryKey("ponyville", "node", "team:fleet OR recipe:missing AND sequence:[001 TO 999]"): {
			"ponyville/node/alpha",
			"ponyville/node/bravo",
			"ponyville/node/charlie",
			"ponyville/node/delta",
			"ponyville/node/echo",
			"ponyville/node/foxtrot",
			"ponyville/node/golf",
			"ponyville/node/hotel",
			"ponyville/node/india",
			"ponyville/node/juliet",
			"ponyville/node/kilo",
			"ponyville/node/stale",
			"canterlot/node/wrong-org",
			"ponyville/role/wrong-index",
			"ponyville/node/zeta",
		},
	})
	restarted := newActivePostgresOpenSearchFixture(t, persisted.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return denyingSearchAuthorizer{
			base: authz.NewACLAuthorizer(state),
			denyRead: map[string]struct{}{
				"node:charlie": {},
				"node:hotel":   {},
			},
		}
	})

	expected := []string{"alpha", "bravo", "delta", "echo", "foxtrot", "golf", "india", "juliet", "kilo", "zeta"}
	assertActiveOpenSearchPageNames(t, restarted.router, "silent-bob", searchPath("/search/node", query), "/search/node", 0, 10, expected)
	assertActiveOpenSearchPageNames(t, restarted.router, "silent-bob", searchPath("/search/node", query)+"&rows=0", "/search/node", 0, 10, expected)
	assertActiveOpenSearchPageNames(t, restarted.router, "silent-bob", searchPath("/search/node", query)+"&rows=100000", "/search/node", 0, 10, expected)
	assertActiveOpenSearchPageNames(t, restarted.router, "silent-bob", searchPath("/search/node", query)+"&start=2&rows=3", "/search/node", 2, 10, []string{"delta", "echo", "foxtrot"})
	assertActiveOpenSearchPageNames(t, restarted.router, "silent-bob", searchPath("/search/node", query)+"&start=999&rows=25", "/search/node", 999, 10, []string{})
}

func TestActivePostgresOpenSearchMutationEventsDriveSearchResults(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)

	fixture.createOrganizationWithValidator("ponyville")
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:ponyville-validator"), "/search/client", []string{"ponyville-validator"})

	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/clients", mustMarshalDataBagJSON(t, map[string]any{
		"name":       "twilight",
		"public_key": publicKeyPEM,
	}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:twilight"), "/search/client", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/clients/twilight", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/client", "name:twilight"), "/search/client", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/environments", []byte(`{"name":"qa","json_class":"Chef::Environment","chef_type":"environment","description":"old-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:old-env-term"), "/search/environment", []string{"qa"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/environments/qa", []byte(`{"name":"qa","json_class":"Chef::Environment","chef_type":"environment","description":"new-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:old-env-term"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:new-env-term"), "/search/environment", []string{"qa"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/environments/qa", []byte(`{"name":"staging","json_class":"Chef::Environment","chef_type":"environment","description":"renamed-env-term","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "name:qa"), "/search/environment", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "name:staging"), "/search/environment", []string{"staging"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/environments/staging", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/environment", "description:renamed-env-term"), "/search/environment", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "twilight", "normal": map[string]any{"team": "weather"}}), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:friendship"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "wrong-name", "normal": map[string]any{"team": "bad"}}), http.StatusBadRequest)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:bad"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{"twilight"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{})
	transport.ForceDocument("ponyville/node/twilight", map[string]any{
		"organization": "ponyville",
		"index":        "node",
		"name":         []any{"twilight"},
		"team":         []any{"weather"},
	})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:weather"), "/search/node", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/roles", []byte(`{"name":"web","description":"old-role-term","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{},"run_list":["base"],"env_run_lists":{}}`), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:old-role-term"), "/search/role", []string{"web"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/roles/web", []byte(`{"name":"web","description":"new-role-term"}`), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:old-role-term"), "/search/role", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:new-role-term"), "/search/role", []string{"web"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/roles/web", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/role", "description:new-role-term"), "/search/role", []string{})

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/data/ponies", mustMarshalDataBagJSON(t, map[string]any{"id": "alice", "color": "blue"}), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:blue"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/data/ponies/alice", mustMarshalDataBagJSON(t, map[string]any{"id": "alice", "color": "green"}), http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:blue"), "/search/ponies", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/data/ponies/alice", mustMarshalDataBagJSON(t, map[string]any{"id": "wrong", "color": "red"}), http.StatusBadRequest)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:red"), "/search/ponies", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{"data_bag_item_ponies_alice"})
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/data/ponies/alice", nil, http.StatusOK)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/ponies", "color:green"), "/search/ponies", []string{})
}

func TestActivePostgresOpenSearchUnsupportedObjectMutationsDoNotIndexDocuments(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")
	primeActiveOpenSearchCoreObjectIndexer(t, fixture.router)

	wantDocs := transport.SnapshotDocuments()
	wantMutations := transport.SnapshotMutationRequests()

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/cookbooks/unindexed/1.0.0", mustMarshalSandboxJSON(t, cookbookVersionPayload("unindexed", "1.0.0", "", nil)), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/cookbooks/unindexed/1.0.0", nil, http.StatusOK)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/cookbook_artifacts/unindexedartifact/1111111111111111111111111111111111111111", mustMarshalSandboxJSON(t, cookbookArtifactPayload("unindexedartifact", "1111111111111111111111111111111111111111", "1.0.0", "", nil)), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/cookbook_artifacts/unindexedartifact/1111111111111111111111111111111111111111", nil, http.StatusOK)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/policy_groups/dev/policies/appserver", mustMarshalPolicyJSON(t, minimalPolicyPayload("appserver", "1111111111111111111111111111111111111111")), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/policy_groups/dev/policies/appserver", nil, http.StatusOK)
	createAndCommitUnindexedSandbox(t, fixture.router)

	transport.RequireDocuments(t, wantDocs)
	transport.RequireMutationRequests(t, wantMutations)
}

func TestActivePostgresOpenSearchUnavailableRoutesUseStableErrors(t *testing.T) {
	fixture := newActivePostgresBootstrapFixtureWithSearch(t, pgtest.NewState(pgtest.Seed{}), func(state *bootstrap.Service) search.Index {
		return search.NewOpenSearchIndex(state, nil, "http://opensearch.example")
	})
	fixture.createOrganizationWithValidator("ponyville")

	listReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/search", nil)
	listRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable /search status = %d, want %d, body = %s", listRec.Code, http.StatusServiceUnavailable, listRec.Body.String())
	}
	assertAPIError(t, listRec, "search_unavailable")
	if strings.Contains(listRec.Body.String(), "opensearch.example") {
		t.Fatalf("unavailable /search leaked provider details: %s", listRec.Body.String())
	}

	statusReq := httptest.NewRequest(http.MethodGet, "/_status", nil)
	statusRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(statusRec, statusReq)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("/_status status = %d, want %d, body = %s", statusRec.Code, http.StatusOK, statusRec.Body.String())
	}
	var statusPayload map[string]any
	if err := json.Unmarshal(statusRec.Body.Bytes(), &statusPayload); err != nil {
		t.Fatalf("json.Unmarshal(/_status) error = %v", err)
	}
	openSearchStatus := statusPayload["dependencies"].(map[string]any)["opensearch"].(map[string]any)
	if openSearchStatus["backend"] != "opensearch" || openSearchStatus["configured"] != true || openSearchStatus["message"] != "OpenSearch is configured but unavailable; search routes cannot reach the provider" {
		t.Fatalf("opensearch status = %v, want configured unavailable wording", openSearchStatus)
	}
}

func TestActivePostgresOpenSearchUnsupportedIndexesIgnoreUnavailableProvider(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	persisted.createOrganizationWithValidator("ponyville")
	seedUnsupportedSearchObjectFamilies(t, persisted.router)

	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, unavailableAPIOpenSearchTransport{}, nil)
	for _, index := range []string{"cookbooks", "cookbook_artifacts", "policy", "policy_groups", "sandbox", "checksums"} {
		t.Run(index, func(t *testing.T) {
			assertUnsupportedSearchIndex(t, fixture.router, http.MethodGet, searchPath("/search/"+index, "*:*"), "/search/"+index, nil, index)
			assertUnsupportedSearchIndex(t, fixture.router, http.MethodPost, searchPath("/search/"+index, "name:*"), "/search/"+index, []byte(`{"name":["name"]}`), index)
		})
	}
}

func TestActivePostgresOpenSearchQueryUnavailableUsesStableError(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, unavailableAPIOpenSearchTransport{}, nil)
	fixture.createOrganizationWithValidator("ponyville")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)

	req := httptest.NewRequest(http.MethodGet, searchPath("/search/node", "team:friendship"), nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable search query status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	assertAPIError(t, rec, "search_unavailable")
	for _, leaked := range []string{"opensearch.example", "provider body", "internal cluster"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("unavailable search query leaked %q in body: %s", leaked, rec.Body.String())
		}
	}
}

func TestActivePostgresOpenSearchRejectedQueryUsesStableError(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, rejectedAPIOpenSearchTransport{}, nil)
	fixture.createOrganizationWithValidator("ponyville")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)

	req := httptest.NewRequest(http.MethodGet, searchPath("/search/node", "team:friendship"), nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/search/node", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("rejected search query status = %d, want %d, body = %s", rec.Code, http.StatusInternalServerError, rec.Body.String())
	}
	assertAPIError(t, rec, "search_failed")
	for _, leaked := range []string{"opensearch.example", "provider body", "query parser internals"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("rejected search query leaked %q in body: %s", leaked, rec.Body.String())
		}
	}
}

// TestActivePostgresOpenSearchEncryptedDataBagQueryUnavailableUsesStableError
// keeps Chef-facing provider-unavailable behavior redacted for encrypted-looking
// data bag search routes as well as built-in indexes.
func TestActivePostgresOpenSearchEncryptedDataBagQueryUnavailableUsesStableError(t *testing.T) {
	persisted := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture := newActivePostgresOpenSearchFixture(t, persisted.pgState, unavailableAPIOpenSearchTransport{}, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	bagPath := "/organizations/ponyville/data/" + bagName
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)

	rawPath := searchPath("/search/"+bagName, "password_encrypted_data:"+encryptedDataBagFixtureField(t, "password", "encrypted_data"))
	req := httptest.NewRequest(http.MethodGet, rawPath, nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/search/"+bagName, nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	fixture.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("encrypted unavailable search status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	assertAPIError(t, rec, "search_unavailable")
	for _, leaked := range []string{"opensearch.example", "provider body", "internal cluster", "correct horse battery staple"} {
		if strings.Contains(rec.Body.String(), leaked) {
			t.Fatalf("encrypted unavailable search leaked %q in body: %s", leaked, rec.Body.String())
		}
	}
}

func TestActivePostgresOpenSearchIndexingFailuresDoNotRollbackObjectPersistence(t *testing.T) {
	indexer := &failingAPIDocumentIndexer{}
	fixture := newActivePostgresBootstrapFixtureWithSearchIndexerAndAuthorizer(t, pgtest.NewState(pgtest.Seed{}), nil, indexer, nil)
	fixture.createOrganizationWithValidator("ponyville")
	if indexer.upserts == 0 {
		t.Fatal("organization creation did not attempt a validator-client index upsert")
	}

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{}, map[string]any{"team": "friendship"}, []string{"base"}), http.StatusCreated)
	if indexer.upserts < 2 {
		t.Fatalf("node creation upserts = %d, want at least validator plus node attempts", indexer.upserts)
	}

	getReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/nodes/twilight", nil)
	getRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("node persisted after failed index upsert status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	before := indexer.snapshot()
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, "/nodes/twilight", mustMarshalDataBagJSON(t, map[string]any{"name": "wrong-name", "normal": map[string]any{"team": "bad"}}), http.StatusBadRequest)
	indexer.requireSnapshot(t, before)

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK)
	if indexer.deletes == 0 {
		t.Fatal("node delete did not attempt an index delete")
	}

	missingReq := newSignedJSONRequestAs(t, "pivotal", http.MethodGet, "/nodes/twilight", nil)
	missingRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("node deleted despite failed index delete status = %d, want %d, body = %s", missingRec.Code, http.StatusNotFound, missingRec.Body.String())
	}
}

// TestActivePostgresOpenSearchEncryptedDataBagInvalidWritesDoNotMutateDocuments
// proves invalid encrypted-looking item writes do not emit stale OpenSearch
// document changes when provider-backed indexing is active.
func TestActivePostgresOpenSearchEncryptedDataBagInvalidWritesDoNotMutateDocuments(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/organizations/ponyville/data/" + bagName
	itemPath := bagPath + "/" + itemID
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:"+itemID), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})

	beforeInvalidWrites := transport.SnapshotDocuments()
	mismatchedUpdate := encryptedEnvelopeVariantPayload(t, itemID, func(envelope map[string]any) {
		envelope["encrypted_data"] = "opensearch-should-not-see-this"
	})
	mismatchedUpdate["id"] = "wrong-item"
	mismatchedUpdate["environment"] = "blocked"
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, mismatchedUpdate), http.StatusBadRequest)

	missingID := testfixtures.CloneDataBagPayload(testfixtures.EncryptedDataBagItem())
	delete(missingID, "id")
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, missingID), http.StatusBadRequest)

	transport.RequireDocuments(t, beforeInvalidWrites)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:"+itemID), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "environment:blocked"), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "id:wrong-item"), "/search/"+bagName, []string{})
}

// TestActivePostgresOpenSearchEncryptedDataBagSearchAndPartialSearch pins the
// active PostgreSQL plus OpenSearch-backed path to the same opaque encrypted
// data bag search contract as the in-memory search index.
func TestActivePostgresOpenSearchEncryptedDataBagSearchAndPartialSearch(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	fixture.createOrganizationWithValidator("ponyville")

	bagName := testfixtures.EncryptedDataBagName()
	itemID := testfixtures.EncryptedDataBagItemID()
	bagPath := "/organizations/ponyville/data/" + bagName
	passwordCiphertext := encryptedDataBagFixtureField(t, "password", "encrypted_data")
	passwordIV := encryptedDataBagFixtureField(t, "password", "iv")
	apiAuthTag := encryptedDataBagFixtureField(t, "api_key", "auth_tag")

	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, bagPath, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)

	fullRec := performActiveOpenSearchRequest(t, fixture.router, http.MethodGet, searchPath("/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, nil)
	assertEncryptedDataBagSearchFullRow(t, decodeSearchPayload(t, fullRec), bagName, itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, fullRec)

	partialRec := performActiveOpenSearchRequest(t, fixture.router, http.MethodPost, searchPath("/organizations/ponyville/search/"+bagName, "environment:production AND api_key_auth_tag:"+apiAuthTag), "/organizations/ponyville/search/"+bagName, encryptedDataBagPartialSearchBody(t))
	assertEncryptedDataBagPartialSearchRow(t, decodeSearchPayload(t, partialRec), "/organizations/ponyville/data/"+bagName+"/"+itemID)
	assertSearchResponseOmitsDecryptedPlaintext(t, partialRec)

	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "password_iv:"+passwordIV), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "environment:production"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "*_encrypted_data:*"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "raw_data_password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/"+bagName, "correct-horse-battery-staple"), "/search/"+bagName, []string{})

	denied := newActivePostgresOpenSearchFixture(t, fixture.pgState, transport, func(state *bootstrap.Service) authz.Authorizer {
		return &denySearchDocumentAfterIndexGateAuthorizer{
			base:   authz.NewACLAuthorizer(state),
			target: "data_bag:" + bagName,
		}
	})
	assertActiveOpenSearchFullRows(t, denied.router, searchPath("/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), "/search/"+bagName, []string{})
}

func newActivePostgresOpenSearchFixture(t *testing.T, pgState *pgtest.State, transport search.OpenSearchTransport, authorizerFactory func(*bootstrap.Service) authz.Authorizer) *activePostgresBootstrapFixture {
	t.Helper()

	return newActivePostgresBootstrapFixtureWithSearchAndAuthorizer(t, pgState, func(state *bootstrap.Service) search.Index {
		client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
		if err != nil {
			t.Fatalf("NewOpenSearchClient() error = %v", err)
		}
		return search.NewOpenSearchIndex(state, client, "http://opensearch.example")
	}, authorizerFactory)
}

func newActivePostgresOpenSearchIndexingFixture(t *testing.T, pgState *pgtest.State, client *search.OpenSearchClient, authorizerFactory func(*bootstrap.Service) authz.Authorizer) *activePostgresBootstrapFixture {
	t.Helper()

	return newActivePostgresBootstrapFixtureWithSearchIndexerAndAuthorizer(t, pgState, func(state *bootstrap.Service) search.Index {
		return search.NewOpenSearchIndex(state, client, "http://opensearch.example")
	}, client, authorizerFactory)
}

func seedActivePostgresOpenSearchState(t *testing.T, fixture *activePostgresBootstrapFixture) {
	t.Helper()

	fixture.createOrganizationWithValidator("ponyville")
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    "silent-bob",
		DisplayName: "Silent Bob",
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(silent-bob) error = %v", err)
	}
	if err := fixture.state.AddUserToGroup("ponyville", "users", "silent-bob"); err != nil {
		t.Fatalf("AddUserToGroup(silent-bob) error = %v", err)
	}
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/environments", []byte(`{"name":"production","json_class":"Chef::Environment","chef_type":"environment","description":"prod-search-target","cookbook_versions":{},"default_attributes":{"region":"equus"},"override_attributes":{}}`), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalSearchNodePayload(t, "twilight", map[string]any{
		"path":  "foo/bar",
		"build": "010",
	}, map[string]any{
		"team": "friendship",
	}, []string{"web"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalSearchNodePayload(t, "rainbow", map[string]any{}, map[string]any{
		"team":  "weather",
		"build": "100",
	}, []string{"base"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/roles", []byte(`{"name":"web","description":"role-search-target","json_class":"Chef::Role","chef_type":"role","default_attributes":{},"override_attributes":{},"run_list":["base"],"env_run_lists":{}}`), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": "ponies"}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data/ponies", mustMarshalDataBagJSON(t, map[string]any{
		"id":          "alice",
		"path":        "foo/bar",
		"badge":       "primary[blue]",
		"note":        "hello world",
		"punctuation": "hello!world",
		"ssh": map[string]any{
			"public_key":  "---RSA Public Key--- Alice",
			"private_key": "---RSA Private Key--- Alice",
		},
	}), http.StatusCreated)

	secretBag := testfixtures.EncryptedDataBagName()
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": secretBag}), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, fixture.router, http.MethodPost, "/organizations/ponyville/data/"+secretBag, mustMarshalDataBagJSON(t, testfixtures.EncryptedDataBagItem()), http.StatusCreated)
}

func sendActivePostgresPivotalJSON(t *testing.T, router http.Handler, method, path string, body []byte, want int) {
	t.Helper()

	req := newSignedJSONRequestAs(t, "pivotal", method, path, body)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, path, rec.Code, want, rec.Body.String())
	}
}

func primeActiveOpenSearchCoreObjectIndexer(t *testing.T, router http.Handler) {
	t.Helper()

	sendActivePostgresPivotalJSON(t, router, http.MethodPost, "/environments", []byte(`{"name":"index-primer","json_class":"Chef::Environment","chef_type":"environment","description":"temporary index primer","cookbook_versions":{},"default_attributes":{},"override_attributes":{}}`), http.StatusCreated)
	sendActivePostgresPivotalJSON(t, router, http.MethodDelete, "/environments/index-primer", nil, http.StatusOK)
}

func createAndCommitUnindexedSandbox(t *testing.T, router http.Handler) {
	t.Helper()

	content := []byte("unindexed sandbox content")
	checksum := checksumHex(content)
	createReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(sandbox create) error = %v", err)
	}
	sandboxID := payload["sandbox_id"].(string)
	checksums := payload["checksums"].(map[string]any)
	checksumEntry := checksums[checksum].(map[string]any)
	uploadURL := checksumEntry["url"].(string)

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/octet-stream")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload sandbox checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}

	commitReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPut, "/sandboxes/"+sandboxID, mustMarshalSandboxJSON(t, map[string]any{"is_completed": true}))
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusOK {
		t.Fatalf("commit sandbox status = %d, want %d, body = %s", commitRec.Code, http.StatusOK, commitRec.Body.String())
	}
}

func assertActiveOpenSearchIndexURLs(t *testing.T, router http.Handler, path string, want map[string]string) {
	t.Helper()

	rec := performActiveOpenSearchRequest(t, router, http.MethodGet, path, path, nil)
	var payload map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v, body = %s", path, err, rec.Body.String())
	}
	if len(payload) != len(want) {
		t.Fatalf("%s index count = %d, want %d (%v)", path, len(payload), len(want), payload)
	}
	for key, expectedURL := range want {
		if payload[key] != expectedURL {
			t.Fatalf("%s index %q = %q, want %q", path, key, payload[key], expectedURL)
		}
	}
}

func assertActiveOpenSearchFullRows(t *testing.T, router http.Handler, rawPath, signPath string, wantNames []string) {
	t.Helper()

	rec := performActiveOpenSearchRequest(t, router, http.MethodGet, rawPath, signPath, nil)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(len(wantNames)) || len(rows) != len(wantNames) {
		t.Fatalf("%s payload = %v, want %d rows", rawPath, payload, len(wantNames))
	}
	for i, want := range wantNames {
		name := rows[i].(map[string]any)["name"]
		if name != want {
			t.Fatalf("%s row[%d] name = %v, want %q", rawPath, i, name, want)
		}
	}
}

// assertActiveOpenSearchPartialData checks that active provider-backed search
// still hydrates current PostgreSQL state before shaping partial-search rows.
func assertActiveOpenSearchPartialData(t *testing.T, router http.Handler, rawPath, signPath string, body []byte, wantURL string, wantData map[string]any) {
	t.Helper()

	rec := performActiveOpenSearchRequest(t, router, http.MethodPost, rawPath, signPath, body)
	payload := decodeSearchPayload(t, rec)
	rows := payload["rows"].([]any)
	if payload["total"] != float64(1) || len(rows) != 1 {
		t.Fatalf("active partial search %s payload = %v, want one row", rawPath, payload)
	}
	row := rows[0].(map[string]any)
	if row["url"] != wantURL {
		t.Fatalf("active partial search %s url = %v, want %q", rawPath, row["url"], wantURL)
	}
	data := row["data"].(map[string]any)
	if !reflect.DeepEqual(data, wantData) {
		t.Fatalf("active partial search %s data = %v, want %v", rawPath, data, wantData)
	}
}

// assertActiveOpenSearchPageNames verifies provider-backed route paging after
// stale IDs are ignored, objects are hydrated, and ACL filtering has completed.
func assertActiveOpenSearchPageNames(t *testing.T, router http.Handler, userID, rawPath, signPath string, wantStart, wantTotal int, wantNames []string) {
	t.Helper()

	rec := performActiveOpenSearchRequestAs(t, router, userID, http.MethodGet, rawPath, signPath, nil)
	payload := decodeSearchPayload(t, rec)
	if payload["start"] != float64(wantStart) || payload["total"] != float64(wantTotal) {
		t.Fatalf("active search page %s start/total = %v/%v, want %d/%d", rawPath, payload["start"], payload["total"], wantStart, wantTotal)
	}
	rows := payload["rows"].([]any)
	if len(rows) != len(wantNames) {
		t.Fatalf("active search page %s rows len = %d, want %d (%v)", rawPath, len(rows), len(wantNames), rows)
	}
	for i, wantName := range wantNames {
		if got := rows[i].(map[string]any)["name"]; got != wantName {
			t.Fatalf("active search page %s row[%d] name = %v, want %q", rawPath, i, got, wantName)
		}
	}
}

func performActiveOpenSearchRequest(t *testing.T, router http.Handler, method, rawPath, signPath string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	return performActiveOpenSearchRequestAs(t, router, "pivotal", method, rawPath, signPath, body)
}

// performActiveOpenSearchRequestAs lets provider-backed route tests run the
// same hydrated search path as admin and non-admin org members.
func performActiveOpenSearchRequestAs(t *testing.T, router http.Handler, userID, method, rawPath, signPath string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	var reader *bytes.Reader
	if body == nil {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, rawPath, reader)
	applySignedHeaders(t, req, userID, "", method, signPath, body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s status = %d, want %d, body = %s", method, rawPath, rec.Code, http.StatusOK, rec.Body.String())
	}
	return rec
}

func searchPath(path, query string) string {
	return path + "?q=" + url.QueryEscape(query)
}

func apiOpenSearchQueryKey(org, index, query string) string {
	return org + "\x00" + index + "\x00" + query
}

type apiOpenSearchSearchTransport struct {
	t       *testing.T
	ids     map[string][]string
	sources map[string]map[string]any
}

func newAPIOpenSearchSearchTransport(t *testing.T, ids map[string][]string) *apiOpenSearchSearchTransport {
	t.Helper()

	return newAPIOpenSearchSearchTransportWithSources(t, ids, nil)
}

func newAPIOpenSearchSearchTransportWithSources(t *testing.T, ids map[string][]string, sources map[string]map[string]any) *apiOpenSearchSearchTransport {
	t.Helper()

	return &apiOpenSearchSearchTransport{
		t:       t,
		ids:     ids,
		sources: sources,
	}
}

func (t *apiOpenSearchSearchTransport) Do(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodPost || req.URL.Path != "/chef/_search" {
		t.t.Fatalf("OpenSearch request = %s %s, want POST /chef/_search", req.Method, req.URL.Path)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(OpenSearch request) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.t.Fatalf("json.Unmarshal(OpenSearch request) error = %v, body = %s", err, string(body))
	}

	org, index, query := apiOpenSearchSearchIdentity(t.t, payload)
	key := apiOpenSearchQueryKey(org, index, query)
	ids, ok := t.ids[key]
	if !ok {
		t.t.Fatalf("no fake OpenSearch IDs for org=%q index=%q query=%q body=%s", org, index, query, string(body))
	}

	hits := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		hit := map[string]any{"_id": id}
		if source, ok := t.sources[id]; ok {
			hit["_source"] = source
		}
		hits = append(hits, hit)
	}
	raw, err := json.Marshal(map[string]any{
		"hits": map[string]any{
			"hits": hits,
		},
	})
	if err != nil {
		t.t.Fatalf("json.Marshal(OpenSearch response) error = %v", err)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Request:    req,
	}, nil
}

func apiOpenSearchSearchIdentity(t *testing.T, payload map[string]any) (string, string, string) {
	t.Helper()

	boolQuery := payload["query"].(map[string]any)["bool"].(map[string]any)
	org := apiOpenSearchCompatFilterValue(t, boolQuery["filter"], "__org")
	index := apiOpenSearchCompatFilterValue(t, boolQuery["filter"], "__index")
	query := apiOpenSearchCompatQueryString(t, boolQuery["must"])
	return org, index, query
}

func apiOpenSearchCompatFilterValue(t *testing.T, raw any, field string) string {
	t.Helper()

	for _, filter := range raw.([]any) {
		term := filter.(map[string]any)["term"].(map[string]any)
		token, ok := term["compat_terms"].(string)
		if !ok {
			continue
		}
		if value, ok := strings.CutPrefix(token, field+"="); ok {
			return value
		}
	}
	t.Fatalf("OpenSearch filter missing %q: %v", field, raw)
	return ""
}

func apiOpenSearchCompatQueryString(t *testing.T, raw any) string {
	t.Helper()

	clause, ok := raw.(map[string]any)
	if !ok {
		t.Fatalf("OpenSearch query clause = %T %v, want object", raw, raw)
	}
	if _, ok := clause["match_all"].(map[string]any); ok {
		return "*:*"
	}
	if _, ok := clause["match_none"].(map[string]any); ok {
		return ""
	}
	if term, ok := clause["term"].(map[string]any); ok {
		return apiOpenSearchCompatTokenQuery(t, term["compat_terms"].(string), false)
	}
	if prefix, ok := clause["prefix"].(map[string]any); ok {
		return apiOpenSearchCompatTokenQuery(t, prefix["compat_terms"].(string), true)
	}
	if wildcard, ok := clause["wildcard"].(map[string]any); ok {
		return apiOpenSearchCompatWildcardQuery(t, wildcard["compat_terms"].(string))
	}
	if rangeClause, ok := clause["range"].(map[string]any); ok {
		return apiOpenSearchCompatRangeQuery(t, rangeClause["compat_terms"].(map[string]any))
	}

	boolQuery, ok := clause["bool"].(map[string]any)
	if !ok {
		t.Fatalf("OpenSearch query clause = %v, want bool/term/prefix/match", clause)
	}
	if should, ok := boolQuery["should"].([]any); ok {
		parts := make([]string, 0, len(should))
		for _, item := range should {
			parts = append(parts, apiOpenSearchCompatQueryString(t, item))
		}
		return strings.Join(parts, " OR ")
	}

	parts := make([]string, 0)
	if must, ok := boolQuery["must"].([]any); ok {
		for _, item := range must {
			part := apiOpenSearchCompatQueryString(t, item)
			if part != "*:*" && part != "" {
				parts = append(parts, part)
			}
		}
	}
	if mustNot, ok := boolQuery["must_not"].([]any); ok {
		for _, item := range mustNot {
			part := apiOpenSearchCompatQueryString(t, item)
			if part != "" {
				parts = append(parts, "NOT "+part)
			}
		}
	}
	return strings.Join(parts, " AND ")
}

func apiOpenSearchCompatTokenQuery(t *testing.T, token string, wildcard bool) string {
	t.Helper()

	field, value, ok := strings.Cut(token, "=")
	if !ok {
		t.Fatalf("OpenSearch compat token = %q, want field=value", token)
	}
	value = apiOpenSearchEscapeQueryValue(value)
	if strings.ContainsAny(value, " \t") {
		value = `"` + value + `"`
	}
	if wildcard {
		value += "*"
	}
	if field == "__any" {
		return value
	}
	return field + ":" + value
}

func apiOpenSearchCompatWildcardQuery(t *testing.T, token string) string {
	t.Helper()

	field, value, ok := strings.Cut(token, "=")
	if !ok {
		t.Fatalf("OpenSearch compat wildcard token = %q, want field=value", token)
	}
	value = apiOpenSearchEscapeQueryValue(value)
	if field == "__any" {
		return value
	}
	return field + ":" + value
}

func apiOpenSearchCompatRangeQuery(t *testing.T, bounds map[string]any) string {
	t.Helper()

	field, start, startInclusive := apiOpenSearchCompatRangeStart(t, bounds)
	endField, end, endInclusive := apiOpenSearchCompatRangeEnd(t, bounds)
	if endField != "" && field != endField {
		t.Fatalf("OpenSearch range fields = %q/%q, want same field", field, endField)
	}
	if field == "" {
		field = endField
	}
	if field == "" {
		t.Fatalf("OpenSearch range bounds missing field: %v", bounds)
	}
	open := "["
	if !startInclusive {
		open = "{"
	}
	close := "]"
	if !endInclusive {
		close = "}"
	}
	return field + ":" + open + apiOpenSearchEscapeQueryValue(start) + " TO " + apiOpenSearchEscapeQueryValue(end) + close
}

func apiOpenSearchCompatRangeStart(t *testing.T, bounds map[string]any) (string, string, bool) {
	t.Helper()

	if raw, ok := bounds["gte"]; ok {
		field, value := apiOpenSearchCompatRangeToken(t, raw)
		return field, value, true
	}
	if raw, ok := bounds["gt"]; ok {
		field, value := apiOpenSearchCompatRangeToken(t, raw)
		return field, value, false
	}
	return "", "*", true
}

func apiOpenSearchCompatRangeEnd(t *testing.T, bounds map[string]any) (string, string, bool) {
	t.Helper()

	if raw, ok := bounds["lte"]; ok {
		field, value := apiOpenSearchCompatRangeToken(t, raw)
		if value == "\ufff0" {
			value = "*"
		}
		return field, value, true
	}
	if raw, ok := bounds["lt"]; ok {
		field, value := apiOpenSearchCompatRangeToken(t, raw)
		return field, value, false
	}
	return "", "*", true
}

func apiOpenSearchCompatRangeToken(t *testing.T, raw any) (string, string) {
	t.Helper()

	token, ok := raw.(string)
	if !ok {
		t.Fatalf("OpenSearch compat range token = %T(%v), want string", raw, raw)
	}
	field, value, ok := strings.Cut(token, "=")
	if !ok {
		t.Fatalf("OpenSearch compat range token = %q, want field=value", token)
	}
	if value == "" {
		value = "*"
	}
	return field, value
}

func apiOpenSearchEscapeQueryValue(value string) string {
	replacer := strings.NewReplacer(`:`, `\:`, `[`, `\[`, `]`, `\]`, `@`, `\@`, `/`, `\/`)
	return replacer.Replace(value)
}

type unavailableAPIOpenSearchTransport struct{}

func (unavailableAPIOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("raw provider body from internal cluster opensearch.example")),
		Request:    req,
	}, nil
}

type rejectedAPIOpenSearchTransport struct{}

func (rejectedAPIOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("raw provider body with query parser internals opensearch.example")),
		Request:    req,
	}, nil
}

type failingAPIDocumentIndexer struct {
	upserts int
	deletes int
}

type failingAPIDocumentIndexerSnapshot struct {
	upserts int
	deletes int
}

func (i *failingAPIDocumentIndexer) UpsertDocuments(context.Context, []search.Document) error {
	i.upserts++
	return search.ErrUnavailable
}

func (i *failingAPIDocumentIndexer) DeleteDocuments(context.Context, []search.DocumentRef) error {
	i.deletes++
	return search.ErrUnavailable
}

func (i *failingAPIDocumentIndexer) snapshot() failingAPIDocumentIndexerSnapshot {
	return failingAPIDocumentIndexerSnapshot{
		upserts: i.upserts,
		deletes: i.deletes,
	}
}

func (i *failingAPIDocumentIndexer) requireSnapshot(t *testing.T, want failingAPIDocumentIndexerSnapshot) {
	t.Helper()

	if got := i.snapshot(); got != want {
		t.Fatalf("indexer snapshot = %+v, want %+v", got, want)
	}
}

type statefulAPIOpenSearchTransport struct {
	t               *testing.T
	mu              sync.Mutex
	docs            map[string]map[string]any
	bulkRequests    int
	deleteRequests  int
	refreshRequests int
}

type statefulAPIOpenSearchMutationSnapshot struct {
	bulkRequests    int
	deleteRequests  int
	refreshRequests int
}

func newStatefulAPIOpenSearchTransport(t *testing.T) *statefulAPIOpenSearchTransport {
	t.Helper()

	return &statefulAPIOpenSearchTransport{
		t:    t,
		docs: make(map[string]map[string]any),
	}
}

// SnapshotMutationRequests records provider writes so tests can prove
// unsupported Chef object mutations do not enqueue OpenSearch work.
func (tr *statefulAPIOpenSearchTransport) SnapshotMutationRequests() statefulAPIOpenSearchMutationSnapshot {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	return statefulAPIOpenSearchMutationSnapshot{
		bulkRequests:    tr.bulkRequests,
		deleteRequests:  tr.deleteRequests,
		refreshRequests: tr.refreshRequests,
	}
}

// RequireMutationRequests compares provider write counters and leaves search
// query traffic out of the assertion so tests can keep using the fake provider.
func (tr *statefulAPIOpenSearchTransport) RequireMutationRequests(t *testing.T, want statefulAPIOpenSearchMutationSnapshot) {
	t.Helper()

	got := tr.SnapshotMutationRequests()
	if got != want {
		t.Fatalf("OpenSearch mutation requests = %+v, want %+v", got, want)
	}
}

// SnapshotDocuments returns a deep copy of fake OpenSearch documents so tests
// can verify failed Chef-facing mutations did not emit provider-side changes.
func (tr *statefulAPIOpenSearchTransport) SnapshotDocuments() map[string]map[string]any {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	out := make(map[string]map[string]any, len(tr.docs))
	for id, doc := range tr.docs {
		out[id] = cloneOpenSearchSource(doc)
	}
	return out
}

// RequireDocuments compares the fake OpenSearch document set against a prior
// snapshot and fails with the complete diff context when unexpected drift occurs.
func (tr *statefulAPIOpenSearchTransport) RequireDocuments(t *testing.T, want map[string]map[string]any) {
	t.Helper()

	got := tr.SnapshotDocuments()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OpenSearch documents = %#v, want %#v", got, want)
	}
}

func (t *statefulAPIOpenSearchTransport) Do(req *http.Request) (*http.Response, error) {
	switch {
	case req.Method == http.MethodPost && req.URL.Path == "/_bulk":
		return t.handleBulk(req)
	case req.Method == http.MethodPut && req.URL.Path == "/chef/_mapping":
		return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"acknowledged": true}), nil
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_refresh":
		t.recordRefresh()
		return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"refreshed": true}), nil
	case req.Method == http.MethodDelete && strings.HasPrefix(req.URL.EscapedPath(), "/chef/_doc/"):
		return t.handleDelete(req)
	case req.Method == http.MethodPost && req.URL.Path == "/chef/_search":
		return t.handleSearch(req)
	default:
		t.t.Fatalf("OpenSearch request = %s %s, want bulk/refresh/delete/search", req.Method, req.URL.String())
		return nil, nil
	}
}

func (t *statefulAPIOpenSearchTransport) ForceDocument(id string, source map[string]any) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.docs[id] = cloneOpenSearchSource(source)
}

func (t *statefulAPIOpenSearchTransport) handleBulk(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(bulk request) error = %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines)%2 != 0 {
		t.t.Fatalf("bulk request has odd line count %d: %s", len(lines), string(body))
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.bulkRequests++
	for i := 0; i < len(lines); i += 2 {
		action := map[string]any{}
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk action) error = %v, line = %s", err, lines[i])
		}
		indexAction := action["index"].(map[string]any)
		id := indexAction["_id"].(string)
		source := map[string]any{}
		if err := json.Unmarshal([]byte(lines[i+1]), &source); err != nil {
			t.t.Fatalf("json.Unmarshal(bulk source) error = %v, line = %s", err, lines[i+1])
		}
		t.docs[id] = source
	}
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"errors": false}), nil
}

func (t *statefulAPIOpenSearchTransport) handleDelete(req *http.Request) (*http.Response, error) {
	escaped := strings.TrimPrefix(req.URL.EscapedPath(), "/chef/_doc/")
	id, err := url.PathUnescape(escaped)
	if err != nil {
		t.t.Fatalf("PathUnescape(%q) error = %v", escaped, err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteRequests++
	delete(t.docs, id)
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{"result": "deleted"}), nil
}

func (t *statefulAPIOpenSearchTransport) recordRefresh() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.refreshRequests++
}

func (t *statefulAPIOpenSearchTransport) handleSearch(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.t.Fatalf("ReadAll(search request) error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.t.Fatalf("json.Unmarshal(search request) error = %v, body = %s", err, string(body))
	}
	org, index, query := apiOpenSearchSearchIdentity(t.t, payload)
	plan := search.CompileQuery(query)

	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0)
	for id, source := range t.docs {
		if openSearchSourceString(source, "organization") != org || openSearchSourceString(source, "index") != index {
			continue
		}
		if !plan.MatchesFields(openSearchSourceFields(source)) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)

	hits := make([]map[string]string, 0, len(ids))
	for _, id := range ids {
		hits = append(hits, map[string]string{"_id": id})
	}
	return apiOpenSearchJSONResponse(req, http.StatusOK, map[string]any{
		"hits": map[string]any{"hits": hits},
	}), nil
}

func apiOpenSearchJSONResponse(req *http.Request, status int, payload any) *http.Response {
	raw, err := json.Marshal(payload)
	if err != nil {
		raw = []byte(`{"errors":true}`)
	}
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(raw)),
		Request:    req,
	}
}

func openSearchSourceString(source map[string]any, key string) string {
	value, _ := source[key].(string)
	return value
}

func openSearchSourceFields(source map[string]any) map[string][]string {
	fields := make(map[string][]string, len(source))
	for key, raw := range source {
		switch value := raw.(type) {
		case []any:
			for _, item := range value {
				fields[key] = append(fields[key], fmt.Sprint(item))
			}
		case []string:
			fields[key] = append(fields[key], value...)
		case string:
			fields[key] = append(fields[key], value)
		case bool, float64:
			fields[key] = append(fields[key], fmt.Sprint(value))
		}
	}
	return fields
}

func cloneOpenSearchSource(source map[string]any) map[string]any {
	raw, err := json.Marshal(source)
	if err != nil {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}
