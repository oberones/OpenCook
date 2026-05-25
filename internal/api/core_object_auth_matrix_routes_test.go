package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestActivePostgresOpenSearchCrossSurfaceAuthMatrixNoMutation(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, nil)
	validator := fixture.createOrganizationWithValidator("ponyville")
	createActivePostgresAuthMatrixUser(t, fixture, "normal-user", true)
	createActivePostgresAuthMatrixUser(t, fixture, "outside-user", false)
	nodeClient := createClientAsValidator(t, fixture.router, validator, "/organizations/ponyville/clients", "auth-matrix-client")

	node := nodePayloadExpectation{
		Name:            "auth-matrix-node",
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{"origin": "auth-matrix"},
		Normal:          map[string]any{"team": "auth-matrix"},
		Default:         map[string]any{"build": "101"},
		Automatic:       map[string]any{"platform": "equestria"},
		RunList:         []string{"recipe[base]"},
	}
	createNode := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/nodes", mustMarshalAPIVersionNodePayload(t, node), "2")
	if createNode.Code != http.StatusCreated {
		t.Fatalf("create matrix node status = %d, want %d, body = %s", createNode.Code, http.StatusCreated, createNode.Body.String())
	}

	bagName := "auth_matrix_bag"
	itemID := "visible"
	bagPath := "/organizations/ponyville/data/" + bagName
	itemPath := bagPath + "/" + itemID
	createBag := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/ponyville/data", mustMarshalDataBagJSON(t, map[string]any{"name": bagName}), "2")
	if createBag.Code != http.StatusCreated {
		t.Fatalf("create matrix data bag status = %d, want %d, body = %s", createBag.Code, http.StatusCreated, createBag.Body.String())
	}
	currentItem := map[string]any{"id": itemID, "category": "admin-created"}
	createItem := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, bagPath, mustMarshalDataBagJSON(t, currentItem), "2")
	if createItem.Code != http.StatusCreated {
		t.Fatalf("create matrix data bag item status = %d, want %d, body = %s", createItem.Code, http.StatusCreated, createItem.Body.String())
	}

	revisionID := "9191919191919191919191919191919191919191"
	createPolicy := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/organizations/ponyville/policy_groups/auth_matrix/policies/auth_matrix_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("auth_matrix_policy", revisionID)), "2")
	if createPolicy.Code != http.StatusCreated {
		t.Fatalf("create matrix policy assignment status = %d, want %d, body = %s", createPolicy.Code, http.StatusCreated, createPolicy.Body.String())
	}

	normalUpdate := map[string]any{"id": itemID, "category": "normal-user-allowed"}
	normalUpdateRec := serveSignedAPIVersionRequest(t, fixture.router, "normal-user", http.MethodPut, itemPath, mustMarshalDataBagJSON(t, normalUpdate), "2")
	if normalUpdateRec.Code != http.StatusOK {
		t.Fatalf("normal-user data bag item update status = %d, want %d, body = %s", normalUpdateRec.Code, http.StatusOK, normalUpdateRec.Body.String())
	}
	currentItem = normalUpdate

	clientCreateNode := node
	clientCreateNode.Name = "auth-matrix-client-node"
	clientCreateNode.Normal = map[string]any{"team": "client-created"}
	clientCreateRec := serveSignedClientAPIVersionRequest(t, fixture.router, nodeClient, http.MethodPost, "/organizations/ponyville/nodes", mustMarshalAPIVersionNodePayload(t, clientCreateNode), "2")
	if clientCreateRec.Code != http.StatusCreated {
		t.Fatalf("client node create status = %d, want %d, body = %s", clientCreateRec.Code, http.StatusCreated, clientCreateRec.Body.String())
	}

	validatorRead := serveSignedClientAPIVersionRequest(t, fixture.router, validator, http.MethodGet, itemPath, nil, "2")
	if validatorRead.Code != http.StatusOK {
		t.Fatalf("validator data bag item read status = %d, want %d, body = %s", validatorRead.Code, http.StatusOK, validatorRead.Body.String())
	}

	snapshot := transport.SnapshotDocuments()
	nodeACL := readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "node", Name: node.Name, Organization: "ponyville"})
	dataBagACL := readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "data_bag", Name: bagName, Organization: "ponyville"})
	policyACL := readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "policy", Name: "auth_matrix_policy", Organization: "ponyville"})
	policyGroupACL := readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "policy_group", Name: "auth_matrix", Organization: "ponyville"})

	outsideCreate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPost, "/organizations/ponyville/nodes", []byte(`{"name":`), "2")
	if outsideCreate.Code != http.StatusForbidden {
		t.Fatalf("outside-user malformed node create status = %d, want %d, body = %s", outsideCreate.Code, http.StatusForbidden, outsideCreate.Body.String())
	}
	invalidCreate := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodPost, "/organizations/ponyville/nodes", []byte(`{"name":`), "2")
	if invalidCreate.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user malformed node create status = %d, want %d, body = %s", invalidCreate.Code, http.StatusUnauthorized, invalidCreate.Body.String())
	}

	clientDeniedItem := serveSignedClientAPIVersionRequest(t, fixture.router, nodeClient, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, map[string]any{"id": itemID, "category": "client-denied"}), "2")
	if clientDeniedItem.Code != http.StatusForbidden {
		t.Fatalf("client data bag item update status = %d, want %d, body = %s", clientDeniedItem.Code, http.StatusForbidden, clientDeniedItem.Body.String())
	}
	validatorDeniedItem := serveSignedClientAPIVersionRequest(t, fixture.router, validator, http.MethodPut, itemPath, mustMarshalDataBagJSON(t, map[string]any{"id": itemID, "category": "validator-denied"}), "2")
	if validatorDeniedItem.Code != http.StatusForbidden {
		t.Fatalf("validator data bag item update status = %d, want %d, body = %s", validatorDeniedItem.Code, http.StatusForbidden, validatorDeniedItem.Body.String())
	}
	clientDeniedPolicy := serveSignedClientAPIVersionRequest(t, fixture.router, nodeClient, http.MethodPut, "/organizations/ponyville/policy_groups/auth_matrix/policies/auth_matrix_policy",
		mustMarshalPolicyJSON(t, canonicalPolicyPayloadForAPI("auth_matrix_policy", "9292929292929292929292929292929292929292")), "2")
	if clientDeniedPolicy.Code != http.StatusForbidden {
		t.Fatalf("client policy assignment update status = %d, want %d, body = %s", clientDeniedPolicy.Code, http.StatusForbidden, clientDeniedPolicy.Body.String())
	}

	transport.RequireDocuments(t, snapshot)
	assertNodeMissingWithVersion(t, fixture.router, "/organizations/ponyville/nodes/auth-matrix-outside-node", "2")
	assertRawDataBagItemWithVersion(t, fixture.router, itemPath, "2", currentItem)
	assertPolicyAssignmentRevisionForAPIVersion(t, fixture.router, "/organizations/ponyville/policy_groups/auth_matrix/policies/auth_matrix_policy", "2", revisionID)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "node", Name: node.Name, Organization: "ponyville"}), nodeACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "data_bag", Name: bagName, Organization: "ponyville"}), dataBagACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "policy", Name: "auth_matrix_policy", Organization: "ponyville"}), policyACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "policy_group", Name: "auth_matrix", Organization: "ponyville"}), policyGroupACL)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/organizations/ponyville/search/node", "team:auth-matrix"), "/organizations/ponyville/search/node", []string{node.Name})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/organizations/ponyville/search/node", "team:client-created"), "/organizations/ponyville/search/node", []string{clientCreateNode.Name})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/organizations/ponyville/search/"+bagName, "category:client-denied OR category:validator-denied"), "/organizations/ponyville/search/"+bagName, []string{})

	restarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertNodePayload(t, readNodePayloadWithVersion(t, restarted.router, "/organizations/ponyville/nodes/"+node.Name, "2"), node)
	assertNodePayload(t, readNodePayloadWithVersion(t, restarted.router, "/organizations/ponyville/nodes/"+clientCreateNode.Name, "2"), clientCreateNode)
	assertRawDataBagItemWithVersion(t, restarted.router, itemPath, "2", currentItem)
	assertPolicyAssignmentRevisionForAPIVersion(t, restarted.router, "/policy_groups/auth_matrix/policies/auth_matrix_policy", "2", revisionID)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, restarted, authz.Resource{Type: "node", Name: node.Name, Organization: "ponyville"}), nodeACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, restarted, authz.Resource{Type: "data_bag", Name: bagName, Organization: "ponyville"}), dataBagACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, restarted, authz.Resource{Type: "policy", Name: "auth_matrix_policy", Organization: "ponyville"}), policyACL)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, restarted, authz.Resource{Type: "policy_group", Name: "auth_matrix", Organization: "ponyville"}), policyGroupACL)
	assertActiveOpenSearchFullRows(t, restarted.router, searchPath("/search/"+bagName, "category:normal-user-allowed"), "/search/"+bagName, []string{"data_bag_item_" + bagName + "_" + itemID})
}

func TestActivePostgresOpenSearchDeniedACLGrantDoesNotMutate(t *testing.T) {
	transport := newStatefulAPIOpenSearchTransport(t)
	client, err := search.NewOpenSearchClient("http://opensearch.example", search.WithOpenSearchTransport(transport))
	if err != nil {
		t.Fatalf("NewOpenSearchClient() error = %v", err)
	}
	fixture := newActivePostgresOpenSearchIndexingFixture(t, pgtest.NewState(pgtest.Seed{}), client, func(state *bootstrap.Service) authz.Authorizer {
		return denyingPolicyMutationAuthorizer{
			base: authz.NewACLAuthorizer(state),
			deny: map[string]struct{}{
				"update:node:acl-gap-node": {},
			},
		}
	})
	fixture.createOrganizationWithValidator("ponyville")

	current := nodePayloadExpectation{
		Name:            "acl-gap-node",
		JSONClass:       "Chef::Node",
		ChefType:        "node",
		ChefEnvironment: "_default",
		Override:        map[string]any{"origin": "acl-gap"},
		Normal:          map[string]any{"team": "acl-gap"},
		Default:         map[string]any{"build": "202"},
		Automatic:       map[string]any{},
		RunList:         []string{"recipe[base]"},
	}
	createRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/nodes", mustMarshalAPIVersionNodePayload(t, current), "2")
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create ACL-gap node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	snapshot := transport.SnapshotDocuments()
	aclBefore := readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "node", Name: current.Name, Organization: "ponyville"})
	blocked := current
	blocked.Normal = map[string]any{"team": "acl-denied"}
	updateRec := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPut, "/nodes/"+current.Name, mustMarshalAPIVersionNodePayload(t, blocked), "2")
	if updateRec.Code != http.StatusForbidden {
		t.Fatalf("denied ACL node update status = %d, want %d, body = %s", updateRec.Code, http.StatusForbidden, updateRec.Body.String())
	}

	transport.RequireDocuments(t, snapshot)
	assertNodePayload(t, readNodePayloadWithVersion(t, fixture.router, "/organizations/ponyville/nodes/"+current.Name, "2"), current)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, fixture, authz.Resource{Type: "node", Name: current.Name, Organization: "ponyville"}), aclBefore)
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:acl-denied"), "/search/node", []string{})
	assertActiveOpenSearchFullRows(t, fixture.router, searchPath("/search/node", "team:acl-gap"), "/search/node", []string{current.Name})

	restarted := newActivePostgresOpenSearchIndexingFixture(t, fixture.pgState, client, nil)
	assertNodePayload(t, readNodePayloadWithVersion(t, restarted.router, "/nodes/"+current.Name, "2"), current)
	assertObjectACLEqualForAuthMatrixTest(t, readObjectACLForAuthMatrixTest(t, restarted, authz.Resource{Type: "node", Name: current.Name, Organization: "ponyville"}), aclBefore)
}

func TestActivePostgresFilesystemCookbookAuthMatrixPreservesBlobsAndMetadata(t *testing.T) {
	fixture, blobRoot := newFullActivePostgresFilesystemCookbookFixture(t)
	createActivePostgresAuthMatrixUser(t, fixture, "silent-bob", true)
	createActivePostgresAuthMatrixUser(t, fixture, "normal-user", true)
	createActivePostgresAuthMatrixUser(t, fixture, "outside-user", false)
	cookbookClient := createClientAsAdmin(t, fixture.router, "ponyville", "cookbook-auth-client", false)

	checksum := uploadActivePostgresCookbookChecksumWithoutCommit(t, fixture.router, []byte("puts 'auth matrix cookbook original'"))
	createActivePostgresCookbookVersion(t, fixture.router, "auth-matrix-cookbook", "1.0.0", checksum)
	assertCookbookDescription(t, fixture.router, "/cookbooks/auth-matrix-cookbook/1.0.0", "compatibility cookbook")
	assertFilesystemBlobExists(t, blobRoot, checksum, true)

	normalChecksum := uploadActivePostgresCookbookChecksumWithoutCommit(t, fixture.router, []byte("puts 'auth matrix cookbook normal update'"))
	normalPayload := cookbookVersionPayload("auth-matrix-cookbook", "1.0.0", normalChecksum, nil)
	normalPayload["metadata"].(map[string]any)["description"] = "normal user update survived"
	normalUpdate := serveSignedAPIVersionRequest(t, fixture.router, "normal-user", http.MethodPut, "/cookbooks/auth-matrix-cookbook/1.0.0", mustMarshalSandboxJSON(t, normalPayload), "2")
	if normalUpdate.Code != http.StatusOK {
		t.Fatalf("normal-user cookbook update status = %d, want %d, body = %s", normalUpdate.Code, http.StatusOK, normalUpdate.Body.String())
	}

	outsideChecksum := uploadActivePostgresCookbookChecksumWithoutCommit(t, fixture.router, []byte("puts 'auth matrix cookbook outside denied'"))
	outsidePayload := cookbookVersionPayload("auth-matrix-cookbook", "1.0.0", outsideChecksum, nil)
	outsidePayload["metadata"].(map[string]any)["description"] = "outside denied"
	outsideUpdate := serveSignedAPIVersionRequest(t, fixture.router, "outside-user", http.MethodPut, "/cookbooks/auth-matrix-cookbook/1.0.0", mustMarshalSandboxJSON(t, outsidePayload), "2")
	if outsideUpdate.Code != http.StatusForbidden {
		t.Fatalf("outside-user cookbook update status = %d, want %d, body = %s", outsideUpdate.Code, http.StatusForbidden, outsideUpdate.Body.String())
	}

	clientPayload := cookbookVersionPayload("auth-matrix-cookbook", "1.0.0", outsideChecksum, nil)
	clientPayload["metadata"].(map[string]any)["description"] = "client denied"
	clientUpdate := serveSignedClientAPIVersionRequest(t, fixture.router, cookbookClient, http.MethodPut, "/organizations/ponyville/cookbooks/auth-matrix-cookbook/1.0.0", mustMarshalSandboxJSON(t, clientPayload), "2")
	if clientUpdate.Code != http.StatusForbidden {
		t.Fatalf("client cookbook update status = %d, want %d, body = %s", clientUpdate.Code, http.StatusForbidden, clientUpdate.Body.String())
	}

	invalidDelete := serveSignedAPIVersionRequest(t, fixture.router, "invalid-user", http.MethodDelete, "/cookbooks/auth-matrix-cookbook/1.0.0", nil, "2")
	if invalidDelete.Code != http.StatusUnauthorized {
		t.Fatalf("invalid-user cookbook delete status = %d, want %d, body = %s", invalidDelete.Code, http.StatusUnauthorized, invalidDelete.Body.String())
	}

	assertCookbookDescription(t, fixture.router, "/cookbooks/auth-matrix-cookbook/1.0.0", "normal user update survived")
	assertCookbookDownloadBody(t, fixture.router, activePostgresCookbookFileURL(t, fixture.router, "/cookbooks/auth-matrix-cookbook/1.0.0"), "puts 'auth matrix cookbook normal update'")
	assertFilesystemBlobExists(t, blobRoot, normalChecksum, true)

	restarted := restartFullActivePostgresFilesystemCookbookFixture(t, fixture.pgState, blobRoot)
	assertCookbookDescription(t, restarted.router, "/organizations/ponyville/cookbooks/auth-matrix-cookbook/1.0.0", "normal user update survived")
	assertCookbookDownloadBody(t, restarted.router, activePostgresCookbookFileURL(t, restarted.router, "/organizations/ponyville/cookbooks/auth-matrix-cookbook/1.0.0"), "puts 'auth matrix cookbook normal update'")
}

// createActivePostgresAuthMatrixUser registers a signed test user through the
// live service so authn, group membership, and PostgreSQL state stay aligned.
func createActivePostgresAuthMatrixUser(t *testing.T, fixture *activePostgresBootstrapFixture, username string, orgMember bool) {
	t.Helper()

	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	if _, _, err := fixture.state.CreateUser(bootstrap.CreateUserInput{
		Username:    username,
		DisplayName: username,
		PublicKey:   publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateUser(%s) error = %v", username, err)
	}
	if orgMember {
		if err := fixture.state.AddUserToGroup("ponyville", "users", username); err != nil {
			t.Fatalf("AddUserToGroup(%s) error = %v", username, err)
		}
	}
}

// serveSignedClientAPIVersionRequest keeps client and validator matrix rows on
// the same signed API-version path as user-backed test requests.
func serveSignedClientAPIVersionRequest(t *testing.T, router http.Handler, requestor signedClientRequestor, method, path string, body []byte, serverAPIVersion string) *httptest.ResponseRecorder {
	t.Helper()

	req := requestor.newSignedJSONRequestWithServerAPIVersion(t, method, path, body, serverAPIVersion)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// readObjectACLForAuthMatrixTest fetches persisted object ACLs directly from
// the bootstrap resolver so denied writes can prove ACL documents stayed stable.
func readObjectACLForAuthMatrixTest(t *testing.T, fixture *activePostgresBootstrapFixture, resource authz.Resource) authz.ACL {
	t.Helper()

	acl, ok, err := fixture.state.ResolveACL(context.Background(), resource)
	if err != nil {
		t.Fatalf("ResolveACL(%s/%s) error = %v", resource.Type, resource.Name, err)
	}
	if !ok {
		t.Fatalf("ResolveACL(%s/%s) missing ACL", resource.Type, resource.Name)
	}
	return acl
}

// assertObjectACLEqualForAuthMatrixTest gives matrix failures a compact,
// resource-neutral diff instead of reusing family-specific ACL assertions.
func assertObjectACLEqualForAuthMatrixTest(t *testing.T, got, want authz.ACL) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ACL = %#v, want %#v", got, want)
	}
}
