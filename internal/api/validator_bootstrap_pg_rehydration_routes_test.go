package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestActivePostgresValidatorRegistrationRehydratesAcrossRestarts(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	validator := fixture.createOrganizationWithValidator("canterlot")

	afterOrgRestart := fixture.restart()
	client := createClientAsValidator(t, afterOrgRestart.router, validator, "/organizations/canterlot/clients", "twilight")
	assertSignedClientCanReadSelf(t, afterOrgRestart.router, client, "/organizations/canterlot/clients/twilight")
	assertClientsGroupMemberCount(t, afterOrgRestart.router, "canterlot", "canterlot-validator", 1)
	assertClientsGroupMemberCount(t, afterOrgRestart.router, "canterlot", "twilight", 1)

	afterClientRestart := afterOrgRestart.restart()
	assertClientReadPayload(t, afterClientRestart.router, "/organizations/canterlot/clients/twilight", "twilight", false)
	assertSignedClientCanReadSelf(t, afterClientRestart.router, client, "/organizations/canterlot/clients/twilight")

	keys := readSignedClientKeyList(t, afterClientRestart.router, client, "/organizations/canterlot/clients/twilight/keys")
	if len(keys) != 1 || keys[0]["name"] != "default" || keys[0]["expired"] != false {
		t.Fatalf("rehydrated client key list = %v, want active default key", keys)
	}

	acl := readAdminACLJSON(t, afterClientRestart.router, "/organizations/canterlot/clients/twilight/_acl")
	assertACLPermission(t, acl, "create", []string{"pivotal", "twilight"}, []string{"admins"})
	assertACLPermission(t, acl, "read", []string{"pivotal", "twilight"}, []string{"admins", "users"})
	assertACLPermission(t, acl, "update", []string{"pivotal", "twilight"}, []string{"admins"})
	assertACLPermission(t, acl, "delete", []string{"pivotal", "twilight"}, []string{"admins", "users"})
	assertACLPermission(t, acl, "grant", []string{"pivotal", "twilight"}, []string{"admins"})

	assertClientsGroupMemberCount(t, afterClientRestart.router, "canterlot", "canterlot-validator", 1)
	assertClientsGroupMemberCount(t, afterClientRestart.router, "canterlot", "twilight", 1)
	assertClientSearchRow(t, afterClientRestart.router, "/organizations/canterlot/search/client?q=name:twilight", "/organizations/canterlot/search/client", "twilight", "canterlot")

	afterRepeatedRestart := afterClientRestart.restart().restart()
	assertClientReadPayload(t, afterRepeatedRestart.router, "/organizations/canterlot/clients/twilight", "twilight", false)
	assertSignedClientCanReadSelf(t, afterRepeatedRestart.router, client, "/organizations/canterlot/clients/twilight")
	assertClientsGroupMemberCount(t, afterRepeatedRestart.router, "canterlot", "canterlot-validator", 1)
	assertClientsGroupMemberCount(t, afterRepeatedRestart.router, "canterlot", "twilight", 1)
}

func TestValidatorBootstrapRegistrationInMemoryModeRemainsRuntimeCompatible(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")
	client := createClientAsValidator(t, router, validator, "/organizations/canterlot/clients", "twilight")

	assertClientReadPayload(t, router, "/organizations/canterlot/clients/twilight", "twilight", false)
	assertSignedClientCanReadSelf(t, router, client, "/organizations/canterlot/clients/twilight")
	assertClientsGroupMemberCount(t, router, "canterlot", "canterlot-validator", 1)
	assertClientsGroupMemberCount(t, router, "canterlot", "twilight", 1)

	keys := readSignedClientKeyList(t, router, client, "/organizations/canterlot/clients/twilight/keys")
	if len(keys) != 1 || keys[0]["uri"] != "/organizations/canterlot/clients/twilight/keys/default" {
		t.Fatalf("in-memory client key list = %v, want explicit-org default key URI", keys)
	}

	req := client.newSignedJSONRequest(t, http.MethodGet, "/organizations/canterlot/containers/data", nil)
	rec := httptestRecorder(t, router, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("in-memory registered client container read status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func httptestRecorder(t *testing.T, router http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}
