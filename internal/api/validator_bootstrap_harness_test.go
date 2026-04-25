package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestValidatorBootstrapHarnessCreatesOrgAndSignsExplicitOrgRequest(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	req := validator.newSignedJSONRequest(t, http.MethodGet, "/organizations/canterlot/containers/data", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("validator signed explicit-org request status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestValidatorBootstrapHarnessSignsConfiguredDefaultOrgRequest(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		DefaultOrganization: "canterlot",
	})
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	req := validator.newSignedJSONRequest(t, http.MethodGet, "/nodes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("validator signed default-org request status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestValidatorBootstrapHarnessBuildsRegisteredClientSignerFromCreateResponse(t *testing.T) {
	router := newTestRouter(t)

	body := []byte(`{"name":"twilight","create_key":true}`)
	createReq := newSignedJSONRequestAs(t, "pivotal", http.MethodPost, "/organizations/ponyville/clients", body)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	client := newSignedClientRequestorFromClientCreateResponse(t, "ponyville", "twilight", createRec)
	getReq := client.newSignedJSONRequest(t, http.MethodGet, "/organizations/ponyville/clients/twilight", nil)
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("new client signed self-read status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestActivePostgresValidatorBootstrapHarnessRehydratesValidatorKey(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	validator := fixture.createOrganizationWithValidator("canterlot")

	restarted := fixture.restart()
	req := validator.newSignedJSONRequest(t, http.MethodGet, "/organizations/canterlot/containers/data", nil)
	rec := httptest.NewRecorder()
	restarted.router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("rehydrated validator signed request status = %d, want %d, body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestActivePostgresValidatorBootstrapHarnessBuildsRegisteredClientSigner(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("canterlot")

	body := []byte(`{"name":"twilight","create_key":true}`)
	createReq := httptest.NewRequest(http.MethodPost, "/organizations/canterlot/clients", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "pivotal", "", http.MethodPost, "/organizations/canterlot/clients", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	fixture.router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("active postgres create client status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	client := newSignedClientRequestorFromClientCreateResponse(t, "canterlot", "twilight", createRec)
	restarted := fixture.restart()
	getReq := client.newSignedJSONRequest(t, http.MethodGet, "/organizations/canterlot/clients/twilight", nil)
	getRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("rehydrated new client signed self-read status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}
