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

func TestValidatorBootstrapHarnessSupportsRegisteredClientV13NodeLifecycle(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	createClientBody := []byte(`{"name":"testnode2"}`)
	createClientReq := validator.newSignedJSONRequestWithSigning(t, http.MethodPost, "/organizations/canterlot/clients", createClientBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "0")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)
	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("validator create client status = %d, want %d, body = %s", createClientRec.Code, http.StatusCreated, createClientRec.Body.String())
	}

	client := newSignedClientRequestorFromClientCreateResponse(t, "canterlot", "testnode2", createClientRec)

	getReq := client.newSignedJSONRequestWithSigning(t, http.MethodGet, "/organizations/canterlot/nodes/testnode2", nil, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusNotFound {
		t.Fatalf("registered client missing node read status = %d, want %d, body = %s", getRec.Code, http.StatusNotFound, getRec.Body.String())
	}

	createNode := nodePayloadExpectation{
		Name:            "testnode2",
		ChefEnvironment: "_default",
		Override:        map[string]any{},
		Normal:          map[string]any{},
		Default:         map[string]any{},
		Automatic: map[string]any{
			"fqdn":     "testnode2",
			"hostname": "testnode2",
		},
		RunList: []string{},
	}
	createNodeBody := mustMarshalAPIVersionNodePayload(t, createNode)
	createNodeReq := client.newSignedJSONRequestWithSigning(t, http.MethodPost, "/organizations/canterlot/nodes", createNodeBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2")
	createNodeRec := httptest.NewRecorder()
	router.ServeHTTP(createNodeRec, createNodeReq)
	if createNodeRec.Code != http.StatusCreated {
		t.Fatalf("registered client create node status = %d, want %d, body = %s", createNodeRec.Code, http.StatusCreated, createNodeRec.Body.String())
	}

	updateNode := nodePayloadExpectation{
		Name:            "testnode2",
		ChefEnvironment: "_default",
		Override:        map[string]any{},
		Normal: map[string]any{
			"team": "friendship",
		},
		Default: map[string]any{},
		Automatic: map[string]any{
			"fqdn":     "testnode2",
			"hostname": "testnode2",
			"platform": "linux",
		},
		RunList: []string{},
	}
	updateNodeBody := mustMarshalAPIVersionNodePayload(t, updateNode)
	updateNodeReq := client.newSignedJSONRequestWithSigning(t, http.MethodPut, "/organizations/canterlot/nodes/testnode2", updateNodeBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2")
	updateNodeRec := httptest.NewRecorder()
	router.ServeHTTP(updateNodeRec, updateNodeReq)
	if updateNodeRec.Code != http.StatusOK {
		t.Fatalf("registered client update node status = %d, want %d, body = %s", updateNodeRec.Code, http.StatusOK, updateNodeRec.Body.String())
	}

	updatePayload := mustDecodeObject(t, updateNodeRec)
	if updatePayload["name"] != "testnode2" {
		t.Fatalf("updated node name = %v, want %q", updatePayload["name"], "testnode2")
	}
	assertMapFieldEqual(t, updatePayload, "normal", updateNode.Normal)
	assertMapFieldEqual(t, updatePayload, "automatic", updateNode.Automatic)
}

func TestValidatorBootstrapHarnessAuthenticatesExplicitOrgUnknownReportingRoute(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	createClientBody := []byte(`{"name":"testnode2"}`)
	createClientReq := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", createClientBody)
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)
	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("validator create client status = %d, want %d, body = %s", createClientRec.Code, http.StatusCreated, createClientRec.Body.String())
	}

	client := newSignedClientRequestorFromClientCreateResponse(t, "canterlot", "testnode2", createClientRec)
	reportReq := client.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/reports/nodes/testnode2/runs", []byte(`{"action":"start"}`))
	reportRec := httptest.NewRecorder()
	router.ServeHTTP(reportRec, reportReq)
	if reportRec.Code != http.StatusNotFound {
		t.Fatalf("report route status = %d, want %d, body = %s", reportRec.Code, http.StatusNotFound, reportRec.Body.String())
	}
}

func TestValidatorBootstrapHarnessAuthenticatesExplicitOrgUnknownRequiredRecipeRoute(t *testing.T) {
	router := newTestRouter(t)
	validator := createOrganizationAndCaptureValidator(t, router, "canterlot")

	createClientBody := []byte(`{"name":"testnode2"}`)
	createClientReq := validator.newSignedJSONRequest(t, http.MethodPost, "/organizations/canterlot/clients", createClientBody)
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)
	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("validator create client status = %d, want %d, body = %s", createClientRec.Code, http.StatusCreated, createClientRec.Body.String())
	}

	client := newSignedClientRequestorFromClientCreateResponse(t, "canterlot", "testnode2", createClientRec)
	requiredRecipeReq := client.newSignedJSONRequest(t, http.MethodGet, "/organizations/canterlot/required_recipe", nil)
	requiredRecipeRec := httptest.NewRecorder()
	router.ServeHTTP(requiredRecipeRec, requiredRecipeReq)
	if requiredRecipeRec.Code != http.StatusNotFound {
		t.Fatalf("required_recipe route status = %d, want %d, body = %s", requiredRecipeRec.Code, http.StatusNotFound, requiredRecipeRec.Body.String())
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
