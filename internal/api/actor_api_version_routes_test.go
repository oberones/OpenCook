package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/store/pg/pgtest"
)

func TestAPIVersionOneUserCreateKeySemantics(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	generatedBody := []byte(`{"username":"v1-generated","create_key":true}`)
	generated := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", generatedBody, "1")
	if generated.Code != http.StatusCreated {
		t.Fatalf("generated-key create status = %d, want %d, body = %s", generated.Code, http.StatusCreated, generated.Body.String())
	}
	generatedPayload := mustDecodeObject(t, generated)
	assertNoTopLevelKeyFields(t, generatedPayload)
	assertChefKey(t, generatedPayload, true)

	omittedBody := []byte(`{"username":"v1-omitted"}`)
	omitted := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", omittedBody, "1")
	if omitted.Code != http.StatusCreated {
		t.Fatalf("omitted-key create status = %d, want %d, body = %s", omitted.Code, http.StatusCreated, omitted.Body.String())
	}
	omittedPayload := mustDecodeObject(t, omitted)
	assertNoTopLevelKeyFields(t, omittedPayload)
	if _, ok := omittedPayload["chef_key"]; ok {
		t.Fatalf("omitted-key create response includes chef_key: %v", omittedPayload)
	}
	assertActorKeyListLength(t, router, "/users/v1-omitted/keys", 0)

	explicitBody := []byte(`{"username":"v1-explicit","create_key":false,"public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	explicit := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", explicitBody, "1")
	if explicit.Code != http.StatusCreated {
		t.Fatalf("explicit-key create status = %d, want %d, body = %s", explicit.Code, http.StatusCreated, explicit.Body.String())
	}
	explicitPayload := mustDecodeObject(t, explicit)
	assertNoTopLevelKeyFields(t, explicitPayload)
	chefKey := assertChefKey(t, explicitPayload, false)
	if strings.TrimSpace(chefKey["public_key"].(string)) != strings.TrimSpace(publicKeyPEM) {
		t.Fatalf("chef_key.public_key = %q, want explicit public key", chefKey["public_key"])
	}
	selfRead := serveSignedAPIVersionRequest(t, router, "v1-explicit", http.MethodGet, "/users/v1-explicit", nil, "1")
	if selfRead.Code != http.StatusOK {
		t.Fatalf("explicit-key self read status = %d, want %d, body = %s", selfRead.Code, http.StatusOK, selfRead.Body.String())
	}

	conflictBody := []byte(`{"username":"v1-conflict","create_key":true,"public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	conflict := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", conflictBody, "1")
	if conflict.Code != http.StatusBadRequest {
		t.Fatalf("conflicting key fields status = %d, want %d, body = %s", conflict.Code, http.StatusBadRequest, conflict.Body.String())
	}

	privateBody := []byte(`{"username":"v1-private","private_key":true}`)
	private := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", privateBody, "1")
	if private.Code != http.StatusBadRequest {
		t.Fatalf("private_key create status = %d, want %d, body = %s", private.Code, http.StatusBadRequest, private.Body.String())
	}
}

func TestAPIVersionGatesUserPublicKeyReadAndUpdate(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	body := []byte(`{"username":"v1-readable","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	create := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/users", body, "1")
	if create.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d, body = %s", create.Code, http.StatusCreated, create.Body.String())
	}

	v1Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/users/v1-readable", nil, "1")
	if v1Read.Code != http.StatusOK {
		t.Fatalf("v1 read status = %d, want %d, body = %s", v1Read.Code, http.StatusOK, v1Read.Body.String())
	}
	if _, ok := mustDecodeObject(t, v1Read)["public_key"]; ok {
		t.Fatalf("API v1 user read included top-level public_key")
	}

	v0Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/users/v1-readable", nil, "0")
	if v0Read.Code != http.StatusOK {
		t.Fatalf("v0 read status = %d, want %d, body = %s", v0Read.Code, http.StatusOK, v0Read.Body.String())
	}
	if got := mustDecodeObject(t, v0Read)["public_key"]; strings.TrimSpace(got.(string)) != strings.TrimSpace(publicKeyPEM) {
		t.Fatalf("API v0 user public_key = %q, want explicit key", got)
	}

	updateBody := []byte(`{"public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	update := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/users/v1-readable", updateBody, "1")
	if update.Code != http.StatusBadRequest {
		t.Fatalf("v1 key-mutation update status = %d, want %d, body = %s", update.Code, http.StatusBadRequest, update.Body.String())
	}
}

func TestAPIVersionOneClientCreateKeySemantics(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		DefaultOrganization: "ponyville",
	})
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	generatedBody := []byte(`{"name":"v1-generated","create_key":true}`)
	generated := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", generatedBody, "1")
	if generated.Code != http.StatusCreated {
		t.Fatalf("generated-key client create status = %d, want %d, body = %s", generated.Code, http.StatusCreated, generated.Body.String())
	}
	generatedPayload := mustDecodeObject(t, generated)
	assertNoTopLevelKeyFields(t, generatedPayload)
	assertChefKey(t, generatedPayload, true)

	defaultAliasBody := []byte(`{"name":"v1-default-alias","create_key":true}`)
	defaultAlias := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/clients", defaultAliasBody, "1")
	if defaultAlias.Code != http.StatusCreated {
		t.Fatalf("default-org client create status = %d, want %d, body = %s", defaultAlias.Code, http.StatusCreated, defaultAlias.Body.String())
	}
	defaultAliasPayload := mustDecodeObject(t, defaultAlias)
	assertNoTopLevelKeyFields(t, defaultAliasPayload)
	if chefKey := assertChefKey(t, defaultAliasPayload, true); chefKey["uri"] != "/clients/v1-default-alias/keys/default" {
		t.Fatalf("default-org chef_key.uri = %v, want default-org alias URI", chefKey["uri"])
	}

	omittedBody := []byte(`{"name":"v1-omitted"}`)
	omitted := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", omittedBody, "1")
	if omitted.Code != http.StatusCreated {
		t.Fatalf("omitted-key client create status = %d, want %d, body = %s", omitted.Code, http.StatusCreated, omitted.Body.String())
	}
	omittedPayload := mustDecodeObject(t, omitted)
	assertNoTopLevelKeyFields(t, omittedPayload)
	if _, ok := omittedPayload["chef_key"]; ok {
		t.Fatalf("omitted-key client create response includes chef_key: %v", omittedPayload)
	}
	assertActorKeyListLength(t, router, "/organizations/ponyville/clients/v1-omitted/keys", 0)

	explicitBody := []byte(`{"name":"v1-explicit","create_key":false,"public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	explicit := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", explicitBody, "1")
	if explicit.Code != http.StatusCreated {
		t.Fatalf("explicit-key client create status = %d, want %d, body = %s", explicit.Code, http.StatusCreated, explicit.Body.String())
	}
	explicitPayload := mustDecodeObject(t, explicit)
	assertNoTopLevelKeyFields(t, explicitPayload)
	chefKey := assertChefKey(t, explicitPayload, false)
	if strings.TrimSpace(chefKey["public_key"].(string)) != strings.TrimSpace(publicKeyPEM) {
		t.Fatalf("chef_key.public_key = %q, want explicit public key", chefKey["public_key"])
	}
	selfRead := serveSignedAPIVersionRequest(t, router, "v1-explicit", http.MethodGet, "/organizations/ponyville/clients/v1-explicit", nil, "1")
	if selfRead.Code != http.StatusOK {
		t.Fatalf("explicit-key client self read status = %d, want %d, body = %s", selfRead.Code, http.StatusOK, selfRead.Body.String())
	}

	conflictBody := []byte(`{"name":"v1-conflict","create_key":true,"public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	conflict := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", conflictBody, "1")
	if conflict.Code != http.StatusBadRequest {
		t.Fatalf("conflicting client key fields status = %d, want %d, body = %s", conflict.Code, http.StatusBadRequest, conflict.Body.String())
	}

	privateBody := []byte(`{"name":"v1-private","private_key":true}`)
	private := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", privateBody, "1")
	if private.Code != http.StatusBadRequest {
		t.Fatalf("private_key client create status = %d, want %d, body = %s", private.Code, http.StatusBadRequest, private.Body.String())
	}
}

func TestAPIVersionGatesClientPublicKeyReadAndUpdate(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		DefaultOrganization: "ponyville",
	})
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	body := []byte(`{"name":"v1-readable","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	create := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPost, "/organizations/ponyville/clients", body, "1")
	if create.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d, body = %s", create.Code, http.StatusCreated, create.Body.String())
	}

	v1Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/clients/v1-readable", nil, "1")
	if v1Read.Code != http.StatusOK {
		t.Fatalf("v1 client read status = %d, want %d, body = %s", v1Read.Code, http.StatusOK, v1Read.Body.String())
	}
	if _, ok := mustDecodeObject(t, v1Read)["public_key"]; ok {
		t.Fatalf("API v1 client read included top-level public_key")
	}

	v0Read := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodGet, "/organizations/ponyville/clients/v1-readable", nil, "0")
	if v0Read.Code != http.StatusOK {
		t.Fatalf("v0 client read status = %d, want %d, body = %s", v0Read.Code, http.StatusOK, v0Read.Body.String())
	}
	if got := mustDecodeObject(t, v0Read)["public_key"]; strings.TrimSpace(got.(string)) != strings.TrimSpace(publicKeyPEM) {
		t.Fatalf("API v0 client public_key = %q, want explicit key", got)
	}

	explicitUpdate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/organizations/ponyville/clients/v1-readable", []byte(`{"public_key":`+strconv.Quote(publicKeyPEM)+`}`), "1")
	if explicitUpdate.Code != http.StatusBadRequest {
		t.Fatalf("v1 explicit-org key-mutation update status = %d, want %d, body = %s", explicitUpdate.Code, http.StatusBadRequest, explicitUpdate.Body.String())
	}
	defaultUpdate := serveSignedAPIVersionRequest(t, router, "pivotal", http.MethodPut, "/clients/v1-readable", []byte(`{"create_key":false}`), "1")
	if defaultUpdate.Code != http.StatusBadRequest {
		t.Fatalf("v1 default-org key-mutation update status = %d, want %d, body = %s", defaultUpdate.Code, http.StatusBadRequest, defaultUpdate.Body.String())
	}
}

func TestAPIVersionOneActivePostgresClientKeyAuthenticatesAfterRestart(t *testing.T) {
	fixture := newActivePostgresBootstrapFixture(t, pgtest.NewState(pgtest.Seed{}))
	fixture.createOrganizationWithValidator("canterlot")

	body := []byte(`{"name":"v1-persisted","create_key":true}`)
	create := serveSignedAPIVersionRequest(t, fixture.router, "pivotal", http.MethodPost, "/organizations/canterlot/clients", body, "1")
	if create.Code != http.StatusCreated {
		t.Fatalf("active Postgres client create status = %d, want %d, body = %s", create.Code, http.StatusCreated, create.Body.String())
	}
	payload := mustDecodeObject(t, create)
	assertNoTopLevelKeyFields(t, payload)
	assertChefKey(t, payload, true)
	client := newSignedClientRequestorFromClientCreatePayload(t, "canterlot", "v1-persisted", payload)

	restarted := fixture.restart()
	readReq := client.newSignedJSONRequestWithServerAPIVersion(t, http.MethodGet, "/organizations/canterlot/clients/v1-persisted", nil, "1")
	readRec := httptest.NewRecorder()
	restarted.router.ServeHTTP(readRec, readReq)
	if readRec.Code != http.StatusOK {
		t.Fatalf("persisted signed client read status = %d, want %d, body = %s", readRec.Code, http.StatusOK, readRec.Body.String())
	}
	if _, ok := mustDecodeObject(t, readRec)["public_key"]; ok {
		t.Fatalf("API v1 persisted client read included top-level public_key")
	}
}

func serveSignedAPIVersionRequest(t *testing.T, router http.Handler, userID, method, path string, body []byte, serverAPIVersion string) *httptest.ResponseRecorder {
	t.Helper()

	req := newSignedJSONRequestAsWithServerAPIVersion(t, userID, method, path, body, serverAPIVersion)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func mustDecodeObject(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(object) error = %v; body = %s", err, rec.Body.String())
	}
	return payload
}

func assertNoTopLevelKeyFields(t *testing.T, payload map[string]any) {
	t.Helper()

	if _, ok := payload["private_key"]; ok {
		t.Fatalf("response includes top-level private_key: %v", payload)
	}
	if _, ok := payload["public_key"]; ok {
		t.Fatalf("response includes top-level public_key: %v", payload)
	}
}

func assertChefKey(t *testing.T, payload map[string]any, wantPrivate bool) map[string]any {
	t.Helper()

	chefKey, ok := payload["chef_key"].(map[string]any)
	if !ok {
		t.Fatalf("response missing chef_key: %v", payload)
	}
	if chefKey["name"] != "default" {
		t.Fatalf("chef_key.name = %v, want default", chefKey["name"])
	}
	if chefKey["expiration_date"] != "infinity" {
		t.Fatalf("chef_key.expiration_date = %v, want infinity", chefKey["expiration_date"])
	}
	if !strings.Contains(chefKey["public_key"].(string), "BEGIN PUBLIC KEY") {
		t.Fatalf("chef_key.public_key missing PEM: %v", chefKey)
	}
	if _, ok := chefKey["private_key"]; ok != wantPrivate {
		t.Fatalf("chef_key.private_key present = %v, want %v: %v", ok, wantPrivate, chefKey)
	}
	return chefKey
}

func assertActorKeyListLength(t *testing.T, router http.Handler, path string, want int) {
	t.Helper()

	req := newSignedJSONRequestAsWithServerAPIVersion(t, "pivotal", http.MethodGet, path, nil, "1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("key list %s status = %d, want %d, body = %s", path, rec.Code, http.StatusOK, rec.Body.String())
	}

	var payload []map[string]any
	if err := json.NewDecoder(bytes.NewReader(rec.Body.Bytes())).Decode(&payload); err != nil {
		t.Fatalf("json.Decode(key list) error = %v; body = %s", err, rec.Body.String())
	}
	if len(payload) != want {
		t.Fatalf("key list %s length = %d, want %d: %v", path, len(payload), want, payload)
	}
}
