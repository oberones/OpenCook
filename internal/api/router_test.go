package api

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"log"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/compat"
	"github.com/oberones/OpenCook/internal/config"
	"github.com/oberones/OpenCook/internal/search"
	"github.com/oberones/OpenCook/internal/store/pg"
	"github.com/oberones/OpenCook/internal/version"
)

const testSigningKeyPEM = `-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA49TA0y81ps0zxkOpmf5V4/c4IeR5yVyQFpX3JpxO4TquwnRh
8VSUhrw8kkTLmB3cS39Db+3HadvhoqCEbqPE6915kXSuk/cWIcNozujLK7tkuPEy
YVsyTioQAddSdfe+8EhQVf3oHxaKmUd6waXrWqYCnhxgOjxocenREYNhZ/OETIei
PbOku47vB4nJK/0GhKBytL2XnsRgfKgDxf42BqAi1jglIdeq8lAWZNF9TbNBU21A
O1iuT7Pm6LyQujhggPznR5FJhXKRUARXBJZawxpGV4dGtdcahwXNE4601aXPra+x
PcRd2puCNoEDBzgVuTSsLYeKBDMSfs173W1QYwIDAQABAoIBAGF05q7vqOGbMaSD
2Q7YbuE/JTHKTBZIlBI1QC2x+0P5GDxyEFttNMOVzcs7xmNhkpRw8eX1LrInrpMk
WsIBKAFFEfWYlf0RWtRChJjNl+szE9jQxB5FJnWtJH/FHa78tR6PsF24aQyzVcJP
g0FGujBihwgfV0JSCNOBkz8MliQihjQA2i8PGGmo4R4RVzGfxYKTIq9vvRq/+QEa
Q4lpVLoBqnENpnY/9PTl6JMMjW2b0spbLjOPVwDaIzXJ0dChjNXo15K5SHI5mALJ
I5gN7ODGb8PKUf4619ez194FXq+eob5YJdilTFKensIUvt3YhP1ilGMM+Chi5Vi/
/RCTw3ECgYEA9jTw4wv9pCswZ9wbzTaBj9yZS3YXspGg26y6Ohq3ZmvHz4jlT6uR
xK+DDcUiK4072gci8S4Np0fIVS7q6ivqcOdzXPrTF5/j+MufS32UrBbUTPiM1yoO
ECcy+1szl/KoLEV09bghPbvC58PFSXV71evkaTETYnA/F6RK12lEepcCgYEA7OSy
bsMrGDVU/MKJtwqyGP9ubA53BorM4Pp9VVVSCrGGVhb9G/XNsjO5wJC8J30QAo4A
s59ZzCpyNRy046AB8jwRQuSwEQbejSdeNgQGXhZ7aIVUtuDeFFdaIz/zjVgxsfj4
DPOuzieMmJ2MLR4F71ocboxNoDI7xruPSE8dDhUCgYA3vx732cQxgtHwAkeNPJUz
dLiE/JU7CnxIoSB9fYUfPLI+THnXgzp7NV5QJN2qzMzLfigsQcg3oyo6F2h7Yzwv
GkjlualIRRzCPaCw4Btkp7qkPvbs1QngIHALt8fD1N69P3DPHkTwjG4COjKWgnJq
qoHKS6Fe/ZlbigikI6KsuwKBgQCTlSLoyGRHr6oj0hqz01EDK9ciMJzMkZp0Kvn8
OKxlBxYW+jlzut4MQBdgNYtS2qInxUoAnaz2+hauqhSzntK3k955GznpUatCqx0R
b857vWviwPX2/P6+E3GPdl8IVsKXCvGWOBZWTuNTjQtwbDzsUepWoMgXnlQJSn5I
YSlLxQKBgQD16Gw9kajpKlzsPa6XoQeGmZALT6aKWJQlrKtUQIrsIWM0Z6eFtX12
2jjHZ0awuCQ4ldqwl8IfRogWMBkHOXjTPVK0YKWWlxMpD/5+bGPARa5fir8O1Zpo
Y6S6MeZ69Rp89ma4ttMZ+kwi1+XyHqC/dlcVRW42Zl5Dc7BALRlJjQ==
-----END RSA PRIVATE KEY-----`

func TestUsersEndpointRequiresAuthentication(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestUsersEndpointAcceptsSignedRequest(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/users", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	requestor := payload["requestor"].(map[string]any)
	if requestor["name"] != "pivotal" {
		t.Fatalf("requestor.name = %v, want %q", requestor["name"], "pivotal")
	}
}

func TestUsersEndpointCollectionRequiresCollectionReadAuthz(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/users", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestUsersEndpointCollectionAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/users/", nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/users/", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["users"] == nil {
		t.Fatalf("users payload missing for trailing slash collection path")
	}
}

func TestOrgClientsEndpointUsesOrgScopedLookup(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/ponyville/clients", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if _, ok := payload["org-validator"]; !ok {
		t.Fatalf("clients did not include seeded org client, got %v", payload)
	}
}

func TestOrgClientsEndpointCollectionAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/ponyville/clients/", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if _, ok := payload["org-validator"]; !ok {
		t.Fatalf("clients payload missing seeded org client for trailing slash collection path: %v", payload)
	}
}

func TestClientsEndpointUsesDefaultOrganizationAlias(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"name":"twilight","create_key":true}`)
	createReq := httptest.NewRequest(http.MethodPost, "/clients", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/clients", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create client) error = %v", err)
	}
	if createPayload["uri"] != "/clients/twilight" {
		t.Fatalf("create client uri = %v, want %q", createPayload["uri"], "/clients/twilight")
	}
	chefKey, ok := createPayload["chef_key"].(map[string]any)
	if !ok {
		t.Fatalf("chef_key payload missing: %v", createPayload)
	}
	if chefKey["uri"] != "/clients/twilight/keys/default" {
		t.Fatalf("chef_key uri = %v, want %q", chefKey["uri"], "/clients/twilight/keys/default")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/clients/twilight", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/clients/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get client status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get client) error = %v", err)
	}
	if getPayload["orgname"] != "ponyville" {
		t.Fatalf("client orgname = %v, want %q", getPayload["orgname"], "ponyville")
	}
	if getPayload["json_class"] != "Chef::ApiClient" {
		t.Fatalf("client json_class = %v, want %q", getPayload["json_class"], "Chef::ApiClient")
	}

	listKeysReq := httptest.NewRequest(http.MethodGet, "/clients/twilight/keys", nil)
	applySignedHeaders(t, listKeysReq, "silent-bob", "", http.MethodGet, "/clients/twilight/keys", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	listKeysRec := httptest.NewRecorder()
	router.ServeHTTP(listKeysRec, listKeysReq)

	if listKeysRec.Code != http.StatusOK {
		t.Fatalf("list client keys status = %d, want %d, body = %s", listKeysRec.Code, http.StatusOK, listKeysRec.Body.String())
	}

	var keysPayload []map[string]any
	if err := json.Unmarshal(listKeysRec.Body.Bytes(), &keysPayload); err != nil {
		t.Fatalf("json.Unmarshal(client keys) error = %v", err)
	}
	if len(keysPayload) != 1 {
		t.Fatalf("client keys len = %d, want 1 (%v)", len(keysPayload), keysPayload)
	}
	if keysPayload[0]["uri"] != "/clients/twilight/keys/default" {
		t.Fatalf("client key uri = %v, want %q", keysPayload[0]["uri"], "/clients/twilight/keys/default")
	}

}

func TestClientsEndpointDeleteMissingClientReturnsClientSpecificNotFound(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodDelete, "/clients/missing", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodDelete, "/clients/missing", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusNotFound, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(delete missing client) error = %v", err)
	}
	if payload["message"] != "client not found" {
		t.Fatalf("message = %v, want %q", payload["message"], "client not found")
	}
}

func TestUsersEndpointCreatesUserWithPrivateKey(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	req := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/users", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	privateKey, _ := payload["private_key"].(string)
	if !strings.Contains(privateKey, "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("private_key missing PEM payload: %v", payload["private_key"])
	}

	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeaders(t, getReq, "pivotal", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusOK)
	}
}

func TestUsersEndpointRejectsTrailingJSONData(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"username":"rainbow"}{"username":"pinkie"}`)
	req := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/users", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["error"] != "invalid_json" {
		t.Fatalf("error = %v, want %q", payload["error"], "invalid_json")
	}
}

func TestUserKeysEndpointListsDefaultKey(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "pivotal", "", http.MethodPost, "/users", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", createRec.Code, http.StatusCreated)
	}

	req := httptest.NewRequest(http.MethodGet, "/users/rainbow/keys/", nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/users/rainbow/keys/", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(payload) != 1 {
		t.Fatalf("len(payload) = %d, want %d", len(payload), 1)
	}
	if payload[0]["name"] != "default" {
		t.Fatalf("name = %v, want %q", payload[0]["name"], "default")
	}
	if payload[0]["uri"] != "/users/rainbow/keys/default" {
		t.Fatalf("uri = %v, want %q", payload[0]["uri"], "/users/rainbow/keys/default")
	}
	if payload[0]["expired"] != false {
		t.Fatalf("expired = %v, want false", payload[0]["expired"])
	}
}

func TestUserKeyEndpointReturnsPublicKey(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	body := []byte(`{"username":"rainbow","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	createReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "pivotal", "", http.MethodPost, "/users", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", createRec.Code, http.StatusCreated)
	}

	req := httptest.NewRequest(http.MethodGet, "/users/rainbow/keys/default", nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/users/rainbow/keys/default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["name"] != "default" {
		t.Fatalf("name = %v, want %q", payload["name"], "default")
	}
	if strings.TrimSpace(payload["public_key"].(string)) != strings.TrimSpace(publicKeyPEM) {
		t.Fatalf("public_key = %v, want seeded PEM", payload["public_key"])
	}
	if payload["expiration_date"] != "infinity" {
		t.Fatalf("expiration_date = %v, want %q", payload["expiration_date"], "infinity")
	}
}

func TestUserKeysEndpointCreatesGeneratedKeyAndAuthenticates(t *testing.T) {
	router := newTestRouter(t)

	createUserBody := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","create_key":true,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/users/rainbow/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "pivotal", "", http.MethodPost, "/users/rainbow/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}
	if createKeyRec.Header().Get("Location") != "/users/rainbow/keys/alt" {
		t.Fatalf("Location = %q, want %q", createKeyRec.Header().Get("Location"), "/users/rainbow/keys/alt")
	}

	var payload map[string]any
	if err := json.Unmarshal(createKeyRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(payload["private_key"].(string)))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeadersWithPrivateKey(t, getReq, privateKey, "rainbow", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestUserKeysEndpointRejectsExpiredKeyAuthentication(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createUserBody := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"expired","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"2012-01-01T00:00:00Z"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/users/rainbow/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "pivotal", "", http.MethodPost, "/users/rainbow/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/users/rainbow/keys", nil)
	applySignedHeaders(t, listReq, "pivotal", "", http.MethodGet, "/users/rainbow/keys", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list keys status = %d, want %d", listRec.Code, http.StatusOK)
	}

	var listPayload []map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	foundExpired := false
	for _, key := range listPayload {
		if key["name"] == "expired" {
			foundExpired = true
			if key["expired"] != true {
				t.Fatalf("expired key flag = %v, want true", key["expired"])
			}
		}
	}
	if !foundExpired {
		t.Fatalf("expired key missing from list payload: %v", listPayload)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeaders(t, getReq, "rainbow", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusUnauthorized {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusUnauthorized)
	}
}

func TestUserKeysEndpointDeleteRemovesAuthentication(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)
	createUserBody := []byte(`{"username":"rainbow","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/users/rainbow/keys/default", nil)
	applySignedHeaders(t, deleteReq, "rainbow", "", http.MethodDelete, "/users/rainbow/keys/default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d", deleteRec.Code, http.StatusOK)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeaders(t, getReq, "rainbow", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusUnauthorized {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusUnauthorized)
	}
}

func TestUserKeysEndpointUpdateRenamesKey(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createUserBody := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/users/rainbow/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "pivotal", "", http.MethodPost, "/users/rainbow/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	updateBody := []byte(`{"name":"renamed","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"2049-12-24T21:00:00Z"}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/users/rainbow/keys/alt", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "pivotal", "", http.MethodPut, "/users/rainbow/keys/alt", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusCreated {
		t.Fatalf("update status = %d, want %d", updateRec.Code, http.StatusCreated)
	}
	if updateRec.Header().Get("Location") != "/users/rainbow/keys/renamed" {
		t.Fatalf("Location = %q, want %q", updateRec.Header().Get("Location"), "/users/rainbow/keys/renamed")
	}

	var payload map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload["name"] != "renamed" {
		t.Fatalf("name = %v, want %q", payload["name"], "renamed")
	}
	if payload["expiration_date"] != "2049-12-24T21:00:00Z" {
		t.Fatalf("expiration_date = %v, want %q", payload["expiration_date"], "2049-12-24T21:00:00Z")
	}

	oldReq := httptest.NewRequest(http.MethodGet, "/users/rainbow/keys/alt", nil)
	applySignedHeaders(t, oldReq, "pivotal", "", http.MethodGet, "/users/rainbow/keys/alt", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	oldRec := httptest.NewRecorder()
	router.ServeHTTP(oldRec, oldReq)

	if oldRec.Code != http.StatusNotFound {
		t.Fatalf("old key GET status = %d, want %d", oldRec.Code, http.StatusNotFound)
	}
}

func TestUserKeysEndpointUpdateCreateKeyAuthenticates(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createUserBody := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/users/rainbow/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "pivotal", "", http.MethodPost, "/users/rainbow/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	updateBody := []byte(`{"create_key":true}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/users/rainbow/keys/alt", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "pivotal", "", http.MethodPut, "/users/rainbow/keys/alt", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d, want %d", updateRec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(payload["private_key"].(string)))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/users/rainbow", nil)
	applySignedHeadersWithPrivateKey(t, getReq, privateKey, "rainbow", "", http.MethodGet, "/users/rainbow", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestUserKeysEndpointRejectsEmptyUpdateBody(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createUserBody := []byte(`{"username":"rainbow","display_name":"Rainbow Dash"}`)
	createUserReq := httptest.NewRequest(http.MethodPost, "/users", bytes.NewReader(createUserBody))
	applySignedHeaders(t, createUserReq, "pivotal", "", http.MethodPost, "/users", createUserBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createUserRec := httptest.NewRecorder()
	router.ServeHTTP(createUserRec, createUserReq)

	if createUserRec.Code != http.StatusCreated {
		t.Fatalf("create user status = %d, want %d", createUserRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/users/rainbow/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "pivotal", "", http.MethodPost, "/users/rainbow/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/users/rainbow/keys/alt", bytes.NewReader([]byte(`{}`)))
	applySignedHeaders(t, updateReq, "pivotal", "", http.MethodPut, "/users/rainbow/keys/alt", []byte(`{}`), signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("update status = %d, want %d", updateRec.Code, http.StatusBadRequest)
	}
}

func TestOrganizationsEndpointCreatesBootstrapArtifacts(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"name":"canterlot","full_name":"Canterlot","org_type":"Business"}`)
	req := httptest.NewRequest(http.MethodPost, "/organizations", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/organizations", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["clientname"] != "canterlot-validator" {
		t.Fatalf("clientname = %v, want %q", payload["clientname"], "canterlot-validator")
	}
	if !strings.Contains(payload["private_key"].(string), "BEGIN RSA PRIVATE KEY") {
		t.Fatalf("private_key missing PEM payload")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot", nil)
	applySignedHeaders(t, getReq, "pivotal", "", http.MethodGet, "/organizations/canterlot", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusOK)
	}
}

func TestOrganizationBootstrapExposesGroupsContainersAndACLs(t *testing.T) {
	router := newTestRouter(t)
	createOrgForTest(t, router, "canterlot")

	groupReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot/groups", nil)
	applySignedHeaders(t, groupReq, "pivotal", "", http.MethodGet, "/organizations/canterlot/groups", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	groupRec := httptest.NewRecorder()
	router.ServeHTTP(groupRec, groupReq)

	if groupRec.Code != http.StatusOK {
		t.Fatalf("groups status = %d, want %d", groupRec.Code, http.StatusOK)
	}

	var groups map[string]any
	if err := json.Unmarshal(groupRec.Body.Bytes(), &groups); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if _, ok := groups["admins"]; !ok {
		t.Fatalf("groups missing admins entry: %v", groups)
	}

	containerReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot/containers", nil)
	applySignedHeaders(t, containerReq, "pivotal", "", http.MethodGet, "/organizations/canterlot/containers", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	containerRec := httptest.NewRecorder()
	router.ServeHTTP(containerRec, containerReq)

	if containerRec.Code != http.StatusOK {
		t.Fatalf("containers status = %d, want %d", containerRec.Code, http.StatusOK)
	}

	var containers map[string]any
	if err := json.Unmarshal(containerRec.Body.Bytes(), &containers); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if _, ok := containers["clients"]; !ok {
		t.Fatalf("containers missing clients entry: %v", containers)
	}

	aclReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot/containers/clients/_acl", nil)
	applySignedHeaders(t, aclReq, "pivotal", "", http.MethodGet, "/organizations/canterlot/containers/clients/_acl", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	aclRec := httptest.NewRecorder()
	router.ServeHTTP(aclRec, aclReq)

	if aclRec.Code != http.StatusOK {
		t.Fatalf("acl status = %d, want %d", aclRec.Code, http.StatusOK)
	}

	var acl map[string]map[string]any
	if err := json.Unmarshal(aclRec.Body.Bytes(), &acl); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	readGroups := stringSliceFromAny(t, acl["read"]["groups"])
	if !containsString(readGroups, "users") || !containsString(readGroups, "admins") {
		t.Fatalf("read groups = %v, want admins/users", readGroups)
	}
}

func TestOrgClientsEndpointCreatesBackedClient(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"name":"twilight"}`)
	req := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(body))
	applySignedHeaders(t, req, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusCreated)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/twilight", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/organizations/ponyville/clients/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusOK)
	}
}

func TestOrgClientKeysEndpointListsDefaultKey(t *testing.T) {
	router := newTestRouter(t)
	body := []byte(`{"name":"twilight"}`)
	createReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want %d", createRec.Code, http.StatusCreated)
	}

	req := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/twilight/keys", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/ponyville/clients/twilight/keys", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(payload) != 1 {
		t.Fatalf("len(payload) = %d, want %d", len(payload), 1)
	}
	if payload[0]["uri"] != "/organizations/ponyville/clients/twilight/keys/default" {
		t.Fatalf("uri = %v, want %q", payload[0]["uri"], "/organizations/ponyville/clients/twilight/keys/default")
	}
}

func TestOrgClientKeyEndpointReturnsPublicKey(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/org-validator/keys/default", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/organizations/ponyville/clients/org-validator/keys/default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["name"] != "default" {
		t.Fatalf("name = %v, want %q", payload["name"], "default")
	}
	if payload["public_key"] == "" {
		t.Fatalf("public_key missing from payload: %v", payload)
	}
	if payload["expiration_date"] != "infinity" {
		t.Fatalf("expiration_date = %v, want %q", payload["expiration_date"], "infinity")
	}
}

func TestOrgClientKeysEndpointCreatesGeneratedKeyAndAuthenticates(t *testing.T) {
	router := newTestRouter(t)
	createClientBody := []byte(`{"name":"twilight"}`)
	createClientReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(createClientBody))
	applySignedHeaders(t, createClientReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", createClientBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)

	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d", createClientRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","create_key":true,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients/twilight/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients/twilight/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	var payload map[string]any
	if err := json.Unmarshal(createKeyRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(payload["private_key"].(string)))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/twilight", nil)
	applySignedHeadersWithPrivateKey(t, getReq, privateKey, "twilight", "ponyville", http.MethodGet, "/organizations/ponyville/clients/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestOrgClientKeysEndpointDeleteRemovesAuthentication(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createClientBody := []byte(`{"name":"twilight","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	createClientReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(createClientBody))
	applySignedHeaders(t, createClientReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", createClientBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)

	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d", createClientRec.Code, http.StatusCreated)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/organizations/ponyville/clients/twilight/keys/default", nil)
	applySignedHeaders(t, deleteReq, "twilight", "ponyville", http.MethodDelete, "/organizations/ponyville/clients/twilight/keys/default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d", deleteRec.Code, http.StatusOK)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/twilight", nil)
	applySignedHeaders(t, getReq, "twilight", "ponyville", http.MethodGet, "/organizations/ponyville/clients/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusUnauthorized {
		t.Fatalf("GET status = %d, want %d", getRec.Code, http.StatusUnauthorized)
	}
}

func TestOrgClientKeysEndpointUpdateRenamesKey(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createClientBody := []byte(`{"name":"twilight","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	createClientReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(createClientBody))
	applySignedHeaders(t, createClientReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", createClientBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)

	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d", createClientRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients/twilight/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients/twilight/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	updateBody := []byte(`{"name":"renamed","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"2049-12-24T21:00:00Z"}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/organizations/ponyville/clients/twilight/keys/alt", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "silent-bob", "", http.MethodPut, "/organizations/ponyville/clients/twilight/keys/alt", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusCreated {
		t.Fatalf("update status = %d, want %d", updateRec.Code, http.StatusCreated)
	}
	if updateRec.Header().Get("Location") != "/organizations/ponyville/clients/twilight/keys/renamed" {
		t.Fatalf("Location = %q, want %q", updateRec.Header().Get("Location"), "/organizations/ponyville/clients/twilight/keys/renamed")
	}
}

func TestOrgClientKeysEndpointUpdateCreateKeyAuthenticates(t *testing.T) {
	router := newTestRouter(t)
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &mustParsePrivateKey(t).PublicKey)

	createClientBody := []byte(`{"name":"twilight","public_key":` + strconv.Quote(publicKeyPEM) + `}`)
	createClientReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(createClientBody))
	applySignedHeaders(t, createClientReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", createClientBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)

	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d", createClientRec.Code, http.StatusCreated)
	}

	createKeyBody := []byte(`{"name":"alt","public_key":` + strconv.Quote(publicKeyPEM) + `,"expiration_date":"infinity"}`)
	createKeyReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients/twilight/keys", bytes.NewReader(createKeyBody))
	applySignedHeaders(t, createKeyReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients/twilight/keys", createKeyBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createKeyRec := httptest.NewRecorder()
	router.ServeHTTP(createKeyRec, createKeyReq)

	if createKeyRec.Code != http.StatusCreated {
		t.Fatalf("create key status = %d, want %d", createKeyRec.Code, http.StatusCreated)
	}

	updateBody := []byte(`{"create_key":true}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/organizations/ponyville/clients/twilight/keys/alt", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "silent-bob", "", http.MethodPut, "/organizations/ponyville/clients/twilight/keys/alt", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d, want %d", updateRec.Code, http.StatusOK)
	}

	var payload map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(payload["private_key"].(string)))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/twilight", nil)
	applySignedHeadersWithPrivateKey(t, getReq, privateKey, "twilight", "ponyville", http.MethodGet, "/organizations/ponyville/clients/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}
}

func TestNodesEndpointCreateListGetAndHead(t *testing.T) {
	router := newTestRouter(t)
	body := mustMarshalNodePayload(t, "twilight")

	createReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/nodes", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create node) error = %v", err)
	}
	if createPayload["uri"] != "/nodes/twilight" {
		t.Fatalf("uri = %v, want %q", createPayload["uri"], "/nodes/twilight")
	}

	listReq := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list nodes status = %d, want %d", listRec.Code, http.StatusOK)
	}

	var listPayload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list nodes) error = %v", err)
	}
	if listPayload["twilight"] != "/nodes/twilight" {
		t.Fatalf("node uri = %q, want %q", listPayload["twilight"], "/nodes/twilight")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/nodes/twilight", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/nodes/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get node status = %d, want %d", getRec.Code, http.StatusOK)
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get node) error = %v", err)
	}
	if getPayload["name"] != "twilight" {
		t.Fatalf("name = %v, want %q", getPayload["name"], "twilight")
	}
	if getPayload["json_class"] != "Chef::Node" {
		t.Fatalf("json_class = %v, want %q", getPayload["json_class"], "Chef::Node")
	}
	if getPayload["chef_environment"] != "_default" {
		t.Fatalf("chef_environment = %v, want %q", getPayload["chef_environment"], "_default")
	}

	headReq := httptest.NewRequest(http.MethodHead, "/nodes/twilight", nil)
	applySignedHeaders(t, headReq, "silent-bob", "", http.MethodHead, "/nodes/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:20Z")
	headRec := httptest.NewRecorder()
	router.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("head node status = %d, want %d", headRec.Code, http.StatusOK)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("head node body length = %d, want 0", headRec.Body.Len())
	}

	collectionHeadReq := httptest.NewRequest(http.MethodHead, "/nodes", nil)
	applySignedHeaders(t, collectionHeadReq, "silent-bob", "", http.MethodHead, "/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:25Z")
	collectionHeadRec := httptest.NewRecorder()
	router.ServeHTTP(collectionHeadRec, collectionHeadReq)

	if collectionHeadRec.Code != http.StatusOK {
		t.Fatalf("head nodes collection status = %d, want %d", collectionHeadRec.Code, http.StatusOK)
	}
	if collectionHeadRec.Body.Len() != 0 {
		t.Fatalf("head nodes collection body length = %d, want 0", collectionHeadRec.Body.Len())
	}
}

func TestNodesEndpointUpdateRejectsNameMismatch(t *testing.T) {
	router := newTestRouter(t)
	createBody := mustMarshalNodePayload(t, "twilight")

	createReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/nodes", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	updateBody := []byte(`{"name":"rainbow","json_class":"Chef::Node"}`)
	updateReq := httptest.NewRequest(http.MethodPut, "/nodes/twilight", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "silent-bob", "", http.MethodPut, "/nodes/twilight", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusBadRequest {
		t.Fatalf("update node status = %d, want %d", updateRec.Code, http.StatusBadRequest)
	}

	var payload map[string][]string
	if err := json.Unmarshal(updateRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(update node) error = %v", err)
	}
	if len(payload["error"]) != 1 || payload["error"][0] != "Node name mismatch." {
		t.Fatalf("error payload = %v, want Node name mismatch", payload)
	}
}

func TestNodesEndpointDeleteReturnsNode(t *testing.T) {
	router := newTestRouter(t)
	createBody := mustMarshalNodePayload(t, "twilight")

	createReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/nodes", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/nodes/twilight", nil)
	applySignedHeaders(t, deleteReq, "silent-bob", "", http.MethodDelete, "/nodes/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete node status = %d, want %d", deleteRec.Code, http.StatusOK)
	}

	var deletePayload map[string]any
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &deletePayload); err != nil {
		t.Fatalf("json.Unmarshal(delete node) error = %v", err)
	}
	if deletePayload["name"] != "twilight" {
		t.Fatalf("name = %v, want %q", deletePayload["name"], "twilight")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/nodes/twilight", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/nodes/twilight", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("get deleted node status = %d, want %d", getRec.Code, http.StatusNotFound)
	}
}

func TestOrgNodesEndpointClientCanUpdateOwnNode(t *testing.T) {
	router := newTestRouter(t)
	createClientBody := []byte(`{"name":"twilight","create_key":true}`)

	createClientReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/clients", bytes.NewReader(createClientBody))
	applySignedHeaders(t, createClientReq, "silent-bob", "", http.MethodPost, "/organizations/ponyville/clients", createClientBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createClientRec := httptest.NewRecorder()
	router.ServeHTTP(createClientRec, createClientReq)

	if createClientRec.Code != http.StatusCreated {
		t.Fatalf("create client status = %d, want %d, body = %s", createClientRec.Code, http.StatusCreated, createClientRec.Body.String())
	}

	var clientPayload map[string]any
	if err := json.Unmarshal(createClientRec.Body.Bytes(), &clientPayload); err != nil {
		t.Fatalf("json.Unmarshal(create client) error = %v", err)
	}

	privateKeyPEM, ok := clientPayload["private_key"].(string)
	if !ok || privateKeyPEM == "" {
		t.Fatalf("private_key missing from create client payload: %v", clientPayload)
	}

	privateKey, err := authn.ParseRSAPrivateKeyPEM([]byte(privateKeyPEM))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}

	createNodeBody := mustMarshalNodePayload(t, "twilight")
	createNodeReq := httptest.NewRequest(http.MethodPost, "/organizations/ponyville/nodes", bytes.NewReader(createNodeBody))
	applySignedHeadersWithPrivateKey(t, createNodeReq, privateKey, "twilight", "ponyville", http.MethodPost, "/organizations/ponyville/nodes", createNodeBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	createNodeRec := httptest.NewRecorder()
	router.ServeHTTP(createNodeRec, createNodeReq)

	if createNodeRec.Code != http.StatusCreated {
		t.Fatalf("create node as client status = %d, want %d, body = %s", createNodeRec.Code, http.StatusCreated, createNodeRec.Body.String())
	}

	updateNodeBody := []byte(`{"json_class":"Chef::Node","normal":{"role":"librarian"}}`)
	updateNodeReq := httptest.NewRequest(http.MethodPut, "/organizations/ponyville/nodes/twilight", bytes.NewReader(updateNodeBody))
	applySignedHeadersWithPrivateKey(t, updateNodeReq, privateKey, "twilight", "ponyville", http.MethodPut, "/organizations/ponyville/nodes/twilight", updateNodeBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	updateNodeRec := httptest.NewRecorder()
	router.ServeHTTP(updateNodeRec, updateNodeReq)

	if updateNodeRec.Code != http.StatusOK {
		t.Fatalf("update node as client status = %d, want %d, body = %s", updateNodeRec.Code, http.StatusOK, updateNodeRec.Body.String())
	}

	var nodePayload map[string]any
	if err := json.Unmarshal(updateNodeRec.Body.Bytes(), &nodePayload); err != nil {
		t.Fatalf("json.Unmarshal(update node) error = %v", err)
	}
	normal, ok := nodePayload["normal"].(map[string]any)
	if !ok {
		t.Fatalf("normal payload = %T, want map[string]any", nodePayload["normal"])
	}
	if normal["role"] != "librarian" {
		t.Fatalf("normal.role = %v, want %q", normal["role"], "librarian")
	}
}

func TestNodesEndpointRequiresOrganizationWhenAmbiguous(t *testing.T) {
	router := newTestRouter(t)
	createOrgForTest(t, router, "canterlot")

	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	applySignedHeaders(t, req, "pivotal", "", http.MethodGet, "/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("list nodes status = %d, want %d", rec.Code, http.StatusBadRequest)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(nodes ambiguity) error = %v", err)
	}
	if payload["error"] != "organization_required" {
		t.Fatalf("error = %v, want %q", payload["error"], "organization_required")
	}
}

func TestNodesEndpointUsesConfiguredDefaultOrganization(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:         "opencook",
		Environment:         "test",
		AuthSkew:            15 * time.Minute,
		DefaultOrganization: "canterlot",
	})
	createOrgForTest(t, router, "canterlot")

	body := mustMarshalNodePayload(t, "shining-armor")
	createReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(body))
	applySignedHeaders(t, createReq, "pivotal", "", http.MethodPost, "/nodes", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create node status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	applySignedHeaders(t, listReq, "pivotal", "", http.MethodGet, "/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list nodes status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var payload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(list nodes) error = %v", err)
	}
	if payload["shining-armor"] != "/nodes/shining-armor" {
		t.Fatalf("node uri = %q, want %q", payload["shining-armor"], "/nodes/shining-armor")
	}

	explicitReq := httptest.NewRequest(http.MethodGet, "/organizations/canterlot/nodes/shining-armor", nil)
	applySignedHeaders(t, explicitReq, "pivotal", "", http.MethodGet, "/organizations/canterlot/nodes/shining-armor", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	explicitRec := httptest.NewRecorder()
	router.ServeHTTP(explicitRec, explicitReq)

	if explicitRec.Code != http.StatusOK {
		t.Fatalf("explicit get node status = %d, want %d", explicitRec.Code, http.StatusOK)
	}

	explicitHeadReq := httptest.NewRequest(http.MethodHead, "/organizations/canterlot/nodes", nil)
	applySignedHeaders(t, explicitHeadReq, "pivotal", "", http.MethodHead, "/organizations/canterlot/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:20Z")
	explicitHeadRec := httptest.NewRecorder()
	router.ServeHTTP(explicitHeadRec, explicitHeadReq)

	if explicitHeadRec.Code != http.StatusOK {
		t.Fatalf("explicit head nodes collection status = %d, want %d", explicitHeadRec.Code, http.StatusOK)
	}
	if explicitHeadRec.Body.Len() != 0 {
		t.Fatalf("explicit head nodes collection body length = %d, want 0", explicitHeadRec.Body.Len())
	}
}

func TestEnvironmentsEndpointListCreateGetAndHead(t *testing.T) {
	router := newTestRouter(t)

	listReq := httptest.NewRequest(http.MethodGet, "/environments", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/environments", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list environments status = %d, want %d", listRec.Code, http.StatusOK)
	}

	var listPayload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list environments) error = %v", err)
	}
	if listPayload["_default"] != "/environments/_default" {
		t.Fatalf("_default uri = %q, want %q", listPayload["_default"], "/environments/_default")
	}

	createBody := mustMarshalEnvironmentPayload(t, "production")
	createReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/environments", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create environment) error = %v", err)
	}
	if createPayload["uri"] != "/environments/production" {
		t.Fatalf("uri = %v, want %q", createPayload["uri"], "/environments/production")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/environments/production", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/environments/production", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get environment status = %d, want %d", getRec.Code, http.StatusOK)
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get environment) error = %v", err)
	}
	if getPayload["name"] != "production" {
		t.Fatalf("name = %v, want %q", getPayload["name"], "production")
	}
	if getPayload["json_class"] != "Chef::Environment" {
		t.Fatalf("json_class = %v, want %q", getPayload["json_class"], "Chef::Environment")
	}

	headReq := httptest.NewRequest(http.MethodHead, "/environments/production", nil)
	applySignedHeaders(t, headReq, "silent-bob", "", http.MethodHead, "/environments/production", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:20Z")
	headRec := httptest.NewRecorder()
	router.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("head environment status = %d, want %d", headRec.Code, http.StatusOK)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("head environment body length = %d, want 0", headRec.Body.Len())
	}
}

func TestEnvironmentsEndpointUpdateRenamesEnvironment(t *testing.T) {
	router := newTestRouter(t)
	createBody := mustMarshalEnvironmentPayload(t, "production")

	createReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/environments", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	updateBody := mustMarshalEnvironmentPayload(t, "staging")
	updateReq := httptest.NewRequest(http.MethodPut, "/environments/production", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "silent-bob", "", http.MethodPut, "/environments/production", updateBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusCreated {
		t.Fatalf("update environment status = %d, want %d, body = %s", updateRec.Code, http.StatusCreated, updateRec.Body.String())
	}
	if updateRec.Header().Get("Location") != "/environments/staging" {
		t.Fatalf("Location = %q, want %q", updateRec.Header().Get("Location"), "/environments/staging")
	}

	oldReq := httptest.NewRequest(http.MethodGet, "/environments/production", nil)
	applySignedHeaders(t, oldReq, "silent-bob", "", http.MethodGet, "/environments/production", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	oldRec := httptest.NewRecorder()
	router.ServeHTTP(oldRec, oldReq)

	if oldRec.Code != http.StatusNotFound {
		t.Fatalf("old environment status = %d, want %d", oldRec.Code, http.StatusNotFound)
	}

	newReq := httptest.NewRequest(http.MethodGet, "/environments/staging", nil)
	applySignedHeaders(t, newReq, "silent-bob", "", http.MethodGet, "/environments/staging", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:20Z")
	newRec := httptest.NewRecorder()
	router.ServeHTTP(newRec, newReq)

	if newRec.Code != http.StatusOK {
		t.Fatalf("new environment status = %d, want %d", newRec.Code, http.StatusOK)
	}
}

func TestEnvironmentsEndpointDefaultEnvironmentIsImmutable(t *testing.T) {
	router := newTestRouter(t)

	deleteReq := httptest.NewRequest(http.MethodDelete, "/environments/_default", nil)
	applySignedHeaders(t, deleteReq, "silent-bob", "", http.MethodDelete, "/environments/_default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("delete _default status = %d, want %d", deleteRec.Code, http.StatusMethodNotAllowed)
	}

	var payload map[string][]string
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(delete _default) error = %v", err)
	}
	if len(payload["error"]) != 1 || payload["error"][0] != "The '_default' environment cannot be modified." {
		t.Fatalf("error payload = %v, want immutable _default message", payload)
	}
}

func TestEnvironmentNodesEndpointFiltersNodes(t *testing.T) {
	router := newTestRouter(t)
	createEnvironmentBody := mustMarshalEnvironmentPayload(t, "production")

	createEnvironmentReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(createEnvironmentBody))
	applySignedHeaders(t, createEnvironmentReq, "silent-bob", "", http.MethodPost, "/environments", createEnvironmentBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createEnvironmentRec := httptest.NewRecorder()
	router.ServeHTTP(createEnvironmentRec, createEnvironmentReq)
	if createEnvironmentRec.Code != http.StatusCreated {
		t.Fatalf("create environment status = %d, want %d, body = %s", createEnvironmentRec.Code, http.StatusCreated, createEnvironmentRec.Body.String())
	}

	prodNodeBody := mustMarshalNodePayloadWithEnvironment(t, "twilight", "production")
	prodNodeReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(prodNodeBody))
	applySignedHeaders(t, prodNodeReq, "silent-bob", "", http.MethodPost, "/nodes", prodNodeBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:10Z")
	prodNodeRec := httptest.NewRecorder()
	router.ServeHTTP(prodNodeRec, prodNodeReq)
	if prodNodeRec.Code != http.StatusCreated {
		t.Fatalf("create prod node status = %d, want %d, body = %s", prodNodeRec.Code, http.StatusCreated, prodNodeRec.Body.String())
	}

	defaultNodeBody := mustMarshalNodePayload(t, "rainbow")
	defaultNodeReq := httptest.NewRequest(http.MethodPost, "/nodes", bytes.NewReader(defaultNodeBody))
	applySignedHeaders(t, defaultNodeReq, "silent-bob", "", http.MethodPost, "/nodes", defaultNodeBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:15Z")
	defaultNodeRec := httptest.NewRecorder()
	router.ServeHTTP(defaultNodeRec, defaultNodeReq)
	if defaultNodeRec.Code != http.StatusCreated {
		t.Fatalf("create default node status = %d, want %d, body = %s", defaultNodeRec.Code, http.StatusCreated, defaultNodeRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/environments/production/nodes", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/environments/production/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:20Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list environment nodes status = %d, want %d", listRec.Code, http.StatusOK)
	}

	var payload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(environment nodes) error = %v", err)
	}
	if len(payload) != 1 {
		t.Fatalf("len(payload) = %d, want 1: %v", len(payload), payload)
	}
	if payload["twilight"] != "/nodes/twilight" {
		t.Fatalf("twilight uri = %q, want %q", payload["twilight"], "/nodes/twilight")
	}
	if _, ok := payload["rainbow"]; ok {
		t.Fatalf("unexpected default-environment node in filtered list: %v", payload)
	}

	headReq := httptest.NewRequest(http.MethodHead, "/organizations/ponyville/environments/production/nodes", nil)
	applySignedHeaders(t, headReq, "silent-bob", "", http.MethodHead, "/organizations/ponyville/environments/production/nodes", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:25Z")
	headReq.SetPathValue("org", "ponyville")
	headReq.SetPathValue("name", "production")
	headRec := httptest.NewRecorder()
	router.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("head environment nodes status = %d, want %d", headRec.Code, http.StatusOK)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("head environment nodes body length = %d, want 0", headRec.Body.Len())
	}
}

func TestRolesEndpointListCreateGetAndHead(t *testing.T) {
	router := newTestRouter(t)

	listReq := httptest.NewRequest(http.MethodGet, "/roles", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/roles", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list roles status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var listPayload map[string]string
	if err := json.Unmarshal(listRec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("json.Unmarshal(list roles) error = %v", err)
	}
	if len(listPayload) != 0 {
		t.Fatalf("list payload = %v, want empty map", listPayload)
	}

	createBody := mustMarshalRolePayloadWithRunListAndEnvRunLists(
		t,
		"web",
		[]string{"base", "recipe[base]", "foo::default", "recipe[foo::default]", "role[db]"},
		map[string][]string{
			"production": []string{"nginx", "recipe[nginx]"},
		},
	)
	createReq := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/roles", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create role status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create role) error = %v", err)
	}
	if createPayload["uri"] != "/roles/web" {
		t.Fatalf("uri = %v, want %q", createPayload["uri"], "/roles/web")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/roles/web", nil)
	applySignedHeaders(t, getReq, "silent-bob", "", http.MethodGet, "/organizations/ponyville/roles/web", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	getRec := httptest.NewRecorder()
	router.ServeHTTP(getRec, getReq)

	if getRec.Code != http.StatusOK {
		t.Fatalf("get role status = %d, want %d, body = %s", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var getPayload map[string]any
	if err := json.Unmarshal(getRec.Body.Bytes(), &getPayload); err != nil {
		t.Fatalf("json.Unmarshal(get role) error = %v", err)
	}
	if getPayload["json_class"] != "Chef::Role" {
		t.Fatalf("json_class = %v, want %q", getPayload["json_class"], "Chef::Role")
	}
	if getPayload["chef_type"] != "role" {
		t.Fatalf("chef_type = %v, want %q", getPayload["chef_type"], "role")
	}
	assertStringSliceFromAnyEqual(t, getPayload["run_list"], []string{"recipe[base]", "recipe[foo::default]", "role[db]"})
	envRunLists, ok := getPayload["env_run_lists"].(map[string]any)
	if !ok {
		t.Fatalf("env_run_lists = %T, want map[string]any", getPayload["env_run_lists"])
	}
	assertStringSliceFromAnyEqual(t, envRunLists["production"], []string{"recipe[nginx]"})

	headReq := httptest.NewRequest(http.MethodHead, "/roles/web", nil)
	applySignedHeaders(t, headReq, "silent-bob", "", http.MethodHead, "/roles/web", nil, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	headRec := httptest.NewRecorder()
	router.ServeHTTP(headRec, headReq)

	if headRec.Code != http.StatusOK {
		t.Fatalf("head role status = %d, want %d", headRec.Code, http.StatusOK)
	}
	if headRec.Body.Len() != 0 {
		t.Fatalf("head role body length = %d, want 0", headRec.Body.Len())
	}

	collectionHeadReq := httptest.NewRequest(http.MethodHead, "/organizations/ponyville/roles", nil)
	applySignedHeaders(t, collectionHeadReq, "silent-bob", "", http.MethodHead, "/organizations/ponyville/roles", nil, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	collectionHeadRec := httptest.NewRecorder()
	router.ServeHTTP(collectionHeadRec, collectionHeadReq)

	if collectionHeadRec.Code != http.StatusOK {
		t.Fatalf("head roles collection status = %d, want %d", collectionHeadRec.Code, http.StatusOK)
	}
	if collectionHeadRec.Body.Len() != 0 {
		t.Fatalf("head roles collection body length = %d, want 0", collectionHeadRec.Body.Len())
	}
}

func TestRolesEndpointUpdateAndDelete(t *testing.T) {
	router := newTestRouter(t)

	createBody := mustMarshalRolePayload(t, "web")
	createReq := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(createBody))
	applySignedHeaders(t, createReq, "silent-bob", "", http.MethodPost, "/roles", createBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)

	if createRec.Code != http.StatusCreated {
		t.Fatalf("create role status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	updateBody := mustMarshalRoleUpdatePayload(
		t,
		"Updated web role",
		[]string{"apache2", "recipe[apache2]", "role[db]", "role[db]"},
		map[string][]string{
			"production": []string{"nginx", "recipe[nginx]"},
			"staging":    []string{},
		},
	)
	updateReq := httptest.NewRequest(http.MethodPut, "/roles/web", bytes.NewReader(updateBody))
	applySignedHeaders(t, updateReq, "silent-bob", "", http.MethodPut, "/roles/web", updateBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	updateRec := httptest.NewRecorder()
	router.ServeHTTP(updateRec, updateReq)

	if updateRec.Code != http.StatusOK {
		t.Fatalf("update role status = %d, want %d, body = %s", updateRec.Code, http.StatusOK, updateRec.Body.String())
	}

	var updatePayload map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updatePayload); err != nil {
		t.Fatalf("json.Unmarshal(update role) error = %v", err)
	}
	if updatePayload["name"] != "web" {
		t.Fatalf("name = %v, want %q", updatePayload["name"], "web")
	}
	if updatePayload["description"] != "Updated web role" {
		t.Fatalf("description = %v, want %q", updatePayload["description"], "Updated web role")
	}
	assertStringSliceFromAnyEqual(t, updatePayload["run_list"], []string{"recipe[apache2]", "role[db]"})
	envRunLists, ok := updatePayload["env_run_lists"].(map[string]any)
	if !ok {
		t.Fatalf("env_run_lists = %T, want map[string]any", updatePayload["env_run_lists"])
	}
	assertStringSliceFromAnyEqual(t, envRunLists["production"], []string{"recipe[nginx]"})
	assertStringSliceFromAnyEqual(t, envRunLists["staging"], []string{})

	mismatchBody := []byte(`{"name":"db","json_class":"Chef::Role","chef_type":"role"}`)
	mismatchReq := httptest.NewRequest(http.MethodPut, "/roles/web", bytes.NewReader(mismatchBody))
	applySignedHeaders(t, mismatchReq, "silent-bob", "", http.MethodPut, "/roles/web", mismatchBody, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	mismatchRec := httptest.NewRecorder()
	router.ServeHTTP(mismatchRec, mismatchReq)

	if mismatchRec.Code != http.StatusBadRequest {
		t.Fatalf("mismatch update role status = %d, want %d, body = %s", mismatchRec.Code, http.StatusBadRequest, mismatchRec.Body.String())
	}

	var mismatchPayload map[string][]string
	if err := json.Unmarshal(mismatchRec.Body.Bytes(), &mismatchPayload); err != nil {
		t.Fatalf("json.Unmarshal(mismatch update role) error = %v", err)
	}
	if len(mismatchPayload["error"]) != 1 || mismatchPayload["error"][0] != "Role name mismatch." {
		t.Fatalf("error payload = %v, want Role name mismatch", mismatchPayload)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/roles/web", nil)
	applySignedHeaders(t, deleteReq, "silent-bob", "", http.MethodDelete, "/roles/web", nil, signDescription{
		Version:   "1.3",
		Algorithm: "sha256",
	}, "2026-04-02T15:04:05Z")
	deleteRec := httptest.NewRecorder()
	router.ServeHTTP(deleteRec, deleteReq)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("delete role status = %d, want %d, body = %s", deleteRec.Code, http.StatusOK, deleteRec.Body.String())
	}

	var deletePayload map[string]any
	if err := json.Unmarshal(deleteRec.Body.Bytes(), &deletePayload); err != nil {
		t.Fatalf("json.Unmarshal(delete role) error = %v", err)
	}
	if deletePayload["name"] != "web" {
		t.Fatalf("deleted role name = %v, want %q", deletePayload["name"], "web")
	}
}

func TestRoleEnvironmentsEndpoints(t *testing.T) {
	router := newTestRouter(t)

	productionBody := mustMarshalEnvironmentPayload(t, "production")
	productionReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(productionBody))
	applySignedHeaders(t, productionReq, "silent-bob", "", http.MethodPost, "/environments", productionBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	productionRec := httptest.NewRecorder()
	router.ServeHTTP(productionRec, productionReq)

	if productionRec.Code != http.StatusCreated {
		t.Fatalf("create production environment status = %d, want %d, body = %s", productionRec.Code, http.StatusCreated, productionRec.Body.String())
	}

	stagingBody := mustMarshalEnvironmentPayload(t, "staging")
	stagingReq := httptest.NewRequest(http.MethodPost, "/environments", bytes.NewReader(stagingBody))
	applySignedHeaders(t, stagingReq, "silent-bob", "", http.MethodPost, "/environments", stagingBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	stagingRec := httptest.NewRecorder()
	router.ServeHTTP(stagingRec, stagingReq)

	if stagingRec.Code != http.StatusCreated {
		t.Fatalf("create staging environment status = %d, want %d, body = %s", stagingRec.Code, http.StatusCreated, stagingRec.Body.String())
	}

	createRoleBody := mustMarshalRolePayloadWithRunListAndEnvRunLists(
		t,
		"web",
		[]string{"base", "recipe[base]", "role[db]", "role[db]"},
		map[string][]string{
			"production": []string{"nginx", "recipe[nginx]", "role[app]", "role[app]"},
		},
	)
	createRoleReq := httptest.NewRequest(http.MethodPost, "/roles", bytes.NewReader(createRoleBody))
	applySignedHeaders(t, createRoleReq, "silent-bob", "", http.MethodPost, "/roles", createRoleBody, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	createRoleRec := httptest.NewRecorder()
	router.ServeHTTP(createRoleRec, createRoleReq)

	if createRoleRec.Code != http.StatusCreated {
		t.Fatalf("create role status = %d, want %d, body = %s", createRoleRec.Code, http.StatusCreated, createRoleRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/roles/web/environments", nil)
	applySignedHeaders(t, listReq, "silent-bob", "", http.MethodGet, "/roles/web/environments", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	listRec := httptest.NewRecorder()
	router.ServeHTTP(listRec, listReq)

	if listRec.Code != http.StatusOK {
		t.Fatalf("list role environments status = %d, want %d, body = %s", listRec.Code, http.StatusOK, listRec.Body.String())
	}

	var envNames []any
	if err := json.Unmarshal(listRec.Body.Bytes(), &envNames); err != nil {
		t.Fatalf("json.Unmarshal(role environments) error = %v", err)
	}
	gotNames := stringSliceFromAny(t, envNames)
	if len(gotNames) != 2 || gotNames[0] != "_default" || gotNames[1] != "production" {
		t.Fatalf("role environments = %v, want [_default production]", gotNames)
	}

	defaultReq := httptest.NewRequest(http.MethodGet, "/roles/web/environments/_default", nil)
	applySignedHeaders(t, defaultReq, "silent-bob", "", http.MethodGet, "/roles/web/environments/_default", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	defaultRec := httptest.NewRecorder()
	router.ServeHTTP(defaultRec, defaultReq)

	if defaultRec.Code != http.StatusOK {
		t.Fatalf("default role environment status = %d, want %d, body = %s", defaultRec.Code, http.StatusOK, defaultRec.Body.String())
	}

	var defaultPayload map[string]any
	if err := json.Unmarshal(defaultRec.Body.Bytes(), &defaultPayload); err != nil {
		t.Fatalf("json.Unmarshal(default role env) error = %v", err)
	}
	assertStringSliceFromAnyEqual(t, defaultPayload["run_list"], []string{"recipe[base]", "role[db]"})

	productionRoleReq := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/roles/web/environments/production", nil)
	applySignedHeaders(t, productionRoleReq, "silent-bob", "", http.MethodGet, "/organizations/ponyville/roles/web/environments/production", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	productionRoleRec := httptest.NewRecorder()
	router.ServeHTTP(productionRoleRec, productionRoleReq)

	if productionRoleRec.Code != http.StatusOK {
		t.Fatalf("production role environment status = %d, want %d, body = %s", productionRoleRec.Code, http.StatusOK, productionRoleRec.Body.String())
	}

	var productionPayload map[string]any
	if err := json.Unmarshal(productionRoleRec.Body.Bytes(), &productionPayload); err != nil {
		t.Fatalf("json.Unmarshal(production role env) error = %v", err)
	}
	assertStringSliceFromAnyEqual(t, productionPayload["run_list"], []string{"recipe[nginx]", "role[app]"})

	stagingRoleReq := httptest.NewRequest(http.MethodGet, "/roles/web/environments/staging", nil)
	applySignedHeaders(t, stagingRoleReq, "silent-bob", "", http.MethodGet, "/roles/web/environments/staging", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	stagingRoleRec := httptest.NewRecorder()
	router.ServeHTTP(stagingRoleRec, stagingRoleReq)

	if stagingRoleRec.Code != http.StatusOK {
		t.Fatalf("staging role environment status = %d, want %d, body = %s", stagingRoleRec.Code, http.StatusOK, stagingRoleRec.Body.String())
	}

	var stagingPayload map[string]any
	if err := json.Unmarshal(stagingRoleRec.Body.Bytes(), &stagingPayload); err != nil {
		t.Fatalf("json.Unmarshal(staging role env) error = %v", err)
	}
	if value, ok := stagingPayload["run_list"]; !ok || value != nil {
		t.Fatalf("staging run_list = %v, want nil", stagingPayload["run_list"])
	}

	missingEnvReq := httptest.NewRequest(http.MethodGet, "/roles/web/environments/preprod", nil)
	applySignedHeaders(t, missingEnvReq, "silent-bob", "", http.MethodGet, "/roles/web/environments/preprod", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	missingEnvRec := httptest.NewRecorder()
	router.ServeHTTP(missingEnvRec, missingEnvReq)

	if missingEnvRec.Code != http.StatusNotFound {
		t.Fatalf("missing role environment status = %d, want %d, body = %s", missingEnvRec.Code, http.StatusNotFound, missingEnvRec.Body.String())
	}

	var missingEnvPayload map[string][]string
	if err := json.Unmarshal(missingEnvRec.Body.Bytes(), &missingEnvPayload); err != nil {
		t.Fatalf("json.Unmarshal(missing role env) error = %v", err)
	}
	if len(missingEnvPayload["error"]) != 1 || missingEnvPayload["error"][0] != "Cannot load environment preprod" {
		t.Fatalf("missing environment payload = %v, want Cannot load environment preprod", missingEnvPayload)
	}
}

func TestUsersEndpointRejectsOversizedBodyBeforeVerification(t *testing.T) {
	router := newTestRouterWithConfig(t, config.Config{
		ServiceName:      "opencook",
		Environment:      "test",
		AuthSkew:         15 * time.Minute,
		MaxAuthBodyBytes: 4,
	})
	req := httptest.NewRequest(http.MethodGet, "/users", strings.NewReader("12345"))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["error"] != "request_body_too_large" {
		t.Fatalf("error = %v, want %q", payload["error"], "request_body_too_large")
	}
}

func TestUsersEndpointHidesInternalVerifierErrors(t *testing.T) {
	var logs bytes.Buffer
	router := newTestRouterWithOverrides(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	}, log.New(&logs, "", 0), failingVerifier{err: errors.New("keystore exploded")})
	req := httptest.NewRequest(http.MethodGet, "/users", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["message"] != "internal authentication error" {
		t.Fatalf("message = %v, want %q", payload["message"], "internal authentication error")
	}

	if strings.Contains(rec.Body.String(), "keystore exploded") {
		t.Fatalf("response leaked verifier error: %s", rec.Body.String())
	}

	if !strings.Contains(logs.String(), "keystore exploded") {
		t.Fatalf("expected internal error to be logged, logs = %q", logs.String())
	}
}

func TestUserACLRouteHandlesMissingBootstrap(t *testing.T) {
	router := newTestRouterWithoutBootstrap(t)
	req := httptest.NewRequest(http.MethodGet, "/users/silent-bob/_acl", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/users/silent-bob/_acl", nil, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload["error"] != "bootstrap_unavailable" {
		t.Fatalf("error = %v, want %q", payload["error"], "bootstrap_unavailable")
	}
}

func newTestRouter(t *testing.T) http.Handler {
	return newTestRouterWithConfig(t, config.Config{
		ServiceName: "opencook",
		Environment: "test",
		AuthSkew:    15 * time.Minute,
	})
}

func newTestRouterWithConfig(t *testing.T, cfg config.Config) http.Handler {
	return newTestRouterWithOverrides(t, cfg, nil, nil)
}

func newTestRouterWithBlob(t *testing.T, cfg config.Config, blobStore blob.Store) http.Handler {
	return newTestRouterWithOverridesAndBlob(t, cfg, nil, nil, blobStore)
}

func newTestRouterWithOverrides(t *testing.T, cfg config.Config, logger *log.Logger, verifier authn.Verifier) http.Handler {
	return newTestRouterWithOverridesAndBlob(t, cfg, logger, verifier, blob.NewMemoryStore(""))
}

func newTestRouterWithOverridesAndBlob(t *testing.T, cfg config.Config, logger *log.Logger, verifier authn.Verifier, blobStore blob.Store) http.Handler {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
	store := authn.NewMemoryKeyStore()
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	})
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "pivotal",
		},
		PublicKey: &privateKey.PublicKey,
	})
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "outside-user",
		},
		PublicKey: &privateKey.PublicKey,
	})
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "normal-user",
		},
		PublicKey: &privateKey.PublicKey,
	})
	state := bootstrap.NewService(store, bootstrap.Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "silent-bob"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
	}
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "outside-user"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "outside-user"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(outside-user) error = %v", err)
	}
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "normal-user"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "normal-user"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(normal-user) error = %v", err)
	}
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "pivotal"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(pivotal) error = %v", err)
	}
	if _, _, _, err := state.CreateOrganization(bootstrap.CreateOrganizationInput{
		Name:      "ponyville",
		FullName:  "Ponyville",
		OrgType:   "Business",
		OwnerName: "silent-bob",
	}); err != nil {
		t.Fatalf("CreateOrganization() error = %v", err)
	}
	if _, _, err := state.CreateClient("ponyville", bootstrap.CreateClientInput{
		Name:      "org-validator",
		PublicKey: publicKeyPEM,
	}); err != nil {
		t.Fatalf("CreateClient() error = %v", err)
	}
	if err := state.AddUserToGroup("ponyville", "users", "normal-user"); err != nil {
		t.Fatalf("AddUserToGroup(normal-user) error = %v", err)
	}

	skew := 15 * time.Minute
	if cfg.MaxAuthBodyBytes == 0 {
		cfg.MaxAuthBodyBytes = config.DefaultMaxAuthBodyBytes
	}

	if logger == nil {
		logger = log.New(ioDiscard{}, "", 0)
	}

	if verifier == nil {
		verifier = authn.NewChefVerifier(store, authn.Options{
			AllowedClockSkew: &skew,
			Now: func() time.Time {
				return mustParseTime(t, "2026-04-02T15:04:35Z")
			},
		})
	}
	now := func() time.Time {
		return mustParseTime(t, "2026-04-02T15:04:35Z")
	}
	if blobStore == nil {
		blobStore = blob.NewMemoryStore("")
	}

	return NewRouter(Dependencies{
		Logger:           logger,
		Config:           cfg,
		Version:          version.Current(),
		Compat:           compat.NewDefaultRegistry(),
		Now:              now,
		Authn:            verifier,
		Authz:            authz.NewACLAuthorizer(state),
		Bootstrap:        state,
		Blob:             blobStore,
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         pg.New(""),
	})
}

func newTestRouterWithoutBootstrap(t *testing.T) http.Handler {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
	store := authn.NewMemoryKeyStore()
	mustPutKey(t, store, authn.Key{
		ID: "default",
		Principal: authn.Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	})

	skew := 15 * time.Minute
	now := func() time.Time {
		return mustParseTime(t, "2026-04-02T15:04:35Z")
	}
	return NewRouter(Dependencies{
		Logger: log.New(ioDiscard{}, "", 0),
		Config: config.Config{
			ServiceName:      "opencook",
			Environment:      "test",
			AuthSkew:         skew,
			MaxAuthBodyBytes: config.DefaultMaxAuthBodyBytes,
		},
		Version: version.Current(),
		Compat:  compat.NewDefaultRegistry(),
		Now:     now,
		Authn: authn.NewChefVerifier(store, authn.Options{
			AllowedClockSkew: &skew,
			Now:              now,
		}),
		Authz:            authz.NoopAuthorizer{},
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(nil, ""),
		Postgres:         pg.New(""),
	})
}

func mustPutKey(t *testing.T, store *authn.MemoryKeyStore, key authn.Key) {
	t.Helper()
	if err := store.Put(key); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}
}

func mustParsePrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()

	key, err := authn.ParseRSAPrivateKeyPEM([]byte(testSigningKeyPEM))
	if err != nil {
		t.Fatalf("ParseRSAPrivateKeyPEM() error = %v", err)
	}
	return key
}

func mustParseTime(t *testing.T, raw string) time.Time {
	t.Helper()

	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("time.Parse() error = %v", err)
	}
	return ts
}

func mustMarshalPublicKeyPEM(t *testing.T, publicKey *rsa.PublicKey) string {
	t.Helper()

	der, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("x509.MarshalPKIXPublicKey() error = %v", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: der,
	}))
}

func createOrgForTest(t *testing.T, router http.Handler, orgName string) {
	t.Helper()

	body, err := json.Marshal(map[string]string{
		"name":      orgName,
		"full_name": orgName,
		"org_type":  "Business",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/organizations", bytes.NewReader(body))
	applySignedHeaders(t, req, "pivotal", "", http.MethodPost, "/organizations", body, signDescription{
		Version:   "1.1",
		Algorithm: "sha1",
	}, "2026-04-02T15:04:05Z")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("create org status = %d, want %d, body = %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func mustMarshalNodePayload(t *testing.T, name string) []byte {
	return mustMarshalNodePayloadWithEnvironment(t, name, "_default")
}

func mustMarshalNodePayloadWithEnvironment(t *testing.T, name, environment string) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":             name,
		"json_class":       "Chef::Node",
		"chef_type":        "node",
		"chef_environment": environment,
		"override":         map[string]any{},
		"normal":           map[string]any{"is_anyone": "no"},
		"default":          map[string]any{},
		"automatic":        map[string]any{},
		"run_list":         []string{},
	})
	if err != nil {
		t.Fatalf("json.Marshal(node payload) error = %v", err)
	}

	return body
}

func mustMarshalEnvironmentPayload(t *testing.T, name string) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":                name,
		"json_class":          "Chef::Environment",
		"chef_type":           "environment",
		"description":         "",
		"cookbook_versions":   map[string]string{},
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
	})
	if err != nil {
		t.Fatalf("json.Marshal(environment payload) error = %v", err)
	}

	return body
}

func mustMarshalRolePayload(t *testing.T, name string) []byte {
	return mustMarshalRolePayloadWithRunListAndEnvRunLists(t, name, []string{"recipe[base]"}, map[string][]string{})
}

func mustMarshalRolePayloadWithEnvRunLists(t *testing.T, name string, envRunLists map[string][]string) []byte {
	return mustMarshalRolePayloadWithRunListAndEnvRunLists(t, name, []string{"recipe[base]"}, envRunLists)
}

func mustMarshalRolePayloadWithRunListAndEnvRunLists(t *testing.T, name string, runList []string, envRunLists map[string][]string) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"name":                name,
		"description":         "",
		"json_class":          "Chef::Role",
		"chef_type":           "role",
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
		"run_list":            runList,
		"env_run_lists":       envRunLists,
	})
	if err != nil {
		t.Fatalf("json.Marshal(role payload) error = %v", err)
	}

	return body
}

func mustMarshalRoleUpdatePayload(t *testing.T, description string, runList []string, envRunLists map[string][]string) []byte {
	t.Helper()

	body, err := json.Marshal(map[string]any{
		"description":   description,
		"json_class":    "Chef::Role",
		"chef_type":     "role",
		"run_list":      runList,
		"env_run_lists": envRunLists,
	})
	if err != nil {
		t.Fatalf("json.Marshal(role update payload) error = %v", err)
	}

	return body
}

func stringSliceFromAny(t *testing.T, value any) []string {
	t.Helper()

	raw, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %T, want []any", value)
	}

	out := make([]string, 0, len(raw))
	for _, item := range raw {
		str, ok := item.(string)
		if !ok {
			t.Fatalf("slice item = %T, want string", item)
		}
		out = append(out, str)
	}
	return out
}

func assertStringSliceFromAnyEqual(t *testing.T, value any, want []string) {
	t.Helper()

	got := stringSliceFromAny(t, value)
	if len(got) != len(want) {
		t.Fatalf("len(%v) = %d, want %d", got, len(got), len(want))
	}
	for idx := range want {
		if got[idx] != want[idx] {
			t.Fatalf("got[%d] = %q, want %q (full slice %v)", idx, got[idx], want[idx], got)
		}
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func applySignedHeaders(t *testing.T, req *http.Request, userID, organization, method, path string, body []byte, sign signDescription, timestamp string) {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
	applySignedHeadersWithPrivateKey(t, req, privateKey, userID, organization, method, path, body, sign, timestamp)
}

func applySignedHeadersWithPrivateKey(t *testing.T, req *http.Request, privateKey *rsa.PrivateKey, userID, organization, method, path string, body []byte, sign signDescription, timestamp string) {
	t.Helper()

	for key, value := range manufactureSignedHeaders(t, privateKey, userID, method, path, body, sign, timestamp, "0") {
		req.Header.Set(key, value)
	}
	if organization != "" {
		req.SetPathValue("org", organization)
	}
}

type signDescription struct {
	Version   string
	Algorithm string
}

func manufactureSignedHeaders(t *testing.T, privateKey *rsa.PrivateKey, userID, method, path string, body []byte, sign signDescription, timestamp, serverAPIVersion string) map[string]string {
	t.Helper()

	if body == nil {
		body = []byte{}
	}

	contentHash := hashBody(body, sign)
	stringToSign := canonicalString(method, path, contentHash, timestamp, userID, serverAPIVersion, sign)
	signature := signRequest(t, privateKey, stringToSign, sign)
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)

	headers := map[string]string{
		"X-Ops-Sign":         renderSignDescription(sign),
		"X-Ops-Userid":       userID,
		"X-Ops-Timestamp":    timestamp,
		"X-Ops-Content-Hash": contentHash,
	}
	for index, chunk := range splitBase64(signatureBase64) {
		headers["X-Ops-Authorization-"+strconv.Itoa(index+1)] = chunk
	}
	return headers
}

func canonicalString(method, path, contentHash, timestamp, userID, serverAPIVersion string, sign signDescription) string {
	if sign.Version == "1.3" {
		return strings.Join([]string{
			"Method:" + strings.ToUpper(method),
			"Path:" + path,
			"X-Ops-Content-Hash:" + contentHash,
			"X-Ops-Sign:version=1.3",
			"X-Ops-Timestamp:" + timestamp,
			"X-Ops-UserId:" + userID,
			"X-Ops-Server-API-Version:" + serverAPIVersion,
		}, "\n")
	}

	return strings.Join([]string{
		"Method:" + strings.ToUpper(method),
		"Hashed Path:" + hashPath(path, sign),
		"X-Ops-Content-Hash:" + contentHash,
		"X-Ops-Timestamp:" + timestamp,
		"X-Ops-UserId:" + userID,
	}, "\n")
}

func hashBody(body []byte, sign signDescription) string {
	if sign.Algorithm == "sha256" {
		sum := sha256.Sum256(body)
		return base64.StdEncoding.EncodeToString(sum[:])
	}

	sum := sha1.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func hashPath(path string, sign signDescription) string {
	return hashBody([]byte(path), sign)
}

func signRequest(t *testing.T, privateKey *rsa.PrivateKey, stringToSign string, sign signDescription) []byte {
	t.Helper()

	if sign.Version == "1.3" {
		sum := sha256.Sum256([]byte(stringToSign))
		signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, sum[:])
		if err != nil {
			t.Fatalf("rsa.SignPKCS1v15() error = %v", err)
		}
		return signature
	}

	signature, err := legacyPrivateEncrypt(privateKey, []byte(stringToSign))
	if err != nil {
		t.Fatalf("legacyPrivateEncrypt() error = %v", err)
	}
	return signature
}

func renderSignDescription(sign signDescription) string {
	switch sign.Version {
	case "1.0":
		return "version=1.0"
	case "1.1":
		return "algorithm=sha1;version=1.1;"
	case "1.3":
		return "algorithm=sha256;version=1.3"
	default:
		return ""
	}
}

func splitBase64(encoded string) []string {
	const width = 60

	var out []string
	for len(encoded) > width {
		out = append(out, encoded[:width])
		encoded = encoded[width:]
	}
	if encoded != "" {
		out = append(out, encoded)
	}
	return out
}

func legacyPrivateEncrypt(privateKey *rsa.PrivateKey, msg []byte) ([]byte, error) {
	k := privateKey.Size()
	if len(msg) > k-11 {
		return nil, rsa.ErrMessageTooLong
	}

	em := make([]byte, k)
	em[0] = 0x00
	em[1] = 0x01
	for i := 2; i < k-len(msg)-1; i++ {
		em[i] = 0xff
	}
	em[k-len(msg)-1] = 0x00
	copy(em[k-len(msg):], msg)

	m := new(big.Int).SetBytes(em)
	c := new(big.Int).Exp(m, privateKey.D, privateKey.N)
	return leftPad(c.Bytes(), k), nil
}

func leftPad(in []byte, size int) []byte {
	if len(in) >= size {
		return in
	}

	out := make([]byte, size)
	copy(out[size-len(in):], in)
	return out
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

type failingVerifier struct {
	err error
}

func (f failingVerifier) Name() string {
	return "failing-verifier"
}

func (f failingVerifier) Capabilities() authn.Capabilities {
	return authn.Capabilities{}
}

func (f failingVerifier) Verify(_ context.Context, _ authn.RequestContext) (authn.VerificationResult, error) {
	return authn.VerificationResult{}, f.err
}
