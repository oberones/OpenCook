package api

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
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
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/users", nil, signDescription{
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
	if requestor["name"] != "silent-bob" {
		t.Fatalf("requestor.name = %v, want %q", requestor["name"], "silent-bob")
	}
}

func TestUsersEndpointCollectionAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/users/", nil)
	applySignedHeaders(t, req, "silent-bob", "", http.MethodGet, "/users/", nil, signDescription{
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
	applySignedHeaders(t, req, "org-validator", "ponyville", http.MethodGet, "/organizations/ponyville/clients", nil, signDescription{
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

	if payload["organization"] != "ponyville" {
		t.Fatalf("organization = %v, want %q", payload["organization"], "ponyville")
	}
}

func TestOrgClientsEndpointCollectionAcceptsTrailingSlash(t *testing.T) {
	router := newTestRouter(t)
	req := httptest.NewRequest(http.MethodGet, "/organizations/ponyville/clients/", nil)
	applySignedHeaders(t, req, "org-validator", "ponyville", http.MethodGet, "/organizations/ponyville/clients/", nil, signDescription{
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

	if payload["clients"] == nil {
		t.Fatalf("clients payload missing for trailing slash collection path")
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

func newTestRouterWithOverrides(t *testing.T, cfg config.Config, logger *log.Logger, verifier authn.Verifier) http.Handler {
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
			Type:         "client",
			Name:         "org-validator",
			Organization: "ponyville",
		},
		PublicKey: &privateKey.PublicKey,
	})

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

	return NewRouter(Dependencies{
		Logger:   logger,
		Config:   cfg,
		Version:  version.Current(),
		Compat:   compat.NewDefaultRegistry(),
		Authn:    verifier,
		Authz:    authz.NoopAuthorizer{},
		Blob:     blob.NewNoopStore(""),
		Search:   search.NewNoopIndex(""),
		Postgres: pg.New(""),
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

func applySignedHeaders(t *testing.T, req *http.Request, userID, organization, method, path string, body []byte, sign signDescription, timestamp string) {
	t.Helper()

	privateKey := mustParsePrivateKey(t)
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
