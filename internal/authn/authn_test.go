package authn

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"strconv"
	"testing"
	"time"
)

const signingKeyPEM = `-----BEGIN RSA PRIVATE KEY-----
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

func TestChefVerifierVerifyVersion11LegacyRequest(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	store := NewMemoryKeyStore()
	if err := store.Put(Key{
		ID: "default",
		Principal: Principal{
			Type:         "user",
			Name:         "silent-bob",
			Organization: "ponyville",
		},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	timestamp := "2026-04-02T15:04:05Z"
	body := []byte(`{"bar":"baz"}`)
	path := "/organizations/ponyville/nodes"

	verifier := NewChefVerifier(store, Options{
		AllowedClockSkew: durationPtr(15 * time.Minute),
		Now: func() time.Time {
			return mustParseTime(t, timestamp).Add(30 * time.Second)
		},
	})

	req := RequestContext{
		Method:       "POST",
		Path:         path,
		Body:         body,
		Organization: "ponyville",
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "POST", path, body, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, timestamp, defaultServerAPIVersion),
	}

	result, err := verifier.Verify(context.Background(), req)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	if !result.Authenticated {
		t.Fatal("Verify() returned unauthenticated result")
	}
	if result.Principal.Name != "silent-bob" {
		t.Fatalf("Principal.Name = %q, want %q", result.Principal.Name, "silent-bob")
	}
	if result.SignVersion != "1.1" {
		t.Fatalf("SignVersion = %q, want %q", result.SignVersion, "1.1")
	}
}

func TestChefVerifierVerifyVersion13Request(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	store := NewMemoryKeyStore()
	if err := store.Put(Key{
		ID: "default",
		Principal: Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	timestamp := "2026-04-02T15:04:05Z"
	body := []byte(`{"run_list":["recipe[foo]"]}`)
	path := "/users"

	verifier := NewChefVerifier(store, Options{
		AllowedClockSkew: durationPtr(15 * time.Minute),
		Now: func() time.Time {
			return mustParseTime(t, timestamp).Add(30 * time.Second)
		},
	})

	req := RequestContext{
		Method: "POST",
		Path:   path,
		Body:   body,
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "POST", path, body, signDescription{
			Version:   "1.3",
			Algorithm: "sha256",
		}, timestamp, defaultServerAPIVersion),
	}

	result, err := verifier.Verify(context.Background(), req)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}

	if !result.Authenticated {
		t.Fatal("Verify() returned unauthenticated result")
	}
	if result.Algorithm != "sha256" {
		t.Fatalf("Algorithm = %q, want %q", result.Algorithm, "sha256")
	}
}

func TestChefVerifierMissingHeaders(t *testing.T) {
	verifier := NewChefVerifier(NewMemoryKeyStore(), Options{})

	_, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   "/users",
		Headers: map[string]string{
			"X-Ops-Userid":       "silent-bob",
			"X-Ops-Timestamp":    "2026-04-02T15:04:05Z",
			"X-Ops-Content-Hash": "abc",
		},
	})
	if err == nil {
		t.Fatal("Verify() error = nil, want non-nil")
	}

	authErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("Verify() error type = %T, want *Error", err)
	}
	if authErr.Kind != ErrorKindMissingHeaders {
		t.Fatalf("Error.Kind = %q, want %q", authErr.Kind, ErrorKindMissingHeaders)
	}
	if authErr.HTTPStatus() != 400 {
		t.Fatalf("HTTPStatus() = %d, want 400", authErr.HTTPStatus())
	}
}

func TestChefVerifierClockSkew(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	store := NewMemoryKeyStore()
	if err := store.Put(Key{
		ID: "default",
		Principal: Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	timestamp := "2026-04-02T15:04:05Z"
	path := "/users"
	body := []byte{}

	verifier := NewChefVerifier(store, Options{
		AllowedClockSkew: durationPtr(15 * time.Minute),
		Now: func() time.Time {
			return mustParseTime(t, timestamp).Add(16 * time.Minute)
		},
	})

	_, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   path,
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "GET", path, body, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, timestamp, defaultServerAPIVersion),
	})
	if err == nil {
		t.Fatal("Verify() error = nil, want non-nil")
	}

	authErr := err.(*Error)
	if authErr.Kind != ErrorKindBadClock {
		t.Fatalf("Error.Kind = %q, want %q", authErr.Kind, ErrorKindBadClock)
	}
}

func TestChefVerifierRequestorNotFound(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	verifier := NewChefVerifier(NewMemoryKeyStore(), Options{
		AllowedClockSkew: durationPtr(15 * time.Minute),
		Now: func() time.Time {
			return mustParseTime(t, "2026-04-02T15:04:35Z")
		},
	})

	_, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   "/users",
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "GET", "/users", nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, "2026-04-02T15:04:05Z", defaultServerAPIVersion),
	})
	if err == nil {
		t.Fatal("Verify() error = nil, want non-nil")
	}

	authErr := err.(*Error)
	if authErr.Kind != ErrorKindRequestorNotFound {
		t.Fatalf("Error.Kind = %q, want %q", authErr.Kind, ErrorKindRequestorNotFound)
	}
}

func TestChefVerifierUsesInjectedClockForKeyExpiration(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	store := NewMemoryKeyStore()
	expiresAt := mustParseTime(t, "2010-01-01T00:00:00Z")
	if err := store.Put(Key{
		ID: "default",
		Principal: Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
		ExpiresAt: &expiresAt,
	}); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	timestamp := "2000-01-01T00:00:00Z"
	verifier := NewChefVerifier(store, Options{
		AllowedClockSkew: durationPtr(15 * time.Minute),
		Now: func() time.Time {
			return mustParseTime(t, timestamp).Add(30 * time.Second)
		},
	})

	result, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   "/users",
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "GET", "/users", nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, timestamp, defaultServerAPIVersion),
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !result.Authenticated {
		t.Fatal("Verify() returned unauthenticated result")
	}
}

func TestParseSignDescriptionAcceptsLegacyAndModernFormats(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		version   string
		algorithm string
	}{
		{name: "version 1.0", raw: "version=1.0", version: "1.0", algorithm: "sha1"},
		{name: "version 1.1", raw: "algorithm=sha1;version=1.1;", version: "1.1", algorithm: "sha1"},
		{name: "version 1.3", raw: "algorithm=sha256;version=1.3", version: "1.3", algorithm: "sha256"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseSignDescription(tt.raw)
			if err != nil {
				t.Fatalf("parseSignDescription() error = %v", err)
			}
			if got.Version != tt.version {
				t.Fatalf("Version = %q, want %q", got.Version, tt.version)
			}
			if got.Algorithm != tt.algorithm {
				t.Fatalf("Algorithm = %q, want %q", got.Algorithm, tt.algorithm)
			}
		})
	}
}

func TestChefVerifierExplicitZeroClockSkewDisablesValidation(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	store := NewMemoryKeyStore()
	if err := store.Put(Key{
		ID: "default",
		Principal: Principal{
			Type: "user",
			Name: "silent-bob",
		},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	timestamp := "2026-04-02T15:04:05Z"
	verifier := NewChefVerifier(store, Options{
		AllowedClockSkew: durationPtr(0),
		Now: func() time.Time {
			return mustParseTime(t, timestamp).Add(6 * time.Hour)
		},
	})

	result, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   "/users",
		Headers: manufactureSignedHeaders(t, privateKey, "silent-bob", "GET", "/users", nil, signDescription{
			Version:   "1.1",
			Algorithm: "sha1",
		}, timestamp, defaultServerAPIVersion),
	})
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if !result.Authenticated {
		t.Fatal("Verify() returned unauthenticated result")
	}
}

func TestChefVerifierParseFailurePreservesUserHint(t *testing.T) {
	verifier := NewChefVerifier(NewMemoryKeyStore(), Options{})

	result, err := verifier.Verify(context.Background(), RequestContext{
		Method: "GET",
		Path:   "/users",
		Headers: map[string]string{
			"X-Ops-Userid":       "silent-bob",
			"X-Ops-Timestamp":    "2026-04-02T15:04:05Z",
			"X-Ops-Content-Hash": "abc",
		},
	})
	if err == nil {
		t.Fatal("Verify() error = nil, want non-nil")
	}
	if result.Principal.Name != "silent-bob" {
		t.Fatalf("Principal.Name = %q, want %q", result.Principal.Name, "silent-bob")
	}
}

func mustParsePrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()

	key, err := ParseRSAPrivateKeyPEM([]byte(signingKeyPEM))
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

func durationPtr(duration time.Duration) *time.Duration {
	return &duration
}

func manufactureSignedHeaders(t *testing.T, privateKey *rsa.PrivateKey, userID, method, path string, body []byte, sign signDescription, timestamp, serverAPIVersion string) map[string]string {
	t.Helper()

	if body == nil {
		body = []byte{}
	}

	contentHash := hashBase64(body, sign)
	req := parsedRequest{
		UserID:           userID,
		Body:             body,
		Method:           method,
		Path:             path,
		ServerAPIVersion: serverAPIVersion,
		TimestampRaw:     timestamp,
		ContentHash:      contentHash,
		Sign:             sign,
	}

	stringToSign := canonicalStringToSign(req)

	var signature []byte
	var err error
	if sign.Version == "1.3" {
		sum := sha256.Sum256([]byte(stringToSign))
		signature, err = rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, sum[:])
	} else {
		signature, err = legacyPrivateEncrypt(privateKey, []byte(stringToSign))
	}
	if err != nil {
		t.Fatalf("sign request: %v", err)
	}

	signatureBase64 := base64.StdEncoding.EncodeToString(signature)
	headers := map[string]string{
		"X-Ops-Sign":         renderSignDescription(sign),
		"X-Ops-Userid":       userID,
		"X-Ops-Timestamp":    timestamp,
		"X-Ops-Content-Hash": contentHash,
	}
	for index, chunk := range splitBase64Signature(signatureBase64) {
		headers["X-Ops-Authorization-"+strconv.Itoa(index+1)] = chunk
	}

	return headers
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

func splitBase64Signature(encoded string) []string {
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

func TestParseRSAPublicKeyPEM(t *testing.T) {
	privateKey := mustParsePrivateKey(t)
	publicDER := x509.MarshalPKCS1PublicKey(&privateKey.PublicKey)
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: publicDER})

	key, err := ParseRSAPublicKeyPEM(publicPEM)
	if err != nil {
		t.Fatalf("ParseRSAPublicKeyPEM() error = %v", err)
	}
	if key.E != privateKey.PublicKey.E {
		t.Fatalf("Public exponent = %d, want %d", key.E, privateKey.PublicKey.E)
	}
}
