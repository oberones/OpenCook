package admin

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
)

func TestConfigFromEnvAndFlags(t *testing.T) {
	env := map[string]string{
		"OPENCOOK_ADMIN_SERVER_URL":         "https://admin.example",
		"OPENCOOK_ADMIN_REQUESTOR_NAME":     "pivotal",
		"OPENCOOK_ADMIN_REQUESTOR_TYPE":     "user",
		"OPENCOOK_ADMIN_PRIVATE_KEY_PATH":   "/keys/pivotal.pem",
		"OPENCOOK_DEFAULT_ORGANIZATION":     "ponyville",
		"OPENCOOK_ADMIN_SERVER_API_VERSION": "2",
	}
	cfg := configFromLookup(func(key string) string {
		return env[key]
	})
	if cfg.DefaultOrg != "ponyville" {
		t.Fatalf("DefaultOrg = %q, want ponyville", cfg.DefaultOrg)
	}

	fs := flag.NewFlagSet("admin-test", flag.ContinueOnError)
	cfg.BindFlags(fs)
	if err := fs.Parse([]string{
		"--server-url", "http://localhost:4000",
		"--requestor-name", "silent-bob",
		"--requestor-type", "client",
		"--private-key", "/keys/client.pem",
		"--default-org", "canterlot",
		"--server-api-version", "1",
	}); err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if cfg.ServerURL != "http://localhost:4000" ||
		cfg.RequestorName != "silent-bob" ||
		cfg.RequestorType != "client" ||
		cfg.PrivateKeyPath != "/keys/client.pem" ||
		cfg.DefaultOrg != "canterlot" ||
		cfg.ServerAPIVersion != "1" {
		t.Fatalf("flagged config = %+v", cfg)
	}
}

func TestClientDoJSONSignsRequestAndDecodesResponse(t *testing.T) {
	privateKey := mustGeneratePrivateKey(t)
	fixedNow := time.Date(2026, 4, 25, 12, 30, 0, 0, time.UTC)
	store := authn.NewMemoryKeyStore()
	if err := store.Put(authn.Key{
		ID:        "default",
		Principal: authn.Principal{Type: "user", Name: "pivotal"},
		PublicKey: &privateKey.PublicKey,
	}); err != nil {
		t.Fatalf("put key: %v", err)
	}
	verifier := authn.NewChefVerifier(store, authn.Options{
		Now: func() time.Time { return fixedNow },
	})

	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", req.Method)
		}
		if req.URL.String() != "http://opencook.local/organizations/ponyville/nodes?pretty=true" {
			t.Fatalf("url = %s", req.URL.String())
		}
		if got := req.Header.Get("Accept"); got != "application/json" {
			t.Fatalf("Accept = %q, want application/json", got)
		}
		if got := req.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", got)
		}
		if got := req.Header.Get("X-Ops-Server-API-Version"); got != "2" {
			t.Fatalf("X-Ops-Server-API-Version = %q, want 2", got)
		}
		if got := req.Header.Get("X-Ops-Timestamp"); got != "2026-04-25T12:30:00Z" {
			t.Fatalf("X-Ops-Timestamp = %q, want fixed timestamp", got)
		}
		for key, values := range req.Header {
			if strings.HasPrefix(key, "X-Ops-Authorization-") && len(values[0]) > 60 {
				t.Fatalf("%s length = %d, want <= 60", key, len(values[0]))
			}
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		result, err := verifier.Verify(req.Context(), authn.RequestContext{
			Method:           req.Method,
			Path:             req.URL.Path,
			Body:             body,
			Headers:          requestHeaders(req),
			Organization:     "ponyville",
			ServerAPIVersion: req.Header.Get("X-Ops-Server-API-Version"),
		})
		if err != nil {
			t.Fatalf("Verify() error = %v", err)
		}
		if !result.Authenticated || result.Principal.Name != "pivotal" || result.SignVersion != "1.3" {
			t.Fatalf("Verify() = %+v, want authenticated pivotal sign v1.3", result)
		}

		return jsonResponse(http.StatusCreated, `{"ok":true,"name":"twilight"}`), nil
	})

	client, err := NewClient(Config{
		ServerURL:        "http://opencook.local",
		RequestorName:    "pivotal",
		RequestorType:    "user",
		ServerAPIVersion: "2",
	}, WithPrivateKey(privateKey), WithNow(func() time.Time { return fixedNow }), WithHTTPDoer(transport))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	var out map[string]any
	err = client.DoJSON(context.Background(), http.MethodPost, "/organizations/ponyville/nodes?pretty=true", map[string]any{"name": "twilight"}, &out)
	if err != nil {
		t.Fatalf("DoJSON() error = %v", err)
	}
	if out["name"] != "twilight" || out["ok"] != true {
		t.Fatalf("decoded response = %v", out)
	}
}

func TestClientDoUnsignedFollowsSignedBlobURLWithoutChefHeaders(t *testing.T) {
	privateKey := mustGeneratePrivateKey(t)
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", req.Method)
		}
		if req.URL.String() != "http://opencook.local/_blob/checksums/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?signature=secret" {
			t.Fatalf("url = %s, want signed blob URL", req.URL.String())
		}
		if got := req.Header.Get("Accept"); got != "*/*" {
			t.Fatalf("Accept = %q, want */*", got)
		}
		for key := range req.Header {
			if strings.HasPrefix(key, "X-Ops-") {
				t.Fatalf("unexpected Chef signing header %s on unsigned download", key)
			}
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("cookbook bytes")),
			Header:     make(http.Header),
		}, nil
	})
	client, err := NewClient(Config{
		ServerURL:     "http://opencook.local",
		RequestorName: "pivotal",
		RequestorType: "user",
	}, WithPrivateKey(privateKey), WithHTTPDoer(transport))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	resp, err := client.DoUnsigned(context.Background(), http.MethodGet, "http://opencook.local/_blob/checksums/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?signature=secret")
	if err != nil {
		t.Fatalf("DoUnsigned() error = %v", err)
	}
	if resp.StatusCode != http.StatusOK || string(resp.Body) != "cookbook bytes" {
		t.Fatalf("DoUnsigned() = status %d body %q, want 200 cookbook bytes", resp.StatusCode, resp.Body)
	}
}

func TestClientReadsPrivateKeyFromPath(t *testing.T) {
	privateKey := mustGeneratePrivateKey(t)
	path := writePrivateKey(t, privateKey)

	client, err := NewClient(Config{
		ServerURL:      "http://opencook.local",
		RequestorName:  "pivotal",
		RequestorType:  "user",
		PrivateKeyPath: path,
	}, WithHTTPDoer(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{}`), nil
	})))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	if client.key == nil {
		t.Fatal("client key = nil, want key loaded from path")
	}
}

func TestClientRedactsProviderAndDecodeErrors(t *testing.T) {
	privateKey := mustGeneratePrivateKey(t)
	for _, tc := range []struct {
		name      string
		transport HTTPDoer
		want      string
	}{
		{
			name: "http failure",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusInternalServerError, `postgres://opencook:secret@postgres private key material`), nil
			}),
			want: "request_failed: GET /users returned HTTP 500",
		},
		{
			name: "decode failure",
			transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return jsonResponse(http.StatusOK, `{"secret":"private key material"`), nil
			}),
			want: "decode_failed: GET /users returned invalid JSON",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewClient(Config{
				ServerURL:     "http://opencook.local",
				RequestorName: "pivotal",
				RequestorType: "user",
			}, WithPrivateKey(privateKey), WithHTTPDoer(tc.transport))
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			var out map[string]any
			err = client.DoJSON(context.Background(), http.MethodGet, "/users?token=super-secret", nil, &out)
			if err == nil {
				t.Fatal("DoJSON() error = nil, want error")
			}
			if got := err.Error(); got != tc.want {
				t.Fatalf("error = %q, want %q", got, tc.want)
			}
			if strings.Contains(err.Error(), "super-secret") ||
				strings.Contains(err.Error(), "postgres://") ||
				strings.Contains(err.Error(), "private key material") {
				t.Fatalf("error leaked sensitive detail: %q", err.Error())
			}
		})
	}
}

func TestClientPrivateKeyErrorsAreRedacted(t *testing.T) {
	_, err := NewClient(Config{
		ServerURL:        "http://opencook.local",
		RequestorName:    "pivotal",
		RequestorType:    "user",
		PrivateKeyPath:   "/tmp/opencook-secret-admin-key.pem",
		DefaultOrg:       "ponyville",
		ServerAPIVersion: "1",
	})
	if err == nil {
		t.Fatal("NewClient() error = nil, want missing private key error")
	}
	if got := err.Error(); got != "invalid_configuration: private key could not be read" {
		t.Fatalf("error = %q, want redacted read failure", got)
	}
	if strings.Contains(err.Error(), "opencook-secret-admin-key.pem") {
		t.Fatalf("error leaked private key path: %q", err.Error())
	}
}

func TestClientRejectsUnsafeRequestPath(t *testing.T) {
	client, err := NewClient(Config{
		ServerURL:     "http://opencook.local",
		RequestorName: "pivotal",
		RequestorType: "user",
	}, WithPrivateKey(mustGeneratePrivateKey(t)))
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	_, err = client.NewJSONRequest(context.Background(), http.MethodGet, "https://evil.example/users", nil)
	if err == nil {
		t.Fatal("NewJSONRequest() error = nil, want absolute path rejection")
	}
	if got := err.Error(); got != "invalid_configuration: request path must be relative" {
		t.Fatalf("error = %q, want relative-path rejection", got)
	}
}

func mustGeneratePrivateKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	return key
}

func writePrivateKey(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	path := t.TempDir() + "/admin.pem"
	data := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}
	return path
}

func requestHeaders(req *http.Request) map[string]string {
	headers := make(map[string]string, len(req.Header))
	for key, values := range req.Header {
		if len(values) == 0 {
			continue
		}
		headers[key] = values[0]
	}
	return headers
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) Do(req *http.Request) (*http.Response, error) {
	return f(req)
}
