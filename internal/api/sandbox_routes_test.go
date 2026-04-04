package api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestSandboxesEndpointCreateUploadCommitAndReuseChecksum(t *testing.T) {
	router := newTestRouter(t)

	content := []byte("friendship is content-addressed")
	checksum := checksumHex(content)
	createBody := mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	})

	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", createBody)
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	sandboxID := createPayload["sandbox_id"].(string)
	if sandboxID == "" {
		t.Fatal("sandbox_id = empty, want non-empty id")
	}
	if createPayload["uri"] != "http://example.com/sandboxes/"+sandboxID {
		t.Fatalf("uri = %v, want %q", createPayload["uri"], "http://example.com/sandboxes/"+sandboxID)
	}

	checksums := createPayload["checksums"].(map[string]any)
	checksumEntry := checksums[checksum].(map[string]any)
	if checksumEntry["needs_upload"] != true {
		t.Fatalf("needs_upload = %v, want true", checksumEntry["needs_upload"])
	}
	uploadURL := checksumEntry["url"].(string)
	if !strings.HasPrefix(uploadURL, "http://example.com/_blob/checksums/"+checksum) {
		t.Fatalf("upload url = %q, want checksum upload URL", uploadURL)
	}

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadReq.Header.Set("Content-Type", "application/x-binary")
	uploadReq.Header.Set("Content-MD5", checksumBase64(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusNoContent {
		t.Fatalf("upload checksum status = %d, want %d, body = %s", uploadRec.Code, http.StatusNoContent, uploadRec.Body.String())
	}
	if uploadRec.Body.Len() != 0 {
		t.Fatalf("upload body length = %d, want 0", uploadRec.Body.Len())
	}

	commitBody := mustMarshalSandboxJSON(t, map[string]any{"is_completed": true})
	commitReq := newSignedJSONRequest(t, http.MethodPut, "/sandboxes/"+sandboxID, commitBody)
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusOK {
		t.Fatalf("commit sandbox status = %d, want %d, body = %s", commitRec.Code, http.StatusOK, commitRec.Body.String())
	}

	var commitPayload map[string]any
	if err := json.Unmarshal(commitRec.Body.Bytes(), &commitPayload); err != nil {
		t.Fatalf("json.Unmarshal(commit sandbox) error = %v", err)
	}
	if commitPayload["guid"] != sandboxID || commitPayload["name"] != sandboxID {
		t.Fatalf("guid/name = %v/%v, want %q", commitPayload["guid"], commitPayload["name"], sandboxID)
	}
	if commitPayload["is_completed"] != true {
		t.Fatalf("is_completed = %v, want true", commitPayload["is_completed"])
	}
	committedChecksums := stringSliceFromAny(t, commitPayload["checksums"])
	if len(committedChecksums) != 1 || committedChecksums[0] != checksum {
		t.Fatalf("checksums = %v, want [%s]", committedChecksums, checksum)
	}

	reuseReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", createBody)
	reuseRec := httptest.NewRecorder()
	router.ServeHTTP(reuseRec, reuseReq)
	if reuseRec.Code != http.StatusCreated {
		t.Fatalf("reuse sandbox status = %d, want %d, body = %s", reuseRec.Code, http.StatusCreated, reuseRec.Body.String())
	}

	var reusePayload map[string]any
	if err := json.Unmarshal(reuseRec.Body.Bytes(), &reusePayload); err != nil {
		t.Fatalf("json.Unmarshal(reuse sandbox) error = %v", err)
	}
	reuseEntry := reusePayload["checksums"].(map[string]any)[checksum].(map[string]any)
	if reuseEntry["needs_upload"] != false {
		t.Fatalf("reused needs_upload = %v, want false", reuseEntry["needs_upload"])
	}
	if _, ok := reuseEntry["url"]; ok {
		t.Fatalf("reused checksum unexpectedly included url: %v", reuseEntry)
	}
}

func TestSandboxesEndpointRejectsInvalidPayloads(t *testing.T) {
	router := newTestRouter(t)

	tests := []struct {
		name    string
		payload map[string]any
		message string
	}{
		{
			name:    "missing checksums",
			payload: map[string]any{},
			message: "Field 'checksums' missing",
		},
		{
			name: "empty checksums",
			payload: map[string]any{
				"checksums": map[string]any{},
			},
			message: "Bad checksums!",
		},
		{
			name: "non nil checksum value",
			payload: map[string]any{
				"checksums": map[string]any{
					"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": "not-null",
				},
			},
			message: "Bad checksums!",
		},
		{
			name: "invalid checksum text",
			payload: map[string]any{
				"checksums": map[string]any{
					"not-a-checksum": nil,
				},
			},
			message: "Bad checksums!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, tt.payload))
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			var payload map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			messages := payload["error"].([]any)
			if len(messages) != 1 || messages[0] != tt.message {
				t.Fatalf("error messages = %v, want %q", messages, tt.message)
			}
		})
	}
}

func TestSandboxesEndpointCommitIncompleteReturns503(t *testing.T) {
	router := newTestRouter(t)

	checksum := checksumHex([]byte("rainbow dash"))
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	sandboxID := createPayload["sandbox_id"].(string)

	commitReq := newSignedJSONRequest(t, http.MethodPut, "/sandboxes/"+sandboxID, mustMarshalSandboxJSON(t, map[string]any{"is_completed": true}))
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("commit incomplete sandbox status = %d, want %d, body = %s", commitRec.Code, http.StatusServiceUnavailable, commitRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(commitRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(commit incomplete) error = %v", err)
	}
	messages := payload["error"].([]any)
	want := "Cannot update sandbox " + sandboxID + ": the following checksums have not been uploaded: " + checksum
	if len(messages) != 1 || messages[0] != want {
		t.Fatalf("error messages = %v, want %q", messages, want)
	}
}

func TestSandboxesEndpointOrgScopedAliasUsesOrgScopedURI(t *testing.T) {
	router := newTestRouter(t)

	checksum := checksumHex([]byte("twilight sparkle"))
	createReq := newSignedJSONRequest(t, http.MethodPost, "/organizations/ponyville/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("org-scoped create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(org-scoped sandbox) error = %v", err)
	}
	sandboxID := createPayload["sandbox_id"].(string)
	if createPayload["uri"] != "http://example.com/organizations/ponyville/sandboxes/"+sandboxID {
		t.Fatalf("uri = %v, want org-scoped sandbox uri", createPayload["uri"])
	}

	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)
	parsed, err := url.Parse(uploadURL)
	if err != nil {
		t.Fatalf("url.Parse(uploadURL) error = %v", err)
	}
	if parsed.Query().Get("org") != "ponyville" {
		t.Fatalf("upload url org query = %q, want %q", parsed.Query().Get("org"), "ponyville")
	}
}

func TestBlobChecksumUploadRejectsChecksumMismatch(t *testing.T) {
	router := newTestRouter(t)

	content := []byte("applejack")
	checksum := checksumHex(content)
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader([]byte("rarity")))
	uploadReq.Header.Set("Content-Type", "application/x-binary")
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusBadRequest {
		t.Fatalf("upload mismatch status = %d, want %d, body = %s", uploadRec.Code, http.StatusBadRequest, uploadRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(uploadRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(upload mismatch) error = %v", err)
	}
	if payload["error"] != "invalid_checksum" {
		t.Fatalf("error = %v, want %q", payload["error"], "invalid_checksum")
	}
}

func TestSandboxesEndpointCommitRejectsFalseIsCompletedWithCorrectFieldName(t *testing.T) {
	router := newTestRouter(t)

	checksum := checksumHex([]byte("fluttershy"))
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	sandboxID := createPayload["sandbox_id"].(string)

	commitReq := newSignedJSONRequest(t, http.MethodPut, "/sandboxes/"+sandboxID, mustMarshalSandboxJSON(t, map[string]any{
		"is_completed": false,
	}))
	commitRec := httptest.NewRecorder()
	router.ServeHTTP(commitRec, commitReq)
	if commitRec.Code != http.StatusBadRequest {
		t.Fatalf("commit status = %d, want %d, body = %s", commitRec.Code, http.StatusBadRequest, commitRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(commitRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(commit sandbox) error = %v", err)
	}
	messages := payload["error"].([]any)
	want := `JSON body must contain key "is_completed" with value true.`
	if len(messages) != 1 || messages[0] != want {
		t.Fatalf("error messages = %v, want %q", messages, want)
	}
}

func TestSandboxesEndpointCommitUsesUpdateAuthorization(t *testing.T) {
	fixedNow := mustParseTime(t, "2026-04-02T15:04:35Z")
	now := func() time.Time { return fixedNow }
	authorizer := &sandboxActionAuthorizer{allowed: map[authz.Action]bool{
		authz.ActionUpdate: true,
	}}
	router, state := newSandboxRouterWithAuthorizer(t, now, authorizer)

	sandbox, err := state.CreateSandbox("ponyville", bootstrap.CreateSandboxInput{
		Checksums: []string{checksumHex([]byte("pinkie pie"))},
	})
	if err != nil {
		t.Fatalf("CreateSandbox() error = %v", err)
	}

	req := newSignedJSONRequest(t, http.MethodPut, "/sandboxes/"+sandbox.ID, mustMarshalSandboxJSON(t, map[string]any{
		"is_completed": true,
	}))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if authorizer.lastAction != authz.ActionUpdate {
		t.Fatalf("lastAction = %q, want %q", authorizer.lastAction, authz.ActionUpdate)
	}
}

func TestBlobChecksumUploadRequiresSignedUploadURL(t *testing.T) {
	router := newTestRouter(t)

	content := []byte("starlight glimmer")
	checksum := checksumHex(content)
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)
	parsed, err := url.Parse(uploadURL)
	if err != nil {
		t.Fatalf("url.Parse(uploadURL) error = %v", err)
	}

	values := parsed.Query()
	values.Del("signature")
	parsed.RawQuery = values.Encode()

	uploadReq := httptest.NewRequest(http.MethodPut, parsed.String(), bytes.NewReader(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusBadRequest && uploadRec.Code != http.StatusForbidden {
		t.Fatalf("upload missing signature status = %d, want 400 or 403, body = %s", uploadRec.Code, uploadRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(uploadRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(upload missing signature) error = %v", err)
	}
	if payload["error"] != "invalid_upload_url" {
		t.Fatalf("error = %v, want %q", payload["error"], "invalid_upload_url")
	}
}

func TestBlobChecksumUploadRejectsExpiredUploadURL(t *testing.T) {
	currentNow := mustParseTime(t, "2026-04-02T15:04:35Z")
	now := func() time.Time { return currentNow }
	router, _ := newSandboxRouterWithAuthorizer(t, now, &sandboxActionAuthorizer{allowed: map[authz.Action]bool{
		authz.ActionCreate: true,
		authz.ActionUpdate: true,
		authz.ActionRead:   true,
	}})

	content := []byte("sunset shimmer")
	checksum := checksumHex(content)
	createReq := newSignedJSONRequest(t, http.MethodPost, "/sandboxes", mustMarshalSandboxJSON(t, map[string]any{
		"checksums": map[string]any{
			checksum: nil,
		},
	}))
	createRec := httptest.NewRecorder()
	router.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusCreated {
		t.Fatalf("create sandbox status = %d, want %d, body = %s", createRec.Code, http.StatusCreated, createRec.Body.String())
	}

	var createPayload map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createPayload); err != nil {
		t.Fatalf("json.Unmarshal(create sandbox) error = %v", err)
	}
	uploadURL := createPayload["checksums"].(map[string]any)[checksum].(map[string]any)["url"].(string)

	currentNow = currentNow.Add(defaultBlobUploadURLTTL + time.Second)

	uploadReq := httptest.NewRequest(http.MethodPut, uploadURL, bytes.NewReader(content))
	uploadRec := httptest.NewRecorder()
	router.ServeHTTP(uploadRec, uploadReq)
	if uploadRec.Code != http.StatusForbidden {
		t.Fatalf("upload expired status = %d, want %d, body = %s", uploadRec.Code, http.StatusForbidden, uploadRec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(uploadRec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal(upload expired) error = %v", err)
	}
	if payload["error"] != "invalid_upload_url" {
		t.Fatalf("error = %v, want %q", payload["error"], "invalid_upload_url")
	}
}

type sandboxActionAuthorizer struct {
	allowed    map[authz.Action]bool
	lastAction authz.Action
}

func (a *sandboxActionAuthorizer) Name() string {
	return "sandbox-action-authorizer"
}

func (a *sandboxActionAuthorizer) Authorize(_ context.Context, _ authz.Subject, action authz.Action, _ authz.Resource) (authz.Decision, error) {
	a.lastAction = action
	return authz.Decision{
		Allowed: a.allowed[action],
		Reason:  "test authorizer",
	}, nil
}

func newSandboxRouterWithAuthorizer(t *testing.T, now func() time.Time, authorizer authz.Authorizer) (http.Handler, *bootstrap.Service) {
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

	state := bootstrap.NewService(store, bootstrap.Options{SuperuserName: "pivotal"})
	publicKeyPEM := mustMarshalPublicKeyPEM(t, &privateKey.PublicKey)
	state.SeedPrincipal(authn.Principal{Type: "user", Name: "silent-bob"})
	if err := state.SeedPublicKey(authn.Principal{Type: "user", Name: "silent-bob"}, "default", publicKeyPEM); err != nil {
		t.Fatalf("SeedPublicKey(silent-bob) error = %v", err)
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

	skew := 15 * time.Minute
	router := NewRouter(Dependencies{
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
		Authz:            authorizer,
		Bootstrap:        state,
		Blob:             blob.NewMemoryStore(""),
		BlobUploadSecret: []byte("test-blob-upload-secret"),
		Search:           search.NewMemoryIndex(state, ""),
		Postgres:         pg.New(""),
	})

	return router, state
}

func mustMarshalSandboxJSON(t *testing.T, payload map[string]any) []byte {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return body
}

func checksumHex(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}

func checksumBase64(body []byte) string {
	sum := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}
