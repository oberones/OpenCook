package functional

import (
	"bytes"
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	policyName       = "appserver"
	policyGroupName  = "dev"
	policyRevisionID = "1111111111111111111111111111111111111111"
)

var sandboxBlob = []byte("opencook functional sandbox blob\n")

type functionalConfig struct {
	baseURL        *url.URL
	actorName      string
	privateKeyPath string
	org            string
	stateDir       string
	phase          string
}

type functionalClient struct {
	httpClient       *http.Client
	baseURL          *url.URL
	actorName        string
	serverAPIVersion string
	privateKey       *rsa.PrivateKey
}

type apiResponse struct {
	Status int
	Body   []byte
	JSON   any
}

func TestFunctional(t *testing.T) {
	cfg := loadFunctionalConfig(t)
	client := newFunctionalClient(t, cfg)

	phases := []string{cfg.phase}
	if cfg.phase == "all" {
		phases = []string{"create", "verify", "invalid", "delete", "verify-deleted"}
	}

	for _, phase := range phases {
		phase := phase
		t.Run(phase, func(t *testing.T) {
			client.requireReady(t)
			switch phase {
			case "create":
				runCreatePhase(t, client, cfg)
			case "verify":
				runVerifyPhase(t, client, cfg)
			case "invalid":
				runInvalidPhase(t, client, cfg)
			case "delete":
				runDeletePhase(t, client, cfg)
			case "verify-deleted":
				runVerifyDeletedPhase(t, client, cfg)
			default:
				t.Fatalf("unsupported functional phase %q", phase)
			}
		})
	}
}

func loadFunctionalConfig(t *testing.T) functionalConfig {
	t.Helper()

	rawBaseURL := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_BASE_URL"))
	if rawBaseURL == "" {
		t.Skip("OPENCOOK_FUNCTIONAL_BASE_URL is not set; functional Compose stack is not active")
	}

	baseURL, err := url.Parse(rawBaseURL)
	if err != nil {
		t.Fatalf("parse OPENCOOK_FUNCTIONAL_BASE_URL: %v", err)
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		t.Fatalf("OPENCOOK_FUNCTIONAL_BASE_URL must include scheme and host, got %q", rawBaseURL)
	}

	privateKeyPath := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_PRIVATE_KEY_PATH"))
	if privateKeyPath == "" {
		privateKeyPath = "test/functional/fixtures/bootstrap_private.pem"
	}

	org := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_ORG"))
	if org == "" {
		org = "ponyville"
	}

	actorName := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_ACTOR_NAME"))
	if actorName == "" {
		actorName = "pivotal"
	}

	stateDir := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_STATE_DIR"))
	if stateDir == "" {
		stateDir = filepath.Join(os.TempDir(), "opencook-functional")
	}

	phase := strings.TrimSpace(os.Getenv("OPENCOOK_FUNCTIONAL_PHASE"))
	if phase == "" {
		phase = "verify"
	}

	return functionalConfig{
		baseURL:        baseURL,
		actorName:      actorName,
		privateKeyPath: privateKeyPath,
		org:            org,
		stateDir:       stateDir,
		phase:          phase,
	}
}

func newFunctionalClient(t *testing.T, cfg functionalConfig) *functionalClient {
	t.Helper()

	privateKey := parsePrivateKeyFile(t, cfg.privateKeyPath)
	return &functionalClient{
		httpClient:       &http.Client{Timeout: 15 * time.Second},
		baseURL:          cfg.baseURL,
		actorName:        cfg.actorName,
		serverAPIVersion: "0",
		privateKey:       privateKey,
	}
}

func runCreatePhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	ensureOrganization(t, client, cfg.org)
	requireOrganizationBootstrap(t, client, cfg.org)

	client.expectJSON(t, http.MethodPost, "/environments", environmentPayload("production"), http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/roles", rolePayload("web"), http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/nodes", nodePayload("twilight", "production"), http.StatusCreated, http.StatusConflict)

	client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/data", map[string]any{"name": "ponies"}, http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/data/ponies", map[string]any{
		"id":   "twilight",
		"kind": "unicorn",
		"nested": map[string]any{
			"assistant": "spike",
		},
	}, http.StatusCreated, http.StatusConflict)

	client.expectJSON(t, http.MethodPut, "/policy_groups/"+policyGroupName+"/policies/"+policyName, policyPayload(policyName, policyRevisionID), http.StatusCreated, http.StatusOK)
	createPendingSandbox(t, client, cfg)
}

func runVerifyPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	requireOrganizationBootstrap(t, client, cfg.org)

	orgPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org, nil, http.StatusOK).JSON)
	if orgPayload["name"] != cfg.org {
		t.Fatalf("organization name = %v, want %q", orgPayload["name"], cfg.org)
	}

	envPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/environments/production", nil, http.StatusOK).JSON)
	if envPayload["name"] != "production" {
		t.Fatalf("environment name = %v, want production", envPayload["name"])
	}

	nodePayload := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/nodes/twilight", nil, http.StatusOK).JSON)
	if nodePayload["chef_environment"] != "production" {
		t.Fatalf("node chef_environment = %v, want production", nodePayload["chef_environment"])
	}

	rolePayload := asMap(t, client.expectJSON(t, http.MethodGet, "/roles/web", nil, http.StatusOK).JSON)
	if rolePayload["name"] != "web" {
		t.Fatalf("role name = %v, want web", rolePayload["name"])
	}

	envNodes := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/environments/production/nodes", nil, http.StatusOK).JSON)
	if _, ok := envNodes["twilight"]; !ok {
		t.Fatalf("production environment nodes = %v, want twilight", envNodes)
	}

	envRole := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/environments/production/roles/web", nil, http.StatusOK).JSON)
	if envRole["run_list"] == nil {
		t.Fatalf("environment role payload missing run_list: %v", envRole)
	}

	itemPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/data/ponies/twilight", nil, http.StatusOK).JSON)
	if itemPayload["kind"] != "unicorn" {
		t.Fatalf("data bag item kind = %v, want unicorn", itemPayload["kind"])
	}

	searchNode := asMap(t, client.expectJSON(t, http.MethodGet, "/search/node?q=name:twilight", nil, http.StatusOK).JSON)
	requireRows(t, searchNode, 1)
	searchBag := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/search/ponies?q=id:twilight", nil, http.StatusOK).JSON)
	requireRows(t, searchBag, 1)

	assignmentPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/policy_groups/"+policyGroupName+"/policies/"+policyName, nil, http.StatusOK).JSON)
	if assignmentPayload["revision_id"] != policyRevisionID {
		t.Fatalf("policy assignment revision_id = %v, want %s", assignmentPayload["revision_id"], policyRevisionID)
	}
	client.expectJSON(t, http.MethodGet, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusOK)

	commitPendingSandboxIfNeeded(t, client, cfg)
	requireSandboxBlobReuse(t, client, cfg.org)
}

func runInvalidPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	beforeNode := asMap(t, client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusOK).JSON)
	beforeData := asMap(t, client.expectJSON(t, http.MethodGet, "/data/ponies/twilight", nil, http.StatusOK).JSON)

	client.expectJSON(t, http.MethodPut, "/nodes/twilight", map[string]any{
		"name":       "rainbow",
		"json_class": "Chef::Node",
	}, http.StatusBadRequest)
	client.expectJSON(t, http.MethodPut, "/organizations/"+cfg.org+"/roles/web", map[string]any{
		"name":       "db",
		"json_class": "Chef::Role",
		"chef_type":  "role",
	}, http.StatusBadRequest)
	client.expectJSON(t, http.MethodPut, "/data/ponies/twilight", map[string]any{
		"id": "rainbow",
	}, http.StatusBadRequest)
	client.expectJSON(t, http.MethodPut, "/policy_groups/"+policyGroupName+"/policies/"+policyName, map[string]any{
		"name":        "wrong-policy",
		"revision_id": policyRevisionID,
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9", "version": "1.2.3"},
		},
	}, http.StatusBadRequest)

	afterNode := asMap(t, client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusOK).JSON)
	if afterNode["name"] != beforeNode["name"] || afterNode["chef_environment"] != beforeNode["chef_environment"] {
		t.Fatalf("node changed after invalid writes: before=%v after=%v", beforeNode, afterNode)
	}
	afterData := asMap(t, client.expectJSON(t, http.MethodGet, "/data/ponies/twilight", nil, http.StatusOK).JSON)
	if afterData["id"] != beforeData["id"] || afterData["kind"] != beforeData["kind"] {
		t.Fatalf("data bag item changed after invalid writes: before=%v after=%v", beforeData, afterData)
	}
}

func runDeletePhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	client.expectJSON(t, http.MethodDelete, "/policy_groups/"+policyGroupName+"/policies/"+policyName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/policy_groups/"+policyGroupName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusOK, http.StatusNotFound)

	client.expectJSON(t, http.MethodDelete, "/data/ponies/twilight", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/data/ponies", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/roles/web", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/environments/production", nil, http.StatusOK, http.StatusNotFound)
}

func runVerifyDeletedPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/roles/web", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/environments/production", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/data/ponies", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/policy_groups/"+policyGroupName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org, nil, http.StatusOK)
}

func requireOperationalStatus(t *testing.T, client *functionalClient) {
	t.Helper()

	payload := asMap(t, client.expectUnsignedJSON(t, "/_status", http.StatusOK).JSON)
	deps := asMap(t, payload["dependencies"])
	postgres := asMap(t, deps["postgres"])
	if postgres["configured"] != true || !strings.Contains(fmt.Sprint(postgres["message"]), "active") {
		t.Fatalf("postgres status = %v, want active configured postgres", postgres)
	}
	blob := asMap(t, deps["blob"])
	if blob["backend"] != "filesystem" || blob["configured"] != true {
		t.Fatalf("blob status = %v, want filesystem configured", blob)
	}
	search := asMap(t, deps["opensearch"])
	if search["configured"] != true {
		t.Fatalf("opensearch status = %v, want configured search adapter", search)
	}
}

func ensureOrganization(t *testing.T, client *functionalClient, org string) {
	t.Helper()

	resp := client.expectJSON(t, http.MethodPost, "/organizations", map[string]any{
		"name":      org,
		"full_name": "Ponyville",
		"org_type":  "Business",
	}, http.StatusCreated, http.StatusConflict)
	if resp.Status == http.StatusCreated {
		payload := asMap(t, resp.JSON)
		if payload["clientname"] != org+"-validator" {
			t.Fatalf("validator clientname = %v, want %s-validator", payload["clientname"], org)
		}
	}
}

func requireOrganizationBootstrap(t *testing.T, client *functionalClient, org string) {
	t.Helper()

	groups := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+org+"/groups", nil, http.StatusOK).JSON)
	if _, ok := groups["admins"]; !ok {
		t.Fatalf("groups = %v, want admins", groups)
	}
	containers := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+org+"/containers", nil, http.StatusOK).JSON)
	if _, ok := containers["nodes"]; !ok {
		t.Fatalf("containers = %v, want nodes", containers)
	}
	client.expectJSON(t, http.MethodGet, "/organizations/"+org+"/containers/nodes/_acl", nil, http.StatusOK)
	client.expectJSON(t, http.MethodGet, "/organizations/"+org+"/_acl", nil, http.StatusOK)
}

func createPendingSandbox(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	checksum := checksumHex(sandboxBlob)
	resp := client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/sandboxes", map[string]any{
		"checksums": map[string]any{checksum: nil},
	}, http.StatusCreated)

	payload := asMap(t, resp.JSON)
	sandboxID, _ := payload["sandbox_id"].(string)
	if sandboxID == "" {
		t.Fatalf("sandbox_id missing from create response: %v", payload)
	}
	checksums := asMap(t, payload["checksums"])
	entry := asMap(t, checksums[checksum])
	uploadURL, _ := entry["url"].(string)
	switch entry["needs_upload"] {
	case true:
		if uploadURL == "" {
			t.Fatalf("sandbox checksum entry = %v, want upload URL", entry)
		}
		uploadResp := client.doUnsigned(t, http.MethodPut, uploadURL, sandboxBlob, map[string]string{
			"Content-Type": "application/x-binary",
			"Content-MD5":  checksumBase64(sandboxBlob),
		}, http.StatusNoContent)
		if len(uploadResp.Body) != 0 {
			t.Fatalf("sandbox upload body = %q, want empty", uploadResp.Body)
		}
	case false:
		if uploadURL != "" {
			t.Fatalf("sandbox checksum entry = %v, did not need upload but included URL", entry)
		}
	default:
		t.Fatalf("sandbox checksum entry = %v, want needs_upload boolean", entry)
	}

	if err := os.MkdirAll(cfg.stateDir, 0o755); err != nil {
		t.Fatalf("create state dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.stateDir, "sandbox_id"), []byte(sandboxID), 0o644); err != nil {
		t.Fatalf("write sandbox_id state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cfg.stateDir, "sandbox_checksum"), []byte(checksum), 0o644); err != nil {
		t.Fatalf("write sandbox_checksum state: %v", err)
	}
	_ = os.Remove(filepath.Join(cfg.stateDir, "sandbox_committed"))
}

func commitPendingSandboxIfNeeded(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	committedPath := filepath.Join(cfg.stateDir, "sandbox_committed")
	if _, err := os.Stat(committedPath); err == nil {
		return
	}

	sandboxID := strings.TrimSpace(readStateFile(t, cfg.stateDir, "sandbox_id"))
	if sandboxID == "" {
		t.Fatalf("sandbox_id state is empty")
	}

	resp := client.expectJSON(t, http.MethodPut, "/organizations/"+cfg.org+"/sandboxes/"+sandboxID, map[string]any{
		"is_completed": true,
	}, http.StatusOK)
	payload := asMap(t, resp.JSON)
	if payload["guid"] != sandboxID || payload["is_completed"] != true {
		t.Fatalf("commit sandbox payload = %v, want completed %s", payload, sandboxID)
	}
	if err := os.WriteFile(committedPath, []byte(time.Now().UTC().Format(time.RFC3339)), 0o644); err != nil {
		t.Fatalf("write sandbox_committed state: %v", err)
	}
}

func requireSandboxBlobReuse(t *testing.T, client *functionalClient, org string) {
	t.Helper()

	checksum := checksumHex(sandboxBlob)
	resp := client.expectJSON(t, http.MethodPost, "/organizations/"+org+"/sandboxes", map[string]any{
		"checksums": map[string]any{checksum: nil},
	}, http.StatusCreated)
	payload := asMap(t, resp.JSON)
	sandboxID, _ := payload["sandbox_id"].(string)
	checksums := asMap(t, payload["checksums"])
	entry := asMap(t, checksums[checksum])
	if entry["needs_upload"] != false {
		t.Fatalf("reused sandbox checksum entry = %v, want needs_upload=false", entry)
	}
	client.expectJSON(t, http.MethodPut, "/organizations/"+org+"/sandboxes/"+sandboxID, map[string]any{"is_completed": true}, http.StatusOK)
}

func readStateFile(t *testing.T, dir, name string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s state: %v", name, err)
	}
	return string(data)
}

func environmentPayload(name string) map[string]any {
	return map[string]any{
		"name":                name,
		"json_class":          "Chef::Environment",
		"chef_type":           "environment",
		"description":         "functional test environment",
		"cookbook_versions":   map[string]string{},
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
	}
}

func nodePayload(name, environment string) map[string]any {
	return map[string]any{
		"name":             name,
		"json_class":       "Chef::Node",
		"chef_type":        "node",
		"chef_environment": environment,
		"override":         map[string]any{},
		"normal":           map[string]any{"functional": true},
		"default":          map[string]any{},
		"automatic":        map[string]any{},
		"run_list":         []string{"recipe[web]"},
	}
}

func rolePayload(name string) map[string]any {
	return map[string]any{
		"name":                name,
		"description":         "functional test role",
		"json_class":          "Chef::Role",
		"chef_type":           "role",
		"default_attributes":  map[string]any{},
		"override_attributes": map[string]any{},
		"run_list":            []string{"recipe[base]"},
		"env_run_lists": map[string][]string{
			"production": []string{"recipe[nginx]"},
		},
	}
}

func policyPayload(name, revisionID string) map[string]any {
	return map[string]any{
		"name":        name,
		"revision_id": revisionID,
		"run_list":    []any{"recipe[policyfile_demo::default]"},
		"cookbook_locks": map[string]any{
			"policyfile_demo": map[string]any{
				"identifier": "f04cc40faf628253fe7d9566d66a1733fb1afbe9",
				"version":    "1.2.3",
			},
		},
	}
}

func (c *functionalClient) requireReady(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := c.unsignedRequest(http.MethodGet, "/readyz", nil, nil)
		if err == nil && resp.Status == http.StatusOK {
			return
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status %d body %s", resp.Status, string(resp.Body))
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("OpenCook did not become ready: %v", lastErr)
}

func (c *functionalClient) expectJSON(t *testing.T, method, path string, payload any, wantStatuses ...int) apiResponse {
	t.Helper()

	body := []byte(nil)
	if payload != nil {
		var err error
		body, err = json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal request payload for %s %s: %v", method, path, err)
		}
	}

	resp, err := c.signedRequest(method, path, body)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, path, err)
	}
	requireStatus(t, resp, wantStatuses...)
	decodeJSONBody(t, &resp)
	return resp
}

func (c *functionalClient) expectUnsignedJSON(t *testing.T, path string, wantStatuses ...int) apiResponse {
	t.Helper()

	resp, err := c.unsignedRequest(http.MethodGet, path, nil, nil)
	if err != nil {
		t.Fatalf("GET %s failed: %v", path, err)
	}
	requireStatus(t, resp, wantStatuses...)
	decodeJSONBody(t, &resp)
	return resp
}

func (c *functionalClient) doUnsigned(t *testing.T, method, rawURL string, body []byte, headers map[string]string, wantStatuses ...int) apiResponse {
	t.Helper()

	resp, err := c.unsignedRequest(method, rawURL, body, headers)
	if err != nil {
		t.Fatalf("%s %s failed: %v", method, rawURL, err)
	}
	requireStatus(t, resp, wantStatuses...)
	return resp
}

func (c *functionalClient) signedRequest(method, path string, body []byte) (apiResponse, error) {
	req, err := http.NewRequest(method, c.resolveURL(path), bytes.NewReader(body))
	if err != nil {
		return apiResponse{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if err := c.sign(req, body); err != nil {
		return apiResponse{}, err
	}
	return c.do(req)
}

func (c *functionalClient) unsignedRequest(method, rawURL string, body []byte, headers map[string]string) (apiResponse, error) {
	req, err := http.NewRequest(method, c.resolveURL(rawURL), bytes.NewReader(body))
	if err != nil {
		return apiResponse{}, err
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	return c.do(req)
}

func (c *functionalClient) do(req *http.Request) (apiResponse, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return apiResponse{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return apiResponse{}, err
	}
	return apiResponse{Status: resp.StatusCode, Body: body}, nil
}

func (c *functionalClient) resolveURL(raw string) string {
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	rel, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return c.baseURL.ResolveReference(rel).String()
}

func (c *functionalClient) sign(req *http.Request, body []byte) error {
	if body == nil {
		body = []byte{}
	}

	contentSum := sha256.Sum256(body)
	contentHash := base64.StdEncoding.EncodeToString(contentSum[:])
	timestamp := time.Now().UTC().Format(time.RFC3339)
	signPath := req.URL.Path
	if signPath == "" {
		signPath = "/"
	}

	stringToSign := strings.Join([]string{
		"Method:" + strings.ToUpper(req.Method),
		"Path:" + signPath,
		"X-Ops-Content-Hash:" + contentHash,
		"X-Ops-Sign:version=1.3",
		"X-Ops-Timestamp:" + timestamp,
		"X-Ops-UserId:" + c.actorName,
		"X-Ops-Server-API-Version:" + c.serverAPIVersion,
	}, "\n")

	signatureSum := sha256.Sum256([]byte(stringToSign))
	signature, err := rsa.SignPKCS1v15(rand.Reader, c.privateKey, crypto.SHA256, signatureSum[:])
	if err != nil {
		return err
	}
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)

	req.Header.Set("X-Ops-Sign", "algorithm=sha256;version=1.3")
	req.Header.Set("X-Ops-Userid", c.actorName)
	req.Header.Set("X-Ops-Timestamp", timestamp)
	req.Header.Set("X-Ops-Content-Hash", contentHash)
	req.Header.Set("X-Ops-Server-API-Version", c.serverAPIVersion)
	for index, chunk := range splitBase64(signatureBase64, 60) {
		req.Header.Set(fmt.Sprintf("X-Ops-Authorization-%d", index+1), chunk)
	}
	return nil
}

func requireStatus(t *testing.T, resp apiResponse, wantStatuses ...int) {
	t.Helper()

	for _, status := range wantStatuses {
		if resp.Status == status {
			return
		}
	}
	t.Fatalf("status = %d, want one of %v, body = %s", resp.Status, wantStatuses, string(resp.Body))
}

func decodeJSONBody(t *testing.T, resp *apiResponse) {
	t.Helper()

	if len(bytes.TrimSpace(resp.Body)) == 0 {
		return
	}
	var decoded any
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		t.Fatalf("decode response JSON for status %d: %v body=%s", resp.Status, err, string(resp.Body))
	}
	resp.JSON = decoded
}

func asMap(t *testing.T, value any) map[string]any {
	t.Helper()

	out, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("value = %T %v, want JSON object", value, value)
	}
	return out
}

func requireRows(t *testing.T, payload map[string]any, want int) {
	t.Helper()

	rows, ok := payload["rows"].([]any)
	if !ok || len(rows) != want {
		t.Fatalf("rows = %T %v, want %d rows", payload["rows"], payload["rows"], want)
	}
}

func parsePrivateKeyFile(t *testing.T, path string) *rsa.PrivateKey {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read private key %s: %v", path, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("decode private key %s: no PEM block", path)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse private key %s: %v", path, err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		t.Fatalf("private key %s has type %T, want RSA", path, parsed)
	}
	return key
}

func checksumHex(body []byte) string {
	sum := md5.Sum(body)
	return hex.EncodeToString(sum[:])
}

func checksumBase64(body []byte) string {
	sum := md5.Sum(body)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func splitBase64(encoded string, width int) []string {
	if width <= 0 {
		return []string{encoded}
	}
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
