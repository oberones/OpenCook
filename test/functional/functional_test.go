package functional

import (
	"bytes"
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/oberones/OpenCook/internal/testfixtures"
)

const (
	policyName                           = "appserver"
	policyGroupName                      = "dev"
	policyRevisionID                     = "1111111111111111111111111111111111111111"
	functionalSearchClientName           = "functional-search-client"
	functionalSearchEnvironmentName      = "searchenv"
	functionalSearchNodeName             = "searchnode"
	functionalSearchRoleName             = "searchrole"
	functionalSearchBagName              = "searchbag"
	functionalSearchItemID               = "searchitem"
	functionalSearchAlpha                = "functionalsearchalpha"
	functionalSearchBeta                 = "functionalsearchbeta"
	functionalUnsupportedCookbookName    = "functional-search-cookbook"
	functionalUnsupportedCookbookVersion = "1.0.0"
	functionalUnsupportedArtifactName    = "functional-search-artifact"
	functionalUnsupportedArtifactID      = "2222222222222222222222222222222222222222"
	functionalUnsupportedArtifactVersion = "1.0.1"
	validatorBootstrapClientName         = "functional-bootstrap-client"
	validatorDefaultBootstrapClientName  = "functional-default-bootstrap-client"
	validatorBadSignatureClientName      = "functional-legacy-bad-signature-client"
	validatorBootstrapClientKeyStateFile = "validator_bootstrap_client_private.pem"
	validatorDefaultClientKeyStateFile   = "validator_default_bootstrap_client_private.pem"
	validatorKeyStateFile                = "validator_private.pem"
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
	signVersion      string
	signAlgorithm    string
	signPathOverride string
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
		phases = []string{"create", "verify", "query-compat", "invalid", "search-update", "verify-search-updated", "query-compat", "delete", "verify-deleted"}
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
			case "query-compat":
				runQueryCompatibilityPhase(t, client, cfg)
			case "invalid":
				runInvalidPhase(t, client, cfg)
			case "search-update":
				runSearchUpdatePhase(t, client, cfg)
			case "verify-search-updated":
				runVerifySearchUpdatedPhase(t, client, cfg)
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
	return newFunctionalClientFromPrivateKey(cfg.baseURL, cfg.actorName, privateKey)
}

func newFunctionalClientFromPrivateKey(baseURL *url.URL, actorName string, privateKey *rsa.PrivateKey) *functionalClient {
	return &functionalClient{
		httpClient:       &http.Client{Timeout: 15 * time.Second},
		baseURL:          baseURL,
		actorName:        actorName,
		serverAPIVersion: "1",
		privateKey:       privateKey,
		signVersion:      "1.3",
		signAlgorithm:    "sha256",
	}
}

func (c *functionalClient) withServerAPIVersion(version string) *functionalClient {
	clone := *c
	clone.serverAPIVersion = version
	return &clone
}

func (c *functionalClient) withLegacySign() *functionalClient {
	clone := *c
	clone.signVersion = "1.1"
	clone.signAlgorithm = "sha1"
	clone.signPathOverride = ""
	return &clone
}

func (c *functionalClient) withSignPathOverride(path string) *functionalClient {
	clone := *c
	clone.signPathOverride = path
	return &clone
}

func runCreatePhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	orgPayload := ensureOrganization(t, client, cfg.org)
	requireOrganizationBootstrap(t, client, cfg.org)
	ensureValidatorBootstrapRegistration(t, client, cfg, orgPayload)

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

	ensureFunctionalSearchFixtures(t, client, functionalSearchAlpha)
	ensureFunctionalEncryptedDataBagFixture(t, client, cfg, testfixtures.EncryptedDataBagItem())

	client.expectJSON(t, http.MethodPut, "/policy_groups/"+policyGroupName+"/policies/"+policyName, policyPayload(policyName, policyRevisionID), http.StatusCreated, http.StatusOK)
	createPendingSandbox(t, client, cfg)
	ensureFunctionalUnsupportedSearchFixtures(t, client, cfg)
}

func runVerifyPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	requireOrganizationBootstrap(t, client, cfg.org)
	requireLegacyValidatorSignedRead(t, cfg)
	requireValidatorBootstrapRegisteredClient(t, client, cfg)

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
	requireFunctionalSearchFixtures(t, client, cfg, functionalSearchAlpha)
	requireFunctionalEncryptedDataBagFixture(t, client, cfg, encryptedPayloadWithID(testfixtures.EncryptedDataBagItem()))

	assignmentPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/policy_groups/"+policyGroupName+"/policies/"+policyName, nil, http.StatusOK).JSON)
	if assignmentPayload["revision_id"] != policyRevisionID {
		t.Fatalf("policy assignment revision_id = %v, want %s", assignmentPayload["revision_id"], policyRevisionID)
	}
	client.expectJSON(t, http.MethodGet, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusOK)

	commitPendingSandboxIfNeeded(t, client, cfg)
	requireSandboxBlobReuse(t, client, cfg.org)
	requireFunctionalUnsupportedSearchFixtures(t, client, cfg)
	requireFunctionalAPIVersionCoverage(t, client, cfg)
}

func runQueryCompatibilityPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	marker := currentFunctionalSearchMarker(t, client, cfg)
	requireFunctionalSearchQueryCompatibility(t, client, cfg, marker)
	switch marker {
	case functionalSearchAlpha:
		requireFunctionalSearchWidenedOldTermsAbsent(t, client, cfg, functionalSearchBeta)
	case functionalSearchBeta:
		requireFunctionalSearchWidenedOldTermsAbsent(t, client, cfg, functionalSearchAlpha)
	}
}

func runInvalidPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	beforeNode := asMap(t, client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusOK).JSON)
	beforeData := asMap(t, client.expectJSON(t, http.MethodGet, "/data/ponies/twilight", nil, http.StatusOK).JSON)
	encryptedPath := functionalEncryptedDataBagItemPath()
	beforeEncrypted := asMap(t, client.expectJSON(t, http.MethodGet, encryptedPath, nil, http.StatusOK).JSON)

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
	badEncrypted := encryptedPayloadWithID(testfixtures.UpdatedEncryptedDataBagItem())
	badEncrypted["id"] = "wrong-encrypted-id"
	badEncrypted["environment"] = "functional-invalid-encrypted"
	client.expectJSON(t, http.MethodPut, encryptedPath, badEncrypted, http.StatusBadRequest)
	requireValidatorLegacyPathMismatchBadSignature(t, client, cfg)

	afterNode := asMap(t, client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusOK).JSON)
	if afterNode["name"] != beforeNode["name"] || afterNode["chef_environment"] != beforeNode["chef_environment"] {
		t.Fatalf("node changed after invalid writes: before=%v after=%v", beforeNode, afterNode)
	}
	afterData := asMap(t, client.expectJSON(t, http.MethodGet, "/data/ponies/twilight", nil, http.StatusOK).JSON)
	if afterData["id"] != beforeData["id"] || afterData["kind"] != beforeData["kind"] {
		t.Fatalf("data bag item changed after invalid writes: before=%v after=%v", beforeData, afterData)
	}
	afterEncrypted := asMap(t, client.expectJSON(t, http.MethodGet, encryptedPath, nil, http.StatusOK).JSON)
	if !reflect.DeepEqual(afterEncrypted, beforeEncrypted) {
		t.Fatalf("encrypted data bag item changed after invalid writes: before=%v after=%v", beforeEncrypted, afterEncrypted)
	}
	requireFunctionalEncryptedDataBagFixture(t, client, cfg, beforeEncrypted)
	requireFunctionalSearchFixtures(t, client, cfg, functionalSearchAlpha)
}

func runSearchUpdatePhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	updateFunctionalSearchFixtures(t, client, functionalSearchBeta)
	updatedEncrypted := updateFunctionalEncryptedDataBagFixture(t, client, cfg, testfixtures.UpdatedEncryptedDataBagItem())
	requireFunctionalSearchFixtures(t, client, cfg, functionalSearchBeta)
	requireFunctionalSearchOldTermsAbsent(t, client, cfg, functionalSearchAlpha)
	requireFunctionalEncryptedDataBagFixture(t, client, cfg, updatedEncrypted)
	requireFunctionalEncryptedOldTermsAbsent(t, client, cfg, testfixtures.EncryptedDataBagItem())
}

func runVerifySearchUpdatedPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	requireFunctionalSearchFixtures(t, client, cfg, functionalSearchBeta)
	requireFunctionalSearchOldTermsAbsent(t, client, cfg, functionalSearchAlpha)
	requireFunctionalEncryptedDataBagFixture(t, client, cfg, encryptedPayloadWithID(testfixtures.UpdatedEncryptedDataBagItem()))
	requireFunctionalEncryptedOldTermsAbsent(t, client, cfg, testfixtures.EncryptedDataBagItem())
}

func runDeletePhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/clients/"+validatorBootstrapClientName, nil, http.StatusOK, http.StatusNotFound)
	_ = os.Remove(filepath.Join(cfg.stateDir, validatorBootstrapClientKeyStateFile))
	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/clients/"+validatorDefaultBootstrapClientName, nil, http.StatusOK, http.StatusNotFound)
	_ = os.Remove(filepath.Join(cfg.stateDir, validatorDefaultClientKeyStateFile))
	client.expectJSON(t, http.MethodDelete, "/clients/"+functionalSearchClientName, nil, http.StatusOK, http.StatusNotFound)

	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/cookbook_artifacts/"+functionalUnsupportedArtifactName+"/"+functionalUnsupportedArtifactID, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/cookbooks/"+functionalUnsupportedCookbookName+"/"+functionalUnsupportedCookbookVersion, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/policy_groups/"+policyGroupName+"/policies/"+policyName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/policy_groups/"+policyGroupName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusOK, http.StatusNotFound)

	client.expectJSON(t, http.MethodDelete, "/data/"+functionalSearchBagName+"/"+functionalSearchItemID, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, functionalEncryptedDataBagItemPath(), nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, functionalEncryptedDataBagPath(), nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/nodes/"+functionalSearchNodeName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/roles/"+functionalSearchRoleName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/environments/"+functionalSearchEnvironmentName, nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/data/ponies/twilight", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/data/ponies", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/nodes/twilight", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/organizations/"+cfg.org+"/roles/web", nil, http.StatusOK, http.StatusNotFound)
	client.expectJSON(t, http.MethodDelete, "/environments/production", nil, http.StatusOK, http.StatusNotFound)
}

func runVerifyDeletedPhase(t *testing.T, client *functionalClient, cfg functionalConfig) {
	requireOperationalStatus(t, client)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/clients/"+validatorBootstrapClientName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/clients/"+validatorDefaultBootstrapClientName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/roles/web", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/environments/production", nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/data/ponies", nil, http.StatusNotFound)
	requireFunctionalEncryptedDataBagDeleted(t, client, cfg)
	client.expectJSON(t, http.MethodGet, "/policy_groups/"+policyGroupName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/cookbook_artifacts/"+functionalUnsupportedArtifactName+"/"+functionalUnsupportedArtifactID, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/cookbooks/"+functionalUnsupportedCookbookName+"/"+functionalUnsupportedCookbookVersion, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org, nil, http.StatusOK)
	requireFunctionalSearchFixturesDeleted(t, client, cfg)
	requireFunctionalUnsupportedSearchContract(t, client, cfg)
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
	message := strings.ToLower(fmt.Sprint(search["message"]))
	if search["backend"] != "opensearch" || search["configured"] != true || !strings.Contains(message, "active") {
		t.Fatalf("opensearch status = %v, want active OpenSearch-backed search provider", search)
	}
	for _, want := range []string{"search provider active", "search-after pagination", "delete-by-query", "total hits"} {
		if !strings.Contains(message, want) {
			t.Fatalf("opensearch status message = %q, want provider capability detail %q", search["message"], want)
		}
	}
}

func ensureFunctionalSearchFixtures(t *testing.T, client *functionalClient, marker string) {
	t.Helper()

	client.expectJSON(t, http.MethodPost, "/clients", map[string]any{
		"name": functionalSearchClientName,
	}, http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/environments", searchEnvironmentPayload(functionalSearchEnvironmentName, marker), http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/roles", searchRolePayload(functionalSearchRoleName, marker), http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/nodes", searchNodePayload(functionalSearchNodeName, functionalSearchEnvironmentName, marker), http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/data", map[string]any{"name": functionalSearchBagName}, http.StatusCreated, http.StatusConflict)
	client.expectJSON(t, http.MethodPost, "/data/"+functionalSearchBagName, searchDataBagItemPayload(functionalSearchItemID, marker), http.StatusCreated, http.StatusConflict)
	updateFunctionalSearchFixtures(t, client, marker)
}

func updateFunctionalSearchFixtures(t *testing.T, client *functionalClient, marker string) {
	t.Helper()

	client.expectJSON(t, http.MethodPut, "/environments/"+functionalSearchEnvironmentName, searchEnvironmentPayload(functionalSearchEnvironmentName, marker), http.StatusOK)
	client.expectJSON(t, http.MethodPut, "/roles/"+functionalSearchRoleName, searchRolePayload(functionalSearchRoleName, marker), http.StatusOK)
	client.expectJSON(t, http.MethodPut, "/nodes/"+functionalSearchNodeName, searchNodePayload(functionalSearchNodeName, functionalSearchEnvironmentName, marker), http.StatusOK)
	client.expectJSON(t, http.MethodPut, "/data/"+functionalSearchBagName+"/"+functionalSearchItemID, searchDataBagItemPayload(functionalSearchItemID, marker), http.StatusOK)
}

func requireFunctionalSearchFixtures(t *testing.T, client *functionalClient, cfg functionalConfig, marker string) {
	t.Helper()

	indexes := asMap(t, client.expectJSON(t, http.MethodGet, "/search", nil, http.StatusOK).JSON)
	for _, index := range []string{"client", "environment", "node", "role", functionalSearchBagName} {
		if _, ok := indexes[index]; !ok {
			t.Fatalf("search indexes = %v, want %q index", indexes, index)
		}
	}

	requireSearchRows(t, client, "/search/client?q=name:"+functionalSearchClientName, 1)
	requireSearchRows(t, client, "/search/environment?q=functional_search_marker:"+marker, 1)
	requireSearchRows(t, client, "/organizations/"+cfg.org+"/search/node?q=functional_search_marker:"+marker, 1)
	requireSearchRows(t, client, "/search/role?q=functional_search_marker:"+marker, 1)
	requireSearchRows(t, client, "/organizations/"+cfg.org+"/search/"+functionalSearchBagName+"?q=kind:"+marker, 1)
	requireFunctionalUnsupportedSearchContract(t, client, cfg)
}

func requireFunctionalSearchOldTermsAbsent(t *testing.T, client *functionalClient, cfg functionalConfig, marker string) {
	t.Helper()

	requireSearchRows(t, client, "/search/environment?q=functional_search_marker:"+marker, 0)
	requireSearchRows(t, client, "/organizations/"+cfg.org+"/search/node?q=functional_search_marker:"+marker, 0)
	requireSearchRows(t, client, "/search/role?q=functional_search_marker:"+marker, 0)
	requireSearchRows(t, client, "/organizations/"+cfg.org+"/search/"+functionalSearchBagName+"?q=kind:"+marker, 0)
	requireFunctionalSearchWidenedOldTermsAbsent(t, client, cfg, marker)
}

func currentFunctionalSearchMarker(t *testing.T, client *functionalClient, cfg functionalConfig) string {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		alpha := searchRowCount(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "functional_search_marker:"+functionalSearchAlpha))
		beta := searchRowCount(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "functional_search_marker:"+functionalSearchBeta))
		switch {
		case alpha == 1 && beta == 0:
			return functionalSearchAlpha
		case beta == 1 && alpha == 0:
			return functionalSearchBeta
		}
		if time.Now().After(deadline) {
			t.Fatalf("functional search marker state alpha=%d beta=%d, want exactly one active marker", alpha, beta)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func requireFunctionalSearchQueryCompatibility(t *testing.T, client *functionalClient, cfg functionalConfig, marker string) {
	t.Helper()

	requireSearchRows(t, client, searchQueryPath("/search/client", "name:functional-search-*"), 1)
	requireSearchRows(t, client, searchQueryPath("/search/environment", `description:"functional OpenSearch environment `+marker+`"`), 1)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "(functional_search_marker:"+marker+" OR recipe:missing) AND functional_sequence:[001 TO 999]"), 1)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", `functional_path:functional\/search\/*`), 1)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "policy_name:"+policyName+" AND policy_group:"+policyGroupName), 1)
	requireSearchRows(t, client, searchQueryPath("/search/role", "description:*"+marker), 1)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+functionalSearchBagName, "ba*:functional*"), 1)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+functionalSearchBagName, `note:"hello `+marker+`"`), 1)

	nodePartial := requirePartialSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "(functional_search_marker:"+marker+" OR recipe:missing) AND functional_sequence:[001 TO 999]"), map[string][]string{
		"marker":   {"functional_search_marker"},
		"sequence": {"functional_sequence"},
	}, 1)
	nodeData := asMap(t, asMap(t, searchRows(t, nodePartial)[0])["data"])
	if nodeData["marker"] != marker || nodeData["sequence"] != "050" {
		t.Fatalf("node partial query compat data = %v, want marker %q sequence 050", nodeData, marker)
	}

	policyPartial := requirePartialSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "policy_name:"+policyName+" AND policy_group:"+policyGroupName), map[string][]string{
		"policy_name":  {"policy_name"},
		"policy_group": {"policy_group"},
	}, 1)
	policyData := asMap(t, asMap(t, searchRows(t, policyPartial)[0])["data"])
	if policyData["policy_name"] != policyName || policyData["policy_group"] != policyGroupName {
		t.Fatalf("node policy partial data = %v, want %s/%s", policyData, policyName, policyGroupName)
	}

	bagPartial := requirePartialSearchRows(t, client, searchQueryPath("/search/"+functionalSearchBagName, "ba*:functional*"), map[string][]string{
		"badge":  {"badge"},
		"marker": {"nested", "functional_search_marker"},
	}, 1)
	bagData := asMap(t, asMap(t, searchRows(t, bagPartial)[0])["data"])
	if bagData["badge"] != "functional["+marker+"]" || bagData["marker"] != marker {
		t.Fatalf("data bag partial query compat data = %v, want marker %q", bagData, marker)
	}

	currentEncrypted := asMap(t, client.expectJSON(t, http.MethodGet, functionalEncryptedDataBagItemPath(), nil, http.StatusOK).JSON)
	requireFunctionalEncryptedDataBagQueryCompatibility(t, client, cfg, currentEncrypted)
}

func requireFunctionalSearchWidenedOldTermsAbsent(t *testing.T, client *functionalClient, cfg functionalConfig, marker string) {
	t.Helper()

	requireSearchRows(t, client, searchQueryPath("/search/environment", `description:"functional OpenSearch environment `+marker+`"`), 0)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/node", "(functional_search_marker:"+marker+" OR recipe:missing) AND functional_sequence:[001 TO 999]"), 0)
	requireSearchRows(t, client, searchQueryPath("/search/role", "description:*"+marker), 0)
	requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+functionalSearchBagName, `note:"hello `+marker+`"`), 0)
}

func requireFunctionalSearchFixturesDeleted(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	client.expectJSON(t, http.MethodGet, "/clients/"+functionalSearchClientName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/environments/"+functionalSearchEnvironmentName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/nodes/"+functionalSearchNodeName, nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/roles/"+functionalSearchRoleName, nil, http.StatusNotFound)

	requireSearchRows(t, client, "/search/client?q=name:"+functionalSearchClientName, 0)
	requireSearchRows(t, client, "/search/environment?q=name:"+functionalSearchEnvironmentName, 0)
	requireSearchRows(t, client, "/search/node?q=name:"+functionalSearchNodeName, 0)
	requireSearchRows(t, client, "/search/role?q=name:"+functionalSearchRoleName, 0)
	for _, marker := range []string{functionalSearchAlpha, functionalSearchBeta} {
		requireSearchRows(t, client, searchQueryPath("/search/environment", `description:"functional OpenSearch environment `+marker+`"`), 0)
		requireSearchRows(t, client, searchQueryPath("/search/node", "(functional_search_marker:"+marker+" OR recipe:missing) AND functional_sequence:[001 TO 999]"), 0)
		requireSearchRows(t, client, searchQueryPath("/search/role", "description:*"+marker), 0)
	}

	resp := client.expectJSON(t, http.MethodGet, "/data/"+functionalSearchBagName, nil, http.StatusOK, http.StatusNotFound)
	if resp.Status == http.StatusOK {
		requireSearchRows(t, client, "/organizations/"+cfg.org+"/search/"+functionalSearchBagName+"?q=id:"+functionalSearchItemID, 0)
		for _, marker := range []string{functionalSearchAlpha, functionalSearchBeta} {
			requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+functionalSearchBagName, `note:"hello `+marker+`"`), 0)
		}
		client.expectJSON(t, http.MethodDelete, "/data/"+functionalSearchBagName, nil, http.StatusOK, http.StatusNotFound)
	}
}

func ensureFunctionalUnsupportedSearchFixtures(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	checksum := checksumHex(sandboxBlob)
	client.expectJSON(t, http.MethodPut, "/organizations/"+cfg.org+"/cookbooks/"+functionalUnsupportedCookbookName+"/"+functionalUnsupportedCookbookVersion+"?force=true", cookbookVersionPayload(functionalUnsupportedCookbookName, functionalUnsupportedCookbookVersion, checksum), http.StatusCreated, http.StatusOK)
	client.expectJSON(t, http.MethodPut, "/organizations/"+cfg.org+"/cookbook_artifacts/"+functionalUnsupportedArtifactName+"/"+functionalUnsupportedArtifactID, cookbookArtifactPayload(functionalUnsupportedArtifactName, functionalUnsupportedArtifactID, functionalUnsupportedArtifactVersion, checksum), http.StatusCreated, http.StatusConflict)
}

func requireFunctionalUnsupportedSearchFixtures(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	cookbook := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/cookbooks/"+functionalUnsupportedCookbookName+"/"+functionalUnsupportedCookbookVersion, nil, http.StatusOK).JSON)
	if cookbook["cookbook_name"] != functionalUnsupportedCookbookName || cookbook["version"] != functionalUnsupportedCookbookVersion {
		t.Fatalf("unsupported cookbook fixture = %v, want %s/%s", cookbook, functionalUnsupportedCookbookName, functionalUnsupportedCookbookVersion)
	}
	artifact := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/cookbook_artifacts/"+functionalUnsupportedArtifactName+"/"+functionalUnsupportedArtifactID, nil, http.StatusOK).JSON)
	if artifact["name"] != functionalUnsupportedArtifactName || artifact["identifier"] != functionalUnsupportedArtifactID {
		t.Fatalf("unsupported cookbook artifact fixture = %v, want %s/%s", artifact, functionalUnsupportedArtifactName, functionalUnsupportedArtifactID)
	}
	requireFunctionalUnsupportedSearchContract(t, client, cfg)
}

// requireFunctionalAPIVersionCoverage exercises representative versioned
// surfaces against the same PostgreSQL/OpenSearch-backed stack used by the
// functional restart phases.
func requireFunctionalAPIVersionCoverage(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	requireFunctionalServerAPIVersionDiscovery(t, client)
	requireFunctionalActorAPIVersionKeyBehavior(t, client, cfg)
	requireFunctionalCookbookAPIVersionFileShapes(t, client, cfg)
	requireFunctionalObjectAPIVersionReads(t, client, cfg)
	requireFunctionalSearchAPIVersionFields(t, client, cfg)
	requireFunctionalSandboxAPIVersionLifecycle(t, client, cfg)
}

func requireFunctionalServerAPIVersionDiscovery(t *testing.T, client *functionalClient) {
	t.Helper()

	discovery := asMap(t, client.expectUnsignedJSON(t, "/server_api_version", http.StatusOK).JSON)
	if discovery["min_api_version"] != float64(0) || discovery["max_api_version"] != float64(2) {
		t.Fatalf("/server_api_version = %v, want min 0 max 2", discovery)
	}

	resp, err := client.unsignedRequest(http.MethodGet, "/server_api_version", nil, map[string]string{
		"X-Ops-Server-API-Version": "3",
	})
	if err != nil {
		t.Fatalf("GET /server_api_version with invalid version failed: %v", err)
	}
	requireStatus(t, resp, http.StatusNotAcceptable)
	decodeJSONBody(t, &resp)
	requireFunctionalInvalidAPIVersionPayload(t, asMap(t, resp.JSON), "3")

	signedInvalid := client.withServerAPIVersion("3").expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusNotAcceptable)
	requireFunctionalInvalidAPIVersionPayload(t, asMap(t, signedInvalid.JSON), "3")
}

func requireFunctionalActorAPIVersionKeyBehavior(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	v0 := client.withServerAPIVersion("0")
	v1 := client.withServerAPIVersion("1")

	userV0 := asMap(t, v0.expectJSON(t, http.MethodGet, "/users/"+cfg.actorName, nil, http.StatusOK).JSON)
	requireFunctionalPublicKey(t, userV0, "API v0 user read")
	userV1 := asMap(t, v1.expectJSON(t, http.MethodGet, "/users/"+cfg.actorName, nil, http.StatusOK).JSON)
	requireFunctionalNoPublicKey(t, userV1, "API v1 user read")
	requireFunctionalKeyListDefault(t, asSlice(t, v1.expectJSON(t, http.MethodGet, "/users/"+cfg.actorName+"/keys", nil, http.StatusOK).JSON), "API v1 user keys", "/users/"+cfg.actorName+"/keys/default")
	userKey := asMap(t, v1.expectJSON(t, http.MethodGet, "/users/"+cfg.actorName+"/keys/default", nil, http.StatusOK).JSON)
	requireFunctionalPublicKey(t, userKey, "API v1 user key detail")

	clientPath := "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName
	clientV0 := asMap(t, v0.expectJSON(t, http.MethodGet, clientPath, nil, http.StatusOK).JSON)
	requireFunctionalPublicKey(t, clientV0, "API v0 client read")
	clientV1 := asMap(t, v1.expectJSON(t, http.MethodGet, clientPath, nil, http.StatusOK).JSON)
	requireFunctionalNoPublicKey(t, clientV1, "API v1 client read")
	requireFunctionalKeyListDefault(t, asSlice(t, v1.expectJSON(t, http.MethodGet, clientPath+"/keys", nil, http.StatusOK).JSON), "API v1 client keys", clientPath+"/keys/default")
	clientKey := asMap(t, v1.expectJSON(t, http.MethodGet, clientPath+"/keys/default", nil, http.StatusOK).JSON)
	requireFunctionalPublicKey(t, clientKey, "API v1 client key detail")
}

func requireFunctionalCookbookAPIVersionFileShapes(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	path := "/organizations/" + cfg.org + "/cookbooks/" + functionalUnsupportedCookbookName + "/" + functionalUnsupportedCookbookVersion
	checksum := checksumHex(sandboxBlob)

	legacy := asMap(t, client.withServerAPIVersion("0").expectJSON(t, http.MethodGet, path, nil, http.StatusOK).JSON)
	if _, ok := legacy["all_files"]; ok {
		t.Fatalf("API v0 cookbook unexpectedly included all_files: %v", legacy)
	}
	legacyFile := requireFunctionalCookbookSegmentFile(t, legacy, "recipes", "recipes/default.rb", checksum)
	if _, ok := legacyFile["url"].(string); !ok {
		t.Fatalf("API v0 cookbook recipe url = %T, want string (%v)", legacyFile["url"], legacyFile)
	}

	v2 := asMap(t, client.withServerAPIVersion("2").expectJSON(t, http.MethodGet, path, nil, http.StatusOK).JSON)
	for _, segment := range functionalLegacyCookbookSegments() {
		if _, ok := v2[segment]; ok {
			t.Fatalf("API v2 cookbook unexpectedly included legacy segment %q: %v", segment, v2)
		}
	}
	v2File := requireFunctionalCookbookSegmentFile(t, v2, "all_files", "recipes/default.rb", checksum)
	downloadURL, ok := v2File["url"].(string)
	if !ok {
		t.Fatalf("API v2 cookbook file url = %T, want string (%v)", v2File["url"], v2File)
	}
	download := client.doUnsigned(t, http.MethodGet, downloadURL, nil, nil, http.StatusOK)
	if !bytes.Equal(download.Body, sandboxBlob) {
		t.Fatalf("API v2 cookbook download body = %q, want sandbox blob", download.Body)
	}
}

func requireFunctionalObjectAPIVersionReads(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	for _, version := range []string{"0", "1", "2"} {
		versioned := client.withServerAPIVersion(version)

		env := asMap(t, versioned.expectJSON(t, http.MethodGet, "/environments/production", nil, http.StatusOK).JSON)
		if env["name"] != "production" {
			t.Fatalf("API v%s environment = %v, want production", version, env)
		}
		role := asMap(t, versioned.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/roles/web", nil, http.StatusOK).JSON)
		if role["name"] != "web" {
			t.Fatalf("API v%s role = %v, want web", version, role)
		}
		node := asMap(t, versioned.expectJSON(t, http.MethodGet, "/nodes/twilight", nil, http.StatusOK).JSON)
		if node["name"] != "twilight" || node["chef_environment"] != "production" {
			t.Fatalf("API v%s node = %v, want twilight in production", version, node)
		}
		item := asMap(t, versioned.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/data/ponies/twilight", nil, http.StatusOK).JSON)
		if item["id"] != "twilight" || item["kind"] != "unicorn" {
			t.Fatalf("API v%s data bag item = %v, want twilight unicorn", version, item)
		}
		revision := asMap(t, versioned.expectJSON(t, http.MethodGet, "/policies/"+policyName+"/revisions/"+policyRevisionID, nil, http.StatusOK).JSON)
		if revision["name"] != policyName || revision["revision_id"] != policyRevisionID {
			t.Fatalf("API v%s policy revision = %v, want %s/%s", version, revision, policyName, policyRevisionID)
		}
		assignment := asMap(t, versioned.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/policy_groups/"+policyGroupName+"/policies/"+policyName, nil, http.StatusOK).JSON)
		if assignment["revision_id"] != policyRevisionID {
			t.Fatalf("API v%s policy assignment = %v, want revision %s", version, assignment, policyRevisionID)
		}
	}
}

func requireFunctionalSearchAPIVersionFields(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	for _, version := range []string{"0", "1", "2"} {
		versioned := client.withServerAPIVersion(version)
		path := searchQueryPath("/organizations/"+cfg.org+"/search/node", "policy_name:"+policyName+" AND policy_group:"+policyGroupName)
		requireSearchRows(t, versioned, path, 1)
		partial := requirePartialSearchRows(t, versioned, path, map[string][]string{
			"policy_name":  {"policy_name"},
			"policy_group": {"policy_group"},
		}, 1)
		data := asMap(t, asMap(t, searchRows(t, partial)[0])["data"])
		if data["policy_name"] != policyName || data["policy_group"] != policyGroupName {
			t.Fatalf("API v%s search partial data = %v, want %s/%s", version, data, policyName, policyGroupName)
		}
	}
}

func requireFunctionalSandboxAPIVersionLifecycle(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	for _, version := range []string{"0", "1", "2"} {
		versioned := client.withServerAPIVersion(version)
		body := []byte("functional api-version sandbox " + version + "\n")
		checksum := checksumHex(body)
		create := asMap(t, versioned.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/sandboxes", map[string]any{
			"checksums": map[string]any{checksum: nil},
		}, http.StatusCreated).JSON)
		sandboxID, uploadURL := requireFunctionalSandboxCreatePayload(t, create, checksum, cfg.org)
		if uploadURL != "" {
			upload := client.doUnsigned(t, http.MethodPut, uploadURL, body, map[string]string{
				"Content-Type": "application/x-binary",
				"Content-MD5":  checksumBase64(body),
			}, http.StatusNoContent)
			if len(upload.Body) != 0 {
				t.Fatalf("API v%s sandbox upload body = %q, want empty", version, upload.Body)
			}
		}
		commit := asMap(t, versioned.expectJSON(t, http.MethodPut, "/organizations/"+cfg.org+"/sandboxes/"+sandboxID, map[string]any{
			"is_completed": true,
		}, http.StatusOK).JSON)
		if commit["guid"] != sandboxID || commit["is_completed"] != true || !containsString(jsonStringSlice(t, commit["checksums"]), checksum) {
			t.Fatalf("API v%s sandbox commit = %v, want completed %s with checksum %s", version, commit, sandboxID, checksum)
		}
	}
}

func requireFunctionalUnsupportedSearchContract(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	requireSearchIndexesExcludeUnsupported(t, client, "/search")
	requireSearchIndexesExcludeUnsupported(t, client, "/organizations/"+cfg.org+"/search")
	for _, index := range unsupportedFunctionalSearchIndexes() {
		for _, base := range []string{"/search/", "/organizations/" + cfg.org + "/search/"} {
			client.expectJSON(t, http.MethodGet, searchQueryPath(base+index, "*:*"), nil, http.StatusNotFound)
			client.expectJSON(t, http.MethodPost, searchQueryPath(base+index, "*:*"), map[string][]string{"name": {"name"}}, http.StatusNotFound)
		}
	}
}

func requireSearchIndexesExcludeUnsupported(t *testing.T, client *functionalClient, path string) {
	t.Helper()

	indexes := asMap(t, client.expectJSON(t, http.MethodGet, path, nil, http.StatusOK).JSON)
	for _, index := range unsupportedFunctionalSearchIndexes() {
		if _, exists := indexes[index]; exists {
			t.Fatalf("%s indexes = %v, unexpectedly included unsupported index %q", path, indexes, index)
		}
	}
}

func unsupportedFunctionalSearchIndexes() []string {
	return []string{
		"cookbook",
		"cookbooks",
		"cookbook_artifact",
		"cookbook_artifacts",
		"policy",
		"policies",
		"policy_group",
		"policy_groups",
		"sandbox",
		"sandboxes",
		"checksum",
		"checksums",
	}
}

func requireFunctionalInvalidAPIVersionPayload(t *testing.T, payload map[string]any, requested string) {
	t.Helper()

	if payload["error"] != "invalid-x-ops-server-api-version" {
		t.Fatalf("invalid API-version error = %v, want invalid-x-ops-server-api-version", payload["error"])
	}
	if payload["message"] != "Specified version "+requested+" not supported" {
		t.Fatalf("invalid API-version message = %v, want requested version %s", payload["message"], requested)
	}
	if payload["min_version"] != float64(0) || payload["max_version"] != float64(2) {
		t.Fatalf("invalid API-version bounds = %v, want 0..2", payload)
	}
}

func requireFunctionalPublicKey(t *testing.T, payload map[string]any, label string) {
	t.Helper()

	publicKey, ok := payload["public_key"].(string)
	if !ok || !strings.Contains(publicKey, "BEGIN PUBLIC KEY") {
		t.Fatalf("%s public_key = %T/%v, want PEM public key", label, payload["public_key"], payload["public_key"])
	}
}

func requireFunctionalNoPublicKey(t *testing.T, payload map[string]any, label string) {
	t.Helper()

	if _, ok := payload["public_key"]; ok {
		t.Fatalf("%s unexpectedly included top-level public_key: %v", label, payload)
	}
}

func requireFunctionalKeyListDefault(t *testing.T, keys []any, label, wantURI string) {
	t.Helper()

	if len(keys) == 0 {
		t.Fatalf("%s = %v, want at least one key", label, keys)
	}
	for _, raw := range keys {
		key := asMap(t, raw)
		if key["name"] == "default" && key["uri"] == wantURI && key["expired"] == false {
			return
		}
	}
	t.Fatalf("%s = %v, want default key metadata with uri %q", label, keys, wantURI)
}

func requireFunctionalCookbookSegmentFile(t *testing.T, payload map[string]any, segment, path, checksum string) map[string]any {
	t.Helper()

	rawFiles := asSlice(t, payload[segment])
	for _, raw := range rawFiles {
		file := asMap(t, raw)
		if file["path"] == path {
			if file["checksum"] != checksum {
				t.Fatalf("%s file checksum = %v, want %s", segment, file["checksum"], checksum)
			}
			return file
		}
	}
	t.Fatalf("%s missing path %q in %v", segment, path, rawFiles)
	return nil
}

func functionalLegacyCookbookSegments() []string {
	return []string{"recipes", "files", "templates", "attributes", "definitions", "libraries", "providers", "resources", "root_files"}
}

func requireFunctionalSandboxCreatePayload(t *testing.T, payload map[string]any, checksum, org string) (string, string) {
	t.Helper()

	sandboxID, ok := payload["sandbox_id"].(string)
	if !ok || sandboxID == "" {
		t.Fatalf("sandbox_id = %T/%v, want non-empty string", payload["sandbox_id"], payload["sandbox_id"])
	}
	uri, ok := payload["uri"].(string)
	if !ok || !strings.HasSuffix(uri, "/organizations/"+org+"/sandboxes/"+sandboxID) {
		t.Fatalf("sandbox uri = %T/%v, want org-scoped sandbox URI ending in %s", payload["uri"], payload["uri"], sandboxID)
	}
	entry := asMap(t, asMap(t, payload["checksums"])[checksum])
	switch entry["needs_upload"] {
	case true:
		uploadURL, ok := entry["url"].(string)
		if !ok || uploadURL == "" {
			t.Fatalf("sandbox upload url = %T/%v, want non-empty string", entry["url"], entry["url"])
		}
		requireFunctionalSandboxUploadURL(t, uploadURL, checksum, org, sandboxID)
		return sandboxID, uploadURL
	case false:
		if _, ok := entry["url"]; ok {
			t.Fatalf("reused sandbox checksum included upload URL: %v", entry)
		}
		return sandboxID, ""
	default:
		t.Fatalf("sandbox needs_upload = %T/%v, want boolean", entry["needs_upload"], entry["needs_upload"])
	}
	return sandboxID, ""
}

func requireFunctionalSandboxUploadURL(t *testing.T, rawURL, checksum, org, sandboxID string) {
	t.Helper()

	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse sandbox upload URL %q: %v", rawURL, err)
	}
	if !strings.HasSuffix(parsed.Path, "/_blob/checksums/"+checksum) {
		t.Fatalf("sandbox upload path = %q, want checksum %s", parsed.Path, checksum)
	}
	values := parsed.Query()
	if values.Get("org") != org || values.Get("sandbox_id") != sandboxID || values.Get("expires") == "" || values.Get("signature") == "" {
		t.Fatalf("sandbox upload query = %v, want org/sandbox_id/expires/signature", values)
	}
}

// ensureFunctionalEncryptedDataBagFixture creates or resets the deterministic
// encrypted-looking data bag item through normal Chef-facing routes.
func ensureFunctionalEncryptedDataBagFixture(t *testing.T, client *functionalClient, cfg functionalConfig, payload map[string]any) {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	itemPath := functionalEncryptedDataBagItemPath()
	client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/data", map[string]any{"name": bagName}, http.StatusCreated, http.StatusConflict)
	resp := client.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/data/"+bagName, payload, http.StatusCreated, http.StatusConflict)
	if resp.Status == http.StatusConflict {
		client.expectJSON(t, http.MethodPut, itemPath, payload, http.StatusOK)
	}
}

// updateFunctionalEncryptedDataBagFixture updates the encrypted-looking item and
// returns the expected stored representation after URL-derived id normalization.
func updateFunctionalEncryptedDataBagFixture(t *testing.T, client *functionalClient, cfg functionalConfig, payload map[string]any) map[string]any {
	t.Helper()

	path := "/organizations/" + cfg.org + "/data/" + testfixtures.EncryptedDataBagName() + "/" + testfixtures.EncryptedDataBagItemID()
	client.expectJSON(t, http.MethodPut, path, payload, http.StatusOK)
	return encryptedPayloadWithID(payload)
}

// requireFunctionalEncryptedDataBagFixture proves reads, full search, and
// partial search all see the same opaque encrypted JSON without a secret.
func requireFunctionalEncryptedDataBagFixture(t *testing.T, client *functionalClient, cfg functionalConfig, want map[string]any) {
	t.Helper()

	itemPath := functionalEncryptedDataBagItemPath()
	itemPayload := asMap(t, client.expectJSON(t, http.MethodGet, itemPath, nil, http.StatusOK).JSON)
	requireEncryptedDataBagPayload(t, itemPayload, want)

	bagName := testfixtures.EncryptedDataBagName()
	passwordCiphertext := encryptedEnvelopeString(t, want, "password", "encrypted_data")
	fullSearch := requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+bagName, "password_encrypted_data:"+passwordCiphertext), 1)
	row := asMap(t, searchRows(t, fullSearch)[0])
	rawData := asMap(t, row["raw_data"])
	requireEncryptedDataBagPayload(t, rawData, want)
	assertFunctionalSearchBodyOmitsPlaintext(t, fullSearch)
	requireFunctionalEncryptedDataBagQueryCompatibility(t, client, cfg, want)

	requireFunctionalEncryptedPartialSearch(t, client, cfg, want)
}

func requireFunctionalEncryptedDataBagQueryCompatibility(t *testing.T, client *functionalClient, cfg functionalConfig, want map[string]any) {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	fullSearch := requireSearchRows(t, client, searchQueryPath("/organizations/"+cfg.org+"/search/"+bagName, "*_encrypted_data:*"), 1)
	assertFunctionalSearchBodyOmitsPlaintext(t, fullSearch)
	fullGrouped := requireSearchRows(t, client, searchQueryPath("/search/"+bagName, "environment:"+fmt.Sprint(want["environment"])+" AND *_encrypted_data:*"), 1)
	assertFunctionalSearchBodyOmitsPlaintext(t, fullGrouped)
}

// requireFunctionalEncryptedPartialSearch checks selected encrypted envelope
// members and clear metadata fields through POST partial search.
func requireFunctionalEncryptedPartialSearch(t *testing.T, client *functionalClient, cfg functionalConfig, want map[string]any) {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	body := map[string][]string{
		"password_ciphertext": {"password", "encrypted_data"},
		"password_iv":         {"password", "iv"},
		"api_auth_tag":        {"api_key", "auth_tag"},
		"environment":         {"environment"},
	}
	path := searchQueryPath("/organizations/"+cfg.org+"/search/"+bagName, "environment:"+fmt.Sprint(want["environment"])+" AND *_encrypted_data:*")
	deadline := time.Now().Add(15 * time.Second)
	for {
		payload := asMap(t, client.expectJSON(t, http.MethodPost, path, body, http.StatusOK).JSON)
		rows := searchRows(t, payload)
		if len(rows) == 1 {
			row := asMap(t, rows[0])
			wantURL := "/organizations/" + cfg.org + "/data/" + bagName + "/" + testfixtures.EncryptedDataBagItemID()
			if row["url"] != wantURL {
				t.Fatalf("encrypted partial search url = %v, want %q", row["url"], wantURL)
			}
			data := asMap(t, row["data"])
			if data["password_ciphertext"] != encryptedEnvelopeString(t, want, "password", "encrypted_data") ||
				data["password_iv"] != encryptedEnvelopeString(t, want, "password", "iv") ||
				data["api_auth_tag"] != encryptedEnvelopeString(t, want, "api_key", "auth_tag") ||
				data["environment"] != want["environment"] {
				t.Fatalf("encrypted partial search data = %v, want selected stored envelope fields from %v", data, want)
			}
			assertFunctionalSearchBodyOmitsPlaintext(t, payload)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s rows = %v, want one encrypted partial search row", path, payload["rows"])
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// requireFunctionalEncryptedOldTermsAbsent proves OpenSearch stale terms are
// removed after encrypted-looking data bag item updates.
func requireFunctionalEncryptedOldTermsAbsent(t *testing.T, client *functionalClient, cfg functionalConfig, old map[string]any) {
	t.Helper()

	bagName := testfixtures.EncryptedDataBagName()
	base := "/organizations/" + cfg.org + "/search/" + bagName
	requireSearchRows(t, client, searchQueryPath(base, "environment:"+fmt.Sprint(old["environment"])), 0)
	requireSearchRows(t, client, searchQueryPath(base, "environment:"+fmt.Sprint(old["environment"])+" AND *_encrypted_data:*"), 0)
	requireSearchRows(t, client, searchQueryPath(base, "password_encrypted_data:"+encryptedEnvelopeString(t, old, "password", "encrypted_data")), 0)
	requireSearchRows(t, client, searchQueryPath(base, "api_key_auth_tag:"+encryptedEnvelopeString(t, old, "api_key", "auth_tag")), 0)
}

// requireFunctionalEncryptedDataBagDeleted verifies delete and post-restart
// absence for both the data bag route and its dynamic search index.
func requireFunctionalEncryptedDataBagDeleted(t *testing.T, client *functionalClient, cfg functionalConfig) {
	t.Helper()

	client.expectJSON(t, http.MethodGet, functionalEncryptedDataBagPath(), nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, functionalEncryptedDataBagItemPath(), nil, http.StatusNotFound)
	client.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/search/"+testfixtures.EncryptedDataBagName()+"?q=id:"+testfixtures.EncryptedDataBagItemID(), nil, http.StatusNotFound)
}

func ensureOrganization(t *testing.T, client *functionalClient, org string) map[string]any {
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
		return payload
	}
	return nil
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

func ensureValidatorBootstrapRegistration(t *testing.T, admin *functionalClient, cfg functionalConfig, orgPayload map[string]any) {
	t.Helper()

	validatorPrivateKeyPEM := validatorPrivateKeyFromBootstrapPayload(t, cfg, orgPayload)
	if validatorPrivateKeyPEM == "" {
		validatorPrivateKeyPEM = strings.TrimSpace(readOptionalStateFile(t, cfg.stateDir, validatorKeyStateFile))
	}
	if validatorPrivateKeyPEM == "" {
		validatorPrivateKeyPEM = rotateClientDefaultKey(t, admin, cfg.org, cfg.org+"-validator")
	}
	writeStateFile(t, cfg.stateDir, validatorKeyStateFile, validatorPrivateKeyPEM)

	validator := newFunctionalClientFromPrivateKey(admin.baseURL, cfg.org+"-validator", parsePrivateKeyPEM(t, "validator private key", validatorPrivateKeyPEM))
	ensureValidatorRegisteredClient(t, admin, validator.withLegacySign(), cfg, validatorRegistrationSpec{
		name:             validatorBootstrapClientName,
		createPath:       "/organizations/" + cfg.org + "/clients",
		selfReadPath:     "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName,
		keyListPath:      "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName + "/keys",
		wantURI:          "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName,
		wantChefKeyURI:   "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName + "/keys/default",
		searchPath:       "/organizations/" + cfg.org + "/search/client?q=name:" + validatorBootstrapClientName,
		stateKeyFilename: validatorBootstrapClientKeyStateFile,
	})
	ensureValidatorRegisteredClient(t, admin, validator, cfg, validatorRegistrationSpec{
		name:             validatorDefaultBootstrapClientName,
		createPath:       "/clients",
		selfReadPath:     "/clients/" + validatorDefaultBootstrapClientName,
		keyListPath:      "/clients/" + validatorDefaultBootstrapClientName + "/keys",
		wantURI:          "/clients/" + validatorDefaultBootstrapClientName,
		wantChefKeyURI:   "/clients/" + validatorDefaultBootstrapClientName + "/keys/default",
		searchPath:       "/search/client?q=name:" + validatorDefaultBootstrapClientName,
		stateKeyFilename: validatorDefaultClientKeyStateFile,
	})
}

type validatorRegistrationSpec struct {
	name             string
	createPath       string
	selfReadPath     string
	keyListPath      string
	wantURI          string
	wantChefKeyURI   string
	searchPath       string
	stateKeyFilename string
}

func ensureValidatorRegisteredClient(t *testing.T, admin, validator *functionalClient, cfg functionalConfig, spec validatorRegistrationSpec) {
	t.Helper()

	resp := validator.expectJSON(t, http.MethodPost, spec.createPath, map[string]any{
		"name":       spec.name,
		"create_key": true,
	}, http.StatusCreated, http.StatusConflict)

	clientPrivateKeyPEM := ""
	if resp.Status == http.StatusCreated {
		payload := asMap(t, resp.JSON)
		if payload["uri"] != spec.wantURI {
			t.Fatalf("validator-created client uri = %v, want %q", payload["uri"], spec.wantURI)
		}
		chefKey := asMap(t, payload["chef_key"])
		if chefKey["uri"] != spec.wantChefKeyURI {
			t.Fatalf("validator-created chef_key uri = %v, want %q", chefKey["uri"], spec.wantChefKeyURI)
		}
		clientPrivateKeyPEM = privateKeyFromClientCreatePayload(t, payload)
	} else {
		clientPrivateKeyPEM = strings.TrimSpace(readOptionalStateFile(t, cfg.stateDir, spec.stateKeyFilename))
		if clientPrivateKeyPEM == "" {
			clientPrivateKeyPEM = rotateClientDefaultKey(t, admin, cfg.org, spec.name)
		}
	}
	writeStateFile(t, cfg.stateDir, spec.stateKeyFilename, clientPrivateKeyPEM)

	registeredClient := newFunctionalClientFromPrivateKey(admin.baseURL, spec.name, parsePrivateKeyPEM(t, spec.name+" private key", clientPrivateKeyPEM))
	requireValidatorBootstrapRegisteredClientWithClient(t, admin, registeredClient, cfg.org, spec)
}

func requireValidatorBootstrapRegisteredClient(t *testing.T, admin *functionalClient, cfg functionalConfig) {
	t.Helper()

	specs := []validatorRegistrationSpec{
		{
			name:             validatorBootstrapClientName,
			selfReadPath:     "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName,
			keyListPath:      "/organizations/" + cfg.org + "/clients/" + validatorBootstrapClientName + "/keys",
			searchPath:       "/organizations/" + cfg.org + "/search/client?q=name:" + validatorBootstrapClientName,
			stateKeyFilename: validatorBootstrapClientKeyStateFile,
		},
		{
			name:             validatorDefaultBootstrapClientName,
			selfReadPath:     "/clients/" + validatorDefaultBootstrapClientName,
			keyListPath:      "/clients/" + validatorDefaultBootstrapClientName + "/keys",
			searchPath:       "/search/client?q=name:" + validatorDefaultBootstrapClientName,
			stateKeyFilename: validatorDefaultClientKeyStateFile,
		},
	}
	for _, spec := range specs {
		privateKeyPEM := strings.TrimSpace(readStateFile(t, cfg.stateDir, spec.stateKeyFilename))
		client := newFunctionalClientFromPrivateKey(cfg.baseURL, spec.name, parsePrivateKeyPEM(t, spec.name+" private key", privateKeyPEM))
		requireValidatorBootstrapRegisteredClientWithClient(t, admin, client, cfg.org, spec)
	}
}

func requireLegacyValidatorSignedRead(t *testing.T, cfg functionalConfig) {
	t.Helper()

	validatorPrivateKeyPEM := strings.TrimSpace(readStateFile(t, cfg.stateDir, validatorKeyStateFile))
	validator := newFunctionalClientFromPrivateKey(cfg.baseURL, cfg.org+"-validator", parsePrivateKeyPEM(t, "validator private key", validatorPrivateKeyPEM)).withLegacySign()
	containerPayload := asMap(t, validator.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/containers/data", nil, http.StatusOK).JSON)
	if containerPayload["containername"] != "data" {
		t.Fatalf("legacy validator container read payload = %v, want data container", containerPayload)
	}
}

func requireValidatorBootstrapRegisteredClientWithClient(t *testing.T, admin, client *functionalClient, org string, spec validatorRegistrationSpec) {
	t.Helper()

	clientPayload := asMap(t, client.expectJSON(t, http.MethodGet, spec.selfReadPath, nil, http.StatusOK).JSON)
	if clientPayload["name"] != spec.name || clientPayload["validator"] != false {
		t.Fatalf("validator-created client payload = %v, want normal client %q", clientPayload, spec.name)
	}

	keysPayload := asSlice(t, client.expectJSON(t, http.MethodGet, spec.keyListPath, nil, http.StatusOK).JSON)
	if len(keysPayload) != 1 {
		t.Fatalf("validator-created client keys = %v, want one default key", keysPayload)
	}
	key := asMap(t, keysPayload[0])
	if key["name"] != "default" || key["expired"] != false {
		t.Fatalf("validator-created client default key = %v, want active default key", key)
	}

	containerPayload := asMap(t, client.expectJSON(t, http.MethodGet, "/organizations/"+org+"/containers/data", nil, http.StatusOK).JSON)
	if containerPayload["containername"] != "data" {
		t.Fatalf("registered client container read payload = %v, want data container", containerPayload)
	}

	groupPayload := asMap(t, admin.expectJSON(t, http.MethodGet, "/organizations/"+org+"/groups/clients", nil, http.StatusOK).JSON)
	clientsGroupMembers := jsonStringSlice(t, groupPayload["clients"])
	clientsGroupActors := jsonStringSlice(t, groupPayload["actors"])
	if !containsString(clientsGroupMembers, org+"-validator") || !containsString(clientsGroupMembers, spec.name) {
		t.Fatalf("clients group clients = %v, want validator and %q", clientsGroupMembers, spec.name)
	}
	if !containsString(clientsGroupActors, org+"-validator") || !containsString(clientsGroupActors, spec.name) {
		t.Fatalf("clients group actors = %v, want validator and %q", clientsGroupActors, spec.name)
	}

	aclPayload := asMap(t, admin.expectJSON(t, http.MethodGet, "/organizations/"+org+"/clients/"+spec.name+"/_acl", nil, http.StatusOK).JSON)
	readPermission := asMap(t, aclPayload["read"])
	if !containsString(jsonStringSlice(t, readPermission["actors"]), spec.name) {
		t.Fatalf("client ACL read actors = %v, want %q", readPermission["actors"], spec.name)
	}

	searchPayload := asMap(t, admin.expectJSON(t, http.MethodGet, spec.searchPath, nil, http.StatusOK).JSON)
	requireRows(t, searchPayload, 1)
}

func requireValidatorLegacyPathMismatchBadSignature(t *testing.T, admin *functionalClient, cfg functionalConfig) {
	t.Helper()

	validatorPrivateKeyPEM := strings.TrimSpace(readStateFile(t, cfg.stateDir, validatorKeyStateFile))
	validator := newFunctionalClientFromPrivateKey(cfg.baseURL, cfg.org+"-validator", parsePrivateKeyPEM(t, "validator private key", validatorPrivateKeyPEM)).
		withLegacySign().
		withSignPathOverride("/organizations/" + cfg.org + "/clientz")

	resp := validator.expectJSON(t, http.MethodPost, "/organizations/"+cfg.org+"/clients", map[string]any{
		"name":       validatorBadSignatureClientName,
		"create_key": true,
	}, http.StatusUnauthorized)
	payload := asMap(t, resp.JSON)
	if payload["error"] != "bad_signature" {
		t.Fatalf("legacy path-mismatch error = %v, want bad_signature: %v", payload["error"], payload)
	}
	admin.expectJSON(t, http.MethodGet, "/organizations/"+cfg.org+"/clients/"+validatorBadSignatureClientName, nil, http.StatusNotFound)
}

func validatorPrivateKeyFromBootstrapPayload(t *testing.T, cfg functionalConfig, payload map[string]any) string {
	t.Helper()

	if payload == nil {
		return ""
	}
	if payload["clientname"] != cfg.org+"-validator" {
		t.Fatalf("validator clientname = %v, want %s-validator", payload["clientname"], cfg.org)
	}
	privateKeyPEM, _ := payload["private_key"].(string)
	if strings.TrimSpace(privateKeyPEM) == "" {
		t.Fatalf("organization bootstrap response missing validator private_key: %v", payload)
	}
	return privateKeyPEM
}

func privateKeyFromClientCreatePayload(t *testing.T, payload map[string]any) string {
	t.Helper()

	if chefKey, ok := payload["chef_key"].(map[string]any); ok {
		if privateKeyPEM, ok := chefKey["private_key"].(string); ok && strings.TrimSpace(privateKeyPEM) != "" {
			return privateKeyPEM
		}
	}
	if privateKeyPEM, ok := payload["private_key"].(string); ok && strings.TrimSpace(privateKeyPEM) != "" {
		return privateKeyPEM
	}
	t.Fatalf("client create response missing private key material: %v", payload)
	return ""
}

func rotateClientDefaultKey(t *testing.T, admin *functionalClient, org, clientName string) string {
	t.Helper()

	resp := admin.expectJSON(t, http.MethodPut, "/organizations/"+org+"/clients/"+clientName+"/keys/default", map[string]any{
		"create_key": true,
	}, http.StatusOK)
	payload := asMap(t, resp.JSON)
	privateKeyPEM, _ := payload["private_key"].(string)
	if strings.TrimSpace(privateKeyPEM) == "" {
		t.Fatalf("rotated %s/%s default key response missing private_key: %v", org, clientName, payload)
	}
	return privateKeyPEM
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

func readOptionalStateFile(t *testing.T, dir, name string) string {
	t.Helper()

	data, err := os.ReadFile(filepath.Join(dir, name))
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatalf("read %s state: %v", name, err)
	}
	return string(data)
}

func writeStateFile(t *testing.T, dir, name, value string) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create state dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o600); err != nil {
		t.Fatalf("write %s state: %v", name, err)
	}
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

func searchEnvironmentPayload(name, marker string) map[string]any {
	payload := environmentPayload(name)
	payload["description"] = "functional OpenSearch environment " + marker
	payload["default_attributes"] = map[string]any{
		"functional_search_marker": marker,
		"functional_path":          "functional/search/" + marker,
		"functional_phrase":        "hello " + marker,
	}
	return payload
}

func searchNodePayload(name, environment, marker string) map[string]any {
	payload := nodePayload(name, environment)
	payload["policy_name"] = policyName
	payload["policy_group"] = policyGroupName
	payload["normal"] = map[string]any{
		"functional_search_marker": marker,
		"functional_sequence":      "050",
		"functional_path":          "functional/search/" + marker,
	}
	payload["run_list"] = []string{"role[" + functionalSearchRoleName + "]"}
	return payload
}

func searchRolePayload(name, marker string) map[string]any {
	payload := rolePayload(name)
	payload["description"] = "functional OpenSearch role " + marker
	payload["default_attributes"] = map[string]any{
		"functional_search_marker": marker,
		"functional_path":          "functional/search/" + marker,
	}
	payload["env_run_lists"] = map[string][]string{
		functionalSearchEnvironmentName: []string{"recipe[search_demo]"},
	}
	return payload
}

func searchDataBagItemPayload(id, marker string) map[string]any {
	return map[string]any{
		"id":    id,
		"kind":  marker,
		"badge": "functional[" + marker + "]",
		"note":  "hello " + marker,
		"path":  "functional/search/" + marker,
		"nested": map[string]any{
			"functional_search_marker": marker,
		},
	}
}

// functionalEncryptedDataBagPath returns the default-org encrypted fixture bag
// route so restart phases exercise the configured default organization alias.
func functionalEncryptedDataBagPath() string {
	return "/data/" + testfixtures.EncryptedDataBagName()
}

// functionalEncryptedDataBagItemPath returns the default-org encrypted fixture
// item route used for read/update/delete lifecycle checks.
func functionalEncryptedDataBagItemPath() string {
	return functionalEncryptedDataBagPath() + "/" + testfixtures.EncryptedDataBagItemID()
}

// encryptedPayloadWithID copies a fixture payload and adds the URL-derived id
// expected after PUT bodies omit the `id` field.
func encryptedPayloadWithID(payload map[string]any) map[string]any {
	out := testfixtures.CloneDataBagPayload(payload)
	out["id"] = testfixtures.EncryptedDataBagItemID()
	return out
}

// requireEncryptedDataBagPayload compares opaque JSON payloads exactly so the
// functional suite catches accidental encrypted-envelope normalization.
func requireEncryptedDataBagPayload(t *testing.T, got, want map[string]any) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("encrypted data bag payload = %#v, want %#v", got, want)
	}
}

// encryptedEnvelopeString extracts one stored encrypted envelope field from the
// JSON fixture without teaching the server-side tests any crypto semantics.
func encryptedEnvelopeString(t *testing.T, payload map[string]any, envelopeName, fieldName string) string {
	t.Helper()

	envelope := asMap(t, payload[envelopeName])
	value, ok := envelope[fieldName].(string)
	if !ok {
		t.Fatalf("encrypted envelope %s.%s = %T(%v), want string", envelopeName, fieldName, envelope[fieldName], envelope[fieldName])
	}
	return value
}

// assertFunctionalSearchBodyOmitsPlaintext keeps the functional encrypted data
// bag checks focused on stored ciphertext rather than invented plaintext values.
func assertFunctionalSearchBodyOmitsPlaintext(t *testing.T, payload map[string]any) {
	t.Helper()

	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal search payload for plaintext check: %v", err)
	}
	for _, forbidden := range []string{"correct horse battery staple", "data_bag_secret"} {
		if strings.Contains(string(raw), forbidden) {
			t.Fatalf("search payload leaked %q: %s", forbidden, string(raw))
		}
	}
}

// searchQueryPath URL-encodes the q parameter so base64 ciphertext fields with
// padding characters survive through the HTTP client and router unchanged.
func searchQueryPath(path, query string) string {
	return path + "?q=" + url.QueryEscape(query)
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

func cookbookVersionPayload(name, version, checksum string) map[string]any {
	return map[string]any{
		"name":          name + "-" + version,
		"cookbook_name": name,
		"version":       version,
		"json_class":    "Chef::CookbookVersion",
		"chef_type":     "cookbook_version",
		"frozen?":       false,
		"metadata": map[string]any{
			"version":          version,
			"name":             name,
			"maintainer":       "OpenCook",
			"maintainer_email": "opencook@example.test",
			"description":      "functional unsupported-search cookbook fixture",
			"long_description": "functional unsupported-search cookbook fixture",
			"license":          "Apache v2.0",
			"dependencies":     map[string]any{},
			"attributes":       map[string]any{},
			"recipes":          map[string]any{name + "::default": ""},
		},
		"recipes": []any{cookbookFilePayload(checksum)},
		"all_files": []any{
			cookbookFilePayload(checksum),
		},
	}
}

func cookbookArtifactPayload(name, identifier, version, checksum string) map[string]any {
	return map[string]any{
		"name":       name,
		"identifier": identifier,
		"version":    version,
		"chef_type":  "cookbook_version",
		"frozen?":    false,
		"metadata": map[string]any{
			"version":          version,
			"name":             name,
			"maintainer":       "OpenCook",
			"maintainer_email": "opencook@example.test",
			"description":      "functional unsupported-search cookbook artifact fixture",
			"long_description": "functional unsupported-search cookbook artifact fixture",
			"license":          "Apache v2.0",
			"dependencies":     map[string]any{},
			"attributes":       map[string]any{},
			"recipes":          map[string]any{name + "::default": ""},
			"providing":        map[string]any{name + "::default": ">= 0.0.0"},
		},
		"recipes": []any{cookbookFilePayload(checksum)},
		"all_files": []any{
			cookbookFilePayload(checksum),
		},
	}
}

func cookbookFilePayload(checksum string) map[string]any {
	return map[string]any{
		"name":        "recipes/default.rb",
		"path":        "recipes/default.rb",
		"checksum":    checksum,
		"specificity": "default",
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

	timestamp := time.Now().UTC().Format(time.RFC3339)
	signPath := c.signPathOverride
	if signPath == "" {
		signPath = req.URL.Path
	}
	signPath = canonicalSignPath(signPath)

	contentHash := hashBase64(body, c.signAlgorithm)
	canonicalUserID := canonicalSignedUserID(c.actorName, c.signVersion, c.signAlgorithm)

	var (
		stringToSign string
		signature    []byte
		err          error
	)
	switch c.signVersion {
	case "1.3":
		stringToSign = strings.Join([]string{
			"Method:" + strings.ToUpper(req.Method),
			"Path:" + signPath,
			"X-Ops-Content-Hash:" + contentHash,
			"X-Ops-Sign:version=1.3",
			"X-Ops-Timestamp:" + timestamp,
			"X-Ops-UserId:" + canonicalUserID,
			"X-Ops-Server-API-Version:" + c.serverAPIVersion,
		}, "\n")

		signatureSum := sha256.Sum256([]byte(stringToSign))
		signature, err = rsa.SignPKCS1v15(rand.Reader, c.privateKey, crypto.SHA256, signatureSum[:])
		if err != nil {
			return err
		}
		req.Header.Set("X-Ops-Sign", "algorithm=sha256;version=1.3")
		req.Header.Set("X-Ops-Server-API-Version", c.serverAPIVersion)
	case "1.1":
		stringToSign = strings.Join([]string{
			"Method:" + strings.ToUpper(req.Method),
			"Hashed Path:" + hashBase64([]byte(signPath), c.signAlgorithm),
			"X-Ops-Content-Hash:" + contentHash,
			"X-Ops-Timestamp:" + timestamp,
			"X-Ops-UserId:" + canonicalUserID,
		}, "\n")

		signature, err = legacyPrivateEncrypt(c.privateKey, []byte(stringToSign))
		if err != nil {
			return err
		}
		req.Header.Set("X-Ops-Sign", "algorithm=sha1;version=1.1;")
		req.Header.Del("X-Ops-Server-API-Version")
	default:
		return fmt.Errorf("unsupported sign version %q", c.signVersion)
	}

	signatureBase64 := base64.StdEncoding.EncodeToString(signature)
	req.Header.Set("X-Ops-Userid", c.actorName)
	req.Header.Set("X-Ops-Timestamp", timestamp)
	req.Header.Set("X-Ops-Content-Hash", contentHash)
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

func asSlice(t *testing.T, value any) []any {
	t.Helper()

	out, ok := value.([]any)
	if !ok {
		t.Fatalf("value = %T %v, want JSON array", value, value)
	}
	return out
}

func jsonStringSlice(t *testing.T, value any) []string {
	t.Helper()

	raw := asSlice(t, value)
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		text, ok := item.(string)
		if !ok {
			t.Fatalf("value item = %T %v, want JSON string", item, item)
		}
		out = append(out, text)
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func requireRows(t *testing.T, payload map[string]any, want int) {
	t.Helper()

	rows := searchRows(t, payload)
	if len(rows) != want {
		t.Fatalf("rows = %v, want %d rows", payload["rows"], want)
	}
}

func requireSearchRows(t *testing.T, client *functionalClient, path string, want int) map[string]any {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		payload := asMap(t, client.expectJSON(t, http.MethodGet, path, nil, http.StatusOK).JSON)
		if len(searchRows(t, payload)) == want {
			return payload
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s rows = %v, want %d rows", path, payload["rows"], want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func requirePartialSearchRows(t *testing.T, client *functionalClient, path string, body any, want int) map[string]any {
	t.Helper()

	deadline := time.Now().Add(15 * time.Second)
	for {
		payload := asMap(t, client.expectJSON(t, http.MethodPost, path, body, http.StatusOK).JSON)
		if len(searchRows(t, payload)) == want {
			return payload
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s partial rows = %v, want %d rows", path, payload["rows"], want)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func searchRowCount(t *testing.T, client *functionalClient, path string) int {
	t.Helper()

	payload := asMap(t, client.expectJSON(t, http.MethodGet, path, nil, http.StatusOK).JSON)
	return len(searchRows(t, payload))
}

func searchRows(t *testing.T, payload map[string]any) []any {
	t.Helper()

	rows, ok := payload["rows"].([]any)
	if !ok {
		t.Fatalf("rows = %T %v, want JSON array", payload["rows"], payload["rows"])
	}
	return rows
}

func parsePrivateKeyFile(t *testing.T, path string) *rsa.PrivateKey {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read private key %s: %v", path, err)
	}
	return parsePrivateKeyPEM(t, path, string(data))
}

func parsePrivateKeyPEM(t *testing.T, source, raw string) *rsa.PrivateKey {
	t.Helper()

	data := []byte(raw)
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("decode private key %s: no PEM block", source)
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse private key %s: %v", source, err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		t.Fatalf("private key %s has type %T, want RSA", source, parsed)
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

func canonicalSignPath(path string) string {
	if path == "" {
		return "/"
	}

	path = collapseRepeatedSlashes(path)
	if len(path) > 1 && strings.HasSuffix(path, "/") {
		path = strings.TrimSuffix(path, "/")
	}
	return path
}

func collapseRepeatedSlashes(path string) string {
	if !strings.Contains(path, "//") {
		return path
	}

	var b strings.Builder
	b.Grow(len(path))
	lastSlash := false
	for i := 0; i < len(path); i++ {
		if path[i] == '/' {
			if lastSlash {
				continue
			}
			lastSlash = true
		} else {
			lastSlash = false
		}
		b.WriteByte(path[i])
	}

	return b.String()
}

func canonicalSignedUserID(userID, signVersion, signAlgorithm string) string {
	if signVersion == "1.1" {
		return hashBase64([]byte(userID), signAlgorithm)
	}

	return userID
}

func hashBase64(data []byte, algorithm string) string {
	if algorithm == "sha256" {
		sum := sha256.Sum256(data)
		return base64.StdEncoding.EncodeToString(sum[:])
	}

	sum := sha1.Sum(data)
	return base64.StdEncoding.EncodeToString(sum[:])
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
