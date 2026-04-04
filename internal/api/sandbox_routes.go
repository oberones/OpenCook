package api

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/authz"
	"github.com/oberones/OpenCook/internal/blob"
	"github.com/oberones/OpenCook/internal/bootstrap"
	"github.com/oberones/OpenCook/internal/config"
)

const defaultBlobUploadURLTTL = 15 * time.Minute

func (s *server) handleSandboxes(w http.ResponseWriter, r *http.Request) {
	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	org, basePath, ok := s.resolveSandboxRoute(w, r)
	if !ok {
		return
	}
	if _, exists := state.GetOrganization(org); !exists {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "organization not found",
		})
		return
	}

	if matchesCollectionPath(r.URL.Path, basePath) {
		s.handleSandboxCollection(w, r, state, org, basePath)
		return
	}

	sandboxID, ok := pathTail(r.URL.Path, basePath+"/")
	if !ok {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "route not found in scaffold router",
		})
		return
	}

	s.handleNamedSandbox(w, r, state, org, sandboxID)
}

func (s *server) handleBlobChecksumUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		writeMethodNotAllowed(w, "method not allowed for blob checksum route", http.MethodPut)
		return
	}

	state := s.deps.Bootstrap
	if state == nil {
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "bootstrap_unavailable",
			Message: "bootstrap state service is not configured",
		})
		return
	}

	putter, ok := s.deps.Blob.(blob.Putter)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, apiError{
			Error:   "blob_unavailable",
			Message: "blob upload backend is not available",
		})
		return
	}

	checksum := strings.ToLower(strings.TrimSpace(r.PathValue("checksum")))
	if !bootstrap.ValidSandboxChecksum(checksum) {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_checksum",
			Message: "checksum path must be a valid hex md5 digest",
		})
		return
	}

	org := strings.TrimSpace(r.URL.Query().Get("org"))
	sandboxID := strings.TrimSpace(r.URL.Query().Get("sandbox_id"))
	expires := strings.TrimSpace(r.URL.Query().Get("expires"))
	signature := strings.TrimSpace(r.URL.Query().Get("signature"))
	if org == "" || sandboxID == "" || expires == "" || signature == "" {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_upload_url",
			Message: "upload URL is missing required sandbox authorization context",
		})
		return
	}
	if err := s.verifySandboxUploadURL(checksum, org, sandboxID, expires, signature); err != nil {
		writeJSON(w, http.StatusForbidden, apiError{
			Error:   "invalid_upload_url",
			Message: err.Error(),
		})
		return
	}

	sandbox, orgExists, sandboxExists := state.GetSandbox(org, sandboxID)
	if !orgExists || !sandboxExists || !sandboxContainsChecksum(sandbox, checksum) {
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "sandbox upload target not found",
		})
		return
	}

	reader := http.MaxBytesReader(w, r.Body, s.maxBlobUploadBytes())
	body, err := io.ReadAll(reader)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeJSON(w, http.StatusRequestEntityTooLarge, apiError{
				Error:   "request_body_too_large",
				Message: "blob upload exceeds configured limit",
			})
			return
		}

		s.logf("failed to read blob upload body for checksum %s: %v", checksum, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "read_body_failed",
			Message: "failed to read upload body",
		})
		return
	}
	_ = reader.Close()

	if err := validateUploadedChecksum(checksum, body, r.Header.Get("Content-MD5")); err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "invalid_checksum",
			Message: err.Error(),
		})
		return
	}

	if _, err := putter.Put(r.Context(), blob.PutRequest{
		Key:         checksum,
		ContentType: r.Header.Get("Content-Type"),
		Body:        body,
	}); err != nil {
		if errors.Is(err, blob.ErrInvalidInput) {
			writeJSON(w, http.StatusBadRequest, apiError{
				Error:   "invalid_blob_upload",
				Message: "blob upload request is invalid",
			})
			return
		}
		s.logf("blob upload failed for checksum %s: %v", checksum, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "blob_upload_failed",
			Message: "failed to persist uploaded blob",
		})
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) resolveSandboxRoute(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	org := strings.TrimSpace(r.PathValue("org"))
	if org != "" {
		return org, "/organizations/" + org + "/sandboxes", true
	}

	org, ok := s.resolveDefaultOrganizationName()
	if !ok {
		writeJSON(w, http.StatusBadRequest, apiError{
			Error:   "organization_required",
			Message: "organization context is required for this route",
		})
		return "", "", false
	}

	return org, "/sandboxes", true
}

func (s *server) handleSandboxCollection(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, basePath string) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w, "method not allowed for sandboxes route", http.MethodPost)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionCreate, authz.Resource{
		Type:         "container",
		Name:         "sandboxes",
		Organization: org,
	}) {
		return
	}

	var payload map[string]any
	if !decodeJSON(w, r, &payload) {
		return
	}

	checksums, err := extractSandboxChecksums(payload)
	if err != nil {
		writeSandboxMessages(w, http.StatusBadRequest, err.Error())
		return
	}

	sandbox, err := state.CreateSandbox(org, bootstrap.CreateSandboxInput{Checksums: checksums})
	if !s.writeSandboxError(w, err) {
		return
	}

	writeJSON(w, http.StatusCreated, s.sandboxCreateResponse(r, org, basePath, sandbox))
}

func (s *server) handleNamedSandbox(w http.ResponseWriter, r *http.Request, state *bootstrap.Service, org, sandboxID string) {
	if r.Method != http.MethodPut {
		writeMethodNotAllowed(w, "method not allowed for sandbox route", http.MethodPut)
		return
	}
	if !s.authorizeRequest(w, r, authz.ActionUpdate, authz.Resource{
		Type:         "container",
		Name:         "sandboxes",
		Organization: org,
	}) {
		return
	}

	sandbox, orgExists, sandboxExists := state.GetSandbox(org, sandboxID)
	if !orgExists || !sandboxExists {
		writeSandboxMessages(w, http.StatusNotFound, fmt.Sprintf("No such sandbox '%s'.", sandboxID))
		return
	}

	var payload struct {
		IsCompleted bool `json:"is_completed"`
	}
	if !decodeJSON(w, r, &payload) {
		return
	}
	if !payload.IsCompleted {
		writeSandboxMessages(w, http.StatusBadRequest, `JSON body must contain key "is_completed" with value true.`)
		return
	}

	missing, err := s.missingSandboxChecksums(r.Context(), sandbox)
	if err != nil {
		s.logf("sandbox checksum validation failed for %s: %v", sandboxID, err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "sandbox_failed",
			Message: "internal sandbox compatibility error",
		})
		return
	}
	if len(missing) > 0 {
		writeSandboxMessages(w, http.StatusServiceUnavailable, fmt.Sprintf("Cannot update sandbox %s: the following checksums have not been uploaded: %s", sandboxID, strings.Join(missing, ", ")))
		return
	}

	committed, err := state.DeleteSandbox(org, sandboxID)
	if !s.writeSandboxError(w, err) {
		return
	}

	writeJSON(w, http.StatusOK, sandboxCommitResponse(committed))
}

func extractSandboxChecksums(payload map[string]any) ([]string, error) {
	raw, ok := payload["checksums"]
	if !ok {
		return nil, errors.New("Field 'checksums' missing")
	}

	checksumMap, ok := raw.(map[string]any)
	if !ok || len(checksumMap) == 0 {
		return nil, errors.New("Bad checksums!")
	}

	checksums := make([]string, 0, len(checksumMap))
	for checksum, value := range checksumMap {
		if value != nil {
			return nil, errors.New("Bad checksums!")
		}
		checksum = strings.ToLower(strings.TrimSpace(checksum))
		if !bootstrap.ValidSandboxChecksum(checksum) {
			return nil, errors.New("Bad checksums!")
		}
		checksums = append(checksums, checksum)
	}

	sort.Strings(checksums)
	return checksums, nil
}

func (s *server) sandboxCreateResponse(r *http.Request, org, basePath string, sandbox bootstrap.Sandbox) map[string]any {
	checksumPayload := make(map[string]any, len(sandbox.Checksums))
	for _, checksum := range sandbox.Checksums {
		entry := map[string]any{
			"needs_upload": true,
		}
		uploaded, err := s.blobExists(r.Context(), checksum)
		if err != nil {
			s.logf("blob existence check failed for checksum %s: %v", checksum, err)
		}
		if uploaded {
			entry["needs_upload"] = false
		} else {
			entry["url"] = s.sandboxUploadURL(r, checksum, org, sandbox.ID)
		}
		checksumPayload[checksum] = entry
	}

	return map[string]any{
		"sandbox_id": sandbox.ID,
		"uri":        absoluteURL(r, basePath+"/"+sandbox.ID),
		"checksums":  checksumPayload,
	}
}

func sandboxCommitResponse(sandbox bootstrap.Sandbox) map[string]any {
	return map[string]any{
		"guid":         sandbox.ID,
		"name":         sandbox.ID,
		"checksums":    stringSliceToAny(sandbox.Checksums),
		"create_time":  sandbox.CreatedAt.UTC().Format(time.RFC3339),
		"is_completed": true,
	}
}

func (s *server) sandboxUploadURL(r *http.Request, checksum, org, sandboxID string) string {
	expiresAt := s.now().Add(defaultBlobUploadURLTTL)
	values := url.Values{}
	values.Set("org", org)
	values.Set("sandbox_id", sandboxID)
	values.Set("expires", strconv.FormatInt(expiresAt.Unix(), 10))
	values.Set("signature", s.signSandboxUploadURL(checksum, org, sandboxID, expiresAt.Unix()))
	return absoluteURL(r, "/_blob/checksums/"+checksum) + "?" + values.Encode()
}

func (s *server) verifySandboxUploadURL(checksum, org, sandboxID, expires, signature string) error {
	expiresAt, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return errors.New("upload URL has an invalid expiration")
	}
	if s.now().Unix() > expiresAt {
		return errors.New("upload URL has expired")
	}

	expected := s.signSandboxUploadURL(checksum, org, sandboxID, expiresAt)
	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return errors.New("upload URL signature is invalid")
	}
	return nil
}

func (s *server) signSandboxUploadURL(checksum, org, sandboxID string, expiresAt int64) string {
	mac := hmac.New(sha256.New, s.deps.BlobUploadSecret)
	io.WriteString(mac, checksum)
	io.WriteString(mac, "\n")
	io.WriteString(mac, org)
	io.WriteString(mac, "\n")
	io.WriteString(mac, sandboxID)
	io.WriteString(mac, "\n")
	io.WriteString(mac, strconv.FormatInt(expiresAt, 10))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *server) now() time.Time {
	if s.deps.Now != nil {
		return s.deps.Now().UTC()
	}
	return time.Now().UTC()
}

func absoluteURL(r *http.Request, path string) string {
	scheme := "http"
	if forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")); forwarded != "" {
		scheme = forwarded
	} else if r.TLS != nil {
		scheme = "https"
	}

	host := r.Host
	if host == "" {
		host = "localhost"
	}
	return scheme + "://" + host + path
}

func (s *server) blobExists(ctx context.Context, checksum string) (bool, error) {
	checker, ok := s.deps.Blob.(blob.Checker)
	if !ok {
		return false, nil
	}
	return checker.Exists(ctx, checksum)
}

func (s *server) missingSandboxChecksums(ctx context.Context, sandbox bootstrap.Sandbox) ([]string, error) {
	checker, ok := s.deps.Blob.(blob.Checker)
	if !ok {
		return nil, errors.New("blob existence backend is not available")
	}

	missing := make([]string, 0)
	for _, checksum := range sandbox.Checksums {
		exists, err := checker.Exists(ctx, checksum)
		if err != nil {
			return nil, err
		}
		if !exists {
			missing = append(missing, checksum)
		}
	}
	sort.Strings(missing)
	return missing, nil
}

func validateUploadedChecksum(expected string, body []byte, contentMD5 string) error {
	sum := md5.Sum(body)
	actual := hex.EncodeToString(sum[:])
	if actual != expected {
		return fmt.Errorf("upload body checksum %s does not match expected checksum %s", actual, expected)
	}

	contentMD5 = strings.TrimSpace(contentMD5)
	if contentMD5 == "" {
		return nil
	}

	expectedHeader := base64.StdEncoding.EncodeToString(sum[:])
	if contentMD5 != expectedHeader {
		return fmt.Errorf("content-md5 header %s does not match uploaded body", contentMD5)
	}
	return nil
}

func sandboxContainsChecksum(sandbox bootstrap.Sandbox, checksum string) bool {
	for _, candidate := range sandbox.Checksums {
		if candidate == checksum {
			return true
		}
	}
	return false
}

func (s *server) writeSandboxError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return true
	}

	switch {
	case errors.Is(err, bootstrap.ErrInvalidInput):
		writeSandboxMessages(w, http.StatusBadRequest, "Bad checksums!")
	case errors.Is(err, bootstrap.ErrNotFound):
		writeJSON(w, http.StatusNotFound, apiError{
			Error:   "not_found",
			Message: "sandbox resource not found",
		})
	default:
		s.logf("sandbox compatibility failure: %v", err)
		writeJSON(w, http.StatusInternalServerError, apiError{
			Error:   "sandbox_failed",
			Message: "internal sandbox compatibility error",
		})
	}
	return false
}

func writeSandboxMessages(w http.ResponseWriter, status int, messages ...string) {
	writeJSON(w, status, map[string]any{
		"error": messages,
	})
}

func (s *server) maxBlobUploadBytes() int64 {
	if s.deps.Config.MaxBlobUploadBytes > 0 {
		return s.deps.Config.MaxBlobUploadBytes
	}

	return config.DefaultMaxBlobUploadBytes
}
