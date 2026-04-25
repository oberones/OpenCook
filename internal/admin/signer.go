package admin

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type signer struct {
	requestorName    string
	serverAPIVersion string
	privateKey       *rsa.PrivateKey
	now              func() time.Time
}

func (s signer) sign(req *http.Request, body []byte) error {
	if s.privateKey == nil {
		return errorf(CodeSigningFailed, "private key is required")
	}
	if strings.TrimSpace(s.requestorName) == "" {
		return errorf(CodeSigningFailed, "requestor name is required")
	}
	if body == nil {
		body = []byte{}
	}

	now := s.now
	if now == nil {
		now = time.Now
	}
	serverAPIVersion := strings.TrimSpace(s.serverAPIVersion)
	if serverAPIVersion == "" {
		serverAPIVersion = defaultServerAPIVersion
	}

	contentSum := sha256.Sum256(body)
	contentHash := base64.StdEncoding.EncodeToString(contentSum[:])
	timestamp := now().UTC().Format(time.RFC3339)
	signPath := requestSignPath(req)
	stringToSign := strings.Join([]string{
		"Method:" + strings.ToUpper(req.Method),
		"Path:" + signPath,
		"X-Ops-Content-Hash:" + contentHash,
		"X-Ops-Sign:version=1.3",
		"X-Ops-Timestamp:" + timestamp,
		"X-Ops-UserId:" + s.requestorName,
		"X-Ops-Server-API-Version:" + serverAPIVersion,
	}, "\n")

	signatureSum := sha256.Sum256([]byte(stringToSign))
	signature, err := rsa.SignPKCS1v15(rand.Reader, s.privateKey, crypto.SHA256, signatureSum[:])
	if err != nil {
		return errorf(CodeSigningFailed, "sign request")
	}
	signatureBase64 := base64.StdEncoding.EncodeToString(signature)

	req.Header.Set("X-Ops-Sign", "algorithm=sha256;version=1.3")
	req.Header.Set("X-Ops-Userid", s.requestorName)
	req.Header.Set("X-Ops-Timestamp", timestamp)
	req.Header.Set("X-Ops-Content-Hash", contentHash)
	req.Header.Set("X-Ops-Server-API-Version", serverAPIVersion)
	for index, chunk := range splitBase64(signatureBase64, 60) {
		req.Header.Set(fmt.Sprintf("X-Ops-Authorization-%d", index+1), chunk)
	}
	return nil
}

func requestSignPath(req *http.Request) string {
	if req == nil || req.URL == nil {
		return "/"
	}
	path := req.URL.EscapedPath()
	if path == "" {
		path = req.URL.Path
	}
	if path == "" {
		return "/"
	}
	return path
}

func splitBase64(value string, width int) []string {
	if width <= 0 {
		return []string{value}
	}

	var out []string
	for len(value) > width {
		out = append(out, value[:width])
		value = value[width:]
	}
	if value != "" {
		out = append(out, value)
	}
	return out
}
