package authn

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultServerAPIVersion = "0"

type signDescription struct {
	Raw       string
	Version   string
	Algorithm string
}

type parsedRequest struct {
	UserID           string
	Body             []byte
	Method           string
	Path             string
	ServerAPIVersion string
	TimestampRaw     string
	Timestamp        time.Time
	ContentHash      string
	SignatureBase64  string
	Sign             signDescription
}

func parseRequest(req RequestContext, now time.Time, allowedClockSkew time.Duration) (parsedRequest, error) {
	headers := normalizedHeaders(req.Headers)

	missing := missingHeaders(headers, "x-ops-sign", "x-ops-userid", "x-ops-timestamp", "x-ops-content-hash")
	if len(missing) > 0 {
		return parsedRequest{}, newError(ErrorKindMissingHeaders, "missing required authentication headers", missing...)
	}

	sign, err := parseSignDescription(headers["x-ops-sign"])
	if err != nil {
		return parsedRequest{}, err
	}

	signatureBase64, err := collectAuthorizationHeader(headers)
	if err != nil {
		return parsedRequest{}, err
	}

	timestampRaw := headers["x-ops-timestamp"]
	timestamp, err := parseTimestamp(timestampRaw)
	if err != nil {
		return parsedRequest{}, newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Timestamp")
	}

	if allowedClockSkew > 0 {
		delta := now.Sub(timestamp)
		if delta < 0 {
			delta = -delta
		}
		if delta > allowedClockSkew {
			return parsedRequest{}, newError(ErrorKindBadClock, "request timestamp is outside the allowed clock skew", "X-Ops-Timestamp")
		}
	}

	contentHash := headers["x-ops-content-hash"]
	if err := validateContentHash(contentHash, sign); err != nil {
		return parsedRequest{}, err
	}

	body := req.Body
	if body == nil {
		body = []byte{}
	}

	expectedContentHash := hashBase64(body, sign)
	if contentHash != expectedContentHash {
		return parsedRequest{}, newError(ErrorKindBadHeaders, "request body does not match X-Ops-Content-Hash", "X-Ops-Content-Hash")
	}

	serverAPIVersion := strings.TrimSpace(req.ServerAPIVersion)
	if serverAPIVersion == "" {
		serverAPIVersion = strings.TrimSpace(headers["x-ops-server-api-version"])
	}
	if serverAPIVersion == "" {
		serverAPIVersion = defaultServerAPIVersion
	}

	path := req.Path
	if path == "" {
		path = "/"
	}

	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = "GET"
	}

	return parsedRequest{
		UserID:           strings.TrimSpace(headers["x-ops-userid"]),
		Body:             body,
		Method:           method,
		Path:             path,
		ServerAPIVersion: serverAPIVersion,
		TimestampRaw:     timestampRaw,
		Timestamp:        timestamp,
		ContentHash:      contentHash,
		SignatureBase64:  signatureBase64,
		Sign:             sign,
	}, nil
}

func normalizedHeaders(headers map[string]string) map[string]string {
	out := make(map[string]string, len(headers))
	for key, value := range headers {
		out[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return out
}

func missingHeaders(headers map[string]string, keys ...string) []string {
	var missing []string
	for _, key := range keys {
		if strings.TrimSpace(headers[key]) == "" {
			missing = append(missing, canonicalHeaderName(key))
		}
	}
	return missing
}

func parseSignDescription(raw string) (signDescription, error) {
	desc := signDescription{Raw: strings.TrimSpace(raw)}
	if desc.Raw == "" {
		return signDescription{}, newError(ErrorKindMissingHeaders, "missing required authentication headers", "X-Ops-Sign")
	}

	parts := strings.Split(desc.Raw, ";")
	values := make(map[string]string, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
		}
		values[strings.ToLower(strings.TrimSpace(key))] = strings.ToLower(strings.TrimSpace(value))
	}

	version := values["version"]
	if version == "" {
		return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
	}

	algorithm := values["algorithm"]
	switch version {
	case "1.0":
		if algorithm == "" {
			algorithm = "sha1"
		}
		if algorithm != "sha1" {
			return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
		}
	case "1.1":
		if algorithm == "" {
			algorithm = "sha1"
		}
		if algorithm != "sha1" {
			return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
		}
	case "1.3":
		if algorithm == "" {
			algorithm = "sha256"
		}
		if algorithm != "sha256" {
			return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
		}
	default:
		return signDescription{}, newError(ErrorKindUnsupportedSign, "unsupported authentication protocol version", "X-Ops-Sign")
	}

	desc.Version = version
	desc.Algorithm = algorithm
	return desc, nil
}

func collectAuthorizationHeader(headers map[string]string) (string, error) {
	indices := make(map[int]string)
	for key, value := range headers {
		if !strings.HasPrefix(key, "x-ops-authorization-") {
			continue
		}
		rawIndex := strings.TrimPrefix(key, "x-ops-authorization-")
		index, err := strconv.Atoi(rawIndex)
		if err != nil || index <= 0 {
			return "", newError(ErrorKindBadHeaders, "bad header value", canonicalHeaderName(key))
		}
		if value == "" {
			return "", newError(ErrorKindBadHeaders, "bad header value", canonicalHeaderName(key))
		}
		indices[index] = value
	}

	if len(indices) == 0 {
		return "", newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Authorization-1")
	}

	keys := make([]int, 0, len(indices))
	for index := range indices {
		keys = append(keys, index)
	}
	sort.Ints(keys)

	if keys[0] != 1 {
		return "", newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Authorization-1")
	}

	var b strings.Builder
	for expected := 1; expected <= keys[len(keys)-1]; expected++ {
		part, ok := indices[expected]
		if !ok {
			return "", newError(ErrorKindBadHeaders, "bad header value", fmt.Sprintf("X-Ops-Authorization-%d", expected))
		}
		b.WriteString(part)
	}

	return b.String(), nil
}

func parseTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, fmt.Errorf("empty timestamp")
	}

	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		return ts, nil
	}

	return time.Parse(time.RFC3339Nano, raw)
}

func validateContentHash(contentHash string, sign signDescription) error {
	decoded, err := base64.StdEncoding.DecodeString(contentHash)
	if err != nil {
		return newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Content-Hash")
	}

	expectedLength := sha1.Size
	if sign.Algorithm == "sha256" {
		expectedLength = sha256.Size
	}
	if len(decoded) != expectedLength {
		return newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Content-Hash")
	}

	return nil
}

func hashBase64(data []byte, sign signDescription) string {
	if sign.Algorithm == "sha256" {
		sum := sha256.Sum256(data)
		return base64.StdEncoding.EncodeToString(sum[:])
	}

	sum := sha1.Sum(data)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func canonicalRequestPath(path string) string {
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

func canonicalUserID(userID string, sign signDescription) string {
	if sign.Version == "1.1" {
		return hashBase64([]byte(userID), signDescription{Algorithm: sign.Algorithm})
	}

	return userID
}

// LegacyHashedPathDebugValue returns the server-side hashed path used by
// legacy Chef signing versions for safe authn failure diagnostics.
func LegacyHashedPathDebugValue(rawSign, path string) (string, bool) {
	sign, err := parseSignDescription(rawSign)
	if err != nil {
		return "", false
	}
	if sign.Version == "1.3" {
		return "", false
	}

	return hashBase64([]byte(canonicalRequestPath(path)), signDescription{Algorithm: sign.Algorithm}), true
}

func canonicalHeaderName(key string) string {
	switch strings.ToLower(key) {
	case "x-ops-sign":
		return "X-Ops-Sign"
	case "x-ops-userid":
		return "X-Ops-UserId"
	case "x-ops-timestamp":
		return "X-Ops-Timestamp"
	case "x-ops-content-hash":
		return "X-Ops-Content-Hash"
	case "x-ops-server-api-version":
		return "X-Ops-Server-API-Version"
	default:
		if strings.HasPrefix(strings.ToLower(key), "x-ops-authorization-") {
			prefix := "x-ops-authorization-"
			return "X-Ops-Authorization-" + key[len(prefix):]
		}
		return key
	}
}
