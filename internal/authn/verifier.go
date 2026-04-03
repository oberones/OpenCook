package authn

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

type Options struct {
	AllowedClockSkew        *time.Duration
	Now                     func() time.Time
	DefaultServerAPIVersion string
}

type ChefVerifier struct {
	store KeyStore
	opts  Options
}

func NewChefVerifier(store KeyStore, opts Options) *ChefVerifier {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.DefaultServerAPIVersion == "" {
		opts.DefaultServerAPIVersion = defaultServerAPIVersion
	}
	if store == nil {
		store = NewMemoryKeyStore()
	}

	return &ChefVerifier{
		store: store,
		opts:  opts,
	}
}

func (v *ChefVerifier) Name() string {
	return "chef-request-verifier"
}

func (v *ChefVerifier) Capabilities() Capabilities {
	return Capabilities{
		SupportedSignVersions: []string{"1.0", "1.1", "1.3"},
		SupportedAlgorithms:   []string{"sha1", "sha256"},
		AllowedClockSkew:      v.allowedClockSkew().String(),
		KeyStore:              v.store.Name(),
	}
}

func (v *ChefVerifier) Verify(ctx context.Context, req RequestContext) (VerificationResult, error) {
	if req.ServerAPIVersion == "" {
		req.ServerAPIVersion = v.opts.DefaultServerAPIVersion
	}
	now := v.opts.Now().UTC()

	principalHint := Principal{
		Type:         "unknown",
		Name:         requestUserID(req.Headers),
		Organization: req.Organization,
	}

	parsed, err := parseRequest(req, now, v.allowedClockSkew())
	if err != nil {
		return VerificationResult{
			Authenticated: false,
			Mode:          "failed",
			Principal:     principalHint,
		}, err
	}

	keys, err := v.store.Lookup(ctx, parsed.UserID, req.Organization)
	if err != nil {
		return VerificationResult{
			Authenticated: false,
			Mode:          "failed",
			Principal:     principalHint,
		}, newError(ErrorKindKeyStoreFailure, fmt.Sprintf("key lookup failed: %v", err))
	}
	keys = filterKeysByTime(keys, now)

	if len(keys) == 0 {
		return VerificationResult{
			Authenticated: false,
			Mode:          "failed",
			Principal:     principalHint,
		}, newError(ErrorKindRequestorNotFound, "requestor not found")
	}

	stringToSign := canonicalStringToSign(parsed)
	signature, err := base64.StdEncoding.DecodeString(parsed.SignatureBase64)
	if err != nil {
		return VerificationResult{
			Authenticated: false,
			Mode:          "failed",
			Principal:     keys[0].Principal,
		}, newError(ErrorKindBadHeaders, "bad header value", "X-Ops-Authorization-1")
	}

	for _, key := range keys {
		if verifySignature(parsed, stringToSign, signature, key.PublicKey) == nil {
			return VerificationResult{
				Authenticated: true,
				Mode:          "chef-signed-request",
				Principal:     key.Principal,
				SignVersion:   parsed.Sign.Version,
				Algorithm:     parsed.Sign.Algorithm,
				KeyID:         key.ID,
			}, nil
		}
	}

	return VerificationResult{
		Authenticated: false,
		Mode:          "failed",
		Principal:     keys[0].Principal,
		SignVersion:   parsed.Sign.Version,
		Algorithm:     parsed.Sign.Algorithm,
	}, newError(ErrorKindBadSignature, "signature verification failed")
}

func filterKeysByTime(keys []Key, now time.Time) []Key {
	if len(keys) == 0 {
		return nil
	}

	out := make([]Key, 0, len(keys))
	for _, key := range keys {
		if key.ExpiresAt != nil && !key.ExpiresAt.After(now) {
			continue
		}
		out = append(out, key)
	}
	return out
}

func (v *ChefVerifier) allowedClockSkew() time.Duration {
	if v.opts.AllowedClockSkew == nil {
		return 15 * time.Minute
	}

	return *v.opts.AllowedClockSkew
}

func requestUserID(headers map[string]string) string {
	return normalizedHeaders(headers)["x-ops-userid"]
}

func canonicalStringToSign(req parsedRequest) string {
	if req.Sign.Version == "1.3" {
		return strings.Join([]string{
			"Method:" + req.Method,
			"Path:" + req.Path,
			"X-Ops-Content-Hash:" + req.ContentHash,
			"X-Ops-Sign:version=1.3",
			"X-Ops-Timestamp:" + req.TimestampRaw,
			"X-Ops-UserId:" + req.UserID,
			"X-Ops-Server-API-Version:" + req.ServerAPIVersion,
		}, "\n")
	}

	return strings.Join([]string{
		"Method:" + req.Method,
		"Hashed Path:" + hashBase64([]byte(req.Path), signDescription{Algorithm: req.Sign.Algorithm}),
		"X-Ops-Content-Hash:" + req.ContentHash,
		"X-Ops-Timestamp:" + req.TimestampRaw,
		"X-Ops-UserId:" + req.UserID,
	}, "\n")
}

func verifySignature(req parsedRequest, stringToSign string, signature []byte, publicKey *rsa.PublicKey) error {
	if publicKey == nil {
		return fmt.Errorf("missing public key")
	}

	if req.Sign.Version == "1.3" {
		sum := sha256.Sum256([]byte(stringToSign))
		return rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, sum[:], signature)
	}

	message, err := legacyPublicDecrypt(publicKey, signature)
	if err != nil {
		return err
	}
	if string(message) != stringToSign {
		return fmt.Errorf("legacy signature mismatch")
	}
	return nil
}
