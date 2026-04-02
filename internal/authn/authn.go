package authn

import (
	"context"
	"crypto/rsa"
)

type Principal struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
}

type Key struct {
	ID        string
	Principal Principal
	PublicKey *rsa.PublicKey
}

type RequestContext struct {
	Method           string
	Path             string
	Body             []byte
	Headers          map[string]string
	Organization     string
	ServerAPIVersion string
}

type VerificationResult struct {
	Authenticated bool      `json:"authenticated"`
	Mode          string    `json:"mode"`
	Principal     Principal `json:"principal"`
	SignVersion   string    `json:"sign_version,omitempty"`
	Algorithm     string    `json:"algorithm,omitempty"`
	KeyID         string    `json:"key_id,omitempty"`
}

type Capabilities struct {
	SupportedSignVersions []string `json:"supported_sign_versions"`
	SupportedAlgorithms   []string `json:"supported_algorithms"`
	AllowedClockSkew      string   `json:"allowed_clock_skew"`
	KeyStore              string   `json:"key_store"`
}

type Verifier interface {
	Name() string
	Capabilities() Capabilities
	Verify(context.Context, RequestContext) (VerificationResult, error)
}

type KeyStore interface {
	Name() string
	Lookup(context.Context, string, string) ([]Key, error)
}
