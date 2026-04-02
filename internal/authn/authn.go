package authn

import (
	"context"
	"errors"
)

var ErrNotImplemented = errors.New("chef request verification not implemented")

type Principal struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Organization string `json:"organization,omitempty"`
}

type RequestContext struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
}

type VerificationResult struct {
	Authenticated bool      `json:"authenticated"`
	Mode          string    `json:"mode"`
	Principal     Principal `json:"principal"`
}

type Verifier interface {
	Name() string
	Verify(context.Context, RequestContext) (VerificationResult, error)
}

type NoopVerifier struct{}

func (NoopVerifier) Name() string {
	return "noop-chef-signer"
}

func (NoopVerifier) Verify(_ context.Context, _ RequestContext) (VerificationResult, error) {
	return VerificationResult{
		Authenticated: false,
		Mode:          "scaffold",
		Principal: Principal{
			Type: "unknown",
			Name: "anonymous",
		},
	}, ErrNotImplemented
}

