package search

import (
	"context"
	"errors"

	"github.com/oberones/OpenCook/internal/authz"
)

var (
	ErrOrganizationNotFound = errors.New("search organization not found")
	ErrIndexNotFound        = errors.New("search index not found")
	ErrNotFound             = ErrOrganizationNotFound
	ErrUnavailable          = errors.New("search backend unavailable")
	ErrInvalidConfiguration = errors.New("search backend invalid configuration")
	// ErrInvalidQuery marks Chef search query syntax that OpenCook can parse as invalid.
	ErrInvalidQuery = errors.New("search query invalid")
	ErrRejected     = errors.New("search backend rejected request")
)

type Status struct {
	Backend    string `json:"backend"`
	Configured bool   `json:"configured"`
	Message    string `json:"message"`
}

type Query struct {
	Organization string
	Index        string
	Q            string
}

type Result struct {
	Documents []Document `json:"documents"`
}

type Document struct {
	Index    string
	Name     string
	Object   map[string]any
	Partial  map[string]any
	Fields   map[string][]string
	Resource authz.Resource
}

type Index interface {
	Name() string
	Status() Status
	Indexes(context.Context, string) ([]string, error)
	Search(context.Context, Query) (Result, error)
}

type NoopIndex struct {
	target string
}

func NewNoopIndex(target string) NoopIndex {
	return NoopIndex{target: target}
}

func (i NoopIndex) Name() string {
	return "noop-opensearch"
}

func (i NoopIndex) Status() Status {
	if i.target == "" {
		return Status{
			Backend:    "unconfigured",
			Configured: false,
			Message:    "OpenSearch is not configured; use the in-memory compatibility search index or set OPENCOOK_OPENSEARCH_URL for provider-backed search",
		}
	}

	return Status{
		Backend:    "placeholder",
		Configured: true,
		Message:    "OpenSearch is configured but no active search adapter is available",
	}
}

func (i NoopIndex) Indexes(_ context.Context, _ string) ([]string, error) {
	return nil, ErrUnavailable
}

func (i NoopIndex) Search(_ context.Context, _ Query) (Result, error) {
	return Result{}, ErrUnavailable
}
