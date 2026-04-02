package search

import "context"

type Status struct {
	Backend    string `json:"backend"`
	Configured bool   `json:"configured"`
	Message    string `json:"message"`
}

type Query struct {
	Index string
	Q     string
	Start int
	Rows  int
}

type Result struct {
	Total int      `json:"total"`
	IDs   []string `json:"ids"`
}

type Index interface {
	Name() string
	Status() Status
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
			Message:    "set OPENCOOK_OPENSEARCH_URL to configure search",
		}
	}

	return Status{
		Backend:    "placeholder",
		Configured: true,
		Message:    "OpenSearch adapter scaffold only",
	}
}

func (i NoopIndex) Search(_ context.Context, _ Query) (Result, error) {
	return Result{}, nil
}
