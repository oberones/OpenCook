package blob

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("blob not found")
var ErrInvalidInput = errors.New("invalid blob input")
var ErrUnavailable = errors.New("blob backend unavailable")

type Status struct {
	Backend    string `json:"backend"`
	Configured bool   `json:"configured"`
	Message    string `json:"message"`
}

type Store interface {
	Name() string
	Status() Status
}

type PutRequest struct {
	Key         string
	ContentType string
	Body        []byte
}

type PutResult struct {
	Location string
}

type Getter interface {
	Get(context.Context, string) ([]byte, error)
}

type Putter interface {
	Put(context.Context, PutRequest) (PutResult, error)
}

type Checker interface {
	Exists(context.Context, string) (bool, error)
}

// Lister is an optional safe inventory capability for local/provider adapters
// that can enumerate checksum keys without mutating blob contents.
type Lister interface {
	List(context.Context) ([]string, error)
}

type Deleter interface {
	Delete(context.Context, string) error
}

type NoopStore struct {
	target string
}

func NewNoopStore(target string) NoopStore {
	return NoopStore{target: target}
}

func (s NoopStore) Name() string {
	return "noop-blob-store"
}

func (s NoopStore) Status() Status {
	if s.target == "" {
		return Status{
			Backend:    "unconfigured",
			Configured: false,
			Message:    "blob storage is not configured; use the memory, filesystem, or S3-compatible backend for checksum content",
		}
	}

	return Status{
		Backend:    "placeholder",
		Configured: true,
		Message:    "blob storage is configured but no active checksum blob adapter is available",
	}
}
