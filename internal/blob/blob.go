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
			Message:    "set OPENCOOK_BLOB_STORAGE_URL to configure object storage",
		}
	}

	return Status{
		Backend:    "placeholder",
		Configured: true,
		Message:    "blob storage adapter scaffold only",
	}
}
