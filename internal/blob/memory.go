package blob

import (
	"context"
	"sort"
	"strings"
	"sync"
)

type Object struct {
	ContentType string
	Body        []byte
}

type MemoryStore struct {
	mu      sync.RWMutex
	target  string
	objects map[string]Object
}

func NewMemoryStore(target string) *MemoryStore {
	return &MemoryStore{
		target:  strings.TrimSpace(target),
		objects: make(map[string]Object),
	}
}

func (s *MemoryStore) Name() string {
	return "memory-blob-store"
}

func (s *MemoryStore) Status() Status {
	message := "blob compatibility routes are backed by in-memory content-addressed storage; filesystem and S3-compatible modes are available for provider-backed paths"
	if s.target != "" {
		message = "blob storage is explicitly pinned to the in-memory compatibility adapter for this phase"
	}

	return Status{
		Backend:    "memory-compat",
		Configured: true,
		Message:    message,
	}
}

func (s *MemoryStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	object, ok := s.objects[strings.TrimSpace(key)]
	if !ok {
		return nil, ErrNotFound
	}

	body := make([]byte, len(object.Body))
	copy(body, object.Body)
	return body, nil
}

func (s *MemoryStore) Put(_ context.Context, req PutRequest) (PutResult, error) {
	key := strings.TrimSpace(req.Key)
	if key == "" {
		return PutResult{}, ErrInvalidInput
	}

	body := make([]byte, len(req.Body))
	copy(body, req.Body)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[key] = Object{
		ContentType: strings.TrimSpace(req.ContentType),
		Body:        body,
	}

	return PutResult{Location: key}, nil
}

func (s *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.objects[strings.TrimSpace(key)]
	return ok, nil
}

// List returns the in-memory checksum keys for local migration tests and
// memory-backed development runs without exposing mutable object internals.
func (s *MemoryStore) List(_ context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.objects))
	for key := range s.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *MemoryStore) Delete(_ context.Context, key string) error {
	key = strings.TrimSpace(key)
	if key == "" {
		return ErrInvalidInput
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.objects[key]; !ok {
		return ErrNotFound
	}
	delete(s.objects, key)
	return nil
}
