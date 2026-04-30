package blob

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type FileStore struct {
	root string
}

func NewFileStore(target string) (*FileStore, error) {
	root, err := resolveFileRoot(target)
	if err != nil {
		return nil, err
	}

	return &FileStore{root: root}, nil
}

func (s *FileStore) Name() string {
	return "filesystem-blob-store"
}

func (s *FileStore) Status() Status {
	return Status{
		Backend:    "filesystem",
		Configured: strings.TrimSpace(s.root) != "",
		Message:    "checksum blobs use provider-backed local filesystem storage at the configured path",
	}
}

func (s *FileStore) Get(_ context.Context, key string) ([]byte, error) {
	path, err := s.objectPath(key)
	if err != nil {
		return nil, err
	}

	body, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return body, nil
}

func (s *FileStore) Put(_ context.Context, req PutRequest) (PutResult, error) {
	path, err := s.objectPath(req.Key)
	if err != nil {
		return PutResult{}, err
	}

	if err := os.MkdirAll(s.root, 0o755); err != nil {
		return PutResult{}, err
	}

	temp, err := os.CreateTemp(s.root, ".opencook-blob-*")
	if err != nil {
		return PutResult{}, err
	}
	tempName := temp.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempName)
		}
	}()

	if _, err := temp.Write(req.Body); err != nil {
		_ = temp.Close()
		return PutResult{}, err
	}
	if err := temp.Close(); err != nil {
		return PutResult{}, err
	}
	if err := os.Rename(tempName, path); err != nil {
		if _, statErr := os.Stat(path); statErr == nil {
			if removeErr := os.Remove(path); removeErr != nil {
				return PutResult{}, removeErr
			}
			if err := os.Rename(tempName, path); err != nil {
				return PutResult{}, err
			}
		} else {
			return PutResult{}, err
		}
	}
	cleanupTemp = false

	return PutResult{Location: req.Key}, nil
}

func (s *FileStore) Exists(_ context.Context, key string) (bool, error) {
	path, err := s.objectPath(key)
	if err != nil {
		return false, err
	}

	_, err = os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// List returns the flat checksum keys currently present in the filesystem
// backend, giving migration tooling a safe way to report orphan candidates.
func (s *FileStore) List(_ context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type()&fs.ModeType != 0 {
			continue
		}
		name := entry.Name()
		if _, err := normalizeObjectKey(name); err == nil {
			keys = append(keys, name)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *FileStore) Delete(_ context.Context, key string) error {
	path, err := s.objectPath(key)
	if err != nil {
		return err
	}

	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}

	return nil
}

func (s *FileStore) objectPath(key string) (string, error) {
	key, err := normalizeObjectKey(key)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.root, key), nil
}

func resolveFileRoot(target string) (string, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return "", fmt.Errorf("filesystem blob backend requires OPENCOOK_BLOB_STORAGE_URL")
	}

	if strings.Contains(target, "://") {
		parsed, err := url.Parse(target)
		if err != nil {
			return "", fmt.Errorf("parse filesystem blob URL: %w", err)
		}
		if strings.ToLower(strings.TrimSpace(parsed.Scheme)) != "file" {
			return "", fmt.Errorf("filesystem blob backend requires file:// storage URL")
		}
		if parsed.Host != "" && parsed.Host != "localhost" {
			return "", fmt.Errorf("filesystem blob URL host %q is not supported", parsed.Host)
		}
		target = parsed.Path
	}

	target = filepath.Clean(target)
	if !filepath.IsAbs(target) {
		abs, err := filepath.Abs(target)
		if err != nil {
			return "", err
		}
		target = abs
	}
	if target == "" || target == "." {
		return "", fmt.Errorf("filesystem blob backend requires a non-empty root path")
	}
	return target, nil
}

func normalizeObjectKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", ErrInvalidInput
	}
	if strings.Contains(key, "/") || strings.Contains(key, `\`) {
		return "", ErrInvalidInput
	}

	clean := filepath.Clean(key)
	if clean == "." || clean == ".." || clean != key {
		return "", ErrInvalidInput
	}
	return clean, nil
}
