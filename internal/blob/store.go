package blob

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/oberones/OpenCook/internal/config"
)

const (
	BackendMemory     = "memory"
	BackendFilesystem = "filesystem"
	BackendS3         = "s3"
)

func NewStore(cfg config.Config) (Store, error) {
	switch resolveBackend(cfg) {
	case BackendMemory:
		return NewMemoryStore(cfg.BlobStorageURL), nil
	case BackendFilesystem:
		return NewFileStore(cfg.BlobStorageURL)
	case BackendS3:
		return NewS3CompatibleStore(S3CompatibleConfig{
			StorageURL:     cfg.BlobStorageURL,
			Endpoint:       cfg.BlobS3Endpoint,
			Region:         cfg.BlobS3Region,
			ForcePathStyle: cfg.BlobS3ForcePathStyle,
			DisableTLS:     cfg.BlobS3DisableTLS,
		})
	default:
		return nil, fmt.Errorf("unsupported blob backend %q", strings.TrimSpace(cfg.BlobBackend))
	}
}

func resolveBackend(cfg config.Config) string {
	backend := strings.ToLower(strings.TrimSpace(cfg.BlobBackend))
	if backend != "" {
		return backend
	}

	target := strings.TrimSpace(cfg.BlobStorageURL)
	if target == "" {
		return BackendMemory
	}

	if strings.Contains(target, "://") {
		parsed, err := url.Parse(target)
		if err != nil {
			return BackendMemory
		}

		switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
		case "file":
			return BackendFilesystem
		case "s3":
			return BackendS3
		default:
			return BackendMemory
		}
	}

	if filepath.IsAbs(target) || strings.HasPrefix(target, ".") {
		return BackendFilesystem
	}

	return BackendMemory
}
