package blob

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/oberones/OpenCook/internal/config"
)

const (
	BackendMemory     = "memory"
	BackendFilesystem = "filesystem"
	BackendS3         = "s3"
)

func NewStore(cfg config.Config) (Store, error) {
	backend, err := resolveBackend(cfg)
	if err != nil {
		return nil, err
	}

	switch backend {
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
			AccessKeyID:    cfg.BlobS3AccessKeyID,
			SecretKey:      cfg.BlobS3SecretKey,
			SessionToken:   cfg.BlobS3SessionToken,
			RequestTimeout: cfg.BlobS3RequestTimeout,
			MaxRetries:     cfg.BlobS3MaxRetries,
		})
	default:
		return nil, fmt.Errorf("unsupported blob backend %q", strings.TrimSpace(cfg.BlobBackend))
	}
}

func resolveBackend(cfg config.Config) (string, error) {
	backend := strings.ToLower(strings.TrimSpace(cfg.BlobBackend))
	if backend != "" {
		return backend, nil
	}

	target := strings.TrimSpace(cfg.BlobStorageURL)
	if target == "" {
		return BackendMemory, nil
	}

	if strings.Contains(target, "://") {
		parsed, err := url.Parse(target)
		if err != nil {
			return "", fmt.Errorf("parse blob storage URL: %w", err)
		}

		switch strings.ToLower(strings.TrimSpace(parsed.Scheme)) {
		case "file":
			return BackendFilesystem, nil
		case "s3":
			return BackendS3, nil
		default:
			return BackendMemory, nil
		}
	}

	return BackendFilesystem, nil
}
