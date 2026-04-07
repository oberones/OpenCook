package blob

import (
	"context"
	"fmt"
	"net/url"
	"strings"
)

type S3CompatibleConfig struct {
	StorageURL     string
	Endpoint       string
	Region         string
	ForcePathStyle bool
	DisableTLS     bool
}

type S3CompatibleStore struct {
	bucket         string
	prefix         string
	endpoint       string
	region         string
	forcePathStyle bool
	disableTLS     bool
}

func NewS3CompatibleStore(cfg S3CompatibleConfig) (*S3CompatibleStore, error) {
	store := &S3CompatibleStore{
		endpoint:       strings.TrimSpace(cfg.Endpoint),
		region:         strings.TrimSpace(cfg.Region),
		forcePathStyle: cfg.ForcePathStyle,
		disableTLS:     cfg.DisableTLS,
	}

	target := strings.TrimSpace(cfg.StorageURL)
	if target == "" {
		return store, nil
	}

	parsed, err := url.Parse(target)
	if err != nil {
		return nil, fmt.Errorf("parse s3 blob URL: %w", err)
	}
	if strings.ToLower(strings.TrimSpace(parsed.Scheme)) != "s3" {
		return nil, fmt.Errorf("s3 blob backend requires s3://bucket/prefix storage URL")
	}

	store.bucket = strings.TrimSpace(parsed.Host)
	store.prefix = strings.Trim(strings.TrimSpace(parsed.Path), "/")
	return store, nil
}

func (s *S3CompatibleStore) Name() string {
	return "s3-compatible-blob-store"
}

func (s *S3CompatibleStore) Status() Status {
	if strings.TrimSpace(s.bucket) == "" {
		return Status{
			Backend:    "s3-compatible",
			Configured: false,
			Message:    "set OPENCOOK_BLOB_STORAGE_URL to s3://bucket/prefix to configure the S3-compatible blob adapter scaffold",
		}
	}

	message := "S3-compatible blob adapter scaffold is selected; provider-backed request operations are not implemented yet"
	if s.endpoint != "" {
		message = message + " (endpoint " + s.endpoint + ")"
	}

	return Status{
		Backend:    "s3-compatible",
		Configured: true,
		Message:    message,
	}
}

func (s *S3CompatibleStore) Get(context.Context, string) ([]byte, error) {
	return nil, ErrUnavailable
}

func (s *S3CompatibleStore) Put(context.Context, PutRequest) (PutResult, error) {
	return PutResult{}, ErrUnavailable
}

func (s *S3CompatibleStore) Exists(context.Context, string) (bool, error) {
	return false, ErrUnavailable
}

func (s *S3CompatibleStore) Delete(context.Context, string) error {
	return ErrUnavailable
}
