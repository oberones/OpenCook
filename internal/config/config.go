package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName         string
	Environment         string
	ListenAddress       string
	DefaultOrganization string
	ReadTimeout         time.Duration
	WriteTimeout        time.Duration
	ShutdownTimeout     time.Duration
	AuthSkew            time.Duration
	MaxAuthBodyBytes    int64
	MaxBlobUploadBytes  int64
	BootstrapMode       bool

	PostgresDSN          string
	OpenSearchURL        string
	BlobBackend          string
	BlobStorageURL       string
	BlobS3Endpoint       string
	BlobS3Region         string
	BlobS3ForcePathStyle bool
	BlobS3DisableTLS     bool
	BlobS3AccessKeyID    string
	BlobS3SecretKey      string
	BlobS3SessionToken   string
	BlobS3RequestTimeout time.Duration
	BlobS3MaxRetries     int

	BootstrapRequestorName          string
	BootstrapRequestorType          string
	BootstrapRequestorOrganization  string
	BootstrapRequestorKeyID         string
	BootstrapRequestorPublicKeyPath string
}

const DefaultMaxAuthBodyBytes int64 = 8 << 20
const DefaultMaxBlobUploadBytes int64 = 64 << 20
const DefaultBlobS3RequestTimeout = 30 * time.Second
const DefaultBlobS3MaxRetries = 2

func LoadFromEnv() (Config, error) {
	readTimeout, err := envDuration("OPENCOOK_READ_TIMEOUT", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	writeTimeout, err := envDuration("OPENCOOK_WRITE_TIMEOUT", 30*time.Second)
	if err != nil {
		return Config{}, err
	}

	shutdownTimeout, err := envDuration("OPENCOOK_SHUTDOWN_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	authSkew, err := envDuration("OPENCOOK_AUTH_SKEW", 15*time.Minute)
	if err != nil {
		return Config{}, err
	}

	maxAuthBodyBytes, err := envInt64("OPENCOOK_MAX_AUTH_BODY_BYTES", DefaultMaxAuthBodyBytes)
	if err != nil {
		return Config{}, err
	}
	if maxAuthBodyBytes <= 0 {
		return Config{}, fmt.Errorf("OPENCOOK_MAX_AUTH_BODY_BYTES: must be positive")
	}

	maxBlobUploadBytes, err := envInt64("OPENCOOK_MAX_BLOB_UPLOAD_BYTES", DefaultMaxBlobUploadBytes)
	if err != nil {
		return Config{}, err
	}
	if maxBlobUploadBytes <= 0 {
		return Config{}, fmt.Errorf("OPENCOOK_MAX_BLOB_UPLOAD_BYTES: must be positive")
	}

	blobS3RequestTimeout, err := envDuration("OPENCOOK_BLOB_S3_REQUEST_TIMEOUT", DefaultBlobS3RequestTimeout)
	if err != nil {
		return Config{}, err
	}
	if blobS3RequestTimeout <= 0 {
		return Config{}, fmt.Errorf("OPENCOOK_BLOB_S3_REQUEST_TIMEOUT: must be positive")
	}

	blobS3MaxRetries, err := envInt("OPENCOOK_BLOB_S3_MAX_RETRIES", DefaultBlobS3MaxRetries)
	if err != nil {
		return Config{}, err
	}
	if blobS3MaxRetries < 0 {
		return Config{}, fmt.Errorf("OPENCOOK_BLOB_S3_MAX_RETRIES: must be zero or greater")
	}

	bootstrapMode, err := envBool("OPENCOOK_BOOTSTRAP_MODE", true)
	if err != nil {
		return Config{}, err
	}
	blobS3ForcePathStyle, err := envBool("OPENCOOK_BLOB_S3_FORCE_PATH_STYLE", false)
	if err != nil {
		return Config{}, err
	}
	blobS3DisableTLS, err := envBool("OPENCOOK_BLOB_S3_DISABLE_TLS", false)
	if err != nil {
		return Config{}, err
	}

	return Config{
		ServiceName:                     envString("OPENCOOK_SERVICE_NAME", "opencook"),
		Environment:                     envString("OPENCOOK_ENV", "development"),
		ListenAddress:                   envString("OPENCOOK_LISTEN_ADDRESS", ":4000"),
		DefaultOrganization:             envString("OPENCOOK_DEFAULT_ORGANIZATION", ""),
		ReadTimeout:                     readTimeout,
		WriteTimeout:                    writeTimeout,
		ShutdownTimeout:                 shutdownTimeout,
		AuthSkew:                        authSkew,
		MaxAuthBodyBytes:                maxAuthBodyBytes,
		MaxBlobUploadBytes:              maxBlobUploadBytes,
		BootstrapMode:                   bootstrapMode,
		PostgresDSN:                     strings.TrimSpace(os.Getenv("OPENCOOK_POSTGRES_DSN")),
		OpenSearchURL:                   strings.TrimSpace(os.Getenv("OPENCOOK_OPENSEARCH_URL")),
		BlobBackend:                     envString("OPENCOOK_BLOB_BACKEND", ""),
		BlobStorageURL:                  strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_STORAGE_URL")),
		BlobS3Endpoint:                  strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_S3_ENDPOINT")),
		BlobS3Region:                    strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_S3_REGION")),
		BlobS3ForcePathStyle:            blobS3ForcePathStyle,
		BlobS3DisableTLS:                blobS3DisableTLS,
		BlobS3AccessKeyID:               strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_S3_ACCESS_KEY_ID")),
		BlobS3SecretKey:                 strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY")),
		BlobS3SessionToken:              strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_S3_SESSION_TOKEN")),
		BlobS3RequestTimeout:            blobS3RequestTimeout,
		BlobS3MaxRetries:                blobS3MaxRetries,
		BootstrapRequestorName:          envString("OPENCOOK_BOOTSTRAP_REQUESTOR_NAME", ""),
		BootstrapRequestorType:          envString("OPENCOOK_BOOTSTRAP_REQUESTOR_TYPE", "user"),
		BootstrapRequestorOrganization:  envString("OPENCOOK_BOOTSTRAP_REQUESTOR_ORG", ""),
		BootstrapRequestorKeyID:         envString("OPENCOOK_BOOTSTRAP_REQUESTOR_KEY_ID", "default"),
		BootstrapRequestorPublicKeyPath: envString("OPENCOOK_BOOTSTRAP_PUBLIC_KEY_PATH", ""),
	}, nil
}

func (c Config) Redacted() map[string]string {
	return map[string]string{
		"service_name":               c.ServiceName,
		"environment":                c.Environment,
		"listen_address":             c.ListenAddress,
		"default_organization":       c.DefaultOrganization,
		"read_timeout":               c.ReadTimeout.String(),
		"write_timeout":              c.WriteTimeout.String(),
		"shutdown_timeout":           c.ShutdownTimeout.String(),
		"auth_skew":                  c.AuthSkew.String(),
		"max_auth_body_bytes":        strconv.FormatInt(c.MaxAuthBodyBytes, 10),
		"max_blob_upload_bytes":      strconv.FormatInt(c.MaxBlobUploadBytes, 10),
		"bootstrap_mode":             strconv.FormatBool(c.BootstrapMode),
		"bootstrap_requestor_name":   c.BootstrapRequestorName,
		"bootstrap_requestor_type":   c.BootstrapRequestorType,
		"bootstrap_requestor_org":    c.BootstrapRequestorOrganization,
		"bootstrap_requestor_key_id": c.BootstrapRequestorKeyID,
		"bootstrap_public_key_path":  redact(c.BootstrapRequestorPublicKeyPath),
		"postgres_dsn":               redact(c.PostgresDSN),
		"opensearch_url":             redact(c.OpenSearchURL),
		"blob_backend":               c.BlobBackend,
		"blob_storage_url":           redact(c.BlobStorageURL),
		"blob_s3_endpoint":           redact(c.BlobS3Endpoint),
		"blob_s3_region":             c.BlobS3Region,
		"blob_s3_force_path_style":   strconv.FormatBool(c.BlobS3ForcePathStyle),
		"blob_s3_disable_tls":        strconv.FormatBool(c.BlobS3DisableTLS),
		"blob_s3_access_key_id":      redact(c.BlobS3AccessKeyID),
		"blob_s3_secret_access_key":  redact(c.BlobS3SecretKey),
		"blob_s3_session_token":      redact(c.BlobS3SessionToken),
		"blob_s3_request_timeout":    c.BlobS3RequestTimeout.String(),
		"blob_s3_max_retries":        strconv.Itoa(c.BlobS3MaxRetries),
	}
}

func envString(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}

	return duration, nil
}

func envBool(key string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}

	return parsed, nil
}

func envInt64(key string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}

	return parsed, nil
}

func envInt(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}

	return parsed, nil
}

func redact(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "set"
	}
	return value[:4] + "..."
}
