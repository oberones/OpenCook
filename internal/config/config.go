package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ServiceName     string
	Environment     string
	ListenAddress   string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	AuthSkew        time.Duration
	BootstrapMode   bool

	PostgresDSN    string
	OpenSearchURL  string
	BlobStorageURL string
}

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

	bootstrapMode, err := envBool("OPENCOOK_BOOTSTRAP_MODE", true)
	if err != nil {
		return Config{}, err
	}

	return Config{
		ServiceName:     envString("OPENCOOK_SERVICE_NAME", "opencook"),
		Environment:     envString("OPENCOOK_ENV", "development"),
		ListenAddress:   envString("OPENCOOK_LISTEN_ADDRESS", ":4000"),
		ReadTimeout:     readTimeout,
		WriteTimeout:    writeTimeout,
		ShutdownTimeout: shutdownTimeout,
		AuthSkew:        authSkew,
		BootstrapMode:   bootstrapMode,
		PostgresDSN:     strings.TrimSpace(os.Getenv("OPENCOOK_POSTGRES_DSN")),
		OpenSearchURL:   strings.TrimSpace(os.Getenv("OPENCOOK_OPENSEARCH_URL")),
		BlobStorageURL:  strings.TrimSpace(os.Getenv("OPENCOOK_BLOB_STORAGE_URL")),
	}, nil
}

func (c Config) Redacted() map[string]string {
	return map[string]string{
		"service_name":     c.ServiceName,
		"environment":      c.Environment,
		"listen_address":   c.ListenAddress,
		"read_timeout":     c.ReadTimeout.String(),
		"write_timeout":    c.WriteTimeout.String(),
		"shutdown_timeout": c.ShutdownTimeout.String(),
		"auth_skew":        c.AuthSkew.String(),
		"bootstrap_mode":   strconv.FormatBool(c.BootstrapMode),
		"postgres_dsn":     redact(c.PostgresDSN),
		"opensearch_url":   redact(c.OpenSearchURL),
		"blob_storage_url": redact(c.BlobStorageURL),
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

func redact(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "set"
	}
	return value[:4] + "..."
}
