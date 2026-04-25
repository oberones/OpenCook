package admin

import (
	"flag"
	"os"
	"strings"
)

const defaultServerAPIVersion = "1"

type Config struct {
	ServerURL        string
	RequestorName    string
	RequestorType    string
	PrivateKeyPath   string
	DefaultOrg       string
	ServerAPIVersion string
}

func LoadConfigFromEnv() Config {
	return configFromLookup(os.Getenv)
}

func configFromLookup(lookup func(string) string) Config {
	if lookup == nil {
		lookup = func(string) string { return "" }
	}

	requestorType := envString(lookup, "OPENCOOK_ADMIN_REQUESTOR_TYPE", "user")
	serverAPIVersion := envString(lookup, "OPENCOOK_ADMIN_SERVER_API_VERSION", defaultServerAPIVersion)
	return Config{
		ServerURL:        envString(lookup, "OPENCOOK_ADMIN_SERVER_URL", "http://127.0.0.1:4000"),
		RequestorName:    envString(lookup, "OPENCOOK_ADMIN_REQUESTOR_NAME", ""),
		RequestorType:    requestorType,
		PrivateKeyPath:   envString(lookup, "OPENCOOK_ADMIN_PRIVATE_KEY_PATH", ""),
		DefaultOrg:       envString(lookup, "OPENCOOK_ADMIN_DEFAULT_ORG", envString(lookup, "OPENCOOK_DEFAULT_ORGANIZATION", "")),
		ServerAPIVersion: serverAPIVersion,
	}
}

func (c *Config) BindFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ServerURL, "server-url", c.ServerURL, "OpenCook server URL")
	fs.StringVar(&c.RequestorName, "requestor-name", c.RequestorName, "Chef requestor name used for signed admin requests")
	fs.StringVar(&c.RequestorType, "requestor-type", c.RequestorType, "Chef requestor type, usually user or client")
	fs.StringVar(&c.PrivateKeyPath, "private-key", c.PrivateKeyPath, "path to the requestor private key PEM")
	fs.StringVar(&c.DefaultOrg, "default-org", c.DefaultOrg, "default organization for org-scoped admin commands")
	fs.StringVar(&c.ServerAPIVersion, "server-api-version", c.ServerAPIVersion, "X-Ops-Server-API-Version value for signed requests")
}

func envString(lookup func(string) string, key, fallback string) string {
	value := strings.TrimSpace(lookup(key))
	if value == "" {
		return fallback
	}
	return value
}
