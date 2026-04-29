package admin

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/oberones/OpenCook/internal/authn"
)

type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

type Client struct {
	baseURL *url.URL
	cfg     Config
	key     *rsa.PrivateKey
	doer    HTTPDoer
	now     func() time.Time
}

// RawResponse carries the minimal response data admin tooling needs when it
// follows non-JSON signed blob URLs during migration validation.
type RawResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

type Option func(*Client)

func WithHTTPDoer(doer HTTPDoer) Option {
	return func(c *Client) {
		c.doer = doer
	}
}

func WithNow(now func() time.Time) Option {
	return func(c *Client) {
		c.now = now
	}
}

func WithPrivateKey(key *rsa.PrivateKey) Option {
	return func(c *Client) {
		c.key = key
	}
}

func NewClient(cfg Config, opts ...Option) (*Client, error) {
	client := &Client{
		cfg:  normalizeConfig(cfg),
		doer: http.DefaultClient,
		now:  time.Now,
	}
	for _, opt := range opts {
		opt(client)
	}
	if client.doer == nil {
		client.doer = http.DefaultClient
	}
	if client.now == nil {
		client.now = time.Now
	}

	baseURL, err := parseServerURL(client.cfg.ServerURL)
	if err != nil {
		return nil, err
	}
	client.baseURL = baseURL

	if strings.TrimSpace(client.cfg.RequestorName) == "" {
		return nil, errorf(CodeInvalidConfiguration, "requestor name is required")
	}
	if strings.TrimSpace(client.cfg.RequestorType) == "" {
		return nil, errorf(CodeInvalidConfiguration, "requestor type is required")
	}
	if client.key == nil {
		key, err := readPrivateKey(client.cfg.PrivateKeyPath)
		if err != nil {
			return nil, err
		}
		client.key = key
	}

	return client, nil
}

func (c *Client) DoJSON(ctx context.Context, method, path string, in, out any) error {
	body, err := encodeRequestBody(in)
	if err != nil {
		return err
	}

	req, err := c.NewJSONRequest(ctx, method, path, body)
	if err != nil {
		return err
	}
	resp, err := c.doer.Do(req)
	if err != nil {
		return errorf(CodeRequestFailed, "%s %s request failed", strings.ToUpper(method), safePath(path))
	}
	defer resp.Body.Close()

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return errorf(CodeRequestFailed, "%s %s response read failed", strings.ToUpper(method), safePath(path))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errorf(CodeRequestFailed, "%s %s returned HTTP %d", strings.ToUpper(method), safePath(path), resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	if len(bytes.TrimSpace(payload)) == 0 {
		return errorf(CodeDecodeFailed, "%s %s returned empty JSON response", strings.ToUpper(method), safePath(path))
	}
	if err := json.Unmarshal(payload, out); err != nil {
		return errorf(CodeDecodeFailed, "%s %s returned invalid JSON", strings.ToUpper(method), safePath(path))
	}
	return nil
}

// DoUnsigned performs an unsigned request for server-generated signed URLs,
// such as cookbook and sandbox blob download links returned by Chef APIs.
func (c *Client) DoUnsigned(ctx context.Context, method, rawURL string) (RawResponse, error) {
	if c == nil {
		return RawResponse{}, errorf(CodeInvalidConfiguration, "admin client is not configured")
	}
	target, err := c.resolveUnsigned(rawURL)
	if err != nil {
		return RawResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), target.String(), nil)
	if err != nil {
		return RawResponse{}, errorf(CodeInvalidConfiguration, "request URL is invalid")
	}
	req.Header.Set("Accept", "*/*")
	resp, err := c.doer.Do(req)
	if err != nil {
		return RawResponse{}, errorf(CodeRequestFailed, "%s %s request failed", strings.ToUpper(method), safePath(rawURL))
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		return RawResponse{}, errorf(CodeRequestFailed, "%s %s response read failed", strings.ToUpper(method), safePath(rawURL))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return RawResponse{}, errorf(CodeRequestFailed, "%s %s returned HTTP %d", strings.ToUpper(method), safePath(rawURL), resp.StatusCode)
	}
	return RawResponse{StatusCode: resp.StatusCode, Header: resp.Header.Clone(), Body: payload}, nil
}

func (c *Client) NewJSONRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	if c == nil {
		return nil, errorf(CodeInvalidConfiguration, "admin client is not configured")
	}
	if body == nil {
		body = []byte{}
	}
	target, err := c.resolve(path)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, strings.ToUpper(method), target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errorf(CodeInvalidConfiguration, "request path is invalid")
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	err = signer{
		requestorName:    c.cfg.RequestorName,
		serverAPIVersion: c.cfg.ServerAPIVersion,
		privateKey:       c.key,
		now:              c.now,
	}.sign(req, body)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (c *Client) resolve(path string) (*url.URL, error) {
	if c.baseURL == nil {
		return nil, errorf(CodeInvalidConfiguration, "server URL is not configured")
	}
	raw := strings.TrimSpace(path)
	if raw == "" {
		raw = "/"
	}
	ref, err := url.Parse(raw)
	if err != nil {
		return nil, errorf(CodeInvalidConfiguration, "request path is invalid")
	}
	if ref.IsAbs() {
		return nil, errorf(CodeInvalidConfiguration, "request path must be relative")
	}
	if !strings.HasPrefix(raw, "/") {
		ref.Path = "/" + strings.TrimPrefix(ref.Path, "/")
	}
	return c.baseURL.ResolveReference(ref), nil
}

// resolveUnsigned accepts absolute URLs because signed blob links are returned
// as complete URLs, while still supporting relative paths in tests and tooling.
func (c *Client) resolveUnsigned(rawURL string) (*url.URL, error) {
	raw := strings.TrimSpace(rawURL)
	if raw == "" {
		return nil, errorf(CodeInvalidConfiguration, "request URL is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, errorf(CodeInvalidConfiguration, "request URL is invalid")
	}
	if parsed.IsAbs() {
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, errorf(CodeInvalidConfiguration, "request URL must use http or https")
		}
		return parsed, nil
	}
	return c.resolve(raw)
}

func normalizeConfig(cfg Config) Config {
	cfg.ServerURL = strings.TrimSpace(cfg.ServerURL)
	cfg.RequestorName = strings.TrimSpace(cfg.RequestorName)
	cfg.RequestorType = strings.TrimSpace(cfg.RequestorType)
	cfg.PrivateKeyPath = strings.TrimSpace(cfg.PrivateKeyPath)
	cfg.DefaultOrg = strings.TrimSpace(cfg.DefaultOrg)
	cfg.ServerAPIVersion = strings.TrimSpace(cfg.ServerAPIVersion)
	if cfg.ServerURL == "" {
		cfg.ServerURL = "http://127.0.0.1:4000"
	}
	if cfg.RequestorType == "" {
		cfg.RequestorType = "user"
	}
	if cfg.ServerAPIVersion == "" {
		cfg.ServerAPIVersion = defaultServerAPIVersion
	}
	return cfg
}

func parseServerURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, errorf(CodeInvalidConfiguration, "server URL is invalid")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, errorf(CodeInvalidConfiguration, "server URL must use http or https")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errorf(CodeInvalidConfiguration, "server URL must not include query or fragment")
	}
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	return parsed, nil
}

func readPrivateKey(path string) (*rsa.PrivateKey, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errorf(CodeInvalidConfiguration, "private key path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, errorf(CodeInvalidConfiguration, "private key could not be read")
	}
	key, err := authn.ParseRSAPrivateKeyPEM(data)
	if err != nil {
		return nil, errorf(CodeInvalidConfiguration, "private key could not be parsed")
	}
	return key, nil
}

func encodeRequestBody(in any) ([]byte, error) {
	if in == nil {
		return nil, nil
	}
	switch value := in.(type) {
	case []byte:
		return value, nil
	case json.RawMessage:
		return []byte(value), nil
	case string:
		return []byte(value), nil
	default:
		body, err := json.Marshal(value)
		if err != nil {
			return nil, errorf(CodeInvalidConfiguration, "request JSON could not be encoded")
		}
		return body, nil
	}
}

func safePath(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return "/"
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<invalid>"
	}
	if parsed.Path == "" {
		return "/"
	}
	return parsed.Path
}

func (c Config) String() string {
	return fmt.Sprintf("server_url=%s requestor_name=%s requestor_type=%s default_org=%s server_api_version=%s",
		redactedSet(c.ServerURL),
		c.RequestorName,
		c.RequestorType,
		c.DefaultOrg,
		c.ServerAPIVersion,
	)
}

func redactedSet(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "set"
}
