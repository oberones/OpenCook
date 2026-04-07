package blob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

type S3CompatibleConfig struct {
	StorageURL     string
	Endpoint       string
	Region         string
	ForcePathStyle bool
	DisableTLS     bool
	AccessKeyID    string
	SecretKey      string
	SessionToken   string
	HTTPClient     *http.Client
	Now            func() time.Time
}

type S3CompatibleStore struct {
	bucket         string
	prefix         string
	endpoint       string
	region         string
	forcePathStyle bool
	disableTLS     bool
	accessKeyID    string
	secretKey      string
	sessionToken   string
	client         *http.Client
	now            func() time.Time
}

func NewS3CompatibleStore(cfg S3CompatibleConfig) (*S3CompatibleStore, error) {
	store := &S3CompatibleStore{
		endpoint:       strings.TrimSpace(cfg.Endpoint),
		region:         strings.TrimSpace(cfg.Region),
		forcePathStyle: cfg.ForcePathStyle,
		disableTLS:     cfg.DisableTLS,
		accessKeyID:    strings.TrimSpace(cfg.AccessKeyID),
		secretKey:      strings.TrimSpace(cfg.SecretKey),
		sessionToken:   strings.TrimSpace(cfg.SessionToken),
		client:         cfg.HTTPClient,
		now:            cfg.Now,
	}
	if store.region == "" {
		store.region = "us-east-1"
	}
	if store.client == nil {
		store.client = http.DefaultClient
	}
	if store.now == nil {
		store.now = time.Now
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
	if store.bucket == "" {
		return nil, fmt.Errorf("s3 blob backend requires a bucket in OPENCOOK_BLOB_STORAGE_URL")
	}
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
	if !s.isReady() {
		return Status{
			Backend:    "s3-compatible",
			Configured: false,
			Message:    "set OPENCOOK_BLOB_S3_ACCESS_KEY_ID and OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY to enable S3-compatible blob request operations",
		}
	}

	message := "S3-compatible blob adapter is configured for request operations"
	if s.endpoint != "" {
		message = message + " (endpoint " + s.endpoint + ")"
	}

	return Status{
		Backend:    "s3-compatible",
		Configured: true,
		Message:    message,
	}
}

func (s *S3CompatibleStore) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := s.newRequest(ctx, http.MethodGet, key, "", nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return io.ReadAll(resp.Body)
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, s.unexpectedStatus("get", resp.StatusCode)
	}
}

func (s *S3CompatibleStore) Put(ctx context.Context, req PutRequest) (PutResult, error) {
	httpReq, err := s.newRequest(ctx, http.MethodPut, req.Key, req.ContentType, req.Body)
	if err != nil {
		return PutResult{}, err
	}

	resp, err := s.httpClient().Do(httpReq)
	if err != nil {
		return PutResult{}, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
		return PutResult{Location: strings.TrimSpace(req.Key)}, nil
	default:
		return PutResult{}, s.unexpectedStatus("put", resp.StatusCode)
	}
}

func (s *S3CompatibleStore) Exists(ctx context.Context, key string) (bool, error) {
	req, err := s.newRequest(ctx, http.MethodHead, key, "", nil)
	if err != nil {
		return false, err
	}

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	default:
		return false, s.unexpectedStatus("head", resp.StatusCode)
	}
}

func (s *S3CompatibleStore) Delete(ctx context.Context, key string) error {
	req, err := s.newRequest(ctx, http.MethodDelete, key, "", nil)
	if err != nil {
		return err
	}

	resp, err := s.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusNoContent:
		return nil
	case http.StatusNotFound:
		return ErrNotFound
	default:
		return s.unexpectedStatus("delete", resp.StatusCode)
	}
}

func (s *S3CompatibleStore) newRequest(ctx context.Context, method, key, contentType string, body []byte) (*http.Request, error) {
	objectKey, err := normalizeObjectKey(key)
	if err != nil {
		return nil, err
	}
	if !s.isReady() {
		return nil, ErrUnavailable
	}

	targetURL, host, err := s.objectURL(objectKey)
	if err != nil {
		return nil, err
	}

	payloadHash := hexSHA256(body)
	reader := bytes.NewReader(body)
	if body == nil {
		reader = bytes.NewReader(nil)
	}
	req, err := http.NewRequestWithContext(ctx, method, targetURL, reader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", strings.TrimSpace(contentType))
	}
	req.Host = host
	s.signRequest(req, host, payloadHash)
	return req, nil
}

func (s *S3CompatibleStore) objectURL(key string) (string, string, error) {
	base, err := s.baseURL()
	if err != nil {
		return "", "", err
	}

	objectPath := joinPathSegments(base.Path, s.prefix, key)
	host := base.Host
	if s.usePathStyle() {
		base.Path = joinPathSegments(base.Path, s.bucket, s.prefix, key)
	} else {
		base.Path = objectPath
		base.Host = s.bucket + "." + base.Host
		host = base.Host
	}
	return base.String(), host, nil
}

func (s *S3CompatibleStore) baseURL() (*url.URL, error) {
	if strings.TrimSpace(s.endpoint) != "" {
		parsed, err := url.Parse(s.endpoint)
		if err != nil {
			return nil, fmt.Errorf("parse s3 endpoint: %w", err)
		}
		return parsed, nil
	}

	scheme := "https"
	if s.disableTLS {
		scheme = "http"
	}
	return &url.URL{
		Scheme: scheme,
		Host:   "s3." + s.region + ".amazonaws.com",
	}, nil
}

func (s *S3CompatibleStore) signRequest(req *http.Request, host, payloadHash string) {
	now := s.clock()().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if s.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.sessionToken)
	}

	canonicalHeaders, signedHeaders := canonicalAWSHeaders(host, req.Header)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL),
		canonicalQueryString(req.URL),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := strings.Join([]string{dateStamp, s.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		hexSHA256([]byte(canonicalRequest)),
	}, "\n")
	signature := hex.EncodeToString(signingKey(s.secretKey, dateStamp, s.region, "s3", stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		s.accessKeyID,
		credentialScope,
		signedHeaders,
		signature,
	))
}

func canonicalAWSHeaders(host string, headers http.Header) (string, string) {
	type header struct {
		name  string
		value string
	}

	canonical := []header{{
		name:  "host",
		value: host,
	}}
	for name, values := range headers {
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower != "x-amz-content-sha256" && lower != "x-amz-date" && lower != "x-amz-security-token" {
			continue
		}
		canonical = append(canonical, header{
			name:  lower,
			value: strings.Join(values, ","),
		})
	}
	sort.Slice(canonical, func(i, j int) bool {
		return canonical[i].name < canonical[j].name
	})

	var headerLines []string
	var signed []string
	for _, entry := range canonical {
		headerLines = append(headerLines, entry.name+":"+strings.TrimSpace(entry.value))
		signed = append(signed, entry.name)
	}
	return strings.Join(headerLines, "\n") + "\n", strings.Join(signed, ";")
}

func canonicalURI(u *url.URL) string {
	if u == nil {
		return "/"
	}
	path := u.EscapedPath()
	if path == "" {
		return "/"
	}
	return path
}

func canonicalQueryString(u *url.URL) string {
	if u == nil || u.RawQuery == "" {
		return ""
	}
	values := u.Query()
	var parts []string
	for key, valuesForKey := range values {
		escapedKey := url.QueryEscape(key)
		sort.Strings(valuesForKey)
		for _, value := range valuesForKey {
			parts = append(parts, escapedKey+"="+url.QueryEscape(value))
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "&")
}

func signingKey(secret, dateStamp, region, service, stringToSign string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, service)
	requestKey := hmacSHA256(serviceKey, "aws4_request")
	return hmacSHA256(requestKey, stringToSign)
}

func hmacSHA256(key []byte, value string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = io.WriteString(mac, value)
	return mac.Sum(nil)
}

func hexSHA256(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func joinPathSegments(base string, segments ...string) string {
	parts := make([]string, 0, len(segments)+1)
	if strings.TrimSpace(base) != "" && base != "/" {
		parts = append(parts, strings.Trim(base, "/"))
	}
	for _, segment := range segments {
		segment = strings.Trim(segment, "/")
		if segment != "" {
			parts = append(parts, segment)
		}
	}
	return "/" + path.Join(parts...)
}

func (s *S3CompatibleStore) isReady() bool {
	return strings.TrimSpace(s.bucket) != "" && strings.TrimSpace(s.accessKeyID) != "" && strings.TrimSpace(s.secretKey) != ""
}

func (s *S3CompatibleStore) httpClient() *http.Client {
	if s.client != nil {
		return s.client
	}
	return http.DefaultClient
}

func (s *S3CompatibleStore) clock() func() time.Time {
	if s.now != nil {
		return s.now
	}
	return time.Now
}

func (s *S3CompatibleStore) usePathStyle() bool {
	return s.forcePathStyle || strings.TrimSpace(s.endpoint) != ""
}

func (s *S3CompatibleStore) unexpectedStatus(op string, status int) error {
	return fmt.Errorf("s3-compatible blob %s failed with status %d", op, status)
}
