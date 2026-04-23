package blob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultS3CompatibleRequestTimeout = 30 * time.Second
const defaultS3CompatibleRetryBaseDelay = 100 * time.Millisecond
const maxS3CompatibleRetryDelay = 2 * time.Second

type S3CompatibleConfig struct {
	StorageURL     string
	Endpoint       string
	Region         string
	ForcePathStyle bool
	DisableTLS     bool
	AccessKeyID    string
	SecretKey      string
	SessionToken   string
	RequestTimeout time.Duration
	MaxRetries     int
	HTTPClient     *http.Client
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration) error
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
	requestTimeout time.Duration
	maxRetries     int
	client         *http.Client
	now            func() time.Time
	sleep          func(context.Context, time.Duration) error
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
		requestTimeout: cfg.RequestTimeout,
		maxRetries:     cfg.MaxRetries,
		client:         cfg.HTTPClient,
		now:            cfg.Now,
		sleep:          cfg.Sleep,
	}
	if store.region == "" {
		store.region = "us-east-1"
	}
	if err := validateS3Endpoint(store.endpoint); err != nil {
		return nil, err
	}
	if store.requestTimeout <= 0 {
		store.requestTimeout = defaultS3CompatibleRequestTimeout
	}
	if store.maxRetries < 0 {
		return nil, fmt.Errorf("s3-compatible blob backend requires a non-negative retry count")
	}
	if store.client == nil {
		store.client = &http.Client{Timeout: store.requestTimeout}
	} else if store.requestTimeout > 0 && store.client.Timeout == 0 {
		client := *store.client
		client.Timeout = store.requestTimeout
		store.client = &client
	}
	if store.now == nil {
		store.now = time.Now
	}
	if store.sleep == nil {
		store.sleep = sleepWithContext
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
		missing := s.missingCredentialEnvVars()
		message := "set " + strings.Join(missing, " and ") + " to enable S3-compatible blob request operations"
		return Status{
			Backend:    "s3-compatible",
			Configured: false,
			Message:    message,
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
	resp, err := s.do(ctx, "get", http.MethodGet, key, "", nil)
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
	resp, err := s.do(ctx, "put", http.MethodPut, req.Key, req.ContentType, req.Body)
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
	resp, err := s.do(ctx, "head", http.MethodHead, key, "", nil)
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
	resp, err := s.do(ctx, "delete", http.MethodDelete, key, "", nil)
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

func (s *S3CompatibleStore) do(ctx context.Context, op, method, key, contentType string, body []byte) (*http.Response, error) {
	attempts := s.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		req, err := s.newRequest(ctx, method, key, contentType, body)
		if err != nil {
			return nil, err
		}

		resp, err := s.httpClient().Do(req)
		if err != nil {
			if ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
				return nil, ctx.Err()
			}
			if s.isRetryableTransportError(err) && attempt < attempts-1 {
				if err := s.waitBeforeRetry(ctx, attempt, ""); err != nil {
					return nil, err
				}
				continue
			}
			return nil, s.unavailableTransportError(op, err)
		}

		if s.isRetryableStatus(resp.StatusCode) {
			retryAfter := resp.Header.Get("Retry-After")
			discardAndCloseResponse(resp)
			if attempt < attempts-1 {
				if err := s.waitBeforeRetry(ctx, attempt, retryAfter); err != nil {
					return nil, err
				}
				continue
			}
			return nil, s.unavailableStatusError(op, resp.StatusCode)
		}

		if s.isAvailabilityStatus(resp.StatusCode) {
			discardAndCloseResponse(resp)
			return nil, s.unavailableStatusError(op, resp.StatusCode)
		}

		return resp, nil
	}

	return nil, s.unavailableTransportError(op, ErrUnavailable)
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
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
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
	signingKey := deriveSigningKey(s.secretKey, dateStamp, s.region, "s3")
	signature := hex.EncodeToString(signStringToSign(signingKey, stringToSign))
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

	type queryPart struct {
		key   string
		value string
	}

	rawParts := strings.Split(u.RawQuery, "&")
	parts := make([]queryPart, 0, len(rawParts))
	for _, rawPart := range rawParts {
		rawKey := rawPart
		rawValue := ""
		if idx := strings.Index(rawPart, "="); idx >= 0 {
			rawKey = rawPart[:idx]
			rawValue = rawPart[idx+1:]
		}

		parts = append(parts, queryPart{
			key:   awsPercentEncode(decodeRawQueryComponent(rawKey)),
			value: awsPercentEncode(decodeRawQueryComponent(rawValue)),
		})
	}
	sort.Slice(parts, func(i, j int) bool {
		if parts[i].key == parts[j].key {
			return parts[i].value < parts[j].value
		}
		return parts[i].key < parts[j].key
	})

	canonical := make([]string, 0, len(parts))
	for _, part := range parts {
		canonical = append(canonical, part.key+"="+part.value)
	}
	return strings.Join(canonical, "&")
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secret), dateStamp)
	regionKey := hmacSHA256(dateKey, region)
	serviceKey := hmacSHA256(regionKey, service)
	return hmacSHA256(serviceKey, "aws4_request")
}

func signStringToSign(signingKey []byte, stringToSign string) []byte {
	return hmacSHA256(signingKey, stringToSign)
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

func (s *S3CompatibleStore) missingCredentialEnvVars() []string {
	var missing []string
	if strings.TrimSpace(s.accessKeyID) == "" {
		missing = append(missing, "OPENCOOK_BLOB_S3_ACCESS_KEY_ID")
	}
	if strings.TrimSpace(s.secretKey) == "" {
		missing = append(missing, "OPENCOOK_BLOB_S3_SECRET_ACCESS_KEY")
	}
	return missing
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

func (s *S3CompatibleStore) waitBeforeRetry(ctx context.Context, attempt int, retryAfter string) error {
	delay := s.retryDelay(attempt, retryAfter)
	if delay <= 0 {
		return nil
	}
	if err := s.sleep(ctx, delay); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return ctx.Err()
		}
		return err
	}
	return nil
}

func (s *S3CompatibleStore) retryDelay(attempt int, retryAfter string) time.Duration {
	if delay, ok := s.parseRetryAfter(retryAfter); ok {
		return delay
	}

	delay := defaultS3CompatibleRetryBaseDelay * time.Duration(1<<attempt)
	if delay <= 0 || delay > maxS3CompatibleRetryDelay {
		return maxS3CompatibleRetryDelay
	}
	return delay
}

func (s *S3CompatibleStore) parseRetryAfter(value string) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}

	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}

	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(s.clock()().UTC())
	if delay < 0 {
		return 0, true
	}
	return delay, true
}

func (s *S3CompatibleStore) unexpectedStatus(op string, status int) error {
	return fmt.Errorf("s3-compatible blob %s failed with status %d", op, status)
}

func (s *S3CompatibleStore) unavailableStatusError(op string, status int) error {
	return fmt.Errorf("%w: s3-compatible blob %s failed with status %d", ErrUnavailable, op, status)
}

func (s *S3CompatibleStore) unavailableTransportError(op string, err error) error {
	return fmt.Errorf("%w: s3-compatible blob %s failed: %v", ErrUnavailable, op, err)
}

func (s *S3CompatibleStore) isAvailabilityStatus(status int) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return true
	default:
		return s.isRetryableStatus(status)
	}
}

func (s *S3CompatibleStore) isRetryableStatus(status int) bool {
	switch status {
	case http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func (s *S3CompatibleStore) isRetryableTransportError(err error) bool {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}

	return false
}

func discardAndCloseResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateS3Endpoint(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse s3 endpoint: %w", err)
	}
	if strings.TrimSpace(parsed.Scheme) == "" || strings.TrimSpace(parsed.Host) == "" {
		return fmt.Errorf("parse s3 endpoint: endpoint must include scheme and host")
	}
	return nil
}

func awsPercentEncode(value string) string {
	var builder strings.Builder
	for _, b := range []byte(value) {
		if isAWSUnreserved(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(fmt.Sprintf("%%%02X", b))
	}
	return builder.String()
}

func decodeRawQueryComponent(value string) string {
	decoded := make([]byte, 0, len(value))
	for i := 0; i < len(value); i++ {
		if value[i] == '%' && i+2 < len(value) && isHexDigit(value[i+1]) && isHexDigit(value[i+2]) {
			decoded = append(decoded, fromHex(value[i+1])<<4|fromHex(value[i+2]))
			i += 2
			continue
		}
		decoded = append(decoded, value[i])
	}
	return string(decoded)
}

func isAWSUnreserved(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '-', b == '.', b == '_', b == '~':
		return true
	default:
		return false
	}
}

func isHexDigit(b byte) bool {
	switch {
	case b >= '0' && b <= '9':
		return true
	case b >= 'a' && b <= 'f':
		return true
	case b >= 'A' && b <= 'F':
		return true
	default:
		return false
	}
}

func fromHex(b byte) byte {
	switch {
	case b >= '0' && b <= '9':
		return b - '0'
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10
	default:
		return b - 'A' + 10
	}
}
