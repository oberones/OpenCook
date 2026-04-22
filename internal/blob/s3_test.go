package blob

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestS3CompatibleStorePutGetExistsAndDelete(t *testing.T) {
	var (
		mu      sync.Mutex
		objects = map[string][]byte{}
	)

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if want := "/chef-bucket/checksums/abcdef0123456789"; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
			t.Errorf("Authorization = %q, want AWS4-HMAC-SHA256 prefix", auth)
		}
		if r.Header.Get("X-Amz-Date") == "" {
			t.Error("X-Amz-Date header missing")
		}
		if r.Header.Get("X-Amz-Content-Sha256") == "" {
			t.Error("X-Amz-Content-Sha256 header missing")
		}

		mu.Lock()
		defer mu.Unlock()

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll() error = %v", err)
				return testHTTPResponse(r, http.StatusInternalServerError, ""), nil
			}
			objects[r.URL.Path] = body
			return testHTTPResponse(r, http.StatusOK, ""), nil
		case http.MethodHead:
			if r.Body != nil {
				t.Errorf("HEAD body = non-nil, want nil")
			}
			if _, ok := objects[r.URL.Path]; !ok {
				return testHTTPResponse(r, http.StatusNotFound, ""), nil
			}
			return testHTTPResponse(r, http.StatusOK, ""), nil
		case http.MethodGet:
			if r.Body != nil {
				t.Errorf("GET body = non-nil, want nil")
			}
			body, ok := objects[r.URL.Path]
			if !ok {
				return testHTTPResponse(r, http.StatusNotFound, ""), nil
			}
			return testHTTPResponse(r, http.StatusOK, string(body)), nil
		case http.MethodDelete:
			if r.Body != nil {
				t.Errorf("DELETE body = non-nil, want nil")
			}
			if _, ok := objects[r.URL.Path]; !ok {
				return testHTTPResponse(r, http.StatusNotFound, ""), nil
			}
			delete(objects, r.URL.Path)
			return testHTTPResponse(r, http.StatusNoContent, ""), nil
		default:
			return testHTTPResponse(r, http.StatusMethodNotAllowed, ""), nil
		}
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		SessionToken:   "session-token",
		HTTPClient:     client,
		Now: func() time.Time {
			return time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	if _, err := store.Put(context.Background(), PutRequest{
		Key:         "abcdef0123456789",
		ContentType: "application/x-binary",
		Body:        []byte("rainbow"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	exists, err := store.Exists(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if !exists {
		t.Fatal("Exists() = false, want true")
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}

	if err := store.Delete(context.Background(), "abcdef0123456789"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	exists, err = store.Exists(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Exists() after delete error = %v", err)
	}
	if exists {
		t.Fatal("Exists() = true after delete, want false")
	}
}

func TestS3CompatibleStoreReturnsUnavailableWithoutCredentials(t *testing.T) {
	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL: "s3://chef-bucket/checksums",
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	status := store.Status()
	if status.Configured {
		t.Fatal("Status().Configured = true, want false")
	}

	_, err = store.Put(context.Background(), PutRequest{
		Key:  "abcdef0123456789",
		Body: []byte("rainbow"),
	})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Put() error = %v, want ErrUnavailable", err)
	}
}

func TestCanonicalQueryStringUsesSigV4Encoding(t *testing.T) {
	u := &url.URL{
		RawQuery: "marker=a+b&prefix=foo bar&empty=&encoded=%2F&a=1&a=0",
	}

	got := canonicalQueryString(u)
	want := "a=0&a=1&empty=&encoded=%2F&marker=a%2Bb&prefix=foo%20bar"
	if got != want {
		t.Fatalf("canonicalQueryString() = %q, want %q", got, want)
	}
}

func TestNewRequestUsesNilBodyForReadMethods(t *testing.T) {
	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			req, err := store.newRequest(context.Background(), method, "abcdef0123456789", "", nil)
			if err != nil {
				t.Fatalf("newRequest() error = %v", err)
			}
			if req.Body != nil {
				t.Fatalf("req.Body = non-nil, want nil")
			}
		})
	}
}

func TestS3CompatibleStoreNewRequestUsesExpectedTargetURL(t *testing.T) {
	for _, tc := range []struct {
		name     string
		cfg      S3CompatibleConfig
		wantURL  string
		wantHost string
		wantPath string
	}{
		{
			name: "custom endpoint uses path style",
			cfg: S3CompatibleConfig{
				StorageURL:  "s3://chef-bucket/checksums",
				Endpoint:    "http://minio.test/storage-root",
				Region:      "us-east-1",
				AccessKeyID: "access-key",
				SecretKey:   "secret-key",
			},
			wantURL:  "http://minio.test/storage-root/chef-bucket/checksums/abcdef0123456789",
			wantHost: "minio.test",
			wantPath: "/storage-root/chef-bucket/checksums/abcdef0123456789",
		},
		{
			name: "default aws uses virtual hosted https",
			cfg: S3CompatibleConfig{
				StorageURL:  "s3://chef-bucket/checksums",
				Region:      "us-west-2",
				AccessKeyID: "access-key",
				SecretKey:   "secret-key",
			},
			wantURL:  "https://chef-bucket.s3.us-west-2.amazonaws.com/checksums/abcdef0123456789",
			wantHost: "chef-bucket.s3.us-west-2.amazonaws.com",
			wantPath: "/checksums/abcdef0123456789",
		},
		{
			name: "tls disabled uses http",
			cfg: S3CompatibleConfig{
				StorageURL:  "s3://chef-bucket/checksums",
				Region:      "us-west-2",
				DisableTLS:  true,
				AccessKeyID: "access-key",
				SecretKey:   "secret-key",
			},
			wantURL:  "http://chef-bucket.s3.us-west-2.amazonaws.com/checksums/abcdef0123456789",
			wantHost: "chef-bucket.s3.us-west-2.amazonaws.com",
			wantPath: "/checksums/abcdef0123456789",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewS3CompatibleStore(tc.cfg)
			if err != nil {
				t.Fatalf("NewS3CompatibleStore() error = %v", err)
			}

			req, err := store.newRequest(context.Background(), http.MethodGet, "abcdef0123456789", "", nil)
			if err != nil {
				t.Fatalf("newRequest() error = %v", err)
			}

			if got := req.URL.String(); got != tc.wantURL {
				t.Fatalf("req.URL.String() = %q, want %q", got, tc.wantURL)
			}
			if got := req.URL.Path; got != tc.wantPath {
				t.Fatalf("req.URL.Path = %q, want %q", got, tc.wantPath)
			}
			if got := req.Host; got != tc.wantHost {
				t.Fatalf("req.Host = %q, want %q", got, tc.wantHost)
			}
			if auth := req.Header.Get("Authorization"); !strings.HasPrefix(auth, "AWS4-HMAC-SHA256 ") {
				t.Fatalf("Authorization = %q, want AWS4-HMAC-SHA256 prefix", auth)
			}
		})
	}
}

func TestS3CompatibleStoreNewRequestIncludesSessionTokenInSignedHeaders(t *testing.T) {
	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:   "s3://chef-bucket/checksums",
		Region:       "us-east-1",
		AccessKeyID:  "access-key",
		SecretKey:    "secret-key",
		SessionToken: "session-token",
		Now: func() time.Time {
			return time.Date(2026, time.April, 22, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	req, err := store.newRequest(context.Background(), http.MethodGet, "abcdef0123456789", "", nil)
	if err != nil {
		t.Fatalf("newRequest() error = %v", err)
	}

	if got := req.Header.Get("X-Amz-Security-Token"); got != "session-token" {
		t.Fatalf("X-Amz-Security-Token = %q, want %q", got, "session-token")
	}
	if auth := req.Header.Get("Authorization"); !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token") {
		t.Fatalf("Authorization = %q, want signed headers to include x-amz-security-token", auth)
	}
}

func TestS3CompatibleStoreNewRequestNormalizesObjectKey(t *testing.T) {
	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:  "s3://chef-bucket/checksums",
		Endpoint:    "http://s3.test",
		Region:      "us-east-1",
		AccessKeyID: "access-key",
		SecretKey:   "secret-key",
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	req, err := store.newRequest(context.Background(), http.MethodGet, " abcdef0123456789 ", "", nil)
	if err != nil {
		t.Fatalf("newRequest() error = %v", err)
	}
	if got := req.URL.Path; got != "/chef-bucket/checksums/abcdef0123456789" {
		t.Fatalf("req.URL.Path = %q, want %q", got, "/chef-bucket/checksums/abcdef0123456789")
	}
}

func TestS3CompatibleStoreNewRequestRejectsInvalidObjectKey(t *testing.T) {
	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:  "s3://chef-bucket/checksums",
		Endpoint:    "http://s3.test",
		Region:      "us-east-1",
		AccessKeyID: "access-key",
		SecretKey:   "secret-key",
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	for _, key := range []string{"", "abc/def", `abc\def`, ".", ".."} {
		t.Run(key, func(t *testing.T) {
			_, err := store.newRequest(context.Background(), http.MethodGet, key, "", nil)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("newRequest() error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestS3CompatibleStoreRetriesRetryableStatusThenSucceeds(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts < 3 {
			return testHTTPResponse(r, http.StatusServiceUnavailable, ""), nil
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if string(body) != "rainbow" {
			t.Fatalf("body = %q, want %q", string(body), "rainbow")
		}
		return testHTTPResponse(r, http.StatusCreated, ""), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     2,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	if _, err := store.Put(context.Background(), PutRequest{
		Key:  "abcdef0123456789",
		Body: []byte("rainbow"),
	}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	if attempts != 3 {
		t.Fatalf("attempts = %d, want %d", attempts, 3)
	}
}

func TestS3CompatibleStoreRetriesTransportErrorThenSucceeds(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, temporaryNetError{message: "temporary network failure"}
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want %d", attempts, 2)
	}
}

func TestS3CompatibleStoreRetriesTimeoutTransportErrorThenSucceeds(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, timeoutNetError{message: "request timed out"}
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want %d", attempts, 2)
	}
}

func TestS3CompatibleStoreRetriesTemporaryDNSErrorThenSucceeds(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return nil, &net.DNSError{Err: "temporary dns failure", Name: "s3.test", IsTemporary: true}
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want %d", attempts, 2)
	}
}

func TestS3CompatibleStoreDoesNotRetryNonTransientTransportError(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		return nil, errors.New("permanent transport failure")
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     3,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), "abcdef0123456789")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, want ErrUnavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want %d", attempts, 1)
	}
}

func TestS3CompatibleStoreDoesNotRetryPermanentDNSError(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		return nil, &net.DNSError{Err: "no such host", Name: "s3.test"}
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     3,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), "abcdef0123456789")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, want ErrUnavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want %d", attempts, 1)
	}
}

func TestS3CompatibleStoreReturnsContextErrorWhenCanceled(t *testing.T) {
	attempts := 0
	started := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		started <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     3,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, getErr := store.Get(ctx, "abcdef0123456789")
		errCh <- getErr
	}()
	<-started
	cancel()

	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Get() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, do not want ErrUnavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want %d", attempts, 1)
	}
}

func TestS3CompatibleStoreReturnsContextDeadlineExceededWithoutRetry(t *testing.T) {
	attempts := 0
	started := make(chan struct{}, 1)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		started <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     3,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, getErr := store.Get(ctx, "abcdef0123456789")
		errCh <- getErr
	}()
	<-started

	err = <-errCh
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get() error = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, do not want ErrUnavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want %d", attempts, 1)
	}
}

func TestS3CompatibleStoreRetriesRetryableAvailabilityStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				return testHTTPResponse(r, status, ""), nil
			})}

			store, err := NewS3CompatibleStore(S3CompatibleConfig{
				StorageURL:     "s3://chef-bucket/checksums",
				Endpoint:       "http://s3.test",
				Region:         "us-east-1",
				ForcePathStyle: true,
				AccessKeyID:    "access-key",
				SecretKey:      "secret-key",
				MaxRetries:     2,
				HTTPClient:     client,
			})
			if err != nil {
				t.Fatalf("NewS3CompatibleStore() error = %v", err)
			}

			_, err = store.Get(context.Background(), "abcdef0123456789")
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Get() error = %v, want ErrUnavailable", err)
			}
			if attempts != 3 {
				t.Fatalf("attempts = %d, want %d", attempts, 3)
			}
		})
	}
}

func TestS3CompatibleStoreTreatsAvailabilityStatusesAsUnavailableWithoutRetry(t *testing.T) {
	for _, status := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				return testHTTPResponse(r, status, ""), nil
			})}

			store, err := NewS3CompatibleStore(S3CompatibleConfig{
				StorageURL:     "s3://chef-bucket/checksums",
				Endpoint:       "http://s3.test",
				Region:         "us-east-1",
				ForcePathStyle: true,
				AccessKeyID:    "access-key",
				SecretKey:      "secret-key",
				MaxRetries:     3,
				HTTPClient:     client,
			})
			if err != nil {
				t.Fatalf("NewS3CompatibleStore() error = %v", err)
			}

			_, err = store.Get(context.Background(), "abcdef0123456789")
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Get() error = %v, want ErrUnavailable", err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want %d", attempts, 1)
			}
		})
	}
}

func TestS3CompatibleStoreLeavesNonAvailabilityStatusesAsOperationFailures(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusConflict,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			attempts := 0
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				return testHTTPResponse(r, status, ""), nil
			})}

			store, err := NewS3CompatibleStore(S3CompatibleConfig{
				StorageURL:     "s3://chef-bucket/checksums",
				Endpoint:       "http://s3.test",
				Region:         "us-east-1",
				ForcePathStyle: true,
				AccessKeyID:    "access-key",
				SecretKey:      "secret-key",
				MaxRetries:     3,
				HTTPClient:     client,
			})
			if err != nil {
				t.Fatalf("NewS3CompatibleStore() error = %v", err)
			}

			_, err = store.Get(context.Background(), "abcdef0123456789")
			if err == nil {
				t.Fatal("Get() error = nil, want non-nil")
			}
			if errors.Is(err, ErrUnavailable) {
				t.Fatalf("Get() error = %v, do not want ErrUnavailable", err)
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want %d", attempts, 1)
			}
		})
	}
}

func TestS3CompatibleStoreUsesRetryAfterDelay(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			resp := testHTTPResponse(r, http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "2")
			return resp, nil
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if len(delays) != 1 {
		t.Fatalf("len(delays) = %d, want %d", len(delays), 1)
	}
	if delays[0] != 2*time.Second {
		t.Fatalf("delay = %v, want %v", delays[0], 2*time.Second)
	}
}

func TestS3CompatibleStoreStopsRetryingAfterMaxRetries(t *testing.T) {
	attempts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		return nil, temporaryNetError{message: "temporary network failure"}
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     2,
		HTTPClient:     client,
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	_, err = store.Get(context.Background(), "abcdef0123456789")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, want ErrUnavailable", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want %d", attempts, 3)
	}
}

func TestS3CompatibleStoreUsesRetryAfterHTTPDate(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	now := time.Date(2026, time.April, 22, 12, 0, 0, 0, time.UTC)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			resp := testHTTPResponse(r, http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", now.Add(3*time.Second).Format(http.TimeFormat))
			return resp, nil
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
		Now: func() time.Time {
			return now
		},
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if len(delays) != 1 {
		t.Fatalf("len(delays) = %d, want %d", len(delays), 1)
	}
	if delays[0] != 3*time.Second {
		t.Fatalf("delay = %v, want %v", delays[0], 3*time.Second)
	}
}

func TestS3CompatibleStoreFallsBackToExponentialBackoffForInvalidRetryAfter(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			resp := testHTTPResponse(r, http.StatusTooManyRequests, "")
			resp.Header.Set("Retry-After", "not-a-valid-retry-after")
			return resp, nil
		}
		return testHTTPResponse(r, http.StatusOK, "rainbow"), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
		Sleep: func(_ context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	body, err := store.Get(context.Background(), "abcdef0123456789")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if string(body) != "rainbow" {
		t.Fatalf("Get() = %q, want %q", string(body), "rainbow")
	}
	if len(delays) != 1 {
		t.Fatalf("len(delays) = %d, want %d", len(delays), 1)
	}
	if delays[0] != defaultS3CompatibleRetryBaseDelay {
		t.Fatalf("delay = %v, want %v", delays[0], defaultS3CompatibleRetryBaseDelay)
	}
}

func TestS3CompatibleStoreReturnsContextErrorWhenRetrySleepCanceled(t *testing.T) {
	attempts := 0
	var delays []time.Duration
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		return testHTTPResponse(r, http.StatusServiceUnavailable, ""), nil
	})}

	store, err := NewS3CompatibleStore(S3CompatibleConfig{
		StorageURL:     "s3://chef-bucket/checksums",
		Endpoint:       "http://s3.test",
		Region:         "us-east-1",
		ForcePathStyle: true,
		AccessKeyID:    "access-key",
		SecretKey:      "secret-key",
		MaxRetries:     1,
		HTTPClient:     client,
		Sleep: func(sleepCtx context.Context, delay time.Duration) error {
			delays = append(delays, delay)
			cancel()
			<-sleepCtx.Done()
			return sleepCtx.Err()
		},
	})
	if err != nil {
		t.Fatalf("NewS3CompatibleStore() error = %v", err)
	}

	_, err = store.Get(ctx, "abcdef0123456789")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Get() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf("Get() error = %v, do not want ErrUnavailable", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want %d", attempts, 1)
	}
	if len(delays) != 1 {
		t.Fatalf("len(delays) = %d, want %d", len(delays), 1)
	}
	if delays[0] != defaultS3CompatibleRetryBaseDelay {
		t.Fatalf("delay = %v, want %v", delays[0], defaultS3CompatibleRetryBaseDelay)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

type temporaryNetError struct {
	message string
}

func (e temporaryNetError) Error() string   { return e.message }
func (e temporaryNetError) Timeout() bool   { return false }
func (e temporaryNetError) Temporary() bool { return true }

type timeoutNetError struct {
	message string
}

func (e timeoutNetError) Error() string   { return e.message }
func (e timeoutNetError) Timeout() bool   { return true }
func (e timeoutNetError) Temporary() bool { return false }

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func testHTTPResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}
}
