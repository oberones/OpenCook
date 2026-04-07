package blob

import (
	"context"
	"errors"
	"io"
	"net/http"
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
			if _, ok := objects[r.URL.Path]; !ok {
				return testHTTPResponse(r, http.StatusNotFound, ""), nil
			}
			return testHTTPResponse(r, http.StatusOK, ""), nil
		case http.MethodGet:
			body, ok := objects[r.URL.Path]
			if !ok {
				return testHTTPResponse(r, http.StatusNotFound, ""), nil
			}
			return testHTTPResponse(r, http.StatusOK, string(body)), nil
		case http.MethodDelete:
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

type roundTripFunc func(*http.Request) (*http.Response, error)

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
