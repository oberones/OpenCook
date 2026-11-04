package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReviewPolicyNumericHTTPIdempotence(t *testing.T) {
	router := newTestRouter(t)
	for i, n := range []string{"9007199254740993", "9007199254740993.0", "9007199254740992"} {
		payload := minimalPolicyPayload("numeric", "1111111111111111111111111111111111111111")
		payload["default_attributes"] = map[string]any{"nested": []any{json.Number(n), json.Number("1e6")}}
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, newSignedJSONRequest(t, http.MethodPut, "/policy_groups/dev/policies/numeric", mustMarshalPolicyJSON(t, payload)))
		want := http.StatusOK
		if i == 0 {
			want = http.StatusCreated
		}
		if i == 2 {
			want = http.StatusConflict
		}
		if rec.Code != want {
			t.Fatalf("write %s: %d want %d: %s", n, rec.Code, want, rec.Body.String())
		}
	}
}
