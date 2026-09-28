package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAuditMetricsRequiresOneValidKey(t *testing.T) {
	s := New(testConfig(), &fakeAPI{}, nil, Options{})
	for _, headers := range [][]string{nil, {"Bearer wrong"}, {"Bearer owner:secret"}, {"Basic secret"}, {"Bearer  secret"}, {"Bearer secret extra"}, {"Bearer secret", "Bearer secret"}, {"Bearer secret"}} {
		req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		for _, header := range headers {
			req.Header.Add("Authorization", header)
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		valid := len(headers) == 1 && headers[0] == "Bearer secret"
		if valid {
			if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "capstan_rpc_total") {
				t.Fatalf("valid key: status=%d body=%s", rec.Code, rec.Body.String())
			}
			continue
		}
		if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "capstan_rpc_total") {
			t.Errorf("headers=%q exposed metrics: status=%d", headers, rec.Code)
		}
	}
}
