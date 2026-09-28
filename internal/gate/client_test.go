package gate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestApprovalStatusRealGateJSON(t *testing.T) {
	for _, status := range []string{"PENDING", "APPROVED", "DENIED", "EXPIRED"} {
		t.Run(status, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.EscapedPath() != "/api/approvals/a%2Fb%3F%23" || r.Header.Get("X-API-Key") != "secret" {
					t.Errorf("method=%s path=%s key=%q", r.Method, r.URL.EscapedPath(), r.Header.Get("X-API-Key"))
				}
				decidedBy, decidedAt := `"alice"`, `"2026-09-28T19:20:21.123Z"`
				if status == "PENDING" {
					decidedBy, decidedAt = "null", "null"
				}
				fmt.Fprintf(w, `{"id":"a/b?#","decisionId":"decision-1","status":%q,"decidedBy":%s,"decidedAt":%s,"expiresAt":"2026-10-01T00:00:00Z","createdAt":"2026-09-28T00:00:00Z"}`, status, decidedBy, decidedAt)
			}))
			defer ts.Close()
			result, err := New(ts.URL+"/api/", "secret", ts.Client()).ApprovalStatus(context.Background(), "a/b?#")
			if err != nil || result.Status != status {
				t.Fatalf("result=%v err=%v", result, err)
			}
			if status == "PENDING" {
				if result.DecidedBy != "" || !result.DecidedAt.IsZero() {
					t.Fatalf("pending=%v", result)
				}
			} else {
				want, _ := time.Parse(time.RFC3339Nano, "2026-09-28T19:20:21.123Z")
				if result.DecidedBy != "alice" || !result.DecidedAt.Equal(want) {
					t.Fatalf("decision=%v", result)
				}
			}
		})
	}
}
func TestApprovalStatusRejectsHTTPAndMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		code int
		body string
	}{{404, "private body"}, {500, "private body"}, {503, "private body"}, {204, ""}, {200, `not json`}, {200, `{"status":"ALLOW"}`}, {200, `{}`}, {200, `null`}, {200, `{"status":"APPROVED","decidedAt":"yesterday"}`}, {200, `{"status":"PENDING"} {}`}, {200, strings.Repeat("x", (1<<20)+1)}} {
		t.Run(fmt.Sprint(tc.code)+tc.body[:min(len(tc.body), 30)], func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.code); fmt.Fprint(w, tc.body) }))
			defer ts.Close()
			_, err := New(ts.URL, "secret", ts.Client()).ApprovalStatus(context.Background(), "approval")
			if err == nil || strings.Contains(err.Error(), "private body") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestApprovalStatusDeadlineAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, parentTimeout := range []time.Duration{0, 2 * time.Second} {
			ctx := context.Background()
			cancel := func() {}
			want := 10 * time.Second
			if parentTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, parentTimeout)
				want = parentTimeout
			}
			hc := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() })}
			start := time.Now()
			_, err := New("https://gate.test", "secret", hc).ApprovalStatus(ctx, "id")
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != want {
				t.Fatalf("err=%v elapsed=%v", err, time.Since(start))
			}
			if hc.Timeout != 0 {
				t.Fatal("mutated supplied HTTP client")
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := New("https://gate.test", "secret", &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) { return nil, r.Context().Err() })}).ApprovalStatus(ctx, "id")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v", err)
		}
	})
}
func TestApprovalStatusDoesNotFollowRedirects(t *testing.T) {
	var hits atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1); fmt.Fprint(w, `{"status":"PENDING"}`) }))
	defer other.Close()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, http.StatusFound) }))
	defer ts.Close()
	_, err := New(ts.URL, "secret", ts.Client()).ApprovalStatus(context.Background(), "id")
	if err == nil || hits.Load() != 0 {
		t.Fatalf("followed redirect: hits=%d err=%v", hits.Load(), err)
	}
}
func TestApprovalStatusRejectsBadBaseOrID(t *testing.T) {
	hc := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		t.Error("unexpected network request")
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"status":"PENDING"}`)), Header: make(http.Header)}, nil
	})}
	for _, base := range []string{"", "://bad", "file:///tmp/approvals", "https://user:pass@gate.test", "https://gate.test?query=1"} {
		if _, err := New(base, "key", hc).ApprovalStatus(context.Background(), "id"); err == nil {
			t.Errorf("accepted base %q", base)
		}
	}
	if _, err := New("https://gate.test", "key", hc).ApprovalStatus(context.Background(), ""); err == nil {
		t.Error("accepted empty id")
	}
}
