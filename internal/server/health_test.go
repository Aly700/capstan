package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestHealthAndReadiness(t *testing.T) {
	for _, healthy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unready", true: "ready"}[healthy], func(t *testing.T) {
			calls := 0
			s := New(testConfig(), nil, nil, Options{Ready: func(ctx context.Context) error {
				calls++
				d, ok := ctx.Deadline()
				if !ok || time.Until(d) > time.Second {
					t.Error("readiness has no 1s budget")
				}
				if !healthy {
					return errors.New("database-password-secret")
				}
				return nil
			}})
			health := httptest.NewRecorder()
			s.ServeHTTP(health, httptest.NewRequest("GET", "/healthz", nil))
			if health.Code != 200 || calls != 0 {
				t.Fatalf("health=%d calls=%d", health.Code, calls)
			}
			ready := httptest.NewRecorder()
			s.ServeHTTP(ready, httptest.NewRequest("GET", "/readyz", nil))
			want := 503
			if healthy {
				want = 200
			}
			if ready.Code != want || calls != 1 || strings.Contains(ready.Body.String(), "secret") {
				t.Fatalf("ready=%d calls=%d body=%s", ready.Code, calls, ready.Body)
			}
		})
	}
	s := New(testConfig(), nil, nil, Options{})
	ready := httptest.NewRecorder()
	s.ServeHTTP(ready, httptest.NewRequest("GET", "/readyz", nil))
	if ready.Code != 503 {
		t.Fatalf("unconfigured readiness=%d", ready.Code)
	}
}
func TestReadinessTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(testConfig(), nil, nil, Options{Ready: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }})
		r := httptest.NewRecorder()
		start := time.Now()
		s.ServeHTTP(r, httptest.NewRequest("GET", "/readyz", nil))
		if r.Code != 503 || time.Since(start) != time.Second {
			t.Fatalf("status=%d wait=%v", r.Code, time.Since(start))
		}
	})
}
