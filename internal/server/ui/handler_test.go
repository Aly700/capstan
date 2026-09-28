package ui_test

import (
	"crypto/sha256"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/server"
	"github.com/Aly700/capstan/internal/server/ui"
)

func TestEmbeddedAssets(t *testing.T) {
	h := ui.NewHandler()
	for _, tc := range []struct{ path, contentType, contains string }{
		{"/ui/", "text/html; charset=utf-8", "Capstan"},
		{"/ui/index.html", "text/html; charset=utf-8", "Capstan"},
		{"/ui/style.css", "text/css; charset=utf-8", ":root"},
		{"/ui/app.js", "text/javascript; charset=utf-8", "sessionStorage"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if w.Code != 200 || !strings.Contains(w.Body.String(), tc.contains) {
				t.Fatalf("response %d: %s", w.Code, w.Body.String())
			}
			if got := w.Header().Get("Content-Type"); got != tc.contentType {
				t.Fatalf("type %q", got)
			}
			if got := w.Header().Get("Cache-Control"); got != "no-cache" {
				t.Fatalf("cache %q", got)
			}
			if w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Referrer-Policy") != "no-referrer" {
				t.Fatal("missing browser headers")
			}
			if !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'self'") {
				t.Fatal("missing CSP")
			}
			head := httptest.NewRecorder()
			h.ServeHTTP(head, httptest.NewRequest(http.MethodHead, tc.path, nil))
			if head.Code != 200 || head.Body.Len() != 0 {
				t.Fatal("HEAD must return headers only")
			}
		})
	}
}

func TestOnlyEmbeddedFiles(t *testing.T) {
	for _, path := range []string{"/", "/ui/../AGENTS.md", "/ui/%2e%2e/AGENTS.md", "/ui/static/", "/ui/.env", "/ui/handler.go", "/ui/missing.js", "/ui/app.js/"} {
		w := httptest.NewRecorder()
		ui.NewHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusNotFound {
			t.Errorf("%s: status %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	ui.NewHandler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/ui/", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, HEAD" {
		t.Fatal("method restriction missing")
	}
}

func TestPublicPageAndJSONConnectAuth(t *testing.T) {
	// No engine needed: an unauthenticated RPC must be rejected before dispatch.
	s := server.New(config.Config{APIKeyHashes: map[string][32]byte{"test": sha256.Sum256([]byte("test-key"))}}, nil, nil, server.Options{})
	page := httptest.NewRecorder()
	s.ServeHTTP(page, httptest.NewRequest(http.MethodGet, "/ui/", nil))
	if page.Code != 200 {
		t.Fatalf("public page: %d", page.Code)
	}
	req := httptest.NewRequest(http.MethodPost, "/capstan.v1.ClientService/ListRuns", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || !strings.Contains(w.Body.String(), "unauthenticated") {
		t.Fatalf("RPC auth: %d %s", w.Code, w.Body.String())
	}
}
