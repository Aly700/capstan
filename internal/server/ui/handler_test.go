package ui_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Aly700/capstan/internal/server/ui"
)

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
