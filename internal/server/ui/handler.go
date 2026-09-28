// Package ui serves the public, read-only run viewer. RPC authentication remains
// in the existing ClientService interceptor; static files contain no credentials.
package ui

import (
	"bytes"
	"embed"
	"net/http"
	"time"
)

//go:embed static/index.html static/style.css static/app.js
var assets embed.FS

// NewHandler serves only the three embedded assets at /ui/.
func NewHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'none'")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var name, contentType string
		switch r.URL.Path {
		case "/ui/", "/ui/index.html":
			name, contentType = "index.html", "text/html; charset=utf-8"
		case "/ui/style.css":
			name, contentType = "style.css", "text/css; charset=utf-8"
		case "/ui/app.js":
			name, contentType = "app.js", "text/javascript; charset=utf-8"
		default:
			http.NotFound(w, r)
			return
		}
		data, err := assets.ReadFile("static/" + name)
		if err != nil {
			http.Error(w, "asset unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		http.ServeContent(w, r, name, time.Time{}, bytes.NewReader(data))
	})
}
