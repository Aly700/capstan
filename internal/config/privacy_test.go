package config

import (
	"net/url"
	"strings"
	"testing"
)

func TestRedactKeepsDiagnosticsWithoutConfiguredSecrets(t *testing.T) {
	const password = "password/with+punctuation"
	c := Config{DatabaseURL: "postgres://user:" + url.QueryEscape(password) + "@database/private", GateAPIKey: "gate-canary", APIKey: "api-canary", AnthropicAPIKey: "provider-canary"}
	for _, secret := range []string{c.DatabaseURL, password, url.QueryEscape(password), url.PathEscape(password), c.GateAPIKey, c.APIKey, c.AnthropicAPIKey, "request-canary"} {
		got := c.Redact("upstream returned "+secret, "request-canary")
		if strings.Contains(got, secret) || !strings.Contains(got, "upstream returned [REDACTED]") {
			t.Errorf("unredacted diagnostic %q", got)
		}
	}
	if got := (Config{}).Redact("safe diagnostic"); got != "safe diagnostic" {
		t.Fatalf("empty credentials corrupted message: %q", got)
	}
}
