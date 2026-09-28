package config

import (
	"net/url"
	"strings"
)

// Redact removes configured credentials from diagnostic text. Payloads remain
// opaque; callers must keep credentials out of workflow inputs and results.
func (c Config) Redact(text string, extra ...string) string {
	secrets := append([]string{c.DatabaseURL, c.GateAPIKey, c.APIKey, c.AnthropicAPIKey}, extra...)
	if u, err := url.Parse(c.DatabaseURL); err == nil && u.User != nil {
		if password, ok := u.User.Password(); ok {
			secrets = append(secrets, password, url.QueryEscape(password), url.PathEscape(password))
		}
	}
	for _, secret := range secrets {
		if secret != "" {
			text = strings.ReplaceAll(text, secret, "[REDACTED]")
		}
	}
	return text
}
