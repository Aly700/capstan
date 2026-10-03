package config

import (
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"
)

func env() map[string]string {
	return map[string]string{"CAPSTAN_DATABASE_URL": "postgres://capstan:capstan@127.0.0.1:55432/capstan?sslmode=disable", "CAPSTAN_API_KEY_HASHES": "owner:" + strings.Repeat("ab", 32)}
}
func load(values map[string]string) (Config, error) {
	return Load(func(k string) string { return values[k] })
}

func TestLoadOverridesAndAllExampleFields(t *testing.T) {
	e := env()
	for k, v := range map[string]string{"CAPSTAN_ADDR": "127.0.0.1:7234", "CAPSTAN_DB_MAX_CONNS": "20", "CAPSTAN_POLL_TIMEOUT": "30s", "CAPSTAN_MAX_MESSAGE_BYTES": "1234", "CAPSTAN_DAILY_CAP_USD": "0.05", "CAPSTAN_MIGRATE": "false", "CAPSTAN_LOG_LEVEL": "debug", "CAPSTAN_GATE_URL": "http://127.0.0.1:8000/api", "CAPSTAN_GATE_API_KEY": "gate-secret", "CAPSTAN_ADDRESS": "https://example.test", "CAPSTAN_API_KEY": "worker-secret", "ANTHROPIC_API_KEY": "provider-secret", "CAPSTAN_MODEL_PRICES": `{"custom":{"input":1,"output":2,"cache_read":0.1,"cache_write":1.25}}`} {
		e[k] = v
	}
	c, err := load(e)
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != e["CAPSTAN_ADDR"] || c.DatabaseURL != e["CAPSTAN_DATABASE_URL"] || c.DBMaxConns != 20 || c.PollTimeout != 30*time.Second || c.MaxMessageBytes != 1234 || math.Abs(c.DailyCapUSD-0.05) > 1e-9 || c.Migrate || c.LogLevel != slog.LevelDebug || c.GateURL != e["CAPSTAN_GATE_URL"] || c.GateAPIKey != "gate-secret" || c.Address != e["CAPSTAN_ADDRESS"] || c.APIKey != "worker-secret" || c.AnthropicAPIKey != "provider-secret" || c.ModelPrices["custom"].Output != 2 || c.ModelPrices["claude-sonnet-5"].Input != 2 {
		t.Fatal("override not loaded")
	}
}
func TestLoadRejectsInvalidValuesWithoutLeakingThem(t *testing.T) {
	for key, values := range map[string][]string{
		"CAPSTAN_DATABASE_URL":      {""},
		"CAPSTAN_DB_MAX_CONNS":      {"0", "-1", "1.5", "2147483648", "secret-value"},
		"CAPSTAN_POLL_TIMEOUT":      {"31s", "0s", "-1s", "oops"},
		"CAPSTAN_MAX_MESSAGE_BYTES": {"0", "-1", "NaN"},
		"CAPSTAN_DAILY_CAP_USD":     {"0", "-1", "NaN", "+Inf", "oops"},
		"CAPSTAN_MIGRATE":           {"maybe"}, "CAPSTAN_LOG_LEVEL": {"secret-value"},
		"CAPSTAN_ADDR":           {"bad", "host:bad", "host:65536"},
		"CAPSTAN_MODEL_PRICES":   {`null`, `[]`, `{"custom":null}`, `{"custom":{"input":-1}}`, `{"custom":{"inpt":1}}`, `{"":{}}`, `{} {}`},
		"CAPSTAN_API_KEY_HASHES": {"", "secret-value"},
	} {
		for _, value := range values {
			t.Run(key+"/"+value, func(t *testing.T) {
				e := env()
				e[key] = value
				_, err := load(e)
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("bad value accepted or unnamed error: %v", err)
				}
				if strings.Contains(err.Error(), "secret-value") {
					t.Fatal("configuration value leaked")
				}
			})
		}
	}
}
func TestGateConfigurationIsPairedAndHTTP(t *testing.T) {
	for _, pair := range [][2]string{{"https://gate.test", ""}, {"", "secret"}, {"file:///tmp/gate", "secret"}, {"https://user:pass@gate.test", "secret"}, {"https://gate.test?q=1", "secret"}, {"https://gate.test#frag", "secret"}} {
		e := env()
		e["CAPSTAN_GATE_URL"], e["CAPSTAN_GATE_API_KEY"] = pair[0], pair[1]
		if _, err := load(e); err == nil {
			t.Errorf("accepted gate configuration %q", pair[0])
		}
	}
}
