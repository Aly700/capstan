// Package config reads the server's environment without logging secret values.
package config

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Aly700/capstan/internal/auth"
	"github.com/Aly700/capstan/internal/engine"
)

type Config struct {
	Addr, DatabaseURL   string
	DBMaxConns          int32
	APIKeyHashes        map[string][32]byte
	DailyCapUSD         float64
	Migrate             bool
	LogLevel            slog.Level
	GateURL, GateAPIKey string
	// Worker and CLI settings from .env.example; the server does not use them.
	Address, APIKey, AnthropicAPIKey string
	PollTimeout                      time.Duration
	ModelPrices                      map[string]engine.ModelPrice
	MaxMessageBytes                  int
}

func Load(getenv func(string) string) (Config, error) {
	c := Config{DBMaxConns: 40, Addr: ":7233", DailyCapUSD: 2, Migrate: true, LogLevel: slog.LevelInfo, Address: "http://127.0.0.1:7233", PollTimeout: 20 * time.Second, MaxMessageBytes: 4 << 20, ModelPrices: maps.Clone(engine.DefaultModelPrices)}
	var problems []string
	invalid := func(name, reason string) { problems = append(problems, name+": "+reason) }
	c.DatabaseURL = getenv("CAPSTAN_DATABASE_URL")
	if strings.TrimSpace(c.DatabaseURL) == "" {
		invalid("CAPSTAN_DATABASE_URL", "required")
	}
	rawKeys := getenv("CAPSTAN_API_KEY_HASHES")
	if strings.TrimSpace(rawKeys) == "" {
		invalid("CAPSTAN_API_KEY_HASHES", "required")
	} else {
		var err error
		c.APIKeyHashes, err = auth.ParseHashes(rawKeys)
		if err != nil {
			invalid("CAPSTAN_API_KEY_HASHES", err.Error())
		}
	}
	if v := getenv("CAPSTAN_ADDR"); v != "" {
		c.Addr = v
	}
	_, port, err := net.SplitHostPort(c.Addr)
	p, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || p < 0 || p > 65535 {
		invalid("CAPSTAN_ADDR", "expected host:port with port between 0 and 65535")
	}
	if v := getenv("CAPSTAN_DB_MAX_CONNS"); v != "" {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil || n <= 0 {
			invalid("CAPSTAN_DB_MAX_CONNS", "must be a positive 32-bit integer")
		} else {
			c.DBMaxConns = int32(n)
		}
	}
	if v := getenv("CAPSTAN_POLL_TIMEOUT"); v != "" {
		c.PollTimeout, err = time.ParseDuration(v)
		if err != nil || c.PollTimeout <= 0 || c.PollTimeout > 30*time.Second {
			invalid("CAPSTAN_POLL_TIMEOUT", "must be a duration greater than zero and at most 30s")
		}
	}
	if v := getenv("CAPSTAN_MAX_MESSAGE_BYTES"); v != "" {
		c.MaxMessageBytes, err = strconv.Atoi(v)
		if err != nil || c.MaxMessageBytes <= 0 {
			invalid("CAPSTAN_MAX_MESSAGE_BYTES", "must be a positive integer")
		}
	}
	if v := getenv("CAPSTAN_DAILY_CAP_USD"); v != "" {
		c.DailyCapUSD, err = strconv.ParseFloat(v, 64)
		if err != nil || !finiteNonnegative(c.DailyCapUSD) || c.DailyCapUSD == 0 {
			invalid("CAPSTAN_DAILY_CAP_USD", "must be finite and greater than zero")
		}
	}
	if v := getenv("CAPSTAN_MIGRATE"); v != "" {
		c.Migrate, err = strconv.ParseBool(v)
		if err != nil {
			invalid("CAPSTAN_MIGRATE", "must be a boolean")
		}
	}
	if v := getenv("CAPSTAN_LOG_LEVEL"); v != "" {
		if err := c.LogLevel.UnmarshalText([]byte(v)); err != nil {
			invalid("CAPSTAN_LOG_LEVEL", "must be a slog level")
		}
	}
	if v := getenv("CAPSTAN_MODEL_PRICES"); v != "" {
		var prices map[string]*engine.ModelPrice
		d := json.NewDecoder(strings.NewReader(v))
		d.DisallowUnknownFields()
		if err := d.Decode(&prices); err != nil || prices == nil || d.Decode(new(any)) != io.EOF {
			invalid("CAPSTAN_MODEL_PRICES", "must be a JSON object of model prices")
		} else {
			for name, price := range prices {
				if strings.TrimSpace(name) == "" || price == nil || !finiteNonnegative(price.Input) || !finiteNonnegative(price.Output) || !finiteNonnegative(price.CacheRead) || !finiteNonnegative(price.CacheWrite) {
					invalid("CAPSTAN_MODEL_PRICES", "model names must be nonempty and prices finite and nonnegative")
					break
				}
				c.ModelPrices[name] = *price
			}
		}
	}
	c.GateURL, c.GateAPIKey = getenv("CAPSTAN_GATE_URL"), getenv("CAPSTAN_GATE_API_KEY")
	if (c.GateURL == "") != (c.GateAPIKey == "") {
		invalid("CAPSTAN_GATE_URL / CAPSTAN_GATE_API_KEY", "must be set together")
	}
	if c.GateURL != "" {
		u, err := url.Parse(c.GateURL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			invalid("CAPSTAN_GATE_URL", "must be an HTTP(S) URL without credentials, query or fragment")
		}
	}
	if v := getenv("CAPSTAN_ADDRESS"); v != "" {
		c.Address = v
	}
	c.APIKey, c.AnthropicAPIKey = getenv("CAPSTAN_API_KEY"), getenv("ANTHROPIC_API_KEY")
	if len(problems) > 0 {
		return Config{}, errors.New(strings.Join(problems, "; "))
	}
	return c, nil
}

func finiteNonnegative(v float64) bool { return v >= 0 && !math.IsInf(v, 0) && !math.IsNaN(v) }
