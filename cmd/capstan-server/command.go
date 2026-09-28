package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Aly700/capstan/internal/auth"
	"github.com/Aly700/capstan/internal/config"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/gate"
	"github.com/Aly700/capstan/internal/server"
	"github.com/Aly700/capstan/internal/store"
	"github.com/Aly700/capstan/internal/store/pgstore"
)

// Dependencies isolate process wiring from the concrete engine and store. The
// production path uses only their frozen constructors; tests supply fakes.
type dependencies struct {
	migrate   func(context.Context, string) error
	open      func(context.Context, string) (store.Store, error)
	newEngine func(engine.Deps, engine.Config) (engine.API, error)
	readiness func(context.Context, string) (func(context.Context) error, func(), error)
	listen    func(string, string) (net.Listener, error)
}

func productionDependencies() dependencies {
	return dependencies{migrate: pgstore.Migrate, open: pgstore.Open, newEngine: func(d engine.Deps, c engine.Config) (engine.API, error) { return engine.New(d, c) }, readiness: databaseReadiness, listen: net.Listen}
}

func run(ctx context.Context, args []string, getenv func(string) string, in io.Reader, out, logOut io.Writer, d dependencies) error {
	command := "serve"
	if len(args) > 0 {
		command = args[0]
	}
	switch command {
	case "keygen":
		if len(args) != 2 {
			return errors.New("usage: capstan-server keygen <name>")
		}
		keys, err := auth.ParseHashes(args[1] + ":" + strings.Repeat("00", 32))
		_, validName := keys[args[1]]
		if err != nil || len(keys) != 1 || !validName {
			return errors.New("invalid key name: use 1-200 letters, digits, dots, underscores or hyphens")
		}
		key := auth.NewKey()
		_, err = fmt.Fprintf(out, "%s\n%s:%x\n", key, args[1], sha256.Sum256([]byte(key)))
		return err
	case "hash-key":
		if len(args) != 1 {
			return errors.New("usage: capstan-server hash-key < key-file")
		}
		input, err := io.ReadAll(io.LimitReader(in, 4097))
		if err != nil {
			return fmt.Errorf("read key: %w", err)
		}
		if len(input) > 4096 {
			return errors.New("key input exceeds 4096 bytes")
		}
		key := strings.TrimSpace(string(input))
		sum := sha256.Sum256([]byte(key))
		if _, ok := auth.Identify("Bearer "+key, map[string][32]byte{"key": sum}); !ok {
			return errors.New("stdin must contain one nonempty API key")
		}
		_, err = fmt.Fprintf(out, "%x\n", sum)
		return err
	case "migrate":
		if len(args) != 1 {
			return errors.New("usage: capstan-server migrate")
		}
		dsn := getenv("CAPSTAN_DATABASE_URL")
		if strings.TrimSpace(dsn) == "" {
			return errors.New("CAPSTAN_DATABASE_URL: required")
		}
		return d.migrate(ctx, dsn)
	case "serve":
		if len(args) > 1 {
			return errors.New("usage: capstan-server [serve]")
		}
	default:
		return errors.New("usage: capstan-server [serve | migrate | keygen <name> | hash-key]")
	}
	cfg, err := config.Load(getenv)
	if err != nil {
		return err
	}
	logger := slog.New(slog.NewJSONHandler(logOut, &slog.HandlerOptions{Level: cfg.LogLevel}))
	if cfg.Migrate {
		if err := d.migrate(ctx, cfg.DatabaseURL); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	st, err := d.open(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			logger.Error("close store", "error", err)
		}
	}()
	var gateClient engine.GateClient
	if cfg.GateURL != "" {
		gateClient = gate.New(cfg.GateURL, cfg.GateAPIKey, nil)
	}
	location, err := time.LoadLocation("America/Toronto")
	if err != nil {
		return fmt.Errorf("load cap time zone: %w", err)
	}
	api, err := d.newEngine(engine.Deps{Store: st, Clock: engine.SystemClock{}, Gate: gateClient}, engine.Config{DailyCapUSD: cfg.DailyCapUSD, ModelPrices: cfg.ModelPrices, CapLocation: location})
	if err != nil {
		return fmt.Errorf("create engine: %w", err)
	}
	ready, closeReady, err := d.readiness(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("create readiness check: %w", err)
	}
	defer closeReady()
	listener, err := d.listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	return server.New(cfg, api, st, server.Options{Logger: logger, Ready: ready}).Serve(ctx, listener)
}
