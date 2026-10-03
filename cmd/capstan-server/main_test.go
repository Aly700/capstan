package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	rpc "github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"github.com/Aly700/capstan/internal/auth"
	"github.com/Aly700/capstan/internal/engine"
	"github.com/Aly700/capstan/internal/server"
	"github.com/Aly700/capstan/internal/store"
	"github.com/jackc/pgx/v5"
)

func testEnv(key string) string {
	switch key {
	case "CAPSTAN_DB_MAX_CONNS":
		return "7"
	case "CAPSTAN_DATABASE_URL":
		return "postgres://not-used"
	case "CAPSTAN_API_KEY_HASHES":
		return fmt.Sprintf("owner:%x", sha256.Sum256([]byte("secret")))
	case "CAPSTAN_DAILY_CAP_USD":
		return "0.75"
	case "CAPSTAN_MODEL_PRICES":
		return `{"custom":{"input":1,"output":2}}`
	case "CAPSTAN_GATE_URL":
		return "http://gate.example"
	case "CAPSTAN_GATE_API_KEY":
		return "gate-key"
	}
	return ""
}

type bootStore struct {
	store.Store
	closed atomic.Int32
}

func (s *bootStore) Close() error { s.closed.Add(1); return nil }
func (s *bootStore) Subscribe(store.TaskKind, string) (<-chan struct{}, func()) {
	return nil, func() {}
}

type bootAPI struct {
	engine.API
	pollStarted              chan struct{}
	pollOnce                 sync.Once
	slowStarted, slowRelease chan struct{}
}

func (f *bootAPI) DescribeRun(ctx context.Context, req *v1.DescribeRunRequest) (*v1.DescribeRunResponse, error) {
	if server.Identity(ctx) != "owner" {
		return nil, errors.New("missing authenticated identity")
	}
	if req.RunId == "slow" {
		close(f.slowStarted)
		select {
		case <-f.slowRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &v1.DescribeRunResponse{Run: &v1.RunInfo{RunId: req.RunId, Status: v1.RunStatus_RUN_STATUS_COMPLETED}}, nil
}
func (f *bootAPI) PollWorkflowTask(ctx context.Context, req *v1.PollWorkflowTaskRequest) (*v1.PollWorkflowTaskResponse, bool, error) {
	if f.pollStarted != nil {
		f.pollOnce.Do(func() { close(f.pollStarted) })
	}
	return nil, false, nil
}
func (f *bootAPI) FireDueTimers(context.Context, int) (int, error)       { return 0, nil }
func (f *bootAPI) ProcessDueTasks(context.Context, int) (int, error)     { return 0, nil }
func (f *bootAPI) ProcessDueApprovals(context.Context, int) (int, error) { return 0, nil }
func (f *bootAPI) TimeoutRuns(context.Context, int) (int, error)         { return 0, nil }
func (f *bootAPI) NextWakeup(context.Context) (time.Time, bool, error) {
	return time.Time{}, false, nil
}

func fakeDeps(t *testing.T, l net.Listener, f *bootAPI) (dependencies, *bootStore, *atomic.Int32) {
	t.Helper()
	st := &bootStore{}
	readyClosed := new(atomic.Int32)
	d := dependencies{
		migrate: func(ctx context.Context, dsn string) error {
			if dsn != testEnv("CAPSTAN_DATABASE_URL") {
				t.Error("wrong migrate DSN")
			}
			return nil
		},
		open: func(ctx context.Context, dsn string, maxConns int32) (store.Store, error) {
			if dsn != testEnv("CAPSTAN_DATABASE_URL") || maxConns != 7 {
				t.Error("wrong open DSN or pool limit")
			}
			return st, nil
		},
		newEngine: func(deps engine.Deps, cfg engine.Config) (engine.API, error) {
			if deps.Store != st || deps.Clock == nil || deps.Gate == nil || cfg.DailyCapUSD != 0.75 || cfg.ModelPrices["custom"].Output != 2 || cfg.CapLocation.String() != "America/Toronto" {
				t.Error("engine dependencies/config not wired")
			}
			return f, nil
		},
		readiness: func(ctx context.Context, dsn string) (func(context.Context) error, func(), error) {
			return func(context.Context) error { return nil }, func() { readyClosed.Add(1) }, nil
		},
		listen: func(network, address string) (net.Listener, error) {
			if network != "tcp" || address != ":7233" {
				t.Errorf("listen %s %s", network, address)
			}
			return l, nil
		},
	}
	return d, st, readyClosed
}

type checkingTransport struct {
	base  *http.Transport
	major int
	t     *testing.T
}

func (c checkingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r, err := c.base.RoundTrip(req)
	if err == nil && r.ProtoMajor != c.major {
		c.t.Errorf("protocol=%s want HTTP/%d", r.Proto, c.major)
	}
	return r, err
}
func testHTTP(t *testing.T, major int) *http.Client {
	t.Helper()
	p := new(http.Protocols)
	if major == 1 {
		p.SetHTTP1(true)
	} else {
		p.SetUnencryptedHTTP2(true)
	}
	tr := &http.Transport{Protocols: p}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: checkingTransport{tr, major, t}, Timeout: 5 * time.Second}
}
func TestBootHTTP1AndHTTP2AndGracefulDrain(t *testing.T) {
	for _, major := range []int{1, 2} {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			f := &bootAPI{pollStarted: make(chan struct{}), slowStarted: make(chan struct{}), slowRelease: make(chan struct{})}
			d, st, readyClosed := fakeDeps(t, l, f)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- run(ctx, nil, testEnv, strings.NewReader(""), io.Discard, io.Discard, d) }()
			hc := testHTTP(t, major)
			url := "http://" + l.Addr().String()
			c := rpc.NewClientServiceClient(hc, url)
			req := connect.NewRequest(&v1.DescribeRunRequest{RunId: "r"})
			req.Header().Set("Authorization", "Bearer secret")
			response, err := c.DescribeRun(context.Background(), req)
			if err != nil || response.Msg.GetRun().GetRunId() != "r" {
				t.Fatalf("describe=%v err=%v", response, err)
			}
			for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
				req, err := http.NewRequest(http.MethodGet, url+path, nil)
				if err != nil {
					t.Fatal(err)
				}
				if path == "/metrics" {
					req.Header.Set("Authorization", "Bearer secret")
				}
				r, err := hc.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || r.StatusCode != 200 {
					t.Fatalf("%s status=%d err=%v", path, r.StatusCode, err)
				}
				if path == "/metrics" && !bytes.Contains(body, []byte("capstan_rpc_total")) {
					t.Fatal("missing metrics")
				}
			}
			pollDone := make(chan error, 1)
			go func() {
				req := connect.NewRequest(&v1.PollWorkflowTaskRequest{TaskQueue: "q"})
				req.Header().Set("Authorization", "Bearer secret")
				r, err := rpc.NewWorkerServiceClient(hc, url).PollWorkflowTask(context.Background(), req)
				if err == nil && len(r.Msg.TaskToken) != 0 {
					err = errors.New("shutdown poll was not empty")
				}
				pollDone <- err
			}()
			select {
			case <-f.pollStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("poll did not start")
			}
			slowDone := make(chan error, 1)
			go func() {
				req := connect.NewRequest(&v1.DescribeRunRequest{RunId: "slow"})
				req.Header().Set("Authorization", "Bearer secret")
				_, err := c.DescribeRun(context.Background(), req)
				slowDone <- err
			}()
			select {
			case <-f.slowStarted:
			case <-time.After(2 * time.Second):
				t.Fatal("slow RPC did not start")
			}
			start := time.Now()
			cancel()
			select {
			case err := <-pollDone:
				if err != nil {
					t.Fatalf("shutdown poll: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("poll not cancelled by shutdown")
			}
			select {
			case err := <-slowDone:
				t.Fatalf("in-flight RPC cancelled before drain: %v", err)
			default:
			}
			close(f.slowRelease)
			if err := <-slowDone; err != nil {
				t.Fatalf("drained RPC: %v", err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown did not finish")
			}
			if time.Since(start) >= 25*time.Second || st.closed.Load() != 1 || readyClosed.Load() != 1 {
				t.Fatalf("elapsed=%v store close=%d ready close=%d", time.Since(start), st.closed.Load(), readyClosed.Load())
			}
		})
	}
}

func TestKeyCommandsAndUsage(t *testing.T) {
	var out bytes.Buffer
	noEnv := func(string) string { return "" }
	if err := run(context.Background(), []string{"keygen", "lane-worker"}, noEnv, strings.NewReader(""), &out, io.Discard, dependencies{}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || strings.Count(out.String(), lines[0]) != 1 {
		t.Fatalf("key output must contain two lines and key once: %q", out.String())
	}
	keys, err := auth.ParseHashes(lines[1])
	if err != nil {
		t.Fatal(err)
	}
	if name, ok := auth.Identify("Bearer "+lines[0], keys); !ok || name != "lane-worker" {
		t.Fatal("generated hash does not authenticate")
	}
	plain := lines[0]
	out.Reset()
	if err := run(context.Background(), []string{"hash-key"}, noEnv, strings.NewReader(plain+"\n"), &out, io.Discard, dependencies{}); err != nil {
		t.Fatal(err)
	}
	if out.String() != fmt.Sprintf("%x\n", sha256.Sum256([]byte(plain))) {
		t.Fatal("wrong hash")
	}
	for _, args := range [][]string{{"nope"}, {"keygen"}, {"keygen", "bad:name"}, {"keygen", " owner"}, {"keygen", "a:" + strings.Repeat("ab", 32) + ",b"}, {"keygen", "a", "b"}, {"hash-key", "arg"}, {"serve", "arg"}, {"migrate", "arg"}} {
		out.Reset()
		if err := run(context.Background(), args, noEnv, strings.NewReader(""), &out, io.Discard, dependencies{}); err == nil || out.Len() != 0 {
			t.Errorf("invalid args accepted or emitted key: %q", args)
		}
	}
	for _, input := range []string{"", " \n", "two keys", strings.Repeat("x", 4097)} {
		out.Reset()
		if err := run(context.Background(), []string{"hash-key"}, noEnv, strings.NewReader(input), &out, io.Discard, dependencies{}); err == nil {
			t.Errorf("invalid stdin accepted")
		}
	}
}
func TestStartupFailureClosesResources(t *testing.T) {
	for _, step := range []string{"migrate", "open", "engine", "readiness", "listen"} {
		t.Run(step, func(t *testing.T) {
			f := &bootAPI{}
			d, st, readyClosed := fakeDeps(t, nil, f)
			failure := errors.New("expected failure")
			switch step {
			case "migrate":
				d.migrate = func(context.Context, string) error { return failure }
			case "open":
				d.open = func(context.Context, string, int32) (store.Store, error) { return nil, failure }
			case "engine":
				d.newEngine = func(engine.Deps, engine.Config) (engine.API, error) { return nil, failure }
			case "readiness":
				d.readiness = func(context.Context, string) (func(context.Context) error, func(), error) { return nil, nil, failure }
			case "listen":
				d.listen = func(string, string) (net.Listener, error) { return nil, failure }
			}
			err := run(context.Background(), []string{"serve"}, testEnv, strings.NewReader(""), io.Discard, io.Discard, d)
			if !errors.Is(err, failure) {
				t.Fatalf("err=%v", err)
			}
			wantStore := int32(1)
			if step == "migrate" || step == "open" {
				wantStore = 0
			}
			if st.closed.Load() != wantStore {
				t.Fatalf("store closed=%d want=%d", st.closed.Load(), wantStore)
			}
			wantReady := int32(0)
			if step == "listen" {
				wantReady = 1
			}
			if readyClosed.Load() != wantReady {
				t.Fatalf("ready closed=%d want=%d", readyClosed.Load(), wantReady)
			}
		})
	}
}

func postgresTestURL(t *testing.T, dsn, name string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func TestReadinessQueriesPostgres(t *testing.T) {
	dsn := os.Getenv("CAPSTAN_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://capstan:capstan@127.0.0.1:55432/postgres?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	name := fmt.Sprintf("capstan_audit_server_test_%x", sha256.Sum256([]byte(auth.NewKey())))[:48]
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "create database "+ident); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := admin.Exec(context.Background(), "drop database "+ident+" with (force)"); err != nil {
			t.Error(err)
		}
	}()
	dbURL := postgresTestURL(t, dsn, name)
	ready, closeReady, err := databaseReadiness(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer closeReady()
	probeCtx, probeCancel := context.WithTimeout(ctx, time.Second)
	defer probeCancel()
	if err := ready(probeCtx); err != nil {
		t.Fatalf("SELECT 1 failed: %v", err)
	}
}

func TestServeSignalProcess(t *testing.T) {
	if os.Getenv("CAPSTAN_SERVER_TEST_PROCESS") != "1" {
		return
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	d, _, _ := fakeDeps(t, l, &bootAPI{})
	ctx, stop := signalContext()
	defer stop()
	fmt.Fprintln(os.Stdout, l.Addr().String())
	if err := run(ctx, nil, testEnv, strings.NewReader(""), io.Discard, os.Stderr, d); err != nil {
		t.Fatal(err)
	}
}
func TestSIGTERMExitsWithinGracePeriod(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestServeSignalProcess$")
	cmd.Env = append(os.Environ(), "CAPSTAN_SERVER_TEST_PROCESS=1")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill() }()
	lines := bufio.NewScanner(stdout)
	if !lines.Scan() {
		t.Fatalf("child did not bind: %v", lines.Err())
	}
	address := lines.Text()
	hc := testHTTP(t, 1)
	r, err := hc.Get("http://" + address + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatalf("health=%d", r.StatusCode)
	}
	start := time.Now()
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("child exit: %v\n%s", err, stderr.String())
		}
	case <-time.After(25 * time.Second):
		t.Fatal("SIGTERM exceeded grace period")
	}
	if time.Since(start) >= 25*time.Second {
		t.Fatal("slow SIGTERM exit")
	}
}
