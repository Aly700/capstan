// Command capstan-load measures real five-activity SDK workflows on a local server.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
	"github.com/Aly700/capstan/gen/capstan/v1/capstanv1connect"
	"google.golang.org/protobuf/types/known/durationpb"
)

type sample struct {
	RunID      string  `json:"run_id"`
	LatencyMS  float64 `json:"latency_ms"`
	Activities int     `json:"activities"`
	Error      string  `json:"error,omitempty"`
}
type summary struct {
	Runs                int     `json:"runs"`
	Concurrency         int     `json:"concurrency"`
	Completed           int     `json:"completed"`
	Errors              int     `json:"errors"`
	Activities          int     `json:"activities"`
	ElapsedSeconds      float64 `json:"elapsed_seconds"`
	RunsPerSecond       float64 `json:"runs_per_second"`
	ActivitiesPerSecond float64 `json:"activities_per_second"`
	P50MS               float64 `json:"p50_ms"`
	P99MS               float64 `json:"p99_ms"`
}

func summarize(rows []sample, elapsed time.Duration, concurrency int) summary {
	s := summary{Runs: len(rows), Concurrency: concurrency, ElapsedSeconds: elapsed.Seconds()}
	var latencies []float64
	for _, row := range rows {
		s.Activities += row.Activities
		if row.Error != "" {
			s.Errors++
			continue
		}
		s.Completed++
		latencies = append(latencies, row.LatencyMS)
	}
	slices.Sort(latencies)
	if len(latencies) > 0 {
		s.P50MS = latencies[int(math.Ceil(float64(len(latencies))*.5))-1]
		s.P99MS = latencies[int(math.Ceil(float64(len(latencies))*.99))-1]
	}
	if elapsed > 0 {
		s.RunsPerSecond = float64(s.Completed) / elapsed.Seconds()
		s.ActivitiesPerSecond = float64(s.Activities) / elapsed.Seconds()
	}
	return s
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	address := flag.String("address", "http://127.0.0.1:7301", "local server URL")
	queue := flag.String("queue", "evidence-load", "task queue with real SDK workers")
	n := flag.Int("n", 1000, "total workflows")
	concurrency := flag.Int("concurrency", 50, "maximum simultaneous runs (closed-loop)")
	timeout := flag.Duration("timeout", 3*time.Minute, "deadline for each run")
	samplesPath := flag.String("samples", "", "optional JSONL file, including every error")
	flag.Parse()
	if *n < 1 || *concurrency < 1 || *timeout <= 0 {
		return errors.New("n, concurrency and timeout must be positive")
	}
	parsed, err := url.Parse(*address)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Hostname() != "localhost" && !net.ParseIP(parsed.Hostname()).IsLoopback()) {
		return errors.New("address must be a local http:// URL without credentials or query")
	}
	key := os.Getenv("CAPSTAN_API_KEY")
	if key == "" {
		return errors.New("CAPSTAN_API_KEY is required")
	}
	auth := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+key)
			return next(ctx, req)
		}
	})
	transport := &http.Transport{MaxIdleConns: *concurrency * 2, MaxIdleConnsPerHost: *concurrency * 2, IdleConnTimeout: 30 * time.Second}
	defer transport.CloseIdleConnections()
	client := capstanv1connect.NewClientServiceClient(&http.Client{Transport: transport}, *address, connect.WithInterceptors(auth))
	id := make([]byte, 8)
	if _, err := rand.Read(id); err != nil {
		return err
	}
	prefix := "load-" + hex.EncodeToString(id)
	rows := make([]sample, *n)
	jobs := make(chan int)
	var wg sync.WaitGroup
	start := time.Now()
	for range min(*concurrency, *n) {
		wg.Go(func() {
			for index := range jobs {
				row := sample{RunID: fmt.Sprintf("%s-%06d", prefix, index)}
				began := time.Now()
				runCtx, cancel := context.WithTimeout(ctx, *timeout)
				err := execute(runCtx, client, row.RunID, *queue, *timeout)
				row.LatencyMS = float64(time.Since(began)) / float64(time.Millisecond)
				cancel()
				if err != nil {
					row.Error = err.Error()
				} else {
					row.Activities = 5
				}
				rows[index] = row
			}
		})
	}
	for i := range *n {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(start)
	// Every successful result is 5: the workflow returns the incremented value only
	// after five sequential activity completions. SQL audit in scripts/run-load.mjs
	// independently counts history events after the timed window.
	s := summarize(rows, elapsed, *concurrency)
	if *samplesPath != "" {
		file, err := os.Create(*samplesPath)
		if err != nil {
			return err
		}
		encoder := json.NewEncoder(file)
		for _, row := range rows {
			if err := encoder.Encode(row); err != nil {
				_ = file.Close()
				return err
			}
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(s); err != nil {
		return err
	}
	for _, row := range rows {
		if row.Error != "" {
			fmt.Fprintf(os.Stderr, "%s: %s\n", row.RunID, row.Error)
		}
	}
	if s.Errors > 0 {
		return fmt.Errorf("%d/%d runs errored; activity rate counts successful runs only (see SQL audit for partial runs)", s.Errors, s.Runs)
	}
	return nil
}
func execute(ctx context.Context, client capstanv1connect.ClientServiceClient, runID, queue string, timeout time.Duration) error {
	started, err := client.StartRun(ctx, connect.NewRequest(&v1.StartRunRequest{RunId: runID, WorkflowType: "loadFive", TaskQueue: queue, RunTimeout: durationpb.New(timeout.Truncate(time.Millisecond)), TaskTimeout: durationpb.New(60 * time.Second)}))
	if err != nil {
		return err
	}
	if !started.Msg.Started {
		return errors.New("run ID already existed")
	}
	for {
		response, err := client.AwaitRun(ctx, connect.NewRequest(&v1.AwaitRunRequest{RunId: runID}))
		if err != nil {
			return err
		}
		run := response.Msg.Run
		if run.GetStatus() == v1.RunStatus_RUN_STATUS_BLOCKED {
			return fmt.Errorf("run blocked: %s", run.GetFailure().GetMessage())
		}
		if !response.Msg.Closed {
			continue
		}
		if run.GetStatus() != v1.RunStatus_RUN_STATUS_COMPLETED {
			return fmt.Errorf("%s: %s", run.GetStatus(), run.GetFailure().GetMessage())
		}
		if string(run.GetResult().GetData()) != "5" {
			return fmt.Errorf("unexpected workflow result: %q", run.GetResult().GetData())
		}
		return nil
	}
}
