package server

import (
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"connectrpc.com/connect"
	v1 "github.com/Aly700/capstan/gen/capstan/v1"
)

type rpcMetric struct {
	codes   [17]uint64 // OK (0) and the sixteen Connect codes.
	seconds float64
	count   uint64
}
type metrics struct {
	mu         sync.Mutex
	procedures []string
	rpc        map[string]*rpcMetric
	loops      map[string]uint64
	waiting    map[string]int64
}

func newMetrics() *metrics {
	m := &metrics{rpc: make(map[string]*rpcMetric), loops: map[string]uint64{"timers": 0, "tasks": 0, "approvals": 0, "run_timeouts": 0}, waiting: map[string]int64{"workflow": 0, "activity": 0, "run": 0}}
	services := v1.File_capstan_v1_capstan_proto.Services()
	for i := 0; i < services.Len(); i++ {
		service := services.Get(i)
		methods := service.Methods()
		for j := 0; j < methods.Len(); j++ {
			procedure := "/" + string(service.FullName()) + "/" + string(methods.Get(j).Name())
			m.procedures = append(m.procedures, procedure)
			m.rpc[procedure] = &rpcMetric{}
		}
	}
	slices.Sort(m.procedures)
	return m
}
func (m *metrics) recordRPC(procedure string, err error, elapsed time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rpc[procedure]
	if !ok {
		return
	}
	code := connect.Code(0)
	if err != nil {
		code = connect.CodeOf(err)
	}
	if int(code) < 0 || int(code) >= len(r.codes) {
		code = connect.CodeUnknown
	}
	r.codes[code]++
	r.count++
	r.seconds += elapsed.Seconds()
}
func (m *metrics) loopItems(name string, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.loops[name]; ok && count > 0 {
		m.loops[name] += uint64(count)
	}
}
func (m *metrics) pollWaiting(kind string, delta int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.waiting[kind]; ok {
		m.waiting[kind] += delta
	}
}
func (m *metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Render a bounded snapshot before writing to the network: slow scrapers must
	// not hold the lock that RPCs and loops use to update counters.
	m.mu.Lock()
	var out []byte
	out = fmt.Append(out, "# HELP capstan_rpc_total Completed RPCs by procedure and code.\n# TYPE capstan_rpc_total counter\n")
	for _, p := range m.procedures {
		for code, count := range m.rpc[p].codes {
			if count == 0 {
				continue
			}
			name := "ok"
			if code != 0 {
				name = connect.Code(code).String()
			}
			out = fmt.Appendf(out, "capstan_rpc_total{procedure=%q,code=%q} %d\n", p, name, count)
		}
	}
	out = fmt.Append(out, "# HELP capstan_rpc_seconds RPC elapsed seconds.\n# TYPE capstan_rpc_seconds summary\n")
	for _, p := range m.procedures {
		r := m.rpc[p]
		out = fmt.Appendf(out, "capstan_rpc_seconds_sum{procedure=%q} %g\ncapstan_rpc_seconds_count{procedure=%q} %d\n", p, r.seconds, p, r.count)
	}
	out = fmt.Append(out, "# HELP capstan_loop_items_total Items processed by maintenance loops.\n# TYPE capstan_loop_items_total counter\n")
	for _, name := range []string{"timers", "tasks", "approvals", "run_timeouts"} {
		out = fmt.Appendf(out, "capstan_loop_items_total{loop=%q} %d\n", name, m.loops[name])
	}
	out = fmt.Append(out, "# HELP capstan_long_poll_waiting Long polls currently waiting for work or closure.\n# TYPE capstan_long_poll_waiting gauge\n")
	for _, kind := range []string{"workflow", "activity", "run"} {
		out = fmt.Appendf(out, "capstan_long_poll_waiting{kind=%q} %d\n", kind, m.waiting[kind])
	}
	m.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(out)
}
