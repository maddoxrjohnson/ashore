// Package metrics is the daemon's Prometheus registry and the /metrics and
// /healthz handlers on the internal listener. The packages that produce
// the numbers (router, supervisor, logs) each define a small observer
// interface; *Metrics satisfies all of them, so none of them imports the
// Prometheus client.
package metrics

import (
	"net/http"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Deploy stages the duration histogram is labelled with.
const (
	StageBuild  = "build"
	StageStart  = "start"
	StageHealth = "health"
	StageSwap   = "swap"
)

// Deploy results.
const (
	ResultLive   = "live"
	ResultFailed = "failed"
)

// Instance states ashore_instances is labelled with, the supervisor's.
var instanceStates = []string{"running", "starting", "restarting", "crashed", "stopped"}

// Metrics holds every series the daemon exports. Create it with New.
type Metrics struct {
	reg   *prometheus.Registry
	ready atomic.Bool

	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	upstreamErrors *prometheus.CounterVec
	deploys        *prometheus.CounterVec
	deployDuration *prometheus.HistogramVec
	instances      *prometheus.GaugeVec
	restarts       *prometheus.CounterVec
	logLines       *prometheus.CounterVec
	logDropped     *prometheus.CounterVec
}

// New registers the series. version is the daemon's, rt the configured
// runtime name; both end up as labels on ashore_build_info.
func New(version, rt string) *Metrics {
	m := &Metrics{reg: prometheus.NewRegistry()}
	f := promauto{reg: m.reg}
	m.httpRequests = f.counter("ashore_http_requests_total", "Requests the router proxied, by app and response status.", "app", "status")
	m.httpDuration = f.histogram("ashore_http_request_duration_seconds", "Time from a request's arrival at the router to its last byte.", prometheus.DefBuckets, "app")
	m.upstreamErrors = f.counter("ashore_http_upstream_errors_total", "Requests the app's instance could not answer (502s).", "app")
	m.deploys = f.counter("ashore_deploys_total", "Deploys by app and result.", "app", "result")
	m.deployDuration = f.histogram("ashore_deploy_duration_seconds", "Time spent in each deploy stage.",
		[]float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}, "stage")
	m.instances = f.gauge("ashore_instances", "Instances by app and state; one state per app is 1.", "app", "status")
	m.restarts = f.counter("ashore_instance_restarts_total", "Automatic restarts after an instance exited on its own.", "app")
	m.logLines = f.counter("ashore_log_lines_total", "Log lines collected.", "app")
	m.logDropped = f.counter("ashore_log_lines_dropped_total", "Log lines a slow subscriber did not get.", "app")
	info := f.gauge("ashore_build_info", "The running daemon; always 1.", "version", "go", "runtime")
	info.WithLabelValues(version, runtime.Version(), rt).Set(1)
	m.reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in the exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// SetReady marks the daemon as serving: every listener is open and the
// store is in use. Until then /healthz answers 503.
func (m *Metrics) SetReady(ready bool) { m.ready.Store(ready) }

// Healthz answers 200 "ok" once ready, 503 before.
func (m *Metrics) Healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !m.ready.Load() {
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

// Request implements router.Observer.
func (m *Metrics) Request(app string, status int, d time.Duration) {
	m.httpRequests.WithLabelValues(app, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(app).Observe(d.Seconds())
}

// UpstreamError implements router.Observer.
func (m *Metrics) UpstreamError(app string) { m.upstreamErrors.WithLabelValues(app).Inc() }

// Deploy implements supervisor.Metrics.
func (m *Metrics) Deploy(app, result string) { m.deploys.WithLabelValues(app, result).Inc() }

// Stage implements supervisor.Metrics.
func (m *Metrics) Stage(stage string, d time.Duration) {
	m.deployDuration.WithLabelValues(stage).Observe(d.Seconds())
}

// InstanceState implements supervisor.Metrics: the app's gauge for state
// becomes 1 and every other state's 0, so a sum over apps counts them.
func (m *Metrics) InstanceState(app, state string) {
	for _, st := range instanceStates {
		v := 0.0
		if st == state {
			v = 1
		}
		m.instances.WithLabelValues(app, st).Set(v)
	}
}

// InstanceGone implements supervisor.Metrics: the app no longer exists.
func (m *Metrics) InstanceGone(app string) {
	m.instances.DeletePartialMatch(prometheus.Labels{"app": app})
}

// Restart implements supervisor.Metrics.
func (m *Metrics) Restart(app string) { m.restarts.WithLabelValues(app).Inc() }

// Line implements logs.Observer.
func (m *Metrics) Line(app string) { m.logLines.WithLabelValues(app).Inc() }

// Dropped implements logs.Observer.
func (m *Metrics) Dropped(app string, n int) { m.logDropped.WithLabelValues(app).Add(float64(n)) }

// promauto is the little factory the constructor uses: make and register.
type promauto struct{ reg *prometheus.Registry }

func (f promauto) counter(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(c)
	return c
}

func (f promauto) gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
	f.reg.MustRegister(g)
	return g
}

func (f promauto) histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
	f.reg.MustRegister(h)
	return h
}
