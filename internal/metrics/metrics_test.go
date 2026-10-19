package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

func TestSeries(t *testing.T) {
	m := New("v0.1.0", "docker")
	m.Request("hello", 200, 30*time.Millisecond)
	m.Request("hello", 200, 40*time.Millisecond)
	m.Request("hello", 502, time.Second)
	m.UpstreamError("hello")
	m.Deploy("hello", supervisor.ResultLive)
	m.Deploy("hello", supervisor.ResultFailed)
	m.Stage(supervisor.StageBuild, 12*time.Second)
	m.Stage(supervisor.StageHealth, 800*time.Millisecond)
	m.InstanceState("hello", "running")
	m.InstanceState("hello", "crashed")
	m.Restart("hello")
	m.Line("hello")
	m.Line("hello")
	m.Dropped("hello", 5)

	body := scrape(t, m)
	for _, want := range []string{
		`ashore_http_requests_total{app="hello",status="200"} 2`,
		`ashore_http_requests_total{app="hello",status="502"} 1`,
		`ashore_http_request_duration_seconds_count{app="hello"} 3`,
		`ashore_http_request_duration_seconds_bucket{app="hello",le="0.05"} 2`,
		`ashore_http_upstream_errors_total{app="hello"} 1`,
		`ashore_deploys_total{app="hello",result="live"} 1`,
		`ashore_deploys_total{app="hello",result="failed"} 1`,
		`ashore_deploy_duration_seconds_bucket{stage="build",le="30"} 1`,
		`ashore_deploy_duration_seconds_bucket{stage="build",le="10"} 0`,
		`ashore_deploy_duration_seconds_count{stage="health"} 1`,
		`ashore_instances{app="hello",status="crashed"} 1`,
		`ashore_instances{app="hello",status="running"} 0`,
		`ashore_instance_restarts_total{app="hello"} 1`,
		`ashore_log_lines_total{app="hello"} 2`,
		`ashore_log_lines_dropped_total{app="hello"} 5`,
		`ashore_build_info{go="go`, `",runtime="docker",version="v0.1.0"} 1`,
		`go_goroutines `, `process_open_fds `,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q", want)
		}
	}
	m.InstanceGone("hello")
	if body := scrape(t, m); strings.Contains(body, `ashore_instances{app="hello"`) {
		t.Error("instance series still present after InstanceGone")
	}
}

func TestHealthz(t *testing.T) {
	m := New("dev", "docker")
	rec := httptest.NewRecorder()
	m.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("before ready: %d", rec.Code)
	}
	m.SetReady(true)
	rec = httptest.NewRecorder()
	m.Healthz(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != "ok\n" {
		t.Fatalf("ready: %d %q", rec.Code, rec.Body.String())
	}
}
