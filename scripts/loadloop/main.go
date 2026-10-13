// loadloop sends a steady stream of requests through the router and counts
// what came back, to measure what a deploy does to live traffic. It runs
// until -duration passes or it gets SIGINT, then prints one summary line
// and exits non-zero if any request failed.
//
//	go run ./scripts/loadloop -host slowstart.localhost -rate 200
//
// Requests go to -addr with the Host header set to -host, so no DNS is
// involved. Every distinct response body is counted, which shows how many
// releases answered during the run.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "router address")
	host := flag.String("host", "hello.localhost", "Host header")
	path := flag.String("path", "/", "request path")
	rate := flag.Int("rate", 200, "requests per second")
	duration := flag.Duration("duration", 0, "how long to run; 0 means until SIGINT")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	url := "http://" + *addr + *path
	var st stats
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	defer tick.Stop()
	report := time.NewTicker(time.Second)
	defer report.Stop()
	start := time.Now()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-report.C:
			fmt.Fprintln(os.Stderr, st.line(time.Since(start)))
		case <-tick.C:
			wg.Add(1)
			go func() {
				defer wg.Done()
				st.record(one(client, url, *host))
			}()
		}
	}
	wg.Wait()
	fmt.Println(st.line(time.Since(start)))
	if st.failed() > 0 {
		os.Exit(1)
	}
}

// result is one request's outcome: a status, or an error with no status.
type result struct {
	status  int
	err     string
	body    string
	latency time.Duration
}

func one(client *http.Client, url, host string) result {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return result{err: err.Error()}
	}
	req.Host = host
	t := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return result{err: trimErr(err), latency: time.Since(t)}
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return result{status: resp.StatusCode, body: strings.TrimSpace(string(body)), latency: time.Since(t)}
}

// trimErr drops the URL prefix Go puts on client errors, so equal failures
// count together.
func trimErr(err error) string {
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		return s[i+2:]
	}
	return s
}

type stats struct {
	mu        sync.Mutex
	total     int
	ok        int
	statuses  map[int]int    // non-2xx
	errors    map[string]int // transport errors
	bodies    map[string]int
	latencies []time.Duration
}

func (s *stats) record(r result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.statuses == nil {
		s.statuses, s.errors, s.bodies = map[int]int{}, map[string]int{}, map[string]int{}
	}
	s.total++
	s.latencies = append(s.latencies, r.latency)
	switch {
	case r.err != "":
		s.errors[r.err]++
	case r.status/100 == 2:
		s.ok++
		s.bodies[r.body]++
	default:
		s.statuses[r.status]++
	}
}

func (s *stats) failed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total - s.ok
}

func (s *stats) line(elapsed time.Duration) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	lat := append([]time.Duration(nil), s.latencies...)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		return lat[min(len(lat)-1, int(float64(len(lat))*p))]
	}
	var problems []string
	for code, n := range s.statuses {
		problems = append(problems, fmt.Sprintf("%d x%d", code, n))
	}
	for msg, n := range s.errors {
		problems = append(problems, fmt.Sprintf("%q x%d", msg, n))
	}
	sort.Strings(problems)
	detail := "none"
	if len(problems) > 0 {
		detail = strings.Join(problems, ", ")
	}
	return fmt.Sprintf("%6.1fs requests=%d ok=%d failed=%d (%s) distinct bodies=%d p50=%s p99=%s max=%s",
		elapsed.Seconds(), s.total, s.ok, s.total-s.ok, detail, len(s.bodies), pct(0.5), pct(0.99), pct(1))
}
