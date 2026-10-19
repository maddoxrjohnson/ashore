package router

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLimiter(t *testing.T) {
	l := newLimiter(2, 4) // 2/s, burst 4
	now := time.Unix(1000, 0)
	for i := range 4 {
		if !l.allow("a", now) {
			t.Fatalf("request %d of the burst refused", i+1)
		}
	}
	if l.allow("a", now) {
		t.Fatal("fifth request in the same instant allowed")
	}
	if !l.allow("b", now) {
		t.Fatal("another client should have its own bucket")
	}
	if !l.allow("a", now.Add(500*time.Millisecond)) {
		t.Fatal("half a second refills one token at 2/s")
	}
	if l.allow("a", now.Add(500*time.Millisecond)) {
		t.Fatal("and only one")
	}
	later := now.Add(time.Hour)
	for i := range 4 {
		if !l.allow("a", later) {
			t.Fatalf("an idle bucket refills to the burst: request %d refused", i+1)
		}
	}
	if l.allow("a", later) {
		t.Fatal("burst exceeded: an idle bucket refills to the burst, not beyond")
	}
}

func TestLimiterSweepsIdleBuckets(t *testing.T) {
	l := newLimiter(1, 1)
	now := time.Unix(1000, 0)
	for i := range maxBuckets {
		l.allow("ip"+string(rune(i)), now)
	}
	if len(l.buckets) != maxBuckets {
		t.Fatalf("%d buckets", len(l.buckets))
	}
	l.allow("late", now.Add(time.Second)) // full map: the idle ones go
	if len(l.buckets) != 1 {
		t.Fatalf("%d buckets after the sweep, want 1", len(l.buckets))
	}
}

func TestRateLimitAnswers429(t *testing.T) {
	r, srv, _ := newRouter(t)
	r.SetRateLimit(2) // burst 4
	a := newBackend(t, "a")
	r.Set("hello.localhost", "hello", addrOf(a))
	codes := make([]int, 0, 6)
	for range 6 {
		resp := get(t, srv, "hello.localhost", nil)
		_ = resp.Body.Close()
		codes = append(codes, resp.StatusCode)
		if resp.StatusCode == http.StatusTooManyRequests && resp.Header.Get("Retry-After") != "1" {
			t.Errorf("429 without Retry-After")
		}
	}
	// Four get through at once; the rest are refused until the bucket refills.
	want := []int{200, 200, 200, 200, 429, 429}
	for i := range want {
		if codes[i] != want[i] {
			t.Fatalf("codes %v, want %v", codes, want)
		}
	}
	r.SetRateLimit(0)
	if resp := get(t, srv, "hello.localhost", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("limit off: %d", resp.StatusCode)
	}
}

func TestHTTPRedirectsToHTTPSWhenTLSIsOn(t *testing.T) {
	r := New(Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	r.EnableTLS(TLSConfig{Email: "ops@example.com", CacheDir: t.TempDir()})
	srv := httptest.NewServer(r.httpHandler())
	t.Cleanup(srv.Close)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/path?q=1", strings.NewReader("x"))
	req.Host = "hello.example.com:80"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusPermanentRedirect || resp.Header.Get("Location") != "https://hello.example.com/path?q=1" {
		t.Fatalf("redirect: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}

	// The redirect is rate limited like everything else.
	r.SetRateLimit(1) // burst 2; one request is spent above
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/", nil)
	req.Host = "hello.example.com"
	var last int
	for range 3 {
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		last = resp.StatusCode
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("third redirect request: %d, want 429", last)
	}
	r.SetRateLimit(0)

	// The ACME challenge path reaches the certificate manager instead.
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/.well-known/acme-challenge/token", nil)
	req.Host = "hello.example.com"
	resp, err = client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 == 3 {
		t.Fatalf("challenge path was redirected: %d", resp.StatusCode)
	}

	// Only routed hosts get certificates.
	r.Set("hello.example.com", "hello", "127.0.0.1:1")
	if err := r.hostPolicy(context.Background(), "hello.example.com"); err != nil {
		t.Errorf("routed host refused: %v", err)
	}
	if err := r.hostPolicy(context.Background(), "evil.example.com"); err == nil {
		t.Error("unrouted host allowed a certificate")
	}
	if r.tlsConf.MinVersion != tls.VersionTLS12 {
		t.Error("TLS 1.0 and 1.1 should be off")
	}
}

func TestServeTLS(t *testing.T) {
	r := New(Options{Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := r.ServeTLS(context.Background(), nil); err == nil {
		t.Fatal("ServeTLS without EnableTLS should fail")
	}
	cert := selfSigned(t)
	r.tlsConf = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	a := newBackend(t, "secure")
	r.Set("hello.localhost", "hello", addrOf(a))

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.ServeTLS(ctx, ln) }()

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // a test against a self-signed certificate
	req, _ := http.NewRequest(http.MethodGet, "https://"+ln.Addr().String()+"/", nil)
	req.Host = "hello.localhost"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if b := body(t, resp); b != "secure" || resp.Header.Get("Echo-Proto") != "https" {
		t.Fatalf("over TLS: %q proto %q", b, resp.Header.Get("Echo-Proto"))
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// selfSigned makes a certificate for hello.localhost, valid for an hour.
func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hello.localhost"},
		DNSNames:     []string{"hello.localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
