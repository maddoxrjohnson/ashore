package router

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"golang.org/x/crypto/acme/autocert"
)

// TLSConfig turns HTTPS on: certificates come from Let's Encrypt through
// the ACME HTTP-01 challenge, one per app hostname, issued on the first
// request for that host and cached under CacheDir.
type TLSConfig struct {
	Email    string // the ACME account contact
	CacheDir string // where certificates and the account key are kept
}

// EnableTLS prepares the certificate manager. After it, Serve (the plain
// HTTP listener) answers ACME challenges and redirects everything else to
// https, and ServeTLS serves the apps. Call it before either Serve.
func (r *Router) EnableTLS(cfg TLSConfig) {
	m := &autocert.Manager{
		Prompt:     autocert.AcceptTOS,
		Cache:      autocert.DirCache(cfg.CacheDir),
		HostPolicy: r.hostPolicy,
		Email:      cfg.Email,
	}
	r.acme = m
	r.tlsConf = m.TLSConfig()
	r.tlsConf.MinVersion = tls.VersionTLS12
}

// hostPolicy allows a certificate only for a host the table routes, so a
// client connecting by IP and naming a random host cannot make the daemon
// ask the CA for it (and burn through the CA's rate limit).
func (r *Router) hostPolicy(_ context.Context, host string) error {
	if _, ok := r.Get(host); ok {
		return nil
	}
	return fmt.Errorf("no app at %s", host)
}

// httpHandler is what the plain listener serves: the router itself, or,
// with TLS on, the challenge responder with a redirect for everything
// else. The rate limit applies to redirects too; a flood of plain-HTTP
// requests must not be cheaper than a flood of HTTPS ones.
func (r *Router) httpHandler() http.Handler {
	if r.acme == nil {
		return r
	}
	h := r.acme.HTTPHandler(http.HandlerFunc(r.redirectToHTTPS))
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if r.limited(w, req) {
			return
		}
		h.ServeHTTP(w, req)
	})
}

// redirectToHTTPS sends a plain-HTTP request to the same host and path
// over https. 308 keeps the method, so a POST is not turned into a GET.
func (r *Router) redirectToHTTPS(w http.ResponseWriter, req *http.Request) {
	host := hostOf(req.Host)
	if host == "" {
		http.Error(w, "missing Host header", http.StatusBadRequest)
		return
	}
	target := "https://" + host + req.URL.RequestURI()
	http.Redirect(w, req, target, http.StatusPermanentRedirect)
}

// ServeTLS runs the HTTPS server on ln until ctx is done. EnableTLS must
// have been called.
func (r *Router) ServeTLS(ctx context.Context, ln net.Listener) error {
	if r.tlsConf == nil {
		return fmt.Errorf("https serve: TLS is not enabled")
	}
	return r.serve(ctx, tls.NewListener(ln, r.tlsConf), r, "https")
}
