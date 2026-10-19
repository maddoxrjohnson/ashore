// Package site is how the daemon's apps are addressed: by a hostname under
// the configured domain, through the router's HTTP or HTTPS port, and over
// SSH for pushes. Every URL the daemon prints or renders comes from here,
// so the push output, the API, and the dashboard agree.
package site

import (
	"net"
	"strconv"
)

// Site describes one daemon's addresses.
type Site struct {
	Domain    string // apps are <name>.<Domain>
	HTTPPort  int    // the router's plain port; 0 or 80 is left out of URLs
	HTTPSPort int    // the router's TLS port; 0 means TLS is off, 443 is left out of URLs
	SSHAddr   string // the git listener, as it was told to listen
}

// AppHost is the hostname the router matches for app.
func (s Site) AppHost(app string) string { return app + "." + s.Domain }

// AppURL is where an app is reachable through the router: https when TLS
// is on, with the port only when it is not the scheme's default.
func (s Site) AppURL(host string) string {
	if s.HTTPSPort > 0 {
		if s.HTTPSPort == 443 {
			return "https://" + host
		}
		return "https://" + net.JoinHostPort(host, strconv.Itoa(s.HTTPSPort))
	}
	if s.HTTPPort == 0 || s.HTTPPort == 80 {
		return "http://" + host
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(s.HTTPPort))
}

// RemoteURL is the git remote a pusher adds for app: the SSH listener's
// host and port, or the domain when it listens on every interface.
func (s Site) RemoteURL(app string) string {
	host, port, err := net.SplitHostPort(s.SSHAddr)
	if err != nil {
		return "ssh://ashore@" + s.Domain + "/" + app
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = s.Domain
	}
	return "ssh://ashore@" + net.JoinHostPort(host, port) + "/" + app
}

// PortOf returns the port of a listen address such as ":8080" or
// "127.0.0.1:9000", 0 if it has none.
func PortOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}
