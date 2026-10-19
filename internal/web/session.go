// The session: a token login that mints a cookie, and the check every
// page runs first.
package web

import (
	"crypto/rand"
	"crypto/subtle"
	"net/http"
	"strings"
	"time"
)

// withSession sends a visitor without a valid session to the login page.
func (s *Server) withSession(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.loggedIn(r) {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		h(w, r)
	}
}

func (s *Server) loggedIn(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.sessions[c.Value]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(s.sessions, c.Value)
		return false
	}
	return true
}

// login checks the token in constant time and starts a session. Expired
// sessions are swept here, the one place the map grows.
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	presented := strings.TrimSpace(r.FormValue("token"))
	if len(s.token) == 0 || subtle.ConstantTimeCompare([]byte(presented), s.token) != 1 {
		s.log.Warn("dashboard login refused", "remote", r.RemoteAddr)
		s.render(w, http.StatusUnauthorized, "login", map[string]any{"Error": "wrong token"})
		return
	}
	id := rand.Text()
	now := time.Now()
	s.mu.Lock()
	for k, exp := range s.sessions {
		if now.After(exp) {
			delete(s.sessions, k)
		}
	}
	s.sessions[id] = now.Add(sessionTTL)
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: id, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil, MaxAge: int(sessionTTL / time.Second)})
	s.log.Info("dashboard login", "remote", r.RemoteAddr)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(cookieName); err == nil {
		s.mu.Lock()
		delete(s.sessions, c.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) loginPage(w http.ResponseWriter, r *http.Request) {
	if s.loggedIn(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	s.render(w, http.StatusOK, "login", nil)
}
