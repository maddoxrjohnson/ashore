package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/logs"
)

const (
	// DefaultTail is how many recent lines a logs request gets unless it
	// says otherwise; MaxTail is the most it can ask for, the ring's size.
	DefaultTail = 100
	MaxTail     = logs.RingSize

	// eventLine carries one JSON-encoded logs.Line.
	eventLine = "line"
	// keepalive is how often a quiet follow stream sends a comment, so a
	// proxy or a client with an idle timeout keeps the connection.
	keepalive = 30 * time.Second
)

// appLogs answers GET /api/v1/apps/{app}/logs?tail=N&follow=1 as an event
// stream: one "line" event per logs.Line, then "done" unless follow is set,
// in which case lines keep coming until the client goes away or the app is
// destroyed. The tail comes from the in-memory ring, so it is bounded by
// MaxTail.
func (s *Server) appLogs(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	if s.logs == nil {
		writeError(w, http.StatusServiceUnavailable, "log collection is not enabled")
		return
	}
	tail := DefaultTail
	if v := r.URL.Query().Get("tail"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "tail must be a non-negative number")
			return
		}
		tail = min(n, MaxTail)
	}
	follow := r.URL.Query().Get("follow") == "1"

	st, err := newStream(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !follow {
		for _, l := range s.logs.Tail(app.Name, tail) {
			st.json(eventLine, l)
		}
		st.done()
		return
	}
	ch, cancel := s.logs.Subscribe(app.Name, tail)
	defer cancel()
	tick := time.NewTicker(keepalive)
	defer tick.Stop()
	for {
		select {
		case l, ok := <-ch:
			if !ok { // the app was destroyed
				st.done()
				return
			}
			st.json(eventLine, l)
		case <-tick.C:
			st.comment("keepalive")
		case <-r.Context().Done():
			return
		}
	}
}

// json sends one event whose data is v encoded as JSON.
func (st *stream) json(name string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.event(name, string(b))
}

// comment sends a line the client ignores, to keep the connection warm.
func (st *stream) comment(text string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, err := st.w.Write([]byte(": " + text + "\n\n")); err != nil {
		return
	}
	_ = st.rc.Flush()
}
