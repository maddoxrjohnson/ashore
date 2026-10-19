// Answering: JSON bodies, the error shape, and the map from store errors
// to status codes.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

// fail maps a store error to a status. Anything unexpected is a 500 with a
// generic body and the detail in the log.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrInvalidName), errors.Is(err, store.ErrInvalidKey), errors.Is(err, store.ErrReservedKey):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		s.log.Error("api internal error", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// errorBody is every non-2xx JSON response.
type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
