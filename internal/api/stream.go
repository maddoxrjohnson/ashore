package api

import "errors"

// ErrStream reports a stream that ended without a verdict: the connection
// dropped, or the daemon went away mid-operation.
var ErrStream = errors.New("stream ended without a result")
