package monitor

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"time"
)

// DefaultRequestTimeout is how long a request of the HTTP API, or a call given to
// APIService.Timed, may take by default. The longest ones that are meant to be are those that
// go through many: a name filter or a payload search reads up to 100,000 workflows, batches or
// jobs, which takes a few seconds (half a second for 10,000 batches of 50 items). Proxies such as
// nginx give up after 60 seconds, so that a client gets the 503 of dureq before that.
const DefaultRequestTimeout = 30 * time.Second

// unavailableRetryAfter is the Retry-After, in seconds, of an answer that Redis did not answer in
// time, as of a WebSocket that is refused while the hub is stalled.
const unavailableRetryAfter = "5"

// unavailable is the error of a request or call that Redis did not answer in time.
func unavailable() *ApiError {
	return &ApiError{Msg: "Redis did not answer in time", StatusCode: http.StatusServiceUnavailable}
}

// SetRequestTimeout sets how long a call given to Timed may take; 0 or less is no limit. Default:
// DefaultRequestTimeout. It is called before the service is used.
func (a *APIService) SetRequestTimeout(d time.Duration) { a.requestTimeout = d }

// RequestTimeout returns how long a call given to Timed may take; 0 is no limit.
func (a *APIService) RequestTimeout() time.Duration { return max(a.requestTimeout, 0) }

// Timed calls f with ctx limited to the request timeout. If the time is up before f returns, as
// when Redis does not answer, it returns an *ApiError with 503, whatever f returned, the same as
// the HTTP API of dureqd answers: the answer of f may be wrong, as some of what it read failed.
// A server of your own answers it with Retry-After, as dureqd does.
func (a *APIService) Timed(ctx context.Context, f func(ctx context.Context) error) error {
	d := a.RequestTimeout()
	if d == 0 {
		return f(ctx)
	}
	timed, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	err := f(timed)
	if timedOut(ctx, timed) {
		return unavailable()
	}
	return err
}

// timedOut reports whether the time of timed, made from parent, is up. A read that rueidis gives
// up for it, such as a dial, can end a moment before timed says so itself, so that it goes by the
// clock, too.
func timedOut(parent, timed context.Context) bool {
	if parent.Err() != nil {
		return false
	}
	if errors.Is(timed.Err(), context.DeadlineExceeded) {
		return true
	}
	deadline, ok := timed.Deadline()
	return ok && !time.Now().Before(deadline)
}

// withTimeout gives a request no more than d, and answers 503 with Retry-After if the time is up
// before it is done. The answer is held back until then. A WebSocket is not limited.
func withTimeout(d time.Duration, next http.Handler) http.Handler {
	if d <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "" {
			next.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), d)
		defer cancel()
		buf := &bufferedResponse{header: make(http.Header), status: http.StatusOK}
		next.ServeHTTP(buf, r.WithContext(ctx))
		if timedOut(r.Context(), ctx) {
			w.Header().Set("Retry-After", unavailableRetryAfter)
			writeServiceError(w, unavailable())
			return
		}
		maps.Copy(w.Header(), buf.header)
		w.WriteHeader(buf.status)
		w.Write(buf.body.Bytes())
	})
}
