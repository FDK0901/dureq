package monitor

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"net/http"
)

// revalidated are the GETs whose answers a browser may keep, and ask each time whether they
// have changed. They hold nothing of what jobs carry: no payloads, results, progress or errors of
// handlers, which may be personal data.
var revalidated = map[string]bool{
	"GET /api/stats":                                true,
	"GET /api/stats/daily":                          true,
	"GET /api/nodes":                                true,
	"GET /api/nodes/{nodeID}":                       true,
	"GET /api/nodes/{nodeID}/drain":                 true,
	"GET /api/queues":                               true,
	"GET /api/schedules":                            true,
	"GET /api/schedules/{jobID}":                    true,
	"GET /api/workflows/{workflowID}/signals/stats": true,
	"GET /api/redis/info":                           true,
	"GET /api/sync-retries":                         true,
	"GET /api/health":                               true,
}

// withETag answers 304 to a GET whose response the client has already, and saves sending it,
// if the response is one that may be kept (revalidated). Others a browser is not to keep at all.
func withETag(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Upgrade") != "" {
			mux.ServeHTTP(w, r)
			return
		}
		if _, pattern := mux.Handler(r); r.Method != http.MethodGet || !revalidated[pattern] {
			w.Header().Set("Cache-Control", "private, no-store")
			mux.ServeHTTP(w, r)
			return
		}
		buf := &bufferedResponse{header: w.Header(), status: http.StatusOK}
		mux.ServeHTTP(buf, r)
		if buf.status == http.StatusOK {
			h := fnv.New64a()
			h.Write(buf.body.Bytes())
			etag := fmt.Sprintf(`W/"%x"`, h.Sum64())
			w.Header().Set("ETag", etag)
			w.Header().Set("Cache-Control", "no-cache") // ask each time
			if r.Header.Get("If-None-Match") == etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.WriteHeader(buf.status)
		w.Write(buf.body.Bytes())
	})
}

// bufferedResponse holds a response back until it is known.
type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header         { return b.header }
func (b *bufferedResponse) WriteHeader(status int)      { b.status = status }
func (b *bufferedResponse) Write(p []byte) (int, error) { return b.body.Write(p) }
