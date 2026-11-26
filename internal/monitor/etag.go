package monitor

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"net/http"
)

// withETag answers 304 to a GET whose response the client has already, and saves sending it.
func withETag(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Upgrade") != "" {
			next.ServeHTTP(w, r)
			return
		}
		buf := &bufferedResponse{header: w.Header(), status: http.StatusOK}
		next.ServeHTTP(buf, r)
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
