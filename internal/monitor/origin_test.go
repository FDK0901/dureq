package monitor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestOrigins(t *testing.T) {
	st, _ := newTestAPI(t, nil)
	serve := func(allowed ...string) http.Handler {
		api := NewAPI(st, nil, nil)
		t.Cleanup(api.Shutdown)
		if err := api.AllowOrigins(allowed); err != nil {
			t.Fatal(err)
		}
		return api.Handler()
	}
	none, some, all := serve(), serve("https://ui.example.com/"), serve("*")
	for _, tc := range []struct {
		name    string
		h       http.Handler
		method  string
		path    string
		headers map[string]string
		want    int
		cors    string
	}{
		{"not a browser", none, "GET", "/api/health", nil, 200, ""},
		{"not a browser, changing", none, "POST", "/api/queues/normal/pause", nil, 200, ""},
		{"same origin", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "http://api.example.com"}, 200, ""},
		{"same origin, as the browser says", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://proxy.example.com", "Sec-Fetch-Site": "same-origin"}, 200, ""},
		{"same origin behind a proxy", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://proxy.example.com", "X-Forwarded-Host": "proxy.example.com, inner"}, 200, ""},
		{"other origin", none, "GET", "/api/health", map[string]string{"Origin": "https://evil.example.com"}, 403, ""},
		{"other origin, changing", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://evil.example.com"}, 403, ""},
		{"other origin, as the browser says", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "http://api.example.com", "Sec-Fetch-Site": "cross-site"}, 403, ""},
		{"other origin, behind a proxy", none, "DELETE", "/api/jobs/x", map[string]string{"Origin": "https://evil.example.com", "X-Forwarded-Host": "proxy.example.com"}, 403, ""},
		{"form without an origin", none, "POST", "/api/queues/normal/pause", map[string]string{"Sec-Fetch-Site": "cross-site"}, 403, ""},
		{"opaque origin", none, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "null"}, 403, ""},
		{"link from elsewhere", none, "GET", "/api/health", map[string]string{"Sec-Fetch-Site": "cross-site"}, 200, ""},
		{"websocket of another origin", none, "GET", "/api/ws", map[string]string{"Origin": "https://evil.example.com", "Upgrade": "websocket", "Connection": "Upgrade"}, 403, ""},
		{"preflight of another origin", none, "OPTIONS", "/api/jobs/x", map[string]string{"Origin": "https://evil.example.com", "Access-Control-Request-Method": "DELETE"}, 204, ""},
		{"allowed", some, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://ui.example.com", "Sec-Fetch-Site": "cross-site"}, 200, "https://ui.example.com"},
		{"preflight of one that is allowed", some, "OPTIONS", "/api/jobs/x", map[string]string{"Origin": "https://ui.example.com"}, 204, "https://ui.example.com"},
		{"not allowed", some, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://evil.example.com"}, 403, ""},
		{"same name, other scheme", some, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "http://ui.example.com"}, 403, ""},
		{"all allowed", all, "POST", "/api/queues/normal/pause", map[string]string{"Origin": "https://evil.example.com"}, 200, "https://evil.example.com"},
	} {
		r := httptest.NewRequest(tc.method, "http://api.example.com"+tc.path, nil)
		for k, v := range tc.headers {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		tc.h.ServeHTTP(w, r)
		if w.Code != tc.want || w.Header().Get("Access-Control-Allow-Origin") != tc.cors {
			t.Errorf("%s: %d, allowed origin %q, want %d, %q", tc.name, w.Code, w.Header().Get("Access-Control-Allow-Origin"), tc.want, tc.cors)
		}
	}
	for _, bad := range []string{"ui.example.com", "https://ui.example.com/path", ""} {
		if _, err := newOriginGuard([]string{bad}); err == nil {
			t.Errorf("origin %q is accepted", bad)
		}
	}
}
