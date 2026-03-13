package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

// HTTPWebhookPayload is the input for the built-in HTTP webhook handler.
type HTTPWebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method,omitempty"` // default: POST
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
	Timeout int               `json:"timeout_seconds,omitempty"` // default: 30
}

// HTTPWebhookResult is the output of the webhook handler.
type HTTPWebhookResult struct {
	StatusCode int             `json:"status_code"`
	Body       json.RawMessage `json:"body,omitempty"`
}

func init() {
	Register(types.HandlerDefinition{
		TaskType:    "http_webhook",
		Concurrency: 20,
		Timeout:     60 * time.Second,
		HandlerWithResult: types.TypedHandlerWithResult(func(ctx context.Context, p HTTPWebhookPayload) (HTTPWebhookResult, error) {
			method := p.Method
			if method == "" {
				method = http.MethodPost
			}
			timeout := 30 * time.Second
			if p.Timeout > 0 {
				timeout = time.Duration(p.Timeout) * time.Second
			}

			var bodyReader io.Reader
			if len(p.Body) > 0 {
				bodyReader = bytes.NewReader(p.Body)
			}

			req, err := http.NewRequestWithContext(ctx, method, p.URL, bodyReader)
			if err != nil {
				return HTTPWebhookResult{}, &types.NonRetryableError{Err: fmt.Errorf("build request: %w", err)}
			}
			if bodyReader != nil {
				req.Header.Set("Content-Type", "application/json")
			}
			for k, v := range p.Headers {
				req.Header.Set(k, v)
			}

			client := &http.Client{Timeout: timeout}
			resp, err := client.Do(req)
			if err != nil {
				return HTTPWebhookResult{}, fmt.Errorf("http request: %w", err)
			}
			defer resp.Body.Close()

			respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit

			result := HTTPWebhookResult{
				StatusCode: resp.StatusCode,
			}
			if json.Valid(respBody) {
				result.Body = respBody
			}

			if resp.StatusCode >= 500 {
				return result, fmt.Errorf("webhook returned %d", resp.StatusCode)
			}
			if resp.StatusCode >= 400 {
				return result, &types.NonRetryableError{Err: fmt.Errorf("webhook returned %d", resp.StatusCode)}
			}

			return result, nil
		}),
	})
}
