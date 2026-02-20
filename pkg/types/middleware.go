package types

import (
	"context"
	"encoding/json"
)

// MiddlewareFunc wraps a HandlerFunc, allowing pre/post processing.
type MiddlewareFunc func(HandlerFunc) HandlerFunc

// MiddlewareWithResultFunc wraps a HandlerFuncWithResult.
type MiddlewareWithResultFunc func(HandlerFuncWithResult) HandlerFuncWithResult

// ChainMiddleware applies middlewares to a HandlerFunc in order.
// The first middleware in the list is the outermost (executed first).
func ChainMiddleware(handler HandlerFunc, middlewares ...MiddlewareFunc) HandlerFunc {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// ChainMiddlewareWithResult applies middlewares to a HandlerFuncWithResult.
func ChainMiddlewareWithResult(handler HandlerFuncWithResult, middlewares ...MiddlewareWithResultFunc) HandlerFuncWithResult {
	for i := len(middlewares) - 1; i >= 0; i-- {
		handler = middlewares[i](handler)
	}
	return handler
}

// AdaptMiddleware converts a MiddlewareFunc for use with HandlerFuncWithResult.
// The adapted middleware wraps the result handler by converting it to/from a plain handler.
func AdaptMiddleware(mw MiddlewareFunc) MiddlewareWithResultFunc {
	return func(next HandlerFuncWithResult) HandlerFuncWithResult {
		return func(ctx context.Context, payload json.RawMessage) (json.RawMessage, error) {
			var result json.RawMessage
			var resultErr error

			adapted := mw(func(ctx context.Context, payload json.RawMessage) error {
				result, resultErr = next(ctx, payload)
				return resultErr
			})

			err := adapted(ctx, payload)
			if err != nil {
				return nil, err
			}
			return result, resultErr
		}
	}
}
