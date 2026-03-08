// Package ratelimit provides a distributed token bucket rate limiter
// backed by Redis Lua scripts.
package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// luaTokenBucket atomically checks and decrements a token bucket.
// Keys: [bucket_key]
// Args: [max_tokens, refill_rate_per_sec, now_micros, requested]
// Returns: 1 if allowed, 0 if denied.
var luaTokenBucket = rueidis.NewLuaScript(`
local key = KEYS[1]
local max_tokens = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens = tonumber(bucket[1])
local last_refill = tonumber(bucket[2])

if tokens == nil then
    tokens = max_tokens
    last_refill = now
end

-- Refill tokens based on elapsed time.
local elapsed = (now - last_refill) / 1000000.0
local refilled = elapsed * refill_rate
tokens = math.min(max_tokens, tokens + refilled)

if tokens >= requested then
    tokens = tokens - requested
    redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(now))
    redis.call('PEXPIRE', key, 60000)
    return 1
else
    redis.call('HMSET', key, 'tokens', tostring(tokens), 'last_refill', tostring(now))
    redis.call('PEXPIRE', key, 60000)
    return 0
end
`)

// Limiter is a distributed rate limiter using Redis token bucket.
type Limiter struct {
	rdb       rueidis.Client
	keyPrefix string
}

// Config configures a rate limit for a specific tier/queue.
type Config struct {
	// MaxTokens is the bucket capacity (burst size).
	MaxTokens int
	// RefillRate is the number of tokens added per second.
	RefillRate float64
}

// New creates a new distributed rate limiter.
func New(rdb rueidis.Client, keyPrefix string) *Limiter {
	return &Limiter{rdb: rdb, keyPrefix: keyPrefix}
}

// Allow checks if n tokens are available for the given tier.
// Returns true if the request is allowed, false if rate-limited.
func (l *Limiter) Allow(ctx context.Context, tier string, n int, cfg Config) (bool, error) {
	key := fmt.Sprintf("%s:ratelimit:%s", l.keyPrefix, tier)
	nowMicros := time.Now().UnixMicro()

	result, err := luaTokenBucket.Exec(ctx, l.rdb, []string{key}, []string{
		strconv.Itoa(cfg.MaxTokens),
		strconv.FormatFloat(cfg.RefillRate, 'f', -1, 64),
		strconv.FormatInt(nowMicros, 10),
		strconv.Itoa(n),
	}).AsInt64()
	if err != nil {
		return false, fmt.Errorf("rate limit check: %w", err)
	}
	return result == 1, nil
}
