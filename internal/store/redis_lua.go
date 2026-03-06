package store

// Lua scripts for atomic Redis operations.
// All scripts are single-key (KEYS[1] only) for Redis Cluster compatibility.

// luaCASUpdate performs a compare-and-swap update on a hash key.
// KEYS[1] = hash key
// ARGV[1] = expected version
// ARGV[2] = new JSON data
// ARGV[3] = new version
// Returns new version on success, error on version mismatch.
const luaCASUpdate = `
local current = redis.call('HGET', KEYS[1], '_version')
if current ~= ARGV[1] then
  return redis.error_reply('CAS_CONFLICT: expected ' .. tostring(ARGV[1]) .. ' got ' .. tostring(current))
end
redis.call('HSET', KEYS[1], 'data', ARGV[2], '_version', ARGV[3])
return ARGV[3]
`

// luaCreateIfNotExists atomically creates a hash key only if it doesn't exist.
// KEYS[1] = hash key
// ARGV[1] = JSON data
// Returns "1" on success, error if key already exists.
const luaCreateIfNotExists = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return redis.error_reply('ALREADY_EXISTS')
end
redis.call('HSET', KEYS[1], 'data', ARGV[1], '_version', '1')
return '1'
`

// luaUnlockIfOwner deletes a lock key only if the value matches the owner.
// KEYS[1] = lock key
// ARGV[1] = expected owner value
// Returns 1 if deleted, 0 if not owner.
const luaUnlockIfOwner = `
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return 1
end
return 0
`

// luaExtendLock extends a lock's TTL only if the caller owns it.
// KEYS[1] = lock key
// ARGV[1] = expected owner value
// ARGV[2] = TTL in seconds
// Returns 1 on success, 0 if not owner.
const luaExtendLock = `
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
return 0
`

// luaSaveJobSingleKey atomically upserts a job hash (single key).
// Returns old data (for index diff) and new version.
// KEYS[1] = job hash key
// ARGV[1] = new JSON data
// Returns: "old_data\nnew_version" (old_data is empty string if creating new).
const luaSaveJobSingleKey = `
local key = KEYS[1]
local oldData = redis.call('HGET', key, 'data') or ''

local cv = redis.call('HGET', key, '_version')
local nv = 1
if cv then
  nv = tonumber(cv) + 1
end

redis.call('HSET', key, 'data', ARGV[1], '_version', tostring(nv))
return oldData .. '\n' .. tostring(nv)
`

// luaCASUpdateJobSingleKey performs a CAS update on a job hash (single key).
// Returns old data (for index diff) and new version.
// KEYS[1] = job hash key
// ARGV[1] = expected version string
// ARGV[2] = new JSON data
// Returns: "old_data\nnew_version" on success, error on CAS conflict.
const luaCASUpdateJobSingleKey = `
local key = KEYS[1]
local expectedVersion = ARGV[1]

local cv = redis.call('HGET', key, '_version')
if cv ~= expectedVersion then
  return redis.error_reply('CAS_CONFLICT: expected ' .. tostring(expectedVersion) .. ' got ' .. tostring(cv))
end

local oldData = redis.call('HGET', key, 'data') or ''
local nv = tonumber(cv) + 1

redis.call('HSET', key, 'data', ARGV[2], '_version', tostring(nv))
return oldData .. '\n' .. tostring(nv)
`

// Public accessors for Lua scripts needed by external packages (lock).

func LuaUnlockIfOwnerScript() string { return luaUnlockIfOwner }
func LuaExtendLockScript() string    { return luaExtendLock }
