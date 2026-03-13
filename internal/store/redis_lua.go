package store

// Lua scripts for atomic Redis operations.
// All scripts are Redis Cluster compatible:
//   - Single-key scripts: CAS, create-if-not-exists, leader refresh, lock ops, job save, flush group
//   - Hash-tagged multi-key: luaMoveDelayedToStream ({tierName}), election scripts ({election})

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

// luaLeaderRefresh refreshes the leader election key TTL only if the caller is still leader.
// KEYS[1] = election leader key
// ARGV[1] = expected nodeID
// ARGV[2] = TTL in seconds
// Returns 1 on success, 0 if not leader.
const luaLeaderRefresh = `
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
  return 1
end
return 0
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

// luaMoveDelayedToStream atomically moves ripe messages from delayed sorted set to work stream.
// Both keys use hash-tagged tier names for Redis Cluster slot co-location.
// KEYS[1] = delayed sorted set (dureq:{tier}:delayed)
// KEYS[2] = work stream (dureq:{tier}:work)
// ARGV[1] = current timestamp (score cutoff)
// ARGV[2] = max messages to move per call
// Returns number of messages moved.
const luaMoveDelayedToStream = `
local delayed = KEYS[1]
local stream = KEYS[2]
local cutoff = ARGV[1]
local maxMsgs = tonumber(ARGV[2])

local msgs = redis.call('ZRANGEBYSCORE', delayed, '-inf', cutoff, 'LIMIT', 0, maxMsgs)
local moved = 0
for _, msgJson in ipairs(msgs) do
  local ok, msg = pcall(cjson.decode, msgJson)
  if ok and msg then
    local args = {
      'run_id', msg.run_id or '',
      'job_id', msg.job_id or '',
      'task_type', msg.task_type or '',
      'payload', msg.payload or '',
      'attempt', tostring(msg.attempt or 0),
      'deadline', msg.deadline or '',
      'priority', tostring(msg.priority or 0),
      'dispatched_at', msg.dispatched_at or '',
      'tier', msg.tier or '',
      'version', msg.version or ''
    }
    if msg.headers and msg.headers ~= '' then
      args[#args+1] = 'headers'
      args[#args+1] = msg.headers
    end
    if msg.metadata and msg.metadata ~= '' then
      args[#args+1] = 'metadata'
      args[#args+1] = msg.metadata
    end
    if msg.concurrency_keys and msg.concurrency_keys ~= '' then
      args[#args+1] = 'concurrency_keys'
      args[#args+1] = msg.concurrency_keys
    end
    redis.call('XADD', stream, '*', unpack(args))
    redis.call('ZREM', delayed, msgJson)
    moved = moved + 1
  end
end
return moved
`

// luaLeaderElectWithEpoch atomically elects a leader and increments the epoch.
// KEYS[1] = leader key
// KEYS[2] = epoch key
// ARGV[1] = nodeID
// ARGV[2] = TTL in seconds
// Returns the new epoch on success, 0 if another node is already leader.
const luaLeaderElectWithEpoch = `
local leader = redis.call('GET', KEYS[1])
if leader then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', tonumber(ARGV[2]))
local epoch = redis.call('INCR', KEYS[2])
return epoch
`

// luaLeaderRefreshWithEpoch refreshes the leader key and returns the current epoch.
// KEYS[1] = leader key
// KEYS[2] = epoch key
// ARGV[1] = expected nodeID
// ARGV[2] = TTL in seconds
// Returns the current epoch on success, 0 if not leader.
const luaLeaderRefreshWithEpoch = `
local current = redis.call('GET', KEYS[1])
if current ~= ARGV[1] then
  return 0
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
local epoch = redis.call('GET', KEYS[2])
return tonumber(epoch) or 0
`

// luaFlushGroup atomically reads and deletes all messages from a group list.
// Only KEYS[1] is used (single-key) for Redis Cluster compatibility.
// The critical atomicity guarantee is preserved: only one concurrent caller
// will see the messages; others get empty. Cleanup of the metadata hash and
// active set is done separately by the caller (idempotent operations).
// KEYS[1] = group messages list  (prefix:group:{name}:messages)
// Returns the list of raw message strings, or empty table.
const luaFlushGroup = `
local msgs = redis.call('LRANGE', KEYS[1], 0, -1)
if #msgs == 0 then
  return {}
end
redis.call('DEL', KEYS[1])
return msgs
`

// luaClaimSideEffect atomically claims a side-effect step or returns the cached result.
// KEYS[1] = side effect key (prefix:sideeffect:{runID}:{stepKey})
// ARGV[1] = TTL in seconds
// Returns:
//   "claimed"  — step was not started; caller should execute then call CompleteSideEffect
//   "pending"  — step was claimed by another caller (concurrent execution)
//   result     — step already completed; return cached result (prefixed with "done:")
const luaClaimSideEffect = `
local key = KEYS[1]
local status = redis.call('HGET', key, 'status')
if status == 'done' then
  return 'done:' .. (redis.call('HGET', key, 'result') or '')
end
if status == 'pending' then
  return 'pending'
end
redis.call('HSET', key, 'status', 'pending')
redis.call('EXPIRE', key, tonumber(ARGV[1]))
return 'claimed'
`

// luaAcquireConcurrencySlot atomically acquires a concurrency slot if capacity allows.
// Uses a sorted set as a semaphore: member=runID, score=acquireTime (for orphan detection).
// KEYS[1] = concurrency slot key (prefix:conc:{concKey})
// ARGV[1] = runID
// ARGV[2] = maxConcurrency
// ARGV[3] = current timestamp (score)
// Returns:
//   1 — slot acquired
//   0 — at capacity, slot not acquired
const luaAcquireConcurrencySlot = `
local key = KEYS[1]
local runID = ARGV[1]
local maxConc = tonumber(ARGV[2])
local now = ARGV[3]

-- Check if this runID already holds a slot (idempotent).
if redis.call('ZSCORE', key, runID) then
  return 1
end

local current = redis.call('ZCARD', key)
if current >= maxConc then
  return 0
end

redis.call('ZADD', key, now, runID)
return 1
`

// luaReleaseConcurrencySlot releases a concurrency slot.
// KEYS[1] = concurrency slot key
// ARGV[1] = runID
// Returns 1 if removed, 0 if not found.
const luaReleaseConcurrencySlot = `
return redis.call('ZREM', KEYS[1], ARGV[1])
`

// Public accessors for Lua scripts needed by external packages (election, lock).

func LuaLeaderRefreshScript() string          { return luaLeaderRefresh }
func LuaLeaderElectWithEpochScript() string   { return luaLeaderElectWithEpoch }
func LuaLeaderRefreshWithEpochScript() string { return luaLeaderRefreshWithEpoch }
func LuaUnlockIfOwnerScript() string          { return luaUnlockIfOwner }
func LuaExtendLockScript() string             { return luaExtendLock }
func LuaFlushGroupScript() string             { return luaFlushGroup }
