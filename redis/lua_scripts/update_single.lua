
local key_counter = KEYS[1] -- Counter key
local key_meta = KEYS[2] -- Meta key


local send_time = ARGV[1] -- When the request was sent in ms
local count = ARGV[2] -- limit count

local length = redis.call('hget', key_meta, 'length') -- When the
local bucket_start = redis.call('hget', key_meta, 'start_time') -- When the bucket was created

-- Check if bucket exists
if redis.call('setnx', key_counter, count) == 0 then
    -- Set to higher of current or new if exists
    local current_count = redis.call('get', key_counter)
    redis.call('set', key_counter, math.max(count, current_count))

    -- If bucket same as when request was sent (the bucket was created before the request)
    -- Reduce the inflights by one
    if bucket_start < send_time then
        local inflight_count = redis.call('hincrby', key_meta, 'inflight', -1)
        if inflight_count < 0 then -- Avoid negative values (shouldnt be possible but to be sure)
            redis.call('hset', key_meta, 'inflight', 0)
        end
    end
end
-- Set timeout if doesnt exist
redis.call('expire', key_counter, length, 'NX')


