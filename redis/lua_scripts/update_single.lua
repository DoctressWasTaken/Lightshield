
local key_counter = KEYS[1] -- Counter key
local key_meta = KEYS[2] -- Meta key


local send_time = tonumber(ARGV[1]) -- When the request was sent in ms
local count = ARGV[2] -- limit count

local length = tonumber(redis.call('hget', key_meta, 'length')) -- When the
local bucket_start = tonumber(redis.call('hget', key_meta, 'start_time')) -- When the bucket was created

-- Check if bucket exists
if redis.call('exists', key_counter) == 1 then
    if bucket_start < send_time then
        local current_count = redis.call('get', key_counter)
        redis.call('set', key_counter, math.max(count, current_count), 'KEEPTTL')
        local inflight_count = redis.call('hincrby', key_meta, 'inflight', -1)
        if inflight_count < 0 then -- Avoid negative values (shouldnt be possible but to be sure)
            redis.call('hset', key_meta, 'inflight', 0)
        end
    end
end
redis.call('expire', key_counter, length, 'NX')
