-- Called to add new limits that were not found when executing the request
-- If another response has already created them they are skipped

-- Separate calls required for zone and server limits

local key = KEYS[1]
local timestamp = tonumber(ARGV[1])

for i=1, math.floor(#ARGV/2) do
    local max_count = tonumber(ARGV[i * 2])
    local bucket_length = tonumber(ARGV[i * 2 + 1])

    local key_meta = key..':'..tostring(bucket_length)..':meta'

    if redis.call('hsetnx', key, bucket_length, max_count) == 1 then
        redis.log(redis.LOG_WARNING, key.." | Initiating "..bucket_length..":"..max_count)
        -- If the key is new set up the meta element
        redis.call('hmset', key_meta,
            'inflight', 0,
            'rollover', 0,
            'start_time', timestamp,
            'length', bucket_length,
            'max', max_count
        )
    end
end
