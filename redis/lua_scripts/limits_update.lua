-- Called to add new limits that were not found when executing the request
-- If another response has already created them they are skipped

-- Separate calls required for zone and server limits

local key = KEYS[1]

for i=1, math.floor(#ARGV/2) do

    local bucket_length = tostring(ARGV[i * 2])
    local new_max = ARGV[i * 2 - 1]
    local key_meta = key..':'..bucket_length..':meta'
    redis.call('hset', key_meta, "max", new_max)
    redis.call('hset', key, bucket_length, new_max)
    redis.log(redis.LOG_WARNING, key.." | Updating "..bucket_length.." to "..new_max)
end
