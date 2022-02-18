-- Called to drop limits that are no longer found in the response

-- Separate calls required for zone and server limits


local key = KEYS[1]

for i=1, #ARGV do

    local bucket_length = ARGV[i]
    redis.log(redis.LOG_WARNING, key.." | Dropping "..bucket_length)

    redis.call('hdel', key, bucket_length)

end
