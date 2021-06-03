redis.log(redis.LOG_WARNING , 'Setting pseudo-limit '..KEYS[1])
if redis.call('exists', KEYS[1]) == 0 then
-- If pseudo count doesnt exist, create it
    redis.call('hset', KEYS[1], 'count', 0)
    redis.call('expire', KEYS[1], 2)
end
