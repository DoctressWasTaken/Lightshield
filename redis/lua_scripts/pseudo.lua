if redis.call('exists', KEYS[1]) == 0 then
-- If pseudo count doesnt exist, create it
    redis.call('hset', KEYS[1], 'count', 0)
    redis.call('set', KEYS[1]..':inflight', 0)
    redis.call('set', KEYS[1]..':rollover', 0)
    redis.call('expire', KEYS[1], 5)
end
