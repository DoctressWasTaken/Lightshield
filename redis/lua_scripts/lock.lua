


if redis.call('setnx', KEYS[1], 'Locked') == 1 then
    if redis.call('setnx', KEYS[2], 'Locked')  == 1 then
        redis.call('expire', KEYS[1], 1)
        redis.call('expire', KEYS[2], 1)
        return 0
    else
        redis.call('del', KEYS[1])
    end
end
return 1
