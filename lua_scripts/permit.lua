local blocked = -3  -- -3 as default for non-blocked as -2 and -1 are reserved
for range = 1, #KEYS do
    --redis.log(redis.LOG_WARNING , 'Checking bucket '..KEYS[range])
    -- Get if current count if exists
    local content = redis.call('hget', KEYS[range], 'count')
    if not content or content == nil then
        --redis.log(redis.LOG_WARNING, 'Active bucket for '..KEYS[range]..' not found.')
        -- If non-existent create with count 0 and passed timestamp
        redis.call('hset', KEYS[range], 'count', 0, 'start', ARGV[1], 'end', ARGV[range * 2 + 1])
    else
        --redis.log(redis.LOG_WARNING , 'Checking bucket '..KEYS[range]..' for block.')
        -- If existent check if blocked
        if tonumber(content) >= tonumber(ARGV[range * 2]) then
            --redis.log(redis.LOG_WARNING , 'Bucket '..KEYS[range]..' is blocked. ['..content..' out of '..ARGV[range * 2]..']')
            local ttl = redis.call('pttl', KEYS[range])
            if ttl == -1 then
                blocked = 1000
            else
                blocked = tonumber(ttl)
            end
            break
        end
    end
end
-- Only if not blocked
if blocked == -3 then
    -- Add one to each limit
    for range = 1, #KEYS do
        --redis.log(redis.LOG_WARNING , 'Incrementing '..KEYS[range]..'.')
        redis.call('hincrby', KEYS[range], 'count', 1)
    end
end
--redis.log(redis.LOG_WARNING , 'Returning blocked='..blocked)
return blocked
