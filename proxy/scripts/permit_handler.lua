local function check_limits(key, timestamp)
    -- Get permission to register a new request
    local max_time = 0
    if redis.call('exists', key) == 1 then
        local duration = tonumber(redis.call('hget', key, 'duration'))
        local max = tonumber(redis.call('hget', key, 'max'))
        local requests_key = key..':requests'
        -- Check if requests key exists
        if redis.call('exists', requests_key) == 1 then
            -- drop timeouts
            redis.call('zremrangebyscore', requests_key, 0, timestamp)
            -- count remaining
            local current_block = redis.call('zcount', requests_key, -1, "+inf")
            -- redis.log(redis.LOG_WARNING, 'Tasks: '..current_block)
            -- redis.log(redis.LOG_WARNING, 'Max: '..max)
            -- redis.log(redis.LOG_WARNING, 'Key: '..key)

            -- check against limit
            if current_block >= max then
                return tonumber(redis.call('zrange', requests_key, 0, 1, 'WITHSCORES')[2])
            end
        end
    else
        redis.call('hset', key, 'duration', 10, 'max', 5)
    end
    return 0
end


local function update_limits(key, request_id, timestamp)
    -- Register a new request
    local duration = tonumber(redis.call('hget', key, 'duration'))
    local max = tonumber(redis.call('hget', key, 'max'))
    local requests_key = key..':requests'

    redis.call('zadd', requests_key, timestamp + 1000 * (duration + 0.5), request_id)

end


local timestamp = ARGV[1]
local request_id = ARGV[2]


local server = KEYS[1]
local endpoint = KEYS[2]
local wait_until = check_limits(server, timestamp)
-- redis.log(redis.LOG_WARNING, "Server"..wait_until)
wait_until = math.max(wait_until, check_limits(endpoint, timestamp))
-- redis.log(redis.LOG_WARNING, "Wait until "..wait_until)

if wait_until > 0 then
    return wait_until
end

update_limits(server, request_id, timestamp)
update_limits(endpoint, request_id, timestamp)

return 0
