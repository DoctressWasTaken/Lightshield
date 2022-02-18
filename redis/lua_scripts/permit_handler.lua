

local timestamp = ARGV[1]


local max_wait = 0
-- Get permits
for i=1, #KEYS do
    local key_type = KEYS[i]
    for n, key in pairs(redis.call('hkeys', key_type)) do
        if key ~= 'placeholder' then
            local key_counter = key_type..':'..key
            local key_meta = key_counter..':meta'
            -- get max
            local max_count = tonumber(redis.call('hget', key_meta, 'max'))
            local rollover = tonumber(redis.call('hget', key_meta, 'rollover'))
            -- get current
            local current = 0
            if redis.call('setnx', key_counter, 0) == 0 then
                current = tonumber(redis.call('get', key_counter))
            end
            if (current + rollover) >= max_count then
                -- Increase wait time if full
                max_wait = math.max(100, max_wait, redis.call('pttl', key_counter))
            end
        end
    end
end

if max_wait > 0 then
    return max_wait
end
-- Get reservations
for i=1, #KEYS do
    local key_type = KEYS[i]
    for n, key in pairs(redis.call('hkeys', key_type)) do
        if key ~= 'placeholder' then
            local key_counter = key_type..':'..key
            local key_meta = key_counter..':meta'
            -- Increment
            if redis.call('incr', key_counter) == 1 then
                redis.call('hset', key_meta, 'start_time', timestamp)
            end
            redis.call('hincrby', key_meta, 'inflight', 1)
        end
    end
end
return 0
