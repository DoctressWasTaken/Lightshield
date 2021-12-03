local function splits(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

local function update_limit(key, limits, counts, request_time)
    redis.call('set', key, limits)
    local limit_counts = splits(counts, ',')
    for i, limit_raw in pairs(splits(limits, ',')) do
        redis.log(redis.LOG_WARNING, 'Update: '..limit_raw..' Key: '..key)
        local limit = splits(limit_raw, ':')
        -- These are each limits max and interval, e.g. 500:10
        local max = limit[1]
        local interval = limit[2]
        local split = splits(limit_counts[i], ':')
        if redis.call('exists',  key..':'..limit_raw) == 0 then
            if redis.call('exists', key..':'..limit_raw..':inflight') == 1 then
                -- No active bucket but inflights -> reduce inflights for next bucket calculation
                redis.call('decr', key..':'..limit_raw..':inflight')
            else
                redis.call('setnx', key..':'..limit_raw..':inflight', '0')
                redis.call('setnx', key..':'..limit_raw..':rollover', '0')
                redis.call('hsetnx', key..':'..limit_raw, 'count', split[1])
                redis.call('hsetnx', key..':'..limit_raw, 'start', request_time)
                redis.call('expire', key..':'..limit_raw..':'..'inflight', 60 * 60 * 6) -- Limit inflight: set auto-cleanup
                redis.call('expire', key..':'..limit_raw..':'..'rollover', 60 * 60) -- Limit rollover: set auto-cleanup
                redis.call('pexpireat', key..':'..limit_raw, tonumber(request_time) + 1000 * interval) -- Limit: Set TTL
            end
        else
            if redis.call('hget', key..':'..limit_raw, 'start') <= request_time then
                local count = redis.call('hget', key..':'..limit_raw, 'count')
                if count < split[1] then redis.call('hset', key..':'..limit_raw, 'count', split[1]) end -- Update count if header higher
                redis.call('decr', key..':'..limit_raw..':inflight') -- Reduce inflights accordingly
            end
        end
    end
end

local key_zone = KEYS[1]
local key_server = KEYS[2]
local request_time = ARGV[1]

local limits_zone = ARGV[2]
local counts_zone = ARGV[3]
local limits_server = ARGV[4]
local counts_server = ARGV[5]

update_limit(key_zone, limits_zone, counts_zone, request_time)
update_limit(key_server, limits_server, counts_server, request_time)
