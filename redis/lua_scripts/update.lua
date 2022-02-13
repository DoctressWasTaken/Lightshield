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
        -- redis.log(redis.LOG_WARNING, 'Update: '..limit_raw..' Key: '..key)
        local limit = splits(limit_raw, ':')
        -- These are each limits max and interval, e.g. 500:10
        local max = limit[1]
        local interval = tonumber(limit[2])
        local split = splits(limit_counts[i], ':')
        local limit_server_count = tonumber(split[1])
        local ten_second = tostring(math.floor(tonumber(request_time) / 10000))
        if redis.call('exists',  key..':'..limit_raw) == 0 then
            -- No active bucket
            if redis.call('setnx', key..':'..limit_raw..':inflight', 0) == 0 then -- If key did exist
                -- Active inflights
                if tonumber(redis.call('get', key..':'..limit_raw..':inflight')) > 0 then
                    -- Only decrease inflights when > 0
                    redis.call('decr', key..':'..limit_raw..':inflight')
                end
            end
        else
            -- Bucket exists
            local bucket_start = redis.call('hget', key..':'..limit_raw, 'start')
            if bucket_start <= request_time then
                -- Request was made in currently active bucket
                local limit_stored_count = tonumber(redis.call('hget', key..':'..limit_raw, 'count'))
                if limit_stored_count < limit_server_count then redis.call('hset', key..':'..limit_raw, 'count', limit_server_count) end -- Update count if header higher
                local inflight = tonumber(redis.call('get', key..':'..limit_raw..':inflight'))
                if inflight == nil then inflight = 0 end
                if inflight > 0 then
                    -- Only decrease inflights when > 0
                    redis.call('decr', key..':'..limit_raw..':inflight') -- Reduce inflights accordingly
                end
                if redis.call('hsetnx', key..':'..limit_raw, 'end', 'returned') == 1 then
                    -- Update bucket end if it wasn't set before
                    redis.call('pexpireat', key..':'..limit_raw, tonumber(request_time) + 1000 * interval)
                end
            end
        end
        -- Tracking WIP: Not Tested
        redis.call('hincrby', key..':'..limit_raw..':status:'..ten_second, ARGV[6], 1)
        redis.call('expire', key..':'..limit_raw..':status:'..ten_second, 60 * 10)
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
