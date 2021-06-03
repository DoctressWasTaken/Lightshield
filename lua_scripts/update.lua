local function subrange(t, first, last)
    local sub = {}
    for i = first, last do
        sub[#sub + 1] = tonumber(t[i])
    end
    return sub
end
local send_time, return_time, retry_after = unpack(ARGV)
local params = subrange(ARGV, 4, #ARGV) -- Count:Max:Span per bucket
for range = 1, #KEYS do
    -- redis.log(redis.LOG_WARNING, '[UPDATE] Bucket ' .. KEYS[range])
    local count, span = unpack(params)
    if #params > 0 then
        params = subrange(params, 3, #params)
    end
    -- Update all limits
    local bucket_data = redis.call('hmget', KEYS[range], 'end', 'start', 'count')
    if not bucket_data[1] then -- Create bucket if not existent
        -- redis.log(redis.LOG_WARNING, '[UPDATE] Bucket doesnt exist. ')
        redis.call('hset', KEYS[range], 'count', count, 'start', send_time, 'end', tonumber(send_time) + span)
    else -- Handle existing bucket
        local end_point, start_point, saved_count = unpack(bucket_data)
        if tonumber(send_time) < tonumber(start_point) then  -- Pass if the request returned before the new bucket was initiated
            -- redis.log(redis.LOG_WARNING, '[UPDATE] Returned before the bucket started.')
        elseif count < tonumber(saved_count) and tonumber(send_time) > tonumber(end_point) then
            -- If returned count < saved count
            -- AND returned timestamp > local end
            -- then assume bucket is over
            -- redis.log(redis.LOG_WARNING, '[UPDATE] Making bucket persistent. '..KEYS[range])
            redis.call('persist', KEYS[range])  -- Set to persist so that it can be reset
            redis.call('hset', KEYS[range], 'count', count, 'start', send_time, 'end', tonumber(send_time) + span)
        else -- Legit request, update count
            local new_count = math.max(count, saved_count)
            redis.call('hset', KEYS[range], 'count', new_count)
        end
    end
    if redis.call('ttl', KEYS[range]) < 0 then
        -- If has no expire, set expire
        local expire_time = tonumber(return_time) + 1000 * span
        -- redis.log(redis.LOG_WARNING, 'Setting expire for ' .. KEYS[range] .. ' to ' .. expire_time)
        redis.call('pexpireat', KEYS[range], expire_time)
    end
end
