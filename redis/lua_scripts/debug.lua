local function splits(s, delimiter)
    local result = {};
    for match in (s..delimiter):gmatch("(.-)"..delimiter) do
        table.insert(result, match);
    end
    return result;
end

for range=1, #KEYS do
    redis.log(redis.LOG_WARNING, 'KEYS', KEYS[range])
    redis.log(redis.LOG_WARNING, 'KEYS', splits(KEYS[range], " "))
end


for range=1, #ARGV do
    redis.log(redis.LOG_WARNING, 'ARGV', ARGV[range])
    redis.log(redis.LOG_WARNING, 'ARGV', splits(ARGV[range], " "))
end


redis.log(redis.LOG_WARNING, 'ARGV', ARGV)
redis.log(redis.LOG_WARNING, 'KEYS', KEYS)
