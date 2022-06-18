local server = KEYS[1]
--redis.log(redis.LOG_WARNING, "Updating server limit to "..ARGV[1]..":"..ARGV[2])
redis.call('hset', server, 'max', ARGV[1], 'duration', ARGV[2])
local endpoint = KEYS[2]
--redis.log(redis.LOG_WARNING, "Updating endpoint limit to "..ARGV[3]..":"..ARGV[4])
redis.call('hset', endpoint, 'max', ARGV[3], 'duration', ARGV[4])
