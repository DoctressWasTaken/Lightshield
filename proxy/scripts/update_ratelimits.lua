local server = KEYS[1]
redis.call('hset', server, ARGV[1], ARGV[2])
local endpoint = KEYS[2]
redis.call('hset', endpoint, ARGV[3], ARGV[4])
