#!/bin/bash

echo "Waiting for redis"
sleep 1
./wait-for-it.sh localhost:6379
echo "Redis is up"
hash_permit=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/permit.lua)")
echo $hash_permit
redis-cli set 'lightshield_permit' $hash_permit

hash_update=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/update.lua)")
echo $hash_update
redis-cli set 'lightshield_update' $hash_update

hash_lock=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/lock.lua)")
echo $hash_lock
redis-cli set 'lightshield_lock' $hash_lock

hash_unlock=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/unlock.lua)")
echo $hash_unlock
redis-cli set 'lightshield_unlock' $hash_unlock

hash_update_slim=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/update_slim.lua)")
echo $hash_update_slim
redis-cli set 'lightshield_update_slim' $hash_update_slim

redis-cli config set appendonly no
redis-cli config set save ""
