#!/bin/bash

echo "Waiting for redis"
sleep 1
./wait-for-it.sh localhost:6379
echo "Redis is up"
hash_permit=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/permit.lua)")
echo $hash_permit
hash_pseudo=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/pseudo.lua)")
echo $hash_pseudo
hash_update=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/update.lua)")
echo $hash_update
hash_debug=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/debug.lua)")
echo $hash_debug
redis-cli set 'lightshield_permit' $hash_permit
redis-cli set 'lightshield_pseudo' $hash_pseudo
redis-cli set 'lightshield_update' $hash_update
redis-cli set 'lightshield_debug' $hash_debug
