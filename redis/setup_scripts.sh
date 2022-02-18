#!/bin/bash

echo "Waiting for redis"
sleep 1
./wait-for-it.sh localhost:6379
echo "Redis is up"

scripts="limits_drop limits_init limits_update permit_handler update_single"

for val in $scripts; do
  echo "Setting up lightshield_$val"
  hash=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/$val.lua)")
  redis-cli set "lightshield_$val" $hash
  echo $hash

done
redis-cli keys '*'

# Basic values
redis-cli set 'apiKey' ''
redis-cli set 'regions' '{}'

redis-cli config set appendonly no
redis-cli config set save ""
