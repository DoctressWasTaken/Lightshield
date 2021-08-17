#!/bin/bash

hash_permit=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/permit.lua)")
echo $hash_permit
hash_pseudo=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/pseudo.lua)")
echo $hash_pseudo
hash_update=$(redis-cli SCRIPT LOAD "$(cat /project/lua_scripts/update.lua)")
echo $hash_update
redis-cli set 'lightshield_permit' $hash_permit
redis-cli set 'lightshield_pseudo' $hash_pseudo
redis-cli set 'lightshield_update' $hash_update
