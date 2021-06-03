#!/bin/bash

hash_permit=$(redis-cli SCRIPT LOAD "$(cat /lua_scripts/permit.lua)")
echo $hash_permit
hash_pseudo=$(redis-cli SCRIPT LOAD "$(cat /lua_scripts/pseudo.lua)")
echo $hash_pseudo
hash_update=$(redis-cli SCRIPT LOAD "$(cat /lua_scripts/update.lua)")
echo $hash_update
redis-cli set 'permit' $hash_permit
redis-cli set 'pseudo' $hash_pseudo
redis-cli set 'update' $hash_update
