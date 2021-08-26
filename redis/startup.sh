#!/usr/bin/env bash


./setup_scripts.sh &
redis-server --appendonly yes
