#!/usr/bin/env bash

./wait-for-it.sh lightshield_proxy:80
./wait-for-it.sh postgres:5432
./wait-for-it.sh redis:6379

python -u run.py
