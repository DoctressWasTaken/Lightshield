#!/usr/bin/env bash

./wait-for-it.sh 192.168.0:5432
./wait-for-it.sh redis:6379

python -u run.py
