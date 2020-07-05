#!/usr/bin/env bash

./wait-for-it.sh proxy:8000
./wait-for-it.sh redis:6379
./wait-for-it.sh provider:9999

python -u run.py
