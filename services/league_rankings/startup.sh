#!/usr/bin/env bash

./wait-for-it.sh lightshield_proxy:80
./wait-for-it.sh lightshield.dev:5432 -t 90

python -u run.py
