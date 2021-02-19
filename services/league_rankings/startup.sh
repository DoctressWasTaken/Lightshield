#!/usr/bin/env bash

./wait-for-it.sh lightshield_proxy:80
./wait-for-it.sh 172.17.0.1:5432

python -u run.py
