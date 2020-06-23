#!/usr/bin/env bash

./wait-for-it.sh proxy:8000
./wait-for-it.sh rabbitmq:5672

python -u run.py
