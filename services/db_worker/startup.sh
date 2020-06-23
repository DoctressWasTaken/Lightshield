#!/usr/bin/env bash

./wait-for-it.sh rabbitmq:6572
./wait-for-it.sh postgres:5432

python -u run.py
