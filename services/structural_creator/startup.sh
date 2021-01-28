#!/usr/bin/env bash

./wait-for-it.sh rabbitmq:5672

python -u main.py
