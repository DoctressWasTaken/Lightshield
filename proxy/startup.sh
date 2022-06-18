#!/usr/bin/env bash

./wait-for-it.sh redis:6379


exec poetry run gunicorn main:init_app \
    --bind 0.0.0.0:8888 \
    --worker-class aiohttp.GunicornWebWorker
#exec poetry run python -u main.py
