#!/usr/bin/env bash

./wait-for-it.sh redis:6379

poetry run python -u run.py
