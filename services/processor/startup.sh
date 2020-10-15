#!/usr/bin/env bash

./wait-for-it.sh postgres:5432
./wait-for-it.sh longterm:5432

python -u run.py
