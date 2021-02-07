#!/usr/bin/env bash

./wait-for-it.sh proxy:8000
./wait-for-it.sh postgres:5432

python -u run.py
