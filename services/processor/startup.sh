#!/usr/bin/env bash

./wait-for-it.sh postgres:5432

python -u run.py
