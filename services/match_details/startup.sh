#!/usr/bin/env bash
./wait-for-it.sh postgres:5432

exec poetry run python -u handler.py
