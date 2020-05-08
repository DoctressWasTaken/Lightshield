#!/bin/sh
./wait-for-it.sh proxy:8000
python -u run.py
