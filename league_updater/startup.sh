#!/bin/sh
wait-for-it.sh playerdata:5432
python -u run.py
