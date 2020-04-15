#!/bin/sh
DEBUG=false python manage.py migrate
python -u run.py
