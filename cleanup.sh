#!/bin/bash
source .venv/bin/activate
black --target-version py36 sync-redis.py
isort -rc sync-redis.py
flake8 sync-redis.py
