#!/usr/bin/env bash

./scripts/wait-for-it.sh "$KAFKA_BOOTRSTRAP_SERVER" -t 5 -- echo "Kafka started"
./scripts/wait-for-it.sh "$POSTGRES_HOST:$POSTGRES_PORT" -t 5 -- echo "Postgres started"

python -m src.pubfinder_worker.py