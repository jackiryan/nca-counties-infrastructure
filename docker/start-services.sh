#!/bin/bash
set -e

service postgresql start

until pg_isready; do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

martin --config /etc/martin-config.yaml