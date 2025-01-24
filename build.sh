#!/bin/bash

docker-compose up --build -d ar-db
docker exec -it nca-counties-infrastructure-ar-db-1 /usr/local/bin/seed-db.sh
docker-compose up -d martin 