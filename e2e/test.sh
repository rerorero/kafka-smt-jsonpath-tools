#!/bin/bash

set -eux

# start services
docker-compose up -d --build connect
trap 'docker-compose down' EXIT

sleep 60
# start connect
curl -X POST -H "Content-Type: application/json" --data @config.json http://localhost:8083/connectors
sleep 20

# Count the number of topics that has a prefix 'topic-User_'
[ "$(docker-compose exec -T kafka kafka-topics --list --bootstrap-server kafka:29092 | grep -c topic-User_)" -gt "5" ]
