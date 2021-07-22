#!/usr/bin/env bash
set -eux

(cd .. && ./gradlew jar) || exit 1
docker-compose up -d --build || exit 1
exec ./client.sh
