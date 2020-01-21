#!/usr/bin/env bash

#docker container prune
docker-compose up -d

JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})
docker cp target/errorDetection-0.1.jar "$JOBMANAGER_CONTAINER":/job.jar
docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar --input /home/ErrorStreamSetB.csv --output /tmp/test
