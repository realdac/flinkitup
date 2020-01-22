#!/usr/bin/env bash

#docker container prune
docker-compose up -d

#    &
#      bash -xeuo pipefail -c "
#        sed -i 's/log4j.rootLogger=INFO, file/log4j.rootLogger=DEBUG, file/' /opt/flink/conf/log4j.properties
#      "

curl -XPUT "http://localhost:9200/nyc-places"

curl -H 'Content-Type: application/json' -XPUT "http://localhost:9200/nyc-places/_mapping/popular-locations" -d'
{
 "popular-locations" : {
   "properties" : {
      "timestamp": {"type": "text"},
      "version": {"type": "text"},
      "error_code": {"type": "text"},
      "count": {"type": "integer"}
    }
 }
}'


JOBMANAGER_CONTAINER=$(docker ps --filter name=jobmanager --format={{.ID}})

docker exec -t -i "$JOBMANAGER_CONTAINER" sed -i 's/log4j.rootLogger=INFO, file/log4j.rootLogger=DEBUG, file/' /opt/flink/conf/log4j.properties
docker exec -t -i "$JOBMANAGER_CONTAINER" sed -i 's/<root level="INFO">/<root level="DEBUG">/' /opt/flink/conf/logback.xml
docker exec -t -i "$JOBMANAGER_CONTAINER" sed -i 's/log4j.rootLogger=INFO, console/log4j.rootLogger=DEBUG, console/' ./conf/log4j-console.properties
docker exec -t -i "$JOBMANAGER_CONTAINER" sed -i 's/<root level="INFO">/<root level="DEBUG">/' ./conf/logback-console.xml
docker cp target/errorDetection-0.1.jar "$JOBMANAGER_CONTAINER":/job.jar
docker exec -t -i "$JOBMANAGER_CONTAINER" flink run /job.jar --writeToElasticsearch --input /home/ErrorStreamSetB.csv --output /tmp/test
