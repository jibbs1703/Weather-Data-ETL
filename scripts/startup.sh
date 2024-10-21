#!/bin/sh

# Start Up Service Containers
docker compose up -d

# Go into Webserver Container Command Line
docker exec -it weather-gas-etl-pipeline-airflow-webserver-1 bash