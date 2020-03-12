#!/bin/bash

export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}

INPUT_PATH="${PROJECT_ROOT_PATH}/input/page-views.json"
OUTPUT_PATH="${PROJECT_ROOT_PATH}/output/abandoned-carts.json"


# export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

MAIN_CLASS="executors.PageViewsExecutor"
RUNNER="DirectRunner"


mvn -f pom.xml clean

mvn -f pom.xml compile exec:java \
    -Dexec.mainClass=${MAIN_CLASS} \
    -Dexec.args="--inputPath=${INPUT_PATH} --outputPath=${OUTPUT_PATH} --groupKey=customer" \
    -Pdirect-runner
