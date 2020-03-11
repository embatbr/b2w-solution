#!/bin/bash

export PROJECT_ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd ${PROJECT_ROOT_PATH}

INPUT_PATH="${PROJECT_ROOT_PATH}/input"
OUTPUT_PATH="${PROJECT_ROOT_PATH}/output"


# export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

MAIN_CLASS="Executor"
RUNNER="DirectRunner"


mvn -f pom.xml clean

mvn -f pom.xml compile exec:java \
    -Dexec.mainClass=${MAIN_CLASS} \
    -Pdirect-runner

# mvn -f pom.xml compile exec:java \
#     -Dexec.mainClass=${MAIN_CLASS} \
#     -Dexec.args="--input_path=${INPUT_PATH} --output_path=${OUTPUT_PATH}" \
#     -Pdirect-runner
