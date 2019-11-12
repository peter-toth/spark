#!/bin/bash

set -ex

usage() {
  echo "Usage $0: REPO OS COMPONENT COMPONENT_VERSION "
  echo "Example $0: cloudera alpine spark-base cloudera-2.4.4"
  exit 1
}

if [[ $# -ne 4 &&  $# -ne 5 ]]
then
  usage
fi

# Convert COMPONENT* to lowercase
REPO=$1
OS=$2
COMPONENT=$3
COMPONENT_VERSION=$4
HAS_F_FLAG=$5

###############################################
# Ensure required variables are set in the env.
###############################################
for v in OS COMPONENT COMPONENT_VERSION
do
  val=$(eval echo '$'$v)
  if [[ -z "$val" ]]
  then
    echo "Please export ENV variable $v";
    exit 1
  fi
done

###############################################
# Setup variables
###############################################

DOCKER_IMAGE_TOOL_CMD="./bin/docker-image-tool.sh"
DOCKERFILE="cloudera/docker/${OS}/${COMPONENT}/Dockerfile"
BASE_COMPONENT="spark-base"
TAG=${COMPONENT}-${COMPONENT_VERSION}-${OS}
BASE_TAG=${BASE_COMPONENT}-${COMPONENT_VERSION}-${OS}

###############################################
# Verify required files exist.
###############################################

for v in DOCKER_IMAGE_TOOL_CMD DOCKERFILE
do
  val=$(eval echo '$'$v)
  if ! test -f "$val"; then
    echo "Can not find required file $v";
    exit 1
  fi
done

if [[ -z "${HAS_F_FLAG}" ]]
then
  BASE_IMAGE_ARG=""
else
  BASE_IMAGE_ARG="-b BASE_IMAGE_ARG=$REPO/spark:$BASE_TAG"
fi

###############################################
# Build docker image
###############################################
(
  ${DOCKER_IMAGE_TOOL_CMD} \
           -f "$DOCKERFILE" \
           -r "$REPO" \
           -t "$TAG" \
           "$BASE_IMAGE_ARG" \
           build
)
