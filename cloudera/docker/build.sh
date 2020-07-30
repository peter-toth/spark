#!/bin/bash

set -ex

usage() {
  echo "Usage $0: REPO OS SPARK_VERSION [-f FLAVOR]"
  echo "Example $0: cloudera alpine cloudera-2.4.4"
  echo "Example $0: cloudera alpine cloudera-2.4.4 -f python"
  echo "Supported OS list: slim, alpine, ubi8"
  echo "Supported FLAVOR list: python"
  exit 1
}

if [[ $# -ne 3 &&  $# -ne 5 ]]
then
  usage
fi

REPO=$1
OS=$2
SPARK_VERSION=$3
HAS_F_FLAG=$4
FLAVOR=$5

###############################################
# Ensure required variables are set in the env.
###############################################
for v in OS REPO SPARK_VERSION
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
TAG=${SPARK_VERSION}
SPARK_MAJOR_MINOR=$(echo "$SPARK_VERSION" | cut -c -5)

if [[ -z "${HAS_F_FLAG}" ]]
then
  # BASE IMAGE
  BUILD_TAG=${OS}-latest
  DOCKERFILE_PATH="cloudera/docker/${OS}/spark/"
  DOCKERFILE="${DOCKERFILE_PATH}Dockerfile"
  EXTRA_ARG=" -b DOCKERFILE_PATH_ARG=${DOCKERFILE_PATH} "
  IMAGE_NO_REPO="spark-${OS}"
  IMAGE_REPO="$REPO/$IMAGE_NO_REPO"
else
  # FLAVORED_IMAGE
  BUILD_TAG=${OS}-${FLAVOR}-latest
  DOCKERFILE_PATH="cloudera/docker/${OS}/spark-${FLAVOR}/"
  DOCKERFILE="${DOCKERFILE_PATH}Dockerfile"
  EXTRA_ARG=" -b BASE_IMAGE_ARG=build/cloudera/spark:${OS}-latest -b DOCKERFILE_PATH_ARG=${DOCKERFILE_PATH}"
  IMAGE_NO_REPO="spark-$FLAVOR-$OS"
  IMAGE_REPO="$REPO/$IMAGE_NO_REPO"
fi

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


###############################################
# Build docker image
###############################################
(
  ${DOCKER_IMAGE_TOOL_CMD} \
           -f "$DOCKERFILE" \
           -r "build/cloudera" \
           -t "$BUILD_TAG" \
           -n \
           $EXTRA_ARG \
           build
)

###############################################
# Tag docker image
###############################################

docker tag "build/cloudera/spark:$BUILD_TAG" "$IMAGE_REPO:$TAG"
docker tag "build/cloudera/spark:$BUILD_TAG" "$IMAGE_REPO:latest"
docker tag "build/cloudera/spark:$BUILD_TAG" "$IMAGE_NO_REPO-$SPARK_MAJOR_MINOR:latest"
