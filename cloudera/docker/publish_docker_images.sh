#!/bin/bash -ex

# Simple script to publish locally built spark docker images
# to a sandbox or other registry.

DOCKER_REGISTRY=docker-private.infra.cloudera.com/cloudera
PUBLISH_DOCKER_REGISTRY=docker-sandbox.infra.cloudera.com/$USER
GBN=""

usage() {
  echo "Usage $0: PUBLISH_DOCKER_REGISTRY OS TAG GBN "
  echo "Example: $0 docker-sandbox.infra.cloudera.com/$USER slim 3.1.1.3.1.7270.0-snapshot \"\""
  exit 1
}

if [[ $# -ne 4 ]]
then
  usage
fi

PUBLISH_DOCKER_REGISTRY=$1
OS=$2
TAG=$3
GBN=$4

if [[ -n $GBN ]]; then
  TAG=$TAG-$GBN
fi

docker tag ${DOCKER_REGISTRY}/spark-py${OS}:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark-py${OS}:"${TAG}"
docker tag ${DOCKER_REGISTRY}/spark${OS}:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark${OS}:"${TAG}"

docker push "${PUBLISH_DOCKER_REGISTRY}"/spark${OS}:${TAG}
docker push "${PUBLISH_DOCKER_REGISTRY}"/spark-py${OS}:${TAG}
