#!/bin/bash

usage() {
  echo "Usage $0: REGISTRY"
  echo "Example $0: docker-sandbox.infra.cloudera.com/denis.royz"
  exit 1
}

if [[ $# -ne 1 ]]
then
  usage
fi

VERSION=$(build/mvn help:evaluate -Dexpression=project.version \
  -Dcdpd.build=true -pl :spark-parent_2.11 2>/dev/null | \
  grep -v "INFO" | tail -n 1)

REGISTRY=$1

docker tag cloudera/spark:spark-python-"${VERSION}"-alpine "${REGISTRY}"/spark:spark-python-"${VERSION}"-alpine
docker tag cloudera/spark:spark-base-"${VERSION}"-alpine "${REGISTRY}"/spark:spark-base-"${VERSION}"-alpine
docker tag cloudera/spark:spark-python-"${VERSION}"-slim "${REGISTRY}"/spark:spark-python-"${VERSION}"-slim
docker tag cloudera/spark:spark-base-"${VERSION}"-slim "${REGISTRY}"/spark:spark-base-"${VERSION}"-slim

docker push "${REGISTRY}"/spark:spark-base-"${VERSION}"-alpine
docker push "${REGISTRY}"/spark:spark-python-"${VERSION}"-alpine
docker push "${REGISTRY}"/spark:spark-base-"${VERSION}"-slim
docker push "${REGISTRY}"/spark:spark-python-"${VERSION}"-slim