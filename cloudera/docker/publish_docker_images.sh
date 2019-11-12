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
  -Dcdpd.build=true -o -pl :spark-parent_2.12 2>/dev/null | \
  grep -v "INFO" | tail -n 1)

REGISTRY=$1

docker tag cloudera/spark-py:"${VERSION}"-slim "${REGISTRY}"/spark-py:"${VERSION}"-slim
docker tag cloudera/spark:"${VERSION}"-slim "${REGISTRY}"/spark:"${VERSION}"-slim

docker push "${REGISTRY}"/spark:"${VERSION}"-slim
docker push "${REGISTRY}"/spark-py:"${VERSION}"-slim
