#!/bin/bash -ex
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

SPARK_HOME="$(cd "$(dirname "$0")"/..; pwd)"
PUBLISH_DOCKER_REGISTRY=docker-private.infra.cloudera.com/spark/spark

function log {
  echo "BUILD_DOCKER_IMAGES: $1"
}

function do_build {
  log "Building Spark on k8s docker image"
  (
  cd $SPARK_HOME/dist
  LANGUAGE_BINDING_BUILD_ARGS="-p kubernetes/dockerfiles/spark/bindings/python/Dockerfile"
  bin/docker-image-tool.sh -r ${PUBLISH_DOCKER_REGISTRY} -t ${TAG} ${LANGUAGE_BINDING_BUILD_ARGS} build
  )
  log "Spark on k8s docker build succeeded"
}


while [[ $# -ge 1 ]]; do
  arg=$1
  case $arg in
    --tag)
    TAG="$2"
    ;;
    *)
    ;;
  esac
  shift
done

if [ ! -n "$TAG" ]; then
  log "Error: Tag must be specified using '--tag' option."
  exit 1
fi

do_build
echo "docker_images:" > $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark: $PUBLISH_DOCKER_REGISTRY/spark" >> $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark-py: $PUBLISH_DOCKER_REGISTRY/spark-py" >> $SPARK_HOME/cloudera/docker_images.yaml

log "Docker images build completed. Success!"
