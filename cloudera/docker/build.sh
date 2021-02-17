#!/bin/bash -ex
# Script to build the JVM and python based spark container images for a specific base OS.

SPARK_HOME=$PWD
DOCKER_IMAGE_TOOL_CMD="./bin/docker-image-tool.sh"
PUBLISH_DOCKER_REGISTRY=docker-private.infra.cloudera.com/cloudera
GBN=""

usage() {
  echo "Usage $0: REPO OS SPARK_VERSION GBN"
  echo "Example $0: docker-private.infra.cloudera.com/cloudera slim 3.1.1.3.1.7270.0-snapshot GBN"
  echo "Supported OS list: slim, ubi8"
  exit 1
}

if [[ $# -ne 4 ]]
then
  usage
fi

PUBLISH_DOCKER_REGISTRY=$1
OS=$2
SPARK_VERSION=$3
GBN=$4

TAG=${SPARK_VERSION}
# Check if Spark version is populated by the RE built system (such as 3.0.0.2.99.0.0-8)
if [[ "$SPARK_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
  # RE build system assumes that images are tagged with relase version (such as 2.99.0.0-8)
  TAG="$(echo $TAG | grep -oE "\b[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+\b")"
fi


if [[ -n $GBN ]]; then
  TAG="$TAG-$GBN"
fi

echo $PWD

# !NOTE make sure to used the same context directory is used as in the docker-image-tool.sh script
CTX_DIR="$SPARK_HOME/target/tmp/docker"

# Copy the required files to the docker image building context which is used by
# docker-image-tool.sh script
mkdir -p $CTX_DIR/base/cloudera
cp -r $SPARK_HOME/cloudera/docker/$OS $CTX_DIR/base/cloudera/$OS

DOCKERFILE_PATH="cloudera/${OS}/spark-base"
EXTRA_ARG=" -b DOCKERFILE_PATH_ARG=$DOCKERFILE_PATH "

echo "Building $OS based spark ${SPARK_VERSION} ${TAG}"
(
  ${DOCKER_IMAGE_TOOL_CMD} \
           -f "$CTX_DIR/base/cloudera/$OS/spark-base/Dockerfile" \
           -p "$CTX_DIR/base/cloudera/$OS/spark-python/Dockerfile" \
           -r "$PUBLISH_DOCKER_REGISTRY" \
           -t "$TAG" \
           -n \
           $EXTRA_ARG \
           build
)

docker tag ${PUBLISH_DOCKER_REGISTRY}/spark-py:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark-py-${OS}:"${TAG}"
docker tag ${PUBLISH_DOCKER_REGISTRY}/spark:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark-${OS}:"${TAG}"

echo "  spark-$OS: $PUBLISH_DOCKER_REGISTRY/spark-$OS" >> $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark-py-$OS: $PUBLISH_DOCKER_REGISTRY/spark-py-$OS" >> $SPARK_HOME/cloudera/docker_images.yaml

docker image rm ${PUBLISH_DOCKER_REGISTRY}/spark:"${TAG}"
docker image rm ${PUBLISH_DOCKER_REGISTRY}/spark-py:"${TAG}"

echo "Docker images built successfully"
