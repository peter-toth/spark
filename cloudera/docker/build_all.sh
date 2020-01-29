#!/bin/bash -ex

PUBLISH_DOCKER_REGISTRY=docker-private.infra.cloudera.com/cloudera
SPARK_HOME="$(cd "$(dirname "$0")"/../..; pwd)"
DOCKER_IMAGE_TOOL_CMD="./bin/docker-image-tool.sh"

while [[ $# -ge 1 ]]; do
  arg=$1
  case $arg in
    --registry)
    PUBLISH_DOCKER_REGISTRY="$2"
    ;;
    *)
    ;;
  esac
  shift
done

SPARK_VERSION=$(build/mvn help:evaluate -Dexpression=project.version \
          -Dcdpd.build=true -pl :spark-parent_2.12 2>/dev/null | \
          grep -v "INFO" | tail -n 1)

CDH_VERSION=$(build/mvn -Dcdh.build=true \
                help:evaluate -Dexpression=hadoop.version  \
                |  grep -v "INFO" \
                | tail -n 1 \
                | cut -d'.' -f4-)

repo=cloudera

if [[ "$SPARK_VERSION" != 3* ]]; then
  echo "Detected spark version (version=$SPARK_VERSION) does not start with 3"
  exit 1
fi

if [[ -z "$CDH_VERSION" ]]; then
    >&2 my_echo "Unable to find the version of CDPD, Spark 3 was built against."
    exit 1
fi

# It's not possible to push multiple images with the same name and different tags via RE build system
echo "Building $os based spark ${SPARK_VERSION}"
(
  cd $SPARK_HOME/dist
  ${DOCKER_IMAGE_TOOL_CMD} \
           -f "$SPARK_HOME/cloudera/docker/slim/spark-base/Dockerfile" \
           -p "$SPARK_HOME/cloudera/docker/slim/spark-python/Dockerfile" \
           -r "$repo" \
           -t "${SPARK_VERSION}-slim" \
           build
)

docker tag cloudera/spark-py:"${SPARK_VERSION}"-slim "${PUBLISH_DOCKER_REGISTRY}"/spark-py:"${SPARK_VERSION}"-slim
docker tag cloudera/spark:"${SPARK_VERSION}"-slim "${PUBLISH_DOCKER_REGISTRY}"/spark:"${SPARK_VERSION}"-slim

echo "docker_images:" > $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark: $PUBLISH_DOCKER_REGISTRY/spark" >> $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark-py: $PUBLISH_DOCKER_REGISTRY/spark-py" >> $SPARK_HOME/cloudera/docker_images.yaml

echo "Docker images built successfully"
