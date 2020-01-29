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

TAG=${SPARK_VERSION}
# Check if Spark version is populated by the RE built system (such as 3.0.0.2.99.0.0-8)
if [[ "$SPARK_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+$ ]]; then
  # RE build system assumes that images are tagged with relase version (such as 2.99.0.0-8)
  TAG=$(echo $TAG | grep -oE "\b[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+-[0-9]+\b")
fi

# It's not possible to push multiple images with the same name and different tags via RE build system
echo "Building $os based spark ${SPARK_VERSION}"
(
  cd $SPARK_HOME/dist
  # Dockerfiles must be within the build context for Docker < 18.03
  mkdir spark-base/
  mkdir spark-python/
  cp "$SPARK_HOME/cloudera/docker/slim/spark-base/Dockerfile" spark-base/
  cp "$SPARK_HOME/cloudera/docker/slim/spark-python/Dockerfile" spark-python/
  ${DOCKER_IMAGE_TOOL_CMD} \
           -f "spark-base/Dockerfile" \
           -p "spark-python/Dockerfile" \
           -r "$repo" \
           -t "${TAG}" \
           build
)

docker tag cloudera/spark-py:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark-py:"${TAG}"
docker tag cloudera/spark:"${TAG}" "${PUBLISH_DOCKER_REGISTRY}"/spark:"${TAG}"

echo "docker_images:" > $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark: $PUBLISH_DOCKER_REGISTRY/spark" >> $SPARK_HOME/cloudera/docker_images.yaml
echo "  spark-py: $PUBLISH_DOCKER_REGISTRY/spark-py" >> $SPARK_HOME/cloudera/docker_images.yaml

echo "Docker images built successfully"
