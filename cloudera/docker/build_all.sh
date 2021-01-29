#!/bin/bash -ex
# Script to build the JVM and python based spark container images for all of the
# available OS version.
declare -a OSES=("slim" "ubi8")

PUBLISH_DOCKER_REGISTRY=docker-private.infra.cloudera.com/cloudera
GBN=""
SPARK_HOME="$(cd "$(dirname "$0")"/../..; pwd)"

while [[ $# -ge 1 ]]; do
  arg=$1
  case $arg in
    --registry)
    PUBLISH_DOCKER_REGISTRY="$2"
    ;;
    --gbn)
    GBN="$2"
    ;;
    *)
    ;;
  esac
  shift
done

function my_echo {
  echo "BUILD_SCRIPT: $1"
}

# Execute everything from the repo root.
cd $SPARK_HOME

SPARK_VERSION=$(./build/mvn help:evaluate -Dexpression=project.version \
          -Dcdpd.build=true -pl :spark-parent_2.12 2>/dev/null | \
          grep -v "INFO" | tail -n 1)

CDH_VERSION=$(./build/mvn -Dcdh.build=true \
                help:evaluate -Dexpression=hadoop.version  \
                |  grep -v "INFO" \
                | tail -n 1 \
                | cut -d'.' -f4-)

if [[ "$SPARK_VERSION" != 3* ]]; then
  echo "Detected spark version (version=$SPARK_VERSION) does not start with 3"
  exit 1
fi

if [[ -z "$CDH_VERSION" ]]; then
    >&2 my_echo "Unable to find the version of CDPD, Spark 3 was built against."
    exit 1
fi

SPARK_VERSION_LC=$(echo "$SPARK_VERSION" | awk '{print tolower($0)}')

echo "docker_images:" > $SPARK_HOME/cloudera/docker_images.yaml

for OS in "${OSES[@]}"
do
  echo "Building $PUBLISH_DOCKER_REGISTRY $OS-base based spark ${SPARK_VERSION_LC}"
  ./cloudera/docker/build.sh "$PUBLISH_DOCKER_REGISTRY" "$OS" "${SPARK_VERSION_LC}" "$GBN"
done
