#!/bin/bash

set -ex

REPO=docker-private.infra.cloudera.com/cloudera
while getopts "*r:" option
do
 case "${option}" in
 r)  REPO=${OPTARG}
     echo "Building for repo $REPO";;
 *)  echo "Usage  $0 [-r]"
     echo "Options:"
     echo "  -r Docker repo in format docker-private.infra.cloudera.com/cloudera"
     echo "Examples:"
     echo "cloudera/docker/build_all.sh -r docker-private.infra.cloudera.com/cloudera"
     exit 1 ;;
 esac
done

SPARK_VERSION=$(build/mvn help:evaluate -Dexpression=project.version \
          -Dcdpd.build=true -pl :spark-parent_2.11 2>/dev/null | \
          grep -v "INFO" | tail -n 1 )

CDH_VERSION=$(build/mvn -Dcdh.build=true \
                help:evaluate -Dexpression=hadoop.version  \
                |  grep -v "INFO" \
                | tail -n 1 \
                | cut -d'.' -f4-)


if [[ "$SPARK_VERSION" != 2* ]]; then
  echo "Detected spark version (version=$SPARK_VERSION) does not start with 2"
  exit 1
fi

if [[ -z "$CDH_VERSION" ]]; then
    >&2 my_echo "Unable to find the version of CDPD, Spark 2 was built against."
    exit 1
fi

declare -a OSES=("alpine" "slim")
declare -a FLAVORS=("python")

SPARK_VERSION_LC=$(echo "$SPARK_VERSION" | awk '{print tolower($0)}')

printf "docker_images:\n" > cloudera/docker_images.yaml
for OS in "${OSES[@]}"
do
  echo "Building $REPO $OS-base based spark ${SPARK_VERSION_LC}"
  cloudera/docker/build.sh "$REPO" "$OS" "${SPARK_VERSION_LC}"
	printf "  spark-%s: %s/spark-%s:%s\n" "$OS" "$REPO" "$OS" "$SPARK_VERSION_LC" >> cloudera/docker_images.yaml

  for FLAVOR in "${FLAVORS[@]}"
  do
    echo "Building $REPO $OS spark ${SPARK_VERSION_LC}" -f "$FLAVOR"
    cloudera/docker/build.sh "$REPO" "$OS" "${SPARK_VERSION_LC}" -f "$FLAVOR"
	  printf "  spark-%s-%s: %s/spark-%s-%s:%s\n" "$OS" "$FLAVOR" "$REPO" "$FLAVOR" "$OS" "$SPARK_VERSION_LC" >> cloudera/docker_images.yaml

  done
done