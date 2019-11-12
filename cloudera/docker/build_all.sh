set -ex
SPARK_VERSION=$(build/mvn help:evaluate -Dexpression=project.version \
          -Dcdpd.build=true -pl :spark-parent_2.11 2>/dev/null | \
          grep -v "INFO" | tail -n 1)

CDH_VERSION=$(build/mvn -Dcdh.build=true \
                help:evaluate -Dexpression=hadoop.version  \
                |  grep -v "INFO" \
                | tail -n 1 \
                | cut -d'.' -f4-)

repo=cloudera

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

for os in "${OSES[@]}"
do
  echo "Building $os based spark-base ${SPARK_VERSION}"
  cloudera/docker/build.sh "$repo" "$os" "spark-base" "${SPARK_VERSION}"
  for FLAVOR in "${FLAVORS[@]}"
  do
    echo "Building $os based spark-${FLAVOR} ${SPARK_VERSION}" -f
    cloudera/docker/build.sh "$repo" "$os" "spark-$FLAVOR" "${SPARK_VERSION}" -f
  done
done