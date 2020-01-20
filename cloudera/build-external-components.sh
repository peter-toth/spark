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

# Script is expected to be called by the './dev/make-distribution.sh' in order to build and include additional external
# components to the Spark distribution. The script is intended to be called after Spark Maven build, since some of
# external components may depend on Spark artifacts. Can be called from any working directory.
# Requirements: JAVA_HOME needs to be set, Python 2.7 or higher
SPARK_HOME="$(cd "$(dirname "$0")"/..; pwd)"
MVN="$SPARK_HOME/build/mvn"

function log {
  echo "EXTERNAL_COMPONENTS: $1"
}

# Builds external components and prints comma-separated list of jar paths to stdout
function do_build_external_components {
  local MAVEN_VERSION=$("$MVN" help:evaluate -Dexpression=maven.version | grep -e '^[^\[]')
  BUILD_OPTS="-Divy.home=${HOME}/.ivy2 -Dsbt.ivy.home=${HOME}/.ivy2 -Duser.home=${HOME} \
              -Drepo.maven.org=$IVY_MIRROR_PROP -s $SPARK_HOME/build/apache-maven-${MAVEN_VERSION}/conf/settings.xml \
              -Dreactor.repo=file://${HOME}/.m2/repository${M2_REPO_SUFFIX} -DskipTests"
  # this might be an issue at times
  # http://maven.40175.n5.nabble.com/Not-finding-artifact-in-local-repo-td3727753.html
  export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m -XX:PermSize=1024m -XX:MaxPermSize=1024m"
  # Build Spark Atlas connector
  local ATLAS_CONNECTOR_OWNER=${ATLAS_CONNECTOR_OWNER:-CDH}
  local ATLAS_CONNECTOR_BRANCH=${ATLAS_CONNECTOR_BRANCH:-spark3-3.0.0}
  local ATLAS_CONNECTOR_DIR=${SPARK_HOME}/build/spark-atlas-"${ATLAS_CONNECTOR_BRANCH//\//\-}"
  if [[ ! -d $ATLAS_CONNECTOR_DIR ]]; then
    git clone "git://github.mtv.cloudera.com/${ATLAS_CONNECTOR_OWNER}/spark-atlas-connector.git" $ATLAS_CONNECTOR_DIR
  fi
  ATLAS_CONNECTOR_BUILD_LOG="$SPARK_HOME/sac-build-output.log"
  (
  cd $ATLAS_CONNECTOR_DIR
  git fetch
  git checkout $ATLAS_CONNECTOR_BRANCH  > /dev/null 2>&1
  "$MVN" clean package -DskipTests $BUILD_OPTS > "$ATLAS_CONNECTOR_BUILD_LOG" 2>&1
  )
  ls $ATLAS_CONNECTOR_DIR/spark-atlas-connector/target/spark-atlas-connector*.jar
  local ATLAS_CONNECTOR_JAR=$(ls $ATLAS_CONNECTOR_DIR/spark-atlas-connector/target/spark-atlas-connector*.jar)
  local ATLAS_CONNECTOR_ASSEMBLY_JAR=$(ls $ATLAS_CONNECTOR_DIR/spark-atlas-connector-assembly/target/spark-atlas-*.jar)
  # Print result to stdout
  echo "$ATLAS_CONNECTOR_JAR,$ATLAS_CONNECTOR_ASSEMBLY_JAR"
}

ADDITIONAL_JARS=$(do_build_external_components)

# Loop over comma-separated list of additional jar paths.
for jar in ${ADDITIONAL_JARS//,/ }
do
  if [ ! -f "$jar" ]; then
    log "Error: Could not find '$jar'."
    log "Build output:"
    cat "$ATLAS_CONNECTOR_BUILD_LOG"
    exit 1
  fi
done

log "External components build completed. Success!"
