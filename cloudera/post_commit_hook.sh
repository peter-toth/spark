#!/bin/bash
#
# This script (post_commit_hook.sh) is executed by post-commit jobs.
#
# This script is called from inside the Spark source code directory, and it
# is used to build and test the current Spark code.
#

# -e will make the script exit if an error happens on any command executed
set -ex

export PATH=${JAVA_HOME}/bin:${PATH}

# To make some of the output quieter
export AMPLAB_JENKINS=1

MVN_REPO_LOCAL=$HOME/.m2/repository

export MAVEN_OPTS="-XX:ReservedCodeCacheSize=512m"

export APACHE_MIRROR=http://mirror.infra.cloudera.com/apache
export SPARK_TESTING=1

if [[ $SCALA_CODE_COVERAGE == 1 ]]; then
  MAVEN_GOAL="scoverage:report"
  echo "Running UTs with code coverage"
else
  MAVEN_GOAL="package"
fi

if [[ $SONAR == 1 ]]; then
  # Fail fast since UTs with code coverage takes about 10.5 hours to complete.
  [[ -z "$SONAR_PROJECT_KEY" ]] && echo "SONAR_PROJECT_KEY is not set" && exit 1
  [[ -z "$SONAR_PROJECT_NAME" ]] && echo "SONAR_PROJECT_NAME is not set" && exit 1
  [[ -z "$SONAR_HOST_URL" ]] && echo "SONAR_HOST_URL is not set" && exit 1
fi

./build/mvn -B -Dcdpd.build=true $MAVEN_GOAL -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS

if [[ $SONAR == 1 ]]; then
  echo "Report code coverage to Sonar"
  ./build/mvn -B -Dcdpd.build=true -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS \
  -Dsonar.projectKey="$SONAR_PROJECT_KEY" \
  -Dsonar.projectName="$SONAR_PROJECT_NAME" \
  -Dsonar.host.url="$SONAR_HOST_URL" \
  -DskipTests package sonar:sonar
fi
