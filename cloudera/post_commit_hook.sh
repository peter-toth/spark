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

# Build machines seem to have a CDH-based settings.xml in their maven config directory,
# which breaks CDPD builds. If that file is found, override it with an empty one.
if [ -f "$HOME/.m2/settings.xml" ]; then
  mkdir -p target
  cat > target/settings.xml <<EOF
<settings></settings>
EOF
  MAVEN_ARGS="-s target/settings.xml"
else
  MAVEN_ARGS=""
fi
export APACHE_MIRROR=http://mirror.infra.cloudera.com/apache
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.6 -y
sudo rm /usr/bin/python3
sudo ln -s /usr/bin/python3.6 /usr/bin/python3
python3 --version
./build/mvn -B $MAVEN_ARGS -Dcdpd.build=true package -fae -Dmaven.repo.local="$MVN_REPO_LOCAL" $EXTRA_MAVEN_ARGS
