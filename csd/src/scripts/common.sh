#!/bin/bash
##
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

#
# Set of utility functions shared across different Spark CSDs.
#

set -ex

function log {
  timestamp=$(date)
  echo "$timestamp: $1"       #stdout
  echo "$timestamp: $1" 1>&2; #stderr
}

# Time marker for both stderr and stdout
log "Running Spark3 CSD control script..."
log "Detected CDH_VERSION of [$CDH_VERSION]"

# Check whether JDK 1.8 is available for spark 3
PATH_TO_JAVA="$JAVA_HOME/bin/java"
JAVA_VER=$("$PATH_TO_JAVA" -version 2>&1 | awk -F '"' '/version/ {print $2}')
JAVA_VER_SHORT=${JAVA_VER:0:3}
if [[ "$JAVA_VER_SHORT" != "1.8" ]]; then
  echo "Java version 1.8 is required for Spark 3."
  exit 1
fi

# Set this to not source defaults
export BIGTOP_DEFAULTS_DIR=""

export HADOOP_HOME=${HADOOP_HOME:-$(readlink -m "$CDH_HADOOP_HOME")}
export HADOOP_BIN=$HADOOP_HOME/bin/hadoop
export HDFS_BIN=$HADOOP_HOME/../../bin/hdfs
export HADOOP_CONF_DIR="$CONF_DIR/yarn-conf"
export HIVE_CONF_DIR="$CONF_DIR/hive-conf"
export HBASE_CONF_DIR="$CONF_DIR/hbase-conf"

# If SPARK3_HOME is not set, make it the default
DEFAULT_SPARK3_HOME=/opt/cloudera/parcels/SPARK3/lib/spark3/

#TODO FOLLOWING LINE RETURNS INCORRECT RESULTS. NEED TO FIX. Temporary using hardcoded value
#SPARK_HOME=${SPARK3_HOME:-${CDH_SPARK3_HOME:-$DEFAULT_SPARK3_HOME}}
SPARK_HOME="${CDH_SPARK3_HOME}"
SPARK3_HOME="${CDH_SPARK3_HOME}"


echo "SPARK_HOME is : ${SPARK_HOME}"
echo "SPARK3_HOME is: ${SPARK3_HOME}"
echo "CDH_SPARK3_HOME is: ${CDH_SPARK3_HOME}"

export SPARK_HOME=$(readlink -m "${SPARK_HOME}")

# We want to use a local conf dir
export SPARK_CONF_DIR="$CONF_DIR/spark3-conf"
if [ ! -d "$SPARK_CONF_DIR" ]; then
  mkdir "$SPARK_CONF_DIR"
fi

# Variables used when generating configs.
export SPARK_ENV="$SPARK_CONF_DIR/spark-env.sh"
export SPARK_DEFAULTS="$SPARK_CONF_DIR/spark-defaults.conf"

# Set JAVA_OPTS for the daemons
# sets preference to IPV4
export SPARK_DAEMON_JAVA_OPTS="$SPARK_DAEMON_JAVA_OPTS -Djava.net.preferIPv4Stack=true"

# Make sure PARCELS_ROOT is in the format we expect, canonicalized and without a trailing slash.
export PARCELS_ROOT=$(readlink -m "$PARCELS_ROOT")

# Make sure DEFAULT_SPARK_KAFKA_VERSION is set (since the config is only available for the gateway
# role).
DEFAULT_SPARK_KAFKA_VERSION=${DEFAULT_SPARK_KAFKA_VERSION:-None}

# Reads a line in the format "$host:$key=$value", setting those variables.
function readconf {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function get_hadoop_conf {
  local conf="$1"
  local key="$2"
  "$HDFS_BIN" --config "$conf" getconf -confKey "$key"
}

function get_default_fs {
  get_hadoop_conf "$1" "fs.defaultFS"
}

# replace $1 with $2 in file $3
function replace {
  perl -pi -e "s#${1}#${2}#g" $3
}

# Read a value from a properties file.
function read_property {
  local key="$1"
  local file="$2"
  echo $(grep "^$key=" "$file" | tail -n 1 | sed "s/^$key=\(.*\)/\\1/")
}

# Replaces a configuration in the Spark config with a new value; keeps just
# one entry for the configuration (in case the value is defined multiple times
# because of safety valves). If the new value is empty, the entry is removed from the config.
function replace_spark_conf {
  local key="$1"
  local value="$2"
  local file="$3"
  local temp="$file.tmp"
  touch "$temp"
  chown --reference="$file" "$temp"
  chmod --reference="$file" "$temp"
  grep -v "^$key=" "$file" >> "$temp"
  if [ -n "$value" ]; then
    echo "$key=$value" >> "$temp"
  fi
  mv "$temp" "$file"
}

# Prepend a given protocol string to the URL if it doesn't have a protocol.
function prepend_protocol {
  local url="$1"
  local proto="$2"
  if [[ "$url" =~ [:alnum:]*:.* ]]; then
    echo "$url"
  else
    echo "$proto$url"
  fi
}

# Blacklists certain jars from being added by optional Spark dependencies. The list here is mostly
# based on what HBase adds to the classpath, and avoids adding conflicting versions of libraries
# that Spark needs. Also avoids adding duplicate jars to the classpath since that makes the JVM open
# the same file multiple times.
#
# This also explicitly blacklists the "hbase-spark" jar since the C5 version does not work with
# Spark 2.
function is_blacklisted {
  local JAR="$1"
  if [[ -f "$SPARK_HOME/jars/$JAR" ]]; then
    return 0
  elif [[ "$JAR" =~ ^jetty.* ]]; then
    return 0
  elif [[ "$JAR" =~ ^jersey.* ]]; then
    return 0
  elif [[ "$JAR" =~ .*servlet.* ]]; then
    return 0
  elif [[ "$JAR" =~ .*-tests.* ]]; then
    return 0
  elif [[ "$JAR" =~ ^junit-.* ]]; then
    return 0
  elif [[ "$JAR" =~ ^hbase-spark.* ]]; then
    return 0
  elif [[ "$JAR" =~ ^hbase-.*-it.* ]]; then
    return 0
  elif [[ "$JAR" =~ ^parquet.* ]]; then
    return 0
  fi
  return 1
}

# Adds jars in classpath entry $2 to the classpath file $1, resolving symlinks so that duplicates
# can be removed. Jars already present in Spark's distribution are also ignored. See CDH-27596.
function add_to_classpath {
  local CLASSPATH_FILE="$1"
  local CLASSPATH="$2"
  local SPARK_JARS="$SPARK3_HOME/jars"

  # Break the classpath into individual entries
  IFS=: read -a CLASSPATH_ENTRIES <<< "$CLASSPATH"

  for pattern in "${CLASSPATH_ENTRIES[@]}"; do
    for entry in $pattern; do
      entry=$(readlink -m "$entry")
      name=$(basename "$entry")
      if [ -f "$entry" ] && ! is_blacklisted "$name" && ! grep -q "/$name\$" "$CLASSPATH_FILE"
      then
        echo "$entry" >> "$CLASSPATH_FILE"
      fi
    done
  done
}

# Prepare the spark-env.sh file specified in $1 for use.
function prepare_spark_env {
  local client="$1"
  replace "{{HADOOP_HOME}}" "$HADOOP_HOME" "$SPARK_ENV"
  replace "{{SPARK_HOME}}" "$SPARK_HOME" "$SPARK_ENV"
  replace "{{SPARK_EXTRA_LIB_PATH}}" "$SPARK_LIBRARY_PATH" "$SPARK_ENV"
  replace "{{PYTHON_PATH}}" "$PYTHON_PATH" "$SPARK_ENV"
  replace "{{CDH_PYTHON}}" "$CDH_PYTHON" "$SPARK_ENV"
  replace "{{DEFAULT_SPARK_KAFKA_VERSION}}" "$DEFAULT_SPARK_KAFKA_VERSION" "$SPARK_ENV"

  local CLASSPATH_FILE="$(dirname $SPARK_ENV)/classpath.txt"
  local CLASSPATH_FILE_TMP="${CLASSPATH_FILE}.tmp"
  touch "$CLASSPATH_FILE_TMP"

  local HADOOP_CLASSPATH=$($HADOOP_BIN --config "$HADOOP_CONF_DIR" classpath)
  add_to_classpath "$CLASSPATH_FILE_TMP" "$HADOOP_CLASSPATH"

  # For client configs, add HBase jars if the service dependency is configured.
  if [ $client = 1 ] && [ -d "$HBASE_CONF_DIR" ]; then
    local HBASE_CP="$(hbase --config $HBASE_CONF_DIR classpath)"
    add_to_classpath "$CLASSPATH_FILE_TMP" "$HBASE_CP"
  fi

  # De-duplicate the classpath when creating the target file.
  cat "$CLASSPATH_FILE_TMP" | sort | uniq > "$CLASSPATH_FILE"
  rm -f "$CLASSPATH_FILE_TMP"
}

# Check whether the given config key ($1) exists in the given conf file ($2).
function has_config {
  local key="$1"
  local file="$2"
  grep -q "^$key=" "$file"
}

# Appends an item ($2) to a comma-separated list ($1).
function add_to_list {
  local list="$1"
  local item="$2"
  if [ -n "$list" ]; then
    list="$list,$item"
  else
    list="$item"
  fi
  echo "$list"
}

# Set a configuration key ($1) to a value ($2) in the file ($3) only if it hasn't already been
# set by the user.
function set_config {
  local key="$1"
  local value="$2"
  local file="$3"
  if ! has_config "$key" "$file"; then
    echo "$key=$value" >> "$file"
  fi
}

# Parse a x.y.z version into an integer.
function parse_version_string {
  echo "$1" | awk -F. '{ printf("%02d%02d%02d", $1, $2, $3) }'
}

# Copies config files from a source directory ($1) into the Spark client config dir ($2).
# $3 should be the final location of the client config. Ignores logging configuration, and
# does not overwrite files, so that multiple source config directories can be merged.
function copy_client_config {
  local source_dir="$1"
  local target_dir="$2"
  local dest_dir="$3"

  for i in "$source_dir"/*; do
    if [ $(basename "$i") != log4j.properties ]; then
      mv $i "$target_dir"

      # CDH-28425. Because of OPSAPS-25695, we need to fix the YARN config ourselves.
      target="$target_dir/$(basename $i)"
      replace "{{CDH_MR2_HOME}}" "$CDH_MR2_HOME" "$target"
      replace "{{HADOOP_CLASSPATH}}" "" "$target"
      replace "{{JAVA_LIBRARY_PATH}}" "" "$target"
      replace "{{CMF_CONF_DIR}}" "$dest_dir" "$target"
    fi
  done
}

function run_spark_class {
  local ARGS=($@)
  ARGS+=($ADDITIONAL_ARGS)
  prepare_spark_env 0
  export SPARK_DAEMON_JAVA_OPTS="$CSD_JAVA_OPTS $SPARK_DAEMON_JAVA_OPTS"
  export SPARK_JAVA_OPTS="$CSD_JAVA_OPTS $SPARK_JAVA_OPTS"
  cmd="$SPARK_HOME/bin/spark-class ${ARGS[@]}"
  echo "Running [$cmd]"
  exec $cmd
}

function start_history_server {
  log "Starting Spark History Server"
  local CONF_FILE="$SPARK_CONF_DIR/spark-history-server.conf"
  local DEFAULT_FS=$(get_default_fs $HADOOP_CONF_DIR)
  local LOG_DIR=$(prepend_protocol "$HISTORY_LOG_DIR" "$DEFAULT_FS")

  # Make a defensive copy of the config file; when startup fails, CM will retry the same
  # process again, so we want to append configs to the original config file, not to the
  # update version.
  if [ ! -f "$CONF_FILE.orig" ]; then
    cp -p "$CONF_FILE" "$CONF_FILE.orig"
  fi
  cp -p "$CONF_FILE.orig" "$CONF_FILE"

  echo "spark.history.fs.logDirectory=$LOG_DIR" >> "$CONF_FILE"
  if [ "$SPARK_PRINCIPAL" != "" ]; then
    echo "spark.history.kerberos.enabled=true" >> "$CONF_FILE"
    echo "spark.history.kerberos.principal=$SPARK_PRINCIPAL" >> "$CONF_FILE"
    echo "spark.history.kerberos.keytab=spark3_on_yarn.keytab" >> "$CONF_FILE"
  fi

  local FILTERS_KEY="spark.ui.filters"
  local FILTERS=$(read_property "$FILTERS_KEY" "$CONF_FILE")

  if [ "$YARN_PROXY_REDIRECT" = "true" ]; then
    FILTERS=$(add_to_list "$FILTERS" "org.apache.spark.deploy.yarn.YarnProxyRedirectFilter")
  fi

  if [ "$ENABLE_SPNEGO" = "true" ] && [ -n "$SPNEGO_PRINCIPAL" ]; then
    local AUTH_FILTER="org.apache.hadoop.security.authentication.server.AuthenticationFilter"
    FILTERS=$(add_to_list "$FILTERS" "$AUTH_FILTER")

    local FILTER_CONF_KEY="spark.$AUTH_FILTER.param"
    echo "$FILTER_CONF_KEY.type=kerberos" >> "$CONF_FILE"
    echo "$FILTER_CONF_KEY.kerberos.principal=$SPNEGO_PRINCIPAL" >> "$CONF_FILE"
    echo "$FILTER_CONF_KEY.kerberos.keytab=spark3_on_yarn.keytab" >> "$CONF_FILE"

    # This config may contain new lines and backslashes, so it needs to be handled in a special way.
    # To preserve those characters in Java properties files, replace them with the respective
    # unicode escape sequence.
    local AUTH_TO_LOCAL=$(get_hadoop_conf "$HADOOP_CONF_DIR" "hadoop.security.auth_to_local" |
      sed 's,\\,\\u005C,g' |
      awk '{printf "%s\\u000A", $0}')
    echo "$FILTER_CONF_KEY.kerberos.name.rules=$AUTH_TO_LOCAL" >> "$CONF_FILE"

    # Also enable ACLs in the History Server, otherwise auth is not very useful.
    echo "spark.history.ui.acls.enable=true" >> "$CONF_FILE"
  fi

  if [ -n "$FILTERS" ]; then
    replace_spark_conf "$FILTERS_KEY" "$FILTERS" "$CONF_FILE"
  fi

  # Write the keystore password to the config file. Disable logging while doing that.
  set +x
  if [ -n "$KEYSTORE_PASSWORD" ]; then
    echo "spark.ssl.historyServer.keyStorePassword=$KEYSTORE_PASSWORD" >> "$CONF_FILE"
  fi
  set -x

  # If local storage is not configured, remove the entry from the properties file, since the
  # mere presence of the configuration enables the feature.
  if [ "$ENABLE_LOCAL_STORAGE" != "true" ]; then
    replace_spark_conf "spark.history.store.path" "" "$CONF_FILE"
  fi

  ARGS=(
    "org.apache.spark.deploy.history.HistoryServer"
    "--properties-file"
    "$CONF_FILE"
  )
  run_spark_class "${ARGS[@]}"
}

function deploy_client_config {
  log "Deploying client configuration"

  prepare_spark_env 1

  set_config 'spark.master' 'yarn' "$SPARK_DEFAULTS"
  set_config 'spark.submit.deployMode' "$DEPLOY_MODE" "$SPARK_DEFAULTS"

  if [ -n "$PYTHON_PATH" ]; then
    echo "spark.executorEnv.PYTHONPATH=$PYTHON_PATH" >> $SPARK_DEFAULTS
  fi

  # Move the Yarn configuration under the Spark config. Do not overwrite Spark's log4j config.
  HADOOP_CONF_NAME=$(basename "$HADOOP_CONF_DIR")
  HADOOP_CLIENT_CONF_DIR="$SPARK_CONF_DIR/$HADOOP_CONF_NAME"
  TARGET_HADOOP_CONF_DIR="$DEST_PATH/$HADOOP_CONF_NAME"

  mkdir "$HADOOP_CLIENT_CONF_DIR"
  copy_client_config "$HADOOP_CONF_DIR" "$HADOOP_CLIENT_CONF_DIR" "$TARGET_HADOOP_CONF_DIR"

  # If there is a Hive configuration directory, then copy all the extra files into the Hadoop
  # conf dir - so that Spark automatically distributes them - and update the configuration to
  # enable the use of the Hive metastore.
  local catalog_impl='in-memory'
  if [ -d "$HIVE_CONF_DIR" ]; then
    local hive_metastore_jars="\${env:HADOOP_COMMON_HOME}/../hive/lib/*"
    hive_metastore_jars="$hive_metastore_jars:\${env:HADOOP_COMMON_HOME}/client/*"
    set_config 'spark.sql.hive.metastore.jars' "$hive_metastore_jars" "$SPARK_DEFAULTS"
    set_config 'spark.sql.hive.metastore.version' '1.1.0' "$SPARK_DEFAULTS"
    copy_client_config "$HIVE_CONF_DIR" "$HADOOP_CLIENT_CONF_DIR" "$TARGET_HADOOP_CONF_DIR"
    catalog_impl='hive'
  fi
  set_config 'spark.sql.catalogImplementation' "$catalog_impl" "$SPARK_DEFAULTS"

  # If there's an HBase configuration directory, copy its files to the Spark config dir.
  if [ -d "$HBASE_CONF_DIR" ]; then
    copy_client_config "$HBASE_CONF_DIR" "$HADOOP_CLIENT_CONF_DIR" "$TARGET_HADOOP_CONF_DIR"
  fi

  DEFAULT_FS=$(get_default_fs "$HADOOP_CLIENT_CONF_DIR")

  # SPARK 1.1 makes "file:" the default protocol for the location of event logs. So we need
  # to fix the configuration file to add the protocol. But if the user has specified a path
  # with a protocol, don't overwrite it.
  key="spark.eventLog.dir"
  value=$(read_property "$key" "$SPARK_DEFAULTS")
  if [ -n "$value" ]; then
    value=$(prepend_protocol "$value" "$DEFAULT_FS")
    replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"
  fi

  # If a history server is configured, set its address in the default config file so that
  # the Yarn RM web ui links to the history server for Spark apps.
  HISTORY_PROPS="$SPARK_CONF_DIR/history2.properties"
  HISTORY_HOST=
  if [ -f "$HISTORY_PROPS" ]; then
    for line in $(cat "$HISTORY_PROPS")
    do
      readconf "$line"
      case $key in
       (spark.history.ui.port)
         HISTORY_HOST="$host"
         HISTORY_PORT="$value"
       ;;
      esac
    done
    if [ -n "$HISTORY_HOST" ]; then
      echo "spark.yarn.historyServer.address=http://$HISTORY_HOST:$HISTORY_PORT" >> \
        "$SPARK_DEFAULTS"
    fi
    rm "$HISTORY_PROPS"
  fi

  # If no Spark jars are defined, look for the location of jars on the local filesystem,
  # which we assume will be the same across the cluster.
  key="spark.yarn.jars"
  value=$(read_property "$key" "$SPARK_DEFAULTS")
  if [ -n "$value" ]; then
    local prefixed_values=
    # the value is a comma separated list of files (which can be globs)
    # Where needed, let's prefix it with the FS scheme.
    IFS=',' read -ra VALUES <<< "$value"
    for i in "${VALUES[@]}"; do
      # Add the new entry to it, and then add a comma too.
      prefixed_values=${prefixed_values}$(prepend_protocol "$i" "$DEFAULT_FS")","
    done
    # Take out the last extra comma at the end, if it exists
    prefixed_values=$(sed 's/,$//' <<< $prefixed_values)
  else
    value="local:$SPARK_HOME/jars/*"
  fi
  replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"

  # Set the default library paths for drivers and executors.
  EXTRA_LIB_PATH="$HADOOP_HOME/lib/native"
  if [ -n "$SPARK_LIBRARY_PATH" ]; then
    EXTRA_LIB_PATH="$EXTRA_LIB_PATH:$SPARK_LIBRARY_PATH"
  fi
  for i in driver executor yarn.am; do
    key="spark.${i}.extraLibraryPath"
    value=$(read_property "$key" "$SPARK_DEFAULTS")
    if [ -n "$value" ]; then
      value="$value:$EXTRA_LIB_PATH"
    else
      value="$EXTRA_LIB_PATH"
    fi
    replace_spark_conf "$key" "$value" "$SPARK_DEFAULTS"
  done

  # Override the YARN / MR classpath configs since we already include them when generating
  # SPARK_DIST_CLASSPATH. This avoids having the same paths added to the classpath a second
  # time and wasting file descriptors.
  replace_spark_conf "spark.hadoop.mapreduce.application.classpath" "" "$SPARK_DEFAULTS"
  replace_spark_conf "spark.hadoop.yarn.application.classpath" "" "$SPARK_DEFAULTS"

  # If using parcels, write extra configuration that tells Spark to replace the parcel
  # path with references to the NM's environment instead, so that users can have different
  # paths on each node.
  if [ -n "$PARCELS_ROOT" ]; then
    echo "spark.yarn.config.gatewayPath=$PARCELS_ROOT" >> "$SPARK_DEFAULTS"
    echo "spark.yarn.config.replacementPath={{HADOOP_COMMON_HOME}}/../../.." >> "$SPARK_DEFAULTS"
  fi

  if [ -n "$CDH_PYTHON" ]; then
    key="spark.yarn.appMasterEnv.PYSPARK_PYTHON"
    if ! has_config "$key" "$SPARK_DEFAULTS"; then
      echo "spark.yarn.appMasterEnv.PYSPARK_PYTHON=$CDH_PYTHON" >> "$SPARK_DEFAULTS"
    fi
    key="spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON"
    if ! has_config "$key" "$SPARK_DEFAULTS"; then
      echo "spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=$CDH_PYTHON" >> "$SPARK_DEFAULTS"
    fi
  fi

  # These values cannot be declared in the descriptor, since the CSD framework will
  # treat them as config references and fail. So add them here unless they've already
  # been set by the user in the safety valve.
  LOG_CONFIG="$SPARK_CONF_DIR/log4j.properties"
  SHELL_CLASSES=("org.apache.spark.repl.Main"
    "org.apache.spark.api.python.PythonGatewayServer")
  for class in "${SHELL_CLASSES[@]}"; do
    key="log4j.logger.$class"
    if ! has_config "$key" "$LOG_CONFIG"; then
      echo "$key=\${shell.log.level}" >> "$LOG_CONFIG"
    fi
  done

  local LINEAGE_ENABLED_KEY="spark.lineage.enabled"

  # Detect whether a supported CM version for lineage is being used, otherwise disable the
  # feature
  local MIN_CM_VERSION=$(parse_version_string "5.14.0")
  local LINEAGE_SUPPORTED=0
  if [ -n "$CM_VERSION" ]; then
    if [ $(parse_version_string "$CM_VERSION") -ge $MIN_CM_VERSION ]; then
      LINEAGE_SUPPORTED=1
    fi
  fi

  if [ $LINEAGE_SUPPORTED = 0 ]; then
    log "Spark 2 lineage is not supported by CM, disabling."
    replace_spark_conf "$LINEAGE_ENABLED_KEY" "false" "$SPARK_DEFAULTS"
  fi

  # If lineage is enabled, add the Navigator listeners to the client config.
  local LINEAGE_ENABLED=$(read_property "$LINEAGE_ENABLED_KEY" "$SPARK_DEFAULTS")
  if [ "$LINEAGE_ENABLED" = "true" ]; then
    local SC_LISTENERS_KEY="spark.extraListeners"
    local SQL_LISTENERS_KEY="spark.sql.queryExecutionListeners"
    local LINEAGE_PKG="com.cloudera.spark.lineage"

    local LISTENERS=$(read_property "$SC_LISTENERS_KEY" "$SPARK_DEFAULTS")
    LISTENERS=$(add_to_list "$LISTENERS" "$LINEAGE_PKG.NavigatorAppListener")
    replace_spark_conf "$SC_LISTENERS_KEY" "$LISTENERS" "$SPARK_DEFAULTS"

    local LISTENERS=$(read_property "$SQL_LISTENERS_KEY" "$SPARK_DEFAULTS")
    LISTENERS=$(add_to_list "$LISTENERS" "$LINEAGE_PKG.NavigatorQueryListener")
    replace_spark_conf "$SQL_LISTENERS_KEY" "$LISTENERS" "$SPARK_DEFAULTS"
  fi
}

function clean_history_cache {
  local STORAGE_DIR="$1"
  log "Cleaning history server cache in $STORAGE_DIR"

  if [ -d "$STORAGE_DIR" ]; then
    rm -rf "$STORAGE_DIR"/*
  fi
}