#!/bin/bash
# Licensed to Cloudera, Inc. under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  Cloudera, Inc. licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


set -x
CMD=$1

function log {
  echo "$(date): $@"
}

function readconf() {
  local conf
  IFS=':' read host conf <<< "$1"
  IFS='=' read key value <<< "$conf"
}

function get_prop() {
  validateProperty=$(sed '/^\#/d' $2 | grep "^$1\s*="  | tail -n 1) # for validation
  if  test -z "$validateProperty" ; then log "[E] '$1' not found in $2 file while getting....!!"; exit 1; fi
  value=$(echo $validateProperty | cut -d "=" -f2-)
  echo $value
}

log "Generating Atlas client configurations."
ATLAS_SERVER_PROPERTIES=${1}
ATLAS_APPLICATION_PROPERTIES=${2}
ATLAS_ZK_QUORUM=${3}
ATLAS_KAFKA_BOOTSTRAP_SERVERS=${4}
ATLAS_CLIENT_PROPERTIES=${5}
KAFKA_SECURITY_PROTOCOL=${6}
ATLAS_KAFKA_ZK_QUORUM=${KAFKA_ZK_QUORUM:-$ATLAS_ZK_QUORUM}
ATLAS_KERBEROS_ENABLED="false"
ATLAS_SSL_ENABLED="false"
ATLAS_SERVER_PORT="31000"
ATLAS_SERVER_HOSTS_COUNT=0
ATLAS_HOSTS_LIST=""
for line in $(cat ${ATLAS_SERVER_PROPERTIES})
  do
    readconf "${line}"
    case ${key} in
      (atlas.authentication.method.kerberos)
        ATLAS_KERBEROS_ENABLED=${value}
      ;;
      (atlas.server.http.port)
        ATLAS_SERVER_HTTP_PORT=${value}
        ATLAS_SERVER_HOST=${host}
      ;;
      (atlas.server.https.port)
        ATLAS_SERVER_HTTPS_PORT=${value}
        ATLAS_SERVER_HOST=${host}
      ;;
      (atlas.enableTLS)
        ATLAS_SSL_ENABLED=${value}
      ;;
      (atlas.kafka.zookeeper.session.timeout.ms)
        ATLAS_KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS=${value}
      ;;
      (atlas.kafka.zookeeper.connection.timeout.ms)
        ATLAS_KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS=${value}
      ;;
      (atlas.kafka.zookeeper.sync.time.ms)
        ATLAS_KAFKA_ZOOKEEPER_SYNC_TIME_MS=${value}
      ;;
    esac
    if [[ "${ATLAS_HOSTS_LIST}" == *"${host}"* ]];then
      ATLAS_SERVER_HOSTS_COUNT=$((ATLAS_SERVER_HOSTS_COUNT+0))
    elif [ "${ATLAS_HOSTS_LIST}" == "" ];then
      ATLAS_HOSTS_LIST=${host}
      ((ATLAS_SERVER_HOSTS_COUNT+=1))
    else
      ATLAS_HOSTS_LIST=${ATLAS_HOSTS_LIST}" "${host}
      ((ATLAS_SERVER_HOSTS_COUNT+=1))
    fi
  done
ATLAS_SERVER_PROTOCOL="http"
if [ "${ATLAS_SSL_ENABLED}" != "true" ]; then
  ATLAS_SERVER_PORT="${ATLAS_SERVER_HTTP_PORT}"
else
  ATLAS_SERVER_PROTOCOL="https"
  ATLAS_SERVER_PORT="${ATLAS_SERVER_HTTPS_PORT}"
fi

echo "atlas.kafka.zookeeper.session.timeout.ms=${ATLAS_KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.connection.timeout.ms=${ATLAS_KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.sync.time.ms=${ATLAS_KAFKA_ZOOKEEPER_SYNC_TIME_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.bootstrap.servers=${ATLAS_KAFKA_BOOTSTRAP_SERVERS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.connect=${ATLAS_KAFKA_ZK_QUORUM}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.security.protocol=${KAFKA_SECURITY_PROTOCOL}" >> ${ATLAS_APPLICATION_PROPERTIES}

if [ "${ATLAS_KERBEROS_ENABLED}" == "true" ];then
  echo "atlas.authentication.method.kerberos=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.loginModuleControlFlag=required" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.option.storeKey=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.option.useKeyTab=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag=required" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.option.useTicketCache=true" >> ${ATLAS_APPLICATION_PROPERTIES}
fi

if [ "${ATLAS_SSL_ENABLED}" == "true" ];then
  set +x
  ATLAS_TRUSTSTORE_LOCATION=$(get_prop "atlas.kafka.ssl.truststore.location"  ${ATLAS_CLIENT_PROPERTIES})
  ATLAS_TRUSTSTORE_PASSWORD=$(get_prop "atlas.kafka.ssl.truststore.password"  ${ATLAS_CLIENT_PROPERTIES})
  if [ ! -z "${ATLAS_TRUSTSTORE_LOCATION}" ];then
    echo "atlas.kafka.ssl.truststore.location=${ATLAS_TRUSTSTORE_LOCATION}" >> ${ATLAS_APPLICATION_PROPERTIES}
    echo "truststore.file=${ATLAS_TRUSTSTORE_LOCATION}" >> ${ATLAS_APPLICATION_PROPERTIES}
    sed -i '/atlas.kafka.ssl.truststore.location=*/d' ${ATLAS_CLIENT_PROPERTIES}
    if [ ! -z "${ATLAS_TRUSTSTORE_PASSWORD}" ];then
      echo "atlas.kafka.ssl.truststore.password=${ATLAS_TRUSTSTORE_PASSWORD}" >> ${ATLAS_APPLICATION_PROPERTIES}
      echo "truststore.password=${ATLAS_TRUSTSTORE_PASSWORD}" >> ${ATLAS_APPLICATION_PROPERTIES}
      sed -i '/atlas.kafka.ssl.truststore.password=*/d' ${ATLAS_CLIENT_PROPERTIES}
    fi
  fi
  set -x
fi
log "[I] Checking if Atlas is in HA"

# Loop to read the peers config file and create the list and count of Atlas Hosts.
#for line in $(cat ${CONF_DIR}/atlas-server.properties)
#do
#readconf "$line"
#if [[ "${ATLAS_HOSTS_LIST}" == *"${host}"* ]];then
#  ATLAS_SERVER_HOSTS_COUNT=$((ATLAS_SERVER_HOSTS_COUNT+0))
#elif [ "${ATLAS_HOSTS_LIST}" == "" ];then
#  ATLAS_HOSTS_LIST=${host}
#  ((ATLAS_SERVER_HOSTS_COUNT+=1))
#else
#  ATLAS_HOSTS_LIST=${ATLAS_HOSTS_LIST}" "${host}
#  ((ATLAS_SERVER_HOSTS_COUNT+=1))
#fi
#done
if [ "${ATLAS_SERVER_HOSTS_COUNT}" -gt "1" ];then
  log "Multiple hosts have Atlas server installed configuring HA properties for same."
  echo "atlas.server.ha.enabled=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.server.ha.zookeeper.connect=${ZK_QUORUM}" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.server.ha.zookeeper.zkroot=/atlas" >> ${ATLAS_APPLICATION_PROPERTIES}
  if [ ${ATLAS_KERBEROS_ENABLED} == "true" ]; then
    echo "atlas.server.ha.zookeeper.acl=auth:" >> ${ATLAS_APPLICATION_PROPERTIES}
  fi
  ATLAS_SERVER_IDS=""
  ATLAS_SERVER_ID_PREFIX="id"
  ATLAS_REST_ADDRESS=""
  ATLAS_HOSTS_ARRAY=(${ATLAS_HOSTS_LIST})
  # Loop to iterate Atlas Hosts list and add required HA properties.
  # Define a comma separated list of these identifiers as the value of the option atlas.server.ids.
  # For each physical machine, list the IP Address/hostname and port as the value of the configuration atlas.server.address.id, where id refers to the identifier string for this physical machine.
  # For e.g., if you have selected 2 machines with hostnames host1.company.com and host2.company.com, you can define the configuration options as below:
  #   atlas.server.ids=id1,id2
  #   atlas.server.address.id1=host1.company.com:31000
  #   atlas.server.address.id2=host2.company.com:31000
  #   atlas.rest.address=http(s)://host1.company.com:31000,http(s)://host2.company.com:31000
  for EACH_HOST in ${!ATLAS_HOSTS_ARRAY[@]}
  do
     echo "atlas.server.address.${ATLAS_SERVER_ID_PREFIX}$((EACH_HOST+1))=${ATLAS_HOSTS_ARRAY[EACH_HOST]}:${ATLAS_SERVER_PORT}" >> ${ATLAS_APPLICATION_PROPERTIES}
     if [ "$((EACH_HOST+1))" -eq "1" ];then
       ATLAS_REST_ADDRESS="${ATLAS_SERVER_PROTOCOL}://${ATLAS_HOSTS_ARRAY[EACH_HOST]}:${ATLAS_SERVER_PORT},"
       ATLAS_SERVER_IDS="${ATLAS_SERVER_ID_PREFIX}$((EACH_HOST+1)),"
     elif [ "$((EACH_HOST+1))" -eq "${ATLAS_SERVER_HOSTS_COUNT}" ];then
       ATLAS_REST_ADDRESS="${ATLAS_REST_ADDRESS}${ATLAS_SERVER_PROTOCOL}://${ATLAS_HOSTS_ARRAY[EACH_HOST]}:${ATLAS_SERVER_PORT}"
       ATLAS_SERVER_IDS="${ATLAS_SERVER_IDS}${ATLAS_SERVER_ID_PREFIX}$((EACH_HOST+1))"
     else
       ATLAS_REST_ADDRESS="${ATLAS_REST_ADDRESS}${ATLAS_SERVER_PROTOCOL}://${ATLAS_HOSTS_ARRAY[EACH_HOST]}:${ATLAS_SERVER_PORT},"
       ATLAS_SERVER_IDS="${ATLAS_SERVER_IDS}${ATLAS_SERVER_ID_PREFIX}$((EACH_HOST+1)),"
     fi
  done
  echo "atlas.server.ids=${ATLAS_SERVER_IDS}" >> ${ATLAS_APPLICATION_PROPERTIES}
else
  ATLAS_REST_ADDRESS="${ATLAS_SERVER_PROTOCOL}://${ATLAS_SERVER_HOST}:${ATLAS_SERVER_PORT}"
fi
echo "atlas.rest.address=${ATLAS_REST_ADDRESS}" >> ${ATLAS_APPLICATION_PROPERTIES}

# remove empty configs from client config file CDPD-7459
sed -i -E '/^[^=]+=\s*$/d' ${ATLAS_CLIENT_PROPERTIES}
cat ${ATLAS_CLIENT_PROPERTIES} >> ${ATLAS_APPLICATION_PROPERTIES}
