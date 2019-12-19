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

log "Generating Atlas client configurations."
ATLAS_SERVER_PROPERTIES=${1}
ATLAS_APPLICATION_PROPERTIES=${2}
ATLAS_ZK_QUORUM=${3}
ATLAS_KAFKA_BOOTSTRAP_SERVERS=${4}
ATLAS_CLIENT_PROPERTIES=${5}
ATLAS_KAFKA_ZK_QUORUM=${KAFKA_ZK_QUORUM:-$ATLAS_ZK_QUORUM}
ATLAS_KERBEROS_ENABLED="false"
ATLAS_SSL_ENABLED="false"
ATLAS_SERVER_PORT="31000"
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
  done
ATLAS_SERVER_PROTOCOL="http"
if [ "${ATLAS_SSL_ENABLED}" != "true" ]; then
  ATLAS_SERVER_PORT="${ATLAS_SERVER_HTTP_PORT}"
else
  ATLAS_SERVER_PROTOCOL="https"
  ATLAS_SERVER_PORT="${ATLAS_SERVER_HTTPS_PORT}"
fi
ATLAS_REST_ADDRESS="${ATLAS_SERVER_PROTOCOL}://${ATLAS_SERVER_HOST}:${ATLAS_SERVER_PORT}"

KAFKA_SECURITY_PROTOCOL="PLAINTEXT"
if [ "${ATLAS_KERBEROS_ENABLED}" == "true" ] && [ "${ATLAS_SSL_ENABLED}" == "false" ];then
  KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT"
elif [ "${ATLAS_KERBEROS_ENABLED}" == "false" ] && [ "${ATLAS_SSL_ENABLED}" == "true" ];then
  KAFKA_SECURITY_PROTOCOL="SSL"
elif [ "${ATLAS_KERBEROS_ENABLED}" == "true" ] && [ "${ATLAS_SSL_ENABLED}" == "true" ];then
  KAFKA_SECURITY_PROTOCOL="SASL_SSL"
fi

echo "atlas.metadata.namespace=cm" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.rest.address=${ATLAS_REST_ADDRESS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.session.timeout.ms=${ATLAS_KAFKA_ZOOKEEPER_SESSION_TIMEOUT_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.connection.timeout.ms=${ATLAS_KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.sync.time.ms=${ATLAS_KAFKA_ZOOKEEPER_SYNC_TIME_MS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.bootstrap.servers=${ATLAS_KAFKA_BOOTSTRAP_SERVERS}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.zookeeper.connect=${ATLAS_KAFKA_ZK_QUORUM}" >> ${ATLAS_APPLICATION_PROPERTIES}
echo "atlas.kafka.security.protocol=${KAFKA_SECURITY_PROTOCOL}" >> ${ATLAS_APPLICATION_PROPERTIES}

if [ "${ATLAS_KERBEROS_ENABLED}" == "true" ];then
  echo "atlas.authentication.method.kerberos=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.kafka.sasl.kerberos.service.name=kafka" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.loginModuleControlFlag=required" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.option.serviceName=kafka" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.option.storeKey=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.KafkaClient.option.useKeyTab=true" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.loginModuleControlFlag=required" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.loginModuleName=com.sun.security.auth.module.Krb5LoginModule" >> ${ATLAS_APPLICATION_PROPERTIES}
  echo "atlas.jaas.ticketBased-KafkaClient.option.useTicketCache=true" >> ${ATLAS_APPLICATION_PROPERTIES}
fi
cat ${ATLAS_CLIENT_PROPERTIES} >> ${ATLAS_APPLICATION_PROPERTIES}
#rm "${ATLAS_SERVER_PROPERTIES}"