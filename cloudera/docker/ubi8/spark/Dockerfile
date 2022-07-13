ARG BASE_IMAGE_ARG=docker-private.infra.cloudera.com/cloudera_base/ubi8/ubi:8.6-855

FROM $BASE_IMAGE_ARG

ENV BASE_IMAGE $BASE_IMAGE_ARG

ARG spark_jars=jars
ARG example_jars=examples/jars
ARG k8s_tests=kubernetes/tests

# Required
ARG DOCKERFILE_PATH_ARG

USER root

ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0" \
    JAVA_VENDOR="openjdk" \
    JAVA_VERSION="1.8.0" \
    TINI_VERSION="v0.19.0"

# TODO - cache in Cloudera s3 bucket
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini

RUN set -ex \
    && yum install --disableplugin=subscription-manager -y java-1.8.0-openjdk-devel krb5-workstation openssl openblas \
    && yum clean all \
    && chmod +rx /usr/bin/tini \
    && mkdir -p /opt/spark \
    && mkdir -p /opt/spark/examples \
    && mkdir -p /opt/spark/work-dir \
    && touch /opt/spark/RELEASE \
    && echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su \
    && chgrp root /etc/passwd && chmod ug+rw /etc/passwd 

COPY ${DOCKERFILE_PATH_ARG}/entrypoint.sh /opt/
COPY ${spark_jars} /opt/spark/jars
COPY bin /opt/spark/bin
COPY sbin /opt/spark/sbin
COPY ${example_jars} /opt/spark/examples/jars
COPY examples/src /opt/spark/examples/src
COPY ${k8s_tests} /opt/spark/tests
COPY data /opt/spark/data

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir
RUN chmod +x /opt/*.sh

ENTRYPOINT [ "/opt/entrypoint.sh" ]
