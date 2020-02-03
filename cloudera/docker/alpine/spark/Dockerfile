ARG BASE_IMAGE_ARG=docker-private.infra.cloudera.com/cloudera_base/openjdk:8u212-jre-alpine3.9

FROM $BASE_IMAGE_ARG

ENV BASE_IMAGE $BASE_IMAGE_ARG

RUN echo $BASE_IMAGE_ARG

ARG spark_jars=jars
ARG example_jars=examples/jars
ARG img_path=kubernetes/dockerfiles
ARG k8s_tests=kubernetes/tests

RUN set -ex && \
    apk upgrade --no-cache && \
    ln -s /lib /lib64 && \
    apk add --no-cache bash tini libc6-compat linux-pam krb5 krb5-libs && \
    apk add --no-cache openssl && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd

COPY ${spark_jars} /opt/spark/jars
COPY bin /opt/spark/bin
COPY sbin /opt/spark/sbin
COPY ${img_path}/spark/entrypoint.sh /opt/
COPY ${example_jars} /opt/spark/examples/jars
COPY examples/src /opt/spark/examples/src
COPY ${k8s_tests} /opt/spark/tests
COPY data /opt/spark/data


ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]
