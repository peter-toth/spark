ARG base_img=docker-private.infra.cloudera.com/cloudera_base/openjdk:8-slim

FROM $base_img

ENV BASE_IMAGE $base_img

RUN echo $BASE_IMAGE_ARG

ARG spark_jars=jars
ARG example_jars=examples/jars
ARG k8s_tests=kubernetes/tests
ARG spark_uid=185

# Required
ARG DOCKERFILE_PATH_ARG

RUN set -ex && \
    apt-get update && \
    ln -s /lib /lib64 && \
    apt install -y bash tini libc6 libpam-modules krb5-user libnss3 && \
    apt install -y openssl && \
    mkdir -p /opt/spark && \
    mkdir -p /opt/spark/examples && \
    mkdir -p /opt/spark/work-dir && \
    touch /opt/spark/RELEASE && \
    rm /bin/sh && \
    ln -sv /bin/bash /bin/sh && \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su && \
    chgrp root /etc/passwd && chmod ug+rw /etc/passwd && \
    rm -rf /var/cache/apt/*

RUN echo ${DOCKERFILE_PATH_ARG}

COPY ${spark_jars} /opt/spark/jars
COPY bin /opt/spark/bin
COPY sbin /opt/spark/sbin
COPY ${DOCKERFILE_PATH_ARG}/entrypoint.sh /opt/
COPY ${example_jars} /opt/spark/examples/jars
COPY examples/src /opt/spark/examples/src
COPY ${k8s_tests} /opt/spark/tests
COPY data /opt/spark/data

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir
RUN chmod g+w /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
USER ${spark_uid}
