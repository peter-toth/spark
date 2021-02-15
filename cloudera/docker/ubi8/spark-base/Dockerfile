ARG BASE_IMAGE_ARG=docker-private.infra.cloudera.com/cloudera_base/ubi8/ubi:8.2-299

FROM $BASE_IMAGE_ARG

ENV BASE_IMAGE $BASE_IMAGE_ARG

ARG spark_jars=jars
ARG example_jars=examples/jars
ARG k8s_tests=kubernetes/tests

# 1345 user id is a special user id used by DEX
# please contact the DEX team before modifying it
ARG spark_uid=1345

# Required
ARG DOCKERFILE_PATH_ARG

ENV JAVA_HOME="/usr/lib/jvm/java-1.8.0" \
    JAVA_VENDOR="openjdk" \
    JAVA_VERSION="1.8.0" \
    TINI_VERSION="v0.19.0"

# TODO - cache in Cloudera s3 bucket
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini

ENV SPARK_HOME /opt/spark
ENV SPARK_WORKDIR /opt/spark/work-dir

RUN set -ex \
    && chmod +x /usr/bin/tini \
    && yum install --disableplugin=subscription-manager -y java-1.8.0-openjdk-devel krb5-workstation openssl openblas \
    && yum clean all \
    && chmod +x /usr/bin/tini \
    && echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su \
    && chgrp root /etc/passwd && chmod ug+rw /etc/passwd \
    && mkdir -p ${SPARK_HOME} \
    && mkdir -p ${SPARK_WORKDIR} \
    && touch /opt/spark/RELEASE \
    && chown -R ${spark_uid}:root ${SPARK_HOME} ${SPARK_WORKDIR}

COPY --chown=${spark_uid}:root ${DOCKERFILE_PATH_ARG}/entrypoint.sh /opt/
RUN chmod +x /opt/*.sh

COPY --chown=${spark_uid}:root ${spark_jars} /opt/spark/jars
COPY --chown=${spark_uid}:root bin /opt/spark/bin
COPY --chown=${spark_uid}:root sbin /opt/spark/sbin
COPY --chown=${spark_uid}:root ${example_jars} /opt/spark/examples/jars
COPY --chown=${spark_uid}:root examples/src /opt/spark/examples/src
COPY --chown=${spark_uid}:root ${k8s_tests} /opt/spark/tests
COPY --chown=${spark_uid}:root data /opt/spark/data

WORKDIR ${SPARK_WORKDIR}
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
USER ${spark_uid}
