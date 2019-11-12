# BASE_IMAGE_ARG Required
ARG BASE_IMAGE_ARG
ARG HADOOP_VERSION_ARG
ARG CDP_ARTIFACT_URL_ARG

FROM $BASE_IMAGE_ARG

ENV BASE_IMAGE       $BASE_IMAGE_ARG
ENV HADOOP_VERSION   $HADOOP_VERSION_ARG
ENV CDP_ARTIFACT_URL $CDP_ARTIFACT_URL_ARG

# This hack is widely applied to avoid python printing issues in docker containers.
# See: https://github.com/Docker-Hub-frolvlad/docker-alpine-python3/pull/13
ENV PYTHONUNBUFFERED=1

WORKDIR /
RUN mkdir ${SPARK_HOME}/python
#INSTALL PYTHON 2
RUN apk add --no-cache python && \
    python -m ensurepip  && \
    pip install --upgrade pip setuptools && \
    pip install virtualenv

#INSTALL PYTHON 3
RUN  apk add --no-cache python3 && \
    if [ ! -e /usr/bin/python ]; then ln -sf python3 /usr/bin/python ; fi && \
       \
    python3 -m ensurepip && \
    pip3 install --upgrade pip && \
    pip3 install virtualenv  && \
    if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi

RUN rm -r /usr/lib/python*/ensurepip && \
    # You may install with python3 packages by using pip3.6
    # Removed the .cache to save space
    rm -r /root/.cache

COPY python/lib ${SPARK_HOME}/python/lib
ENV PYTHONPATH ${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]
