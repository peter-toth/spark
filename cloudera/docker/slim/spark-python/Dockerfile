# base_img Required
ARG base_img
ARG HADOOP_VERSION_ARG
ARG CDP_ARTIFACT_URL_ARG

FROM $base_img

ENV BASE_IMAGE       $base_img
ENV HADOOP_VERSION   $HADOOP_VERSION_ARG
ENV CDP_ARTIFACT_URL $CDP_ARTIFACT_URL_ARG

# Reset to root to run installation tasks
USER 0

RUN apt install -y python python-pip && \
    apt install -y python3 python3-pip && \
    # We remove ensurepip since it adds no functionality since pip is
    # installed on the image and it just takes up 1.6MB on the image
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools && \
    # You may install with python3 packages by using pip3.6
    # Removed the .cache to save space
    rm -r /root/.cache && rm -rf /var/cache/apt/*

COPY python/lib ${SPARK_HOME}/python/lib
ENV PYTHONPATH ${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip

RUN pip3 install virtualenv
RUN pip install virtualenv

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
