# base_img Required # start with python36 image

ARG base_img
ARG HADOOP_VERSION_ARG
ARG CDP_ARTIFACT_URL_ARG

FROM $base_img

ENV BASE_IMAGE       $base_img
ENV HADOOP_VERSION   $HADOOP_VERSION_ARG
ENV CDP_ARTIFACT_URL $CDP_ARTIFACT_URL_ARG

# Reset to root to run installation tasks
USER 0

RUN INSTALL_PKGS="glibc-langpack-en nss_wrapper httpd httpd-devel mod_ssl \
        mod_auth_gssapi mod_ldap mod_session atlas-devel gcc-gfortran \
        libffi-devel libtool-ltdl enchant \
        python3 python3-devel python3-pip python3-setuptools python3-virtualenv python3-numpy \
        python2 python2-devel python2-pip python2-setuptools python2-virtualenv python2-numpy" && \
    YUM_OPTS="--disableplugin=subscription-manager -y" && \
    yum $YUM_OPTS module enable python27:2.7 httpd:2.4 python36:3.6 && \
    yum $YUM_OPTS install $INSTALL_PKGS && \
    # Remove redhat-logos-httpd (httpd dependency) to keep image size smaller.
    rpm -e --nodeps redhat-logos-httpd && \
    yum $YUM_OPTS --enablerepo="*" clean all

COPY python/lib ${SPARK_HOME}/python/lib
ENV PYTHONPATH ${SPARK_HOME}/python/lib/pyspark.zip:${SPARK_HOME}/python/lib/py4j-*.zip

WORKDIR /opt/spark/work-dir
ENTRYPOINT [ "/opt/entrypoint.sh" ]

# Specify the User that the actual main process will run as
ARG spark_uid=185
USER ${spark_uid}
