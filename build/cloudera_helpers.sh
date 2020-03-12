load_conf_file () {
  if [ -n "${SPARK_DEV_CDPD_BUILD}" ]; then
    # If you want to have multiple conf files, for each of the branches you're
    # working in, just change the line below to point to a different conf file.
    # You won't want to check that change in. You also don't really want to set
    # this env variable in your shell.  Its really just exposed for testing.
    ROOT_DIR=$(dirname "$0")
    CONF_FILE=${__TEST_CONF_FILE__:-${ROOT_DIR}/../cloudera/build-cdpd-master.conf}
    if [ -f "$CONF_FILE" ]; then
      echo "loading pre-existing conf file $CONF_FILE"
      source $CONF_FILE
    fi

    echo "executing cloudera_helpers.py to produce build conf file $CONF_FILE"
    ${ROOT_DIR}/cloudera_helpers.py $CONF_FILE || return 1

    # that script could have updated the file, so source it again
    source $CONF_FILE
    echo "SPARK_DEV_BUILD_GBN=$SPARK_DEV_BUILD_GBN"
    echo "SPARK_DEV_GBN_ISOLATED_BUILD=$SPARK_DEV_GBN_ISOLATED_BUILD"
  else
    echo "Skipping dev GBN build logic, as SPARK_DEV_CDPD_BUILD is not set"
    echo "For an official build, this is correct.  For local dev work,"
    echo "you probably want to set SPARK_DEV_CDPD_BUILD in your ~/.bashrc"
  fi
}

extra_gbn_repo_args() {
  build_tool=$1
  if [ -n "$SPARK_DEV_BUILD_GBN" ]; then
    extra_maven_arg="-Pgbn-snapshot-repo"

    if [ -n "$SPARK_DEV_GBN_ISOLATED_BUILD" ] && [ "$SPARK_DEV_GBN_ISOLATED_BUILD" -gt 0 ] ; then
      case $build_tool in
        maven)
          extra_maven_arg+=" -Dmaven.repo.local=gbnRepos/$SPARK_DEV_BUILD_GBN"
          ;;
        sbt)
          extra_maven_arg+=" -Dsbt.ivy.home=gbnRepos/${SPARK_DEV_BUILD_GBN}_ivy"
          ;;
      esac
      echo "Using a gbn specific local repo $GBN_LOCAL_REPO_ARG" 1>&2
      echo "***** WARNING ****  this may try to redownload the internet" 1>&2
    fi
    echo $extra_maven_arg
  fi
}

