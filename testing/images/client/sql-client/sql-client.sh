#!/usr/bin/env bash

${FLINK_HOME}/bin/sql-client.sh embedded \
  -d ${FLINK_HOME}/conf/sql-client-conf.yaml \
  -i ${FLINK_HOME}/conf/init.sql \
  -l ${SQL_CLIENT_HOME}/lib
