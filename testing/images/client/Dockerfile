FROM flink:1.13.1-scala_2.12
ARG FLINK_VERSION=1.13.1

COPY sql-client/ /opt/sql-client

COPY jars/* /opt/sql-client/lib/
COPY conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV FLINK_PROPERTIES "jobmanager.rpc.address: jobmanager"
ENV SQL_CLIENT_HOME /opt/sql-client
