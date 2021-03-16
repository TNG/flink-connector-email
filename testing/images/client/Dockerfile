FROM flink:1.12.2-scala_2.12
ARG FLINK_VERSION=1.12.2

COPY sql-client/ /opt/sql-client

COPY jars/* /opt/sql-client/lib/
COPY conf/* /opt/flink/conf/

WORKDIR /opt/sql-client
ENV SQL_CLIENT_HOME /opt/sql-client
