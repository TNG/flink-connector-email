FROM flink:1.11.2-scala_2.12

RUN echo "s3.access-key: minioaccesskey" >> /opt/flink/conf/flink-conf.yaml; \
    echo "s3.secret-key: miniosecretkey" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.endpoint: http://minio:9000" >> /opt/flink/conf/flink-conf.yaml; \
    echo "fs.s3a.path.style.access: true" >> /opt/flink/conf/flink-conf.yaml;
