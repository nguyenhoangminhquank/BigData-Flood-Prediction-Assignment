FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir \
        numpy==1.26.4 \
        pandas==2.2.3 \
        pyarrow==17.0.0 \
        psycopg2-binary==2.9.9 \
        pyyaml==6.0.2
RUN mkdir -p /tmp/.ivy2 && chmod 777 /tmp/.ivy2
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
USER spark