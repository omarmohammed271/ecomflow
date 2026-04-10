FROM apache/spark:3.5.1
 
USER root
 
RUN apt-get update && apt-get install -y curl python3 python3-pip && \
    pip3 install pyspark==3.5.1 delta-spark==3.1.0
 
# Download Delta Lake jars (compatible with Spark 3.5.x)
RUN curl -L -o /opt/spark/jars/delta-spark_2.12-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar && \
    curl -L -o /opt/spark/jars/delta-storage-3.1.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar
 
USER spark
 