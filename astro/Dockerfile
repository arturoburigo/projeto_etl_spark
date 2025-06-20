FROM --platform=linux/amd64 astrocrpublic.azurecr.io/runtime:3.0-2
USER root
RUN apt-get update && \
    apt-get install -y curl gnupg2 apt-transport-https ca-certificates unixodbc unixodbc-dev gcc g++ default-jdk && \
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Spark dependencies for Delta Lake and Azure
RUN mkdir -p /opt/airflow/spark-jars && \
    cd /opt/airflow/spark-jars && \
    curl -L -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.6/hadoop-azure-3.3.6.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar && \
    curl -L -O https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.6/azure-storage-8.6.6.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar

# Set Spark configuration
ENV SPARK_JARS_DIR=/opt/airflow/spark-jars
ENV SPARK_CLASSPATH=/opt/airflow/spark-jars/*
ENV PYSPARK_SUBMIT_ARGS="--jars /opt/airflow/spark-jars/* pyspark-shell"

USER astro