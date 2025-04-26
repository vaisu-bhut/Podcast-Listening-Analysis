FROM jupyter/pyspark-notebook:spark-3.1.2

USER root

# Install Java
RUN apt-get update && apt-get install -y openjdk-11-jdk-headless

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Plotly
RUN pip install plotly

# Download XGBoost JAR files (updated to version 2.0.0)
RUN cd /usr/local/spark/jars && \
    wget https://repo1.maven.org/maven2/ml/dmlc/xgboost4j_2.12/2.0.0/xgboost4j_2.12-2.0.0.jar && \
    wget https://repo1.maven.org/maven2/ml/dmlc/xgboost4j-spark_2.12/2.0.0/xgboost4j-spark_2.12-2.0.0.jar

# Copy the script
COPY your_script.py /home/jovyan/work/your_script.py

# Set working directory
WORKDIR /home/jovyan/work