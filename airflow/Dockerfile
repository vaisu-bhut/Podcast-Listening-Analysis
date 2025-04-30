FROM apache/airflow:2.7.0-python3.10

USER root
# Install the Docker CLI (for DockerOperator) and clean up apt cache
RUN apt-get update \
 && apt-get install -y docker.io \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user for Python installs
USER airflow

# Install only what the airflow user needs
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      apache-airflow-providers-docker \
      docker \
 && pip install --no-cache-dir -r /requirements.txt
