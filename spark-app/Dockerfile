FROM python:3.10-slim

# Install OpenJDK 17, clean up apt caches
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Tell Spark where Java lives
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Python dependencies in one shot
RUN pip install --no-cache-dir \
      pyspark==3.3.0 \
      xgboost==1.7.1 \
      plotly \
      pandas \
      scikit-learn

WORKDIR /app
COPY podcast_pipeline.py ./

ENTRYPOINT ["python", "podcast_pipeline.py"]
