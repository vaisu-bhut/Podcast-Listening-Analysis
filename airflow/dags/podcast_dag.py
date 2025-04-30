from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
}

# ← the actual host folder
HOST_DATA = "/home/parva/projects/Podcast-Listening-Analysis/data"

with DAG(
    dag_id='podcast_prediction',
    default_args=default_args,
    start_date=datetime(2025, 4, 29),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_pipeline = DockerOperator(
        task_id='run_spark_pipeline',
        image='spark-app:latest',
        api_version='auto',
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=HOST_DATA,
                target='/data',    # ← where spark-app expects it
                type='bind',
                read_only=True
            ),
        ],
        docker_url='unix://var/run/docker.sock',
        network_mode='podcast_net',
        environment={
            "SPARK_MASTER": "spark://spark-master:7077"
        },
    )
