from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_pyspark_job():
    import subprocess
    subprocess.run([
        'docker', 'run', '--memory=4g', '--network', 'my_network', '-v', 'C:/data:/data',
        'my_pyspark_image', 'spark-submit', '--master', 'local[*]', 'your_script.py'
    ])

with DAG('my_dag', start_date=datetime(2023, 1, 1), schedule_interval='@daily') as dag:
    task = PythonOperator(
        task_id='run_pyspark',
        python_callable=run_pyspark_job
    )