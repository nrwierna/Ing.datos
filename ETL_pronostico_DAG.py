from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'RAFA',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='DAG para ejecutar el ETL',
    schedule_interval=timedelta(days=1),
)

run_etl = DockerOperator(
    task_id='run_etl',
    image='apache/airflow',  # Nombre de la imagen Docker
    command='python 03-Tercera entrega.py',  # Comando para ejecutar el script dentro del contenedor
    api_version='auto',
    auto_remove=True,
    dag=dag,
)
