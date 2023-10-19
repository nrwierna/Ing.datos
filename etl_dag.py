from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
    'owner': 'RAFA',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 3),
    'email': 'nrwnet@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=10),
}


dag_path = os.getcwd()     #path original.. home en Docker

url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"

with open(dag_path+'/keys/'+"db.txt",'r') as f:
    data_base= f.read()
with open(dag_path+'/keys/'+"user.txt",'r') as f:
    user= f.read()
with open(dag_path+'/keys/'+"pwd.txt",'r') as f:
    pwd= f.read()

redshift_conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
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
    command='python 04-Cuarta entrega.py',  # Comando para ejecutar el script dentro del contenedor
    api_version='auto',
    auto_remove=True,
    dag=dag,
)
