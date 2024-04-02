import sys
sys.path.append("C:\\Users\\eyawo\\airflow-docker\\plugins")
from data_manipulation import main

from datetime import datetime
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.python.PythonOperator import PythonOperator
from airflow.operators.python import PythonOperator  #In Airflow >=2.0.0

from airflow.utils.dates import days_ago
#from plugins.data_manipulation import main
#from .plugins.data_manipulation import main


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 2,
}

dag = DAG(
    'my_data_pipeline',
    default_args=default_args,
    description='Congestion data pipeline',
    schedule_interval='@daily',
)

run_data_manipulation = PythonOperator(
    task_id='run_data_manipulation',
    python_callable=main,
    dag=dag,
)
