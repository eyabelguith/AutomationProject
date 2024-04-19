import sys
sys.path.append("C:\\Users\\eyawo\\airflow-docker\\plugins")
#from data_manipulation import main
from data_manipulation import main as data_manipulation_main
from Tdata_manipulation import main as tilt_data_manipulation_main
from Pdata_manipulation import main as power_data_manipulation_main

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

#wrapping
def safe_main(): #error handling mta3 potential exceptions fil main function
    try:
        data_manipulation_main()
    except Exception as e:
        print(f"An error occurred: {e}")

dag = DAG(
    'my_data_pipeline',
    default_args=default_args,
    description='Congestion data pipeline',
    schedule_interval='@daily',
)

#lel KPI
run_data_manipulation = PythonOperator(
    task_id='run_data_manipulation',
    python_callable=safe_main,
    dag=dag,
)

#lel Tilt 
run_tilt_data_manipulation = PythonOperator(
    task_id='run_tilt_data_manipulation',
    python_callable=tilt_data_manipulation_main,
    dag=dag,
)

#lel Power
run_power_data_manipulation = PythonOperator(
    task_id='run_power_data_manipulation',
    python_callable=power_data_manipulation_main,
    dag=dag,
)

run_tilt_data_manipulation.set_upstream(run_data_manipulation)
run_power_data_manipulation.set_upstream(run_tilt_data_manipulation)