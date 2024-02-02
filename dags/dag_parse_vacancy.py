from airflow import DAG 
from airflow.operators.python_operator import PythonOperator 
from datetime import datetime 
from functions import *


with DAG(
          dag_id='parse_vacancy',
          start_date = datetime(2023, 12, 29),
          schedule_interval='0 20 * * *',
          catchup=False
) as dag:

          load_urls = PythonOperator(
                  task_id = 'load_urls',
                  python_callable=get_vacancies_all
          )
          
          load_vacancies_raw = PythonOperator(
                  task_id = 'parse_vacancy',
                  python_callable=parse_vacancies
          )

load_urls>>load_vacancies_raw