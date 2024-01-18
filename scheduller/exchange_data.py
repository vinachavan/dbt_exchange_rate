from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowFailException
import sys
import 
sys.path.append('V:\dbt-test-env\dbt_test_project\load_data')
from load_exchange_data import ExchangeRateDatesCZ

from airflow.operators.bash import BashOperator


with DAG(
        dag_id='cz-republic-exchange-rates-data',
        start_date=datetime(2024, 1, 17),
        schedule='0 0 * * *', # runs every day at 12 AM
        tags=['exchange rate'],
        catchup=False
) as dag:

    def load_exchange_data():
        erd = ExchangeRateDatesCZ()
        erd.load_3month_data()

    def generate_weekly_matrices():
        #trigger dbt using bash operator
        



    start = DummyOperator(task_id='start')
    
    task1 = PythonOperator(
                            task_id="create_conf_template",
                            provide_context=True,
                            python_callable=load_exchange_data,
                            retries=2,
                            retry_delay=timedelta(seconds=30)
                            )
    task2 = PythonOperator(
                            task_id="create_conf_template",
                            provide_context=True,
                            python_callable=generate_weekly_matrices,
                            retries=2,
                            retry_delay=timedelta(seconds=30)
                            )

    end = DummyOperator(task_id='end')

    start >> task1 >> task2 >> end
