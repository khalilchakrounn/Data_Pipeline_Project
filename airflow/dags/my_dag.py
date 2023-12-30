from airflow import DAG
from datetime import datetime, timedelta
from random import randint
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from scripts_airflow.script import main



with DAG(dag_id="my_dag",start_date=datetime(2023,1,1),catchup=False) as dag:
    main_dag = PythonOperator(
        task_id = "my_dag",
        python_callable=main,
    )
    main_dag