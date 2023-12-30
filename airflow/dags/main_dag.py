from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta



with DAG("my_dag_name",start_date=datetime(2023,1,1)) as dag:
	op = DummyOperator(task_id="task")
	op

