
#%sh pip install "apache-airflow[databricks]"

import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
#from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta

# The next section sets some default arguments applied to each task in the DAG
args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0)
}

#The DAG instantiation statement gives the DAG a unique ID, attaches the default arguments, and gives it a daily schedule (example).
dag = DAG(dag_id='Task_2_5_DAG', default_args=args, schedule_interval='@daily')

task1 = DummyOperator(task_id='Task_1', dag=dag)
task2 = DummyOperator(task_id='Task_2', dag=dag)
task3 = DummyOperator(task_id='Task_3', dag=dag)
task4 = DummyOperator(task_id='Task_4', dag=dag)
task5 = DummyOperator(task_id='Task_5', dag=dag)
task6 = DummyOperator(task_id='Task_6', dag=dag)

task1 >> [task2, task3]
task2 >> [task4, task5, task6]
task3 >> [task4, task5, task6]