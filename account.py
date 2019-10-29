"""
Code that goes along with the Airflow tutorial located at:
https://github.com/apache/airflow/blob/master/airflow/example_dags/tutorial.py
"""
from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'fadzali',
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 24),
    # 'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('mysqltutorial', default_args=default_args, schedule_interval=timedelta(days=1))

# t1, t2  are examples of tasks created by instantiating operators


t1 = MySqlOperator(
    task_id= 'insert_PD',
    sql= 'call storedP1("2019-06-01","2019-06-30")',
    mysql_conn_id= 'xyz_id',
    database= 'dbname',
    dag = dag)

t2 = MySqlOperator(
    task_id= 'update_PD',
    sql= 'call storedP2("2019-06-01","2019-06-30")',
    mysql_conn_id= 'xyz_id',
    database= 'dbname',
    dag = dag)


t2.set_upstream(t1)
