from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#Define DAG
dag = DAG("test_dag", schedule_interval=None, start_date=days_ago(1))

#Parameter
owner="{{ dag_run.conf['owner'] }}"
table="{{ dag_run.conf['table'] }}"

run_this="echo "+owner+"."+table

def test_func(owner,table):
    print(owner+"."+table)

task1 = BashOperator(
    task_id='test_task1',
    bash_command=run_this,
    dag=dag,
)

task2 = PythonOperator(
    task_id='test_task2',
    python_callable=test_func,
    op_kwargs={"owner": owner, "table": table},
    dag=dag,
)

