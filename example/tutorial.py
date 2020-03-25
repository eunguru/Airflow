from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past':  False,
    'start_date': datetime(2020, 3, 21),
    'email': ['airflow@airflow.com'],
    'email_on_failur': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)    
}

dag = DAG(
    dag_id='tutorial',
    description= 'First DAG',
    default_args= default_args,
    schedule_interval=timedelta(minutes=3)
)

t1 = BashOperator(
    task_id= 'print_date',
    bash_command= 'date',
    dag= dag
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
    dag=dag
)

dag.doc_mad = __doc__

t1.doc_md = """\
    #### Task Docmentation
    """

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}
{% endfor %}
"""

t3 = BashOperator(
    task_id= 'templated',
    depends_on_past= False,
    bash_command= templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag= dag
)

def print_hello():
    return 'Hello Airflow'

t4 = PythonOperator(
    task_id= 'python_operator',
    python_callable= print_hello,
    dag=dag
)

# t1.set_downstream(t2)
# t1.set_downstream(t3)
# t4.set_upstream(t1)
t1 >> [t2, t3, t4]