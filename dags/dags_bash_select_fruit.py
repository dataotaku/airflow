from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_email_operator',
    schedule='10 0 * * 6#1',
    start_date=pendulum.datetime(2023, 10, 28, tz='Asia/Seoul'),
    catchup=False
) as dag:
    t1_orange = BashOperator(
        task_id = "t1_orange",
        bash_command= "/opt/airflow/plugins/shell/select_fruit.sh ORANGE"
    )

    t2_avocado = BashOperator(
        task_id = "t2_avocado",
        bash_command= "/opt/airflow/plugins/shell/select_fruit.sh AVOCADO"
    )

    t1_orange >> t2_avocado