from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id='dags_email_operator',
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2003, 10, 28, tz='Asia/Seoul'),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='unho.chang@gmail.com',
        subject='Airflow 성공메일',
        html_content='WSL Airflow에서 보내는 메일 입니다'
    )