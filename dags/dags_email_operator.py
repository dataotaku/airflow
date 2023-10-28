from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id='dags_email_operator',
    schedule='0 8 * * *',
    start_date=pendulum.datetime(2023, 10, 28, tz='Asia/Seoul'),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email_task',
        to='unho.chang@gmail.com',
        subject='Airflow 접속 확인 메일',
        html_content='오늘도 즐거운 Airflow 생활되시기 바랍니다. <br/>{{ data_interval_start }}에 <br/>WSL Airflow에서 장운호 님께 보내드리는 는 메일 입니다'
    )