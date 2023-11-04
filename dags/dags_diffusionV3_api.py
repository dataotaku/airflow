from operators.diffusion_api_to_csv import DiffusionApiToCsvOperator
from airflow import DAG 
import pendulum

with DAG(
    dag_id='dags_diffusinoV3_api',
    schedule='0 5 * * *',
    start_date=pendulum.datetime(2023, 11, 6, tz='Asia/Seoul'),
    catchup=False
) as dag:
    daily_diffusion_index = DiffusionApiToCsvOperator(
        task_id='daily_diffusion_index',
        path='/opt/airflow/files/DailyAirDiffusionIndex/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}',
        file_name='daily_diffusion_index.csv'
    )

    daily_diffusion_index