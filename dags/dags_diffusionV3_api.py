from operators.diffusion_api_to_csv import DiffusionApiToCsvOperator
from airflow import DAG 
from airflow.operators.python import PythonOperator
import pendulum

with DAG(
    dag_id='dags_diffusinoV3_api',
    schedule='0 5 * * *',
    start_date=pendulum.datetime(2023, 11, 4, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    daily_diffusion_index = DiffusionApiToCsvOperator(
        task_id='daily_diffusion_index',
        path="/opt/airflow/files/DailyAirDiffusionIndex/{{data_interval_end.in_timezone('Asia/Seoul') | ds_nodash}}",
        file_name='daily_diffusion_index.csv'
    )

    def xlsx_read():
        """
        시구 기준의 지역코드를 불러옵니다.
        """
        import pandas as pd
        code=pd.read_excel("/opt/airflow/plugins/files/지역별 지점코드(20220701).xlsx")
        # code=pd.read_excel("./data/KIKcd_B_20180122.xlsx")
        # code["ji_code"] = code["법정동코드"].astype(str).str[0:5]
        code_sido = code.loc[pd.Series.isnull(code['2단계']), '행정구역코드'].astype(str)
        # code_sigu.columns = ["ji_code", "si", "gu"]
        # code_sigu = code_sigu.drop_duplicates()
        # code_sigu = code_sigu.reset_index(drop=True)
        # code_sigu = code_sigu.fillna("-")

        print(code_sido)

        return code_sido
    
    data_excel_read_test = PythonOperator(
        task_id='data_excel_read_test',
        python_callable=xlsx_read
    )

    data_excel_read_test >> daily_diffusion_index