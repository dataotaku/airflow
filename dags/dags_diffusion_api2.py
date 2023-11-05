from operators.diffusion_api_to_csv import DiffusionApiToCsvOperator
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pendulum
import pandas as pd

with DAG(
    dag_id='dags_diffusion_api2',
    schedule='0 5 * * *',
    start_date=pendulum.datetime(2023, 11, 5, tz='Asia/Seoul'),
    catchup=False
) as dag:

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

        return code_sido.tolist()
    
    data_get_level1_test = PythonOperator(
        task_id='data_get_level1_test',
        python_callable=xlsx_read
    )

    def get_level_two():
        """
        시구 기준의 지역코드를 불러옵니다.
        """
        code=pd.read_excel("/opt/airflow/plugins/files/지역별 지점코드(20220701).xlsx")
        # code=pd.read_excel("./data/KIKcd_B_20180122.xlsx")
        # code["ji_code"] = code["법정동코드"].astype(str).str[0:5]
        code_sgg = code.loc[(code['1단계'] == '경기도') & (code['2단계'].isin(['의정부시', '동두천시', '파주시',\
                                                                        ]) != True) & pd.Series.isnull(code['3단계']), '행정구역코드'].astype(str)
        # code_sigu.columns = ["ji_code", "si", "gu"]
        # code_sigu = code_sigu.drop_duplicates()
        # code_sigu = code_sigu.reset_index(drop=True)
        # code_sigu = code_sigu.fillna("-")

        print(code_sgg)

        return code_sgg.tolist()
    
    data_get_level2_test = PythonOperator(
        task_id='data_get_level2_test',
        python_callable=get_level_two
    )

    def get_time():
        from datetime import datetime
        from datetime import timedelta
        # {{data_interval_end}} 활용을 해야 하는 것은 아닌지???? 20231105 0748
        start_str = (datetime.today() + timedelta(days=-1)).strftime("%Y%m%d")
        start_date = datetime.strptime(start_str, "%Y%m%d")
        #start_date
        numdays = 1
        day_here = []
        for x in range(0, numdays):
            day_date = start_date + timedelta(days=x)
            day_here.append(day_date.strftime("%Y%m%d"))
        #print(day_here)
        hour_list = [f'{x:02d}' for x in list(range(24))]

        time_here = []
        for x in day_here:
            for y in hour_list:
                time_here.append(x+y)
        #print(time_here)
        return time_here
    
    data_get_time_test = PythonOperator(
        task_id='data_get_time_test',
        python_callable=get_time
    )

    def call_api(**kwargs):
        import os
        import requests
        import numpy as np
        from xml_to_dict import XMLtoDict

        http_conn_id = 'apis.data.go.kr'
        path = f"/opt/airflow/files/DailyAirDiffusionIndex/{str(kwargs['current_date'])}"
        print(path)
        file_name = 'daily_diffusion_index.csv'
        #port=':8088'
        endpoint = '1360000/LivingWthrIdxServiceV4/getAirDiffusionIdxV4' 
        apikey = Variable.get("data_go_kr_apikey1")
        print(apikey)
        base_url = f'http://{http_conn_id}/{endpoint}'
        print(base_url)
        sido_urls = []

        ti = kwargs['ti']

        area_lv1 = ti.xcom_pull(task_ids='data_get_level1_test')
        area_lv2 = ti.xcom_pull(task_ids='data_get_level2_test')
        time_here = ti.xcom_pull(task_ids='data_get_time_test')

        for area_num in area_lv1:
            for time_tgt in time_here:
                sido_urls.append("{}?ServiceKey={}&areaNo={}&time={}&dataType={}".format(base_url, apikey, area_num, time_tgt, "xml"))
            
        sido_urls = []
        for area_num in area_lv1:
            for time_tgt in time_here:
                sido_urls.append("{}?ServiceKey={}&areaNo={}&time={}&dataType={}".format(base_url, apikey, area_num, time_tgt, "xml"))


        sgg_urls = []
        for area_num in area_lv2:
            for time_tgt in time_here:
                sgg_urls.append("{}?ServiceKey={}&areaNo={}&time={}&dataType={}".format(base_url, apikey, area_num, time_tgt, "xml"))

        yesterday_urls = sido_urls[::3] + sgg_urls[::3]

        yesterday_api = []

        for url in yesterday_urls:
            print(url)
            # datum = requests.get(url)
            # xd = XMLtoDict()
            # data = xd.parse(datum.content)
            # print(data)
            try:
                datum = requests.get(url)
                xd = XMLtoDict()
                data = xd.parse(datum.content)
                # print(data)
                dum01 = list(data['response']['body']['items']['item'].keys())[3:]
                dum02 = list(data['response']['body']['items']['item'].values())[3:]
                dum02 = [x if x != None else np.nan for x in dum02]
                dum03 = pd.DataFrame({'time':[int(x.replace('h','')) for x in dum01], 'stagnant_idx':dum02})
                dum03['code'] = data['response']['body']['items']['item']['code']
                dum03['areaNo'] = data['response']['body']['items']['item']['areaNo']
                dum03['date'] = data['response']['body']['items']['item']['date']
                print(dum03.head())
                yesterday_api.append(dum03)
            except:
                pass

        yesterday_aggr = pd.concat(yesterday_api)

        yesterday_aggr['stagnant_idx'] = yesterday_aggr['stagnant_idx'].astype(str)
        yesterday_aggr['time'] = yesterday_aggr['time'].astype(str)
        #yesterday_aggr.shape
        diffu_data = yesterday_aggr.drop_duplicates().copy()

        if not os.path.exists(path):
            os.system(f'mkdir -p {path}')
        
        diffu_data.to_csv(path + '/' + file_name, encoding='utf-8', index=False)


    data_go_api_call = PythonOperator(
        task_id='data_go_api_call',
        python_callable=call_api,
        op_kwargs={'current_date':'{{data_interval_start.in_timezone("Asia/Seoul") | ds_nodash}}'}
    )

    data_get_level1_test >> data_get_level2_test >> data_get_time_test >> data_go_api_call