from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class DiffusionApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'apis_data_go_kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '1360000/LivingWthrIdxServiceV3/getAirDiffusionIdxV3' 
        self.apikey = '{{var.value.data_go_kr_apikey1}}'
        self.base_dt = base_dt

    def execute(self, context):
        import os
        import numpy as np
        from xml_to_dict import XMLtoDict
        from datetime import datetime
        from datetime import timedelta

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}/{self.endpoint}'

        area_lv1 = self.get_level_one()
        print(area_lv1)
        

        time_here = self.get_time()
        
        sido_urls = []
        for area_num in area_lv1:
            for time_tgt in time_here:
                sido_urls.append("{}?ServiceKey={}&areaNo={}&time={}&dataType={}".format(self.base_url, self.apikey, area_num, time_tgt, "xml"))

        area_lv2 = self.get_level_two()
        print(area_lv2)

        sgg_urls = []
        for area_num in area_lv2:
            for time_tgt in time_here:
                sgg_urls.append("{}?ServiceKey={}&areaNo={}&time={}&dataType={}".format(self.base_url, self.apikey, area_num, time_tgt, "xml"))

        yesterday_urls = sido_urls[::3] + sgg_urls[::3]

        yesterday_api = []
        for url in yesterday_urls:
            try:
                datum = self._call_api(url)
                yesterday_api.append(datum)
            except:
                pass

        yesterday_aggr = pd.concat(yesterday_api)

        yesterday_aggr['stagnant_idx'] = yesterday_aggr['stagnant_idx'].astype(str)
        yesterday_aggr['time'] = yesterday_aggr['time'].astype(str)
        #yesterday_aggr.shape
        diffu_data = yesterday_aggr.drop_duplicates().copy()

        if not os.path.exists(self.path):
            os.system(f'mkdir -p {self.path}')
        
        diffu_data.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)
        



    def _call_api(self, url):
        import requests
        import numpy as np
        import pandas as pd
        from xml_to_dict import XMLtoDict

        headers = {'Content-Type': 'application/xml',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        datum = requests.get(url, headers)
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
        #print(dum03)
        return dum03

    def get_level_one():
        """
        시구 기준의 지역코드를 불러옵니다.
        """
        code=pd.read_excel("/opt/airflow/plugins/files/지역별 지점코드(20220701).xlsx")
        # code=pd.read_excel("./data/KIKcd_B_20180122.xlsx")
        # code["ji_code"] = code["법정동코드"].astype(str).str[0:5]
        code_sido = code.loc[pd.Series.isnull(code['2단계']), '행정구역코드'].astype(str)
        # code_sigu.columns = ["ji_code", "si", "gu"]
        # code_sigu = code_sigu.drop_duplicates()
        # code_sigu = code_sigu.reset_index(drop=True)
        # code_sigu = code_sigu.fillna("-")

        return code_sido
    
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

        return code_sgg
    
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

