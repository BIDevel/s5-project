from urllib.parse import quote_plus as quote
from airflow.models import Variable

import requests

class ApiConnect:
    def __init__(self,
                 entity: str,
                 sort: str,
                 limit: str,
                 offset: str
                 ) -> None:

        self.entity = entity
        self.sort = sort
        self.limit = limit
        self.offset = offset


    def url(self) -> str:
        return f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{self.entity}'
 

    def client(self):
        nickname = "blebla"
        cohort = "4"
        apy_key = Variable.get('X-API-KEY-PASSWORD', deserialize_json=False)

        print(self.offset,self.limit,self.url())
        headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': apy_key,
        }
    
        response = requests.get(self.url(), headers=headers, params={
            'sort_field': str(self.sort),
            'sort_direction': 'asc',
            'limit': str(self.limit),
            'offset': str(self.offset),
        })

        print(response.status_code)
        if response.status_code == 200:
            dict_list_res = response.json()
            print(dict_list_res)
        else:
            print("Ошибка:", response.status_code, response.reason)

        return dict_list_res    
        