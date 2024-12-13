from abstract_source import AbstractSource
from google.cloud import bigquery
import json
import requests
import pandas as pd
import io
import random
import time

class YandexDirect(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "date_from", "date_to", "access_token", "client_login"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
        
    def authenticate(self):
        pass
    
    def fetch_data(self, date_from, date_to, access_token, client_login):
        def u(x):
                    if type(x) == type(b''):
                        return x.decode('utf8')
                    else:
                        return x
        endpoint = 'https://api.direct.yandex.com/json/v5/reports'
        headers = {
            "Authorization": "Bearer " + access_token,
            "Client-Login": client_login,
            "Accept-Language": "ru",
            "processingMode": "auto"
           }
        body = {
                "params": {
                    "SelectionCriteria": {
                        "DateFrom": date_from,
                        "DateTo": date_to,
                        "Filter": [
                            {
                                "Field": "Clicks",
                                "Operator": "GREATER_THAN",
                                "Values": ["0"]
                            },

                        ]
                    },
                    "FieldNames": [
                        "Date",
                        "CampaignName",
                        "Impressions",
                        "Clicks",
                        "Cost",
                        "AdGroupName",
                        "AdId",
                        "Conversions"
                    ],
                    "ReportName": u(f"Report_{self.config['date_from']}_{self.config['date_to']}_{random.randint(0, 100000)}"),
                    "ReportType": "AD_PERFORMANCE_REPORT",
                    "DateRangeType": "CUSTOM_DATE",
                    "Format": "TSV",
                    "IncludeVAT": "NO",
                    "IncludeDiscount": "NO"
                }
                }
        body = json.dumps(body, indent=4)
        
        req = requests.post(endpoint, body, headers=headers)
        
        # The data is requested from Yandex Direct in 3 steps:
        # 1. Request the data
        # 2. Wait until it is created
        # 3. Get the data
        for i in range(10):
            if req.status_code == 200:
                break
            else:
                time.sleep(30)
                req = requests.post(endpoint, body, headers=headers)
        
        req.encoding = 'utf-8'  # Forcing the response to be processed in UTF-8 encoding
        format(u(req.text))
                
        result = pd.read_csv(io.StringIO(req.text),header=1, sep="\t")
        
        return result

    def fetch_all_data(self):
        self.validate_input()
        data = self.fetch_data(self.config['date_from'], self.config['date_to'], self.config['access_token'], self.config['client_login'])
        transformed_data = self.transform_data(data)
        
        return transformed_data
    
    def transform_data(self, data):    
        data.drop(data.tail(1).index, inplace=True)
    
        # Transform 'Cost' column to millions
        data['Cost'] = data['Cost'] / 1000000
        
        # Replace '--' with 0 in 'Conversions' column
        data['Conversions'] = data['Conversions'].replace('--', 0)
        
        # Rename columns to match BigQuery schema
        data.rename(columns={
            'Date': 'date',
            'CampaignName': 'campaign_name',
            'Impressions': 'impressions',
            'Clicks': 'clicks',
            'Cost': 'cost',
            'AdGroupName': 'ad_group_name',
            'AdId': 'ad_id',
            'Conversions': 'conversions'
        }, inplace=True)
        
        # Convert DataFrame to JSON
        data = data.to_json(orient="records", date_format='iso')
        data = json.loads(data)
        
        return data


    def bq_schema(self):
        yandex_schema = [
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("campaign_id", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("impressions", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("clicks", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("cost", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ad_group_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("conversions", "STRING", mode="NULLABLE")
        ]
        
        return yandex_schema