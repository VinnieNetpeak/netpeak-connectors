from abstract_source import AbstractSource
from google.cloud import bigquery
import requests
from pandas import date_range

class Admitad(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "client_id", "client_secret", "customer_id", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
         
    def authenticate(self):
        """
        Authenticates the user by sending a request to the Admitad API and retrieves an access token.
        """
        
        data = {'client_id': self.config.get("client_id"),
                'client_secret': self.config.get("client_secret"),
                'scope': 'advertiser_statistics',
                'grant_type': 'client_credentials'}
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        r = requests.post('https://api.admitad.com/token/',
                          data=data,
                          headers=headers)
        token = r.json()['access_token']

        return token

    def fetch_data(self, date, token):
        """
        Fetches the statistics data for a specific date from the Admitad API using the provided access token.
        """
        
        params = {'start_date': date, 'end_date': date, 'order_by': 'date'}
        headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
        r = requests.get(f'https://api.admitad.com/advertiser/{self.config.get("customer_id")}/statistics/dates/',
                         params=params,
                         headers=headers)
        content = r.json()['results']

        return content
    
    def transform_data(self):
        pass
    
    def fetch_all_data(self):
        """
        Fetches statistics data for all dates within the specified date range.
        """
        
        self.validate_input()
        token = self.authenticate()

        data = []
        for date in date_range(self.config.get("date_from"), self.config.get("date_to")).strftime('%Y-%m-%d'):
            data.extend(self.fetch_data(date, token))

        return data

    def bq_schema(self):
        schema_admitad = [
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("views", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ecpc", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ecpm", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("cr", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("leads_open", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sales_open", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("payment_sum_open", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("leads_approved", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sales_approved", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("payment_sum_approved", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("leads_declined", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("sales_declined", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("payment_sum_declined", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE")
        ]

        return schema_admitad
