from abstract_source import AbstractSource
from google.cloud import bigquery
import requests
import csv
import datetime

class Pazaruvaj(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "api_key", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
        
    def authenticate(self):
        pass
    
    def fetch_data(self):
        api_key = self.config['api_key']
        date_from = self.config['date_from']
        date_to = self.config['date_to']
        
        def start_export_csv():
            url = 'https://ppapi.arukereso.com/v1.0/Stat/GenerateExport'
            headers = {'Api-Key': api_key}
            params = {'DateFrom': date_from, 'DateTill': date_to}
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()

        def list_generated_csv():
            url = 'https://ppapi.arukereso.com/v1.0/Stat/ListExport'
            headers = {'Api-Key': api_key}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            def find_hash_for_date_range(data):
                matching_hashes = []
                start_date = datetime.datetime.strptime(date_from, '%Y-%m-%d').date()
                end_date = datetime.datetime.strptime(date_to, '%Y-%m-%d').date()
                for item in data['Result']:
                    try:
                        queued_time = datetime.datetime.strptime(item['QueuedTime'], '%Y-%m-%d %H:%M:%S')
                        from_date = datetime.datetime.strptime(item['From'], '%Y-%m-%d').date()
                        to_date = datetime.datetime.strptime(item['To'], '%Y-%m-%d').date()
                    except ValueError:
                        continue
                    if from_date <= start_date <= to_date and from_date <= end_date <= to_date:
                        matching_hashes.append((queued_time, item))
                if matching_hashes:
                    matching_hashes.sort(reverse=True)
                    return matching_hashes[0][1]
                return None

            return find_hash_for_date_range(data)

        def download_csv(hash_value):
            url = f'https://ppapi.arukereso.com/v1.0/Stat/Download?Hash={hash_value}'
            headers = {'Api-Key': api_key}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.content.decode('utf-8')

        start_response = start_export_csv()
        if start_response:
            list_response = list_generated_csv()
            if list_response:
                csv_content = download_csv(list_response['Hash'])
                return csv_content
        return None

    def fetch_all_data(self):
        self.validate_input()
        data = self.fetch_data()
        
        transformed_data = self.transform_data(data)
        
        return transformed_data
    
    def transform_data(self, data):
        def process_csv_data(csv_data):
            reader = csv.reader(csv_data.splitlines(), delimiter=';')
            processed_data = []
            next(reader)
            for row in reader:
                if not row or row == ['', '', '', '', '', '']:
                    break
                result_row = {}
                result_row['identifier'] = row[0]
                result_row['product_name'] = row[1] or None
                result_row['click_count'] = int(row[2]) if row[2] else None
                result_row['cpc'] = float(row[3]) if row[3] else None
                result_row['avg_value'] = float(row[4]) if row[4] else None
                result_row['date'] = row[5]
                processed_data.append(result_row)
            return processed_data

        return process_csv_data(data)

    def bq_schema(self):
        pazaruvaj_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('identifier', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('product_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('click_count', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('cpc', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('avg_value', 'FLOAT', mode='NULLABLE')
        ]
        
        return pazaruvaj_schema
