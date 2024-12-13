import requests
from abstract_source import AbstractSource
from google.cloud import bigquery
import re
from datetime import datetime, timedelta

class eSputnik(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.config.setdefault('date_from', datetime.strftime(datetime.now() - timedelta(days=4), '%Y-%m-%d'))
        self.config.setdefault('date_to', datetime.strftime(datetime.now() - timedelta(days=0), '%Y-%m-%d'))
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "username", "token", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        pass

    def fetch_data(self, offset=''):
        """
        Sends an HTTP request to the Esputnik API.
        """
        
        endpoint = f"https://esputnik.com/api/v2/contacts/activity?dateFrom={self.config['date_from']}&dateTo={self.config['date_to']}&offset={offset}"
        creds = (self.config['username'], self.config['token'])
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
        
        response = requests.get(endpoint, auth=creds, headers=headers, timeout=120)
        print(response.status_code)
        print(response.text)
        content = response.json() if response else []  # Parse JSON if the response is not None
        return content

    def fetch_all_data(self):
        """"
        Fetches all data from the Esputnik API
        """
        
        self.validate_input()
        
        def get_offset(content):
            """
            Updates the offset for the next request
            """
            if content:
                offset = content[-1].get('offset', '')
            return offset
    
        data = []
        content = self.fetch_data()  # Make the initial API request
        
        while content: # If we have 0 results, we reached the end of the data
            offset = get_offset(content)
            data.extend(content)  # Append all items from content to results
            content = self.fetch_data(offset)  # Make subsequent requests with the new offset
        transformed_data = self.transform_data(data)
        
        return transformed_data
    
    def transform_data(self, data):
        """
        Transforms the given data by adding date and time to each record based on activityDateTime
        and converting CamelCase keys to snake_case.
        """
        
        def camel_to_snake(name):
            """
            Convert camelCase string to snake_case.
            """
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        
        def transform_record(record):
            # Extract date and time
            record['date'], record['time'] = record['activityDateTime'].split('T')
            # Create a new dictionary with snake_case keys
            new_record = {camel_to_snake(k): v for k, v in record.items()}
            return new_record
        
        transformed_data = [transform_record(record) for record in data]
        
        # Remove all columns that are not in the schema
        #bq_columns = [column.name for column in self.bq_schema()]
        #transformed_data = [{k: v for k, v in record.items() if k in bq_columns} for record in transformed_data]
        
        return transformed_data

    def bq_schema(self):
        schema_esputnik = [
            bigquery.SchemaField('date', 'DATE', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('campaign', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('new_downloads', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('impressions', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('lat_on_installs', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('taps', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('local_spend', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('ttr', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('conversion_rate', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('avg_cpm', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('installs', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('lat_off_installs', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('redownloads', 'INTEGER', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('avg_cpa', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('avg_cpt', 'FLOAT', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('time', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('iid', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('contact_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('email', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('media_type', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('activity_status', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('message_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('message_instance_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('activity_date_time', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('click_event_link', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('message_tag', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('workflow_block_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('workflow_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('workflow_instance_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('offset', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('broadcast_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('hard_bounce', 'BOOLEAN', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('external_request_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('external_customer_id', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('web_push_token', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('mob_push_token', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('sms', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('status_description', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('view_message_link', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('status_data', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('message_language_code', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('source_event_key', 'STRING', 'NULLABLE', None, None, (), None),
            bigquery.SchemaField('source_event_type_key', 'STRING', 'NULLABLE', None, None, (), None)
        ]


        return schema_esputnik
