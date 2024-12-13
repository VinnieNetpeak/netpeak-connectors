from abstract_source import AbstractSource
import requests
from google.cloud import bigquery
import json
from urllib.parse import urlencode, urlunparse

class TikTok(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "advertiser_id", "access_token", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        """
        Authentication is not needed as access_token is provided in the config.
        """
        return self.config['access_token']

    def fetch_data(self):
        """
        Fetches campaign data from the TikTok API for the specified date range.
        """
        date_from = self.config['date_from']
        date_to = self.config['date_to']
        metrics = [
            'campaign_name', 'campaign_id', 'adgroup_name', 'adgroup_id', 
            'ad_name', 'impressions', 'clicks', 'spend', 'reach', 
            'video_views_p25', 'video_views_p50', 'video_views_p75', 
            'video_views_p100', 'frequency'
        ]
        
        args = {
            'metrics': metrics, 
            'data_level': 'AUCTION_AD',
            'start_date': date_from,
            'end_date': date_to, 
            'page_size': 1000, 
            'page': 1,
            'advertiser_id': self.config['advertiser_id'],
            'report_type': 'BASIC',
            'dimensions': ['ad_id', 'stat_time_day']
        } 
        
        query_string = urlencode({k: v if isinstance(v, str) else json.dumps(v) for k, v in args.items()})
        scheme = "https"
        domain = "business-api.tiktok.com"
        path = "/open_api/v1.3/report/integrated/get/"
        url = urlunparse((scheme, domain, path, "", query_string, ""))
        
        headers = {
            "Access-Token": self.config['access_token'],
        }
        
        response = requests.get(url, headers=headers)
        data = response.json()

        return data
    
    def fetch_all_data(self):
        """
        Fetches all campaign data within the specified date range.
        """
        self.validate_input()
        data = self.fetch_data()        
        transformed_data = self.transform_data(data)
        
        return transformed_data

    def transform_data(self, data):
        """
        Transforms the fetched campaign data into a more usable format.
        """
        result_list = []
        if data['data']['page_info']['total_number'] == 0:
            return result_list

        for row in data['data']['list']:
            result_row = {
                'date': row['dimensions']['stat_time_day'].split(' ')[0],
                'account_id': self.config['advertiser_id'],
                'campaign_name': row['metrics']['campaign_name'],
                'campaign_id': row['metrics']['campaign_id'],
                'adgroup_name': row['metrics']['adgroup_name'],
                'adgroup_id': row['metrics']['adgroup_id'],
                'ad_name': row['metrics']['ad_name'],
                'ad_id': row['dimensions']['ad_id'],
                'impressions': row['metrics']['impressions'],
                'clicks': row['metrics']['clicks'],
                'spend': row['metrics']['spend'],
                'reach': row['metrics']['reach'],
                'video_views_p25': row['metrics']['video_views_p25'],
                'video_views_p50': row['metrics']['video_views_p50'],
                'video_views_p75': row['metrics']['video_views_p75'],
                'video_views_p100': row['metrics']['video_views_p100'],
                'frequency': row['metrics']['frequency']
            }
            result_list.append(result_row)

        return result_list

    def bq_schema(self):
        """
        Returns the BigQuery schema for the campaign data.
        """
        schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('account_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adgroup_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adgroup_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('spend', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('reach', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('video_views_p25', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('video_views_p50', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('video_views_p75', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('video_views_p100', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('frequency', 'FLOAT', mode='NULLABLE'),
        ]
        return schema