import requests
from abstract_source import AbstractSource
from google.cloud import bigquery
from pandas import date_range
import re

class YouTubeAds(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "client_id", "client_secret", "channel_id", "refresh_token", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        endpoint = 'https://oauth2.googleapis.com/token'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        data = {
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'refresh_token': self.config["refresh_token"],
            'grant_type': 'refresh_token'
        }
        r = requests.post(endpoint, data=data, headers=headers, timeout=45)
        token = r.json()['access_token']

        return token

    def fetch_data(self, token, metrics, date):
        """
        Fetches data from the YouTube Analytics API.        
        """
        
        params = {
            'ids': f'channel==MINE',
            'startDate': self.config["date_from"],
            'endDate': self.config["date_to"],
            'metrics': metrics,
            'dimensions': 'video',  
            'sort': '-views', 
            'maxResults': 200  
        }
        
        headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
        }
        
        response = requests.get(
            'https://youtubeanalytics.googleapis.com/v2/reports',
            params=params,
            headers=headers
        )
        response.raise_for_status()
        
        return response.json()

    def fetch_all_data(self):
        """
        YouTube API gives data without dates, so we fetch & transform data for each date separately.
        """
        dates = date_range(start=self.config["date_from"],
                           end=self.config["date_to"]).strftime("%Y-%m-%d").tolist()
        token = self.authenticate()
        metrics = 'comments,dislikes,estimatedMinutesWatched,likes,shares,views'
        
        # Fetch and transform data for each date
        all_data = [self.transform_data(self.fetch_data(token, metrics, date), date, metrics) for date in dates]
        
        # Flatten results into a single list
        data = [item for sublist in all_data for item in sublist]
        
        return data
    
    def transform_data(self, data, date, metrics):
        def camel_to_snake(name):
            """
            Convert camelCase string to snake_case.
            """
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
            
        result = []
        metrics_list = metrics.split(',')
        column_indices = {col['name']: idx for idx, col in enumerate(data['columnHeaders'])}

        for row in data['rows']:
            video_data = {'video': row[0], 'date': date}
            for metric in metrics_list:
                camel_metric = camel_to_snake(metric)
                video_data[camel_metric] = row[column_indices[metric]] if metric in column_indices else None
            result.append(video_data)

        return result

    def bq_schema(self):
        schema_youtube_ads = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("video", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("dislikes", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("estimated_minutes_watched", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("likes", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("shares", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("views", "INTEGER", mode="NULLABLE"),
        ]
        
        return schema_youtube_ads
