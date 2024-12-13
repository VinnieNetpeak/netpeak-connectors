# Not tested on real projects. The code was written based on the previous Google Play connector.

from abstract_source import AbstractSource
from google.cloud import bigquery, storage
from datetime import datetime
from io import StringIO
import pandas as pd
import simplejson
import numpy as np

class GooglePlay(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        
        required_fields = ["netpeak_client", "source_project_id", "bucket_id", "date_from", "date_to", "report", "package_name"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
    
    def authenticate(self):
        """
        Authenticates the user with the Google Play API.
        """
        
        storage_client = storage.Client(project=self.config['source_project_id'])

        return storage_client

    def fetch_data(self, storage_client, bucket_id, report, package_name, year_month):
        '''
        Downloads data from the Cloud Storage         
        '''
        
        bucket = storage_client.bucket(bucket_id)

        if report == 'reviews':
            source_blob_name = f'{report}/{report}_{package_name}_{year_month}.csv'
        else:
            source_blob_name = f'stats/{report}/{report}_{package_name}_{year_month}_country.csv'

        blob = bucket.blob(source_blob_name)
        df = pd.read_csv(StringIO(blob.download_as_text()))

        return df

    def transform_data(self, df, date_from, date_to, report):
        '''
        Transforms downloaded from the Google Play Console data 
        '''
        
        df.columns = [w.replace(' ', '_').lower().strip() for w in df.columns]

        if report == 'reviews':
            df.rename(columns={'review_submit_date_and_time': 'submit_date', 'review_submit_millis_since_epoch': 'submit_millis',
                            'review_last_update_date_and_time': 'update_date', 'review_last_update_millis_since_epoch': 'update_millis',
                            'developer_reply_date_and_time': 'reprly_date', 'developer_reply_millis_since_epoch' : 'reply_millis', 
                            'developer_reply_text': 'reply_text'}, inplace=True)

            df['submit_date'] = pd.to_datetime(df['submit_date'])
            df['date'] = df['submit_date'].dt.date.strftime('%Y-%m-%d')
            df['update_date'] = pd.to_datetime(df['update_date'])
            df['reprly_date'] = pd.to_datetime(df['reprly_date'])
            df['review_title'] = df['review_title'].astype(str)
            df['update_date'] = df['update_date'].astype(str)
        
            df['app_version_code'] = df['app_version_code'].astype(str)
            df['app_version_code'] = df['app_version_code'].apply(lambda x: x.split('.')[0])
            df['app_version_code'] = df['app_version_code'].replace('-1', None)
            df['app_version_code'] = df['app_version_code'].astype(float).astype('Int64')

            df['reply_millis'].replace(-1, np.nan, inplace=True)
            df['reply_millis'] = df['reply_millis'].astype(float).astype('Int64')
            
            if date_from is not None and date_to is not None:
                date_fromtime = date_from + ' 00:00+00:00'
                date_totime = date_to + ' 24:59+00:00'
                df = df[(df['update_date'] >= date_fromtime) & (df['update_date'] <= date_totime)]
            
            df['submit_date'] = df['submit_date'].astype(str)
            df['update_date'] = df['update_date'].astype(str)
            df['reprly_date'] = df['reprly_date'].astype(str)
        
            fd_columns=df.dtypes.reset_index()
        
            for d in range(0, len(fd_columns)):
                column_name=fd_columns.iloc[d]['index']
                d_type=fd_columns.iloc[d][0]
            
                if str(d_type)=="object":
                    df.loc[:, column_name] = df[column_name].astype(str)
                
                if str(d_type)=='float64':
                    df.loc[:, column_name] = df[column_name].astype(float)
        
            df = df.where(pd.notnull(df), None)
            df['submit_date'] = df['submit_date'].str.replace('\+00\:00', '.000000')
            df['update_date'] = df['update_date'].str.replace('\+00\:00', '.000000')
            df['reprly_date'] = df['reprly_date'].str.replace('\+00\:00', '.000000')
            
            df['submit_date'] = df['submit_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
            df['update_date'] = df['update_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'))
            df['reprly_date'] = df['reprly_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S') if x != 'NaT' else None)
        else:
            if date_from is not None and date_to is not None:
                df['date'] = pd.to_datetime(df['date'])
                df = df[(df['date'] >= date_from) & (df['date'] <= date_to)]
                df['date'] = df['date'].astype(str)

        df = df.replace('nan', pd.NA)
        df = df.replace('NaT', pd.NA)

        dict_file = df.to_dict(orient='records')
        
        dict_file = simplejson.dumps(dict_file, ignore_nan=True)
        dict_file = simplejson.loads(dict_file)

        return dict_file

    def fetch_all_data(self):
        '''
        Fetches and transforms all data from the Google Play Console for the specified date range
        '''
        self.validate_input()
        
        storage_client = self.authenticate()
        year_months = pd.date_range(self.config['date_from'], self.config['date_to']).strftime('%Y%m').unique().tolist()
        dataframes = [self.fetch_data(storage_client=storage_client,
                                      bucket_id=self.config['bucket_id'],
                                      report=self.config['report'],
                                      package_name=self.config['package_name'],
                                      year_month=year_month) for year_month in year_months]
        data = pd.concat(dataframes, ignore_index=True)
        del dataframes
        
        data = self.transform_data(df=data,
                                        date_from=self.config['date_from'],
                                        date_to=self.config['date_to'],
                                        report=self.config['report'])

        return data

    def bq_schema(self):
        '''
        Returns the schema depending on which report is needed
        '''
        report = self.config['report']

        if report == 'reviews':
            schema = [
            bigquery.SchemaField('package_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('app_version_code', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('app_version_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reviewer_language', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('device', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('submit_date', 'DATETIME', mode='NULLABLE'),
            bigquery.SchemaField('submit_millis', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('update_date', 'DATETIME', mode='NULLABLE'),
            bigquery.SchemaField('update_millis', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('star_rating', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('review_title', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('review_text', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reprly_date', 'DATETIME', mode='NULLABLE'),
            bigquery.SchemaField('reply_millis', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('reply_text', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('review_link', 'STRING', mode='NULLABLE') 
                ]
        elif report == 'installs':
            schema = [
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('package_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('daily_device_installs', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('daily_device_uninstalls', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('daily_device_upgrades', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('total_user_installs', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('daily_user_installs', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('daily_user_uninstalls', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('active_device_installs', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('install_events', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('update_events', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('uninstall_events', 'INTEGER', mode='NULLABLE')
                ]
        elif report == 'ratings':
            schema = [
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('package_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('country', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('daily_average_rating', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('total_average_rating', 'FLOAT', mode='NULLABLE')
                ]  

        return schema