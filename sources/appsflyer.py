from abstract_source import AbstractSource
import requests
import pandas as pd
import io
import simplejson
from google.cloud import bigquery

class AppsFlyer(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "app_id", "api_key", "date_from", "date_to", "report_name"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        pass

    def fetch_data(self, api_key):
        """
        Fetches data from the AppsFlyer API using the provided API key.
        """
        
        def api_path(report_name):        
            report_paths = {
                "non_organic_installs": f'https://hq1.appsflyer.com/api/raw-data/export/app/{self.config["app_id"]}/installs_report/v5?from={self.config["date_from"]}&to={self.config["date_to"]}&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,install_app_store,contributor1_match_type,contributor2_match_type,contributor3_match_type,match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type,att,conversion_type,campaign_type,is_lat&maximum_rows=1000000',
                "organic_installs": f'https://hq1.appsflyer.com/api/raw-data/export/app/{self.config["app_id"]}/organic_installs_report/v5?from={self.config["date_from"]}&to={self.config["date_to"]}&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,install_app_store,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type,att,conversion_type,campaign_type,is_lat&maximum_rows=1000000',
                "non_organic_events": f'https://hq1.appsflyer.com/api/raw-data/export/app/{self.config["app_id"]}/in_app_events_report/v5?from={self.config["date_from"]}&to={self.config["date_to"]}&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,install_app_store,contributor1_match_type,contributor2_match_type,contributor3_match_type,match_type,device_category,gp_referrer,gp_click_time,gp_install_begin,amazon_aid,keyword_match_type,att,conversion_type,campaign_type,is_lat&maximum_rows=1000000',
                "organic_events": f'https://hq1.appsflyer.com/api/raw-data/export/app/{self.config["app_id"]}/organic_in_app_events_report/v5?from={self.config["date_from"]}&to={self.config["date_to"]}&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,amazon_aid,keyword_match_type,att,conversion_type,campaign_type&maximum_rows=1000000',
                "postbacks": f'https://hq1.appsflyer.com/api/raw-data/export/app/{self.config["app_id"]}/postbacks/v5?from={self.config["date_from"]}&to={self.config["date_to"]}&additional_fields=device_model,keyword_id,store_reinstall,deeplink_url,oaid,device_download_time,install_app_store,match_type,contributor1_match_type,contributor2_match_type,contributor3_match_type,device_category,postback_retry,att,is_lat&maximum_rows=1000000'
            }
            
            return f'{report_paths[report_name]}'
        
        headers = {"authorization": "Bearer " + api_key}
        request_data=requests.get(api_path(self.config["report_name"]), headers=headers)
        
        content = pd.read_csv(io.StringIO(request_data.text), low_memory=False)
        return content

    def fetch_all_data(self):
        """
        Fetches all data from the AppsFlyer API using the configured API key.
        """
        self.validate_input()
        data = self.fetch_data(self.config['api_key'])
        
        data = self.transform_data(data)

        return data

    def transform_data(self, df):
        """
        Transforms the fetched data into a desired format.
        """
        
        columns=[w.replace(' ', '_').lower().strip() for w in df.columns.values]
        df.columns=columns
        
        fd_columns=df.dtypes.reset_index()

        for d in range(0, len(fd_columns)):
            columns=fd_columns.iloc[d]['index']
            d_type=fd_columns.iloc[d][0]
            
            if str(d_type)=="object":
                df[columns]=df[columns].apply(lambda x : str(x))
                
            if str(d_type)=='float64':
                df[columns]=df[columns].apply(lambda x: float(x))

        df = df.replace(['nan'], [None], regex=True)
        df['date'] = pd.to_datetime(df['event_time']).dt.date.apply(lambda x: x.isoformat())
        dict_file = df.to_dict(orient='records')

        final_dict = []

        for row in dict_file: 
            try:
                row['campaign_id'] = int(row['campaign_id'])
            except ValueError:
                row['campaign_id'] = None
            try:
                row['adset_id'] = int(row['adset_id'])
            except ValueError:
                row['adset_id'] = None
            try:
                row['ad_id'] = int(row['ad_id'])
            except ValueError:
                row['ad_id'] = None
        
            row_list = []

            if str(row['event_value']) == 'None' or str(row['event_value']) == 'nan':
                row['event_value'] = [{'key': None, 'value': [{'string_value': None, 'int_value': None, 'float_value': None, 'bool_value': None}]}]
            else:
                try:
                    tmp_json = simplejson.loads(row['event_value'])
                except simplejson.errors.JSONDecodeError:
                    print(f"JSONDecodeError on row: {row['event_value']}")
                    row['event_value'] = [{'key': None, 'value': [{'string_value': None, 'int_value': None, 'float_value': None, 'bool_value': None}]}]
                    continue
                for key in tmp_json:
                    row_dict = {}
                    values_list = []
                    row_dict['key'] = key 
                        
                    if type(tmp_json[key]) == str:
                        values_dict = {'string_value': tmp_json[key], 'int_value': None, 'float_value': None, 'bool_value': None, 'array_value': None}
                    elif type(tmp_json[key]) == int:
                        values_dict = {'string_value': None, 'int_value': tmp_json[key], 'float_value': None, 'bool_value': None, 'array_value': None}
                    elif type(tmp_json[key]) == float:
                        values_dict = {'string_value': None, 'int_value': None, 'float_value': tmp_json[key], 'bool_value': None, 'array_value': None}
                    elif type(tmp_json[key]) == bool:
                        values_dict = {'string_value': None, 'int_value': None, 'float_value': None, 'bool_value': tmp_json[key], 'array_value': None}
                    elif type(tmp_json[key]) == list:
                        values_dict = {'string_value': None, 'int_value': None, 'float_value': None, 'bool_value': None, 'array_value': tmp_json[key]}
                    
                    values_list.append(values_dict)

                    row_dict['value'] = values_list

                    row_list.append(row_dict)
                    
                row['event_value'] = row_list

            final_dict.append(row)

        final_dict = simplejson.dumps(final_dict, ignore_nan=True)
        final_dict = simplejson.loads(final_dict)

        return final_dict
    
    def bq_schema(self):
        schema = [
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('attributed_touch_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('attributed_touch_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('install_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('event_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('event_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('event_value', 'RECORD', mode='REPEATED', fields=[
                bigquery.SchemaField('key', 'STRING'),
                bigquery.SchemaField('value', 'RECORD', mode='REPEATED', fields=[
                    bigquery.SchemaField('string_value', 'STRING', mode='NULLABLE'),
                    bigquery.SchemaField('int_value', 'INTEGER', mode='NULLABLE'),
                    bigquery.SchemaField('float_value', 'FLOAT', mode='NULLABLE'),
                    bigquery.SchemaField('bool_value', 'BOOLEAN', mode='NULLABLE'),
                    bigquery.SchemaField('array_value', 'STRING', mode='REPEATED'),
                ]),
            ]),
            bigquery.SchemaField('event_revenue', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('event_revenue_currency', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('event_revenue_usd', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('event_source', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_receipt_validated', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('partner', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('media_source', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('channel', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('keywords', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('adset', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adset_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('ad', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('ad_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('site_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_site_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_param_1', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_param_2', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_param_3', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_param_4', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sub_param_5', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('cost_model', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('cost_value', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('cost_currency', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_partner', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_media_source', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_campaign', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_touch_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_touch_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_partner', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_media_source', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_campaign', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_touch_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_touch_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_partner', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_media_source', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_campaign', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_touch_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_touch_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('region', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('country_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('state', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('city', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('postal_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('dma', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ip', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('wifi', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('operator', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('carrier', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('language', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('appsflyer_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('advertising_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('idfa', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('android_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('customer_user_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('imei', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('idfv', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_1_match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_2_match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contributor_3_match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('device_category', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('platform', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('device_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('os_version', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('app_version', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('sdk_version', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('app_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('app_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('bundle_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_retargeting', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('retargeting_conversion_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('attribution_lookback', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('reengagement_window', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_primary_attribution', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('user_agent', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('http_referrer', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('original_url', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('device_model', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('keyword_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('store_reinstall', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('deeplink_url', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('oaid', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('install_app_store', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('google_play_referrer', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('google_play_click_time', 'DATETIME', mode='NULLABLE'),
            bigquery.SchemaField('google_play_install_begin_time', 'DATETIME', mode='NULLABLE'),
            bigquery.SchemaField('amazon_fire_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('keyword_match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('att', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('conversion_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('is_lat', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('postback_url', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('postback_method', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('postback_http_response_code', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('postback_error_message', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('device_download_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('postback_retry', 'STRING', mode='NULLABLE')
        ]

        return schema