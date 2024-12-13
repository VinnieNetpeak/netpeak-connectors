from abstract_source import AbstractSource
from google.cloud import bigquery
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.api import FacebookAdsApi
import time

class MetaAds(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "access_token", "account_id", "app_id", "app_secret", "country", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        pass

    def fetch_data(self):
        """
        Fetches data from the Meta API.
        """
        FacebookAdsApi.init(self.config["app_id"], self.config["app_secret"], self.config["access_token"], api_version='v20.0')
        account = AdAccount('act_'+str(self.config["account_id"]))

        job = account.get_insights(fields=[
                AdsInsights.Field.account_id,
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.adset_name,
                AdsInsights.Field.adset_id,
                AdsInsights.Field.ad_name,
                AdsInsights.Field.ad_id,
                AdsInsights.Field.spend,
                AdsInsights.Field.impressions,
                AdsInsights.Field.clicks,
                AdsInsights.Field.actions,
                AdsInsights.Field.conversions
            ], params={
                'level': 'ad',
                'time_range': {
                    'since':  self.config["date_from"],
                    'until': self.config["date_to"]
                },
                'time_increment': 1
            }, is_async=True)

        while True:
            job = job.api_get()
            time.sleep(1)
            if job[AdReportRun.Field.async_status] == 'Job Completed':
                insights = job.get_result(params={"limit": 1000})
                break

        return insights or []
    
    def transform_data(self, insights):
        """
        Transforms the given insights data into a BigQuery format.
        """
        meta_source = []

        for item in insights:
            actions = [{'action_type': value['action_type'], 'value': value['value']}
                    for value in item.get('actions', [])]

            conversions = [{'action_type': value['action_type'], 'value': value['value']}
                        for value in item.get('conversions', [])]

            meta_source.append({
                'date': item.get('date_start'),
                'account_id': item.get('account_id'),
                'ad_id': item.get('ad_id'),
                'ad_name': item.get('ad_name'),
                'adset_id': item.get('adset_id'),
                'adset_name': item.get('adset_name'),
                'campaign_id': item.get('campaign_id'),
                'campaign_name': item.get('campaign_name'),
                'clicks': item.get('clicks', 0),
                'impressions': item.get('impressions', 0),
                'spend': item.get('spend', 0.0),
                'conversions': conversions,
                'actions': actions
            })

        return meta_source

    def fetch_all_data(self):
        """"
        Fetches all data from the Meta API
        """
        self.validate_input()        
        insights = self.fetch_data()
        meta_report = self.transform_data(insights)
        
        return meta_report
    
    def bq_schema(self):
        schema_meta_ads = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("account_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ad_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("adset_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("adset_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("campaign_id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField('conversions', 'RECORD', mode='REPEATED',
                                fields=(bigquery.SchemaField('action_type', 'STRING'),
                                        bigquery.SchemaField('value', 'STRING'))),
            bigquery.SchemaField('actions', 'RECORD', mode='REPEATED',
                                fields=(bigquery.SchemaField('action_type', 'STRING'),
                                        bigquery.SchemaField('value', 'STRING')))
        ]

        return schema_meta_ads