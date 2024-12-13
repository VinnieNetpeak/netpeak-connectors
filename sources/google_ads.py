from abstract_source import AbstractSource
import requests
from google.cloud import bigquery
from pandas import date_range

# Each report has its own class and uses authentication and data fetching methods from the parent class, or replaces them with its own methods.
# So in the end each report class has only custom settings and doesn't need to implement every method from scratch.
# It's called factory pattern.

class GoogleAds(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        self.report = GoogleAds.get_report_class(self.config["report"])

    @staticmethod
    def get_report_class(report_type):
        report_classes = {
            'keywords': GoogleAdsKeywordsReport,
            'campaign_performance': GoogleAdsCampaignPerformanceReport,
            'campaigns': GoogleAdsCampaignsReport,
            'calls': GoogleAdsCallsReport,
            'click_view': GoogleAdsClickViewReport
        }

        if report_type not in report_classes:
            raise ValueError(f"Unknown report type: {report_type}")

        return report_classes[report_type]

    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "client_id", "client_secret", "customer_id", "developer_token", "login_customer_id", "refresh_token", "report", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        '''
        Refreshes the Google Ads request token
        '''

        endpoint = 'https://oauth2.googleapis.com/token'
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        body = {
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'refresh_token': self.config["refresh_token"],
            'grant_type': 'refresh_token'
        }
        r = requests.post(endpoint, data=body, headers=headers, timeout=45)
        token = r.json()['access_token']

        return token
    
    def fetch_data(self, token, query):
        '''
        Prepares an HTTP request to the Google Ads API
        '''
        
        url = f'https://googleads.googleapis.com/v18/customers/{self.config["customer_id"]}/googleAds:searchStream'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}',
            'developer-token': self.config["developer_token"],
            'login-customer-id': self.config["login_customer_id"]
        }
                
        request = requests.post(url, json=query, headers=headers, timeout=45)
        request.raise_for_status()
        
        result = request.json()

        return result

    def fetch_all_data(self):
        token = self.authenticate()
        
        # For GoogleAdsClickViewReport we need to pass the date to the get_query method, 1 day at a time. This class is the only one that needs this.
        if self.config["report"] == 'click_view':
            dates = date_range(start=self.config["date_from"], end=self.config["date_to"]).strftime("%Y-%m-%d").tolist()
            data = [self.fetch_data(token, self.report.get_query(self, date)) for date in dates]
        else:
            data = self.fetch_data(token, self.report.get_query(self)) 
        
        transformed_data = self.report.transform_data(self, data)

        return transformed_data
    
    @staticmethod
    def bq_schema(self):
        report_instance = self.report(self.config)
        
        return report_instance.bq_schema()

class GoogleAdsCampaignsReport(GoogleAds):
    def __init__(self, config):
        super().__init__(config)
    
    def transform_data(self, data):
        data_list = []
        for batch in data:
            for row in batch['results']:
                result_row = {}
                result_row['date'] = row['segments']['date']
                result_row['account_name'] = row['customer']['descriptiveName']
                result_row['account_id'] = row['customer']['id']
                result_row['campaign_type'] = row['campaign']['advertisingChannelType']
                result_row['campaign_name'] = row['campaign']['name']
                result_row['campaign_id'] = row['campaign']['id']
                result_row['ad_group_name'] = row['adGroup']['name']
                result_row['ad_group_id'] = row['adGroup']['id']
                result_row['ad_type'] = row['adGroupAd']['ad']['type']
                result_row['ad_id'] = row['adGroupAd']['ad']['id']
                result_row['impressions'] = row['metrics']['impressions']
                result_row['clicks'] = row['metrics']['clicks']
                result_row['cost_micros'] = str(
                    float(row['metrics']['costMicros']) / 1000000)

                data_list.append(result_row)
                
        return data_list

    def get_query(self):
        query = {'query': f"""
            SELECT segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.advertising_channel_type,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.id,
            ad_group_ad.ad.type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
            FROM ad_group_ad 
            WHERE segments.date BETWEEN '{self.config["date_from"]}' AND '{self.config["date_to"]}'
            """
            }
        
        return query
    
    def bq_schema(self):
        campaigns_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('account_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('account_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('cost_micros', 'FLOAT', mode='NULLABLE')
        ]
        
        return campaigns_schema

class GoogleAdsCampaignPerformanceReport(GoogleAds):
    def __init__(self, config):
        super().__init__(config)
    
    def transform_data(self, data):
        result_list = []
        for batch in data:
            for row in batch['results']:
                result_row = {}
                result_row['date'] = row['segments']['date']
                result_row['account_name'] = row['customer']['descriptiveName']
                result_row['account_id'] = row['customer']['id']
                result_row['campaign_type'] = row['campaign']['advertisingChannelType']
                result_row['campaign_name'] = row['campaign']['name']
                result_row['campaign_id'] = row['campaign']['id']
                result_row['impressions'] = row['metrics']['impressions']
                result_row['clicks'] = row['metrics']['clicks']
                result_row['cost_micros'] = str(
                    float(row['metrics']['costMicros']) / 1000000)

                result_list.append(result_row)
                
        return result_list

    def get_query(self):       
        query = {'query': f"""
            SELECT
            segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.advertising_channel_type,
            campaign.id,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
            FROM campaign
            WHERE segments.date BETWEEN '{self.config["date_from"]}' AND '{self.config["date_to"]}'
            AND campaign.advertising_channel_type = 'PERFORMANCE_MAX'
            """
            }
        
        return query
    
    def bq_schema(self):
        campaigns_performance_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('account_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('account_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('cost_micros', 'FLOAT', mode='NULLABLE')
        ]
        return campaigns_performance_schema
    
class GoogleAdsKeywordsReport(GoogleAds):
    def __init__(self, config):
        super().__init__(config)
            
    def transform_data(self, data):
        result_list = []
        for batch in data:
            for row in batch['results']:
                result_row = {}
                result_row['date'] = row['segments']['date']
                result_row['account_name'] = row['customer']['descriptiveName']
                result_row['account_id'] = row['customer']['id']
                result_row['campaign_name'] = row['campaign']['name']
                result_row['campaign_id'] = row['campaign']['id']
                result_row['ad_group_name'] = row['adGroup']['name']
                result_row['ad_group_id'] = row['adGroup']['id']
                result_row['ad_type'] = row['adGroupAd']['ad']['type']
                result_row['ad_id'] = row['adGroupAd']['ad']['id']
                result_row['keyword'] = row['segments']['keyword']['info']['text']
                result_row['match_type'] = row['segments']['keyword']['info']['matchType']
                result_row['ad_network_type'] = row['segments']['adNetworkType']
                result_row['click_type'] = row['segments']['clickType']
                result_row['impressions'] = row['metrics']['impressions']
                result_row['clicks'] = row['metrics']['clicks']
                result_row['cost_micros'] = str(
                    float(row['metrics']['costMicros']) / 1000000)

                result_list.append(result_row)

        return result_list
        
    def get_query(self):        
        query = {'query': f"""
            SELECT
            segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.advertising_channel_type,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            segments.click_type,
            segments.keyword.info.text,
            segments.keyword.info.match_type,
            ad_group_ad.ad.type,
            ad_group_ad.ad.id,
            segments.ad_network_type,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{self.config["date_from"]}' AND '{self.config["date_to"]}'
            """
            }
        
        return query
    
    def bq_schema(self):
        campaigns_keywords_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('account_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('account_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('keyword', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_network_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('click_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('cost_micros', 'FLOAT', mode='NULLABLE')
        ]
        return campaigns_keywords_schema

class GoogleAdsCallsReport(GoogleAds):
    def __init__(self, config):
        super().__init__(config)
    
    def transform_data(self, data):
        data_list = []
        for batch in data:
            for row in batch['results']:
                result_row = {}
                result_row['date'] = row['segments']['date']
                result_row['account_name'] = row['customer']['descriptiveName']
                result_row['campaign_type'] = row['campaign']['advertisingChannelType']
                result_row['campaign_name'] = row['campaign']['name']
                result_row['campaign_id'] = row['campaign']['id']
                result_row['ad_group_name'] = row['adGroup']['name']
                result_row['ad_group_id'] = row['adGroup']['id']
                result_row['ad_type'] = row['adGroupAd']['ad']['type']
                result_row['ad_id'] = row['adGroupAd']['ad']['id']
                result_row['phone_number'] = row['adGroupAd']['ad']['callAd']['phoneNumber']
                result_row['impressions'] = row['metrics']['impressions']
                result_row['clicks'] = row['metrics']['clicks']
                result_row['cost_micros'] = str(
                    float(row['metrics']['costMicros']) / 1000000)

                data_list.append(result_row)

        return data_list

    def get_query(self):
        query = {'query': f"""
            SELECT
            segments.date,
            customer.descriptive_name,
            customer.id,
            campaign.advertising_channel_type,
            campaign.id,
            campaign.name,
            ad_group.id,
            ad_group.name,
            ad_group_ad.ad.type,
            ad_group_ad.ad.id,
            ad_group_ad.ad.call_ad.phone_number,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros
            FROM ad_group_ad
            WHERE segments.date BETWEEN '{self.config["date_from"]}' AND '{self.config["date_to"]}'
            and ad_group_ad.ad.type = 'CALL_AD'
            """
            }
        
        return query
    
    def bq_schema(self):
        calls_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('account_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('account_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('phone_number', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('impressions', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('clicks', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('cost_micros', 'FLOAT', mode='NULLABLE')
        ]

        return calls_schema

class GoogleAdsClickViewReport(GoogleAds):
    def __init__(self, config):
        super().__init__(config)
    
    def transform_data(self, data_batches):
        result_list = []

        for batch in data_batches:
            for row in batch:
                results = row.get('results', [])
                for result in results:
                    click_view = result.get('clickView', {})
                    campaign = result.get('campaign', {})
                    ad_group = result.get('adGroup', {})
                    segments = result.get('segments', {})

                    result_row = {
                        'gclid': click_view.get('gclid', 'null'),
                        'campaign_id': campaign.get('id', 'null'),
                        'campaign_name': campaign.get('name', 'null'),
                        'ad_group_id': ad_group.get('id', 'null'),
                        'ad_group_name': ad_group.get('name', 'null'),
                        'date': segments.get('date', 'null'),
                        'keyword': click_view.get('keywordInfo', {}).get('text', 'null'),
                        'match_type': click_view.get('keywordInfo', {}).get('matchType', 'null')
                    }

                    result_list.append(result_row)

        return result_list



    def get_query(self, report_date):
        query = {'query': f"""
            SELECT
                click_view.gclid,
                campaign.id,
                campaign.name,
                ad_group.id,
                ad_group.name,
                click_view.keyword_info.text,
                click_view.keyword_info.match_type,
                click_view.ad_group_ad,
                click_view.keyword,
                segments.date
                FROM click_view
                WHERE segments.date = '{report_date}'
            """
            }
        
        return query
    
    def bq_schema(self):
        click_view_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('gclid', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('keyword', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('match_type', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_group_ad', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_name', 'STRING', mode='NULLABLE')
        ]

        return click_view_schema
    