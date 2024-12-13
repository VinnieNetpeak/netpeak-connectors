from abstract_source import AbstractSource
from google.cloud import bigquery
import requests
import re

class AppleSearchAds(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "client_id", "client_secret", "org_id", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        """
        Authenticates with the Apple Search Ads API and retrieves an access token.
        """
        
        request_params = {'client_id': self.config["client_id"],
                          'client_secret': self.config["client_secret"],
                          'grant_type': 'client_credentials',
                          'scope': 'searchadsorg'}
        r = requests.post(url='https://appleid.apple.com/auth/oauth2/token',
                          params=request_params,
                          headers={'Host': 'appleid.apple.com',
                                   'Content-Type': 'application/x-www-form-urlencoded'
                                   })
        token = r.json()['access_token']

        return token

    def fetch_data(self, token, date_from, date_to):
        """
        Fetches campaign data from the Apple Search Ads API for a specific date range.
        """
        
        params = {
            "startTime": date_from,
            "endTime": date_to,
            "selector": {
                "orderBy": [
                    {
                        "field": "countryOrRegion",
                        "sortOrder": "ASCENDING"
                    }
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 5000
                }
            },
            "groupBy": [
                "countryOrRegion"
            ],
            "timeZone": "UTC",
            "returnRecordsWithNoMetrics": 'true',
            "returnRowTotals": 'false',
            "granularity": "DAILY",
            "returnGrandTotals": 'false'
        }

        r = requests.post(url='https://api.searchads.apple.com/api/v4/reports/campaigns',
                        json=params,
                        headers={
                            'Authorization': f'Bearer {token}',
                            'X-AP-Context': f'orgId={self.config["org_id"]}'
                        })
        
        # Check if the request was successful
        if r.status_code != 200:
            raise Exception(f"API request failed with status code {r.status_code}: {r.text}")

        response_json = r.json()

        # Check if the expected keys are in the response
        if 'data' not in response_json or 'reportingDataResponse' not in response_json['data'] or 'row' not in response_json['data']['reportingDataResponse']:
            raise KeyError("Response JSON does not contain expected keys")

        content = response_json['data']['reportingDataResponse']['row']

        return content

    def transform_data(self, data):
        """
        Transforms the fetched campaign data into a more usable format.
        """
        
        def extract_amount(value):
            """Extracts the amount from a value that could be a dictionary with
               'amount' and 'currency' keys or a direct numeric value."""
            if isinstance(value, dict):
                return value.get('amount', 0)
            try:
                return int(value) if '.' not in value else float(value)
            except (ValueError, TypeError):
                return 0  # Default to 0 if conversion fails or type is unexpected

        # List of keys that are expected to be numeric and directly accessible
        numeric_keys = {"impressions", "taps", "installs", "newDownloads", "redownloads",
                        "latOnInstalls", "latOffInstalls", "ttr", "conversionRate"}
        # Keys for which we need to access a nested 'amount'
        amount_keys = {"avgCPA", "avgCPT", "avgCPM", "localSpend"}

        results = []
        for item in data:
            campaign_name = item['metadata'].get('campaignName', 'Unknown Campaign')
            for day_data in item['granularity']:
                processed_entry = {key: day_data.get(key, 0) if key in numeric_keys
                                   else extract_amount(day_data) if key in amount_keys
                                   else day_data.get(key, "")
                                   for key in numeric_keys.union(amount_keys, {"date"})}
                processed_entry["campaign"] = campaign_name
                results.append(processed_entry)

        # Transform column names to snake_case
        def camel_to_snake(name):
            """
            Convert camelCase string to snake_case.
            """
            s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
            return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        transform_record = lambda record: {camel_to_snake(k): v for k, v in record.items()}
        results = [transform_record(record) for record in results]

        return results

    def fetch_all_data(self):
        """
        Fetches and transforms all campaign data within the specified date range.
        """
        self.validate_input()
        token = self.authenticate()
        data = self.fetch_data(token, self.config['date_from'], self.config['date_to'])
        data = self.transform_data(data)

        return data

    def bq_schema(self):
        schema_asa = [
            bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
            bigquery.SchemaField("campaign", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("new_downloads", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("lat_on_installs", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("taps", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("local_spend", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ttr", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("conversion_rate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avg_cpm", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("installs", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("lat_off_installs", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("redownloads", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("avg_cpa", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("avg_cpt", "FLOAT", mode="NULLABLE")
        ]

        return schema_asa
