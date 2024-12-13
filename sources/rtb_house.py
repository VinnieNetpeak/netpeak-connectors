from rtbhouse_sdk.client import BasicAuth, Client as RTBClient
from rtbhouse_sdk.schema import CountConvention, StatsGroupBy, StatsMetric
from google.cloud import bigquery
from abstract_source import AbstractSource
import pandas as pd
import json

class RTBHouse(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "login", "password", "date_from", "date_to"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        """
        Authenticates the RTBHouse API client using the provided login and password.
        """
        
        api_client = RTBClient(auth=BasicAuth(self.config["login"], self.config["password"]))
        return api_client

    def fetch_data(self, api_client, advertiser_hash, dimensions, metrics):
        """
        Fetches data from the RTBHouse API using the provided API client, advertiser hash, dimensions, and metrics.
        """
        
        stats = api_client.get_rtb_stats(
            adv_hash=advertiser_hash,
            day_from=self.config["date_from"],
            day_to=self.config["date_to"],
            group_by=dimensions,
            metrics=metrics,
            utc_offset_hours=0,
            count_convention=CountConvention.ATTRIBUTED_POST_CLICK)
        
        return stats

    def transform_data(self, stats, dimensions, metrics):
        """
        Transforms the fetched data into a desired format.
        """
        
        def attrgetter(*items):
            """
            A helper function that returns a function to get attributes from an object.
            """
            if any(not isinstance(item, str) for item in items):
                raise TypeError('attribute name must be a string')
            if len(items) == 1:
                attr = items[0]
                def g(obj):
                    return resolve_attr(obj, attr)
            else:
                def g(obj):
                    return tuple(resolve_attr(obj, attr) for attr in items)
            return g
        
        def resolve_attr(obj, attr):
            """
            A helper function that resolves nested attributes of an object.
            """
            for name in attr.split("."):
                obj = getattr(obj, name)
            return obj
        
        columns = dimensions + metrics
        data_frame = [
            [getattr(row, c.name.lower()) for c in columns]
            for row in sorted(stats, key=attrgetter("day"))
            ]
        new_columns = ['date', 'user_segment', 'country', 'advertiser',
            'subcampaign', 'imps_count', 'clicks_count', 'campaign_cost',
            'conversions_cost', 'ctr', 'cr', 'ecpa', 'ecps']
        
        df = pd.DataFrame(data_frame, columns=new_columns)
        ress = df.to_json(orient="records", date_format = 'iso')
        parsed = json.loads(ress)

        for row in parsed:
            row['date'] = pd.to_datetime(row['date']).strftime("%Y-%m-%d")

        return parsed

    def fetch_all_data(self):
        """
        Fetches all data from the RTBHouse API for the configured advertiser, dimensions, and metrics.
        """
        self.validate_input()
        api_client = self.authenticate()
        advertiser = api_client.get_advertisers()[0].hash

        dimensions = [StatsGroupBy.DAY,
                    StatsGroupBy.USER_SEGMENT,
                    StatsGroupBy.COUNTRY,
                    StatsGroupBy.ADVERTISER,
                    StatsGroupBy.SUBCAMPAIGN
                    ]
        metrics = [StatsMetric.IMPS_COUNT,
                   StatsMetric.CLICKS_COUNT,
                   StatsMetric.CAMPAIGN_COST,
                   StatsMetric.CONVERSIONS_COUNT,
                   StatsMetric.CTR,
                   StatsMetric.CR,
                   StatsMetric.ECPA,
                   StatsMetric.ECPS
                   ]

        data = self.fetch_data(api_client, advertiser, dimensions, metrics)
        if data:
            data = self.transform_data(data, dimensions, metrics)

        return data

    def bq_schema(self):
        schema_rtb = [
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("user_segment", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("advertiser", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subcampaign", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("imps_count", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("clicks_count", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("campaign_cost", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("conversions_cost", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ctr", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("cr", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ecpa", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ecps", "FLOAT", mode="NULLABLE")
        ]

        return schema_rtb