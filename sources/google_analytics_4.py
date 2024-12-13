from abstract_source import AbstractSource
from google.cloud import bigquery
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Metric, Dimension
from datetime import datetime


class GoogleAnalytics4(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'

    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "date_from", "date_to", "property_id", "dimensions", "metrics"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        client = BetaAnalyticsDataClient()
        return client

    def fetch_data(self):
        pass

    def transform_data(self, response):
        data = []
        for row in response.rows:
            row_data = {}
            # Process dimensions
            for i in range(len(row.dimension_values)):
                dim_name = self.config["dimensions"][i]
                value = row.dimension_values[i].value
                if dim_name == 'date':
                    # Reformat value from 'YYYYMMDD' to 'YYYY-MM-DD'
                    value = datetime.strptime(value, '%Y%m%d').strftime('%Y-%m-%d')
                row_data[dim_name] = value
            # Process metrics
            for i in range(len(row.metric_values)):
                metric_name = self.config["metrics"][i]
                value = row.metric_values[i].value
                # Convert metric values to int, then float, then keep as string
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass  # Keep as string if conversion fails
                row_data[metric_name] = value
            data.append(row_data)
        return data

    def fetch_all_data(self):
        """
        Fetches data from the Google Analytics 4 API.
        """
        self.validate_input()

        self.config["dimensions"] = [dim.strip() for dim in self.config["dimensions"].split(",")]
        self.config["metrics"] = [metric.strip() for metric in self.config["metrics"].split(",")]

        dimensions = [Dimension(name=dim) for dim in self.config["dimensions"]]
        metrics = [Metric(name=metric) for metric in self.config["metrics"]]   
        date_range = [DateRange(start_date=self.config["date_from"], end_date=self.config["date_to"])]
        property_id = self.config["property_id"]
        
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=dimensions,
            metrics=metrics,
            date_ranges=date_range,
        )
        
        client = self.authenticate()
        response = client.run_report(request)
    
        data = self.transform_data(response)
        
        return data

    def bq_schema(self):
        schema_ga4 = []            
        for dim in self.config["dimensions"]:
            if dim == "date":
                schema_ga4.append(bigquery.SchemaField(dim, "DATE", mode="REQUIRED"))
            else:
                schema_ga4.append(bigquery.SchemaField(dim, "STRING", mode="REQUIRED"))
        for metric in self.config["metrics"]:
            schema_ga4.append(bigquery.SchemaField(metric, "FLOAT", mode="REQUIRED"))

        return schema_ga4