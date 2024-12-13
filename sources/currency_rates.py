from abstract_source import AbstractSource
from google.cloud import bigquery
import requests

class CurrencyRates(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "date_from", "date_to", "from_currency", "to_currency", "api_key"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        pass

    def fetch_data(self):
        pass

    def transform_data(self):
        pass
    
    def fetch_all_data(self):
        """
        Fetches currency data from an external API and returns it in a specific format.
        """
        self.validate_input()
        endpoint = 'https://api.apilayer.com/currency_data/timeframe'

        params = {'currencies': self.config['to_currency'],
                  'source': self.config['from_currency'],
                  'start_date': self.config['date_from'],
                  'end_date': self.config['date_to']
                  }
        headers = {'apikey': self.config['api_key']}

        r = requests.get(endpoint, params=params, headers=headers)

        # Parse exchange rates from the response, dynamically based on the 'to_currency' field
        to_currencies = self.config['to_currency'].split(',')
        from_currency = self.config['from_currency']

        if r.json().get("success"):
            data = []
            quotes = r.json().get('quotes', {})
            for date, quote in quotes.items():
                entry = {'date': date}
                for to_currency in to_currencies:
                    currency_pair = f'{from_currency}{to_currency}'
                    entry[currency_pair] = quote.get(currency_pair, None)
                data.append(entry)
                
        return data
    
    def bq_schema(self):
        to_currencies = self.config['to_currency'].split(',')
        from_currency = self.config['from_currency']
        
        schema_currency = [bigquery.SchemaField("date", "DATE", mode="REQUIRED")]
        for to_currency in to_currencies:
            currency_pair = f'{from_currency}{to_currency}'
            schema_currency.append(bigquery.SchemaField(currency_pair, "FLOAT", mode="NULLABLE"))
        
        return schema_currency
