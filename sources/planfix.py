from abstract_source import AbstractSource
from google.cloud import bigquery
from datetime import datetime
import requests
import json
import re
from bs4 import BeautifulSoup

class Planfix:
    def __new__(cls, config):
        report_type = config.get("report")
        if report_type == "contacts":
            instance = super(Planfix, cls).__new__(PlanfixContactsReport)
        elif report_type == "leads":
            instance = super(Planfix, cls).__new__(PlanfixLeadsReport)
        else:
            raise ValueError(f"Unsupported report type: {report_type}")
        
        instance.__init__(config)
        return instance

class PlanfixContactsReport(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["netpeak_client", "date_from", "date_to", "access_token"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")
        
    def authenticate(self):
        pass
    
    def fetch_data(self, date_from, date_to, access_token):
        url_contact = 'https://gremi.planfix.com/rest/contact/list'
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + access_token}
        offset = 0
        page_size = 100
        all_data_contacts = []

        while True:
            query = {
                "offset": offset,
                "pageSize": page_size,
                "filters": [
                    {
                        "type": 12,
                        "operator": "equal",
                        "value": {
                            "dateType": "anotherperiod",
                            "dateFrom": date_from,
                            "dateTo": date_to
                        }
                    }
                ],
                "fields": "id,group,dateOfLastUpdate"
            }

            response = requests.post(url_contact, data=json.dumps(query), headers=headers, timeout=45)
            data = response.json()

            if not data.get("contacts"):
                break
            
            for contact in data["contacts"]:
                formatted_lastDate = datetime.strptime(contact['dateOfLastUpdate']['date'], "%d-%m-%Y").strftime("%Y-%m-%d")
                contact_data = {'id': contact['id'],
                                'date': formatted_lastDate,
                                'last_update': formatted_lastDate}
                
                if 'group' in contact:
                    group_name = re.sub(r'🔴|🟤|⚪|🟡|🟢', '', contact['group']['name'])
                    group_name = group_name.strip()
                    
                    if group_name:  
                        contact_data['lead_status'] = group_name

                if 'lead_status' in contact_data:
                    all_data_contacts.append(contact_data)

            if len(data["contacts"]) < page_size:
                break

            offset += page_size

        return all_data_contacts


    def fetch_all_data(self):
        self.validate_input()
        
        return self.fetch_data(self.config["date_from"], self.config["date_to"], self.config["access_token"])
    
    def transform_data(self):
        pass

    def bq_schema(self):
        planfix_schema = [
            bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('last_update', 'DATE', mode='NULLABLE'),
            bigquery.SchemaField('lead_status', 'STRING', mode='NULLABLE')
            ]
        
        return planfix_schema

class PlanfixLeadsReport(AbstractSource):
    def __init__(self, config):
        super().__init__(config)
        self.partition_by = 'date'
        
    def validate_input(self):
        """
        Validates the configuration file.
        """
        required_fields = ["date_from", "date_to", "access_token"]
        for field in required_fields:
            if field not in self.config:
                raise ValueError(f"Missing required field: {field}")

    def authenticate(self):
        pass

    def fetch_data(self, offset, page_size, date_from, date_to, token):
        """
        Fetches task data from the API for a specific offset.
        """
        url = 'https://gremi.planfix.com/rest/task/list'
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }
        query = {
            "offset": offset,
            "pageSize": page_size,
            "filters": [
                {
                    "type": 12,
                    "operator": "equal",
                    "value": {
                        "dateType": "anotherperiod",
                        "dateFrom": date_from,
                        "dateTo": date_to
                    }
                },
                {
                    "type": 1,
                    "operator": "equal",
                    "value": "user:230"
                }
            ],
            "fields": "id,name,description,dateTime,counterparty"
        }
        response = requests.post(url, data=json.dumps(query), headers=headers, timeout=45)
        response.raise_for_status()
        
        return response.json()

    def transform_data(self, all_tasks):
        """
        Transforms raw task data into the desired format.
        """
        def parse_description(description_html):
            """
            Parses the task description HTML into a dictionary.
            """
            soup = BeautifulSoup(description_html, 'html.parser')
            table_rows = soup.select('table tr')
            parsed_data = {}

            for row in table_rows:
                columns = row.find_all('td')
                if len(columns) == 2:
                    key = columns[0].text.strip()
                    value = columns[1].text.strip()
                    parsed_data[key] = value

            return parsed_data
        
        all_formatted_data = []
        for task_data in all_tasks:
            description_html = task_data.get('description', '')
            task_data['description_parsed'] = parse_description(description_html)

            counterparty_data = task_data.get('counterparty', {})
            counterparty_id = counterparty_data.get('id', '').replace('contact:', '')
            counterparty_name = counterparty_data.get('name', '')
            formatted_date = datetime.strptime(task_data['dateTime']['date'], "%d-%m-%Y").strftime("%Y-%m-%d")
            formatted_datetime = datetime.strptime(task_data['dateTime']['datetime'], "%Y-%m-%dT%H:%MZ").strftime("%Y-%m-%d %H:%M:%S")

            formatted_data = {
                'date': formatted_date,
                'date_time': formatted_datetime,
                'task_id': str(task_data['id']),
                'name': task_data['name'],
                'id': task_data.get('description_parsed', {}).get('id', ''),
                'ad_id': task_data.get('description_parsed', {}).get('ad id', ''),
                'ad_name': task_data.get('description_parsed', {}).get('ad name', ''),
                'adset_id': task_data.get('description_parsed', {}).get('adset id', ''),
                'adset_name': task_data.get('description_parsed', {}).get('adset name', ''),
                'campaign_id': task_data.get('description_parsed', {}).get('campaign id', ''),
                'campaign_name': task_data.get('description_parsed', {}).get('campaign name', ''),
                'platform': task_data.get('description_parsed', {}).get('platform', ''),
                'contact_id': counterparty_id,
                'contact_name': counterparty_name
            }
            all_formatted_data.append(formatted_data)
        
        return all_formatted_data

    def fetch_all_data(self):
        """
        Fetches all task data within the specified date range and transforms it.
        """
        token = self.config['access_token']
        date_from = self.config['date_from']
        date_to = self.config['date_to']
        offset = 0
        page_size = 100
        all_tasks = []

        while True:
            data = self.fetch_data(offset, page_size, date_from, date_to, token)
            tasks = data.get("tasks", [])
            all_tasks.extend(tasks)

            if len(tasks) < page_size:
                break

            offset += page_size

        all_formatted_data = self.transform_data(all_tasks)
        return all_formatted_data

    def bq_schema(self):
        planfix_leads_schema = [
            bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
            bigquery.SchemaField('date_time', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('task_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('ad_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adset_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('adset_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('campaign_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('platform', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contact_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('contact_name', 'STRING', mode='NULLABLE')
            ]
        
        return planfix_leads_schema
