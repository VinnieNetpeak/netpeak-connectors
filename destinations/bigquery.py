# Destination schema is different - fail explicitly: CHECKED.

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import logging

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class BigQueryDestination:
    def __init__(self, project_id, dataset_id, table_id, bq_schema, json_data, 
                 dataset_location, date_from, date_to, partition_by, cluster_by=None, full_refresh=False):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bq_schema = bq_schema
        self.json_data = json_data
        self.dataset_location = dataset_location
        self.date_from = date_from
        self.date_to = date_to
        self.partition_by = partition_by
        self.cluster_by = cluster_by
        self.full_refresh = full_refresh
        self.table_ref = f'{self.project_id}.{self.dataset_id}.{self.table_id}'
        self.client = bigquery.Client()

    def create_table_if_not_exists(self):
        logging.info('BigQuery: Сhecking and creating table if not exists.')
        dataset_ref = f'{self.project_id}.{self.dataset_id}'

        try:
            self.client.get_dataset(dataset_ref)
        except NotFound:
            try:
                dataset = bigquery.Dataset(dataset_ref)
                dataset.location = self.dataset_location
                self.client.create_dataset(dataset)
                logging.info(f'BigQuery: Dataset created: {dataset_ref}')
            except Exception as e:
                logging.error(f'BigQuery: Failed to create dataset: {str(e)}')
                raise

        try:
            self.client.get_table(self.table_ref)
        except NotFound:
            try:
                table = bigquery.Table(self.table_ref, schema=self.bq_schema)
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=self.partition_by
                )
                if self.cluster_by:
                    table.clustering_fields = [self.cluster_by]
                self.client.create_table(table)
                logging.info(f'BigQuery: Table created: {self.table_ref}')
            except Exception as e:
                logging.error(f'BigQuery: Failed to create table: {str(e)}')
                raise
        else:
            self.validate_table_schema()

    def validate_table_schema(self):
        logging.info('BigQuery: Starting to validate provided table schema vs remote.')
        remote_fields = {field.name: field for field in self.client.get_table(self.table_ref).schema}
        local_fields = {field.name: field for field in self.bq_schema}
        
        errors = []
        
        # Check for fields in local schema that are missing or different in the remote schema
        for field_name, local_field in local_fields.items():
            if field_name not in remote_fields:
                errors.append(f"BigQuery: Field '{field_name}' is missing in remote schema.")
            else:
                remote_field = remote_fields[field_name]
                if local_field.field_type != remote_field.field_type:
                    errors.append(f"BigQuery: Field '{field_name}' type mismatch: local={local_field.field_type}, remote={remote_field.field_type}")
                if local_field.mode != remote_field.mode:
                    errors.append(f"BigQuery: Field '{field_name}' mode mismatch: local={local_field.mode}, remote={remote_field.mode}")
        
        # Check for fields in remote schema that are not in the local schema
        for field_name in remote_fields.keys():
            if field_name not in local_fields:
                errors.append(f"Field '{field_name}' is present in remote schema but missing in local schema.")
        
        if errors:
            error_message = "BigQuery: Schema mismatch found:\n" + "\n".join(errors)
            logging.error(error_message)
            raise ValueError(error_message)
        else:
            logging.info("BigQuery: Schemas match.")

    def delete_existing_data(self):
        logging.info('BigQuery: Starting to delete existing data for the dates that we got from source connector.')
        query = f"""
            DELETE FROM `{self.table_ref}`
            WHERE {self.partition_by} BETWEEN @date_from AND @date_to
            """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date_from", "DATE", self.date_from),
                bigquery.ScalarQueryParameter("date_to", "DATE", self.date_to)
            ]
        )
        try:
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()  # Wait for job to complete
            # Log the number of rows deleted
            logging.info(f'BigQuery: {query_job.num_dml_affected_rows} rows deleted for dates between {self.date_from} and {self.date_to}')
        except Exception as e:
            logging.error(f'BigQuery: Failed to delete existing data: {str(e)}')
            raise

    def insert_data(self):
        logging.info('BigQuery: Starting to insert new data.')
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.schema = self.bq_schema
        job_config.autodetect = False

        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"

        try:
            load_job = self.client.load_table_from_json(
                self.json_data,
                destination=table_ref,
                job_config=job_config
            )
            load_job.result()  # Wait for job to complete
            # Log the number of rows inserted
            logging.info(f'BigQuery: {load_job.output_rows} rows were uploaded to table {self.table_id}')
        except Exception as e:
            logging.error(f'BigQuery: Failed to insert data: {str(e)}')
            raise
        
    def drop_table(self):
        logging.info(f'BigQuery: Dropping table (as full_refresh tag is True): {self.table_ref}')
        try:
            self.client.delete_table(self.table_ref)
            logging.info(f'BigQuery: Table dropped: {self.table_ref}')
        except NotFound:
            logging.warning(f'Table not found: {self.table_ref}')
        except Exception as e:
            logging.error(f'BigQuery: Failed to drop table: {str(e)}')
            raise

    def execute(self):
        logging.info('BigQuery: Starting upload.')
        try:
            if self.full_refresh:
                self.drop_table()
            self.create_table_if_not_exists()
            self.delete_existing_data()
            self.insert_data()
        except Exception as e:
            logging.error(f'BigQuery: Failed to execute BigQuery upload: {str(e)}')
            raise