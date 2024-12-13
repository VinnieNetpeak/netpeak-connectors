from source_factory import Source
from destinations.bigquery import BigQueryDestination
import logging
import functions_framework
from abstract_source import send_notification

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@functions_framework.http
def main(request):
    """
    This function is triggered by an HTTP request.
    It extracts configuration from the request, fetches data from a source, and writes it to BigQuery.
    """
    # 1. Extract configuration from the request
    request_args = request.args
    if request_args:
        config = {key: value for key, value in request_args.items()}
    else:
        return {"message": "No URL parameters found."}, 400
    
    # 2. Get data from a source
    try:
        source_connector = Source.connector(config)
        data = source_connector.fetch_all_data()
    except Exception as e:
        if config.get('report'):
            send_notification(f"⛔️ <b>{config['netpeak_client']}</b>: Failed to fetch data from {source_connector.__class__.__name__} connector, {config['report']} report.")
        else:
            send_notification(f"⛔️ <b>{config['netpeak_client']}</b>: Failed to fetch data from {source_connector.__class__.__name__} connector.")
        raise Exception(f"Failed to fetch data from {source_connector.__class__.__name__} source: {str(e)}")

    logger.info(f"Data fetched and transformed from {source_connector.__class__.__name__} source. Rows: {len(data)}")
    
    # If the data is empty, send a notification and return
    if not data:
        send_notification(f"🔷 <b>{config['netpeak_client']}</b>: Fetched 0 rows for dates {config['date_from']} - {config['date_from']} for {source_connector.__class__.__name__} source.")
        return f"No data fetched from {source_connector.__class__.__name__} source", 200

    # 3. Write data to BigQuery
    try:
        bq_dest = BigQueryDestination(
        project_id=config["project_id"],
        dataset_id=config["dataset_id"],
        table_id=config["table_id"],
        bq_schema=source_connector.bq_schema(),
        json_data=data, 
        dataset_location=config["dataset_location"],
        date_from=source_connector.config["date_from"],
        date_to=source_connector.config["date_to"],
        partition_by=source_connector.partition_by,
        full_refresh=config.get("full_refresh", False)
        )
        bq_dest.execute()
        logger.info(f"{source_connector.__class__.__name__} data loaded to BigQuery.")
    except Exception as e:
        if config.get('report'):
            send_notification(f"⛔️ <b>{config['netpeak_client']}</b>: Failed to load data to BigQuery. Got {len(data)} rows for {config['report']} report from {source_connector.__class__.__name__} source.")
        else:
            send_notification(f"⛔️ <b>{config['netpeak_client']}</b>: Failed to load data to BigQuery. Got {len(data)} rows from {source_connector.__class__.__name__} source.")
        logger.info(f"Got {len(data)} rows from {source_connector.__class__.__name__} source. Failed to load them to BigQuery: {str(e)}")
        raise Exception(f"Failed to load data to BigQuery: {str(e)}")
    
    try:
        if config.get('report'):
            send_notification(f"✅ <b>{config['netpeak_client']}</b>: {source_connector.__class__.__name__} {config['report']} data loaded, last date: {config['date_to']}. Rows: {len(data)}")
        else:
            send_notification(f"✅ <b>{config['netpeak_client']}</b>: {source_connector.__class__.__name__} data loaded, last date: {config['date_to']}. Rows: {len(data)}")
        
        logger.info(f"Telegram notification sent.")
        return f"{config['netpeak_client']}: Data fetched and transformed from {source_connector.__class__.__name__} source and loded into BigQuery ({len(data)} rows)", 200
    except Exception as e:
        send_notification(f"⛔️ <b>{config['netpeak_client']}</b>: Failed to load data to BigQuery.")
        raise Exception(f"Failed to load data to BigQuery: {str(e)}")
