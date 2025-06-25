import datetime
import logging
import json
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from config import COSMOS_CONNECTION_STRING, COSMOS_DB_NAME, COSMOS_CONTAINER_NAME
from config import BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME
import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    logging.info('Python timer trigger function started.')

    # Connect to Cosmos DB
    cosmos_client = CosmosClient(COSMOS_CONNECTION_STRING)
    db = cosmos_client.get_database_client(COSMOS_DB_NAME)
    container = db.get_container_client(COSMOS_CONTAINER_NAME)

    # Connect to Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

    # Calculate date three months ago
    three_months_ago = datetime.datetime.utcnow() - datetime.timedelta(days=90)

    # Query to find records older than three months
    old_records_query = f"SELECT * FROM c WHERE c.timestamp < '{three_months_ago.isoformat()}'"
    old_records = container.query_items(query=old_records_query, enable_cross_partition_query=True)

    for record in old_records:
        # Serialize the record to JSON
        record_json = json.dumps(record)

        # Upload the record to Blob Storage
        blob_name = f"{record['id']}.json"
        blob_container_client.upload_blob(name=blob_name, data=record_json)

        # Optionally, delete the record from Cosmos DB
        container.delete_item(item=record, partition_key=record['partition_key'])

    logging.info('Archiving completed.')