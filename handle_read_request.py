from config import COSMOS_CONNECTION_STRING, COSMOS_DB_NAME, COSMOS_CONTAINER_NAME
from config import BLOB_CONNECTION_STRING, BLOB_CONTAINER_NAME
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
import json

def handle_read_request(record_id):
    # Connect to Cosmos DB
    cosmos_client = cosmos_client.CosmosClient(COSMOS_CONNECTION_STRING)
    db = cosmos_client.get_database_client(COSMOS_DB_NAME)
    container = db.get_container_client(COSMOS_CONTAINER_NAME)

    try:
        # Try retrieving record from Cosmos DB
        record = container.read_item(item=record_id, partition_key='sri')
        return record
    except cosmos_client.exceptions.CosmosResourceNotFoundError:
        # If not found, retrieve from Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        blob_name = f"{record_id}.json"
        blob_client = container_client.get_blob_client(blob_name)
        download_stream = blob_client.download_blob()
        archived_record = download_stream.readall()

        return json.loads(archived_record)