import datetime
import json
import os
import requests
import logging
from typing import Dict, Any
import azure.functions as func
from azure.storage.blob import BlobServiceClient


app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="0 */5 * * * *", arg_name="mytimer", run_on_startup=False, # Schedule NCRONTab expression to run every 5 minutes 0 */5 * * * *
              use_monitor=False)

def main(mytimer: func.TimerRequest) -> None: # Runs this function whenever there's a time trigger. 
    if not validate_config(CONFIG):
        logging.info('Invalid configuration. Missing required values.')
        return
    
    api_key, lat, lon, storage_connection_string, container_name = list(CONFIG.values())
    
    # Get data from OW API and transform
    data = get_ow_data(api_key, lat, lon)

    data = json.dumps(data, indent=4)

    # Define the blob name with unique timestamp
    blob_name = f"data_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"

    # Upload data blob to container
    upload_data_blob(storage_connection_string, container_name, blob_name, data)

    logging.info("Weather data collection and storage completed successfully")


def get_ow_data(api_key, lat, lon):
    url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={api_key}'

    response = requests.get(url)

    if response.status_code == 200:
        return response.json()
    else:
        return {'error': 'Failed to request data'}

def upload_data_blob(storage_connection_string, container_name, blob_name, data):
    # Initialize BlobServiceClient using Azurite connection string
    blob_service_client = BlobServiceClient.from_connection_string(storage_connection_string)

    # Get the BlobContainerClient
    container_client = blob_service_client.get_container_client(container_name)

    # Initialize the BlobClient for the file to upload
    blob_client = container_client.get_blob_client(blob_name)

    # Upload the data to the Blob Storage
    try:
        blob_client.upload_blob(data, overwrite=True)
        logging.info(f'Blob named {blob_name} uploaded successfully.')
    except Exception as e:
        logging.error(f'Error uploading blob: {e}')

CONFIG = {
    'api_key': os.getenv('API_KEY'),
    'lat': '-33.437778',
    'lon': '-70.650278',
    'storage_connection_string': os.getenv('STORAGE_CONNECTION_STRING'),
    'container_name': os.getenv('CONTAINERNAME')
}

def validate_config(config: Dict[str, Any]) -> bool:
    required_keys = ['api_key', 'lat', 'lon', 'storage_connection_string', 'container_name']

    return all(config.get(key) for key in required_keys) # Checks CONFIG dictionary and returns False if it is incomplete.