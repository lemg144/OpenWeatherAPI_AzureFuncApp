import datetime
import json
import os
import requests
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="*/10 * * * * *", arg_name="mytimer", run_on_startup=False, # Schedule NCRONTab expression to run every 5 minutes 0 */5 * * * *
              use_monitor=False)

def main(mytimer: func.TimerRequest) -> None: # Runs this function whenever there's a time trigger. 
    # Get data from OW API and transform
    data = get_ow_data(api_key, lat, lon)

    data = json.dumps(data, indent=4)

    # Define the blob name with unique timestamp
    blob_name = f"data_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"

    # Upload data blob to container
    upload_data_blob(storage_connection_string, container_name, blob_name, data)


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
        print(f'Blob named {blob_name} uploaded successfully.')
    except Exception as e:
        print(f'Error uploading blob: {e}')


# api_key = 'd2fa0cfc469ae15fd4b9141c5e4ab631'
api_key = os.getenv('API_KEY')
lat = '-33.437778'
lon = '-70.650278'
# storage_connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1"  # Connection string for Azurite
# container_name = "mycontainer"
storage_connection_string = os.getenv('STORAGE_CONNECTION_STRING')
container_name = os.getenv('CONTAINERNAME')

