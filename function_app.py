import datetime
import json
import os
import requests
import logging
from typing import Dict, Any, Optional
import azure.functions as func
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="*/10 * * * * *", arg_name="mytimer", run_on_startup=False, # Schedule NCRONTab expression to run every 5 minutes 0 */5 * * * *
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

CONFIG = {
    'api_key': os.getenv('API_KEY'),
    'lat': '-33.437778',
    'lon': '-70.650278',
    'storage_connection_string': os.getenv('STORAGE_CONNECTION_STRING'),
    'container_name': os.getenv('CONTAINERNAME')
}

def validate_config(config: Dict[str, Any]) -> bool:
    required_keys = ['api_key', 'lat', 'lon', 'storage_connection_string', 'container_name']

    return all(config.get(key) for key in required_keys) # Returns False if the dictionary is incomplete.

if __name__ == '__main__':
    print(all(CONFIG.get(key) for key in CONFIG))


# import datetime
# import json
# import logging
# import os
# from typing import Dict, Any, Optional

# import azure.functions as func
# import requests
# from azure.storage.blob import BlobServiceClient
# from azure.core.exceptions import AzureError

# # Configuration
# WEATHER_API_BASE_URL = 'https://api.openweathermap.org/data/2.5/weather'
# CONFIG = {
#     'api_key': os.getenv('API_KEY'),
#     'lat': '-33.437778',
#     'lon': '-70.650278',
#     'storage_connection_string': os.getenv('STORAGE_CONNECTION_STRING'),
#     'container_name': os.getenv('CONTAINERNAME')
# }

# def get_ow_data(api_key: str, lat: str, lon: str) -> Dict[str, Any]:
#     """
#     Fetch weather data from OpenWeatherMap API.
    
#     Args:
#         api_key: OpenWeatherMap API key
#         lat: Latitude
#         lon: Longitude
    
#     Returns:
#         Dict containing weather data or error message
#     """
#     try:
#         response = requests.get(
#             WEATHER_API_BASE_URL,
#             params={
#                 'lat': lat,
#                 'lon': lon,
#                 'units': 'metric',
#                 'appid': api_key
#             },
#             timeout=10
#         )
#         response.raise_for_status()
#         return response.json()
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Failed to fetch weather data: {str(e)}")
#         return {'error': f'Failed to request data: {str(e)}'}

# def upload_data_blob(
#     connection_string: str,
#     container_name: str,
#     blob_name: str,
#     data: str
# ) -> Optional[str]:
#     """
#     Upload data to Azure Blob Storage.
    
#     Args:
#         connection_string: Azure Storage connection string
#         container_name: Container name
#         blob_name: Name of the blob
#         data: Data to upload
    
#     Returns:
#         Blob name if successful, None if failed
#     """
#     try:
#         blob_service_client = BlobServiceClient.from_connection_string(connection_string)
#         container_client = blob_service_client.get_container_client(container_name)
#         blob_client = container_client.get_blob_client(blob_name)
        
#         blob_client.upload_blob(data, overwrite=True)
#         logging.info(f'Blob named {blob_name} uploaded successfully.')
#         return blob_name
#     except AzureError as e:
#         logging.error(f'Error uploading blob: {str(e)}')
#         return None

# def validate_config(config: Dict[str, Any]) -> bool:
#     """
#     Validate that all required configuration values are present.
    
#     Args:
#         config: Dictionary containing configuration values
    
#     Returns:
#         True if all required values are present, False otherwise
#     """
#     required_keys = ['api_key', 'lat', 'lon', 'storage_connection_string', 'container_name']
#     return all(config.get(key) for key in required_keys)

# app = func.FunctionApp()

# @app.function_name(name="weather_data_collector")
# @app.timer_trigger(
#     schedule="*/10 * * * * *",  # Run every 10 minutes
#     arg_name="timer",
#     run_on_startup=False,
#     use_monitor=False
# )
# def main(timer: func.TimerRequest) -> None:
#     """
#     Main function triggered by timer to collect and store weather data.
    
#     Args:
#         timer: Timer request object
#     """
#     if not validate_config(CONFIG):
#         logging.error("Invalid configuration. Missing required values.")
#         return

#     # Get and transform weather data
#     weather_data = get_ow_data(CONFIG['api_key'], CONFIG['lat'], CONFIG['lon'])
    
#     if 'error' in weather_data:
#         logging.error(f"Failed to get weather data: {weather_data['error']}")
#         return

#     # Add metadata to the weather data
#     weather_data['metadata'] = {
#         'timestamp': datetime.datetime.now().isoformat(),
#         'location': {
#             'lat': CONFIG['lat'],
#             'lon': CONFIG['lon']
#         }
#     }

#     # Convert data to JSON
#     json_data = json.dumps(weather_data, indent=4)
    
#     # Generate blob name with UTC timestamp
#     blob_name = f"weather_data_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
#     # Upload to blob storage
#     if not upload_data_blob(
#         CONFIG['storage_connection_string'],
#         CONFIG['container_name'],
#         blob_name,
#         json_data
#     ):
#         logging.error("Failed to upload data to blob storage")
#         return

#     logging.info("Weather data collection and storage completed successfully")