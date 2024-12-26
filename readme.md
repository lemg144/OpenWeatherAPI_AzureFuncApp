# Weather Data Collector

An Azure Function that periodically collects weather data from OpenWeatherMap API and stores it in Azure Blob Storage.

## Features

- Collects weather data every 5 minutes using OpenWeatherMap API
- Stores data in Azure Blob Storage with timestamp-based naming
- Configurable location parameters (latitude and longitude)
- Environmental variable configuration for secure credential management

## Prerequisites

- Python 3.9 or higher
- Azure Functions Core Tools
- Azure Storage Account
- OpenWeatherMap API key
- Azure CLI (optional, for deployment)

## Installation

1. Clone the repository
2. Create a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
```
3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

The `local.settings.json` file should have the following structure:

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "",
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "API_KEY": "your_openweathermap_api_key",
        "STORAGE_CONNECTION_STRING": "your_storage_connection_string",
        "CONTAINERNAME": "your_container_name"
    }
}
```

## Local Development

1. Start Azurite (for local storage emulation):
```bash
azurite --silent --location c:\azurite --debug c:\azurite\debug.log
```

2. Run the function locally:
```bash
func start
```

## Deployment

Deploy to Azure Functions using Azure Functions Core Tools:

```bash
func azure functionapp publish <YourFunctionAppName>
```
