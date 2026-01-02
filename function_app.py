import azure.functions as func
from azure.storage.blob import BlobServiceClient
import logging
import re
import requests
from bs4 import BeautifulSoup


# The "app" instance must be at the top level
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="manual_scrape")
def manual_bike_scrape(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Manual scrape triggered via HTTP.')

    # 1. Setup Storage Client
    # Replace with your actual connection string or use an environment variable
    connection_string = "zqgOdUlvtvTRnvgMCV6NQPFh7Qw08D+yufsDepsYzn246/wRdh+wrFO2BS5kNbKYf6Y3umfk/n7s+AStNSE1Rg=="
    container_name = "bikes-raw-data"
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)
        
        # Ensure container exists
        if not container_client.exists():

            container_client.create_container()

        # 2. Scrape the HTML page
        index_url = "https://s3.amazonaws.com/hubway-data/index.html"
        base_download_url = "https://s3.amazonaws.com/hubway-data/"
        
        response = requests.get(index_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Regex to find all zip files
        zip_links = soup.find_all('a', href=re.compile(r'.*\.zip$', re.IGNORECASE))
        
        files_processed = []

        for link in zip_links:
            file_name = link.get('href')
            full_url = base_download_url + file_name
            
            logging.info(f"Processing: {file_name}")
            
            # 3. Stream data to Blob Storage
            with requests.get(full_url, stream=True) as r:
                r.raise_for_status()
                blob_client = container_client.get_blob_client(file_name)
                
                # Directly upload the raw stream
                blob_client.upload_blob(r.raw, overwrite=True)
                files_processed.append(file_name)

        return func.HttpResponse(
            f"Successfully uploaded {len(files_processed)} files to Blob Storage.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)