import azure.functions as func
import logging
import io
import zipfile
from azure.storage.blob import BlobServiceClient
import os

app = func.FunctionApp()

# Configuration
CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bikeflowproject;AccountKey=DuQuPGD9eyNGb9yOhylBdkR2TjVOfbhr7oSCWja1vJiLoJ+cZhZId1haQrvX98hTxTmr083rYdU6+AStLgh0JQ==;EndpointSuffix=core.windows.net"
DEST_CONTAINER = "bikes-raw-data/bluebikes-upziped-files"

@app.blob_trigger(arg_name="myblob", path="bikes-raw-data/bluebikes-zipfiles/{name}.zip",
                               connection="AzureWebJobsStorage")
def unzip_blob_function(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    # 1. Initialize Blob Client
    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # 2. Read the triggered blob into memory
    zip_data = myblob.read()
    
    with zipfile.ZipFile(io.BytesIO(zip_data)) as z:
        for file_name in z.namelist():
            # Skip directories if any exist in the zip
            if file_name.endswith('/'):
                continue
                
            logging.info(f"Unzipping {file_name}...")
            
            # 3. Get the file content
            with z.open(file_name) as extracted_file:
                # 4. Upload to the destination container
                blob_client = blob_service_client.get_blob_client(
                    container=DEST_CONTAINER, 
                    blob=f"unzipped/{file_name}"
                )
                blob_client.upload_blob(extracted_file, overwrite=True)

    logging.info("Unzipping process complete.")