import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.storage.blob import BlobServiceClient

# --- Azure Configuration ---
# Get this from Azure Portal: Storage Account -> Access Keys -> Connection String
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bikeflowproject;AccountKey=zqgOdUlvtvTRnvgMCV6NQPFh7Qw08D+yufsDepsYzn246/wRdh+wrFO2BS5kNbKYf6Y3umfk/n7s+AStNSE1Rg==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "bikes-raw-data"

# Initialize Azure Blob Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# 1. Setup Browser
website_url = "https://s3.amazonaws.com/hubway-data/index.html"
driver = webdriver.Chrome() 
driver.get(website_url)

# 2. Wait for JavaScript
wait = WebDriverWait(driver, 10)
try:
    wait.until(EC.presence_of_element_located((By.XPATH, '//a[contains(@href, ".zip")]')))
    zip_links = driver.find_elements(By.XPATH, '//a[contains(@href, ".zip")]')
    print(f"Total files found: {len(zip_links)}")

    # 3. Loop and Upload Directly to Azure
    for link in zip_links:
        url = link.get_attribute("href")
        file_name = link.text
        
        print(f"Uploading {file_name} to Azure container: {CONTAINER_NAME}...")
        
        # Stream the download from S3
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            # Create a blob client for the specific file
            blob_client = container_client.get_blob_client(file_name)
            
            # Upload the stream directly to Azure (no local save needed)
            blob_client.upload_blob(response.raw, overwrite=True)
        else:
            print(f"Failed to download {file_name}")
                
    print("--- All uploads to Azure complete! ---")

finally:
    driver.quit()