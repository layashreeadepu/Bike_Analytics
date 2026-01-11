import requests
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from azure.storage.blob import BlobServiceClient

# --- Azure Configuration ---
AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=bikeflowproject;AccountKey=DuQuPGD9eyNGb9yOhylBdkR2TjVOfbhr7oSCWja1vJiLoJ+cZhZId1haQrvX98hTxTmr083rYdU6+AStLgh0JQ==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "bikes-raw-data/bluebikes-zipfiles"

def run_upload():
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(CONTAINER_NAME)

    # Setup Browser
    website_url = "https://s3.amazonaws.com/hubway-data/index.html"
    driver = webdriver.Chrome() 
    
    try:
        driver.get(website_url)
        wait = WebDriverWait(driver, 15)
        
        # Wait for links to appear
        wait.until(EC.presence_of_element_located((By.XPATH, '//a[contains(@href, ".zip")]')))
        zip_links = driver.find_elements(By.XPATH, '//a[contains(@href, ".zip")]')
        
        print(f"Total files found: {len(zip_links)}")

        for link in zip_links:
            url = link.get_attribute("href")
            file_name = link.text
            
            if not file_name: continue 

            print(f"Uploading {file_name}...")
            
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                blob_client = container_client.get_blob_client(file_name)
                # Use upload_blob with the raw stream
                blob_client.upload_blob(response.raw, overwrite=True)
            else:
                print(f"Failed to download {file_name}")
                    
        print("--- All uploads to Azure complete! ---")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    run_upload()