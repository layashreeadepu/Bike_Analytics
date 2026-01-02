import azure.functions as func
import requests
import re
import json
from bs4 import BeautifulSoup

def main(req: func.HttpRequest) -> func.HttpResponse:
    # 1. Get Parameters from ADF (the caller)
    req_body = req.get_json()
    target_url = req_body.get('target_url')      # e.g., https://s3.amazonaws.com/hubway-data/
    regex_pattern = req_body.get('pattern')      # e.g., \d{6}-hubway-tripdata\.zip
    user_email = req_body.get('email')           # For logging/metadata

    if not target_url or not regex_pattern:
        return func.HttpResponse("Missing URL or Pattern", status_code=400)

    try:
        # 2. Scrape the page
        response = requests.get(target_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 3. Apply Regex Pattern
        all_links = [a['href'] for a in soup.find_all('a', href=True)]
        matched_files = [f for f in all_links if re.search(regex_pattern, f)]

        # 4. Return JSON list to ADF
        return func.HttpResponse(
            body=json.dumps({
                "file_list": matched_files,
                "count": len(matched_files),
                "processed_by": user_email
            }),
            mimetype="application/json",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)