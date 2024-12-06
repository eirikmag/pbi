import requests
import json
from datetime import datetime, timedelta
import time
import os

# Get the directory containing the script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# set up parameters for data model
tenant_id = "add3b991-e43e-46af-9738-637484ef4f25"

def extract_ids_from_url(url):
    """Extract workspace_id and dataset_id from a Power BI URL."""
    try:
        workspace_id = url.split('/groups/')[1].split('/')[0]
        dataset_id = url.split('/datasets/')[1].split('/')[0].split('?')[0]
        return workspace_id, dataset_id
    except Exception as e:
        print(f"Error parsing URL: {e}")
        return None, None

# Prompt for dataset URL and refresh options
dataset_url = input("Enter the Power BI dataset URL: ")
workspace_id, dataset_id = extract_ids_from_url(dataset_url)

if not workspace_id or not dataset_id:
    print("Failed to extract workspace and dataset IDs from URL. Please check the URL format.")
    exit(1)

refresh_full = input("Fully refresh model (Y/N)?: ")
report_back = input("Report back on refresh status (Y/N)?: ")

report_refresh_status = True if report_back.lower() == 'y' else False

# Set correct api apply_refresh_policy
if refresh_full.lower() == 'y':  # Full
    apply_refresh_policy = False
    refresh_type = 'Full'
else:  # Delta
    apply_refresh_policy = True
    refresh_type = 'Full'

def get_credentials_from_file(filepath):
    full_path = os.path.join(SCRIPT_DIR, filepath)
    with open(full_path, 'r') as file:
        data = json.load(file)
    return data.get("client_id"), data.get("client_secret")

# Main functions
def get_access_token(client_id, client_secret, tenant_id):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "https://analysis.windows.net/powerbi/api/.default"
    }
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    return response.json()["access_token"]

def refresh_dataset(workspace_id, dataset_id, access_token, refresh_type="Full", commit_mode="transactional", max_parallelism=2, retry_count=2, objects=None, apply_refresh_policy=True, effective_date=None):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    body = {
        "type": refresh_type,
        "commitMode": commit_mode,
        "maxParallelism": max_parallelism,
        "retryCount": retry_count,
        "applyRefreshPolicy": apply_refresh_policy,
    }
    
    if objects is not None:
        body["objects"] = objects
    
    if effective_date is not None:
        body["effectiveDate"] = effective_date
    
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    location_header = response.headers.get("Location")
    return response, location_header

def check_refresh_status(workspace_id, dataset_id, request_id, access_token):
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes/{request_id}"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def wait_for_refresh_completion(workspace_id, dataset_id, request_id, access_token, timeout=3000):
    start_time = time.time()
    key_part = "refreshes/"
    refresh_id = request_id.split(key_part)[-1] if key_part in request_id else None
    
    while True:
        response = check_refresh_status(workspace_id, dataset_id, refresh_id, access_token)
        status = response['status']
        if status == 'Completed':
            return f"Refresh completed for dataset_id: {dataset_id} and refresh_id {refresh_id}"
        if time.time() - start_time > timeout:
            return f"Timeout reached. The refresh did not complete within the specified time for dataset_id: {dataset_id} and refresh_id {refresh_id}"
        
        print("Refresh still in progress, checking again in 30 seconds...")
        time.sleep(30)

# Get secrets for authentication to PBI API
client_id, client_secret = get_credentials_from_file('pbi_client_info_secret.json')

refresh_timeout = 3000  # Default timeout for pbi model refreshes

# Generate access token for authentication
access_token = get_access_token(client_id, client_secret, tenant_id)

print("apply_refresh_policy:", apply_refresh_policy)
print("refresh_type:", refresh_type)
print(f"Workspace ID: {workspace_id}")
print(f"Dataset ID: {dataset_id}")

# Initiate refresh operation
refresh_response, request_id = refresh_dataset(
    workspace_id,
    dataset_id,
    access_token,
    refresh_type=refresh_type,
    commit_mode="transactional",
    max_parallelism=2,
    retry_count=2,
    apply_refresh_policy=apply_refresh_policy
)

print("Refresh initiated:", refresh_response)
print("Request ID:", request_id)

if report_refresh_status:
    refresh_result = wait_for_refresh_completion(workspace_id, dataset_id, request_id, access_token, refresh_timeout)
    print(refresh_result)