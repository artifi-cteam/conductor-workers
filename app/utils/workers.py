import tempfile
import time
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.configuration import AuthenticationSettings
from conductor.client.worker.worker import Worker
from dotenv import load_dotenv
import os
import requests
 
from app.utils.parsers import (
    parse_advanced_property,
    parse_auto,
    parse_general_liability,
    parse_property_json,
    parse_us_common,
)
 
load_dotenv(override=True)
 
# Conductor API URL
API_URL = "http://localhost:8080/api"
 
DATA_PACKAGE_IDS = [
    "elevate-us-common-c0001",
    "default-us-admitted-advanced-property-l0001",
    "default-us-loss-run-c0001",
    "elevate-us-gl-c0001",
    "elevate-us-property-l0001",
    "elevate-us-admitted-auto-c0001",
    "elevate-us-admitted-workers-comp-c0001",
]
 
# Authentication settings

# Conductor Configuration
config = Configuration(
    server_api_url=API_URL,
    auth_token_ttl_min=45,
)

def wait_for_file_upload(task):
    # Directly receive file from workflow input
    file = task.input_data.get('file')
    filename = task.input_data.get('filename')
    
    # Validate file
    if not file or not filename:
        raise ValueError("No file or filename provided")
    
    # Prepare output data with file details
    output_data = {
        "filename": filename,
        "file_content": file,  # Pass entire file content if needed
        "file_size": len(file),
        "validation_status": "success"
    }
    
    return output_data
 
# Worker for retrieving auth token
def my_task_function(task):
    print(f"Getting auth token for BP service")
 
    auth_url = "https://boldpenguin-auth-uat.beta.boldpenguin.com/auth/token"
    payload = {
        "client_id": os.getenv("BP_CLIENT_ID"),
        "client_secret": os.getenv("BP_CLIENT_SECRET"),
        "api_key": os.getenv("BP_API_KEY"),
        "grant_type": "client_credentials",
    }
 
    try:
        response = requests.post(auth_url, data=payload)
        response.raise_for_status()
        auth_data = response.json()
        token = auth_data.get("access_token", "")
 
        if token:
            print("Authentication successful. Token retrieved.")
            return {
                "status": "COMPLETED",
                "outputData": {"auth_token": token},
            }
        else:
            print("Failed to retrieve token.")
            return {"status": "FAILED", "error": "Authentication failed."}
 
    except requests.exceptions.RequestException as e:
        print(f"Error during authentication: {e}")
        return {"status": "FAILED", "error": str(e)}
 
 
def get_upload_url(task):
    input_data = task.input_data
    
    # Get filename from previous task (wait_for_file_upload)
    filename = input_data.get("filename", "default_filename.eml")
    token = input_data.get("auth_token", "")
 
    print(f"Get Upload URL task running for: {filename}")
    if not token:
        print("Error: Missing authentication token.")
        return {"status": "FAILED", "error": "Missing auth_token"}
 
    print(f"Token received: {token}")
 
    url = f"https://api-smartdata.di-beta.boldpenguin.com/universal/v4/universal-submit/file-upload-url"
    headers = {
        "x-api-key": os.getenv("BP_API_KEY"),
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    payload = {"filename": filename}
 
    try:
        print(url)
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        print("Upload URL retrieved successfully.")
 
        return {
            "status": "COMPLETED",
            "outputData": {
                "tx_id": data["tx_id"],
                "upload_url": data["upload_url"],
                "filename": filename  # Pass filename forward
            },
        }
    except requests.exceptions.RequestException as e:
        print(f"Error getting upload URL: {e}")
        return {"status": "FAILED", "error": str(e)}
 
 
def upload_file(task):
    """Uploads the file to the provided upload URL using multipart/form-data."""
    print(f"Uploading file worker at work sir!")
    try:
        input_data = task.input_data
        
        # Get file content and filename from previous task
        file_content = input_data.get("file_content")
        filename = input_data.get("filename", "default_filename.eml")
        upload_url = input_data.get("upload_url", "")
        
        if not file_content:
            raise ValueError("No file content provided")
        
        if not upload_url:
            raise ValueError("No upload URL provided")
        
        # Ensure file_content is in bytes
        if isinstance(file_content, str):
            file_content = file_content.encode('utf-8')
        
        # Create a temporary file to upload
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name
        
        try:
            with open(temp_file_path, "rb") as file:
                files = {
                    "file": (filename, file, "application/octet-stream")
                }
    
                response = requests.put(upload_url, files=files)
    
            print(f"Response status code: {response.status_code}")
            print(f"Response text: {response.text}")
    
            if response.status_code not in (200, 201):
                raise Exception(
                    f"Failed to upload file: {response.status_code} - {response.text}"
                )
    
            print(f"File '{filename}' uploaded successfully")
            
            return {
                "status": "COMPLETED",
                "outputData": {
                    "upload_status": "success",
                    "filename": filename
                }
            }
    
        finally:
            # Clean up the temporary file
            os.unlink(temp_file_path)
    
    except FileNotFoundError as e:
        print(e)
        return {"status": "FAILED", "error": str(e)}
    except requests.exceptions.RequestException as e:
        print(f"Request error while uploading file: {e}")
        return {"status": "FAILED", "error": str(e)}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"status": "FAILED", "error": str(e)}
 
 
def trigger_processing(task):
    input_data = task.input_data
    auth_token = input_data.get("auth_token", "")
    tx_id = input_data.get("tx_id", "")
    print(f"Process triggered for task id :{tx_id}")
    url = f"https://api-smartdata.di-beta.boldpenguin.com/universal/v4/universal-submit/file/{tx_id}"
    headers = {
        "x-api-key": os.getenv("BP_API_KEY"),
        "Authorization": f"Bearer {auth_token}",
    }
 
    response = requests.post(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error triggering file processing: {response.text}")
 
    return response.json()
 
 
def poll_submission_status(task):
    input_data = task.input_data
    auth_token = input_data.get("auth_token", "")
    tx_id = input_data.get("tx_id", "")
    
    retry_interval = 30  # Starting retry interval (in seconds)
    max_retry_interval = 120  # Maximum retry interval (in seconds), for example 30 minutes
    
    url = f"https://api-smartdata.di-beta.boldpenguin.com/universal/v4/universal-submit/status/{tx_id}"
    headers = {
        "x-api-key": os.getenv("BP_API_KEY"),
        "Authorization": f"Bearer {auth_token}",
    }
    
    # Initial request to check status
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Error fetching status: {response.text}")
    
    data = response.json()
    tx_status = data.get("tx_status")
    print(f"Transaction status: {tx_status}")
    
    # Retry logic for "Scheduled" status
    while tx_status not in ["COMPLETED", "Review_required", "FAILED"]:
        # Wait for the next retry with exponential backoff (doubling the interval)
        time.sleep(retry_interval)
        
        # Exponential backoff (doubling the interval each time), but not exceeding the max retry interval
        retry_interval = min(retry_interval * 2, max_retry_interval)
        
        # Fetch status again
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            raise Exception(f"Error fetching status: {response.text}")
        
        data = response.json()
        tx_status = data.get("tx_status")
        print(f"Transaction status: {tx_status}")
    
    return data  # Return the final status after retries
 
 
def fetch_submission_data(task):
    """
    Fetches submission data for a list of dp_ids and a single report_id.
    Returns a list of results in the same order as dp_ids.
    If a dp_id doesn't return data, None is added in its place.
    """
 
    input_data = task.input_data
    print(input_data)
    auth_token = input_data.get("auth_token", "")
    tx_id = input_data.get("tx_id", "")
    """
    Fetches and parses submission data for a given report_id.
    Calls respective parsers based on the data package ID.
    Returns a structured response with parsed results.
    """
    DATA_PACKAGE_IDS = [
        "elevate-us-common-c0001",
        "default-us-admitted-advanced-property-l0001",
        "default-us-loss-run-c0001",
        "elevate-us-gl-c0001",
        "elevate-us-property-l0001",
        "elevate-us-admitted-auto-c0001",
        "elevate-us-admitted-workers-comp-c0001",
    ]
 
    structured_response = {}
 
    for dp_id in DATA_PACKAGE_IDS:
        try:
            # Fetch data from API
            url = (
                f"https://api-smartdata.di-beta.boldpenguin.com/data/v4/{dp_id}/{tx_id}"
            )
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "x-api-key": os.getenv("BP_API_KEY"),
            }
            response = requests.get(url, headers=headers)
 
            if response.status_code == 200:
                raw_data = response.json()
 
                # Call the respective parser
                if dp_id == "elevate-us-common-c0001":
                    structured_response["Common"] = parse_us_common(raw_data)
                elif dp_id == "default-us-loss-run-c0001":
                    structured_response["Loss Run"] = raw_data.get("data", {})
                elif dp_id == "elevate-us-property-l0001":
                    structured_response["Property"] = parse_property_json(raw_data)
                elif dp_id == "default-us-admitted-advanced-property-l0001":
                    structured_response["Advanced Property"] = parse_advanced_property(
                        raw_data
                    )
                elif dp_id == "elevate-us-gl-c0001":
                    structured_response["General Liability"] = parse_general_liability(
                        raw_data
                    )
                elif dp_id == "elevate-us-admitted-auto-c0001":
                    structured_response["Auto"] = parse_auto(raw_data)
                elif dp_id == "elevate-us-admitted-workers-comp-c0001":
                    structured_response["Workers Compensation"] = raw_data.get(
                        "data", {}
                    )
                # Handle other dp_ids as needed
            else:
                print(f"Failed to fetch {dp_id}: {response.status_code}")
        except Exception as e:
            print(f"Error fetching {dp_id}: {e}")
 
    return structured_response
 
 
# Register Workers
worker_auth = Worker(
    task_definition_name="generate_auth_token", execute_function=my_task_function
)
worker_get_upload_url = Worker(
    task_definition_name="get_upload_url", execute_function=get_upload_url
)
worker_upload_file = Worker(
    task_definition_name="upload_file", execute_function=upload_file
)
worker_trigger_processing = Worker(
    task_definition_name="trigger_processing", execute_function=trigger_processing
)
worker_poll_submission_status = Worker(
    task_definition_name="poll_submission_status",
    execute_function=poll_submission_status,
)
worker_fetch_submission_data = Worker(
    task_definition_name="fetch_submission_data",
    execute_function=fetch_submission_data,
)
worker_callback = Worker(
    task_definition_name="wait_for_file_upload",
    execute_function=wait_for_file_upload
)
 
handler = TaskHandler(
    configuration=config,
    workers=[
        worker_callback,
        worker_auth,
        worker_get_upload_url,
        worker_upload_file,
        worker_trigger_processing,
        worker_poll_submission_status,
        worker_fetch_submission_data,
    ],
)
handler.start_processes()