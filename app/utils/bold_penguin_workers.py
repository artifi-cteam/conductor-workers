from datetime import datetime, timezone
import mimetypes
import os
import tempfile
import time

import requests

from app.utils.conductor_logger import log_message

def my_task_function(task):

    task_id = task.task_id
    log_message(task_id,f"Getting auth token for BP service")
 
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
            log_message(task_id,"Authentication successful. Token retrieved.")
            return {
                "status": "COMPLETED",
                "outputData": {"auth_token": token},
            }
        else:
            log_message(task_id,"Failed to retrieve token.")
            return {"status": "FAILED", "error": "Authentication failed."}
 
    except requests.exceptions.RequestException as e:
        log_message(task_id,f"Error during authentication: {e}")
        return {"status": "FAILED", "error": str(e)}
 
 
def get_upload_url(task):
    input_data = task.input_data
    task_id = task.task_id
    
    # Get filename from previous task (wait_for_file_upload)
    filename = input_data.get("filename", "default_filename.eml")
    token = input_data.get("auth_token", "")
 
    log_message(task_id,f"Get Upload URL task running for: {filename}")
    if not token:
        log_message(task_id,"Error: Missing authentication token.")
        return {"status": "FAILED", "error": "Missing auth_token"}
 
    log_message(task_id,f"Token received: {token}")
 
    url = f"https://api-smartdata.di-beta.boldpenguin.com/universal/v4/universal-submit/file-upload-url"
    headers = {
        "x-api-key": os.getenv("BP_API_KEY"),
        "Authorization": f"Bearer {token.strip()}",
        "Content-Type": "application/json",
    }
    payload = {"filename": filename}
 
    try:
        log_message(task_id,url)
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()
        log_message(task_id,"Upload URL retrieved successfully.")
 
        return {
            "status": "COMPLETED",
            "outputData": {
                "tx_id": data["tx_id"],
                "upload_url": data["upload_url"],
                "filename": filename  # Pass filename forward
            },
        }
    except requests.exceptions.RequestException as e:
        log_message(task_id,f"Error getting upload URL: {e}")
        return {"status": "FAILED", "error": str(e)}
 
 
def upload_file(task):
    """
    Uploads an existing file (specified by path) to the provided upload URL
    using multipart/form-data via PUT request.
    Expects inputParameters: eml_file_path, upload_url
    Outputs: upload_status, uploaded_filename
    """
    task_id = task.task_id
    log_message(task_id,f"Starting upload_file worker for task: {task_id}")

    try:
        input_data = task.input_data

        # --- 1. Extract Input Parameters ---
        # Get the PATH to the EML file created by the previous task
        eml_file_path = input_data.get("filename")
        upload_url = input_data.get("upload_url")

        # --- 2. Validate Inputs ---
        if not eml_file_path:
            raise ValueError("Missing required input parameter: filename")
        if not upload_url:
            raise ValueError("Missing required input parameter: upload_url")

        log_message(task_id,f"Target Upload URL: {upload_url}")
        log_message(task_id,f"File path to upload: {eml_file_path}")

        # Check if the source file actually exists before attempting to upload
        if not os.path.exists(eml_file_path):
            raise FileNotFoundError(f"The specified file does not exist: {eml_file_path}")

        # Determine the filename part for the multipart request
        upload_filename = os.path.basename(eml_file_path)

        
        content_type = 'application/octet-stream'

        log_message(task_id,f"Uploading file '{upload_filename}' with content type '{content_type}'")

        # --- 3. Open Existing File and Upload ---
        with open(eml_file_path, "rb") as file_handle:
            files = {
                # The key ("file") should match what the receiving API expects
                "file": (upload_filename, file_handle, content_type)
            }

            # Using PUT as in the original example. Change to POST if needed.
            response = requests.put(upload_url, files=files, timeout=60) # Added timeout

        log_message(task_id,f"Upload response status code: {response.status_code}")
        log_message(task_id,f"Upload response text: {response.text[:500]}...") # Log snippet of response

        # Check for successful HTTP status codes (adjust as needed for your API)
        # Common success codes for upload: 200 OK, 201 Created, 202 Accepted, 204 No Content
        if not (200 <= response.status_code < 300):
            error_message = f"Failed to upload file: {response.status_code} - {response.text}"
            log_message(task_id,error_message)
            # Optionally include more details if response is JSON
            try:
                 response_json = response.json()
                 log_message(task_id,f"API Error Details: {response_json}")
                 error_message = f"Failed to upload file: {response.status_code} - {response_json}"
            except ValueError: # response is not JSON
                 pass
            raise Exception(error_message)

        log_message(task_id,f"File '{upload_filename}' uploaded successfully to {upload_url}")

        # --- 4. Return Success Status ---
        return {
            'status': 'COMPLETED',
            'outputData': {
                'upload_status': 'success',
                'uploaded_filename': upload_filename # Return the name used for upload
            },
            'logs': [f"File '{upload_filename}' uploaded successfully."]
        }

    # Specific error handling first
    except FileNotFoundError as e:
        log_message(task_id,f"File not found error in task {task_id}: {e}")
        return {'status': 'FAILED', 'reasonForFailure': 'Upload_File_Not_Found', 'logs': [str(e)]}
    except requests.exceptions.RequestException as e:
        log_message(task_id,f"Request error while uploading file in task {task_id}: {e}")
        return {'status': 'FAILED', 'reasonForFailure': 'Upload_Request_Error', 'logs': [str(e)]}
    except ValueError as e: # Catch input validation errors
         log_message(task_id,f"Input validation error in task {task_id}: {e}")
         return {'status': 'FAILED', 'reasonForFailure': 'Upload_Input_Error', 'logs': [str(e)]}
    # General error handling last
    except Exception as e:
        log_message(task_id,f"Unexpected error in upload_file worker for task {task_id}: {e}") # Log traceback
        return {
            'status': 'FAILED',
            'reasonForFailure': 'Upload_Unexpected_Error',
            'logs': [f"Unexpected error during upload task {task_id}: {str(e)}"]
        }
 
 
def trigger_processing(task):
    input_data = task.input_data
    task_id = task.task_id
    auth_token = input_data.get("auth_token", "")
    tx_id = input_data.get("tx_id", "")
    log_message(task_id,f"Process triggered for task id :{tx_id}")
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
    """
    Polls the submission status API until a terminal state is reached,
    using exponential backoff (max delay 120 seconds), and logs progress.
    """

    task_id = task.task_id
    workflow_instance_id = task.workflow_instance_id
    log_message(task_id, f"Task poll_submission_status started. Workflow: {workflow_instance_id}")

    try:
        input_data = task.input_data
        auth_token = input_data.get("auth_token")
        tx_id = input_data.get("tx_id")

        if not auth_token:
            raise ValueError("Missing 'auth_token' in input data")
        if not tx_id:
            raise ValueError("Missing 'tx_id' in input data")
        if not os.getenv("BP_API_KEY"):
            raise ValueError("Environment variable BP_API_KEY is not set")

        retry_interval_sec = 180  # initial polling interval (seconds)
        max_retry_interval_sec = 180  # max polling interval (seconds)
        url = f"https://api-smartdata.di-beta.boldpenguin.com/universal/v4/universal-submit/status/{tx_id}"
        headers = {
            "x-api-key": os.getenv("BP_API_KEY"),
            "Authorization": f"Bearer {auth_token}",
            "Accept": "application/json"
        }

        log_message(task_id, f"Making initial status request to: {url}")
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code != 200:
            error_msg = f"Initial API call failed with status {response.status_code}: {response.text}"
            log_message(task_id, error_msg)
            return {
                "status": "FAILED",
                "reasonForIncompletion": error_msg,
                "logs": [f"{datetime.now(timezone.utc).isoformat()} - {error_msg}"]
            }

        data = response.json()
        tx_status = data.get("tx_status")
        log_message(task_id, f"Initial status received: {tx_status}")

        # Continue polling until a terminal state is reached
        while tx_status not in ["COMPLETED", "Review_required", "FAILED"]:
            log_message(task_id, f"Current status '{tx_status}'. Sleeping for {retry_interval_sec}s.")
            time.sleep(retry_interval_sec)
            log_message(task_id, f"Polling status request to: {url}")
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                error_msg = f"Polling API call failed with status {response.status_code}: {response.text}"
                log_message(task_id, error_msg)
                return {
                    "status": "FAILED",
                    "reasonForIncompletion": error_msg,
                    "logs": [f"{datetime.now(timezone.utc).isoformat()} - {error_msg}"]
                }
            data = response.json()
            new_status = data.get("tx_status")
            log_message(task_id, f"Polled status received: {new_status}")
            tx_status = new_status
            retry_interval_sec = min(retry_interval_sec * 2, max_retry_interval_sec)

        log_message(task_id, f"Polling finished. Final status: {tx_status}.")
        return {
            "status": "COMPLETED",
            "outputData": data
        }

    except ValueError as ve:
        error_msg = f"Configuration/Input Error: {ve}"
        log_message(task_id, error_msg)
        return {
            "status": "FAILED",
            "reasonForIncompletion": error_msg,
            "logs": [f"{datetime.now(timezone.utc).isoformat()} - {error_msg}"]
        }
    except Exception as e:
        error_msg = f"Unexpected error: {type(e).__name__} - {e}"
        log_message(task_id, error_msg)
        return {
            "status": "FAILED",
            "reasonForIncompletion": error_msg,
            "logs": [f"{datetime.now(timezone.utc).isoformat()} - {error_msg}"]
        }
