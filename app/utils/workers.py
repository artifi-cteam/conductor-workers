from datetime import datetime, timezone
import json
import tempfile
import time
import uuid
from app.utils.conductor_logger import log_message
from app.utils.dummy_response import dummy_response
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.configuration import AuthenticationSettings
from conductor.client.worker.worker import Worker
from dotenv import load_dotenv
import os
import requests
from app.service.mongo_service import save_report_data, client
from app.utils.parsers import (
    parse_advanced_property,
    parse_auto,
    parse_general_liability,
    parse_property_json,
    parse_us_common,
)
 
load_dotenv(override=True)

print("Starting worker...")
start = time.time()
 
# Conductor API URL
API_URL = os.getenv('CONDUCTOR_URL')
 
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
    """
    Polls the submission status API until a terminal state is reached,
    using exponential backoff (max delay 120 seconds), and logs progress.
    """
    from datetime import datetime, timezone
    import json, os, time, requests

    def log_message(task_id, message):
        print(f"{datetime.now(timezone.utc).isoformat()} [Task {task_id}] {message}")

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

        retry_interval_sec = 60  # initial polling interval (seconds)
        max_retry_interval_sec = 120  # max polling interval (seconds)
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

def call_agent_service(task):
    try:
        input_data = task.input_data.get("submission_data")
        print(f"Calling agent service for {input_data}")

        filtered_data = {key: input_data[key] for key in ["Common", "Property", "Advanced Property"]}

        # response = requests.post(
        #     'http://34.229.60.129:31480/query',
        #     json={"message": str(filtered_data)}
        # )

        return "Agent service is not working."

    except Exception as e:
        print(f"Error in call_agent_service: {e}")
        raise


    # Call agent service with whole data:



def send_to_service_now(task):
    input_data = task.input_data
    print(f"Sending the data to service now: {input_data}")

    url = "https://elevatenowtechdemo1.service-now.com/api/x_elete_ins/load_package/commons"
    headers = {"Content-Type": "application/json"}

    try:
        # Extract actual input parameters
        case_id = input_data.get("case_id")
        tx_id = input_data.get("tx_id")
        agent_output = input_data.get("agent_output", {})
        submission_data = input_data.get("submission_data", {})

        # Extract insights from agent_output
        appetite = agent_output
        property_insights = agent_output
        loss_insights = agent_output

        data = {
            "case_id": case_id,
            "tx_id": tx_id,
            "parsed_data": submission_data,
            "insights": {
                "property_insights": str(property_insights),
                "loss": str(loss_insights),
                "appetite": str(appetite),
            },
        }

        # data = dummy_response

        response = requests.post(url, headers=headers, json=data)
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.json()}")

        return response

    except Exception as e:
        print(f"Error sending data to ServiceNow: {e}")
        raise




def push_to_mongo(task):
    input_data = task.input_data
    print(input_data)

    db = client["Submission_Intake"]
    tx_id = input_data.get("tx_id", "")
    case_id = input_data.get("case_id", "")
    file_data = input_data.get("file", "")
    submission_data = input_data.get("submission_data", {})
    agent_response = input_data.get("agent_output", {})

    artifi_id = str(uuid.uuid4())
    timestamp = datetime.now()

    # BP_DATA
    bp_data_doc = {
        "artifi_id": artifi_id,
        "case_id": case_id,
        "file_data": file_data,
        "created_at": timestamp
    }
    db["BP_DATA"].insert_one(bp_data_doc)

    # BP_service
    report_data = submission_data
    save_report_data(report_data, artifi_id, tx_id)  # This already adds timestamp inside

    # AGENT_RESPONSES
    agent_response_doc = {
        "artifi_id": artifi_id,
        "agent_response": agent_response,
        "created_at": timestamp
    }
    db["AGENT_RESPONSES"].insert_one(agent_response_doc)
 
 
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
worker_send_to_service_now = Worker(
    task_definition_name = "send_to_service_now",
    execute_function = send_to_service_now
)
worker_call_agent_service = Worker(
    task_definition_name = "call_agent_service",
    execute_function = call_agent_service
)
worker_push_to_mongo = Worker(
    task_definition_name = "push_to_mongo",
    execute_function = push_to_mongo
)
 
handler = TaskHandler(
    configuration=config,
    workers=[
        worker_auth,
        worker_get_upload_url,
        worker_upload_file,
        worker_trigger_processing,
        worker_poll_submission_status,
        worker_fetch_submission_data,
        worker_callback,
        worker_call_agent_service,
        worker_send_to_service_now,
        worker_push_to_mongo
    ],
)
handler.start_processes()
print(f"Worker started in {time.time() - start:.2f}s")