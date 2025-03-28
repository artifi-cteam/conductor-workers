import tempfile
import time
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.configuration import AuthenticationSettings
from conductor.client.worker.worker import Worker
from dotenv import load_dotenv
import os
import requests
 
from app.service.mongo_service import save_report_data
from parsers import (
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
    print("Returning dummy auth token for BP service")

    dummy_token = "dummy_access_token_12345"  # Replace with your desired dummy token

    return {
        "status": "COMPLETED",
        "outputData": {"auth_token": dummy_token},
    }

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

    # Simulated response data
    dummy_tx_id = "dummy_tx_id_12345"
    dummy_upload_url = "https://dummy-upload-url.com/upload"

    print("Returning dummy upload URL.")

    return {
        "status": "COMPLETED",
        "outputData": {
            "tx_id": dummy_tx_id,
            "upload_url": dummy_upload_url,
            "filename": filename  # Pass filename forward
        },
    }

def upload_file(task):
    """Simulates uploading the file to the provided upload URL."""
    print(f"Simulating file upload worker!")
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
            # Simulate a successful upload response
            print(f"Simulating upload to URL: {upload_url}")
            print(f"Simulating file '{filename}' upload.")

            #dummy response to emulate a success.
            class DummyResponse:
                status_code = 200
                text = "Simulated upload success"

            response = DummyResponse()

            print(f"Response status code: {response.status_code}")
            print(f"Response text: {response.text}")

            if response.status_code not in (200, 201):
                raise Exception(
                    f"Failed to upload file: {response.status_code} - {response.text}"
                )
            
            print(f"File '{filename}' simulated upload successful")
            
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
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"status": "FAILED", "error": str(e)}
    

def trigger_processing(task):
    input_data = task.input_data
    auth_token = input_data.get("auth_token", "")
    tx_id = input_data.get("tx_id", "")
    print(f"Simulating process triggered for task id: {tx_id}")

    # Simulate a successful response
    dummy_response = {
        "status": "processing_started",
        "message": "File processing has been triggered.",
        "tx_id": tx_id,
        # Add any other relevant dummy data here
    }

    print("Simulating successful trigger.")
    return dummy_response

def poll_submission_status(task):
    input_data = task.input_data
    tx_id = input_data.get("tx_id", "")

    retry_interval = 3  # Reduced retry interval for testing
    max_retry_interval = 10  # Reduced max retry interval for testing

    # Simulate transaction status progression
    status_sequence = ["Scheduled", "Processing", "COMPLETED"]
    status_index = 0

    print(f"Simulating polling submission status for tx_id: {tx_id}")

    while status_index < len(status_sequence):
        tx_status = status_sequence[status_index]
        print(f"Simulated transaction status: {tx_status}")

        if tx_status in ["COMPLETED", "Review_required", "FAILED"]:
            return {"tx_status": tx_status, "tx_id": tx_id}

        time.sleep(retry_interval)
        retry_interval = min(retry_interval * 2, max_retry_interval)
        status_index += 1

    # In case the loop completes without a terminal status
    return {"tx_status": "FAILED", "tx_id": tx_id, "error": "Simulated timeout"}
 
def fetch_submission_data(task):
    """
    Simulates fetching submission data for a list of dp_ids and a single report_id.
    """

    input_data = task.input_data
    print(input_data)
    tx_id = input_data.get("tx_id", "")

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
            # Simulate fetching data from API
            print(f"Simulating fetching data for {dp_id} and tx_id: {tx_id}")

            # Simulate dummy JSON data
            if dp_id == "elevate-us-common-c0001":
                structured_response["Common"] = {"common_field": "dummy_common_data"}
            elif dp_id == "default-us-loss-run-c0001":
                structured_response["Loss Run"] = {"loss_run_field": "dummy_loss_run_data"}
            elif dp_id == "elevate-us-property-l0001":
                structured_response["Property"] = {"property_field": "dummy_property_data"}
            elif dp_id == "default-us-admitted-advanced-property-l0001":
                structured_response["Advanced Property"] = {"advanced_property_field": "dummy_advanced_property_data"}
            elif dp_id == "elevate-us-gl-c0001":
                structured_response["General Liability"] = {"general_liability_field": "dummy_general_liability_data"}
            elif dp_id == "elevate-us-admitted-auto-c0001":
                structured_response["Auto"] = {"auto_field": "dummy_auto_data"}
            elif dp_id == "elevate-us-admitted-workers-comp-c0001":
                structured_response["Workers Compensation"] = {"workers_comp_field": "dummy_workers_comp_data"}
            else:
                print(f"Simulated data for {dp_id}")
        except Exception as e:
            print(f"Error simulating fetching {dp_id}: {e}")

    return structured_response

def push_to_mongo(task):
    input_data = task.input_data
    print(input_data)
    tx_id = input_data.get("tx_id","")
    report_data = input_data.get("report_id","")
    artifi_id = "TEST001"

    save_report_data(report_data, artifi_id, tx_id)
 
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