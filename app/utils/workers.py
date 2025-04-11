from datetime import datetime
import uuid
from app.utils.bold_penguin_workers import get_upload_url, my_task_function, poll_submission_status, trigger_processing, upload_file
from app.utils.conductor_logger import log_message
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.configuration.configuration import Configuration
from conductor.client.worker.worker import Worker
from dotenv import load_dotenv
import os
import requests
from app.service.mongo_service import save_report_data, client
from app.utils.eml_file_handlers import cleanup_eml_file_worker, package_to_eml_worker
from app.utils.parsers import (
    parse_advanced_property,
    parse_auto,
    parse_general_liability,
    parse_property_json,
    parse_us_common,
)
 
load_dotenv(override=True)
 
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
 
# Worker for retrieving auth token

def fetch_submission_data(task):
    """
    Fetches submission data for a list of dp_ids and a single report_id.
    Returns a list of results in the same order as dp_ids.
    If a dp_id doesn't return data, None is added in its place.
    """
 
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,input_data)
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
                log_message(task_id,f"Failed to fetch {dp_id}: {response.status_code}")
        except Exception as e:
            log_message(task_id,f"Error fetching {dp_id}: {e}")
 
    return structured_response

def call_agent_service(task):
    task_id = task.task_id
    try:
        # input_data = task.input_data.get("submission_data")
        # log_message(task_id,f"Calling agent service for {input_data}")

        # filtered_data = {key: input_data[key] for key in ["Common", "Property", "Advanced Property"]}

        # response = requests.post(
        #     'http://34.229.60.129:31480/query',
        #     json={"message": str(filtered_data)}
        # )

        return "Agent service is not working."

    except Exception as e:
        log_message(task_id,f"Error in call_agent_service: {e}")
        raise


    # Call agent service with whole data:

def send_to_service_now(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,f"Sending the data to service now")

    url = "https://elevatenowtechdemo1.service-now.com/api/x_elete_ins/load_package/commons"
    headers = {"Content-Type": "application/json"}

    try:
        # Extract actual input parameters
        case_id = input_data.get("case_id")
        tx_id = input_data.get("tx_id")
        agent_output = input_data.get("agent_output", {})
        submission_data = input_data.get("submission_data", {})

        # Extract insights from agent_output
        appetite = "agent service is not working"
        property_insights = "agent service is not working"
        loss_insights = "agent service is not working"

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
        log_message(task_id,f"Response status: {response.status_code}")
        log_message(task_id,f"Response body: {response.json()}")

        return response.json()

    except Exception as e:
        log_message(task_id,f"Error sending data to ServiceNow: {e}")
        raise




def push_to_mongo(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,input_data)

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
worker_package_to_eml = Worker(
    task_definition_name = "package_to_eml",
    execute_function = package_to_eml_worker
)
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
worker_cleanup_eml_file_worker = Worker(
    task_definition_name= "cleanup_eml_file",
    execute_function=cleanup_eml_file_worker
)
 
handler = TaskHandler(
    configuration=config,
    workers=[
        worker_package_to_eml,
        worker_auth,
        worker_get_upload_url,
        worker_upload_file,
        worker_trigger_processing,
        worker_poll_submission_status,
        worker_fetch_submission_data,
        worker_call_agent_service,
        worker_push_to_mongo,
        worker_send_to_service_now,
        worker_cleanup_eml_file_worker
    ],
)
handler.start_processes()