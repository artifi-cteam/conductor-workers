from datetime import datetime
import json
import uuid
from app.utils.agent_service import call_agent_service, call_agent_service_rerun
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
from app.utils.service_now import send_to_service_now, send_to_service_now_rerun_worker
 
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

# Conductor Configuration
config = Configuration(
    server_api_url=API_URL,
    auth_token_ttl_min=45,
)

# Worker for retrieving auth token

def fetch_submission_data(task):
    """
    Fetches submission data for a list of dp_ids and a single report_id.
    Returns a structured combined JSON from all data packages and writes it to a file.
    """
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,"Fetching message..")

    auth_token = input_data.get("auth_token", "")
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
            url = f"https://api-smartdata.di-beta.boldpenguin.com/data/v5/{dp_id}/{tx_id}"
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


def push_to_mongo(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id,"Pusing to Mongo...")

    db = client["Submission_Intake"]
    tx_id = input_data.get("tx_id", "")
    case_id = input_data.get("case_id", "")
    submission_data = input_data.get("submission_data", {})
    agent_response = input_data.get("agent_output", {})

    artifi_id = str(uuid.uuid4())
    timestamp = datetime.now()

    # BP_DATA (upsert by case_id)
    bp_data_doc = {
        "artifi_id": artifi_id,
        "tx_id": tx_id,
        "case_id": case_id,
        "submission_data": submission_data,
        "history_sequence_id": 1,
        "transaction_type": "Initial",
        "created_at": timestamp
    }
    db["BP_DATA"].update_one(
        {"case_id": case_id},
        {"$set": bp_data_doc},
        upsert=True
    )

    # BP_service (assumes idempotent save)
    report_data = submission_data
    save_report_data(report_data, artifi_id, tx_id)

    # AGENT_RESPONSES (upsert by case_id)
    agent_response_doc = {
        "artifi_id": artifi_id,
        "tx_id": tx_id,
        "case_id": case_id,
        "agent_response": agent_response,
        "history_sequence_id": 1,
        "transaction_type": "Initial",
        "created_at": timestamp
    }
    db["AGENT_RESPONSES"].update_one(
        {"case_id": case_id},
        {"$set": agent_response_doc},
        upsert=True
    )

def push_to_mongo_updated(task):
    input_data = task.input_data
    task_id = task.task_id
    log_message(task_id, "Pushing updated data to Mongo...")

    try:
        db = client["Submission_Intake"]
        case_id = input_data.get("case_id", "")
        submission_data = input_data.get("submission_data", {})
        agent_response = input_data.get("agent_output", {})
        timestamp = datetime.now()

        # Fetch latest BP_DATA
        try:
            existing_doc = list(db["BP_DATA"].find({"case_id": case_id}).sort("created_at", -1).limit(1))
            if not existing_doc:
                log_message(task_id, f"No BP_DATA found for case_id: {case_id}")
                return
            tx_id = existing_doc[0]["tx_id"]
            artifi_id = existing_doc[0]["artifi_id"]
        except Exception as e:
            log_message(task_id, f"Error fetching BP_DATA: {str(e)}")
            return

        # Get latest history_sequence_id
        try:
            last_response = list(db["AGENT_RESPONSES"].find({"case_id": case_id}).sort("history_sequence_id", -1).limit(1))
            history_sequence_id = last_response[0]["history_sequence_id"] + 1 if last_response else 1
        except Exception as e:
            log_message(task_id, f"Error fetching AGENT_RESPONSES: {str(e)}")
            history_sequence_id = 1

        # Insert into BP_DATA
        try:
            bp_data_doc = {
                "artifi_id": artifi_id,
                "tx_id": tx_id,
                "case_id": case_id,
                "submission_data": submission_data,
                "history_sequence_id": history_sequence_id,
                "transaction_type": "Updated",
                "created_at": timestamp
            }
            db["BP_DATA"].insert_one(bp_data_doc)
        except Exception as e:
            log_message(task_id, f"Error inserting into BP_DATA: {str(e)}")

        # Save report data
        try:
            save_report_data(submission_data, artifi_id, tx_id)
        except Exception as e:
            log_message(task_id, f"Error saving report data: {str(e)}")

        # Insert into AGENT_RESPONSES
        try:
            agent_response_doc = {
                "artifi_id": artifi_id,
                "tx_id": tx_id,
                "case_id": case_id,
                "agent_response": agent_response,
                "history_sequence_id": history_sequence_id,
                "transaction_type": "Updated",
                "created_at": timestamp
            }
            db["AGENT_RESPONSES"].insert_one(agent_response_doc)
        except Exception as e:
            log_message(task_id, f"Error inserting into AGENT_RESPONSES: {str(e)}")

    except Exception as e:
        log_message(task_id, f"Unexpected error: {str(e)}")


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
worker_call_agent_service_rerun = Worker(
    task_definition_name="call_agent_service_rerun",
    execute_function=call_agent_service_rerun
)
worker_push_to_mongo_updated = Worker(
    task_definition_name="push_to_mongo_updated",
    execute_function=push_to_mongo_updated
)
worker_send_to_service_now_rerun = Worker(
    task_definition_name="send_to_service_now_rerun",
    execute_function=send_to_service_now_rerun_worker
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
        worker_cleanup_eml_file_worker,
        worker_call_agent_service_rerun,
        worker_push_to_mongo_updated,
        worker_send_to_service_now_rerun
    ],
)
handler.start_processes()