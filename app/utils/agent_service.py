from datetime import time
import json
import random
import requests
from app.utils.conductor_logger import log_message
from app.service.mongo_service import save_report_data, client

TARGET_AGENT_IDS = {
    "a9b0250c-0e6c-45a2-9214-0441af43b36a",  # LossInsight
    "cb8d305d7cf54bbbbf0490787079dbcb",  # ExposureInsight
    "48e0fde3-2c69-44f0-98d6-b6a5b031c2bb",  # EligibilityCheck
    "6097c379-9637-4198-abad-a9d5416fb650",  # InsuranceVerify
    "8c72ba1d-9403-4782-8f8c-12564ab73f9c",  # PropEval
    "383daaad-4b46-491b-b987-9dd17d430ca3"   # BusineesProfileSearch
}

import json

def craft_agent_config(agent_data):
    cfg = agent_data.get("Configuration", {})
    kb = agent_data.get("selectedKnowledgeBase")
    toggle = cfg.get("structured_output_toggle", False)
    raw = cfg.get("structured_output", "{}")

    if not toggle:
        structured = {}
    elif isinstance(raw, bool) and not raw:
        structured = None
    elif isinstance(raw, str):
        structured = json.loads(raw)
        structured = structured.get("structured_output", structured)
    else:
        structured = raw.get("structured_output", raw)

    agent_config = {
        "AgentID": agent_data.get("AgentID", ""),
        "AgentName": agent_data.get("AgentName", ""),
        "AgentDesc": agent_data.get("AgentDesc", ""),
        "CreatedOn": agent_data.get("CreatedOn", ""),
        "Configuration": {
            "name": cfg.get("name", ""),
            "function_description": cfg.get("function_description", ""),
            "system_message": cfg.get("system_message", ""),
            "tools": cfg.get("tools", []),
            "category": cfg.get("category", ""),
            "structured_output": structured,
            "knowledge_base": {
                "id": kb.get("id", ""),
                "name": kb.get("name", ""),
                "enabled": "yes",
                "collection_name": kb.get("collection_name", ""),
                "embedding_model": "BAAI/bge-small-en-v1.5",
                "description": kb.get("description", ""),
                "number_of_chunks": 5
            } if kb else {}
        },
        "isManagerAgent": agent_data.get("isManagerAgent", False),
        "selectedManagerAgents": agent_data.get("selectedManagerAgents", []),
        "managerAgentIntention": agent_data.get("managerAgentIntention", ""),
        "selectedKnowledgeBase": kb if kb else {},
        "knowledge_base": {
            "id": kb.get("id", ""),
            "name": kb.get("name", ""),
            "enabled": "yes",
            "collection_name": kb.get("collection_name", ""),
            "embedding_model": "BAAI/bge-small-en-v1.5",
            "description": kb.get("description", ""),
            "number_of_chunks": 5
        } if kb else {},
        "coreFeatures": agent_data.get("coreFeatures", {}),
        "llmProvider": agent_data.get("llmProvider", ""),
        "llmModel": agent_data.get("llmModel", "")
    }

    return agent_config

def call_agent_service_rerun(task):
    task_id    = task.task_id
    input_data = task.input_data
    log_message(task_id, "Rerun: pull + call agents")

    # 1) pull from Mongo
    case_id       = input_data.get("case_id")
    modified_data = input_data.get("modified_data", {})
    db = client["Submission_Intake"]
    collection = db["BP_DATA"]
    mongo_doc     = collection.find_one({"case_id": case_id}) or {}
    submission_data = mongo_doc.get("submission_data", {})

    # 2) merge
    merged_data = {**submission_data, **modified_data}
    thread_id = input_data.get("thread_id", random.randint(1, 100000))

    # 3) call each agent
    results = {}
    db = client["Agent_Database"]
    collection = db["AgentCatalog"]
    agents = collection.find({"AgentID": {"$in": list(TARGET_AGENT_IDS)}})
    for agent in agents:
        name   = agent.get("AgentName", agent["AgentID"])
        config = craft_agent_config(agent)
        try:
            log_message(task_id, f"Calling the agent {name}")
            resp = requests.post(
                "http://34.224.79.136:8000/query",
                json={"agent_config": config, "message": str(merged_data), "thread_id": thread_id},
                timeout=300
            )
            log_message(task_id,f"agent config: {config} \nresponse: {resp.json()}")
            results[name] = resp.json()
        except Exception as e:
            results[name] = {"error": str(e)}

    return {
        "status": "COMPLETED",
        "outputData": {
            "agent_output":    results,
            "submission_data": merged_data
        }
    }


def call_agent_service(task):
    task_id = task.task_id
    try:
        input_data = task.input_data
        submission_data = input_data.get("submission_data", {})
        thread_id = input_data.get("thread_id", random.randint(1, 100000))

        
        db = client["Agent_Database"]
        collection = db["AgentCatalog"]

        agents = collection.find({"AgentID": {"$in": list(TARGET_AGENT_IDS)}})

        results = {}

        for agent in agents:
            agent_id = agent.get("AgentID")
            agent_name = agent.get("AgentName", agent_id)  # fallback to ID if name missing
            agent_config = craft_agent_config(agent)
            log_message(task_id,f"agent config is: {agent_config}")

            try:
                response = requests.post(
                    "http://34.224.79.136:8000/query",
                    json={
                        "agent_config": agent_config,
                        "message": str(submission_data),
                        "thread_id": thread_id
                    },
                    timeout=300
                )
                results[agent_name] = response.json()
            except Exception as e:
                results[agent_name] = {"error": str(e)}

        return results

    except Exception as e:
        log_message(task_id, f"Error in call_agent_service: {e}")
        raise

