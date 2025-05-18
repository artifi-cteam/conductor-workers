import copy
from datetime import time
import json
import random
import markdown
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
AGENT_PROMPTS = {
    "LossInsight":       "Please provide loss insights for the data.",
    "ExposureInsight":   "Please provide exposure insights for the data.",
    "EligibilityCheck":  "Please check eligibility based on the data.",
    "InsuranceVerify":   "Please verify insurance details in the data.",
    "PropEval":          "Please provide Property evaluation insights for the data.",
    "BusineesProfileSearch": "Please search the business profile based on the data.",
}

#change the default score to 100
def deep_update(original: dict, updates: dict) -> dict:
    """
    Recursively walk `original`. Whenever you encounter a key that's in `updates`:
      - if original[k] is a dict with a 'value' key, replace original[k]['value'] and update 'score' if present
      - otherwise replace original[k] outright.
    Returns the mutated `original`.
    """
    for k, v in list(original.items()):
        # If this key needs updating, apply update logic
        if k in updates:
            new_val = updates[k]
            if isinstance(original[k], dict) and 'value' in original[k]:
                original[k]['value'] = new_val
                # Update score if it exists, otherwise set a default score
                if 'score' in original[k]:
                    original[k]['score'] = "100"  # Default score, can be customized if needed
            else:
                original[k] = new_val

        # If the value is itself a dict, recurse into it
        if isinstance(original[k], dict):
            deep_update(original[k], updates)

    return original

# def deep_update(original: dict, updates: dict) -> dict:
#     """
#     Recursively walk `original`.  Whenever you encounter a key that's in `updates`:
#       - if original[k] is a dict with a 'value' key, replace original[k]['value']
#       - otherwise replace original[k] outright.
#     Returns the mutated `original`.
#     """
#     for k, v in list(original.items()):
#         # If this key needs updating, apply update logic
#         if k in updates:
#             new_val = updates[k]
#             if isinstance(original[k], dict) and 'value' in original[k]:
#                 original[k]['value'] = new_val
#             else:
#                 original[k] = new_val

#         # If the value is itself a dict, recurse into it
#         if isinstance(original[k], dict):
#             deep_update(original[k], updates)

#     return original

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
    task_id       = task.task_id
    input_data    = task.input_data or {}
    log_message(task_id, "Rerun: pull + call agents")

    # 1) pull from Mongo
    case_id       = input_data.get("case_id")
    modified_data = input_data.get("modified_data", {})
    mongo_doc     = client["Submission_Intake"]["BP_DATA"].find_one({"case_id": case_id}) or {}
    submission    = mongo_doc.get("submission_data", {})

    # 2) merge
    merged_data = deep_update(copy.deepcopy(submission), modified_data)
    thread_id   = input_data.get("thread_id", random.randint(1,100000))

    # helper: safely convert markdown→HTML
    def md2html(s: str) -> str:
        return markdown.markdown(s, extensions=['extra'])

    # process each top‑level field in an agent’s response
    def convert_section(val):
        # if the entire section is a plain string → convert it
        if isinstance(val, str):
            return md2html(val)

        # if it’s a dict:
        if isinstance(val, dict):
            
            if isinstance(val, dict) and "result" in val:
                # convert resp["result"]
                val["result"] = md2html(val["result"])
            elif isinstance(val, str):
                # convert the whole response string
                val["response"] = md2html(val)
            # leave metadata or other keys untouched
            return val

        # any other type → leave
        return val

    # 3) call agents
    results = {}
    agents = client["Agent_Database"]["AgentCatalog"].find({
        "AgentID": {"$in": list(TARGET_AGENT_IDS)}
    })

    for agent in agents:
        name   = agent.get("AgentName", agent["AgentID"])
        agent_id   = agent.get("AgentID")
        config = craft_agent_config(agent)
        suffix = AGENT_PROMPTS.get(name, "")

        if agent_id == "6097c379-9637-4198-abad-a9d5416fb650": # DataAnalysis (InsuranceVerify)
            # Send common
            sub_data = submission.get("Common", "") 

        elif agent_id == "a9b0250c-0e6c-45a2-9214-0441af43b36a": # LossInsights
            # Only send Common + Loss Run
            sub_data = {
                "Common": submission.get("Common",""),
                "Loss Runs": submission.get("Loss Runs","")
            }

        elif agent_id == "8c72ba1d-9403-4782-8f8c-12564ab73f9c": # PropEval (Analytics 2)
            # Only send Common + Property + Advanced Property
            sub_data = {
                "Common": submission.get("Common"),
                "Advanced Property": submission.get("Advanced Property")
            }

        elif agent_id == "cb8d305d7cf54bbbbf0490787079dbcb": # ExposureInsights
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Runs", None)

        elif agent_id == "48e0fde3-2c69-44f0-98d6-b6a5b031c2bb": # Appetite and Eligibility
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Runs", None)

        elif agent_id == "383daaad-4b46-491b-b987-9dd17d430ca3": # Business Operations (Analytics 1)
            # Only send Common within Submission Data
            sub_data = {
                "Common": submission.get("Common")
            }

        else:
            # Default case, send the entire submission
            sub_data = submission

        full_message = f"{sub_data} {suffix}".strip()

        try:
            log_message(task_id, f"Calling {name}")
            raw = requests.post(
                "http://34.224.79.136:8000/query",
                json={"agent_config": config, "message": full_message, "thread_id": thread_id},
                timeout=300
            ).json()

            # convert every top‑level value
            for k, v in raw.items():
                raw[k] = convert_section(v)

            results[name] = raw

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
    task_id    = task.task_id
    input_data = task.input_data or {}
    submission = input_data.get("submission_data", {})
    thread_id  = input_data.get("thread_id", random.randint(1,100000))

    agents = client["Agent_Database"]["AgentCatalog"].find({
        "AgentID": {"$in": list(TARGET_AGENT_IDS)}
    })

    # helper: safely convert markdown→HTML
    def md2html(s: str) -> str:
        return markdown.markdown(s, extensions=['extra'])

    # convert top-level sections like in the rerun function
    def convert_section(val):
        if isinstance(val, str):
            return md2html(val)
        if isinstance(val, dict):
            if "result" in val:
                val["result"] = md2html(val["result"])
            elif isinstance(val, str):
                val["response"] = md2html(val)
            return val
        return val

    results = {}
    for agent in agents:
        agent_id   = agent.get("AgentID")
        agent_name = agent.get("AgentName", agent["AgentID"])
        agent_cfg  = craft_agent_config(agent)
        suffix     = AGENT_PROMPTS.get(agent_name, "")
        # if agent_id == "6097c379-9637-4198-abad-a9d5416fb650":# InsuranceVerify
        #     sub_data = submission.get("Common", "")

        # elif agent_id == "383daaad-4b46-491b-b987-9dd17d430ca3":#LossInsight
        #     sub_data= submission.get("Loss Runs", "")

        if agent_id == "6097c379-9637-4198-abad-a9d5416fb650": # DataAnalysis (InsuranceVerify)
            # Send common
            sub_data = submission.get("Common", "") 

        elif agent_id == "a9b0250c-0e6c-45a2-9214-0441af43b36a": # LossInsights
            # Only send Common + Loss Run
            sub_data = {
                "Common": submission.get("Common",""),
                "Loss Runs": submission.get("Loss Runs","")
            }

        elif agent_id == "8c72ba1d-9403-4782-8f8c-12564ab73f9c": # PropEval (Analytics 2)
            # Only send Common + Property + Advanced Property
            sub_data = {
                "Common": submission.get("Common"),
                "Advanced Property": submission.get("Advanced Property")
            }

        elif agent_id == "cb8d305d7cf54bbbbf0490787079dbcb": # ExposureInsights
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Runs", None)

        elif agent_id == "48e0fde3-2c69-44f0-98d6-b6a5b031c2bb": # Appetite and Eligibility
            # Only send submission data without LossRun
            sub_data = submission.copy()
            sub_data.pop("Loss Runs", None)

        elif agent_id == "383daaad-4b46-491b-b987-9dd17d430ca3": # Business Operations (Analytics 1)
            # Only send Common within Submission Data
            sub_data = {
                "Common": submission.get("Common")
            }

        else:
            # Default case, send the entire submission
            sub_data = submission

        full_message = f"{sub_data} {suffix}".strip()
        log_message(task_id, f"sending to {agent_name!r}")

        try:
            r = requests.post(
                "http://34.224.79.136:8000/query",
                json={
                    "agent_config": agent_cfg,
                    "message":      full_message,
                    "thread_id":    thread_id
                },
                timeout=300
            )
            raw = r.json()

            # apply markdown→HTML conversion to each top-level field
            for k, v in raw.items():
                raw[k] = convert_section(v)

            results[agent_name] = raw

        except Exception as e:
            results[agent_name] = {"error": str(e)}

    return results