# --- Logging Helper Function ---
from datetime import datetime, timezone
import os

import requests


def log_message(task_id, message):
    """Sends a log message to the Conductor API for a specific task."""
    if not task_id:
        print(f"WARNING: task_id not provided. Log message: {message}")
        return
    timestamp = datetime.now(timezone.utc).isoformat()
    log_entry = f"{timestamp} - {message}"
    try:
        # Use a timeout for the request
        response = requests.post(
            f"{os.getenv('CONDUCTOR_URL')}/tasks/{task_id}/log",
            data=log_entry.encode('utf-8'), # Send raw bytes
            headers={'Content-Type': 'text/plain; charset=utf-8'},
            timeout=5 # Timeout in seconds
        )
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
    except requests.exceptions.RequestException as log_err:
        # Log locally if pushing to Conductor fails
        print(f"ERROR: Failed to push log to Conductor for task {task_id}: {log_err}")
        print(f"Original Log: {log_entry}")
    except Exception as e:
        # Catch any other unexpected errors during logging
        print(f"ERROR: Unexpected error in log_message for task {task_id}: {e}")
        print(f"Original Log: {log_entry}")