import base64
import time
from flask import Flask, request, jsonify
import requests
from conductor.client.workflow_client import WorkflowClient
from conductor.client.configuration.configuration import Configuration

# Configuration setup
config = Configuration(
    base_url="http://localhost:8080/api",  
    debug=True  
)

app = Flask(__name__)

CONDUCTOR_URL = 'http://localhost:8080/api'

WORKFLOW_NAME = 'get_submission_analysis'

@app.route('/start-workflow', methods=['POST'])
def start_workflow():
    file = request.files['file']
    file_content = file.read()
    filename = file.filename
    
    workflow_input = {
        "file": file_content.decode('utf-8'),  
        "filename": filename
    }
    payload = {
        "name": WORKFLOW_NAME,
        "version": 6,  
        "input": workflow_input
    }
    
    try:
        start_response = requests.post(f"{CONDUCTOR_URL}/workflow", json=payload)
        
        if start_response.status_code != 200:
            return jsonify({
                "error": "Failed to trigger workflow", 
                "details": start_response.json()
            }), 500
        
        workflow_id = start_response.json()['workflowId']
        
        max_attempts = 60  # 10 minutes with 10-second intervals
        for attempt in range(max_attempts):
            # Retrieve workflow status
            status_url = f"{CONDUCTOR_URL}/workflow/{workflow_id}"
            status_response = requests.get(status_url)
            
            if status_response.status_code != 200:
                return jsonify({
                    "error": "Failed to retrieve workflow status", 
                    "details": status_response.json()
                }), 500
            
            workflow_status = status_response.json()
            
            if workflow_status['status'] in ['COMPLETED', 'FAILED']:
                # Retrieve final output
                result_url = f"{CONDUCTOR_URL}/workflow/{workflow_id}/output"
                result_response = requests.get(result_url)
                
                if result_response.status_code == 200:
                    final_result = result_response.json()
                    
                    return jsonify({
                        "workflow_id": workflow_id,
                        "status": workflow_status['status'],
                        "result": final_result
                    }), 200
                else:
                    return jsonify({
                        "error": "Failed to retrieve workflow result", 
                        "workflow_id": workflow_id,
                        "status": workflow_status['status']
                    }), 500
            
            # Wait before next poll
            time.sleep(10)
        
        # Timeout reached
        return jsonify({
            "error": "Workflow did not complete in expected time", 
            "workflow_id": workflow_id
        }), 408
    
    except Exception as e:
        return jsonify({"error": f"Error triggering/tracking workflow: {str(e)}"}), 500
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3000)
