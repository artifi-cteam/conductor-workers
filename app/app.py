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

        return jsonify({
            "message": "Workflow has started. Please wait for the response.",
            "Workflow ID": start_response.text
        }), 200
        
    
    except Exception as e:
        return jsonify({"error": f"Error triggering/tracking workflow: {str(e)}"}), 500
    
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3000)
