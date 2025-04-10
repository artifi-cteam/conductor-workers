import json
import time
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

CONDUCTOR_URL = os.getenv('CONDUCTOR_URL')
WORKFLOW_NAME = 'get_submission_analysis'

@app.route('/')
def home():
    return '''
    <h2>Register Workflow</h2>
    <form method="POST" action="/register-workflow">
      <textarea name="workflow_json" rows="20" cols="80" placeholder='Paste your workflow JSON here'></textarea><br>
      <input type="submit" value="Register Workflow">
    </form>
    <br>
    <h2>Start Workflow via File Upload</h2>
    <form method="POST" action="/start-workflow" enctype="multipart/form-data">
      <input type="file" name="file"><br>
      <input type="submit" value="Start Workflow">
    </form>
    '''

@app.route('/start-workflow', methods=['POST'])
def start_workflow():
    if 'file' not in request.files:
        return 'No file part', 400
    print("Starting workflow...")
    case_id = request.form['case_id']
    file = request.files['file']  # this should now work if sent as form-data
    file_content = file.read()
    filename = file.filename

    

    workflow_input = {
        "case_id": case_id,
        "filename": filename,
        "file": file_content.decode('utf-8')
    }
    payload = {
        "name": WORKFLOW_NAME,
        "version": 9,
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

@app.route('/register-workflow', methods=['POST'])
def register_workflow():
    workflow_json = request.form.get('workflow_json')
    try:
        workflow_data = json.loads(workflow_json)
    except Exception as e:
        return jsonify({"error": "Invalid JSON", "details": str(e)}), 400

    try:
        reg_response = requests.post(f"{CONDUCTOR_URL}/metadata/workflow", json=workflow_data)
        if reg_response.status_code != 200:
            return jsonify({
                "error": "Workflow registration failed",
                "details": reg_response.text
            }), 500
        return jsonify({
            "message": "Workflow registered successfully",
            "details": reg_response.text
        }), 200
    except Exception as e:
        return jsonify({"error": "Error registering workflow", "details": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=3000)
