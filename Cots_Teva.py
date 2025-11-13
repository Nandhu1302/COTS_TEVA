from flask import Flask, request, jsonify
import uuid
import time
import os
import pandas as pd
from db_handler import init_db, insert_or_update_job_batch, update_detail_status_and_check_completion

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
# ------------------------------------
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# (Other functions: search_website, process_single_identifier... no changes needed)
# ---------------------------
# Simulate External Search (Simplified)
# ---------------------------
def search_website(identifier):
    """Simulates the core processing logic that takes time."""
    print(f"Searching website for identifier: {identifier}")
    time.sleep(0.1) 
    print(f"Search completed for identifier: {identifier}")

# ---------------------------
# Core Processing Function
# ---------------------------
def process_single_identifier(job_id, identifier):
    """
    Executes the external search and updates the database status.
    """
    # 1. Simulate the work
    search_website(identifier)
    
    # 2. Update status in job_details and check main job completion
    update_detail_status_and_check_completion(job_id, identifier, "PROCESSED")
    
    print(f"Identifier {identifier} for Job {job_id} is PROCESSED.")
# ---------------------------

@app.route('/start-job', methods=['POST'])
def start_job():
    
    # 1. Handle File Upload (key1)
    if 'key1' not in request.files:
        return jsonify({"error": "Excel/CSV file 'key1' is missing"}), 400
    data_file = request.files['key1']
    if data_file.filename == '':
        return jsonify({"error": "No selected file in 'key1'"}), 400
    
    # Read data (no changes)
    try:
        filename = data_file.filename.lower()
        if filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(data_file)
        elif filename.endswith('.csv'):
            df = pd.read_csv(data_file)
        else:
            return jsonify({"error": "Unsupported file type. Use Excel (.xlsx, .xls) or CSV."}), 400

        if 'identifier' not in df.columns:
            return jsonify({"error": "File must contain an 'identifier' column"}), 400
        
        identifiers = [str(i) for i in df['identifier'].tolist() if pd.notna(i)]
        if not identifiers:
             return jsonify({"error": "No valid identifiers found in the file."}), 400
             
    except Exception as e:
        return jsonify({"error": f"Failed to read or process file: {str(e)}"}), 500

    # 2. Get Job ID and Optional Details
    job_id = request.form.get('job_id') or str(uuid.uuid4())
    job_details = request.form.get('job_details') # Get from form

    # --- THIS IS THE KEY DEBUG STEP ---
    # This will print to your terminal (where you ran `python app.py`)
    print("***********************************")
    print(f"RECEIVED job_id: {job_id}")
    print(f"RECEIVED job_details: {job_details}")
    print(f"Type of job_details: {type(job_details)}")
    print("***********************************")
    # --- END OF DEBUG STEP ---
    # 3. Insert/Update Job Batch in DB
    if not insert_or_update_job_batch(job_id, identifiers, job_details):
        return jsonify({"error": "Database failed to initialize the job batch."}), 500

    # 4. Process Jobs
    results = []
    for identifier in identifiers:
        try:
            process_single_identifier(job_id, identifier)
            results.append({"identifier": identifier, "status": "processed"})
        except Exception as e:
            results.append({"identifier": identifier, "status": "failed", "reason": f"Processing error: {str(e)}"})

    # 5. Return Final Response
    return jsonify({
        "message": "Job batch initialized and processed successfully",
        "job_id": job_id,
        "details_count": len(identifiers),
        "details": results
    })


if __name__ == "__main__":
    init_db() 
    print("Starting Flask server...")
    # Make sure debug=True is on so it reloads changes
    app.run(host="0.0.0.0", port=5000, debug=True) 
