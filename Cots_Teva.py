from flask import Flask, request, jsonify
import uuid
import time
import os
import pandas as pd
import asyncio
# --- IMPORT THE NEW FUNCTIONS ---
from db_handler import (
    init_db, 
    insert_or_update_job_batch, 
    update_detail_status,  # <-- NEW
    check_and_complete_job # <-- NEW
)

# --- Configuration ---
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)
# ------------------------------------
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# ---------------------------
# Simulate External Search (Async)
# ---------------------------
async def search_website(identifier):
    """Simulates the core processing logic that takes time (non-blocking)."""
    print(f"Searching website for identifier: {identifier}")
    await asyncio.sleep(0.1) 
    print(f"Search completed for identifier: {identifier}")

# ---------------------------
# Dummy Async Function
# ---------------------------
def write_dummy_file(file_name, content):
    """Synchronous helper function to write a file."""
    file_path = os.path.join(UPLOAD_FOLDER, file_name)
    with open(file_path, 'w') as f:
        f.write(content)

async def dummy_async_function(job_id):
    """Simulates another asynchronous function."""
    print(f"[Job {job_id}] Starting dummy async task (will write a log file)...")
    await asyncio.sleep(0.5)
    
    safe_job_id = job_id.replace('/', '_').replace('\\', '_')
    file_name = f"dummy_log_{safe_job_id}.txt"
    file_content = f"Log generated for job_id {job_id} at {time.ctime()}"
    
    try:
        await asyncio.to_thread(write_dummy_file, file_name, file_content)
        print(f"[Job {job_id}] Finished dummy async task (file '{file_name}' written).")
        return f"Dummy task for {job_id} completed, file '{file_name}' created."
    except Exception as e:
        print(f"[Job {job_id}] Dummy task FAILED: {e}")
        return f"Dummy task failed: {str(e)}"

# ---------------------------
# Core Processing Function (Async) - UPDATED
# ---------------------------
async def process_single_identifier(job_id, identifier):
    """
    Executes the external search (async) and updates the database (sync in thread).
    This function NO LONGER checks for job completion.
    """
    try:
        await search_website(identifier)
        
        # --- CHANGED ---
        # Call the simpler function that only updates
        await asyncio.to_thread(
            update_detail_status, # <-- USE NEW FUNCTION
            job_id, 
            identifier, 
            "PROCESSED"
        )
        # --- END CHANGE ---
        
        print(f"Identifier {identifier} for Job {job_id} is PROCESSED.")
        return {"identifier": identifier, "status": "processed"}
    
    except Exception as e:
        print(f"Identifier {identifier} for Job {job_id} FAILED: {e}")
        
        try:
            # --- CHANGED ---
            # Call the simpler function that only updates
            await asyncio.to_thread(
                update_detail_status, # <-- USE NEW FUNCTION
                job_id, 
                identifier, 
                "FAILED"
            )
            # --- END CHANGE ---
        except Exception as db_e:
            print(f"Identifier {identifier} for Job {job_id} FAILED, and DB update also failed: {db_e}")
        
        return {"identifier": identifier, "status": "failed", "reason": f"Processing error: {str(e)}"}
# ---------------------------

@app.route('/start-job', methods=['POST'])
async def start_job():
    
    # 1. Handle File Upload (No change)
    if 'key1' not in request.files:
        return jsonify({"error": "Excel/CSV file 'key1' is missing"}), 400
    data_file = request.files['key1']
    if data_file.filename == '':
        return jsonify({"error": "No selected file in 'key1'"}), 400
    
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

    # 2. Get Job ID and Details (No change)
    job_id = request.form.get('job_id') or str(uuid.uuid4())
    job_details = request.form.get('job_details')

    # 3. Insert/Update Job Batch in DB (No change)
    try:
        await asyncio.to_thread(
            insert_or_update_job_batch, 
            job_id, 
            identifiers, 
            job_details
        )
    except Exception as e:
         return jsonify({"error": f"Database failed to initialize the job batch: {str(e)}"}), 500

    # 4. Create tasks (No change)
    processing_tasks = []
    for identifier in identifiers:
        processing_tasks.append(
            process_single_identifier(job_id, identifier)
        )
        
    # 5. Create dummy task (No change)
    dummy_task = dummy_async_function(job_id)
    
    # 6. Run tasks (No change)
    all_tasks = [dummy_task] + processing_tasks
    results = await asyncio.gather(*all_tasks)
    
    dummy_result = results[0]
    processing_results = results[1:]

    # --- THIS IS THE NEW "MANAGER" STEP ---
    # 7. Final check for completion
    # After all tasks are gathered, run the final check ONCE.
    try:
        await asyncio.to_thread(check_and_complete_job, job_id)
    except Exception as e:
        print(f"Failed during final job completion check: {e}")
        # Note: The job response is still sent, even if this check fails
    # --- END OF NEW STEP ---

    # 8. Return Final Response (Renumbered from 7)
    return jsonify({
        "message": "Job batch initialized and processed successfully (async)",
        "job_id": job_id,
        "details_count": len(identifiers),
        "dummy_task_result": dummy_result,
        "details": processing_results
    })


if __name__ == "__main__":
    init_db() 
    print("Starting Flask server...")
    app.run(host="0.0.0.0", port=5000, debug=True)
