import psycopg2
from datetime import datetime
from db_config import DATABASE_CONFIG 

def get_connection():
    return psycopg2.connect(**DATABASE_CONFIG)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id character varying(50) PRIMARY KEY,
                status character varying(20) NOT NULL,
                job_details TEXT NULL
            );
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_details (
                id SERIAL,
                job_id character varying(50) REFERENCES jobs(job_id) ON DELETE CASCADE,
                identifier character varying(100) NOT NULL,
                timestamp timestamp without time zone,
                status character varying(20) NOT NULL,
                PRIMARY KEY (job_id, identifier) 
            );
        """)
        conn.commit()
        print("Database tables initialized/verified successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error during DB initialization: {e}")
    finally:
        cursor.close()
        conn.close()

def insert_or_update_job_batch(job_id, identifiers, job_details=None):
    """Inserts or updates the job header (with optional details) and all detail lines."""
    conn = get_connection()
    cursor = conn.cursor()
    current_time = datetime.now()
    
    try:
        cursor.execute(
            """
            INSERT INTO jobs (job_id, status, job_details)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE 
            SET status = EXCLUDED.status,
                job_details = COALESCE(EXCLUDED.job_details, jobs.job_details);
            """,
            (job_id, "IN_PROGRESS", job_details) 
        )
        
        for identifier in identifiers:
            cursor.execute(
                """
                INSERT INTO job_details (job_id, identifier, timestamp, status)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (job_id, identifier) DO UPDATE 
                SET timestamp = EXCLUDED.timestamp, 
                    status = EXCLUDED.status;
                """,
                (job_id, identifier, current_time, "IN_PROGRESS")
            )
        
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error processing job batch {job_id}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

# --- NEW FUNCTION (Replaces old logic) ---
def update_detail_status(job_id, identifier, new_status):
    """
    Updates a single identifier's status.
    This function NO LONGER checks for completion.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            """
            UPDATE job_details 
            SET status = %s 
            WHERE job_id = %s AND identifier = %s;
            """,
            (new_status, job_id, identifier)
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error updating detail status for {job_id}/{identifier}: {e}")
    finally:
        cursor.close()
        conn.close()

# --- NEW FUNCTION (The "Manager" Check) ---
def check_and_complete_job(job_id):
    """
    Checks if all details for a job are done (PROCESSED or FAILED).
    If so, marks the main job as COMPLETED.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Check for any items that are still 'IN_PROGRESS'
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM job_details 
            WHERE job_id = %s AND status = 'IN_PROGRESS';
            """,
            (job_id,)
        )
        
        remaining_count = cursor.fetchone()[0]
        
        if remaining_count == 0:
            # All items are either PROCESSED or FAILED, so the job is COMPLETED
            cursor.execute(
                """
                UPDATE jobs 
                SET status = %s 
                WHERE job_id = %s;
                """,
                ("COMPLETED", job_id)
            )
            print(f"Job {job_id} marked as COMPLETED.")
        else:
            print(f"Job {job_id} still has {remaining_count} items in progress.")

        conn.commit()
        return remaining_count == 0
    except Exception as e:
        conn.rollback()
        print(f"Error in check_and_complete_job for {job_id}: {e}")
        return False
    finally:
        cursor.close()
        conn.close()
