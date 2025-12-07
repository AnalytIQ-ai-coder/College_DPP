import sqlite3
import time
from datetime import datetime

DB_FILE = "queue.db"

def init_db():
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT
            )
        """)
        conn.commit()

def fetch_pending_job():

    conn = sqlite3.connect(DB_FILE)
    conn.isolation_level = "EXCLUSIVE"

    try:
        conn.execute("BEGIN EXCLUSIVE")

        row = conn.execute("""
            SELECT id, status, created_at, updated_at
            FROM jobs
            WHERE status = 'pending'
            ORDER BY id
            LIMIT 1
        """).fetchone()

        if not row:
            conn.rollback()
            conn.close()
            return None

        job_id = row[0]

        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            ("in_progress", datetime.now().isoformat(), job_id)
        )

        conn.commit()
        conn.close()

        return {"id": job_id}

    except Exception:
        conn.rollback()
        conn.close()
        raise

def consume_job(job):
    print(f"Processing job #{job['id']}...")
    time.sleep(30)  # symulacja pracy
    print(f"Job #{job['id']} done.")

def mark_done(job_id):
    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?",
            ("done", datetime.now().isoformat(), job_id)
        )
        conn.commit()

def worker_loop():
    init_db()

    while True:
        try:
            job = fetch_pending_job()

            if not job:
                print("No pending jobs. Sleeping...")
                time.sleep(5)
                continue

            consume_job(job)
            mark_done(job["id"])

        except Exception as e:
            print("Error in consumer:", e)
            time.sleep(5)

if __name__ == "__main__":
    worker_loop()
