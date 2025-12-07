import sqlite3
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

def add_job():
    init_db()
    now = datetime.now().isoformat()

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute(
            "INSERT INTO jobs (status, created_at) VALUES (?, ?)",
            ("pending", now)
        )
        job_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    print(f"Added job #{job_id} (pending)")

if __name__ == "__main__":
    add_job()
