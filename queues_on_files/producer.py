import csv
import os
from datetime import datetime
import msvcrt

QUEUE_FILE = "queue.csv"
LOCK_LEN = 0x7fffffff

def lock_file(f):
    f.seek(0)
    msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, LOCK_LEN)

def unlock_file(f):
    f.seek(0)
    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, LOCK_LEN)

def add_job():
    file_exists = os.path.exists(QUEUE_FILE)

    mode = "r+b" if file_exists else "w+b"

    with open(QUEUE_FILE, mode) as f:
        lock_file(f)

        f.seek(0)
        content = f.read().decode("utf-8") if file_exists else ""
        rows = [r.split(",") for r in content.splitlines()] if content else []

        if rows:
            header = rows[0]
            data_rows = rows[1:]
        else:
            header = ["id", "status", "created_at", "updated_at"]
            data_rows = []

        new_id = len(data_rows) + 1

        data_rows.append([
            str(new_id),
            "pending",
            datetime.now().isoformat(),
            ""
        ])

        csv_string = ",".join(header) + "\n"
        for row in data_rows:
            csv_string += ",".join(row) + "\n"

        f.seek(0)
        f.truncate()
        f.write(csv_string.encode("utf-8"))
        f.flush()
        os.fsync(f.fileno())

        unlock_file(f)

    print(f"Added job #{new_id} (pending)")

if __name__ == "__main__":
    add_job()
