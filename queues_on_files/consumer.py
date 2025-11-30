import os
import time
import msvcrt
from datetime import datetime

QUEUE_FILE = "queue.csv"
LOCK_LEN = 0x7fffffff

def lock_file(f):
    f.seek(0)
    while True:
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, LOCK_LEN)
            return
        except PermissionError:
            time.sleep(0.05)
def unlock_file(f):
    f.seek(0)
    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, LOCK_LEN)
def read_queue_with_lock():
    if not os.path.exists(QUEUE_FILE):
        return None, None, []

    f = open(QUEUE_FILE, "r+b")
    lock_file(f)

    f.seek(0)
    content = f.read().decode("utf-8")

    if not content.strip():
        header = ["id", "status", "created_at", "updated_at"]
        jobs = []
        return f, header, jobs

    lines = content.splitlines()
    rows = [line.split(",") for line in lines]

    header = rows[0]
    jobs = []
    for row in rows[1:]:
        row += [""] * (len(header) - len(row))
        jobs.append(dict(zip(header, row)))

    return f, header, jobs

def write_queue_with_lock(f, header, jobs):
    csv_lines = []
    csv_lines.append(",".join(header))
    for job in jobs:
        row = [
            str(job.get("id", "")),
            job.get("status", ""),
            job.get("created_at", ""),
            job.get("updated_at", ""),
        ]
        csv_lines.append(",".join(row))

    data = ("\n".join(csv_lines) + "\n").encode("utf-8")

    f.seek(0)
    f.truncate()
    f.write(data)
    f.flush()
    os.fsync(f.fileno())

def find_pending_job(jobs):
    for job in jobs:
        if job.get("status") == "pending":
            return job
    return None

def consume_job(job):
    print(f"Processing job #{job['id']}...")
    time.sleep(30)
    print(f"Job #{job['id']} done.")

def worker_loop():
    while True:
        try:
            f, header, jobs = read_queue_with_lock()

            if f is None:
                print("Queue file does not exist yet. Sleeping...")
                time.sleep(5)
                continue

            job = find_pending_job(jobs)

            if job is None:
                unlock_file(f)
                f.close()
                print("No pending jobs. Sleeping...")
                time.sleep(5)
                continue

            job["status"] = "in_progress"
            job["updated_at"] = datetime.now().isoformat()

            write_queue_with_lock(f, header, jobs)

            unlock_file(f)
            f.close()

            consume_job(job)

            f2, header2, jobs2 = read_queue_with_lock()
            if f2 is None:
                print("Queue file disappeared while processing. Skipping done update.")
                continue

            for j in jobs2:
                if j.get("id") == job["id"]:
                    j["status"] = "done"
                    j["updated_at"] = datetime.now().isoformat()

            write_queue_with_lock(f2, header2, jobs2)
            unlock_file(f2)
            f2.close()

        except Exception as e:
            print("Error in consumer:", e)
            time.sleep(5)

if __name__ == "__main__":
    worker_loop()
