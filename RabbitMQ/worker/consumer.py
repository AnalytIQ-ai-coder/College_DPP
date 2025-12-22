import json
import pika
import requests
import numpy as np
import cv2
from ultralytics import YOLO

model = YOLO("yolov8n.pt")

PERSON_CLASS_ID = 0
def detect_people(image: np.ndarray) -> int:
    results = model(image, verbose=False)[0]
    if results.boxes is None:
        return 0

    classes = results.boxes.cls.cpu().numpy()
    return int((classes == PERSON_CLASS_ID).sum())

def callback(ch, method, properties, body):
    data = json.loads(body)
    image_url = data["image_url"]

    print(f"[x] Downloading image: {image_url}")

    resp = requests.get(
        image_url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=15
    )
    resp.raise_for_status()

    img_array = np.asarray(bytearray(resp.content), dtype=np.uint8)
    image = cv2.imdecode(img_array, cv2.IMREAD_COLOR)

    if image is None:
        print("[!] Failed to decode image")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    count = detect_people(image)
    print(f"[✓] Detected people: {count}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost")
)
channel = connection.channel()
channel.queue_declare(queue="image_jobs", durable=True)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue="image_jobs", on_message_callback=callback)

print("[*] Waiting for jobs...")
channel.start_consuming()
