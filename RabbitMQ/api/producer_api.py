import json
import pika
from fastapi import FastAPI
from pydantic import BaseModel
app = FastAPI()

class ImageRequest(BaseModel):
    image_url: str

def get_channel():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost")
    )
    channel = connection.channel()
    channel.queue_declare(queue="image_jobs", durable=True)
    return channel

@app.post("/analyze_img")
def analyze_image(req: ImageRequest):
    channel = get_channel()

    channel.basic_publish(
        exchange="",
        routing_key="image_jobs",
        body=json.dumps({"image_url": req.image_url}),
        properties=pika.BasicProperties(delivery_mode=2),
    )

    return {"status": "queued"}
