import json
import os
import random
import time
import uuid
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC = "click-events"
CATALOG = {
    "python tutorial": ["doc_py_1", "doc_py_2", "doc_py_3"],
    "java tutorial": ["doc_java_1", "doc_java_2", "doc_java_3"],
    "c tutorial": ["doc_c_1", "doc_c_2", "doc_c_3"]
}
EVENT_TYPES = ["views", "clicks", "purchases"]

def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    for i in range(200):
        catalog_choice = random.choice(list(CATALOG.keys()))
        result_choice = random.choice(CATALOG[catalog_choice])
        event_choice = random.choices(
            EVENT_TYPES,
            weights=[60, 30, 10],
            k=1
        )[0]
        user_id = str(uuid.uuid4())
        event = {
            "query": catalog_choice, 
            "result_id": result_choice,
            "event_type": event_choice,
            "user_id": user_id
        }
        producer.send(TOPIC, event)
        time.sleep(0.05)
    producer.flush()

if __name__ == "__main__": 
    run()