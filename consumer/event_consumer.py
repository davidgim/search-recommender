import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from collections import defaultdict
from db.postgres_client import upsert_ranking, init_db, get_global_rankings
from cache.redis_client import set_rankings

load_dotenv()
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC = "click-events"
GROUP_ID = "search-recommender-group"
FLUSH_EVERY = 10

def run():
    init_db()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        group_id=GROUP_ID
    )

    event_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    message_count = 0

    print("Consumer ready, waiting for messages...")
    for message in consumer:
        print(f"Received: {message.value}")
        event = message.value
        query = event["query"]
        result_id = event["result_id"]
        event_type = event["event_type"]
        user_id = event["user_id"]
        event_counts[query][(result_id, user_id)][event_type] += 1
        message_count += 1
        if message_count % FLUSH_EVERY == 0:
            _flush(event_counts)

def _flush(event_counts):
    print(f"Flushing {sum(len(v) for v in event_counts.values())} entries to Redis and Postgres...")
    for query in event_counts:
        for (result_id, user_id) in event_counts[query]:
            counts = event_counts[query][(result_id, user_id)]
            try:
                upsert_ranking(
                    query=query,
                    result_id=result_id,
                    clicks=counts["clicks"],
                    views=counts["views"],
                    purchases=counts["purchases"],
                    user_id=user_id
                )
                print(f"Upserted: {query} / {result_id}")
            except Exception as e:
                print(f"ERROR upserting: {e}")
        rankings = get_global_rankings(query=query)
        set_rankings(query, rankings)

    event_counts.clear()

if __name__ == "__main__":
    run()