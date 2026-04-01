import json
import os
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from kafka import KafkaProducer
from pydantic import BaseModel
from cache.redis_client import get_rankings
from db.postgres_client import get_global_rankings, get_user_rankings
from prometheus_fastapi_instrumentator import Instrumentator

load_dotenv()
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
TOPIC = "click-events"

class ClickEvent(BaseModel):
    query: str
    result_id: str
    user_id: str
    event_type: str

@asynccontextmanager
async def lifespan(app):
    global _producer
    _producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    yield
    _producer.flush()
    _producer.close()

app = FastAPI(
    title="Search Recommender API",
    lifespan=lifespan
)

Instrumentator().instrument(app).expose(app)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/event", status_code=202)
def ingest_event(event: ClickEvent):
    _producer.send(TOPIC, event.model_dump())
    return {"status": "accepted", "event": event.model_dump()}

@app.get("/recommend")
def recommend(
    query: str = Query(..., description="Search query"), 
    user_id: str = Query(None, description="Optional user ID for personalization"),
    top_n: int = Query(10, ge=1, le=50)
):
    global_rankings = get_rankings(query=query)
    if not global_rankings:
        global_rankings = get_global_rankings(query=query, top_n=top_n)
    if not global_rankings:
        return {"query": query, "results": [], "hint": "No data yet. Run the producer."}
    global_scores = {item["result_id"]: item["score"] for item in global_rankings}
    res_scores = global_scores
    if user_id:
        user_rankings = get_user_rankings(query=query, user_id=user_id, top_n=top_n)
        if user_rankings:
            personal_scores = {item["result_id"]: item["score"] for item in user_rankings}
            for result_id, g_score in global_scores.items():
                p_score = personal_scores.get(result_id, 0)
                blended = (g_score * 0.7) + (p_score * 0.3)
                res_scores[result_id] = blended
    results = sorted(
        [{"result_id": rid, "score": score} for rid, score in res_scores.items()],
        key=lambda x: x["score"],
        reverse=True
    )
    return {"query": query, "user_id": user_id, "results": results[:top_n]}
            





