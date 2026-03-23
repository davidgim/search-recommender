import os
import json
from dotenv import load_dotenv
import redis

load_dotenv()
host = os.environ.get("REDIS_HOST", "localhost")
port = os.environ.get("REDIS_PORT", "6379")

_client = redis.StrictRedis(host=host, port=port, decode_responses=True)

def _key(query):
    return f"rankings:{query}"

def set_rankings(query, value):
    _client.set(_key(query), json.dumps(value), ex=60)

def get_rankings(query):
    raw = _client.get(_key(query))
    if raw is None:
        return None
    return json.loads(raw)

def delete_rankings(query):
    _client.delete(_key(query))