from datetime import datetime
import json
import redis.asyncio as redis
from typing import Any, Optional


class AsyncRedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.client = None

    async def initialize(self):
        self.client = redis.from_url(f"redis://{self.host}:{self.port}/{self.db}", decode_responses=True)

    def get_client(self):
        return self.client


# Inicializa el cliente de Redis
redis_client = AsyncRedisClient()


async def get_redis_client():
    if redis_client.get_client() is None:
        await redis_client.initialize()
    return redis_client.get_client()


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def json_deserializer(dct):
    for key, value in dct.items():
        if isinstance(value, str) and len(value) == 19:
            try:
                dct[key] = datetime.fromisoformat(value)
            except ValueError:
                pass
    return dct


async def get_cached_response(key: str) -> Optional[Any]:
    client = await get_redis_client()
    cached_data = await client.get(key)
    if cached_data:
        print(f"Data retrieved from Redis for key: {key}")  # Aviso en consola
        return json.loads(cached_data, object_hook=json_deserializer)
    return None


async def set_cached_response(key: str, data: Any, expiration: int = 120):
    client = await get_redis_client()
    print(f"Data sent to Redis for key: {key}")  # Aviso en consola
    await client.setex(key, expiration, json.dumps(data, default=json_serializer))
