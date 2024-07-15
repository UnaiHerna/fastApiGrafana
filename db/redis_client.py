from datetime import datetime

import redis
import json


class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.StrictRedis(host=host, port=port, db=db)

    def get_client(self):
        return self.client


# Inicializa el cliente de Redis
redis_client = RedisClient().get_client()


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


def get_cached_response(key):
    cached_data = redis_client.get(key)
    if cached_data:
        print(f"Data retrieved from Redis for key: {key}")  # Aviso en consola
        return json.loads(cached_data, object_hook=json_deserializer)
    return None


def set_cached_response(key, data, expiration=120):
    print(f"Data sent to Redis for key: {key}")  # Aviso en consola
    redis_client.setex(key, expiration, json.dumps(data, default=json_serializer))
