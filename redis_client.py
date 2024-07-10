from datetime import datetime

import redis


class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.StrictRedis(host=host, port=port, db=db)

    def get_client(self):
        return self.client


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