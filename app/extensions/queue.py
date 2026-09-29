import os

from redis import Redis
from rq import Queue


def get_redis_connection():
    return Redis.from_url(os.environ['REDIS_URL'])


def get_import_queue():
    return Queue('imports', connection=get_redis_connection())
