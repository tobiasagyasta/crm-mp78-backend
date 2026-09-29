import os

from redis import Redis
from rq import Queue, Worker


redis_connection = Redis.from_url(os.environ['REDIS_URL'])
worker = Worker(
    [Queue('imports', connection=redis_connection)],
    connection=redis_connection,
)
worker.work()
