import functools
from logging import getLogger

import redis
from apscheduler.schedulers.background import BackgroundScheduler

logger = getLogger(__name__)


class TaskLockException(Exception):
    pass


class RedisClient:
    _instances: dict = {}

    def __new__(cls, *args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        if cls not in cls._instances:
            cls._instances[cls] = {}
        if key not in cls._instances[cls]:
            cls._instances[cls][key] = super(RedisClient, cls).__new__(cls)
        return cls._instances[cls][key]

    def __init__(self, host: str, port: int) -> None:
        if not hasattr(self, '_redis_client'):
            self._redis_client = redis.Redis(host=host, port=port)

    def get_redis_client(self):
        return self._redis_client


REDIS_TIMEOUT = 180
LOCK_EXTEND_SECONDS = 10


def get_task_lock(redis_host: str, redis_port: int, lock_key: str) -> redis.lock.Lock:
    redis_client = RedisClient(host=redis_host, port=redis_port).get_redis_client()
    task_lock = redis.lock.Lock(redis=redis_client, name=lock_key, timeout=REDIS_TIMEOUT, thread_local=False)
    if not task_lock.acquire(blocking=False):
        # If lock is already taken by other task, raise TaskLockException immediately.
        raise TaskLockException('Lock already taken by other task.')
    return task_lock


def _extend_lock(task_lock: redis.lock.Lock, redis_timeout: int) -> None:
    task_lock.extend(additional_time=redis_timeout, replace_ttl=True)


def set_lock_scheduler(task_lock: redis.lock.Lock) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    extend_lock = functools.partial(_extend_lock, task_lock=task_lock, redis_timeout=REDIS_TIMEOUT)
    scheduler.add_job(
        extend_lock,
        'interval',
        seconds=LOCK_EXTEND_SECONDS,
        max_instances=999999999,
        misfire_grace_time=REDIS_TIMEOUT,
        coalesce=False,
    )
    scheduler.start()
    return scheduler


def build_lock_key(task) -> str:
    return task.output().path()
