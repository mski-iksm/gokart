from logging import getLogger

from gokart.task_collision_lock.task_lock import build_lock_key, get_task_lock
from gokart.utils import flatten

logger = getLogger(__name__)


def check_dependency_lock(task, redis_host: str, redis_port: int) -> None:
    required_tasks = flatten(task.requires())
    for required_task in required_tasks:
        _check_lock(task=required_task, redis_host=redis_host, redis_port=redis_port)


def _check_lock(task, redis_host: str, redis_port: int) -> None:
    lock_key = build_lock_key(task=task)
    task_lock = get_task_lock(redis_host=redis_host, redis_port=redis_port, lock_key=lock_key)
    logger.info(f'Task lock of {lock_key} locked.')
    task_lock.release()
    logger.info(f'Task lock of {lock_key} released.')
