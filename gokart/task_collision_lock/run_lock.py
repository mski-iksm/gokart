from logging import getLogger

from gokart.task_collision_lock.task_lock import build_lock_key, get_task_lock, set_lock_scheduler

logger = getLogger(__name__)


def run_with_lock(task, redis_host: str, redis_port: int):
    lock_key = build_lock_key(task=task)
    task_lock = get_task_lock(redis_host=redis_host, redis_port=redis_port, lock_key=lock_key)
    scheduler = set_lock_scheduler(task_lock=task_lock)

    try:
        logger.info(f'Task lock of {lock_key} locked.')
        result = task.run()
        task_lock.release()
        logger.info(f'Task lock of {lock_key} released.')
        scheduler.shutdown()
        return result
    except BaseException as e:
        logger.info(f'Task lock of {lock_key} released with BaseException.')
        task_lock.release()
        scheduler.shutdown()
        raise e
