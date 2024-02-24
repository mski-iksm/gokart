import functools
from logging import getLogger
from typing import Callable

from gokart.conflict_prevention_lock.task_conflict_prevention_lock import _set_lock_scheduler, _set_redis_lock, make_redis_params_for_run
from gokart.task import TaskOnKart

logger = getLogger(__name__)


def wrap_run_with_lock(run_func: Callable[[], None], task_self: TaskOnKart):
    redis_params = make_redis_params_for_run(task_self=task_self)
    if not redis_params.should_redis_lock:
        return run_func

    @functools.wraps(run_func)
    def wrapped():
        redis_lock = _set_redis_lock(redis_params=redis_params)
        scheduler = _set_lock_scheduler(redis_lock=redis_lock, redis_params=redis_params)

        try:
            logger.debug(f'Task lock of {redis_params.redis_key} locked.')
            result = run_func()
            redis_lock.release()
            logger.debug(f'Task lock of {redis_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task lock of {redis_params.redis_key} released with BaseException.')
            redis_lock.release()
            scheduler.shutdown()
            raise e

    return wrapped
