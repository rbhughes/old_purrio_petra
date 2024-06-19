import functools
import logging

# import os
from typing import Callable
from dotenv import load_dotenv

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

load_dotenv()


# https://ankitbko.github.io/blog/2021/04/logging-in-python/
def debugger(func) -> Callable:
    """
    Wrapped decorator that logs nearly everything.
    Just add @debugger decorator.
    The "name" must match main logging setup since we borrow its setup.
    You need to restart the worker after modifying .env.
    :param func: Any function (except generators)
    :return: Callable wrapper
    """

    # if os.environ.get("DEBUGGER") != "true":
    #     return func

    name = "purrio"
    logger = logging.getLogger(name)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):

        args_repr = [repr(a) for a in args]
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]
        signature = ", ".join(args_repr + kwargs_repr)
        logger.debug(f"DEBUGGER [{func.__name__}] w/args: {signature}")
        try:
            result = func(*args, **kwargs)
            logger.debug(f"DEBUGGER [{func.__name__}] result: {result}")
            return result
        except Exception as e:
            logger.exception(
                f"DEBUGGER Exception raised [{func.__name__}]. exception: {str(e)}"
            )
            raise e

    return wrapper
