import hashlib
import os
import socket
import time
import simplejson as json
from datetime import datetime
from functools import wraps
from realtime.connection import Socket

from typing import Callable


SUITE = 'petra'


def timer(func) -> Callable:
    """
    A wrapped timer that simply prints start/end elapsed execution time. Ex:
    [handle_search START: 2024-04-26 10:12:24]
    [handle_search END: 2024-04-26 10:12:25] ~ 0.89 seconds, 0.01 minutes
    :param func: any function, apply @timer decorator
    :return: None
    """

    @wraps(func)
    def timer_wrapper(*args, **kwargs):
        t0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{func.__name__} START: {t0}]")
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        t1 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        elapsed = f"{total_time:.2f} seconds, {(total_time/60):.2f} minutes"
        print(f"[{func.__name__} END: {t1}] ~ {elapsed}")
        return result

    return timer_wrapper


def hostname() -> str:
    """
    Just print this PC's lowercase hostname
    :return: A hostname string
    """
    return socket.gethostname().lower()


def hashify(s: str) -> str:
    """
    Return an MD5 hash on any string
    :param s: Any input string
    :return: md5 hash
    """
    return hashlib.md5(s.lower().encode()).hexdigest()


def merge_nested_dict(a: dict, b: dict, path=None) -> dict:
    """
    Merge two dictionaries (b into a), handling nested dictionaries by
    recursively merging their values. If there are conflicts between leaf values
    (i.e., non-dictionary values) at the same path in a and b, it prints a
    message indicating the conflict.
    :param a: Any dict (the parent?)
    :param b: Any dict (the child?)
    :param path: An optional
    :return: a single merged dict
    """
    """merges b into a"""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_nested_dict(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass  # same leaf value
            else:
                print(f"dict merge conflict at {'.'.join(path + [str(key)])}")
        else:
            a[key] = b[key]
    return a


def dir_exists(fs_path: str) -> bool:
    """
    Is the supplied path a valid directory from the context of this PC and user?
    :param fs_path: A file path
    :return: True if valid
    """
    return os.path.isdir(fs_path)


def normalize_path(fs_path: str) -> str:
    """
    Assuming the string is a valid path, replace all backslashes with forward
    slashes. This avoids the double-escape madness on UNC paths. Windows can
    usually resolve forward slash UNCs like: //server/share/path
    :param fs_path: Any path string
    :return: path with forward slashes
    """
    return fs_path.replace("\\", "/")


def local_pg_params() -> dict:
    """
    Default params for the local instance of PostgreSQL. Password is in .env
    :return: dict of connection parameters
    """
    return {
        "user": "postgres",
        "host": "localhost",
        "database": "postgres",
        "password": os.environ.get("LOCAL_PG_PASS"),
        "port": 5432,
    }


def init_socket() -> Socket:
    """
    Initialize supabase realtime socket from .env. Project details are from:
    supabase.com/dashboard/<project>/settings/api
    :return: A realtime.connection Socket
    """
    sb_key: str = os.environ.get("SUPABASE_KEY")
    sb_id: str = os.environ.get("SUPABASE_ID")
    socket_url = (
        f"wss://{sb_id}.supabase.co/realtime/v1/websocket?apikey={sb_key}&vsn=1.0.0"
    )
    return Socket(socket_url, auto_reconnect=True)


def is_valid_json(my_json_string):
    try:
        json.loads(my_json_string)
        return True
    except json.JSONDecodeError:
        return False


class RetryException(Exception):
    """Just a trigger to catch JWT expired exception"""
