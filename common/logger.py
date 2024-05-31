import logging
import os
import sys
import simplejson as json
from common.messenger import Messenger
from common.sb_client import SupabaseClient
from dotenv import load_dotenv

# CRITICAL
# ERROR
# WARNING
# INFO
# DEBUG

load_dotenv()
LOG_DIR = os.environ.get("LOG_DIR") or "logs"
NAME = "purrio"


class Logger:
    """
    Move the regular logger instantiation to a singleton class
    Messenger is
    """

    _instance = None

    # ensure singleton class
    def __new__(cls, source):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.__init__(source)

            file_handler = next(
                (
                    handler
                    for handler in cls._instance.logger.handlers
                    if handler.name == "file_handler"
                ),
                None,
            )
            if file_handler:
                logfile = file_handler.baseFilename
                print(f"initialized log file: {logfile}")

        return cls._instance

    def __init__(self, source):
        # ts = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        # logfile = os.path.join(LOG_DIR, f"{ts}_purrio.log")
        logfile = os.path.join(LOG_DIR, f"{NAME}.log")
        # print("Initialized log file: " + logfile)

        # does not work: the "root" logger is already present
        # logging.root.disable = True

        self.logger = logging.getLogger(NAME)
        self.logger.setLevel(logging.DEBUG)

        # critical to avoid double-stream to root logger
        self.logger.propagate = False

        self.sb_client = SupabaseClient()
        self.messenger = Messenger(self.sb_client)

        formatter = logging.Formatter(
            f"%(asctime)s - %(name)s - %(levelname)s - {source} | %(message)s"
        )

        self.logger.handlers = []

        if "console_handler" not in [x.name for x in self.logger.handlers]:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(formatter)
            console_handler.set_name("console_handler")
            self.logger.addHandler(console_handler)

        if os.environ.get("LOG_DIR"):
            if "file_handler" not in [x.name for x in self.logger.handlers]:
                file_handler = logging.FileHandler(logfile, mode="a")
                file_handler.setLevel(logging.DEBUG)
                file_handler.setFormatter(formatter)
                file_handler.set_name("file_handler")
                self.logger.addHandler(file_handler)

    def critical(self, message):
        self.logger.critical(message)

    def error(self, message):
        self.logger.error(message)

    def warning(self, message):
        self.logger.warning(message)

    def info(self, message):
        self.logger.info(message)

    def debug(self, message):
        self.logger.debug(message)

    def exception(self, message):
        self.logger.exception(message)

    def send_message(self, directive, repo_id=None, data=None, workflow=None):
        if "note" in data:
            self.logger.info(data["note"])
        else:
            self.logger.info(directive + " " + json.dumps(data))

        self.messenger.send(directive, repo_id, data, workflow)
