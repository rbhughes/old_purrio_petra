import os
import simplejson as json
import sys
import threading
from dotenv import load_dotenv

from common.sb_client import SupabaseClient
from common.messenger import Messenger
from common.queue_manager import QueueManager
from common.task_manager import TaskManager
from common.typeish import validate_task, validate_repo, Repo
from common.util import init_socket, hostname, SUITE
from common.logger import Logger
from recon.recon import repo_recon

from typing import Any, Callable, Dict, List

load_dotenv()
logger = Logger(__name__)


class PurrWorker:

    def __init__(self) -> None:
        self.sb_client = SupabaseClient()
        self.task_manager = TaskManager(self.sb_client)
        self.messenger = Messenger(self.sb_client)

        work_max_workers = int(os.environ.get("WORK_MAX_WORKERS"))
        self.work_queue = QueueManager(work_max_workers)
        self.work_queue_thread = threading.Thread(
            target=self.work_queue.process_queue,
            args=(self.task_handler,),
            daemon=True,
        )

        search_max_workers = int(os.environ.get("SEARCH_MAX_WORKERS"))
        self.search_queue = QueueManager(search_max_workers)
        self.search_queue_thread = threading.Thread(
            target=self.search_queue.process_queue,
            args=(self.task_handler,),
            daemon=True,
        )
        self.socket = init_socket()
        self.running = True

        logger.info(f"PurrWorker ({SUITE}) initialized...")

    def register_worker(self):
        data = (
            self.sb_client.table("worker")
            .upsert({"hostname": hostname(), "suite": SUITE})
            .execute()
        )
        if data:
            logger.send_message(
                directive="note",
                data={"note": f"registered {SUITE} worker: {hostname()}"},
                workflow="any",
            )

    def start_queue_processing(self):
        self.work_queue.process_queue(self.task_handler)
        self.search_queue.process_queue(self.task_handler)

    def stop_queue_processing(self):
        self.work_queue.stop()
        self.search_queue.stop()

    def add_to_work_queue(self, task) -> None:
        self.work_queue.add_task(task)

    def process_work_queue(self) -> None:
        self.work_queue.process_queue(self.task_handler)

    def add_to_search_queue(self, task) -> None:
        self.search_queue.add_task(task)

    def process_search_queue(self) -> None:
        self.search_queue.process_queue(self.task_handler)

    ########################################################################

    def halt(self) -> None:
        """
        Stop queues, sign out of Supbase and shut down
        :return: None
        """
        self.stop_queue_processing()
        self.sb_client.sign_out()
        sys.exit()

    def fetch_repo(self, body) -> Repo:
        """
        Fetch a Repo from supabase and validate it as a "real" Repo dataclass
        :param body: The batcher and loader task body contains repo_id
        :return: an instance of Repo
        """
        res = self.sb_client.table("repo").select("*").eq("id", body.repo_id).execute()
        repo = validate_repo((res.data[0]))
        return repo

    ###########################################################################

    def handle_batcher(self, task):
        pass

    ###########################################################################

    def handle_loader(self, task):
        pass

    ###########################################################################

    def handle_recon(self, task):
        """
        This task recursively crawls the given filesystem path to locate
        Petra projects (i.e a repo). A well-centric metadata inventory is
        collected along with some directory stats. The repos discovered during
        recon are available as targets from which to collect assets.
        :param task: An instance of ReconTask
        :return: TODO
        """

        # 0. notify client of job/task start
        logger.send_message(directive="busy", data={"job_id": task.id})

        # 1. run repo_recon (returned as dicts for supabase)
        repos: List[Dict[str, Any]] = repo_recon(task.body)

        # 2. write repos to repo table
        self.sb_client.table("repo").upsert(repos).execute()

        # 3. send message
        for repo in repos:
            logger.send_message(
                directive="note",
                repo_id=repo["id"],
                data={"note": f"added repo: {repo["fs_path"]}"},
                workflow="recon",
            )

        # 4. notify client of job/task end
        logger.send_message(directive="done", data={"job_id": task.id})

        return True

    ###########################################################################

    def handle_search(self, task):
        pass

    ###########################################################################
    ###########################################################################

    # @auto_log
    def task_handler(self, task):

        task_handlers = {
            # "batcher": self.handle_batcher,
            # "loader": self.handle_loader,
            "recon": self.handle_recon,
            # "search": self.handle_search,
            # "export": handle_export,
            # "stats": self.handle_stats,
            # "halt": self.halt,
        }

        self.task_manager.manage_task(task.id, "PROCESSING")

        # TODO: revisit typing here
        handler: Callable[[Any], None] = task_handlers.get(task.directive)

        if task.directive == "halt":
            self.halt()
        elif handler:
            try:
                handler(task)
            except Exception as error:
                logger.exception(error)
            finally:
                # probably needless cleanup
                self.task_manager.manage_task(task.id)
        #
        # else:
        #     print(f"Unknown task directive: {task.directive}")

    def listen(self) -> None:
        self.socket.connect()
        channel = self.socket.set_channel("realtime:public:task")

        def pluck(payload):
            task = validate_task(payload)
            print(task)

            if task:
                logger.debug(f"plucked {task.directive} task from queue")
                if task.directive == "search" or task.directive == "export":
                    self.add_to_search_queue(task)
                else:
                    self.add_to_work_queue(task)

        channel.join().on("INSERT", pluck)
        channel.join().on("UPDATE", pluck)

        self.socket.listen()
