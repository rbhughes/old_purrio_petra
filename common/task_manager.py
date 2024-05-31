import re
from retry import retry
from common.logger import Logger
from common.util import RetryException
from typing import Optional

logger = Logger(__name__)


class TaskManager:
    def __init__(self, sb_client):
        self.sb_client = sb_client

    @retry(RetryException, tries=2)
    def manage_task(self, task_id: int, status: Optional[str] = None) -> None:
        """
        We use the supabase task table with realtime as a queue. This method
        updates the task status and (later) deletes it
        This is the first method called after receiving a task, so we add retry
        for expired sessions.
        :param task_id: An autoincrement int from supabase
        :param status: PENDING, PROCESSING or FAILED. see is_valid_status()
        :return: None
        """
        try:
            if status is None:
                self.sb_client.table("task").delete().eq("id", task_id).execute()
            elif status in ("PROCESSING", "FAILED"):
                (
                    self.sb_client.table("task")
                    .update({"status": status})
                    .eq("id", task_id)
                    .execute()
                )
        except Exception as err:
            if re.search("JWT expired", str(err)):
                print(err)
                logger.warning("Session JWT expired. Retrying after sign-in...")
                self.sb_client.sign_in()
                raise RetryException from err
            else:
                raise err

    def manage_asset_batch(self, task_id, batch_id, status=None) -> None:
        """
        A batcher task can spawn multiple loader (sub)tasks. We keep track of
        them in the supabase batch_ledger table.
        :param task_id: The normal task_id of a loader task
        :param batch_id: Think of it as the "parent" task_id (autoincr int)
        :param status: PENDING, PROCESSING or FAILED
        :return: None
        See also: manage_task() and is_batch_finished()
        """
        if status is None:
            (
                self.sb_client.table("batch_ledger")
                .delete()
                .eq("batch_id", batch_id)
                .eq("task_id", task_id)
                .execute()
            )
        else:
            (
                self.sb_client.table("batch_ledger")
                .update({"status": status})
                .eq("batch_id", batch_id)
                .eq("task_id", task_id)
                .execute()
            )

    def is_batch_finished(self, batch_id) -> bool:
        """
        As loader tasks are processed, check the batch_ledger table to see if
        any tasks remain for the given batch_id. All gone returns True
        :param batch_id: Think of it as the "parent" task_id (autoincr int)
        :return: bool: True if no loader tasks remain in batch_ledger. Note that
        any failed tasks (i.e. status = FAILED) will cause False.
        """
        # pycharm inspector is wrong
        # https://anand2312.github.io/pgrest/reference/builders/
        # noinspection PyTypeChecker
        res = (
            self.sb_client.table("batch_ledger")
            .select("*", count="exact")
            .eq("batch_id", batch_id)
            .execute()
        )
        return res.count == 0
