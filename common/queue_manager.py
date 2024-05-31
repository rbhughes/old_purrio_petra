import queue
import threading
import concurrent.futures


class QueueManager:
    def __init__(self, max_workers):
        self.queue = queue.Queue()
        self.max_workers = max_workers
        self.running = True
        self.thread = None

    def add_task(self, task):
        self.queue.put(task)

    def process_queue(self, task_handler):
        def worker():
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers
            ) as executor:
                while self.running:
                    try:
                        task = self.queue.get(block=True, timeout=1)
                        executor.submit(task_handler, task)
                    except queue.Empty:
                        continue

        self.thread = threading.Thread(target=worker, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        self.thread.join()
