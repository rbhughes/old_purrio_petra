from purr_worker import PurrWorker

# run like this:
# python -m client

if __name__ == "__main__":
    pw = PurrWorker()
    pw.register_worker()
    pw.start_queue_processing()
    pw.listen()
