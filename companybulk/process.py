import os
from abc import ABC, abstractmethod
from multiprocessing import JoinableQueue
from multiprocessing.context import Process
import multiprocessing


class ProcessPool(ABC):
    queue = None

    def __init__(self, nb_workers=None):
        if not nb_workers:
                nb_workers = multiprocessing.cpu_count()
        self.queue = JoinableQueue(maxsize=0)
        self.processes = [Process(target=self.process_queue) for i in range(nb_workers)]
        for p in self.processes:
            p.start()

    def enqueue(self, item):
        self.queue.put(item)

    def process_queue(self):
        process_id = os.getpid()
        print(f'Process #{process_id} started...')

        while True:
            print(f'Process #{process_id} getting from queue')
            item = self.queue.get()
            if item is None:
                print(f'Process #{process_id} received None from queue')
                break

            self.process_item(item, process_id)

            self.queue.task_done()

    @abstractmethod
    def process_item(self, item, process_id):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        """ wait until queue is empty and terminate processes """
        self.queue.join()
        for p in self.processes:
            p.terminate()
