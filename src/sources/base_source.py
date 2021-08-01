from collections import deque
from functools import lru_cache
from multiprocessing.pool import ThreadPool

from ..pubfinder_worker import PubFinderWorker


# base source, to be extended for use
class BaseSource(object):
    work_queue = deque()
    work_pool = None
    running = True
    tag = 'base'

    def __init__(self, pubfinder):
        if not self.work_pool:
            self.work_pool = ThreadPool(4, self.worker, (self.work_queue,))
        self.pubfinder = pubfinder

    def worker(self, queue):
        while self.running:
            item = queue.pop()

            publication = PubFinderWorker.get_publication(item)

            # todo source stuff
            self.add_data_to_publication(publication)

            publication['source'] = self.tag

            if PubFinderWorker.is_event(item):
                item.data['obj'] = publication

            if PubFinderWorker.is_publication_done(publication):
                self.pubfinder.finish_work(item, self.tag)

    @lru_cache(maxsize=1000)
    def add_data_to_publication(self, publication):
        # todo replace with real work
        return publication

    def stop(self):
        self.running = False
        self.work_pool.close()
