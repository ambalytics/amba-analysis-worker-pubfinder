import logging
from collections import deque
from functools import lru_cache
from multiprocessing.pool import ThreadPool
from event_stream.event import Event


# from pubfinder_worker import PubFinderWorker


# base source, to be extended for use
class BaseSource(object):
    work_queue = deque()
    work_pool = None
    running = True
    tag = 'base'
    log = 'Source ' + tag
    threads = 1

    def __init__(self, pubfinder):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, (self.work_queue,))
        self.pubfinder = pubfinder

    def worker(self, queue):
        while self.running:
            try:
                item = queue.pop()
            except IndexError:
                pass
            else:
                if item:
                    # logging.warning(self.log + " item " + str(item.get_json()))
                    publication = self.pubfinder.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])
                    # logging.warning(self.log + " q " + str(queue))x

                    # todo source stuff
                    publication_temp = self.add_data_to_publication(publication)

                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder.finish_work(item, self.tag)

    # todo cache?
    def add_data_to_publication(self, publication):
        # todo replace with real work
        return publication

    def stop(self):
        self.running = False
        self.work_pool.close()
