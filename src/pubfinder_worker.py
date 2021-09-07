import json
import logging
import os
import re
import time
import uuid
from functools import lru_cache

import requests
import pymongo
from multiprocessing.pool import ThreadPool
from collections import deque

from event_stream.dao import DAO
from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event
from event_stream.dao import DAO

import crossref_source
import amba_source
import meta_source

from kafka import KafkaConsumer
from kafka.vendor import six


class PubFinderWorker(EventStreamProducer):
    state = "unknown"
    relation_type = "discusses"

    log = "PubFinderWorker "
    group_id = "pub-finder-worker"

    required_fields = {
        'type', 'doi', 'abstract', 'pubDate', 'publisher', 'citationCount', 'title', 'normalizedTitle', 'year',
        'citations', 'refs', 'authors', 'fieldOfStudy', 'source_id'
    }

    consumer = None
    collection = None
    collectionFailed = None
    # 'meta': Source
    source_order = ['db', 'amba', 'crossref']

    db_pool = None
    db_queue = deque()

    result_pool = None
    results = deque()

    amba_source = None
    crossref_source = None
    meta_source = None

    dao = None
    process_number = 2

    def create_consumer(self):
        # logging.warning(self.log + "rt: %s" % self.relation_type)

        if self.state == 'all':
            self.topics = self.build_topic_list()

        if isinstance(self.state, six.string_types):
            self.state = [self.state]

        if isinstance(self.relation_type, six.string_types):
            self.relation_type = [self.relation_type]

        results = deque()
        if not self.result_pool:
            self.result_pool = ThreadPool(1, self.worker_results, (self.results,))

        self.amba_source = amba_source.AmbaSource(self.results)
        self.crossref_source = crossref_source.CrossrefSource(self.results)
        self.meta_source = meta_source.MetaSource(self.results)

        if not self.topics:
            self.topics = list()
            for state in self.state:
                for relation_type in self.relation_type:
                    self.topics.append(self.get_topic_name(state=state, relation_type=relation_type))

        # self.topic_name = 'tweets'
        logging.debug(self.log + "get consumer for topic: %s" % self.topics)
        # consumer.topics()
        self.consumer = KafkaConsumer(group_id=self.group_id,
                                      bootstrap_servers=self.bootstrap_servers, api_version=self.api_version,
                                      consumer_timeout_ms=self.consumer_timeout_ms)

        for topic in self.topics:
            logging.debug(self.log + "consumer subscribe: %s" % topic)
            self.consumer.subscribe(topic)

        logging.warning(self.log + "consumer subscribed to: %s" % self.consumer.topics())

    def consume(self):
        logging.warning(self.log + "start consume")
        self.running = True

        if not self.consumer:
            self.create_consumer()

        if not self.db_pool:
            self.db_pool = ThreadPool(1, self.worker_db, (self.db_queue,))

        if not self.dao:
            self.dao = DAO()

        logging.debug(self.log + "wait for messages")
        while self.running:
            try:
                for msg in self.consumer:
                    # logging.warning(self.log + 'msg in consumer ')

                    e = Event()
                    e.from_json(json.loads(msg.value.decode('utf-8')))
                    if e is not None:
                        self.db_queue.append(e)

            except Exception as exc:
                self.consumer.close()
                logging.error(self.log + 'stream Consumer generated an exception: %s' % exc)
                logging.warning(self.log + "Consumer closed")
                break

        # keep alive
        if self.running:
            return self.consume()

        # stop all sources
        # for key, source in self.source_order.items():
        #     source.stop()

        self.result_pool.close()
        self.db_pool.close()
        logging.warning(self.log + "Consumer shutdown")

    def worker_results(self, queue):
        logging.warning(self.log + "worker results")
        while self.running:
            try:
                result = queue.pop()
            except IndexError:
                time.sleep(0.1)
                # logging.warning(self.log + "sleep worker results")
                pass
            else:
                if result and 'item' in result and 'tag' in result:
                    self.finish_work(result['item'], result['tag'])

    def worker_db(self, queue):
        while self.running:
            try:
                item = queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                publication = PubFinderWorker.get_publication(item)
                logging.warning(self.log + "work item mongo " + publication['doi'])

                # todo
                publication_temp = self.dao.get_publication(publication['doi'])

                if publication_temp:
                    logging.warning(self.log + " found in db " + publication['doi'])
                    publication = publication_temp

                publication['source'] = 'db'
                publication['source_id'] = [{'title': 'DB', 'url': 'https://analysis.ambalytics.cloud/',
                                             'license': 'MIT'}]

                if type(item) is Event:
                    item.data['obj']['data'] = publication

                # todo non event type
                self.finish_work(item, 'db')

    def finish_work(self, item, source):
        publication = PubFinderWorker.get_publication(item)
        # logging.warning(self.log + "finish_work %s item %s" % (publication['doi'], source))

        # logging.warning(self.log + "finish_work publication " + json.dumps(publication))
        if self.is_publication_done(publication):
            logging.warning(self.log + "publication done " + publication['doi'])

            if source != 'db':
                self.dao.save_publication(publication)

            # todo go through refs
            # foreach ref appendLeft to queue

            if type(item) is Event:
                item.set('state', 'linked')
                self.publish(item)

        else:
            # put it in next queue or stop
            if source in self.source_order:

                if source == 'db':
                    logging.warning('db -> amba ' + publication['doi'])
                    self.amba_source.work_queue.append(item)

                if source == 'amba':
                    logging.warning('amba -> crossref ' + publication['doi'])
                    self.crossref_source.work_queue.append(item)

                if source == 'crossref':
                    logging.warning('crossref -> meta ' + publication['doi'])
                    self.meta_source.work_queue.append(item)

            # if type(item) is Event:
            #     self.save_not_found(item)
            # todo save unresolved dois with the reason (whats missing)

    @staticmethod
    def get_publication(item):
        publication = None  # todo for refs in case of publication type
        # logging.warning("get_publication 1 " + item.get_json())

        if type(item) is Event:
            publication = item.data['obj']['data']

        # logging.warning("get_publication 2 " + json.dumps(publication))
        return publication

    @staticmethod
    def is_publication_done(publication):
        if not publication:
            return False

        # they can be empty but must me set, id should be enough? citationCount, citations, refs

        keys = ("type", "doi", "abstract", "pubDate", "publisher", "title", "normalizedTitle", "year",
                "authors", "fieldsOfStudy", "source_id")  # todo use required fields??
        # if not (set(keys) - publication.keys()):
        if all(key in publication for key in keys):
            # add check for length of title/abstract etc, content check not just existence?
            logging.warning('publication done ' + publication['doi'])
            return True

        logging.debug('publication missing ' + str(set(keys) - publication.keys()))
        return False

    @staticmethod
    def normalize(string):
        # todo numbers, special characters/languages
        return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()

    @staticmethod
    def start(i=0):
        """start the consumer
        """
        pfw = PubFinderWorker(i)
        logging.debug(PubFinderWorker.log + 'Start %s' % str(i))
        pfw.consume()


if __name__ == '__main__':
    PubFinderWorker.start(1)
