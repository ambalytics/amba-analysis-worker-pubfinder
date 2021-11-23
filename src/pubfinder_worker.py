import json
import logging
import re
import time
from multiprocessing.pool import ThreadPool
from collections import deque
import os
import sentry_sdk
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event
from .pubfinder_helper import PubFinderHelper
from event_stream.dao import DAO
import amba_source
import crossref_source
import meta_source
import openaire_source
import semanticscholar_source

from kafka import KafkaConsumer
from kafka.vendor import six


class PubFinderWorker(EventStreamProducer):
    state = "unknown"
    relation_type = "discusses"

    log = "PubFinderWorker "
    group_id = "pub-finder-worker"

    required_fields = {
        'type', 'doi', 'abstract', 'pub_date', 'publisher', 'citation_count', 'title', 'normalized_title', 'year',
        'citations', 'refs', 'authors', 'fieldOfStudy', 'source_id'
    }

    consumer = None
    collection = None
    collectionFailed = None

    sources = ['db', 'amba', 'crossref', 'openaire', 'semanticscholar']

    db_pool = None
    db_queue = deque()

    result_pool = None
    results = deque()

    amba_source = None
    crossref_source = None
    meta_source = None
    openaire_source = None
    semanticscholar_source = None

    dao = None

    def create_consumer(self):
        """ create and setup all needed sources and connectinos"""

        if self.state == 'all':
            self.topics = self.build_topic_list()

        if isinstance(self.state, six.string_types):
            self.state = [self.state]

        if isinstance(self.relation_type, six.string_types):
            self.relation_type = [self.relation_type]

        self.results = deque()
        if not self.result_pool:
            self.result_pool = ThreadPool(1, self.worker_results, (self.results,))

        self.amba_source = amba_source.AmbaSource(self.results)
        self.crossref_source = crossref_source.CrossrefSource(self.results)
        self.meta_source = meta_source.MetaSource(self.results)
        self.openaire_source = openaire_source.OpenAireSource(self.results)
        self.semanticscholar_source = semanticscholar_source.SemanticScholarSource(self.results)

        if not self.topics:
            self.topics = list()
            for state in self.state:
                for relation_type in self.relation_type:
                    self.topics.append(self.get_topic_name(state=state, relation_type=relation_type))

        logging.debug(self.log + "get consumer for topic: %s" % self.topics)
        self.consumer = KafkaConsumer(group_id=self.group_id,
                                      bootstrap_servers=self.bootstrap_servers, api_version=self.api_version,
                                      consumer_timeout_ms=self.consumer_timeout_ms)

        for topic in self.topics:
            logging.debug(self.log + "consumer subscribe: %s" % topic)
            self.consumer.subscribe(topic)

        logging.warning(self.log + "consumer subscribed to: %s" % self.consumer.topics())

    def consume(self):
        """ consume new events from kafka using a thread pool"""
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

        self.result_pool.close()
        self.db_pool.close()
        logging.warning(self.log + "Consumer shutdown")

    def worker_results(self, queue):
        """ worker functions to handle resulting  """
        logging.warning(self.log + "worker results")
        while self.running:
            try:
                result = queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                if result and 'item' in result and 'tag' in result:
                    self.finish_work(result['item'], result['tag'])

    def worker_db(self, queue):
        """ worker function to retrieve publication data from the postgreSQL """
        while self.running:
            try:
                item = queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                publication = PubFinderHelper.get_publication(item)

                publication_temp = self.dao.get_publication(publication['doi'])
                if publication_temp and isinstance(publication, dict):
                    logging.warning(self.log + " found in db " + publication['doi'])
                    publication = publication_temp

                publication['source'] = 'db'
                publication['source_id'] = [{'title': 'DB', 'url': 'https://ambalytics.com/',
                                             'license': 'MIT'}]

                if type(item) is Event:
                    item.data['obj']['data'] = publication

                self.finish_work(item, 'db')

    def finish_work(self, item, source):
        """ check if a publication is finished or needs further processing
         Arguments:
             item: the item to work on (publication)
             source: the source specifies the last used source
         """
        publication = PubFinderHelper.get_publication(item)

        pub_is_done = PubFinderHelper.is_publication_done(publication)
        if pub_is_done is True:
            logging.warning(self.log + "publication done " + publication['doi'])

            if source != 'db':
                self.dao.save_publication(publication)

            if type(item) is Event:
                item.set('state', 'linked')
                logging.warning(publication['doi'])
                self.publish(item)

        else:
            # put it in next queue or stop
            if source in self.sources:

                if source == 'db':
                    logging.debug('db -> amba ' + publication['doi'])
                    self.amba_source.work_queue.append(item)

                if source == 'amba':
                    logging.debug('amba -> crossref ' + publication['doi'])
                    self.crossref_source.work_queue.append(item)

                if source == 'crossref':
                    logging.debug('crossref -> openaire ' + publication['doi'])
                    self.openaire_source.work_queue.append(item)

                if source == 'openaire':
                    logging.debug('openaire -> semanticscholar ' + publication['doi'])
                    self.semanticscholar_source.work_queue.append(item)

                if source == 'semanticscholar':
                    logging.debug('semanticscholar -> meta ' + publication['doi'])
                    self.meta_source.work_queue.append(item)

            else:
                done_now = PubFinderHelper.is_publication_done(publication, True)
                if done_now is True:
                    logging.warning(self.log + "publication done " + publication['doi'])

                    if source != 'db':
                        self.dao.save_publication(publication)
                else:
                    self.dao.save_publication_not_found(publication['doi'], pub_is_done)
                    logging.warning('unable to find publication data for ' + publication['doi'] + ' - ' + str(done_now))

    @staticmethod
    def start(i=0):
        """start the consumer
        """
        pfw = PubFinderWorker(i)
        logging.debug(PubFinderWorker.log + 'Start %s' % str(i))
        pfw.consume()


if __name__ == '__main__':
    SENTRY_DSN = os.environ.get('SENTRY_DSN')
    SENTRY_TRACE_SAMPLE_RATE = os.environ.get('SENTRY_TRACE_SAMPLE_RATE')
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        traces_sample_rate=SENTRY_TRACE_SAMPLE_RATE
    )

    PubFinderWorker.start(1)
