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

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event

from crossref_source import CrossrefSource
from amba_source import AmbaSource
from meta_source import MetaSource

from kafka import KafkaConsumer, KafkaProducer
from kafka.vendor import six


class PubFinderWorker(EventStreamConsumer, EventStreamProducer):
    state = "unknown"
    relation_type = "discusses"

    log = "PubFinderWorker "
    group_id = "pub-finder-worker"

    config = {
        'mongo_url': "mongodb://mongo_db:27017/",
        'mongo_client': "events",
        'mongo_collection': "publication",
    }

    # config = {
    #     'mongo_url': "mongodb://root:example@mongo_db:27017/",
    #     'mongo_client': "publications",
    #     'mongo_collection': "linked",
    #     'mongo_collection_author': "author",
    #     'mongo_collection_field_of_research': "fieldOfResearch",
    #     'mongo_collection_affiliation': "affiliation",
    # }

    required_fields = {
        'type', 'doi', 'abstract', 'pubDate', 'publisher', 'citationCount', 'title', 'normalizedTitle', 'year',
        'citations', 'refs', 'authors', 'fieldOfStudy'
    }

    collection = None
    # 'meta': Source
    source_order = ['mongo', 'amba', 'crossref']

    mongo_pool = None
    mongo_queue = deque()

    amba_source = None
    crossref_source = None
    meta_source = None

    def create_consumer(self):
        logging.warning(self.log + "rt: %s" % self.relation_type)

        if self.state == 'all':
            self.topics = self.build_topic_list()

        if isinstance(self.state, six.string_types):
            self.state = [self.state]

        if isinstance(self.relation_type, six.string_types):
            self.relation_type = [self.relation_type]

        # if not self.source_order:
        #     self.source_order = {
        #         'mongo': AmbaSource(self),
        #         'amba': CrossrefSource(self),
        #         'crossref': MetaSource(self),
        #     }

        self.amba_source = AmbaSource(self)
        self.crossref_source = CrossrefSource(self)
        self.meta_source = MetaSource(self)

        if not self.topics:
            self.topics = list()
            for state in self.state:
                for relation_type in self.relation_type:
                    self.topics.append(self.get_topic_name(state=state, relation_type=relation_type))

        # self.topic_name = 'tweets'
        logging.warning(self.log + "get consumer for topic: %s" % self.topics)
        # consumer.topics()
        self.consumer = KafkaConsumer(group_id=self.group_id,
                                      bootstrap_servers=self.bootstrap_servers, api_version=self.api_version,
                                      consumer_timeout_ms=self.consumer_timeout_ms)

        for topic in self.topics:
            logging.warning(self.log + "consumer subscribe: %s" % topic)
            self.consumer.subscribe(topic)

        logging.warning(self.log + "consumer subscribed to: %s" % self.consumer.topics())

    def consume(self):
        logging.warning(self.log + "start consume")
        self.running = True

        if not self.consumer:
            self.create_consumer()

        if not self.collection:
            self.prepare_mongo_connection()

        if not self.mongo_pool:
            self.mongo_pool = ThreadPool(1, self.worker_mongo, (self.mongo_queue,))

        logging.warning(self.log + "wait for messages")
        while self.running:
            try:
                for msg in self.consumer:
                    # logging.warning(self.log + 'msg in consumer ')

                    e = Event()
                    e.from_json(json.loads(msg.value.decode('utf-8')))
                    if e is not None:
                        self.mongo_queue.append(e)


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

        self.mongo_pool.close()
        logging.warning(self.log + "Consumer shutdown")

    def worker_mongo(self, queue):
        while self.running:
            try:
                item = queue.pop()
            except IndexError:
                pass
            else:
                publication = PubFinderWorker.get_publication(item)
                logging.warning(self.log + "work item mongo " + publication['doi'])

                publication_temp = self.get_publication_from_mongo(publication['doi'])

                if publication_temp:
                    logging.warning(self.log + " found in mongo " + publication['doi'])
                    publication = publication_temp

                publication['source'] = 'mongo'

                if type(item) is Event:
                    item.data['obj']['data'] = publication

                # todo non event type
                # logging.warning(self.log + "mongo is done continues " + item.get_json())
                self.finish_work(item, 'mongo')

    def finish_work(self, item, source):
        publication = PubFinderWorker.get_publication(item)
        # logging.warning(self.log + "finish_work %s item %s" % (publication['doi'], source))

        # logging.warning(self.log + "finish_work publication " + json.dumps(publication))
        if self.is_publication_done(publication):
            logging.warning(self.log + "publication done " + publication['doi'])

            if source != 'mongo':
                self.save_to_mongo(publication)

            # todo go through refs
            # foreach ref appendLeft to queue

            if type(item) is Event:
                item.set('state', 'linked')
                self.publish(item)

        else:
            # put it in next queue or stop
            if source in self.source_order:

                if source == 'mongo':
                    logging.warning('mongo -> amba ' + publication['doi'])
                    self.amba_source.work_queue.append(item)

                if source == 'amba':
                    logging.warning('amba -> crossref ' + publication['doi'])
                    self.crossref_source.work_queue.append(item)

                if source == 'crossref':
                    logging.warning('crossref -> meta ' + publication['doi'])
                    self.meta_source.work_queue.append(item)

            #     logging.warning(self.log + "publication %s continues, key %s tag %s" % (
            #         publication['doi'], source, self.source_order[source].tag))
            #     self.source_order[source].work_queue.append(item)
            # for source in self.source_order:
            #     print(self.source_order[source].work_queue)

    @lru_cache(maxsize=100)
    def get_publication_from_mongo(self, doi):
        if not self.collection:
            self.prepare_mongo_connection()
        result = self.collection.find_one({"doi": doi})
        return result

    def prepare_mongo_connection(self):
        mongo_client = pymongo.MongoClient(host=self.config['mongo_url'],
                                           serverSelectionTimeoutMS=3000,  # 3 second timeout
                                           username="root",
                                           password="example"
                                           )
        db = mongo_client[self.config['mongo_client']]
        self.collection = db[self.config['mongo_collection']]

    def save_to_mongo(self, publication):
        try:
            # save publication to db todo remove 'id' ?
            # publication['_id'] = uuid.uuid4().hex
            publication['_id'] = publication['doi']
            self.collection.insert_one(publication)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB can't save publication %s" % publication['doi'])

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
                "authors", "fieldsOfStudy")
        # if not (set(keys) - publication.keys()):
        if all(key in publication for key in keys):
            logging.warning('publication done ' + publication['doi'])
            return True

        logging.warning('publication missing ' + str(set(keys) - publication.keys()))
        return False

    @staticmethod
    def normalize(string):
        # todo numbers, special characters/languages
        return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s.%(msecs)03d %(threadName)s:%(message)s")
    logging.warning("start pubfinder connector")

    time.sleep(11)

    e = PubFinderWorker(1)
    e.consume()
