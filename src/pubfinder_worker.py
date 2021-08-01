import json
import logging
import os
import re
import time
import uuid
import requests
import pymongo
from multiprocessing.pool import ThreadPool
from collections import deque

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event

from .sources.crossref_source import CrossrefSource
from .sources.amba_source import AmbaSource
from .sources.meta_source import MetaSource


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
    source_order = {
        'mongo': AmbaSource,
        'amba': CrossrefSource,
        'crossref': MetaSource,
    }

    mongo_pool = None
    mongo_queue = deque()

    def consume(self):
        logging.warning(self.log + "start consume")
        self.running = True

        if not self.consumer:
            self.create_consumer()

        if not self.collection:
            self.prepare_mongo_connection()

        if not self.mongo_pool:
            self.mongo_pool = ThreadPool(4, self.worker_mongo, (self.mongo_queue,))

        while self.running:
            try:
                for msg in self.consumer:
                    logging.debug(self.log + 'msg in consumer ')

                    e = Event()
                    e.from_json(json.loads(msg.value.decode('utf-8')))

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
        for key, source in self.source_order.items():
            source.stop()

        self.mongo_pool.close()
        logging.warning(self.log + "Consumer shutdown")

    def worker_mongo(self, queue):
        while self.running:
            item = queue.pop()
            publication = PubFinderWorker.get_publication(item)

            self.get_publication_from_mongo(publication['doi'])

            if PubFinderWorker.is_event(item):
                item.data['obj'] = publication

            if self.is_publication_done(publication):
                self.finish_work(item, 'mongo')

    def finish_work(self, item, source):
        publication = PubFinderWorker.get_publication(item)

        if self.is_publication_done(publication):

            self.save_to_mongo(publication)

            # todo go through refs
            # foreach ref appendLeft to queue

            if self.is_event(item):
                item.set('state', 'linked')
                self.publish(item)

        else:
            # put it in next queue or stop
            if source in self.source_order:
                self.source_order[source].work_queue.append(item)

    def get_publication_from_mongo(self, doi):
        if not self.collection:
            self.prepare_mongo_connection()
        return self.collection.find_one({"doi": doi})

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
            publication['_id'] = uuid.uuid4().hex
            self.collection.insert_one(publication)
        except pymongo.errors.DuplicateKeyError:
            logging.warning("MongoDB can't save publication %s" % publication['doi'])

    @staticmethod
    def get_publication(item):
        publication = item
        if PubFinderWorker.is_event(item):
            publication = item.data['obj']['data']
        return publication

    @staticmethod
    def is_publication_done(publication):
        if not publication:
            return False

        # they can be empty but must me set, id should be enough?
        if not all(k in publication for k in (
                "type", "doi", "abstract", "pubDate", "publisher", "title", "normalizedTitle", "year",
                "citations", "refs", "authors", "fieldsOfStudy")):
            return False

        return True

    @staticmethod
    def normalize(string):
        # todo numbers, special characters/languages
        return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()

    @staticmethod
    def is_event(item):
        return "obj_id" in item


if __name__ == '__main__':
    logging.warning("start Mongo DB connector")
    time.sleep(10)
    e = PubFinderWorker(1)
    time.sleep(5)
    # connection manager for api with restriction
    # pool up dois (max?) and send them together in repeated loop
    # have just one that needs to work with the others
    e.consume()
