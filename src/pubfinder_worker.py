import logging
import time
import uuid

import requests
import pymongo

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


class PubFinderWorker(EventStreamConsumer, EventStreamProducer):
    state = "unknown"
    relation_type = "discusses"

    log = "PubFinderWorker "
    group_id = "pub-finder-worker"

    mongoClient = False

    config = {
        'mongo_url': "mongodb://mongo_db:27017/",
        'mongo_client': "events",
        'mongo_collection': "publication",
    }

    def __init__(self, id):
        super().__init__(id)
        # todo client for each thread
        self.mongoClient = pymongo.MongoClient(host=self.config['mongo_url'],
                                               serverSelectionTimeoutMS=3000,  # 3 second timeout
                                               username="root",
                                               password="example"
                                               )
        self.db = self.mongoClient[self.config['mongo_client']]
        self.collection = self.db[self.config['mongo_collection']]

    def on_message(self, json_msg):
        logging.warning(self.log + "on message pubfinder worker")

        e = Event()
        e.from_json(json_msg)

        # todo set id as uuid int
        publication = self.get_data_from_crossref(e.data['obj']['data']['doi'])
        if publication:
            logging.warning(self.log + "linked publication crossref")

            cp = publication
            try:
                # save publication to db
                cp['_id'] = uuid.uuid4().hex
                self.collection.insert_one(cp)
            except pymongo.errors.DuplicateKeyError:
                logging.warning("MongoDB collection/state%s, Duplicate found, continue" % json_msg['state'])

            # set event to linked
            e.data['obj']['data'] = publication
            e.data['obj']['data']['doi'] = e.data['obj']['data']['DOI']  # todo
            # event.data['obj']['pid'] = publication['id']
            # event.set('obj_id', )
            e.set('state', 'linked')
            self.publish(e)

    def get_data_from_crossref(self, doi):
        base_url = "https://api.crossref.org/works/"

        r = requests.get(base_url + doi)
        if r.status_code == 200:
            json_response = r.json()
            if 'status' in json_response:
                if json_response['status'] == 'ok':
                    # print(json_response)
                    if 'message' in json_response:
                        return json_response['message']
        return False


if __name__ == '__main__':
    logging.warning("start Mongo DB connector")
    time.sleep(10)
    e = PubFinderWorker(1)
    time.sleep(5)

    e.consume()
