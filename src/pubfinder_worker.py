import logging
import time

import requests
from multiprocessing.context import Process

from event_stream.event_stream_consumer import EventStreamConsumer
from event_stream.event_stream_producer import EventStreamProducer
from event_stream.event import Event


class PubFinderWorker(EventStreamConsumer, EventStreamProducer):
    state = "unknown"
    relation_type = "discusses"

    log = "PubFinderWorker "
    group_id = "pub-finder-worker"


    def on_message(self, json_msg):
        logging.warning(self.log + "on message pubfinder worker")

        e = Event()
        e.from_json(json_msg)

        publication = self.get_data_from_crossref(e.data['obj']['data']['doi'])
        if publication:
            logging.warning(self.log + "linked publication crossref")
            e.data['obj']['data'] = publication
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
    time.sleep(15)

    e = PubFinderWorker(1)
    e.consume()
