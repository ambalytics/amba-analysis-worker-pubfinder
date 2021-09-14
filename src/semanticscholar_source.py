import json
import logging
import re
import threading
import time
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
# import logging
import requests
from collections import deque
from multiprocessing.pool import ThreadPool
from multiprocessing import Value
# from base_source import BaseSource
from event_stream.event import Event
import pubfinder_worker


@lru_cache(maxsize=10)
def fetch(doi):
    """fetch response to add data to publication
    cache up to 100 since we should not have doi be occurring multiple times

    Arguments:
        doi: the doi to be fetched
    """
    r = requests.get(SemanticScholarSource.base_url + requests.utils.quote(doi))  # check encoding
    if r.status_code == 200:
        json_response = r.json()
        if 'error' not in json_response:
            return json_response
    return None


def reset_api_limit(v, time_delta):
    logging.warning('reset semanticscholar api limit ' + str(v.value))
    with v.get_lock():
        v.value = 0
    threading.Timer(time_delta, reset_api_limit, args=[v, time_delta]).start()


class SemanticScholarSource(object):
    base_url = "https://api.semanticscholar.org/v1/paper/"

    tag = 'semanticscholar'
    log = 'SemanticScholar'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4
    api_limit = 95
    api_time = 300

    def __init__(self, result_deque):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.result_deque = result_deque

        self.fetched_counter = Value('i', 0)
        threading.Timer(self.api_time, reset_api_limit, args=[self.fetched_counter, self.api_time]).start()
        self.api_reset_timestamp = int(time.time())

    def worker(self):
        while self.running:
            try:
                item = self.work_queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                if item:
                    publication = pubfinder_worker.PubFinderWorker.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])

                    publication_temp = self.add_data_to_publication(publication)

                    if publication_temp:
                        publication = publication_temp

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    result = {'item': item, 'tag': self.tag}
                    self.result_deque.append(result)

    def add_data_to_publication(self, publication):
        response = self.api_limit_watcher(publication['doi'])
        return self.map(response, publication)

    def map_fields_of_study(self, fields):
        result = []
        for field in fields:
            name = field
            normalized_name = pubfinder_worker.PubFinderWorker.normalize(name)
            if not any(d['normalizedName'] == normalized_name for d in result):
                result.append({'name': name, 'normalizedName': normalized_name})
        return result

    # map response data to publication
    def map(self, response_data, publication):
        added_data = False
        if response_data:

            if pubfinder_worker.PubFinderWorker.should_update('title', response_data, publication):
                publication['title'] = pubfinder_worker.PubFinderWorker.clean_title(response_data['title'])
                publication['normalizedTitle'] = pubfinder_worker.PubFinderWorker.normalize(publication['title'])
                added_data = True

            if pubfinder_worker.PubFinderWorker.should_update('year', response_data, publication):
                publication['year'] = response_data['year']
                added_data = True

            if 'venue' in response_data and 'publisher' not in publication:
                publication['publisher'] = response_data['venue']
                added_data = True

            if 'numCitedBy' in response_data and 'citationCount' not in publication:
                publication['citationCount'] = response_data['numCitedBy']
                added_data = True

            if pubfinder_worker.PubFinderWorker.should_update('authors', response_data, publication):
                publication['authors'] = self.map_author(response_data['authors'])
                added_data = True

            # todo + citations
            # if 'reference' in response_data and 'refs' not in publication:
            #     publication['refs'] = self.map_refs(response_data['reference'])
            #     added_data = True

            if 'abstract' in response_data and (
                    'abstract' not in publication
                    or not pubfinder_worker.PubFinderWorker.valid_abstract(publication['abstract'])):
                abstract = pubfinder_worker.PubFinderWorker.clean_abstract(response_data['abstract'])
                if pubfinder_worker.PubFinderWorker.valid_abstract(abstract):
                    publication['abstract'] = abstract
                    added_data = True

            if pubfinder_worker.PubFinderWorker.should_update('fieldsOfStudy', response_data, publication):
                # logging.warning("response_data['fieldsOfStudy']")
                # logging.warning(response_data['fieldsOfStudy'])
                # if len(response_data['fieldsOfStudy']) == 1:
                #     publication['fieldsOfStudy'] = [response_data['fieldsOfStudy']]
                # else:
                publication['fieldsOfStudy'] = self.map_fields_of_study(response_data['fieldsOfStudy'])
                added_data = True

        if added_data:
            source_ids = publication['source_id']
            source_ids.append(
                {'title': 'SemanticScholar', 'url': 'https://api.semanticscholar.org', 'license': 'TODO'})
            publication['source_id'] = source_ids

            return publication

    def api_limit_watcher(self, doi):
        if self.fetched_counter.value < self.api_limit:
            with self.fetched_counter.get_lock():
                self.fetched_counter.value += 1
            return fetch(doi)
        else:
            wt = self.api_time - (int(time.time()) - self.api_reset_timestamp) % self.api_time + 1
            logging.warning(self.log + ' api limit reached, wait ' + str(wt))
            time.sleep(wt)
            self.api_limit_watcher(doi)

    def map_author(self, authors):
        result = []
        for author in authors:
            if 'name' in author:
                name = author['name']
                normalized_name = pubfinder_worker.PubFinderWorker.normalize(name)
                result.append({
                    'name': name,
                    'normalizedName': normalized_name
                })
            else:
                logging.warning(self.log + ' no author name ' + json.dumps(author))
        return result
