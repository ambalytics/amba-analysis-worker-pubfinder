import json
import logging
import threading
import time
from functools import lru_cache
import requests
from collections import deque
from multiprocessing.pool import ThreadPool
from multiprocessing import Value
from event_stream.event import Event
from pubfinder_helper import PubFinderHelper


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
    api_limit_thread = threading.Timer(time_delta, reset_api_limit, args=[v, time_delta]).start()
    api_limit_thread.daemon = True
    api_limit_thread.start()


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
        api_limit_thread = threading.Timer(self.api_time, reset_api_limit, args=[self.fetched_counter, self.api_time])
        api_limit_thread.daemon = True
        api_limit_thread.start()
        self.api_reset_timestamp = int(time.time())

    def worker(self):
        """ main work function, fetch items and add data """
        while self.running:
            try:
                item = self.work_queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                if item:
                    publication = PubFinderHelper.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])

                    publication_temp = self.add_data_to_publication(publication)

                    if publication_temp:
                        publication = publication_temp

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    result = {'item': item, 'tag': self.tag}
                    self.result_deque.append(result)

    def add_data_to_publication(self, publication):
        """ add data to a given publication using the doi to fetch a response and map the data """
        response = self.api_limit_watcher(publication['doi'])
        return self.map(response, publication)

    def map_fields_of_study(self, fields):
        """ map fields of study  """
        result = []
        for field in fields:
            name = field
            normalized_name = PubFinderHelper.normalize(name)
            if not any(d['normalized_name'] == normalized_name for d in result):
                result.append({'name': name, 'normalized_name': normalized_name})
        return result

    def map(self, response_data, publication):
        """ map a xml response to the internal data structure """
        added_data = False
        if response_data:

            if PubFinderHelper.should_update('title', response_data, publication):
                publication['title'] = PubFinderHelper.clean_title(response_data['title'])
                publication['normalized_title'] = PubFinderHelper.normalize(publication['title'])
                added_data = True

            if PubFinderHelper.should_update('year', response_data, publication):
                publication['year'] = response_data['year']
                added_data = True

            if 'venue' in response_data and 'publisher' not in publication:
                publication['publisher'] = response_data['venue']
                added_data = True

            if 'numCitedBy' in response_data and (
                    'citation_count' not in publication or publication['citation_count'] == 0):
                publication['citation_count'] = response_data['numCitedBy']
                added_data = True

            if PubFinderHelper.should_update('authors', response_data, publication):
                publication['authors'] = self.map_author(response_data['authors'])
                added_data = True

            if 'abstract' in response_data and (
                    'abstract' not in publication
                    or not PubFinderHelper.valid_abstract(publication['abstract'])):
                abstract = PubFinderHelper.clean_abstract(response_data['abstract'])
                if PubFinderHelper.valid_abstract(abstract):
                    publication['abstract'] = abstract
                    added_data = True

            if PubFinderHelper.should_update('fields_of_study', response_data, publication):
                publication['fields_of_study'] = self.map_fields_of_study(response_data['fields_of_study'])
                added_data = True

        if added_data:
            source_ids = publication['source_id']
            source_ids.append(
                {'title': 'SemanticScholar', 'url': 'https://www.semanticscholar.org?utm_source=api',
                 'license': 'TODO'})
            publication['source_id'] = source_ids

            return publication

    def api_limit_watcher(self, doi):
        """ ensure api limits are kept and if the limit is reached wait for reset """
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
        """ amp authors """
        result = []
        for author in authors:
            if 'name' in author:
                name = author['name']
                normalized_name = PubFinderHelper.normalize(name)
                result.append({
                    'name': name,
                    'normalized_name': normalized_name
                })
            else:
                logging.warning(self.log + ' no author name ' + json.dumps(author))
        return result
