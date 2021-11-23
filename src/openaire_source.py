import logging
import threading
import time
from functools import lru_cache
import requests
from collections import deque
from multiprocessing.pool import ThreadPool
from multiprocessing import Value
from event_stream.event import Event
from .pubfinder_worker import PubFinderWorker
from lxml import html


@lru_cache(maxsize=10)
def fetch(doi):
    """fetch response to add data to publication
    cache up to 100 since we should not have doi be occurring multiple times

    Arguments:
        doi: the doi to be fetched
    """
    return requests.get(OpenAireSource.base_url + requests.utils.quote(doi))  # check encoding


def reset_api_limit(v, time_delta):
    """ timer function resetting the shared value used to keep the limits """
    logging.warning('reset openaire api limit ' + str(v.value))
    with v.get_lock():
        v.value = 0
    threading.Timer(time_delta, reset_api_limit, args=[v, time_delta]).start()


# based on crossref
class OpenAireSource(object):
    base_url = "https://api.openaire.eu/search/publications?doi="

    tag = 'openaire'
    log = 'SourceOpenAIRE'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4
    api_limit = 3550
    api_time = 3600

    tags = ['main title', 'creator', 'relevantdate', 'dateofacceptance', 'description', 'publisher']

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
                    publication = PubFinderWorker.get_publication(item)
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
        data = self.get_lxml(response)
        return self.map(data, publication)

    def map(self, response_data, publication):
        """ map a xml response to the internal data structure """
        added_data = False
        if response_data:

            if PubFinderWorker.should_update('title', response_data, publication):
                publication['title'] = PubFinderWorker.clean_title(response_data['title'])
                publication['normalized_title'] = PubFinderWorker.normalize(publication['title'])
                added_data = True

            if PubFinderWorker.should_update('year', response_data, publication):
                publication['year'] = PubFinderWorker.clean_title(response_data['year'])
                added_data = True

            if PubFinderWorker.should_update('pub_date', response_data, publication):
                publication['pub_date'] = PubFinderWorker.clean_title(response_data['pub_date'])
                added_data = True

            if PubFinderWorker.should_update('publisher', response_data, publication):
                publication['publisher'] = response_data['publisher']
                added_data = True

            if 'abstract' in response_data and \
                    ('abstract' not in publication
                     or not PubFinderWorker.valid_abstract(publication['abstract'])):
                abstract = PubFinderWorker.clean_abstract(response_data['abstract'])
                if PubFinderWorker.valid_abstract(abstract):
                    publication['abstract'] = abstract
                    added_data = True

            if PubFinderWorker.should_update('authors', response_data, publication):
                publication['authors'] = response_data['authors']
                added_data = True

            if PubFinderWorker.should_update('fields_of_study', response_data, publication):
                # logging.warning("response_data['fields_of_study']")
                # logging.warning(response_data['fields_of_study'])
                publication['fields_of_study'] = self.map_fields_of_study(response_data['fields_of_study'])
                added_data = True

        if added_data:
            source_ids = publication['source_id']
            source_ids.append(
                {'title': 'OpenAIRE', 'url': 'https://develop.openaire.eu/overview.html', 'license': 'TODO'})
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

    def map_fields_of_study(self, fields):
        """ map fields of study  """
        result = []
        for field in fields:
            name = field
            normalized_name = PubFinderWorker.normalize(name)
            if not any(d['normalized_name'] == normalized_name for d in result):
                result.append({'name': name, 'normalized_name': normalized_name})
        return result

    def get_lxml(self, page):
        """use lxml to parse the page and create a data dict from this page

        Arguments:
            page: the page
        """
        result = {}

        if not page:
            return None

        content = html.fromstring(page.content)

        d = content.xpath('//description')
        if len(d) > 0:
            description = d[0].text
            result['abstract'] = description

        pu = content.xpath('//publisher')
        if len(pu) > 0:
            publisher = pu[0].text
            result['publisher'] = publisher

        t = content.xpath("//title[@classid='main title']")
        if len(t) > 0:
            title = t[0].text
            result['title'] = title

        p = content.xpath(
            "/response/results/result/metadata/*[name()='oaf:entity']/*[name()='oaf:result']/dateofacceptance")
        if len(p) > 0:
            pub_date = p[0].text
            result['pub_date'] = pub_date
            result['year'] = pub_date.split('-')[0]

        a = content.xpath("/response/results/result/metadata/*[name()='oaf:entity']/*[name()='oaf:result']/creator")
        if len(a) > 0:
            authors = []
            for author in a:
                authors.append(author.text)
            result['authors'] = authors

        f = content.xpath(
            "/response/results/result/metadata/*[name()='oaf:entity']/*[name()='oaf:result']/subject[not(@trust)]")
        if len(f) > 0:
            fos = []
            for fs in f:
                fos.append(fs.text)
            result['fields_of_study'] = fos

        return result
