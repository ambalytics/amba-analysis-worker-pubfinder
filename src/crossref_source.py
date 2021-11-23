import json
import logging
import re
import time
from functools import lru_cache
import requests
from collections import deque
from multiprocessing.pool import ThreadPool
from event_stream.event import Event
from .pubfinder_worker import PubFinderWorker


@lru_cache(maxsize=10)
def fetch(doi):
    """fetch response to add data to publication
    cache up to 100 since we should not have doi be occurring multiple times

    Arguments:
        doi: the doi to be fetched
    """
    r = requests.get(CrossrefSource.base_url + requests.utils.quote(doi) + '?mailto=lukas.jesche.se@gmail.com')
    if r.status_code == 200:
        json_response = r.json()
        if 'status' in json_response:
            if json_response['status'] == 'ok':
                if 'message' in json_response:
                    return json_response['message']
    return None


class CrossrefSource(object):
    base_url = "https://api.crossref.org/works/"

    publication_type_translation = {
        'unknown': 'UNKNOWN',
        'book': 'BOOK',
        'book-chapter': 'BOOK_CHAPTER',
        'proceedings-article': 'CONFERENCE_PAPER',
        'dataset': 'DATASET',
        'journal-article': 'JOURNAL_ARTICLE',
        'patent': 'PATENT',  # doesn't exist
        'repository': 'REPOSITORY',  # doesn't exist
        'reference-book': 'BOOK_REFERENCE_ENTRY'  # or reference-entry
    }

    tag = 'crossref'
    log = 'SourceCrossref'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4

    def __init__(self, result_deque):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.result_deque = result_deque
        self.cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

    def worker(self):
        """
        worker function run in thread pool adding publication data
        """
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
        """
        fetch and map to add data
        """
        response = fetch(publication['doi'])
        return self.map(response, publication)

    def map(self, response_data, publication):
        """
        map given data from a response to a publication object
        """
        added_data = False
        if response_data:

            if 'type' not in publication:
                if response_data['type'] in self.publication_type_translation:
                    publication['type'] = self.publication_type_translation[response_data['type']]
                else:
                    publication['type'] = self.publication_type_translation['unknown']
                added_data = True

            if 'published' in response_data and 'date-parts' in response_data[
                'published'] and 'pub_date' not in publication:
                if len(response_data['published']['date-parts'][0]) == 3:
                    publication['pub_date'] = '{0}-{1}-{2}'.format(str(response_data['published']['date-parts'][0][0]),
                                                                   str(response_data['published']['date-parts'][0][1]),
                                                                   str(response_data['published']['date-parts'][0][2]))
                publication['year'] = response_data['published']['date-parts'][0][0]

            if PubFinderWorker.should_update('publisher', response_data, publication):
                publication['publisher'] = response_data['publisher']

            if 'is-referenced-by-count' in response_data and (
                    'citation_count' not in publication or publication['citation_count'] == 0):
                publication['citation_count'] = response_data['is-referenced-by-count']

            if PubFinderWorker.should_update('title', response_data, publication):
                if len(response_data['title']) > 0:
                    publication['title'] = PubFinderWorker.clean_title(response_data['title'][0])
                    publication['normalized_title'] = PubFinderWorker.normalize(publication['title'])

            if 'reference' in response_data and 'refs' not in publication:
                publication['refs'] = self.map_refs(response_data['reference'])
                added_data = True

            if 'abstract' in response_data and (
                    'abstract' not in publication or not PubFinderWorker.valid_abstract(
                publication['abstract'])):
                abstract = PubFinderWorker.clean_abstract(response_data['abstract'])
                if PubFinderWorker.valid_abstract(abstract):
                    publication['abstract'] = abstract
                    added_data = True

            if 'author' in response_data and 'authors' not in publication:
                publication['authors'] = self.map_author(response_data['author'])
                added_data = True

            if 'subject' in response_data and 'fields_of_study' not in publication:
                publication['fields_of_study'] = self.map_fields_of_study(response_data['subject'])
                added_data = True

            # content-version
            if 'license' in response_data:
                publication['license'] = response_data['license'][0]['URL']
                added_data = True

        if added_data:
            source_ids = publication['source_id']
            source_ids.append({'title': 'Crossref', 'url': 'https://www.crossref.org/', 'license': 'TODO'})
            publication['source_id'] = source_ids

        return publication

    def map_author(self, authors):
        """
        map authors and add normalized
        """
        result = []
        for author in authors:
            name = ''
            if 'given' in author:
                name = author['given'] + ' '
            else:
                logging.warning(self.log + ' no author given ' + json.dumps(author))
            if 'family' in author:
                name = name + author['family']
            else:
                logging.warning(self.log + ' no author family ' + json.dumps(author))

            if len(name.strip()) > 1:
                normalized_name = PubFinderWorker.normalize(name)
                result.append({
                    'name': name,
                    'normalized_name': normalized_name
                })
        return result

    def map_refs(self, refs):
        """
        map references
        """
        result = []
        for ref in refs:
            if 'DOI' in ref:
                result.append({'doi': ref['DOI']})
        return result

    def map_fields_of_study(self, fields):
        """
        map field of study and add normalized
        """
        result = []
        for field in fields:
            name = re.sub(r"[\(\[].*?[\)\]]", "", field)
            normalized_name = PubFinderWorker.normalize(name)
            if not any(d['normalized_name'] == normalized_name for d in result):
                result.append({'name': name, 'normalized_name': normalized_name})
        return result
