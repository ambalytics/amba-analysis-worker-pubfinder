import json
import logging
import re
import time
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
# import logging
import requests
from collections import deque
from multiprocessing.pool import ThreadPool
# from base_source import BaseSource
from event_stream.event import Event


@lru_cache(maxsize=100)
def fetch(doi):
    """fetch response to add data to publication
    cache up to 100 since we should not have doi be occurring multiple times

    Arguments:
        doi: the doi to be fetched
    """
    r = requests.get(CrossrefSource.base_url + requests.utils.quote(doi))  # check encoding
    if r.status_code == 200:
        json_response = r.json()
        if 'status' in json_response:
            if json_response['status'] == 'ok':
                if 'message' in json_response:
                    return json_response['message']
    return None


def cleanhtml(raw_html, cleanr):
    return re.sub(cleanr, '', raw_html)


# based on crossref
class CrossrefSource(object):
    base_url = "https://api.crossref.org/works/"
    # crossref types
    # "book-section",
    # "monograph",
    # "report",
    # "peer-review",
    # "book-track",
    # "journal-article",
    # "book-part",
    # "other",
    # "book",
    # "journal-volume",
    # "book-set",
    # "reference-entry",
    # "proceedings-article",
    # "journal",
    # "component",
    # "book-chapter",
    # "proceedings-series",
    # "report-series",
    # "proceedings",
    # "standard",
    # "reference-book",
    # "posted-content",
    # "journal-issue",
    # "dissertation",
    # "dataset",
    # "book-series",
    # "edited-book",
    # "standard-series",
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

    def __init__(self, pubfinder):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.pubfinder = pubfinder
        self.cleanr = re.compile('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});')

    def worker(self):
        while self.running:
            try:
                item = self.work_queue.pop()
            except IndexError:
                time.sleep(0.1)
                # logging.warning(self.log + "sleep worker mongo")
                pass
            else:
                if item:
                    # logging.warning(self.log + " item " + str(item.get_json()))
                    publication = self.pubfinder.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])
                    # logging.warning(self.log + " q " + str(queue))x

                    # todo source stuff
                    publication_temp = self.add_data_to_publication(publication)

                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag

                    source_ids = publication['source_id']
                    # todo check if actually anything was added
                    source_ids.append({'title': 'Crossref', 'url': 'https://www.crossref.org/', 'license': 'TODO'})
                    publication['source_id'] = source_ids

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder.finish_work(item, self.tag)

    def add_data_to_publication(self, publication):
        response = fetch(publication['doi'])
        return self.map(response, publication)

    # map response data to publication
    def map(self, response_data, publication):
        if response_data:
            # publication['doi'] = response_data['DOI']
            # logging.warning(response_data)

            if response_data['type'] in self.publication_type_translation:
                publication['type'] = self.publication_type_translation[response_data['type']]
            else:
                publication['type'] = self.publication_type_translation['unknown']

            if 'published' in response_data and 'date-parts' in response_data['published']:
                if len(response_data['published']['date-parts'][0]) == 3:
                    publication['pubDate'] = '{0}-{1}-{2}'.format(str(response_data['published']['date-parts'][0][0]),
                                                                  str(response_data['published']['date-parts'][0][1]),
                                                                  str(response_data['published']['date-parts'][0][2]))
                publication['year'] = response_data['published']['date-parts'][0][0]

            if 'publisher' in response_data:
                publication['publisher'] = response_data['publisher']

            if 'is-referenced-by-count' in response_data:
                publication['citationCount'] = response_data['is-referenced-by-count']

            if 'title' in response_data:
                publication['title'] = response_data['title'][0]
                publication['normalizedTitle'] = self.pubfinder.normalize(publication['title'])

            if 'reference' in response_data:
                publication['refs'] = self.map_refs(response_data['reference'])

            if 'abstract' in response_data and len(response_data['abstract']) > 50:

                clean_abstract = cleanhtml(response_data['abstract'], self.cleanr)
                # print('original ', response_data['abstract'])
                # print('clean1 ', clean_abstract)
                # use stop words? Background? -> use also for html only for the first word
                clean_abstract = re.sub(r'(\s*)Abstract(\s*)', '', clean_abstract, flags=re.IGNORECASE)
                # print('clean2 ', clean_abstract)
                publication['abstract'] = clean_abstract

            if 'author' in response_data:
                publication['authors'] = self.map_author(response_data['author'])

            if 'subject' in response_data:
                publication['fieldsOfStudy'] = self.map_fields_of_study(response_data['subject'])

        return publication

    def map_author(self, authors):
        result = []
        for author in authors:
            # todo sequence check, affiliation
            name = ''
            if 'given' in author:
                name = author['given'] + ' '
            else:
                logging.warning(self.log + ' no author given ' + json.dumps(author))
            if 'family' in author:
                name = name + author['family']
            else:
                logging.warning(self.log + ' no author family ' + json.dumps(author))

            normalized_name = self.pubfinder.normalize(name)
            result.append({
                'name': name,
                'normalizedName': normalized_name
            })
        return result

    def map_refs(self, refs):
        result = []
        for ref in refs:
            if 'DOI' in ref:
                result.append({'doi': ref['DOI']})
        return result

    def map_fields_of_study(self, fields):
        result = []
        for field in fields:
            name = re.sub(r"[\(\[].*?[\)\]]", "", field)
            normalized_name = self.pubfinder.normalize(name)
            if not any(d['normalizedName'] == normalized_name for d in result):
                result.append({'name': name, 'normalizedName': normalized_name})
        return result
