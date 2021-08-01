import re
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
import logging
import requests
import motor.motor_asyncio

from .base_source import BaseSource
from ..pubfinder_worker import PubFinderWorker


# based on crossref
class CrossrefSource(BaseSource):
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

    @lru_cache(maxsize=1000)
    def add_data_to_publication(self, publication):
        response = self.fetch(publication['data']['doi'])
        return self.map(response, publication)

    # fetch response to add data to publication
    def fetch(self, doi):
        r = requests.get(self.base_url + requests.utils.quote(doi))  # check encoding
        if r.status_code == 200:
            json_response = r.json()
            if 'status' in json_response:
                if json_response['status'] == 'ok':
                    if 'message' in json_response:
                        return json_response['message']
        return None

    # map response data to publication
    # todo make sources extend not overwrite
    def map(self, response_data, publication):
        response_data = response_data['data']  # move outside
        if response_data:
            publication['doi'] = response_data['DOI']

            if response_data['type'] in self.publication_type_translation:
                publication['type'] = self.publication_type_translation[response_data['type']]
            else:
                publication['type'] = self.publication_type_translation['unknown']

            # todo this or just published?
            publication['pubDate'] = '{0}-{1}-{2}'.format(str(response_data['published-online']['date-parts'][0][0]),
                                                          str(response_data['published-online']['date-parts'][0][1]),
                                                          str(response_data['published-online']['date-parts'][0][2]))
            publication['year'] = response_data['published-online']['date-parts'][0][0]

            publication['publisher'] = response_data['publisher']
            publication['citationCount'] = response_data['is-referenced-by-count']

            publication['title'] = response_data['title'][0]
            # todo numbers, special characters/languages
            publication['normalizedTitle'] = PubFinderWorker.normalize(publication['title'])

            publication['refs'] = self.map_refs(response_data['reference'])
            publication['authors'] = self.map_author(response_data['author'])
            publication['fieldsOfStudy'] = self.map_fields_of_study(response_data['subject'])

        return publication

    # todo own collection?
    def map_author(self, authors):
        result = []
        for author in authors:
            # todo sequence check, affiliation
            name = author['given'] + ' ' + author['family']
            normalized_name = PubFinderWorker.normalize(name)
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
            normalized_name = PubFinderWorker.normalize(name)
            if not any(d['normalizedName'] == normalized_name for d in result):
                result.append({'name': name, 'normalizedName': normalized_name})
        return result
