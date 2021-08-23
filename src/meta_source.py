import json
import re
import time
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
import logging
from multiprocessing.pool import ThreadPool
import requests
from lxml import html
from collections import deque
from event_stream.event import Event

# using meta tags
from requests import Session

@lru_cache(maxsize=100)
def get_response(url, s):
    """get a response from a given url using a given session s, a session can be used for headers,
    this function is cached up to 100 elements

        Arguments:
            url: the url to get
            s: the session to use
    """
    return s.get(url)


class MetaSource(object):
    """" this source will try to append data using meta tags in the url of the resolved doi url """
    base_url = "http://doi.org/"

    # tag must be ordered from better to worst, as soon as a result is found it will stop
    abstract_tags = ['dcterms.abstract', 'dcterms.description', 'prism.teaser', 'eprints.abstract', 'og:description', 'dc.description', 'description', 'twitter:description']

    title_tags = ['og:title', 'dc.title', 'citation_title', 'dcterms.title', 'citation_journal_title', 'dcterms.alternative', 'twitter:title', 'prism.alternateTitle', 'prism.subtitle', 'eprints.title', 'bepress_citation_title']

    date_tags = ['citation_cover_date', 'dc.date', 'citation_online_date', 'citation_date', 'citation_publication_date', 'dcterms.date', 'dcterms.issued', 'dcterms.created', 'prism.coverDate', 'prism.publicationDate', 'bepress_citation_date', 'eprints.date']  # which and order
    # if no date use year
    year_tag = ['citation_year', 'prism.copyrightYear']

    # more author information
    author_tags = ['citation_author', 'citation_authors', 'dcterms.creator', 'bepress_citation_author', 'eprints.creators_name', 'dc.creator']

    publisher_tags = ['dc.publisher', 'citation_publisher', 'dcterms.publisher', 'citation_technical_report_institution', 'prism.corporateEntity', 'prism.distributor', 'eprints.publisher', 'bepress_citation_publisher']

    type_tag = ['og:type', 'dcterms.type', 'dc.type', 'prism.contentType', 'prism.genre', 'prism.aggregationType', 'eprints.type', 'citation_dissertation_name']

    keyword_tag = ['citation_keywords', 'dc.subject', 'prism.academicField', 'prism.keyword']

    citation_tag = ['dcterms.bibliographicCitation', 'eprints.citation']

    tag = 'meta'
    log = 'SourceMeta'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4

    def __init__(self, pubfinder):
        """setup a ThreadPool, don't need the cpu since requests are slow and we wan't to share data

            Arguments:
                pubfinder: the main process where we get data from and to
        """
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.pubfinder = pubfinder

    def worker(self):
        """the worker thread function will ensure that the publications in the queue are all processed,
        it will sleep for 0.1s if no item is in the queue to reduce cpu usage
        """
        while self.running:
            try:
                item = self.work_queue.pop()
            except IndexError:
                time.sleep(0.1)
                pass
            else:
                if item:
                    # logging.warning(self.log + " item " + str(item.get_json()))
                    publication = self.pubfinder.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])
                    # logging.warning(self.log + " q " + str(queue))x

                    # source stuff
                    publication_temp = self.add_data_to_publication(publication)

                    # only if we have any data we set it
                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag
                    # no meta since link already present

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder.finish_work(item, self.tag)


    def add_data_to_publication(self, publication):
        """add data to a given publication, only append, no overwriting if a value is already set

        Arguments:
            publication: the publication to add data too
        """
        response = self.fetch(publication['doi'])
        data = self.get_lxml(response)
        return self.map(data, publication)

    # fetch response to add data to publication
    def fetch(self, doi):
        """fetch data from the source using its doi

        Arguments:
            doi: the doi of the publication
        """
        session = Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'Pragma': 'no-cache'
        }
        session.headers.update(headers)
        return get_response(self.base_url + doi, session)

    # map response data to publication
    def map(self, response_data, publication):
        # min length to use 50
        if response_data and 'abstract' in response_data and len(publication['abstract']) < 50:
            publication['abstract'] = response_data['abstract']

        if response_data and 'abstract' in response_data and len(publication['abstract']) < 50:
            publication['abstract'] = response_data['abstract']
        return None

    def get_lxml(self, page):
        result = {}
        data = {}

        if not page:
            return None

        content = html.fromstring(page.content)
        # go through all meta tags in the head
        for meta in content.xpath('//html//head//meta'):
            # iterate through
            for name, value in sorted(meta.items()):
                # abstracts
                if value.strip().lower() in self.abstract_tags:
                    result[value.strip().lower()] = meta.get('content')

                # todo other stuff

        logging.debug(self.log + " could not resolve: " + json.dumps(result))


        for key in self.abstract_tags:
            if 'abstract' not in data:
                if key in result:
                    data['abstract'] = result[key]

        if 'title' not in data:
            for key in self.title_tags:
                if key in result:
                    data['title'] = result[key]

        if 'pubDate' not in data:
            for key in self.date_tags:
                if key in result:
                    data['pubDate'] = result[key]

        if 'year' not in data:
            for key in self.year_tag:
                if key in result:
                    data['date'] = result[key]

        if 'publisher' not in data:
            for key in self.publisher_tags:
                if key in result:
                    data['publisher'] = result[key]

        if 'type' not in data:
            for key in self.type_tag:
                if key in result:
                    data['type'] = result[key]

        if 'authors' not in data:
            authors = []
            for key in self.author_tags:
                if key in result:
                    authors.append(result[key])
            data['authors'] = authors

        if 'fieldsOfStudy' not in data:
            keywords = []
            for key in self.keyword_tag:
                if key in result:
                    keywords.append(result[key])
            data['fieldsOfStudy'] = keywords

        if 'citations' not in data:
            citations = []
            for key in self.citation_tag:
                if key in result:
                    citations.append(result[key])
            data['citations'] = citations

        return data
