import datetime
import json
import re
import time
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
import logging
from multiprocessing.pool import ThreadPool
import requests
from dateutil.parser import parse
from lxml import html
from collections import deque
from event_stream.event import Event
import pubfinder_worker

# using meta tags
from requests import Session, ConnectTimeout
from urllib3.exceptions import ReadTimeoutError, SSLError, NewConnectionError


@lru_cache(maxsize=10)
def get_response(url, s):
    """get a response from a given url using a given session s, a session can be used for headers,
    this function is cached up to 100 elements

        Arguments:
            url: the url to get
            s: the session to use
    """
    try:
        result = s.get(url, timeout=5)
    except (ConnectionRefusedError, SSLError, ReadTimeoutError, requests.exceptions.TooManyRedirects, ConnectTimeout,
            requests.exceptions.ReadTimeout, NewConnectionError, requests.exceptions.SSLError, ConnectionError,
            TimeoutError, ConnectionResetError):
        logging.warning('Meta Source - Pubfinder')
        s = Session()
        # get the response for the provided url
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'Pragma': 'no-cache'
        }
        s.headers.update(headers)
    else:
        return result
    return None


class MetaSource(object):
    """" this source will try to append data using meta tags in the url of the resolved doi url """
    base_url = "http://doi.org/"

    # tag must be ordered from better to worst, as soon as a result is found it will stop
    abstract_tags = ['dcterms.abstract', 'dcterms.description', 'prism.teaser', 'eprints.abstract', 'og:description',
                     'dc.description', 'description', 'twitter:description', 'citation_abstract']

    title_tags = ['og:title', 'dc.title', 'citation_title', 'dcterms.title', 'citation_journal_title',
                  'dcterms.alternative', 'twitter:title', 'prism.alternateTitle', 'prism.subtitle', 'eprints.title',
                  'bepress_citation_title']

    date_tags = ['citation_cover_date', 'dc.date', 'citation_online_date', 'citation_date', 'citation_publication_date',
                 'dcterms.date', 'dcterms.issued', 'dcterms.created', 'prism.coverDate', 'prism.publicationDate',
                 'bepress_citation_date', 'eprints.date', 'dcterms.date', 'dcterms.dateSubmitted',
                 'dcterms.dateAccepted', 'dcterms.available', 'dcterms.dateCopyrighted', 'prism.creationDate',
                 'prism.dateReceived', 'eprints.datestamp', 'bepress_citation_online_date']  # which and order
    # if no date use year
    year_tag = ['citation_year', 'prism.copyrightYear']

    # more author information
    author_tags = ['citation_author', 'citation_authors', 'dcterms.creator', 'bepress_citation_author',
                   'eprints.creators_name', 'dc.creator']

    publisher_tags = ['dc.publisher', 'citation_publisher', 'dcterms.publisher',
                      'citation_technical_report_institution', 'prism.corporateEntity', 'prism.distributor',
                      'eprints.publisher', 'bepress_citation_publisher']

    type_tag = ['og:type', 'dcterms.type', 'dc.type', 'prism.contentType', 'prism.genre', 'prism.aggregationType',
                'eprints.type', 'citation_dissertation_name']

    keyword_tag = ['citation_keywords', 'dc.subject', 'prism.academicField', 'prism.keyword']

    citation_tag = ['dcterms.bibliographicCitation', 'eprints.citation']

    tag = 'meta'
    log = 'SourceMeta'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4

    def __init__(self, result_deque):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.result_deque = result_deque

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
                    publication = pubfinder_worker.PubFinderWorker.get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])
                    # logging.warning(self.log + " q " + str(queue))x

                    # source stuff
                    publication_temp = self.add_data_to_publication(publication)

                    # only if we have any data we set it
                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag
                    # no meta since link already present
                    source_ids = publication['source_id']
                    # todo check if actually anything was added
                    source_ids.append({
                        'title': 'Meta',
                        'url': 'https://doi.org/' + publication['doi'],
                        'license': 'TODO'
                    })
                    publication['source_id'] = source_ids

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    result = {'item': item, 'tag': self.tag}
                    self.result_deque.append(result)

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
        """map response data and the publication

        Arguments:
            response_data: the response data
            publication: the publication
        """
        if not response_data:
            return None

        if 'abstract' in response_data and \
                ('abstract' not in publication
                 or not pubfinder_worker.PubFinderWorker.valid_abstract(publication['abstract'])):
            abstract = pubfinder_worker.PubFinderWorker.clean_abstract(response_data['abstract'])
            if pubfinder_worker.PubFinderWorker.valid_abstract(abstract):
                publication['abstract'] = abstract

        if response_data and 'title' in response_data and ('title' not in publication or len(publication['title']) < 5):
            publication['title'] = pubfinder_worker.PubFinderWorker.clean_title(response_data['title'])
            publication['normalized_title'] = pubfinder_worker.PubFinderWorker.normalize(publication['title'])

        if pubfinder_worker.PubFinderWorker.should_update('pub_date', response_data, publication):
            pub = MetaSource.format_date(response_data['pub_date'])
            if pub:
                publication['pub_date'] = pub
                publication['year'] = pub.split('-')[0]
        elif pubfinder_worker.PubFinderWorker.should_update('year', response_data, publication):
            publication['year'] = response_data['year']

        if pubfinder_worker.PubFinderWorker.should_update('publisher', response_data, publication):
            publication['publisher'] = response_data['publisher']

        # todo mappings, license?
        # if pubfinder_worker.PubFinderWorker.should_update('type', response_data, publication):
        #     publication['type'] = response_data['type']

        if pubfinder_worker.PubFinderWorker.should_update('authors', response_data, publication):
            publication['authors'] = self.map_object(response_data['authors'])

        if pubfinder_worker.PubFinderWorker.should_update('fields_of_study', response_data, publication):
            publication['fields_of_study'] = self.map_object(response_data['fields_of_study'])

        if response_data and 'citations' in response_data and (
                'citation_count' not in publication or publication['citation_count'] == 0):
            publication['citation_count'] = len(response_data['citations'])

        if pubfinder_worker.PubFinderWorker.should_update('citations', response_data, publication):
            publication['citations'] = response_data['citations']
        return None

    def map_object(self, fields):
        result = []
        for field in fields:
            name = field
            normalized_name = pubfinder_worker.PubFinderWorker.normalize(name)
            if not any(d['normalized_name'] == normalized_name for d in result) and len(normalized_name) < 150:
                result.append({'name': name, 'normalized_name': normalized_name})
        return result

    @staticmethod
    def format_date(date_text):
        """format a date to end up in our preferred format %Y-%m-%d
        possible input formats
        15 Oct 2014
        1969-12-01
        2003-07
        2014-9-11
        July 2021
        Example Output
        2021-02-01
        """
        try:
            date = parse(date_text)
        except ValueError:
            logging.warning("unable to parse date string %s" % date_text)
        else:
            return date.strftime('%Y-%m-%d')

    def get_lxml(self, page):
        """use lxml to parse the page and create a data dict from this page

        Arguments:
            page: the page
        """
        result = {}
        data = {}

        if not page:
            return None

        content = html.fromstring(page.content)
        # go through all meta tags in the head
        for meta in content.xpath('//meta'):
            # iterate through
            for name, value in sorted(meta.items()):
                # abstracts
                if value.strip().lower() in self.abstract_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.title_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.date_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.year_tag:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.publisher_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.type_tag:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.author_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.author_tags:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.keyword_tag:
                    result[value.strip().lower()] = meta.get('content')

                if value.strip().lower() in self.citation_tag:
                    result[value.strip().lower()] = meta.get('content')

        # logging.debug(self.log + " could not resolve: " + json.dumps(result))

        for key in self.abstract_tags:
            if key in result:
                abstract = pubfinder_worker.PubFinderWorker.clean_abstract(result[key])
                if 'abstract' not in data or len(abstract) > len(data['abstract']):
                    data['abstract'] = result[key]

        for key in self.title_tags:
            if 'title' not in data:
                if key in result:
                    data['title'] = result[key]

        for key in self.date_tags:
            if 'pub_date' not in data:
                if key in result:
                    dateTemp = result[key].replace("/", "-")
                    data['pub_date'] = dateTemp

        for key in self.year_tag:
            if 'year' not in data:
                if key in result:
                    data['year'] = result[key]

        for key in self.publisher_tags:
            if 'publisher' not in data:
                if key in result:
                    data['publisher'] = result[key]

        for key in self.type_tag:
            if 'type' not in data:
                if key in result:
                    data['type'] = result[key]

        authors = []
        for key in self.author_tags:
            if key in result and len(result[key].strip()) > 1:
                authors.append(result[key].strip())
        data['authors'] = authors

        keywords = []
        for key in self.keyword_tag:
            if key in result:
                keywords.append(result[key])
        data['fields_of_study'] = keywords

        citations = []
        for key in self.citation_tag:
            if key in result:
                citations.append(result[key])
        data['citations'] = citations

        return data
