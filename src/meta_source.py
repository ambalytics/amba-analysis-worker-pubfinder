import json
import re
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
import logging
from multiprocessing.pool import ThreadPool
import requests
from lxml import html
from collections import deque
# from base_source import BaseSource
from event_stream.event import Event

# <meta name="twitter:title" content="Noncommutative integrable systems on... - Regular and Chaotic Dynamics">
# <meta name="citation_journal_title" content="Regular and Chaotic Dynamics">
# <meta name="citation_title" content="Non-commutative integrable systems on $b$-symplectic manifolds">
# <meta name="dc.title" content="Noncommutative integrable systems on b -symplectic manifolds">
# <meta property="og:title" content="Noncommutative integrable systems on b-symplectic manifolds - Regular and Chaotic Dynamics">

# <meta name="dc.date" content="2016-12-18">^
# <meta name="citation_publication_date" content="2016/11">
# <meta name="citation_online_date" content="2016/12/18">
# <meta name="citation_date" content="2016/06/08">
# <meta name="citation_cover_date" content="2016/11/01">

# <meta name="dc.creator" content="Anna Kiesenhofer">
# <meta name="citation_author" content="Anna Kiesenhofer">

# <meta name="citation_reference" content="citation_journal_title=Bull. London Math. Soc.; citation_title=Convexity and Commuting Hamiltonians; citation_author=M. F. Atiyah; citation_volume=14; citation_issue=1; citation_publication_date=1982; citation_pages=1-15; citation_doi=10.1112/blms/14.1.1; citation_id=CR1">+

# <meta property="og:type" content="article">

# <meta name="citation_publisher" content="John Wiley &amp; Sons, Ltd">
# <meta name="dc.publisher" content="Springer">

# <meta name="twitter:description" content="In this paper we study noncommutative integrable systems on b-Poisson manifolds. One important source of examples (and motivation) of such systems comes from considering noncommutative systems on...">
# <meta name="description" content="In this paper we study noncommutative integrable systems on b-Poisson manifolds. One important source of examples (and motivation) of such systems comes fr">
# <meta name="Description" content="Abstract The opioid epidemic in the United States has accelerated during the COVID-19 pandemic. As of 2021, roughly a third of Americans now live in a state with a recreational cannabis law (RCL). ...">
# <meta name="dc.description" content="In this paper we study noncommutative integrable systems on b-Poisson manifolds. One important source of examples (and motivation) of such systems comes from considering noncommutative systems on manifolds with boundary having the right asymptotics on the boundary. In this paper we describe this and other examples and prove an action-angle theorem for noncommutative integrable systems on a b-symplectic manifold in a neighborhood of a Liouville torus inside the critical set of the Poisson structure associated to the b-symplectic structure.">
# <meta property="og:description" content="In this paper we study noncommutative integrable systems on b-Poisson manifolds. One important source of examples (and motivation) of such systems comes from considering noncommutative systems on manifolds with boundary having the right asymptotics on the boundary. In this paper we describe this and other examples and prove an action-angle theorem for noncommutative integrable systems on a b-symplectic manifold in a neighborhood of a Liouville torus inside the critical set of the Poisson structure associated to the b-symplectic structure.">

# using meta tags
from requests import Session


class MetaSource(object):
    base_url = "http://doi.org/"

    # tag must be ordered from better to worst, as soon as a result is found it will stop
    abstract_tags = ['og:description', 'dc.description', 'description', 'twitter:description']
    title_tags = ['og:title', 'dc.title', 'citation_title', 'citation_journal_title', 'twitter:title']
    date_tags = ['citation_cover_date', 'dc.date', 'citation_online_date', 'citation_date']  # which and order
    author_tags = ['citation_author', 'dc.creator']
    publisher_tags = ['dc.publisher', 'citation_publisher']
    type_tag = ['og:type']


    tag = 'meta'
    log = 'SourceMeta'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 1

    def __init__(self, pubfinder):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, (self.work_queue,))
        self.pubfinder = pubfinder

    def worker(self, queue):
        while self.running:
            try:
                item = queue.pop()
            except IndexError:
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

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder.finish_work(item, self.tag)


    def add_data_to_publication(self, publication):
        response = self.fetch(publication['doi'])
        data = self.get_lxml(response)
        return self.map(data, publication)

    # fetch response to add data to publication
    @lru_cache(maxsize=1000)
    def fetch(self, doi):
        session = Session()
        return self.get_response(self.base_url + doi, session)
        # r = requests.get(self.base_url + doi)
        # if r.status_code == 200:
        #     try:
        #         json_response = r.json()
        #     except json.decoder.JSONDecodeError as e:
        #         logging.warning(self.log + " could not json " + doi)
        #     else:
        #         if 'status' in json_response:
        #             if json_response['status'] == 'ok':
        #                 if 'message' in json_response:
        #                     return json_response['message']
        #
        # logging.debug(self.log + " could not resolve: " + doi)
        # return None

    def get_response(self, url, s):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
            'Pragma': 'no-cache'
        }
        s.headers.update(headers)
        return s.get(url)

    # map response data to publication
    def map(self, response_data, publication):
        if response_data and 'abstract' in response_data:
            publication['abstract'] = response_data['abstract']  # todo make only update
        return publication

    def get_lxml(self, page):
        result = {}
        data = {}

        if not page:
            return None

        content = html.fromstring(page.content)
        for meta in content.xpath('//html//head//meta'):
            for name, value in sorted(meta.items()):
                # abstracts
                if value.strip().lower() in self.abstract_tags:
                    result[value.strip().lower()] = meta.get('content')

                # todo other stuff

        logging.debug(self.log + " could not resolve: " + json.dumps(result))

        for key in self.abstract_tags:
            if key in result and not 'abstract' in data:
                data['abstract'] = result[key]

        return data
