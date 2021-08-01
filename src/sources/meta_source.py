import re
from functools import lru_cache
# from gql import gql, Client
# from gql.transport.aiohttp import AIOHTTPTransport
import logging
import requests
from lxml import html

from .base_source import BaseSource
from ..pubfinder_worker import PubFinderWorker


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
class MetaSource(BaseSource):
    base_url = "http://doi.org/"

    # tag must be ordered from better to worst, as soon as a result is found it will stop
    abstract_tags = ['og:description', 'dc.description', 'description', 'twitter:description']
    title_tags = ['og:title', 'dc.title', 'citation_title', 'citation_journal_title', 'twitter:title']
    date_tags = ['citation_cover_date', 'dc.date', 'citation_online_date', 'citation_date']  # which and order
    author_tags = ['citation_author', 'dc.creator']
    publisher_tags = ['dc.publisher', 'citation_publisher']
    type_tag = ['og:type']

    @lru_cache(maxsize=1000)
    def add_data_to_publication(self, publication):
        response = self.fetch(publication['data']['doi'])
        data = self.get_lxml(response)
        return self.map(data, publication)

    # fetch response to add data to publication
    def fetch(self, doi):
        r = requests.get(self.base_url + doi)
        if r.status_code == 200:
            json_response = r.json()
            if 'status' in json_response:
                if json_response['status'] == 'ok':
                    if 'message' in json_response:
                        return json_response['message']
        return None

    # map response data to publication
    def map(self, response_data, publication):
        if response_data:
            publication['abstract'] = response_data['abstract']  # todo make only update
        return publication

    def get_lxml(self, page):
        content = html.fromstring(page.content)
        result = {}
        data = {}
        for meta in content.xpath('//html//head//meta'):
            for name, value in sorted(meta.items()):
                # abstracts
                if value.strip().lower() in self.abstract_tags:
                    result[value.strip().lower()] = meta.get('content')

                # todo other stuff
        for key in self.abstract_tags:
            if key in result and not data['abstract']:
                data['abstract'] = result[key]

        return result
