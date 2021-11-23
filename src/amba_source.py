import time
from collections import deque
from multiprocessing.pool import ThreadPool
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from functools import lru_cache
import logging
from event_stream.event import Event
from .pubfinder_worker import PubFinderWorker

def get_publication_from_amba(doi, amba_client):
    query = gql(
        """
        query getPublication($doi: [String!]!) {
         publicationsByDoi(doi: $doi) {
          id,
          type,
          doi,
          abstract,
          pubDate,
          publisher,
          rank,
          citationCount,
          title,
          normalizedTitle,
          year,
          authors {
              name,
              normalizedName,
          },
          fieldsOfStudy {
              name,
              normalizedName,
              level,
          }
        } 
    }
    """)

    params = {"doi": doi}
    logging.warning(query)
    logging.warning(params)
    result = amba_client.execute(query, variable_values=params)
    if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
        publication = result['publicationsByDoi'][0]
        publication['pub_date'] = publication['pubDate']
        publication['citation_count'] = publication['citationCount']
        publication['normalized_title'] = publication['normalizedTitle']

        for a in publication['authors']:
            a['normalized_name'] = a['normalizedName']

        for f in publication['fieldsOfStudy']:
            f['normalized_name'] = f['normalizedName']
        publication['field_of_study'] = publication['fieldsOfStudy']


# base source, to be extended for use
class AmbaSource(object):
    tag = 'amba'
    log = 'SourceAmba'
    threads = 1

    url = "https://api.ambalytics.cloud/entities"
    work_queue = deque()
    work_pool = None
    running = True

    def __init__(self, result_deque):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.result_deque = result_deque

    def worker(self):
        """
        worker function run in thread pool adding publication data
        """
        amba_client = self.prepare_amba_connection()

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

                    publication_temp = self.add_data_to_publication(publication, amba_client)

                    if publication_temp:
                        publication = publication_temp

                        publication['source'] = self.tag
                        source_ids = []
                        if 'source_id' in publication:
                            source_ids = publication['source_id']
                        source_ids.append({
                            'title': 'Ambalytics',
                            'url': 'https://ambalytics.com',
                            'license': 'MIT'
                        })
                        publication['source_id'] = source_ids

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    result = {'item': item, 'tag': self.tag}
                    self.result_deque.append(result)

    def add_data_to_publication(self, publication, ac):
        """ add data to publication """
        amba_publication = self.get_publication_wrapper(publication['doi'], ac)
        if not amba_publication:
            return publication
        else:
            return amba_publication

    @staticmethod
    def get_publication_wrapper(doi, ac):
        """just a wrapper"""
        return get_publication_from_amba(doi, ac)

    def prepare_amba_connection(self):
        """prepare the connection to amba"""
        transport = AIOHTTPTransport(url=self.url)
        return Client(transport=transport, fetch_schema_from_transport=True)
