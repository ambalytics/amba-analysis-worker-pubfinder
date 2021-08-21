import time
from collections import deque
from multiprocessing.pool import ThreadPool
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from functools import lru_cache
import logging
from event_stream.event import Event


@lru_cache(maxsize=100)
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
          citations {
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
              year
          },
          refs  {
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
              year
          },
          authors {
              id,
              name,
              normalizedName,
              pubCount,
              citationCount,
              rank
          },
          fieldsOfStudy {
              score,
              name,
              normalizedName,
              level,
              rank,
              citationCount
          }
        }
    }

    """)

    # todo  affiliation: Affiliation author
    #   parents: [FieldOfStudy!]
    #   children: [FieldOfStudy!]

    params = {"doi": doi}
    result = amba_client.execute(query, variable_values=params)
    if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
        # todo better way?
        publication = result['publicationsByDoi'][0]
        return publication
    return None


# base source, to be extended for use
class AmbaSource(object):
    tag = 'amba'
    log = 'SourceAmba'
    threads = 4 # todo make client only once
    # gql.transport.exceptions.TransportAlreadyConnected: Transport is already connected

    url = "https://api.ambalytics.cloud/entities"  # todo config
    work_queue = deque()
    work_pool = None
    running = True

    def __init__(self, pubfinder):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.pubfinder = pubfinder

    def worker(self):
        amba_client = self.prepare_amba_connection()

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

                    publication_temp = self.add_data_to_publication(publication, amba_client)

                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder.finish_work(item, self.tag)

    def add_data_to_publication(self, publication, ac):
        amba_publication = self.get_publication_wrapper(publication['doi'], ac)

        # amba is correctly formatted, just return
        if not amba_publication:
            logging.warning('amba failed')
            return publication
        else:
            logging.warning('amba success')
            return amba_publication

    @staticmethod
    def get_publication_wrapper(doi, ac):
        return get_publication_from_amba(doi, ac)

    def prepare_amba_connection(self):
        transport = AIOHTTPTransport(url=self.url)
        return Client(transport=transport, fetch_schema_from_transport=True)
