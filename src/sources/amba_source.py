from collections import deque
from multiprocessing.pool import ThreadPool
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from functools import lru_cache
import logging

from .base_source import BaseSource


# base source, to be extended for use
class AmbaSource(BaseSource):
    tag = 'amba'

    amba_client = None
    url = "https://api.ambalytics.cloud/entities"  # todo config

    @lru_cache(maxsize=1000)
    def add_data_to_publication(self, publication):
        amba_publication = self.get_publication_from_amba(publication['doi'])

        # amba is correctly formatted, just return
        if not amba_publication:
            return publication
        else:
            return amba_publication


    # todo make these into the packackage to extend from
    def get_publication_from_amba(self, doi):
        if not self.amba_client:
            self.prepare_amba_connection()

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
        result = self.amba_client.execute(query, variable_values=params)
        if 'publicationsByDoi' in result and len(result['publicationsByDoi']) > 0:
            # todo better way?
            publication = result['publicationsByDoi'][0]
            return publication
        return None

    def prepare_amba_connection(self):
        transport = AIOHTTPTransport(url=self.url)
        self.amba_client = Client(transport=transport, fetch_schema_from_transport=True)
