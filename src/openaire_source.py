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
from lxml import html


# fetch response to add data to publication
@lru_cache(maxsize=100)
def fetch(doi):
    r = requests.get(OpenAireSource.base_url + requests.utils.quote(doi))  # check encoding
    if r.status_code == 200:
        json_response = r.json()
        if 'status' in json_response:
            if json_response['status'] == 'ok':
                if 'message' in json_response:
                    return json_response['message']
    return None


# based on crossref
class OpenAireSource(object):
    base_url = "https://api.openaire.eu/search/publications?doi="

    tag = 'openaire'
    log = 'SourceOpenAIRE'
    work_queue = deque()
    work_pool = None
    running = True
    threads = 4

    tags = ['main title', 'creator', 'relevantdate', 'dateofacceptance', 'description', 'publisher']

    def __init__(self, pubfinder_get_publication, pubfinder_finish_work):
        if not self.work_pool:
            self.work_pool = ThreadPool(self.threads, self.worker, ())
        self.pubfinder_get_publication = pubfinder_get_publication
        self.pubfinder_finish_work = pubfinder_finish_work

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
                    publication = self.pubfinder_get_publication(item)
                    logging.warning(self.log + " work on item " + publication['doi'])
                    # logging.warning(self.log + " q " + str(queue))x

                    # todo source stuff
                    publication_temp = self.add_data_to_publication(publication)

                    if publication_temp:
                        publication = publication_temp

                    publication['source'] = self.tag

                    source_ids = publication['obj']['source_id']
                    # todo check if actually anything was added
                    source_ids.append({
                        'title': 'OpenAIRE', 'url': 'https://develop.openaire.eu/overview.html'
                    })
                    publication['obj']['source_id'] = source_ids

                    if type(item) is Event:
                        item.data['obj']['data'] = publication

                    self.pubfinder_finish_work(item, self.tag)

    def add_data_to_publication(self, publication):
        response = fetch(publication['doi'])
        return self.map(response, publication)

    # map response data to publication
    # todo make sources extend not overwrite
    def map(self, response_data, publication):
        if response_data:
            # publication['doi'] = response_data['DOI']

            if response_data['type'] in self.publication_type_translation:
                publication['type'] = self.publication_type_translation[response_data['type']]
            else:
                publication['type'] = self.publication_type_translation['unknown']

            if 'published' in response_data and 'date-parts' in response_data['published']:
                if len(response_data['published']['date-parts']) == 3:
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

            if 'author' in response_data:
                publication['authors'] = self.map_author(response_data['author'])

            if 'subject' in response_data:
                publication['fieldsOfStudy'] = self.map_fields_of_study(response_data['subject'])

        return publication

    # todo own collection?
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
        for meta in content.xpath('//html//head//meta'):
            # iterate through
            for name, value in sorted(meta.items()):
                # abstracts
                if value.strip().lower() in self.tags:
                    result[value.strip().lower()] = meta.get('content')

        # logging.debug(self.log + " could not resolve: " + json.dumps(result))
        # /response/results/result/metadata/oaf:entity/oaf:result/description
        if 'abstract' not in data:
            if 'description' in result:
                data['abstract'] = result['description']
        # <title classid="main title" classname="main title" schemeid="dnet:dataCite_title" schemename="dnet:dataCite_title" inferred="false" provenanceaction="sysimport:crosswalk:repository" trust="0.9">The origin of extracellular fields and currents — EEG, ECoG, LFP and spikes</title>
        # /response/results/result/metadata/oaf:entity/oaf:result/title[2]
        if 'title' not in data:
            for key in self.title_tags:
                if key in result:
                    data['title'] = result[key]
        # /response/results/result/metadata/oaf:entity/oaf:result/dateofacceptance
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

        # /response/results/result/metadata/oaf:entity/oaf:result/creator
        if 'authors' not in data:
            authors = []
            for key in self.author_tags:
                if key in result:
                    authors.append(result[key])
            data['authors'] = authors

        # check that no trust value or context
        # /response/results/result/metadata/oaf:entity/oaf:result/subject
        if 'fieldsOfStudy' not in data:
            keywords = []
            for key in self.keyword_tag:
                if key in result:
                    keywords.append(result[key])
            data['fieldsOfStudy'] = keywords
        # try and find top level existing field of study and put their levels under it
        return data
