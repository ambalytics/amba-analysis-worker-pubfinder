import logging
import re
from event_stream.event import Event


classpubfinder_helper.PubFinderHelper(object):

    @staticmethod
    def get_publication(item):
        """return the publication from an event"""
        publication = None

        if type(item) is Event:
            publication = item.data['obj']['data']

        return publication

    @staticmethod
    def is_publication_done(publication, save_mode=False):
        """
            check if a publication is either done or at least worth to be saved
        """
        if not publication:
            return False

        if save_mode:
            keys = ("doi", "publisher", "abstract", "title", "normalized_title", "year",
                    "authors", "fields_of_study", "source_id")
        else:
            keys = ("type", "doi", "abstract", "publisher", "title", "normalized_title", "year", "pub_date",
                    "authors", "fields_of_study", "source_id", "citation_count")

        if all(key in publication for key in keys):
            logging.debug('publication done ' + publication['doi'])

            if 'pub_date' not in publication:
                publication['pub_date'] = None
            if 'type' not in publication:
                publication['type'] = 'UNKNOWN'
            if 'abstract' not in publication:
                publication['abstract'] = None
            if 'citation_count' not in publication:
                publication['citation_count'] = 0
            if 'license' not in publication:
                publication['license'] = None
            return True

        logging.debug('publication missing ' + str(set(keys) - publication.keys()))
        return str(set(keys) - publication.keys())

    @staticmethod
    def clean_fos(fos):
        """cleans the title and removes unnecessary spaces and line breaks
        """
        results = []
        for f in fos:
            if ';' in f:
                d = f.split('f')
                results.extend(d)
            else:
                results.append(f)

        return results

    @staticmethod
    def clean_title(title):
        """cleans the title and removes unnecessary spaces and line breaks
        """
        title.replace('\n', ' ')
        return re.sub(' +', ' ', title).strip()

    @staticmethod
    def clean_abstract(abstract):
        """cleans the title and removes unnecessary spaces and line breaks
        """
        try:
            abstract = re.sub('<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});', '', abstract)
        except TypeError:
            return ''
        else:
            abstract = abstract.strip()

            remove_words = ['abstract', 'background', 'background:', 'introduction', 'objective', 'nature']

            while True:
                removed_word = False
                for word in remove_words:
                    if re.match(word, abstract, re.I):
                        abstract = abstract[len(word):]
                        removed_word = True
                if not removed_word:
                    break

            abstract = re.sub(r' +', ' ', abstract)
            abstract = re.sub(r' \. ', ' ', abstract)
            abstract = re.sub(r' *: ', ' ', abstract)
            abstract = re.sub(r' - ', ' ', abstract)

            return abstract.strip()

    @staticmethod
    def valid_abstract(abstract):
        """ check if an abstract is valid depending on its length """
        return abstract and len(abstract) > 100

    @staticmethod
    def normalize(string):
        """ normalize a given string"""
        return (re.sub('[^a-zA-Z ]+', '', string)).casefold().strip()

    @staticmethod
    def should_update(field, data, publication):
        """ check if field is in need of an update"""
        return data and field in data and field not in publication
