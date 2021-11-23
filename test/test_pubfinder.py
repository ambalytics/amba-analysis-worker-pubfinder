import logging
import time
import unittest
from collections import deque

from event_stream.event import Event
from src import openaire_source
from src import semanticscholar_source
from src import crossref_source
from src import meta_source
from src import amba_source


class TestPubfinder(unittest.TestCase):

    def test_open_aire_source(self):
        logging.warning('start testing open aire')
        result_queue = deque()
        os = openaire_source.OpenAireSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1038/nrn3241', 'source_id': [{'title': 'test'}]}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                logging.warning('sleep')
                pass
            else:
                # logging.warning(item['item'].data['obj']['data'])
                self.assertEqual(item['tag'], 'openaire')
                self.assertEqual(item['item'].data['obj']['data']['title'],
                                 'The origin of extracellular fields and currents — EEG, ECoG, LFP and spikes')
                self.assertEqual(item['item'].data['obj']['data']['normalized_title'],
                                 'the origin of extracellular fields and currents  eeg ecog lfp and spikes')
                self.assertEqual(item['item'].data['obj']['data']['publisher'], 'Nature Publishing Group')
                self.assertEqual(item['item'].data['obj']['data']['abstract'],
                                 'Neuronal activity in the brain gives rise to transmembrane currents that can be '
                                 'measured in the extracellular medium. Although the major contributor of the '
                                 'extracellular signal is the synaptic transmembrane current, other sources — '
                                 'including Na+ and Ca2+ spikes, ionic fluxes through voltage- and ligand-gated '
                                 'channels, and intrinsic membrane oscillations — can substantially shape the '
                                 'extracellular field. High-density recordings of field activity in animals and '
                                 'subdural grid recordings in humans, combined with recently developed data '
                                 'processing tools and computational modelling, can provide insight into the '
                                 'cooperative behaviour of neurons, their average synaptic input and their spiking '
                                 'output, and can increase our understanding of how these processes contribute to the '
                                 'extracellular signal.')
                os.running = False
                break

    def test_semanticscholar_source(self):
        logging.warning('start testing semantic scholar')
        result_queue = deque()
        os = semanticscholar_source.SemanticScholarSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1038/nrn3241', 'source_id': [{'title': 'test'}]}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                logging.warning('sleep')
                pass
            else:
                # logging.warning(item['item'].data['obj']['data'])
                self.assertEqual(item['tag'], 'semanticscholar')
                self.assertEqual(item['item'].data['obj']['data']['title'],
                                 'The origin of extracellular fields and currents — EEG, ECoG, LFP and spikes')
                self.assertEqual(item['item'].data['obj']['data']['normalized_title'],
                                 'the origin of extracellular fields and currents  eeg ecog lfp and spikes')
                self.assertEqual(item['item'].data['obj']['data']['publisher'], 'Nature Reviews Neuroscience')
                self.assertEqual(str(item['item'].data['obj']['data']['year']), '2012')
                self.assertNotEqual(item['item'].data['obj']['data']['citation_count'], 0)
                self.assertEqual(item['item'].data['obj']['data']['authors'][0]['normalized_name'], 'g buzski')
                self.assertEqual(item['item'].data['obj']['data']['abstract'],
                                 'Neuronal activity in the brain gives rise to transmembrane currents that can be '
                                 'measured in the extracellular medium. Although the major contributor of the '
                                 'extracellular signal is the synaptic transmembrane current, other sources — '
                                 'including Na+ and Ca2+ spikes, ionic fluxes through voltage- and ligand-gated '
                                 'channels, and intrinsic membrane oscillations — can substantially shape the '
                                 'extracellular field. High-density recordings of field activity in animals and '
                                 'subdural grid recordings in humans, combined with recently developed data '
                                 'processing tools and computational modelling, can provide insight into the '
                                 'cooperative behaviour of neurons, their average synaptic input and their spiking '
                                 'output, and can increase our understanding of how these processes contribute to the '
                                 'extracellular signal.')
                os.running = False
                break

    def test_crossref_source(self):
        logging.warning('start testing crossref')
        result_queue = deque()
        os = crossref_source.CrossrefSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1038/nrn3241', 'source_id': [{'title': 'test'}]}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                logging.warning('sleep')
                pass
            else:
                # logging.warning(item['item'].data['obj']['data'])
                self.assertEqual(item['tag'], 'crossref')
                self.assertEqual(item['item'].data['obj']['data']['title'],
                                 'The origin of extracellular fields and currents — EEG, ECoG, LFP and spikes')
                self.assertEqual(item['item'].data['obj']['data']['normalized_title'],
                                 'the origin of extracellular fields and currents  eeg ecog lfp and spikes')
                self.assertEqual(item['item'].data['obj']['data']['publisher'],
                                 'Springer Science and Business Media LLC')
                self.assertEqual(str(item['item'].data['obj']['data']['year']), '2012')
                self.assertNotEqual(item['item'].data['obj']['data']['citation_count'], 0)
                self.assertEqual(item['item'].data['obj']['data']['pub_date'], '2012-5-18')
                self.assertEqual(item['item'].data['obj']['data']['type'], 'JOURNAL_ARTICLE')
                os.running = False
                break

    def test_meta_source(self):
        logging.warning('start testing meta')
        result_queue = deque()
        os = meta_source.MetaSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1038/d41586-021-03470-x', 'source_id': [{'title': 'test'}]}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                logging.warning('sleep')
                pass
            else:
                # logging.warning(item['item'].data['obj']['data'])
                self.assertEqual(item['tag'], 'meta')
                self.assertEqual(item['item'].data['obj']['data']['title'],
                                 'Cuba’s bet on home-grown COVID vaccines is paying off')
                self.assertEqual(item['item'].data['obj']['data']['abstract'],
                                 'Preprint data show that a three-dose combo of Soberana jabs has 92.4% efficacy in '
                                 'clinical trials. Preprint data show that a three-dose combo of Soberana jabs has '
                                 '92.4% efficacy in clinical trials.')
                self.assertEqual(item['item'].data['obj']['data']['normalized_title'],
                                 'cubas bet on homegrown covid vaccines is paying off')
                self.assertEqual(item['item'].data['obj']['data']['pub_date'], '2021-11-22')
                self.assertEqual(item['item'].data['obj']['data']['year'], '2021')
                self.assertEqual(item['item'].data['obj']['data']['publisher'], 'Nature Publishing Group')
                self.assertEqual(item['item'].data['obj']['data']['authors'][0]['normalized_name'], 'reardon sara')
                self.assertEqual(item['item'].data['obj']['data']['fields_of_study'][0]['normalized_name'], 'vaccines')
                os.running = False
                break

    def test_amba_source(self):
        logging.warning('start testing amba')
        result_queue = deque()
        os = amba_source.AmbaSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1145/2330784.2330822', 'source_id': [{'title': 'test'}]}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                logging.warning('sleep')
                pass
            else:
                # logging.warning(item['item'].data['obj']['data'])
                self.assertEqual(item['tag'], 'amba')
                self.assertEqual(str(item['item'].data['obj']['data']['year']), '2012')
                self.assertEqual(item['item'].data['obj']['data']['title'],
                                 'Black-box optimization benchmarking of IPOP-saACM-ES on the BBOB-2012 noisy testbed')
                self.assertEqual(item['item'].data['obj']['data']['normalizedTitle'],
                                 'black box optimization benchmarking of ipop saacm es on the bbob 2012 noisy testbed')
                self.assertEqual(item['item'].data['obj']['data']['type'], 'CONFERENCE_PAPER')
                self.assertNotEqual(item['item'].data['obj']['data']['citation_count'], 0)
                self.assertEqual(item['item'].data['obj']['data']['publisher'], 'ACM')
                self.assertEqual(item['item'].data['obj']['data']['pubDate'], '2012-07-07')
                self.assertEqual(item['item'].data['obj']['data']['abstract'],
                                 'In this paper, we study the performance of IPOP-saACM-ES, recently proposed '
                                 'self-adaptive surrogate-assisted Covariance Matrix Adaptation Evolution Strategy. '
                                 'The algorithm was tested using restarts till a total number of function evaluations '
                                 'of 10^6D was reached, where D is the dimension of the function search space. The '
                                 'experiments show that the surrogate model control allows IPOP-saACM-ES to be as '
                                 'robust as the original IPOP-aCMA-ES and outperforms the latter by a factor from 2 '
                                 'to 3 on 6 benchmark problems with moderate noise. On 15 out of 30 benchmark '
                                 'problems in dimension 20, IPOP-saACM-ES exceeds the records observed during '
                                 'BBOB-2009 and BBOB-2010.')
                self.assertEqual(item['item'].data['obj']['data']['authors'][0]['normalized_name'], 'marc schoenauer')
                self.assertEqual(item['item'].data['obj']['data']['fields_of_study'][0]['normalized_name'], 'testbed')
                os.running = False
                break


if __name__ == '__main__':
    unittest.main()
