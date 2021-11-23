import logging
import time
import unittest
from collections import deque

from event_stream.event import Event
from src import openaire_source


class TestPubfinder(unittest.TestCase):

    def test_open_aire_source(self):
        logging.warning('start testing')
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


if __name__ == '__main__':
    unittest.main()
