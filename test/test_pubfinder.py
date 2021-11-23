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
                logging.warning(item.item.data['obj']['data'])
                self.assertEqual(item.item.data['obj']['data'], '10.1016/j.yjmcc.2021.05.007')
                os.running = False
                break


if __name__ == '__main__':
    unittest.main()
