import time
import unittest
from collections import deque

from event_stream.event import Event
from src import openaire_source


class TestPubfinder(unittest.TestCase):

    def test_open_aire_source(self):
        print('start testing')
        result_queue = deque()
        os = openaire_source.OpenAireSource(result_queue)
        e = Event()
        e.data['obj']['data'] = {'doi': '10.1038/nrn3241'}
        os.work_queue.append(e)
        start = time.time()
        while time.time() - start < 10:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                print('sleep')
                pass
            else:
                self.assertEqual(item, '10.1016/j.yjmcc.2021.05.007')
                break
            finally:
                break


if __name__ == '__main__':
    unittest.main()
