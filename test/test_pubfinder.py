import time
import unittest
from collections import deque

from src import openaire_source


class TestPubfinder(unittest.TestCase):

    def test_open_aire_source(self):
        result_queue = deque()
        os = openaire_source.OpenAireSource(result_queue)
        p = {'doi': '10.1038/nrn3241'}
        os.work_queue.append(p)
        while True:
            try:
                item = result_queue.pop()
            except IndexError:
                time.sleep(1)
                pass
            else:
                self.assertEqual(item, '10.1016/j.yjmcc.2021.05.007')
                break


if __name__ == '__main__':
    unittest.main()
