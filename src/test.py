import unittest
from collections import deque

import openaire_source

class TestOpenAireSource(unittest.TestCase):

    def test_parsing(self):
        d = deque()
        os = openaire_source.OpenAireSource(d)
        p = {'doi': '10.1038/nrn3241'}
        d.append(p)

        self.assertEqual('foo'.upper(), 'FOO')



if __name__ == '__main__':
    unittest.main()
