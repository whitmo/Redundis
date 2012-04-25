from mock import Mock
from mock import patch
from path import path
import tempfile
import unittest


class TestDL(unittest.TestCase):
    url = 'http://haproxy.1wt.eu/download/1.4/src/haproxy-1.4.20.tar.gz'
    fake = path(__file__).parent / path('wee.tar.gz')
    cache = path(tempfile.mkdtemp())

    def tearDown(self):
        [x.remove() for x in self.cache.files()]

    def makeone(self, cache=cache):
        from redundis import utils
        return utils.Downloader(cache=cache)


    def test_downloader(self):
        dl = self.makeone()
        with patch.object(dl, 'GET') as get:
            get.return_value = Mock(name='response')
            get.return_value.content = self.fake.text()
            out = dl.download(self.url)
            
        assert out.exists()
        assert out.text() == self.fake.text()
