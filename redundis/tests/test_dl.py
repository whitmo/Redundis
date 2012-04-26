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
            get.return_value.content = self.fake.bytes()
            out = dl.download(self.url)
            
        assert out.exists()
        assert out.bytes() == self.fake.bytes()

    def place_file(self, dl):
        thefile = dl.cache / dl.quote(self.url)
        thefile.write_text(self.fake.bytes())
        return thefile

    def test_cache_hit(self):
        dl = self.makeone()
        self.place_file(dl)
        out = dl.download(self.url)                
        assert out.exists()
        assert out.text() == self.fake.text()

    def test_cache_creation(self):
        newcache = self.cache / 'new'
        self.makeone(cache=newcache)
        assert newcache.exists()

    def test_unpack_to(self):
        dl = self.makeone()
        thefile = self.place_file(dl)
        dest = self.cache / 'out-test'
        out = dl.unpack_to(thefile, dest)
        assert out == self.cache / 'out-test'
        assert out.exists()
        assert out.isdir()
        assert (out / 'README').exists()

