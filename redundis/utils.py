from path import path
from stuf import frozenstuf
from urllib import quote_plus as quote
from urllib import unquote
import tempfile
import logging
import os
import requests
import tarfile

logger = logging.getLogger(__name__)

environ = frozenstuf([(x,k) for x, k in os.environ.items() if not x.startswith('_')])


class Downloader(object):
    """
    Downloading object for setting up a cluster
    """

    def __init__(self, cache=path(environ.VIRTUAL_ENV) / '.dl_cache'):
        self.cache = path(cache)
        if not self.cache.exists():
            self.cache.makedirs()

    tempdir = staticmethod(tempfile.mkdtemp)
    quote = staticmethod(quote)
    unquote = staticmethod(unquote)
    GET = staticmethod(requests.get)

    def download(self, url):
        thefile = self.cache / quote(url)
        if thefile in self.cache.files():
            logger.info('Return %s from cach @ %s' %(url, self.cache))
            return thefile
        resp = self.GET(url)
        thefile.write_text(resp.content)
        return thefile

    def unpack_to(self, thefile, dest, overwrite=True):
        dest = path(dest)
        if not overwrite and dest.exists():
            return dest
        tmp = path(self.tempdir())
        with tarfile.open(thefile, mode='r:gz') as archive:
            archive.extractall(tmp)

        if len(tmp.dirs()) == 1:
            tmp.dirs()[0].copytree(dest)
        else:
            if not dest.exists(): dest.mkdir()
            for member in tmp.dirs():
                member.copytree(dest)
        return dest

    def __call__(self, url, dest, overwrite=True):
        thefile = self.download(url)
        return self.unpack_to(thefile, dest, overwrite)

