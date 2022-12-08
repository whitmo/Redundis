from contextlib import contextmanager
from path import path
from stuf import frozenstuf
from urllib import quote_plus as quote
from urllib import unquote
import logging
import os
import requests
import subprocess
import tarfile
import tempfile


logger = logging.getLogger(__name__)

environ = frozenstuf([(x,k) for x, k in os.environ.items() if not x.startswith('_')])


class Downloader(object):
    """
    Downloading object for setting up a cluster
    """

    def __init__(self, cache=path(environ.VIRTUAL_ENV) / '.download_cache'):
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
        thefile.write_bytes(resp.content)
        return thefile

    def unpack_to(self, thefile, dest, overwrite=True):
        dest = path(dest)
        if not overwrite and dest.exists():
            return dest
        tmp = path(self.tempdir())
        with tarfile.open(thefile, mode='r:gz') as archive:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(archive, tmp)

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


@contextmanager
def pushd(dir):
    old_dir = os.getcwd()
    os.chdir(dir)
    try:
        yield old_dir
    finally:
        os.chdir(old_dir)

def _escape_split(sep, argstr):
    """
    from https://github.com/fabric/fabric.git

    Allows for escaping of the separator: e.g. task:arg='foo\, bar'

    It should be noted that the way bash et. al. do command line parsing, those
    single quotes are required.
    """
    escaped_sep = r'\%s' % sep

    if escaped_sep not in argstr:
        return argstr.split(sep)

    before, _, after = argstr.partition(escaped_sep)
    startlist = before.split(sep)  # a regular split is fine here
    unfinished = startlist[-1]
    startlist = startlist[:-1]

    # recurse because there may be more escaped separators
    endlist = _escape_split(sep, after)

    # finish building the escaped value. we use endlist[0] becaue the first
    # part of the string sent in recursion is the rest of the escaped value.
    unfinished += sep + endlist[0]

    return startlist + [unfinished] + endlist[1:]  # put together all the parts

def parse_cmd(cmd, splitter=_escape_split):
    """
    Adapted from https://github.com/fabric/fabric.git
    """
    args = []
    kwargs = {}
    if ':' in cmd:
        cmd, argstr = cmd.split(':', 1)
        for pair in splitter(',', argstr):
            result = splitter('=', pair)
            if len(result) > 1:
                k, v = result
                kwargs[k] = v
            else:
                args.append(result[0])
    return cmd, args, kwargs


class BuildFailure(RuntimeError):
    """
    Something went kaboom in building
    """

def sh(command, capture=False, ignore_error=False, cwd=None):
    """
    from https://github.com/paver/paver/blob/master/paver/easy.py

    Runs an external command. If capture is True, the output of the
    command will be captured and returned as a string.  If the command 
    has a non-zero return code raise a BuildFailure. You can pass
    ignore_error=True to allow non-zero return codes to be allowed to
    pass silently, silently into the night.  If you pass cwd='some/path'
    paver will chdir to 'some/path' before exectuting the command.
    """
    def runpipe():
        kwargs = { 'shell': True, 'cwd': cwd}
        if capture:
            kwargs['stderr'] = subprocess.STDOUT
            kwargs['stdout'] = subprocess.PIPE
        p = subprocess.Popen(command, **kwargs)
        p_stdout = p.communicate()[0]
        if p.returncode and not ignore_error:
            if capture:
                logger.error(p_stdout)
            raise BuildFailure("Subprocess return code: %d" % p.returncode)

        if capture:
            return p_stdout

    return runpipe()
