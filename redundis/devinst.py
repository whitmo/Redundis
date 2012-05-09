from .utils import Downloader
from .utils import parse_cmd
from .utils import pushd
from .utils import sh
from cliff.command import Command
from path import path
from stuf import frozenstuf as spec
import logging
import os


class DevInstall(Command):
    """
    Create a local install for development and testing
    """
    log = logging.getLogger(__name__)

    haproxy = spec(url='http://haproxy.1wt.eu/download/1.4/src/haproxy-1.4.20.tar.gz',
                       install=['sh:make TARGET=generic',
                                'sh:make install DESTDIR=$VIRTUAL_ENV',
                                'self:link_redis'])

    redis = spec(url="http://redis.googlecode.com/files/redis-2.4.11.tar.gz",
                 install=['sh:make',
                          'self:link_redis'])
    grab = None
    venv = path(os.environ.get('VIRTUAL_ENV') and os.environ['VIRTUAL_ENV'] or '')
    
    parse_action = staticmethod(parse_cmd)

    def link_redis(self):
        pass

    def link_haproxy(self):
        pass

    def get_parser(self, name):
        parser = super(DevInstall, self).get_parser(name)
        return parser

    def download_and_install(self, name, spec, overwrite=False):
        if self.grab is None:
            self.grab = Downloader()
        out = self.grab(spec.url, self.venv / 'src' / name, overwrite)
        with pushd(out):
            for action in spec.install:
                out = self.run_action(action)
                if not out is None:
                    self.app.stdout.write(out)
    
    def run_action(self, action):
        kind, todo = action.split(':', 1)
        return getattr(self, '_action_%s' %kind)(todo)
    
    def _action_self(self, todo):
        import pdb;pdb.set_trace()
        cmd, args, kwargs = self.parse_action(todo)
        return getattr(self, cmd)(*args, **kwargs)

    _action_sh = staticmethod(sh)

    def run(self, parsed_args):
        self.log.info('And so it begins')
        self.download_and_install('redis', self.redis)
        self.download_and_install('haproxy', self.haproxy)
        self.log.debug('debugging')
        self.app.stdout.write('hi!\n')



