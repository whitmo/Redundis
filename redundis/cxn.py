from redis.connection import Connection as BaseCxn
from redis.connection import ConnectionPool as BaseCxnPool
from redis.connection import DefaultParser
from redis.exceptions import ConnectionError
from itertools import count
import traceback
import logging


logger = logging.getLogger(__name__)


class Connection(BaseCxn):
    """
    A Redis connection object that executes a callback on failure
    """
    defaults = dict(host='localhost',
                    port=6379, db=0,
                    password=None,
                    socket_timeout=None,
                    encoding='utf-8',
                    encoding_errors='strict', 
                    failure_callback=None,
                    parser_class=DefaultParser)
    
    def __init__(self, **kw):
        args = self.defaults.copy()
        self.original_args = args
        args.update(kw)
        failure_callback = args.pop('failure_callback')
        BaseCxn.__init__(self, **args)
        self.attempts = 0
        self._depth = count()
        self.failure_callback = failure_callback is not None and \
                                failure_callback or self.default_callback

    def default_callback(self, cxn, exc):
        logger.debug(traceback.format_exc(limit=None))
        return False
        
    def connect(self):
        self.attempts += 1
        try:
            BaseCxn.connect(self)
            self.attempts = 0
            self.depth = 0
            return 
        except ConnectionError, e:
            out = self.failure_callback(self, e)
            if out is False:
                raise
            return out

    update_keys = ('host',
                   'port',
                   'db',
                   'password',
                   'socket_timeout',
                   'encoding',
                   'encoding_errors')

    def update(self, **kwargs):
        args = self.original_args.copy()
        args.update(**kwargs)
        
        [setattr(self, key, args[key]) for key in self.update_keys]

        self._sock = None
        self.attempts = 0

    @property
    def how_deep(self):
        return next(self._depth)


class ConnectionPool(BaseCxnPool):
    """A connection pool that manages failover candidates"""
    default_spec = dict(host='localhost',
                        port=6379, db=0, password=None,
                        socket_timeout=None, encoding='utf-8',
                        encoding_errors='strict')
    
    def __init__(self, connection_class=Connection, max_connections=None, failure_callback=None,
                 candidates=None, retry=2, depth=1, **connection_kwargs):
        """
        adds `candidates` and `retry`

        `candidates` is a list of maps whose members define how
        failover candidate differ from each other. For example, 3
        instance across 2 hosts::

          >>> [dict(host='h1', port='6953'),
          ...  dict(host='h1', port='6952'),
          ...  dict(host='h2', port='6952')]

        `retry` defines how many times the connection pool will
        attempt to connect to the active candidate before failing
        over using the default failure callback.
        """
        self.depth = depth
        self.retry = retry
        if failure_callback is None:
            failure_callback = self.callback
        
        self.connection_kwargs = dict(self.default_spec,
                                      failure_callback=failure_callback,
                                      **connection_kwargs)
        
        self.candidates = dict(dict(self.connection_kwargs, **c) for c in candidates)

        BaseCxnPool.__init__(self, connection_class=Connection,
                             max_connections=None,
                             **connection_kwargs)

    def manage_failover(self, cxn):
        """
        
        """
        self.candidate.append(self.candidate.pop(0))
        cxn.update(**self.candidate[0])
        return cxn.connect()

    def callback(self, cxn, exc):
        """
        Default failure callback

        Will recursively attempt connections until failed connection
        attempts exceed `retry` value.

        If `retry` is exceeded, `manage_failover` will be called,
        returning either `None` is a new connection has been
        established to a working candidate or `False` if all
        candidates have failed.
        """
        if self.retry > cxn.attempts:
            return cxn.connect()

        if cxn.depth > self.depth * len(self.candidates):
            return False
        
        return self.manage_failover(cxn)

