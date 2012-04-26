from itertools import count
from path import path
import os

venv = path(os.environ.get('VIRTUAL_ENV') and os.environ['VIRTUAL_ENV'] or '')


## class task(object):
##     counter = count()
##     tasks = {}
    
##     def add(cls, func):
##         func._task = next(cls.counter)
##         return func

    
    

class Setup(object):
    """
    create a cluster
    """
    def __init__(self, config):
        self.config = config

    def haproxy(self):
        pass

    def redis(self):
        pass

    def watcher(self):
        pass

    def supervision(self):
        pass
    
    def run(cls, args):
        pass
    
