"""
routines for recovering a failed replication chain
"""
from cliff.command import Command
import yaml
from stuf import frozenstuf 
from .config import resource_spec
import redis
import time


def load_config(spec):
    fp = resource_spec(spec)
    with open(fp) as stream:
        return frozenstuf(yaml.load(stream))

def setup_chain(config):
    hp = dict((num, address.split(':')) for num, address in enumerate(config.redi))
    cxns = r1, r2, r3 = [redis.Redis(host=hp[num][0], port=hp[num][1]) for num in sorted(hp)]
    
    r1.slaveof()
    r2.slaveof(host=hp[0][0], port=hp[0][1])
    r3.slaveof(host=hp[1][0], port=hp[1][1])

    return cxns

        
def remaster_chain(config):
    hp = dict((num, address.split(':')) for num, address in enumerate(config.redi))
    cxns = r1, r2, r3 = [redis.Redis(host=hp[num][0], port=hp[num][1]) for num in sorted(hp)]
    r1.ping() # make sure r1 is up
    r1.slaveof(host=hp[1][0], port=hp[1][1])
    # wait an abritrary amount of time for old cxn to drop backup
    r1.slaveof()
    r2.slaveof(host=hp[0][0], port=hp[0][1])
    return cxns


def time_recovery(hacxn):
    start = time.time()
    still2 = True
    while still2 is True:
        still2 = hacxn.config_get()['dbfilename'] == '2.dump.rdb'
        time.sleep(1)
    return time.time() - start


class RedisRecovery(object):
    def __init__(self, redi):
        self._redi = redi
        self._cxn = None




class BaseCommand(Command):
    def get_parser(self, name):
        parser = super(Promote, self).get_parser(name)
        parser.add_argument('--config', action='store',
                            default='egg:Redundis#redundis/etc/example.yml')
        return parser


class ReMaster(BaseCommand):
    """
    Redis recovery for haproxy backup
    ---------------------------------
    
    A redis replication is a chain of a master and two replicating
    slaves. In the simple haproxy setup, it's starting characteristics
    might look like this::

    r1 (master) -> r2 (primary slave: backup) -> r3 (secondary slave: disabled)

    If the master fails (and there is no automated rechaining)::

    r1 (dead) -> r2 (headless-slave:backup now active) -> r3 (primary slave: disabled)

    Simple redis recovery only talks to the redis instances

    1. Restart r1
    2. r1 SLAVEOF r2, r2 SET CONFIG READONLY
  
    << ?? time passes: how do we tell sync is done or that connections
    have dropped backup 

    apparently, connections may never drop
    >>

    3. r1 SLAVEOF NO ONE, r2 SLAVEOF r1
    """

    def get_parser(self, name):
        parser = super(ReMaster, self).get_parser(name)
        return parser

    def run(self, parsed_args):
        pass


class Promote(BaseCommand):
    """
    Redis recovery, haproxy enable/disable
    --------------------------------------

    haproxy only supports certain server declarations over it's socket
    interface: enable, disable, set weight.  In this case, we would create
    slaves with the following weight and toggles::

     r1 (high) -> r2 (1) -> r3 (1, disabled)
 
    If r1 exits:

     1. haproxy: set r1 (1, disabled), set r2 (high) , set r3 (1, enable) 
     2. bring r1 back up 
     2. r1 SLAVEOF r3
     3. r2 SLAVEOF NO ONE
    """ 
    def run(self, parsed_args):
        pass

    def get_parser(self, name):
        parser = super(Promote, self).get_parser(name)
        parser.add_argument('--config', action='store',
                            default='egg:Redundis#redundis/etc/example.yml')
        return parser
