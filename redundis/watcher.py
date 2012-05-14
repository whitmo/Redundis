import gevent.monkey
gevent.monkey.patch_all()

from . import haproxy
from gevent import pool
from gevent.event import Event
from redis.connection import ConnectionError
from stuf import frozenstuf
import gevent
import inspect as ins
import logging
import operator
import redis
import itertools
from cliff.command import Command


class Logged(object):
    logger = logging.getLogger(__name__)
    info = logger.info
    warn = logger.warn
    error = logger.error
    debug = logger.debug


class RedisCxn(Logged):
    """
    A redis instance in the chain
    """
    redis_class = redis.StrictRedis

    def __init__(self, **cxn_args):
        self.cxn_args = frozenstuf(cxn_args)
        self.cxn = self.connect(self.cxn_args)
        self.connected = False

    @property
    def host(self):
        return self.cxn_args.host

    @property
    def port(self):
        return self.cxn_args.port

    def connect(self, cxn_args):
        cxn = self.redis_class(**cxn_args)
        try:
            cxn.ping()
            self.info("Connected %s:%s", cxn_args.host, cxn_args.port)
            self.connected = True
        except ConnectionError:
            self.warn("Not connected %(host)s:%(port)s " %self.cxn_args)
        return cxn

    @staticmethod
    def parse_redis_spec(spec):
        host, port = spec.split(':')
        port = int(port)
        return dict(host=host, port=port)

    @classmethod
    def from_spec(cls, spec):
        return cls(**cls.parse_redis_spec(spec))

    def monitor_down(self, event, interval=2):
        """
        poll a redis host until it become available
        """
        counter = itertools.count()
        while True:
            try:
                self.cxn.ping()
                self.info("%s:%s back up", self.host, self.port)
                self.connected = True
                event.set()
                return True
            except ConnectionError:
                if not next(counter) % 5:
                    self.debug("%s:%s down", self.host, self.port)
                gevent.sleep(interval)

    def monitor_up(self, event, key='redundis.monitor'):
        """
        A monitor
        
        start a hanging connection that will either return or
        disconnect with an error. 
        """
        try:
            out = self.cxn.blpop(key)
            event.set()
            return out
        except ConnectionError:
            self.warn("%s:%s has had a connection failure", self.host, self.port)
            self.connected = False
            event.set()
            return self
        except gevent.GreenletExit:
            pass

    @property
    def role(self):
        try:
            return self.cxn.info()['role']
        except ConnectionError:
            return None        

    @property
    def ping(self):
        try:
            return self.cxn.ping()
        except ConnectionError:
            return False

    def slaveof(self, rcxn=None):
        if rcxn is None:
            return self.cxn.slaveof()
        return self.cxn.slaveof(host=rcxn.host, port=rcxn.port)


def for_roles(*pattern):
    """
    Annotates a function with a pattern
    """
    def register(func):
        rp = getattr(func, '_role_pattern', None)
        if rp is None:
            rp = []
        rp.append(pattern)
        func._role_pattern = rp
        return func
    return register


def register_patterns(cls):
    funcs = (x for x in cls.__dict__.values() \
             if ins.isroutine(x) and getattr(x, '_role_pattern', False))
    mapping = cls._role_patterns = {}
    for func in funcs:
        for pattern in func._role_pattern:
            mapping[pattern] = func.__name__
    return cls


@register_patterns
class Watcher(Logged):
    redis_class = RedisCxn
    statssocket_class = haproxy.StatsSocket
    defaults = frozenstuf(redi=['localhost:6379',
                                'localhost:6380',
                                'localhost:6381'],
                          redis_proxy='localhost:6666',
                          haproxy_sock='/tmp/redundis-haproxy.sock',
                          ha_backend='redis',
                          ha_prefix='redis-%s')

    weights = (150, 1, 0)

    get_role = operator.attrgetter('role')
    get_ping = operator.attrgetter('ping')

    state_set = frozenstuf(unitialized=set('master' for x in range(3)),
                        stable=set(('master', 'slave', 'slave')))
    
    def __init__(self, redi=None, haproxy_sock=None, redis_proxy=None,
                 ha_backend=None, ha_prefix=None, down_poll=2):
        self.redi = redi and redi or self.defaults.redi
        self.down_poll = down_poll
        self.redis_proxy = redis_proxy and redis_proxy or self.defaults.redis_proxy
        self.haproxy = self.statssocket_class(haproxy_sock and haproxy_sock or self.defaults.haproxy_sock)
        self.ha_backend = ha_backend and ha_backend or self.defaults.ha_backend
        self.ha_prefix = ha_prefix and ha_prefix or self.defaults.ha_prefix
        self.pool = pool.Pool(4)

        self.instances = None
        self.watches = []
        self._cxns = {}
        self.state = {}

    def run(self):
        self.greenlet = gevent.spawn(self.start)
        return self.greenlet
    
    def roles(self):
        if self.instances is None:
            self.load_inst()
        return self.pool.map(self.get_role, self.instances)

    def ping_all_inst(self):
        return self.pool.map(self.get_ping, self.instances)

    def load_inst(self):
        self.instances = []
        self.instances.extend(self.redis_class.from_spec(spec) for spec in self.redi)
        return self.instances

    def logging_setup(self, loglevel=logging.INFO):
        logging.basicConfig(level=loglevel,
                            format='[%(levelname)s] %(message)s')

    def dispatch_for_roles(self, roles):
        self.debug("dfr: %s", roles)
        name = self._role_patterns.get(tuple([x for x in roles if x]), 'default_role_handler')
        method = getattr(self, name)
        try:
            return method(roles)
        except Exception:
            #import pdb, sys;pdb.post_mortem(sys.exc_info()[2])
            self.error("Unexpected exception", exc_info=True)

    def default_role_handler(self, roles):
        if not any(roles):
            return self.instances
        return self.all_up_reversed(roles)

    @for_roles(None, None, None)
    def all_down(self, roles):
        """
        No instances are up. Do nothing.
        """
        return self.instances

    # these conditions likely result from intermittent network issues.

    @for_roles('master', 'master', 'slave') 
    def abberation1(self, roles):
        self.warn("abberation1 '%s'" %roles)
        return self.masterup_fix_chain(roles)

    @for_roles('slave', 'slave', 'slave')  
    def abberation2(self, roles):
        self.warn("abberation2 '%s'" %roles)
        return self.all_up_reversed(roles)


    @for_roles('master', 'slave', 'slave')   # normal order, but may need rechain
    @for_roles('master', 'master', 'master') # initial order
    @for_roles('master', 'slave', 'master')  # a redis returns
    def masterup_fix_chain(self, roles):
        """
        A master is in the master position. Chain redis to each
        other. If not a condition of initialization, an abberation has
        occurred
        """
        r1, r2, r3 = self.instances
        r2.slaveof(r1)
        r3.slaveof(r2)
        self.assign_weights(r1, r2, r3)
        return self.instances

    @for_roles('slave', 'master', 'master')
    @for_roles('slave', 'master', 'slave')
    @for_roles('slave', 'slave', 'master')
    def all_up_reversed(self, roles):
        r1, _, _ = self.instances
        r1.slaveof(); roles[0] = 'master'
        return self.masterup_fix_chain(roles)
        
    @for_roles('master')
    def only_master(self, roles):
        self.instances.insert(0, self.instances.pop(roles.index('master')))
        self.assign_weights(self.instances[0])
        return self.instances

    @for_roles('slave')
    def only_slave(self, roles):
        """
        Promote to master, await return of other redi
        """
        self.instances.insert(0, self.instances.pop(roles.index('slave')))
        self.assign_weights(self.instances[0])
        return self.instances

    def heal_pair(self, roles, r1, r2):
        """
        Reorder and remaster instances so r1 -> r2 
        """
        offline = self.instances.pop(roles.index(None))
        r1, r2 = self.instances
        self.instances.append(offline)
        self.pool.spawn(r1.slaveof)
        self.pool.spawn(r2.slaveof, r1)
        self.pool.join()
        self.assign_weights(r1, r2)
        return self.instances

    @for_roles('slave', 'slave')
    def dead_master(self, roles):
        return self.heal_pair(roles, 'slave', 'slave')

    @for_roles('master', 'slave')
    def dead_slave(self, roles):
        """
        One redis is still down, enforce normal configuration
        """
        return self.heal_pair(roles, 'master', 'slave')

    @for_roles('slave', 'master')
    def flipped_master(self, roles):
        """
        hypothetically, two redi have gone down and one has returned
        """
        return self.heal_pair(roles, 'slave', 'master')

    @for_roles('master', 'master')
    def double_master(self, roles):
        """
        2 redi up, one lagging to return.  Hypothetically a full
        failure of all redi or on initialization
        """
        return self.heal_pair(roles, 'master', 'master')

    def assign_weights(self, *insts):
        """
        Takes a list of `insts` of a maximimum length 3 that is
        assumed to be in chain order and applies the appropriate
        weights in haproxy.
        """
        args = ((self.ha_backend, self.ha_prefix % inst.cxn_args.port, str(lbs)) \
                for inst, lbs in zip(insts, self.weights))
        self.pool.map(self.set_weight, args)

    def set_weight(self, args):
        backend, server, weight = args
        self.haproxy.set_weight(backend, server, weight)
        cur = self.haproxy.get_weight(backend, server).strip()
        self.debug("%s %s", server, cur)

    def check_weights(self, *insts):
        args = ((self.ha_backend, self.ha_prefix % inst.cxn_args.port) for inst in insts)
        return self.pool.map(self.haproxy.get_weight_tuple, args)        

    def do_dispatch(self):
        insts = self.dispatch_for_roles(self.roles())
        return zip((bool(x) for x in self.roles()),
                   self.check_weights(*insts),
                   insts)

    def check_and_respond(self, event):
        """
        Triggers the inspection and reaction to the order of role for
        our chain.
        """
        event.clear()
        try:
            inst_up = self.do_dispatch()
        except ConnectionError:
            gevent.sleep(1)
            inst_up = self.do_dispatch()

        for up, weight, inst in inst_up:
            if up:
                self.info("%s:%s UP => %s", inst.host, inst.port, weight)
                gr = self.pool.spawn(inst.monitor_up, event)
                gr.link(self.monitor_up_exit)
                yield gr 
            else:
                self.info("%s:%s DOWN => %s", inst.host, inst.port, weight)
                yield self.pool.spawn(inst.monitor_down, event, interval=self.down_poll)

    def loop(self):
        """
        The loop follows the series of actions:

         1. check current state redi, adjust to form a suitable chain
            if possible, return a series of greenlets to monitor what
            is up and down.

         2. Wait for a monitor event to indicate a change of
            connection status (lose or reestablish connection)
 
         3. Kill all watches and clear event

         4. Rinse and repeat
        """
        event = Event()
        while True:
            watches = [mon for mon in self.check_and_respond(event)]
            self.debug("%s", [x.strip() for x in self.check_weights(*self.instances)])
            event.wait()
            [w.kill() for w in watches] 


    def monitor_up_exit(self, gr):
        inst = gr.value
        if not inst is None:
            where = self.instances.index(inst)
            self.debug("%s:%s caboosed", inst.host, inst.port)
            self.instances.append(self.instances.pop(where))

    def start(self):
        self.logging_setup()
        return gevent.spawn(self.loop)



class Watch(Command):
    """
    dundis command for running the watcher
    """
    def get_parser(self, name):
        parser = super(Watch, self).get_parser(name)
        parser.add_argument('-c', '--config', action='store',
                            default=None, help='config file')

        parser.add_argument('-r', '--redi', action='store',
                            default=",".join(Watcher.defaults.redi),
                            help='Redis instances (comma delimited)')

        parser.add_argument('-p', '--proxy', action='store',
                            default=Watcher.defaults.redis_proxy, help='Address of redis via haproxy')

        parser.add_argument('--haproxy_sock', action='store',
                            default=Watcher.defaults.haproxy_sock, help='HAProxy stats socket')
        
        parser.add_argument('--haproxy_backend', action='store',
                            default=Watcher.defaults.ha_backend, help='HAProxy stats socket')
        
        #@@ server prefix
        return parser
    
    def run(self, args):
        redi = args.redi.split(',')
        watcher = Watcher(redi, args.haproxy_sock, args.proxy, args.haproxy_backend)
        try:
            watcher.start().join()
        except KeyboardInterrupt:
            return 


        



