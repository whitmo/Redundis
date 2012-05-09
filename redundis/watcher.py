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
        self.debug("MON-DOWN %s:%s", self.host, self.port)
        while True:
            try:
                self.cxn.ping()
                self.info("%s:%s back up", self.host, self.port)
                event.set()
                return True
            except ConnectionError:
                self.debug("%s:%s still down", self.host, self.port)
                gevent.sleep(interval)

    def monitor_up(self, event, key='redundis.monitor'):
        """
        A monitor
        
        start a hanging connection that will either return or
        disconnect with an error. 
        """
        self.debug("MON-UP %s:%s", self.host, self.port)

        try:
            out = self.cxn.blpop(key)
            event.set()
            return out
        except ConnectionError, e:
            self.warn(e)
            self.connected = False
            event.set()
            return False
        except gevent.GreenletExit:
            import pdb;pdb.set_trace()

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
                          haproxy_sock='/tmp/redundis-haproxy.sock')

    weights = (150, 1, 0)

    get_role = operator.attrgetter('role')
    get_ping = operator.attrgetter('ping')

    state_set = frozenstuf(unitialized=set('master' for x in range(3)),
                        stable=set(('master', 'slave', 'slave')))
    
    def __init__(self, redi=None, haproxy_sock=None, redis_proxy=None,
                 ha_backend='redis', ha_prefix='redis-%s', down_poll=2):
        self.redi = redi and redis or self.defaults.redi
        self.down_poll = down_poll
        self.redis_proxy = redis_proxy and redis_proxy or self.defaults.redis_proxy
        self.haproxy = self.statssocket_class(haproxy_sock and haproxy_sock or self.defaults.haproxy_sock)
        self.ha_backend = ha_backend
        self.ha_prefix = ha_prefix
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

    def logging_setup(self):
        logging.basicConfig(level=logging.DEBUG,
                            format='[%(levelname)s] %(message)s')

    def dispatch_for_roles(self, roles):
        self.debug("dfr: %s", roles)
        name = self._role_patterns.get(tuple([x for x in roles if x]), 'default_role_handler')
        method = getattr(self, name)
        return method(roles)

    @for_roles('slave', 'slave', 'slave') # should never happen?!
    def default_role_handler(self, roles):
        self.error("Pathological foul up: triple replicants")
        return self.all_up_reversed(roles)

    @for_roles(None, None, None)
    def all_down(self, roles):
        """
        No instances are up. Do nothing.
        """
        return self.instances

    @for_roles('master', 'slave', 'slave')
    @for_roles('master', 'master', 'master') 
    @for_roles('master', 'master', 'slave') # unlikely
    @for_roles('master', 'slave', 'master') 
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
    @for_roles('slave', 'slave', 'master')
    def all_up_reversed(self, roles):
        r1, _, _ = self.instances
        r1.slaveof(); roles[0] = 'master'
        return self.masterup_fix_chain(roles)
        
    @for_roles('master')
    def only_master(self, roles):
        self.insert(0, self.instances.pop(roles.index('master')))
        self.assign_weights(self.instances[0])
        return self.instances

    @for_roles('slave')
    def only_slave(self, roles):
        """
        Promote to master, await return of other redi
        """
        self.insert(0, self.instances.pop(roles.index('slave')))
        self.assign_weights(self.instances[0])
        return self.instances

    def heal_pair(self, roles, r1, r2):
        """
        Reorder and remaster instances so r1 -> r2 
        """
        r1 = self.instances.pop(roles.index(r1))
        r2 = self.instances.pop(roles.index(r2))
        [self.instances.insert(0, r) for r in r2, r1]
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
        args = ((self.ha_backend, self.ha_prefix % inst.cxn_args.port, str(lbs)) for inst, lbs in zip(insts, self.weights))
        self.pool.map(self.haproxy.set_weight_tuple, args)

    def check_and_respond(self, event):
        init_roles = self.roles()
        inst_up = zip((bool(x) for x in self.roles()), self.dispatch_for_roles(init_roles))
        for up, inst in inst_up:
            if up:
                yield self.pool.spawn(inst.monitor_up, event)
            else:
                yield self.pool.spawn(inst.monitor_down, event, interval=self.down_poll)

    def loop(self):
        event = Event()
        while True:
            monitors = [x for x in self.check_and_respond(event)]
            event.wait()
            event.clear()
            [x.kill() for x in monitors]

    def start(self):
        self.logging_setup()
        return gevent.spawn(self.loop)





        
