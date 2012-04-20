import gevent
import redis


class Watcher(object):
    def __init__(self, redi, haproxy):
        self.redi = redi
        self._monitors = []
        self._cxns = {}
        
    def run(self):
        self.greenlet = gevent.spawn(self.start)
        return self.greenlet

    def _connect(self, spec):
        # calc args
        cxn = redis.Redis(**args) 
        return cxn

    def connect(self, redis_spec):
        cxn = self._connect(redis_spec)
        self._cxns[cxn] = redis_spec
        gr = gevent.spawn(self.monitor, cxn)
        gr.link(self.handle_outage)
        return gr

    def check_state(self, cxn):
        """
        check state of connection and trigger any appropriate actions.

        Primarily for organizing redi as they come up for the first
        time or after they fail.
        """
        pass

    def monitor(self, cxn):
        self.check_state(cxn)
        try:
            cxn.blpop('redundis.check')
        except :
            import pdb;pdb.set_trace()
        return cxn

    def handle_outage(self, greenlet):
        cxn = greenlet.value
        spec = self._cxn.pop(cxn)
        if spec is self.redi[0]:
            self.promote_slaves()

    def start(self):
        self._monitors.extend(self.connect(redis) for redis in self.redi)
        gevent.joinall(self._monitors())
