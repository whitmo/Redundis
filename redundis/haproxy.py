# all credit to: http://www.gefira.pl/blog/2011/07/01/accessing-haproxy-statistics-with-python/
from __future__ import absolute_import, division, print_function, unicode_literals

from cStringIO import StringIO
from contextlib import contextmanager
from time import time
from traceback import format_exc
import logging


logger = logging.getLogger(__name__)
 

class StatsSocket(object):
    """ Used for communicating with HAProxy through its local UNIX socket interface.
    """
    def __init__(self, socket_name=None):
        self.socket_name = socket_name

    def get_weight(self, backend, server):
        return self.execute("get weight",  "%s/%s" %(backend, server))

    def set_weight(self, backend, server, weight):
        self.execute("set weight", "%s/%s %s" %(backend, server, weight))

    def set_weight_tuple(self, (backend, server, weight)):
        return self.set_weight(backend, server, weight)

    def get_weight_tuple(self, (backend, server)):
        return self.get_weight(backend, server).strip()

    def enable(self, backend, server):
        self.execute('enable server', '%s/%s' %(backend, server))

    def disable(self, backend, server):
        self.execute('disable server', '%s/%s' %(backend, server))
        
    def execute(self, command, extra="", timeout=200):
        """
        Executes a HAProxy command by sending a message to a HAProxy's
        local UNIX socket and waiting up to 'timeout' milliseconds for
        the response.
        """
        if extra:
            command = command + ' ' + extra

        logger.debug('haproxy: %s', command)
        
        buff = StringIO()
        end = time() + timeout

        with unixsocket(self.socket_name) as client:
            client.send(command + '\n')
            while time() <=  end:
                data = client.recv(4096)
                if data:
                    buff.write(data)
                else:
                    return buff.getvalue()


@contextmanager
def unixsocket(sockname):
    """
    sets up a unix socket and closes it when the block exits. Catches errors.
    """
    from gevent import socket
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.connect(sockname)
        yield client
    except Exception, e:
        msg = 'An error has occurred, e=[{e}]'.format(e=format_exc(e))
        logger.error(msg)
        raise
    finally:
        client.close()

 

