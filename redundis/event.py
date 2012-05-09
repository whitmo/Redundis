from gevent.event import Event
from stuf import stuf



class StateChange(Event):
    """
    The state of the chain changes

    - master fail
    - master exit
    """
    def __init__(self):
        super(StateChange, self).__init__()
        self.state = None


class State(object):
    """
    A state change
    """


def monitor(cxn, key='redundis.monitor'):
    info = stuf(host=cxn.host,
                port=cxn.port,
                role=cxn.role)
    
    try:
        out = cxn.blpop(key)
        return out, info
    except :
        return None, info


class HandleFail(object):
    def master(self):
        pass

    def slave(self):
        pass

def response(gr):
    out, info = gr.get()
    if info.role == 'master':
        handle_master_fail()
        

    



