import unittest
import mock
import redis
from nose import tools

def test_imports():
    try:
        from redundis import cxn
        assert cxn
    except Exception, e:
        raise AssertionError(e)


class TestConnection(unittest.TestCase):
    """
    exercise the connection object

    CAVEAT!!! assumes port 0 is not available to test process.
    """
    def makeone(self, **kw):
        from redundis import cxn
        return cxn.Connection(**kw)

    @tools.raises(redis.ConnectionError)
    def test_def_callback_raises_on_bad_cxn(self):
        cxn = self.makeone(port=0)
        cxn.connect()

    def test_failure_callback_application(self):
        new_cxn = mock.Mock()
        callback = mock.Mock(name='callback', return_value=new_cxn)
        cxn = self.makeone(port=0, failure_callback=callback)
        out = cxn.connect()
        assert callback.called
        assert out is new_cxn
        assert len(callback.call_args) and len(callback.call_args[0]) == 2, callback.call_args
        cxn_in, error = callback.call_args[0]
        assert isinstance(error, redis.ConnectionError)
        assert cxn_in is cxn


class TestConnectionPool(unittest.TestCase):
    """
    exercise the ConnectionPool
    """
    def makeone(self):
        pass
