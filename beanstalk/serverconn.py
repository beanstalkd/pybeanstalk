import socket
import select
import protohandler
import logging

_debug = False
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ConnectionError(Exception): pass


class ServerConn(object):
    """ServerConn is a simple, single thread single connection serialized
    beanstalk connection.  This class is meant to be used as is, or be the base
    class for more sophisticated connection handling.The methods that are
    intended to be overridden are the ones that begin with _ and __. These
    are the meat of the connection handling. The rest are just convenience
    wrappers around the protohandler methods.

    The Proto class returns a function as part of it's handling/conversion of
    the beanstalk protocol. This function is threadsafe and event safe, meant
    to be used as a callback. This should greatly simplify the writing of a
    twisted or libevent serverconn class

    """
    def __init__(self, server, port, job = False):
        self.poller = getattr(select, 'poll', lambda : None)()
        self.job = job
        self.server = server
        self.port = port

        self._socket  = None
        self.__makeConn()

    def __repr__(self):
        s = "<[%(active)s]%(class)s(%(ip)s:%(port)s)>"
        active_ = "Open" if self._socket else "Closed"
        return s % {"class" : self.__class__.__name__,
                    "active" : active_, "ip" : self.server, "port" : self.port}

    def __getattribute__(self, attr):
        logger.debug("Fetching: %s", attr)
        res = super(ServerConn, self).__getattribute__(attr)
        logger.debug("Attribute found: %s...", res)
        if not hasattr(res, "__name__") or not res.__name__.startswith('process_'):
            return res
        def caller(*args, **kw):
            logger.info("Calling %s with: args(%s), kwargs(%s)",
                         res.__name__, args, kw)
            return self._do_interaction(*res(*args, **kw))
        return caller

    def __eq__(self, comparable):
        # for unit testing
        assert isinstance(comparable, ServerConn)
        return not any([cmp(self.server, comparable.server),
                        cmp(self.port, comparable.port)])

    def __makeConn(self):
        self._socket = socket.socket()
        self._socket.connect((self.server, self.port))
        if self.poller:
            self.poller.register(self._socket, select.POLLIN)
        protohandler.MAX_JOB_SIZE = self.stats()['data']['max-job-size']

    def __writeline(self, line):
        try:
            self._socket.sendall(line)
        except:
            raise protohandler.errors.ProtoError

    def _get_response(self, handler):
        data = ''
        pcount = 0
        while True:
            if _debug and self.poller and not self.poller.poll(1):
                pcount += 1
                if pcount >= 20:
                    raise Exception('poller timeout %s times in a row' % (pcount,))
                else: continue
            pcount = 0
            recv = self._socket.recv(handler.remaining)
            if not recv:
                closedmsg = "Remote server %(server)s:%(port)s has "\
                            "closed connection" % { "server" : self.server.ip,
                                                    "port" : self.server.port}
                self.close()
                raise protohandler.errors.ProtoError(closedmsg)
            res = handler(recv)
            if res: break

        if self.job and 'jid' in res:
            res = self.job(conn=self,**res)
        return res

    def _do_interaction(self, line, handler):
        self.__writeline(line)
        return self._get_response(handler)

    def _get_watchlist(self):
        return self.list_tubes_watched()['data']

    def _set_watchlist(self, seq):
        if len(seq) == 0:
            seq.append('default')
        seq = set(seq)
        current = set(self._get_watchlist())
        add = seq - current
        rem = current - seq

        for x in add:
            self.watch(x)
        for x in rem:
            self.ignore(x)
        return

    watchlist = property(_get_watchlist, _set_watchlist)

    @property
    def tube(self):
        return self.list_tube_used()['tube']

    def close(self):
        self.poller.unregister(self._socket)
        self._socket.close()

    def fileno(self):
        return self._socket.fileno()


ServerConn = protohandler.protProvider(ServerConn)


class ThreadedConn(ServerConn):
    def __init__(self, *args, **kw):
        if 'pool' in kw:
            self.__pool = kw.pop('pool')
        super(ThreadedConn, self).__init__(*args, **kw)

    def __del__(self):
        self.__pool.release(self)
        super(ThreadedConn, self).__del__()


class ThreadedConnPool(object):
    '''
    ThreadedConnPool: A simple pool class for connection objects).
    This object will create a pool of size nconns. It does no thread wrangling,
    and no form of connection management, other than to get a unique connection
    to the thread that calls get.  In fact this could probably be simplified
    even more by subclassing Semaphore.
    '''

    import threading

    def __init__(self, nconns, server, port, job = False):
        self.__conns = list()
        self.__lock = self.threading.Lock()
        # threaded isn't defined here
        if threaded: conntype = ThreadedConn
        else: conntype = ServerConn
        for a in range(nconns):
            self.conns.append(conntype(server, port, job=job, pool=self))

        self.useme = self.threading.Semaphore(nconns)

    def get(self):
        self.useme.aquire()
        self.lock.acquire()
        ret = self.conns.pop(0)
        self.lock.release()

    def release(self, conn):
        self.lock.acquire()
        self.conns.append(conn)
        self.lock.release()
        self.useme.release()


try:
    from _libeventconn import LibeventConn
except ImportError:
    # most likely no libevent or pyevent. Thats fine, dont cause problems
    # for such cases
    pass
