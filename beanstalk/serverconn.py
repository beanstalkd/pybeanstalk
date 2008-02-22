import socket, select
import protohandler

_debug = 1

def has_descriptor(iterable):
    for x in iterable:
        for y in x:
            if hasattr(y, 'fileno'):
                return True
    return False

class ServerConn(object):
    '''ServerConn is a simple, single thread single connection serialized
    beanstalk connection.  This class is meant to be used as is, or be the base
    class for more sophisticated connection handling.The methods that are
    intended to be overridden are the ones that begin with _ and __. These
    are the meat of the connection handling. The rest are just convenience
    wrappers around the protohandler methods.

    The Proto class returns a function as part of it's handling/conversion of
    the beanstalk protocol. This function is threadsafe and event safe, meant
    to be used as a callback. This should greatly simplify the writing of a
    twisted or libevent serverconn class'''

    def __init__(self, server, port, job = False):
        self.server = server
        self.port = port
        self.proto = protohandler.Proto()
        self.job = job or (lambda **x: x)
        self.__makeConn()

    def fileno(self):
        return self._socket.fileno()

    def __makeConn(self):
        self._socket = socket.socket()
        self._socket.connect((self.server, self.port))

    def __writeline(self, line):
        try:
            self._socket.sendall(line)
        except:
            raise protohandler.ProtoError

    def _get_command(self):
        data = ''
        while True:
            #print 'selecting...',
            x = select.select([self._socket],[],[], 1)
            if has_descriptor(x):
                recv = self._socket.recv(1)
                if not recv:
                    self._socket.close()
                    raise errors.ProtoError("Remote host closed conn")
                data += recv
                if data.endswith('\r\n'):
                    break
        return data.rstrip('\r\n')

    def _get_response(self, handler):
        ''' keep in mind handler returns a tuple:
            (satus, expected_data_len or result)
        '''
        command = self._get_command()
        status, result = handler(command)
        if status == 'i':
            result['data'] = ''
            expected = result['bytes']
            result['data'] = result['parse_data'](self._get_data(expected))
            result['status'] = 'd'
        return result

    def _get_data(self, expected):
        # include the clrf len for reading
        expected += 2
        res = ''
        while expected:
            data = self._socket.recv(expected)
            if not data:
                raise errors.ProtoError('Remote Host closed socket')
            # sanity check
            elif len(data) > expected:
                raise errors.ExpectedCrlf('Server sent too much data')

            expected -= len(data)
            res += data

        if not res.endswith('\r\n'):
            raise errors.ExpectedCrlf('Server did not terminate data properly')

        return res.rstrip('\r\n')

    def _do_interaction(self, line, handler):
        self.__writeline(line)
        return self._get_response(handler)

    def put(self, data, pri = 0, delay = 0, ttr = 100):
        return self._do_interaction(
            *self.proto.process_put(data, pri, delay, ttr))

    def reserve(self):
        x = self._do_interaction(*self.proto.process_reserve())
        return self.job(conn=self,**x)

    def delete(self, jid):
        return self._do_interaction(*self.proto.process_delete(jid))

    def release(self, jid, newpri = 0, delay = 0):
        return self._do_interaction(
            *self.proto.process_release(jid, newpri, delay))

    def bury(self, jid, newpri = 0):
        return self._do_interaction(*self.proto.process_bury(jid, newpri))

    def peek(self, jid = 0):
        x = self._do_interaction(*self.proto.process_peek(jid))
        return self.job(conn = self,**x)

    def kick(self, bound = 0 ):
        return self._do_interaction(*self.proto.process_kick(bound))

    def stats(self, jid = 0):
        x = self._do_interaction(*self.proto.process_stats(jid))
        return x


class ThreadedConn(ServerConn):
    def __init__(self, *args, **kw):
        if 'pool' in kw:
            self.__pool = kw.pop('pool')
        super(ThreadedConn, self).__init__(*args, **kw)

    def __del__(self):
        self.__pool.release(self)

class ThreadedConnPool(object):
    '''
    ThreadedConnPool: A simple pool class for connection objects).
    This object will create a pool of size nconns. It does no thread wrangling,
    and no form of connection management, other than to get a unique connection
    to the thread that calls get.  In fact this could probably be simplified
    even more by subclassing Semaphore.
    '''
    def __init__(self, nconns, server, port, job = False):
        self.__conns = list()
        self.__lock = threading.Lock()
        if threaded: conntype = ThreadedConn
        else: conntype = ServerConn
        for a in range(nconns):
            self.conns.append(conntype(server, port, job=job, pool=self))

        self.useme = threading.Semaphore(nconns)

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

