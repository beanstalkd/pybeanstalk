import socket, select
import protohandler

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

    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.proto = protohandler.Proto()
        self.job = None
        self.__makeConn()

    def __makeConn(self):
        self._socket = socket.socket()
        self._socket.connect((self.server, self.port))

    def __writeline(self, line):
        x = self._socket.send(line)
        try:
            self._socket.fileno()
        except:
            raise protohandler.ProtoError

    def _get_command(self):
        data = ''
        while True:
            x = select.select([self._socket],[],[], 1)
            if has_descriptor(x):
                recv = self._socket.recv(1)
                if not recv:
                    self._socket.close()
                    raise protohandler.ProtoError("Remote host closed conn")
                data += recv
                if data.endswith('\r\n'):
                    break
        return data

    def _get_response(self, handler):
        ''' keep in mind handler returns a tuple:
            (satus, expected_data_len or result)
        '''
        command = self._get_command()
        status, result = handler(command)

        while status == 'i':
            remaining = result
            x = select.select([self._socket],[],[], 1)
            if has_descriptor(x):
                get_amount = ((remaining < 20) and remaining) or 20
                data = self._socket.recv(get_amount)
                if not data:
                    raise protohandler.ProtoError('Remotehost closed socket')
                try:
                    status, result = handler(data)
                except Exception, e:
                    print 'got exception! error is: %s' % (e,)
                    raise

        return result

    def _do_interaction(self, line, handler):
        print 'line is: %s handler is %s' % (line.strip(), handler)
        self.__writeline(line)
        return self._get_response(handler)

    def handle_status(self, status):
        if status == 'b':
            print 'job burried'

    def put(self, data, pri = 0, delay = 0):
        return self._do_interaction(*self.proto.process_put(data, pri, delay))

    def reserve(self):
        x = self._do_interaction(*self.proto.process_reserve())
        if self.job:
            return self.job(conn=self,**x)
        else:
            return x

    def delete(self, jid):
        return self._do_interaction(*self.proto.process_delete(jid))

    def release(self, jid, newpri = 0, delay = 0):
        return self._do_interaction(
            *self.proto.process_release(jid, newpri, delay))

    def bury(self, jid, newpri = 0):
        return self._do_interaction(*self.proto.process_bury(jid, newpri))

    def peek(self, jid = 0):
        x = self._do_interaction(*self.proto.process_peek(jid))
        if self.job:
            return self.job(conn = self,**x)
        else:
            return x

    def kick(self, bound = 0 ):
        return self._do_interaction(*self.proto.process_kick(bound))

    def stats(self, jid = 0):
        return self._do_interaction(*self.proto.process_stats(jid))

# poor testing stuff follows, should probably be extended :)
if __name__ == '__main__':
    x = ServerConn('192.168.2.1', 11300)
    x.put('python test\nthings!')
    x.put('python test\nstuff!')
    x.put('python test\nyaitem')
    job = x.reserve()
    x.bury(job['jid'])
    job = x.reserve()
    x.release(job['jid'], job['pri'])
    for i in range(2):
        job = x.reserve()
        x.delete(job['jid'])
    burried = x.peek()
    if burried:
        print burried
    x.kick(1)
    kicked = x.reserve()
    print kicked
    x.delete(kicked['jid'])
