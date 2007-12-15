import socket, select
import protohandler

class ServerConn(object):
    def __init__(self, server, port):
        self.server = server
        self.port = port
        self.proto = protohandler.Proto()
        self.proto.job = dict
        self.__makeConn()

    def __makeConn(self):
        self._socket = socket.socket()
        self._socket.connect((self.server, self.port))

    def __writeline(self, line):
        return self._socket.send(line)

    def _get_response(self, handler):
        while True:
            x = select.select([self._socket],[],[])[0]
            x = x[0]
            if not x is self._socket:
                raise Exception('erich done fucked up')
            data = self._socket.recv(20)
            try:
                result = handler(data)
            except Exception, e:
                print 'got exception! error is: %s' % (e,)
                raise

            if result:
                break
            elif hasattr(handler, 'status') and not handler.status == 'i':
                self.handle_buried()
                break
        return result

    def _do_interaction(self, line, handler):
        print 'line is: %s handler is %s' % (line.strip(), handler)
        self.__writeline(line)
        return self._get_response(handler)

    def handle_buried(self):
        print 'job burried'

    def put(self, data, pri=0, delay=0):
        return self._do_interaction(*self.proto.process_put(data, pri, delay))

    def reserve(self):
        return self._do_interaction(*self.proto.process_reserve())

if __name__ == '__main__':
    x = ServerConn('192.168.2.1', 11300)
    x.put('python test\nthings!')
    print x.reserve()

