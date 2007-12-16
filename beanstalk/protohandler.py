import yaml

class FailureError(Exception): pass
class ProtoError(Exception): pass

class Proto(object):
    """
        Protocol handler for processing the beanstalk protocol

        See reference at:
            http://xph.us/software/beanstalkd/protocol.txt (as of Dec 14, 2007)

    """

    def __init__(self):
        pass

    def _nsplit(self, line, n, sep = ' '):
        x = tuple(line.split(sep, n))
        blanks = len(x) - n
        ret = x + ('',) * blanks
        return ret

    def _protoerror(self, response = ''):
        raise ProtoError('Unexpected response: %s' % (response,))

    def _failure(self, response = 'not found'):
        raise FailureError('Failure: %s' % (response,))

    def make_generic_handler(self, ok = None, full = None, error = None):
        """Create function to handle single word responses"""
        def handler(response):
            response = response.strip()
            if response == ok:
                return ('d', 0)
            elif response == full:
                handler.status = 'b'
                return ('b', 0)
            elif response == error:
                self._failure()
            else:
                self._protoerror(response)
        return handler

    def make_job_handler(self, ok = None, error = None):
        def handler(response):
            if not hasattr(handler, 'status'):
                infoline, rest = self._nsplit(response, 2, sep='\r\n')
                rword, jid, pri, size = self._nsplit(infoline, 4)
                if rword == ok:
                    handler.status = 'i'
                    size = int(size)
                elif rword == error:
                    self._failure(rword)
                else:
                    self._protoerror(response)
                data = rest
            else:
                data = handler.data + response
                pri = handler.pri
                jid = handler.jid
                size = handler.size

            if not len(data) < size:
                del handler.status
                return ('d', dict(pri = pri, id = jid, data = data))
            else:
                handler.pri = pri
                handler.jid = jid
                handler.size = size
                handler.data = data
                return ('i', (size + 2) - len(data))
        return handler

    def make_kick_handler(self):
        def handler(response):
            message, n = self._nsplit(response, 2, sep=' ')
            if message == 'KICKED':
                count = int(n.strip())
                return ('d', count)
            else:
                self._protoerror(response)
        return handler
        return handler

    def make_stats_handler(self):
        error = 'NOT_FOUND'
        ok = 'OK'

        def handler(response):
            if not hasattr(handler, status):
                startline, rest = self._nsplit(response, 2, sep='\r\n')
                rword, size = self._nsplit(startline, 2, sep=' ')
                if rword == ok:
                    handler.status = 'i'
                    size = int(size)
                elif rword == error:
                    self._failure(rword)
                else:
                    self._protoerror(response)
                data = rest
            else:
                data = handler.data + response
                size = handler.size

            if not len(data) < size:
                del handler.status
                return ('d', yaml.load(data.rstrip('\r\n')))
            else:
                handler.size = size
                handler.data = data
                return ('i', (size + 2) - len(data))
        return handler
        return handler

    def process_put(self, data, pri, delay):
        """
        put
            send:
                put <pri> <delay> <bytes>
                <data>

            return:
                INSERTED
                BURIED
        """
        dlen = len(data)
        putline = 'put %(pri)s %(delay)s %(dlen)s\r\n%(data)s\r\n'
        putline %= locals()
        handler = self.make_generic_handler(ok='INSERTED', full='BURRIED')
        return (putline, handler)

    def process_reserve(self):
        """
        reserve
            send:
                reserve

            return:
                RESERVED <id> <pri> <bytes>
                <data>
        """
        line = 'reserve\r\n'
        handler = self.make_job_handler(ok='RESERVED')
        return (line, handler)

    def process_delete(self, jid):
        """
        delete
            send:
                delete <id>

            return:
                DELETED
                NOT_FOUND
        """
        line = 'delete %s\r\n' % (jid,)
        handler = self.make_generic_handler(ok='DELETED', error='NOT_FOUND')
        return (line, handler)

    def process_release(self, jid, pri, delay):
        """
        release
            send:
                release <id> <pri> <delay>

            return:
                RELEASED
                BURIED
                NOT_FOUND
        """
        line = 'release %(jid)s %(pri)s %(delay)s\r\n' % locals()
        handler = self.make_generic_handler(ok='RELEASED', full='BURIED', error='NOT_FOUND')
        return (line, handler)

    def process_bury(self, jid, pri):
        """
        bury
            send:
                bury <id> <pri>

            return:
                BURIED
                NOT_FOUND
        """
        line = 'bury %(jid)s %(pri)s\r\n' % locals()
        handler = self.make_generic_handler(ok='BURIED', error='NOT_FOUND')
        return (line, handler)

    def process_peek(self, jid):
        """
        peek
            send:
                peek [<id>]

            return:
                NOT_FOUND
                FOUND <id> <pri> <bytes>
                <data>

        NOTE: as of beanstalk 0.5, peek without and id param will return the
              first burried job, this is extremely likely to change
        """
        if jid:
            line = 'peek %s\r\n' % (jid,)
        else:
            line = 'peek\r\n'
        handler = self.make_job_handler(ok='FOUND', error='NOT_FOUND')
        return (line, handler)

    def process_kick(self, bound):
        """
        kick
            send:
                kick <bound>

            return:
                KICKED <count>
        """
        line = 'kick %s\r\n' % (bound,)
        handler = self.make_kick_handler()
        return (line, handler)

    def process_stats(self, jid):
        """
        stats
            send:
                stats [<id>]

            return:
                YAML struct
        """
        if jid:
            line = 'stats %s\r\n' % (jid,)
        else:
            line = 'stats\r\n'
        handler = self.make_stats_handler()
        return (line, handler)

