class proto(object):
    def __init__(self, serialize=str):
        self.serialize = serialize

    def _nsplit(self, line, n, sep = ' '):
        x = line.split(sep, n)
        blanks = len(x) - n
        ret = x + ('',)*blanks
        return ret

    def make_generic_handler(self, ok=None, full=None, error=None):
        """Create function to handle single word responses"""
        def handler(response):
            response = response.strip()
            if response == ok:
                return True
            elif response == full:
                handler.status = 'b'
                return False
            elif response = error:
                raise JobError("You dont own that job, or it doesnt exist")
            else:
                raise ProtoError(response)
        return handler

    def make_job_handler(self, ok = None, error = None):
        def handler(response):
            if not hassattr(handler, 'status'):
                done = False
                infoline, rest = self._nsplit(response, 2, sep = '\r\n')
                rword, jid, pri, size = self._nsplit(infoline, 4)
                if rword == ok:
                    handler.status = 'i'
                elif rword == error:
                    raise JobError(rword)
                else:
                    raise ProtocolError('unexpected response: %s' response)
                data = rest
            else:
                data = handler.data + response
                pri = handler.pri
                jid = handler.jid
                size = handler.size

            if len(data) = size:
                del handler.status
                return self.new_job(pri = pri, id = jid, data = data)
            else:
                handler.pri = pri
                handler.jid = jid
                handler.size = size
                handler.data = data
                return False

    def process_put(self, data, pri=0, delay=0):
        data = self.serialize(data)
        dlen = len(data)
        putline = 'put %(pri)s %(delay)s %(dlen)s\r\n%(data)s\r\n'
        putline %= locals()
        handler = self.make_generic_handler(ok='INSERTED',full='BURRIED')
        return (putline, handler)

    def process_reserve(self):
        line = 'reserve\r\n'
        handler = self.make_job_handler('RESERVED')
        return (line, handler)

    def process_delete(self, id):
        line = 'delete %s\r\n' % (id,)
        handler = self.make_generic_handler(ok='DELETED',error='NOT_FOUND')
        return (line, handler)

    def process_release(self, id, pri=0, delay=0):
        line = 'release %(id)s %(pri)s %(delay)s\r\n' % locals()
        handler = self.make_generic_handler(ok='RELEASED', full='BURIED', error='NOT_FOUND')
        return (line, handler)

    def process_bury(self, id, pri=0):
        line = 'bury %(id)s %(pri)s\r\n' % locals()
        handler = self.make_generic_handler(ok='BURIED', error='NOT_FOUND')
        return (line, handler)

    def process_peek(self, id=0)
        if id:
            line = 'peek %s\r\n' % (id,)
        else:
            line = 'peek\r\n
        handler = self.make_job_handler(ok = 'FOUND', error = 'NOT_FOUND')
        return (line, handler)

    def
x = proto()
x.process_put('foobarbaz')

