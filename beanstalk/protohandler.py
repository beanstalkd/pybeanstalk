import yaml
from errors import checkError
import errors

def nsplit(line, n, sep = ' '):
    x = tuple(line.split(sep, n))
    blanks = len(x) - n
    ret = x + ('',) * blanks
    return ret

def makeResponseHandler(ok, full=None, ok_args=[], full_args=[], \
        expect_more=False, handle_extra=(lambda x: x)):
    lookup = dict()
    if ok and expect_more:
        lookup[ok] = (ok_args,'ok', 'i')
    elif ok:
        lookup[ok] = (ok_args, 'ok', 'd')

    if full:
        lookup[full] = (full_args, 'buried', 'd')

    def handler(response):
        # needs to be a full beanstalk proto line, with crlf stripped off
        checkError(response)
        response = response.split(' ')
        word = response.pop(0)
        try:
            args, state, status = lookup[word]
        except KeyError:
            raise errors.UnexpectedResponse("Repsonse was: %s %s" % (word, ' '.join(response)))

        if len(response) == len(args):
            reply = dict(parse_data=handle_extra)
            reply['state'] = state
            for x in args:
                n = response.pop(0)
                try: n = int(n)
                except: pass
                reply[x] = n
            return (status, reply)
        else:
            raise errors.UnexpectedResponse("Repsonse %s had wrong args, got %s (expected %s)" %\
                (word, len(repsonse), len(args)))
    return handler

class Proto(object):
    """
        Protocol handler for processing the beanstalk protocol

        See reference at:
            http://xph.us/software/beanstalkd/protocol.txt (as of Feb 2, 2008)

    """

    def __init__(self):
        pass

    def process_put(self, data, pri, delay, ttr):
        """
        put
            send:
                put <pri> <delay> <ttr> <bytes>
                <data>

            return:
                INSERTED <jid>
                BURIED <jid>
        NOTE: this function does a check for job size <= max job size, and
        raises a protocol error when the size is too big.
        """
        dlen = len(data)
        if dlen >= 2**16:
            raise errors.JobTooBig('Job size is %s (max allowed is %s' %\
                (dlen, 2**16))
        putline = 'put %(pri)s %(delay)s %(ttr)s %(dlen)s\r\n%(data)s\r\n'
        putline %= locals()
        handler = makeResponseHandler(ok='INSERTED', ok_args=['jid'],
            full='BURIED', full_args=['jid'])
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
        handler = makeResponseHandler('RESERVED', ok_args=['jid','pri','bytes'],
            expect_more=True)

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
        handler = makeResponseHandler(ok='DELETED')

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
        handler = makeResponseHandler(ok='RELEASED', full='BURIED')
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
        handler = makeResponseHandler(ok='BURIED')
        return (line, handler)

    def process_peek(self, jid = 0):
        """
        peek
            send:
                peek [<id>]

            return:
                NOT_FOUND
                FOUND <id> <pri> <bytes>
                <data>

        NOTE: as of beanstalk 0.5, peek without and id param will return the
              first buried job, this is extremely likely to change
        """
        if jid:
            line = 'peek %s\r\n' % (jid,)
        else:
            line = 'peek\r\n'
        handler = makeResponseHandler(ok='FOUND',
            ok_args=['jid','pri','bytes'], expect_more = True)
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
        handler = makeResponseHandler(ok='KICKED', ok_args = ['count'])
        return (line, handler)

    def process_stats(self, jid = 0):
        """
        stats
            send:
                stats [<id>]

            return:
                OK <bytes>
                <data> (YAML struct)
        """
        if jid:
            line = 'stats %s\r\n' % (jid,)
        else:
            line = 'stats\r\n'
        handler = makeResponseHandler(ok='OK', ok_args=['bytes'],
            expect_more=True, handle_extra=yaml.load)

        return (line, handler)
