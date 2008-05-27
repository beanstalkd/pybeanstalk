"""
Protocol handler for processing the beanstalk protocol

See reference at:
    http://xph.us/software/beanstalkd/protocol.txt (as of Feb 2, 2008)

This module contains a set of functions which will implement the protocol for
beanstalk.  The beanstalk protocol simple, consisting of a command and a
response. Each command is 1 line of text. Each response is 1 line of text, and
optionally (depending on the nature of the command) a chunk of data.
If the data is related to the beanstalk server, or its jobs, it is encoded
as a yaml file. Otherwise it is a raw character stream.

The implementation is designed so that there is a function for each possible
command line in the protocol. These functions return the command line, and a
function for handling the response. The handler will return a ditcionary
conatining the response. The handler is a generator that when fed data will
yeild None when more input is expected, and the results dict when all the data
is provided. Further, it has an attribute, remaining, which is an integer that
specifies how many bytes are still expected in the data portion of a reply.
"""

import yaml
import re, itertools
from functools import wraps
from errors import checkError
import errors

# default value on server
MAX_JOB_SIZE = (2**16) - 1

class ExpectedData(Exception): pass

def intit(val):
    try: return int(val)
    except: return val


class Handler(object):
    def __init__(self, ok, full=None, ok_args=[], full_args=[], \
        has_data=False, parse=(lambda x: x)):

        self.lookup = {ok:(ok_args,'ok')}
        if full:
            self.lookup[full] = (full_args, 'buried')

        self.has_data = has_data
        self.parse = parse
        self.remaining = 10

        h = self.handler()
        h.next()
        self.__h = h.send

    def __call__(self, val):
        return self.__h(val)

    def handler(self):
        eol = '\r\n'

        response = ''
        sep = ''

        while not sep:
            response += (yield None)
            response, sep, data = response.partition(eol)

        checkError(response)

        response = response.split(' ')
        word = response.pop(0)

        args, state = self.lookup.get(word, ([],''))

        # sanity checks
        if not state:
            errstr = "Repsonse was: %s %s" % (word, ' '.join(response))
        elif len(response) != len(args):
            errstr = "Repsonse %s had wrong # args, got %s (expected %s)"
            errstr %= (word, response, args)
        else: # all good
            errstr = ''

        if errstr: raise errors.UnexpectedResponse(errstr)

        reply = dict(itertools.izip(args, itertools.imap(intit, response)))
        reply['state'] = state

        if not self.has_data:
            self.remaining = 0
            yield reply
            return

        self.remaining = (reply['bytes'] + 2) - len(data)

        while self.remaining > 0:
            newdata = (yield None)
            self.remaining -= len(newdata)
            data += newdata

        if not data.endswith(eol) or not (len(data) == reply['bytes']+2):
            raise errors.ExpectedCrlf('Data not properly sent from server')

        reply['data'] = self.parse(data.rstrip(eol))
        yield reply
        return

# this decorator gets rid of a lot of cruft around protocol commands,
# making the protocol easier to read. The interaction decorator takes
# the args that describe the response, the following function only needs
# to create a command line to the server
def interaction(*args, **kw):
    def deco(func):
        @wraps(func)
        def newfunc(*fargs, **fkw):
            line = func(*fargs, **fkw)
            handler = Handler(*args, **kw)
            return (line, handler)
        return newfunc
    return deco

_namematch = re.compile(r'^[a-zA-Z0-9+\(\);.$][a-zA-Z0-9+\(\);.$-]{0,199}$')
def check_name(name):
    '''used to check the validity of a tube name'''
    if not _namematch.match(name):
        raise errors.BadFormat('Illegal name')

@interaction(ok='INSERTED', ok_args=['jid'], full='BURIED', full_args=['jid'])
def process_put(data, pri=1, delay=0, ttr=60):
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
    if dlen >= MAX_JOB_SIZE:
        raise errors.JobTooBig('Job size is %s (max allowed is %s' %\
            (dlen, MAX_JOB_SIZE))
    putline = 'put %(pri)s %(delay)s %(ttr)s %(dlen)s\r\n%(data)s\r\n'
    return putline % locals()

@interaction('USING', ok_args=['tube'])
def process_use(tube):
    '''
    use
        send:
            use <tube>
        return:
            USING <tube>
    '''
    check_name(tube)
    return 'use %s\r\n' % (tube,)

@interaction('RESERVED', ok_args=['jid','bytes'], has_data=True)
def process_reserve():
    '''
     reserve
        send:
            reserve

        return:
            RESERVED <id> <bytes>
            <data>

            DEADLINE_SOON
    '''
    x = 'reserve\r\n'
    return x

@interaction(ok='DELETED')
def process_delete(jid):
    """
    delete
        send:
            delete <id>

        return:
            DELETED
            NOT_FOUND
    """
    return 'delete %s\r\n' % (jid,)

@interaction(ok='RELEASED', full='BURIED')
def process_release(jid, pri=1, delay=0):
    """
    release
        send:
            release <id> <pri> <delay>

        return:
            RELEASED
            BURIED
            NOT_FOUND
    """
    return 'release %(jid)s %(pri)s %(delay)s\r\n' % locals()

@interaction(ok='BURIED')
def process_bury(jid, pri=1):
    """
    bury
        send:
            bury <id> <pri>

        return:
            BURIED
            NOT_FOUND
    """
    return 'bury %(jid)s %(pri)s\r\n' % locals()

@interaction(ok='WATCHING', ok_args=['count'])
def process_watch(tube):
    '''
    watch
        send:
            watch <tube>
        return:
            WATCHING <tube>
    '''
    check_name(tube)
    return 'watch %s\r\n' % (tube,)

@interaction(ok='WATCHING', ok_args=['count'])
def process_ignore(tube):
    '''
    ignore
        send:
            ignore <tube>
        reply:
            WATCHING <count>

            NOT_IGNORED
    '''
    check_name(tube)
    return 'ignore %s\r\n' % (tube,)

@interaction(ok='FOUND', ok_args=['jid','bytes'], has_data = True)
def process_peek(jid = 0):
    """
    peek
        send:
            peek <id>

        return:
            NOT_FOUND
            FOUND <id> <bytes>
            <data>

    """
    if jid:
        return 'peek %s\r\n' % (jid,)

@interaction(ok='FOUND', ok_args=['jid','bytes'], has_data = True)
def process_peek_ready():
    '''
    peek-ready
        send:
            peek-ready
        return:
            NOT_FOUND
            FOUND
            FOUND <id> <bytes>
    '''
    return 'peek-ready\r\n'

@interaction(ok='FOUND', ok_args=['jid','bytes'], has_data = True)
def process_peek_delayed():
    '''
    peek-delayed
        send:
            peek-delayed
        return:
            NOT_FOUND
            FOUND
            FOUND <id> <bytes>
    '''
    return 'peek-delayed\r\n'

@interaction(ok='FOUND', ok_args=['jid','bytes'], has_data = True)
def process_peek_buried():
    '''
    peek-buried
        send:
            peek-buried
        return:
            NOT_FOUND
            FOUND <id> <bytes>
    '''
    return 'peek-buried\r\n'

@interaction(ok='KICKED', ok_args = ['count'])
def process_kick(bound=10):
    """
    kick
        send:
            kick <bound>

        return:
            KICKED <count>
    """
    return 'kick %s\r\n' % (bound,)

@interaction(ok='OK', ok_args=['bytes'], has_data=True, parse=yaml.load)
def process_stats():
    """
    stats
        send:
            stats
        return:
            OK <bytes>
            <data> (YAML struct)
    """
    return 'stats\r\n'

@interaction(ok='OK', ok_args=['bytes'], has_data=True, parse=yaml.load)
def process_stats_job(jid):
    """
    stats
        send:
            stats-job <jid>
        return:
            OK <bytes>
            <data> (YAML struct)

            NOT_FOUND
    """
    return 'stats-job %s\r\n' % (jid,)

@interaction(ok='OK', ok_args=['bytes'], has_data=True, parse=yaml.load)
def process_stats_tube(tube):
    """
    stats
        send:
            stats-tube <tube>
        return:
            OK <bytes>
            <data> (YAML struct)

            NOT_FOUND
    """
    check_name(tube)
    return 'stats-tube %s\r\n' % (tube,)

@interaction(ok='OK', ok_args=['bytes'], has_data=True, parse=yaml.load)
def process_list_tubes():
    '''
    list-tubes
        send:
            list-tubes
        return:
            OK <bytes>
            <data> (YAML struct)
    '''
    return 'list-tubes\r\n'

@interaction('USING', ok_args = ['tube'])
def process_list_tube_used():
    '''
    list-tube-used
        send:
            list-tubes
        return:
            USING <tube>
    '''
    return 'list-tube-used\r\n'

@interaction(ok='OK', ok_args=['bytes'], has_data=True, parse=yaml.load)
def process_list_tubes_watched():
    '''
    list-tubes-watched
        send:
            list-tubes-watched
        return:
            OK <bytes>
            <data> (YAML struct)
    '''
    return 'list-tubes-watched\r\n'
