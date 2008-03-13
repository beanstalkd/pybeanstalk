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
import re, itertools, functools
from functools import wraps
from errors import checkError
import errors

class ExpectedData(Exception): pass

def intit(val):
    try: return int(val)
    except: return val

_namematch = re.compile(r'^[a-zA-Z0-9+\(\);.$][a-zA-Z0-9+\(\);.$-]{0,199}$')
def check_name(name):
    if not _namematch.match(name):
        raise errors.BadFormat('Illegal name')

def makeResponseHandler(ok, full=None, ok_args=[], full_args=[], \
        has_data=False, parse=(lambda x: x)):
    lookup = dict()

    lookup[ok] = (ok_args,'ok')
    if full: lookup[full] = (full_args, 'buried')

    def handler():
        eol = '\r\n'

        handler.remaining = 1
        response = ''
        data = ''

        trv = None
        while not response.endswith(eol):
            try:
                ndata = (yield trv)
                if ndata: response += ndata
                trv = None
            except ExpectedData:
                trv = handler.remaining
        response = response.rstrip(eol)
        checkError(response)

        response = response.split(' ')
        word = response.pop(0)

        args, state = lookup.get(word, ([],''))

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

        if not has_data:
            handler.remaining = 0
            yield reply
            return

        handler.remaining = reply['bytes'] + 2

        trv = None
        while handler.remaining > 0:
            try:
                newdata = (yield trv)
                handler.remaining -= len(newdata)
                data += newdata
                trv = None
            except ExpectedData:
                trv = handler.remaining

        if not data.endswith(eol) or not (len(data) == reply['bytes']+2):
            raise errors.ExpectedCrlf('Data not properly sent from server')

        reply['data'] = parse(data.rstrip(eol))
        yield reply
        return
    ret = handler()
    ret.next()
    return ret

# this decorator gets rid of a lot of cruft around protocol commands,
# making the protocol easier to read. The interaction decorator takes
# the args that describe the response, the following function only needs
# to create a command line to the server
def interaction(*args, **kw):
    def deco(func):
        @wraps(func)
        def newfunc(*fargs, **fkw):
            line = func(*fargs, **fkw)
            handler = makeResponseHandler(*args, **kw)
            return (line, handler)
        return newfunc
    return deco

def data_remaining(handler):
    return h.throw(ExpectedData)

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
    if dlen >= 2**16:
        raise errors.JobTooBig('Job size is %s (max allowed is %s' %\
            (dlen, 2**16))
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
            RESERVED <id> <pri> <bytes>
            <data>
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
            peek [<id>]

        return:
            NOT_FOUND
            FOUND <id> <pri> <bytes>
            <data>

    NOTE: as of beanstalk 0.5, peek without and id param will return the
          first buried job, this is extremely likely to change
    """
    if jid:
        return 'peek %s\r\n' % (jid,)
    else:
        return 'peek\r\n'

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
