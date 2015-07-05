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

This may seem a bit round-about, but it allows for many different styles* of
programming to use the same bit of code for implementing the protocol.

* e.g. the simple syncronous connection and the twisted client both use this :)

NOTE: there are mre lines of documentation in this file than lines of code.
It may be that I need to practice terseness in this form as much as i do with
my code...
"""


import StringIO
import re
from itertools import izip, imap
from functools import wraps

import yaml

import errors
from errors import checkError

# default value on server
MAX_JOB_SIZE = (2**16) - 1

def load_yaml(yaml_string):
    handler = StringIO.StringIO(yaml_string)
    return yaml.load(handler)


def protProvider(cls):
    ''' Class decorator to be applied to anything that we want to provide the
    beanstalk protocol (e.g. connections).  This will implement all the
    protocol functions (i.e. process_*) as methods in the class that is
    decorated. in ver < py2.6 this should be cls = protProvider(cls), in
    2.6 and higher, they got all nice and implemented the decorator sugar for
    classes'''
    for name, value in globals().items():
        if not name.startswith('process_'):
            continue
        name = name.partition('_')[2]
        setattr(cls, name, staticmethod(value))

    return cls

class ExpectedData(Exception): pass

class Response(object):
    '''This is a simple object for describing the expected response to a
    command. It is intended to be subclassed, and the subclasses to be named
    in such a way as to describe the response.  For example, I've used
    OK for the expected normal response, and Buried for the cases where
    a command can result in a burried job.

    Arguments/attributes:
        word: the first word sent back from the server (eg OK)
        args: the server replies with space separated positional arguments,
              this describes the names of those argumens
        hasData: boolean stating whether or not to expect a data stream after
                 the response line
        parsefunc: a function, used to transform the data. This will be called
                   just prior to returning the dict, and its result will
                   be under the key 'data'
    '''

    def __init__(self, word, args =None , hasData = False, parsefunc = None):
        self.word = word
        self.args = args if args else []
        self.hasData = hasData
        if parsefunc:
            self.parsefunc = parsefunc
        else:
            self.parsefunc = (lambda x: x)

    def __str__(self):
        '''will fail if attr name hasnt been set by subclass or program'''
        return self.__class__.__name__.lower()

class OK(Response): pass
class TimeOut(Response): pass
class Buried(Response): pass

def intit(val):
    try: return int(val)
    except: return val


class Handler(object):
    '''
    Handler: generic response consumer for beanstalk.

    Each handler object has a __call__ method, allowing it to be fed data.
    '''
    def __init__(self, *responses):

        self.lookup =  dict((r.word, r) for r in responses)
        self.remaining = 10

        h = self.handler()
        h.next()
        self.__h = h.send

    def clone(self):
        """Clone the handler

        This method is primarily used in the distributed client to pass fresh
        generators to handle incoming data buffers.

        """
        return Handler(*self.lookup.values())

    def __call__(self, val):
        return self.__h(val)

    # Note: this takes advanage of 2.5+ style generators. The syntax:
    # x = (yield value)
    # yields the value, and expects x.send(foo) to be called. Foo will be
    # assigned to x.
    def handler(self):
        eol = '\r\n'

        response = ''
        sep = ''

        # TODO: figure out the max possible response line, and set the default
        # remaining to that amount. check for sep or that amount of data...
        # its a bit of a sanity check, as this could be attacked.
        #
        while not sep:
            response += (yield None)
            response, sep, data = response.partition(eol)

        checkError(response)

        response = response.split(' ')
        word = response.pop(0)

        resp = self.lookup.get(word, None)

        # sanity checks
        if not resp:
            errstr = "Response was: %s %s" % (word, ' '.join(response))
        elif len(response) != len(resp.args):
            errstr = "Response %s had wrong # args, got %s (expected %s)"
            errstr %= (word, response, args)
        else: # all good
            errstr = ''

        if errstr: raise errors.UnexpectedResponse(errstr)

        reply = dict(izip(resp.args, imap(intit, response)))
        reply['state'] = str(resp)

        if not resp.hasData:
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

        reply['data'] = resp.parsefunc(data.rstrip(eol))
        yield reply
        return

# since the beanstalk protocol uses a simple command-response structure,
# this decorator makes life easy.  The function it wraps corresponds to a
# beanstalk command, and returns the appropriate command text.
# This decorator sets up the structure for handling a response.
#
# One thing that may be a bit tricky for those who aren't familiar with python
# decorators: This changes return value for command function. The functions
# return a single sting, but after decoration return a tuple of:
#      (string, handler)
def interaction(*responses):
    '''Decorator-factory for process_* protocol functions. Takes N response objects
    as arguments, and returns decorator.

    The decorator replaces the wrapped function, and returns the result of
    the original function, as well as a response handler set up to use the
    expected responses.'''
    def deco(func):
        @wraps(func)
        def newfunc(*args, **kw):
            line = func(*args, **kw)
            handler = Handler(*responses)
            return (line, handler)
        return newfunc
    return deco

_namematch = re.compile(r'^[a-zA-Z0-9+\(\)/;.$_][a-zA-Z0-9+\(\)/;.$_-]{0,199}$')
def check_name(name):
    '''used to check the validity of a tube name'''
    if not _namematch.match(name):
        raise errors.BadFormat('Illegal name')

@interaction(OK('INSERTED',['jid']), Buried('BURIED', ['jid']))
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

@interaction(OK('USING', ['tube']))
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

@interaction(OK('RESERVED', ['jid','bytes'], True))
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

@interaction(OK('RESERVED', ['jid','bytes'], True), TimeOut('TIMED_OUT'))
def process_reserve_with_timeout(timeout=0):
    '''
     reserve
        send:
            reserve-with-timeout <timeout>

        return:
            RESERVED <id> <bytes>
            <data>

            TIME_OUT

            DEADLINE_SOON
    Note: After much internal debate I chose to go this route,
    with hte one-to-one mappaing of function to protocol command. Higher level
    objects, like the connection objects, can combine these if they see fit.
    '''
    if int(timeout) < 0:
        raise AttributeError('timeout must be greater than 0')
    return 'reserve-with-timeout %s\r\n' % (timeout,)

@interaction(OK('DELETED'))
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

@interaction(OK('RELEASED'), Buried('BURIED'))
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

# XXX: semantic question: is this better being an OK since burried is the
# expected response. Or is it better being a burried since it is the more
# accurate description, but breaking the rest of semantics of the code?
#   ^^ this isnt a pressing issue, because for now we still return a
#      state string pair, which is currently backwards compatible
@interaction(OK('BURIED'))
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

@interaction(OK('WATCHING', ['count']))
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

@interaction(OK('WATCHING', ['count']))
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

@interaction(OK('FOUND', ['jid','bytes'], True))
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

@interaction(OK('FOUND', ['jid','bytes'], True))
def process_peek_ready():
    '''
    peek-ready
        send:
            peek-ready
        return:
            NOT_FOUND
            FOUND <id> <bytes>
    '''
    return 'peek-ready\r\n'

@interaction(OK('FOUND', ['jid','bytes'], True))
def process_peek_delayed():
    '''
    peek-delayed
        send:
            peek-delayed
        return:
            NOT_FOUND
            FOUND <id> <bytes>
    '''
    return 'peek-delayed\r\n'

@interaction(OK('FOUND', ['jid','bytes'], True))
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

@interaction(OK('KICKED', ['count']))
def process_kick(bound=10):
    """
    kick
        send:
            kick <bound>

        return:
            KICKED <count>
    """
    return 'kick %s\r\n' % (bound,)

@interaction(OK('TOUCHED'))
def process_touch(jid):
    """
    touch
        send:
            touch <job>

        return:
            TOUCHED
            NOT_FOUND
    """
    return 'touch %s\r\n' % (jid,)

@interaction(OK('OK', ['bytes'], True, load_yaml))
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


@interaction(OK('OK', ['bytes'], True, load_yaml))
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

@interaction(OK('OK', ['bytes'], True, load_yaml))
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

@interaction(OK('OK', ['bytes'], True, load_yaml))
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

@interaction(OK('USING', ['tube']))
def process_list_tube_used():
    '''
    list-tube-used
        send:
            list-tubes
        return:
            USING <tube>
    '''
    return 'list-tube-used\r\n'

@interaction(OK('OK', ['bytes'], True, load_yaml))
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
