import socket
import select
import random
import logging
import threading
import asyncore
import asynchat
import errno
import traceback
import time
import sys
import copy

import protohandler
from serverconn import ServerConn
from job import Job

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# This should be set to experiment with from the importing
# module.
# For example:
# from beanstalk import multiserverconn
# from multiserverconn import ServerPool ...
# multiserverconn.ASYNCORE_TIMEOUT = 5

ASYNCORE_TIMEOUT = 0.1
ASYNCORE_COUNT   = 10

class ServerInUse(Exception): pass

class AsyncServerConn(object, asyncore.dispatcher):
    def __init__(self, server, port, job = False):
        self.job = job
        self.server = server
        self.port = port

        self.__line = None
        self.__handler = None
        self.__result = None
        self.__waiting = False
        self.__mutex = threading.Lock()

        self._socket  = None
        asyncore.dispatcher.__init__(self)

    def __repr__(self):
        s = "<0x%(id)s %(object)s>"
        return s % {'id' : id(self), 'object' : self }

    def __str__(self):
        s = "%(class)s(%(ip)s:%(port)s#[%(active)s][%(waiting)s])"
        active_ = "Open" if self._socket else "Closed"
        waiting_ = "Waiting" if self.__waiting else "NotWaiting"
        return s % {"class" : self.__class__.__name__,
                    "active" : active_,
                    "ip" : self.server,
                    "port" : self.port,
                    "waiting" : waiting_}

    def __getattribute__(self, attr):
        res = getattr(protohandler, 'process_%s' % attr, None)
        if res:
            def caller(*args, **kw):
                logger.info("Calling %s on %r with args(%s), kwargs(%s)",
                             res.__name__, self, args, kw)

                func = self._do_interaction
                func(*res(*args, **kw))
                asyncore.loop(use_poll=True,
                              timeout=ASYNCORE_TIMEOUT,
                              count=ASYNCORE_COUNT)
                return self.result
            return caller

        return super(AsyncServerConn, self).__getattribute__(attr)

    def __eq__(self, comparable):
        #for unit testing
        assert isinstance(comparable, AsyncServerConn)
        return not any([cmp(self.server, comparable.server),
                        cmp(self.port, comparable.port)])

    def __assert_not_waiting(self):
        if self.waiting:
            raise ServerInUse, ("%s is currently in use!" % self, self)

    def _get_response(self):
        while True:
            recv = self._socket.recv(self.handler.remaining)

            if not recv:
                closedmsg = "Remote server %(server)s:%(port)s has "\
                            "closed connection" % { "server" : self.server,
                                                    "port" : self.port}
                self.close()
                raise protohandler.errors.NotConnected, (closedmsg, self)

            res = self.handler(recv)
            if res: break

        if self.job and 'jid' in res:
            res = self.job(conn=self,**res)
        return res

    def _do_interaction(self, line, handler):
        self.__assert_not_waiting()
        self.__result = None
        self.line = line
        self.handler = handler

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

    def threadsafe(func):
        def make_threadsafe(self, *args, **kwargs):
            logger.debug("Acquiring mutex")
            self.__mutex.acquire()
            try:
                logger.info("Calling %s with args: %s kwargs: %s",
                             func.__name__, args, kwargs)
                return func(self, *args, **kwargs)
            finally:
                logger.debug("Releasing mutex")
                self.__mutex.release()
        return make_threadsafe

    @threadsafe
    def _set_waiting(self, waiting):
        self.__waiting = waiting

    def _get_waiting(self):
        return self.__waiting

    waiting = property(_get_waiting, _set_waiting)

    @threadsafe
    def _set_line(self, line):
        self.__line = line

    def _get_line(self):
        return self.__line

    line = property(_get_line, _set_line)

    @threadsafe
    def _set_handler(self, handler):
        self.__handler = handler

    def _get_handler(self):
        return self.__handler

    handler = property(_get_handler, _set_handler)

    def _get_result(self):
        return self.__result

    result = property(_get_result)

    @property
    def tube(self):
        return self.list_tube_used()['tube']

    def post_connection(func):
        def handle_post_connect(self, *args, **kwargs):
            value = func(self, *args, **kwargs)
            protohandler.MAX_JOB_SIZE = self.stats()['data']['max-job-size']
            return value
        return handle_post_connect

    @post_connection
    def connect(self):
        # else the socket is not open at all
        # so, open the socket, and add it to the dispatcher
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_socket(self._socket) # set socket to asyncore.dispatcher
        self.set_reuse_addr() # try to re-use the address
        self._socket.connect((self.server, self.port))

    def interact(self, line):
        self.__assert_not_waiting()
        try:
            self._socket.sendall(line)
        except Exception, e:
            raise protohandler.errors.ProtoError(e)

    def close(self):
        self._socket.close()
        self.del_channel()
        # very important in python to set to None
        # if socket is not set to None, then it will try to re-use the same
        # file descriptor...
        self._socket = None

    def fileno(self):
        return self._socket.fileno()

    @threadsafe
    def handle_read(self):
        logger.info("Handling read on %s", self)
        try:
            self.__result = self._get_response()
            logger.info("Results are: %s", self.result)
        finally:
            # must make sure that waiting is set to false!
            self.__waiting = False

    @threadsafe
    def handle_write(self):
        logger.info("writing: %s to %s", self.line, self)
        self.interact(self.line)
        self.__waiting = True
        self.__line = None
        logger.info("Handled write")

    def handle_connect(self):
        logger.info("Connected to: %s", self)

    def handle_close(self):
        logger.info("Closing connection to: %s", self)

    def handle_error(self):
        # get exception information
        exctype, value = sys.exc_info()[:2]
        # if we disconnected..
        if exctype == protohandler.errors.NotConnected:
            msg, server = value
            logger.warn("Got %s, so, reconnecting to: %s", msg, server)
            assert server == self # sanity 
            # remove from the socket map
            del asyncore.socket_map[self.fileno()]
            # reconnect
            self.connect()
        else:
            raise
            asyncore.dispatcher.handle_error(self)

    def handle_accept(self):
        logger.info("Handle socket accept...")

    def readable(self):
        """This socket is only readable if it's waiting."""
        logger.debug("Checking if %s is readable.", self)
        return self.waiting

    def writable(self):
        """This socket is only writeable if something gave a line"""
        logger.debug("Checking if %s is writeable.", self)
        return self.line

class ServerPool(object):
    """ServerPool is a queue implementation of ServerConns with distributed
    server support.

    @serverlist is a list of tuples as so: (ip, port, job)

    """
    def __init__(self, serverlist):
        # build servers into the self.servers list
        self.servers = []
        for ip, port, job in serverlist:
            self.add_server(ip, port, job)

    def _get_watchlist(self):
        """Returns the global watchlist for all servers"""
        # TODO: it's late and I'm getting tired, going to just make
        # a list for now and see maybe later if I want to do a dict
        # with the server IPs as the keys as well as their watchlist..
        L = []
        for server in self.servers:
            L.extend(server.watchlist)
        return list(set(L))

    def _set_watchlist(self, value):
        """Sets the watchlist for all global servers"""
        for server in self.servers:
            server.watchlist = value

    watchlist = property(_get_watchlist, _set_watchlist)

    def _server_cmp(self, ip, port):
        def comparison(server):
            matching_port = 0
            if port:
                matching_port = cmp(server.port, port)
            return not any([cmp(server.server, ip), matching_port])
        return comparison

    def close(self):
        for server in self.servers:
            server.close()
        del self.servers[:]

    def clone(self):
        return ServerPool(map(lambda s: (s.server, s.port, s.job), self.servers))

    def get_random_server(self):
        #random seed by local time
        random.seed()
        try:
            choice = random.choice(self.servers)
        except IndexError, e:
            # implicitly convert IndexError to BeanStalkError
            NotConnected = protohandler.errors.NotConnected
            raise NotConnected("Not connected to a server!")
        else:
            return choice

    def remove_server(self, ip, port=None):
        """Removes the server from the server list and returns True on success.
        Else, if the target server doesn't exist, Returns false.

        If port is None, then all internal matching server ips are removed.

        """
        target = filter(self._server_cmp(ip, port), self.servers)
        if target:
            for t in target:
                t.close()
                self.servers.remove(t)
        return bool(target)

    def add_server(self, ip, port, job=Job):
        """Checks if the server doesn't already exist and adds it. Returns
        True on successful addition or False if the server already exists.

        Upon server addition, the server socket is automatically created
        and a connection is created.

        """
        target = filter(self._server_cmp(ip, port), self.servers)
        # if we got a server back
        if not target:
            server = AsyncServerConn(ip, port, job)
            server.pool_instance = self
            server.connect()
            self.servers.append(server)

        # return the opposite of target
        return not bool(target)

    def retry_until_succeeds(func):
        def retrier(self, *args, **kwargs):
            while True:
                try:
                    value = func(self, *args, **kwargs)
                except ServerInUse, e:
                    logger.exception(e[0])
                    # this should be caught in the function..
                    # clean this up a bit?
                    raise
                except protohandler.errors.Draining, e:
                    # ignore
                    pass
                except protohandler.errors.NotConnected, e:
                    # not connected..
                    logger.warning(e[0])
                    server = e[1]
                    self.remove_server(server.server, server.port)
                    logger.warn("Attempting to re-connect to: %s", server)
                    server.connect()
                    self.servers.append(server)
                else:
                    return value
        return retrier

    def multi_interact(self, line, handler):
        for server in self.servers:
            logger.warn("Sending %s to: %s", line, server)
            try:
                server._do_interaction(line, handler.clone())
            except ServerInUse, (msg, server):
                logger.info(msg)
                # ignore
                pass

        asyncore.loop(use_poll=True,
                      timeout=ASYNCORE_TIMEOUT,
                      count=ASYNCORE_COUNT)
        results = filter(None, (s.result for s in self.servers))
        return results

    @retry_until_succeeds
    def _all_broadcast(self, cmd, *args, **kwargs):
        """Broadcast to all servers and return the results in a compacted
        dictionary, where the keys are the server objects and the values are
        the result of the command.

        """
        func = getattr(protohandler, "process_%s" % cmd)
        return self.multi_interact(*func(*args, **kwargs))

    @retry_until_succeeds
    def _rand_broadcast(self, cmd, *args, **kwargs):
        """Randomly select a server from the pool of servers and broadcast
        the desired command.

        Retries if various error connections are encountered.
        """
        random_server = self.get_random_server()
        return getattr(random_server, cmd)(*args, **kwargs)

    @retry_until_succeeds
    def _first_broadcast_response(self, cmd, *args, **kwargs):
        """Broadcast to all servers and return the first valid server
        response.

        If no responses are found, return an empty list.

        This implementation actually just returns ONE response..

        """
        # TODO Fix this..
        result = self._all_broadcast(cmd, *args, **kwargs)
        if result:
            if isinstance(result, list):
                result = result[0]
            return result

        return []

    def put(self, *args, **kwargs):
        return self._rand_broadcast("put", *args, **kwargs)

    def reserve(self, *args, **kwargs):
        return self._all_broadcast("reserve", *args, **kwargs)

    def reserve_with_timeout(self, *args, **kwargs):
        return self._all_broadcast("reserve_with_timeout", *args, **kwargs)

    def use(self, *args, **kwargs):
        return self._all_broadcast("use", *args, **kwargs)

    def peek(self, *args, **kwargs):
        return self._all_broadcast("peek", *args, **kwargs)

    def peek_delayed(self, *args, **kwargs):
        return self._first_broadcast_response("peek_delayed", *args, **kwargs)

    def peek_buried(self, *args, **kwargs):
        return self._first_broadcast_response("peek_buried", *args, **kwargs)

    def peek_ready(self, *args, **kwargs):
        return self._first_broadcast_response("peek_ready", *args, **kwargs)

    def combine_stats(func):
        def combiner(self, *args, **kwargs):
            appendables = set(['name', 'version', 'pid'])
            returned = func(self, *args, **kwargs)

            if not returned:
                return {}

            # need to combine these results
            result = dict(reduce(lambda x, y: x.items() + y.items(), returned))
            # set-ify that which we want to add
            for a in appendables:
                try:
                    result['data'][a] = set(map(lambda x: x['data'][a],
                                                returned))
                except KeyError:
                    # if the appendable key isnt in the result..
                    # ignore
                    pass

            del returned[:]
            return result
        return combiner

    @combine_stats
    def stats(self, *args, **kwargs):
        return self._all_broadcast("stats", *args, **kwargs)

    @combine_stats
    def stats_tube(self, *args, **kwargs):
        return self._all_broadcast("stats_tube", *args, **kwargs)

    @retry_until_succeeds
    def apply_and_compact(func):
        """Applies func's func.__name__ to all servers in the server pool
        and tallies results into a dictionary.

        Returns a dictionary of all results tallied into one.

        """
        def generic_applier(self, *args, **kwargs):
            cmd = func.__name__
            results = {}
            for server in self.servers:
                results[server] = getattr(server, cmd)(*args, **kwargs)
            return results
        return generic_applier

    @apply_and_compact
    def list_tubes(self, *args, **kwargs):
        # instead of having to repeating myself by writing an iteration of
        # all servers to execute and store results in a hash, the decorator 
        # apply_and_compact will apply the function name (e.g. list_tubes)
        # to all servers and compact/tally them all into a dictionary
        #
        # we don't use multi_interact here because we don't need to handle
        # a response explicitly
        pass

    @apply_and_compact
    def list_tube_used(self, *args, **kwargs):
        pass

    @apply_and_compact
    def list_tubes_watched(self, *args, **kwargs):
        pass

    @property
    def tubes(self):
        """Returns a amalgamated list of tubes used on all servers

        For unit tests, this should be converted to a set.

        """
        return [tubes['tube'] for tubes in self.list_tube_used().itervalues()]
