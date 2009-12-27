import socket
import select
import random
import logging
import asyncore
import threading

import protohandler
from serverconn import ServerConn
from job import Job

_debug = False
logger = logging.getLogger(__name__)

class ServerIsWaiting(Exception): pass

class AsyncServerConn(ServerConn):
    """Similar to the ServerConn object, but uses asyncronous I/O instead of
    blocking

    """
    def __init__(self, server, port, job = False):
        super(AsyncServerConn, self).__init__(sever, port, job)
        self.__mutex = threading.Lock()
        self.__waiting = False
        self.__operation = None

    def __getattribute__(self, attr):
        res = super(ServerConn, self).__getattribute__(attr)
        if not hasattr(res, "__name__") or
           not res.__name__.startswith('process_'):
            return res

        def caller(*args, **kw):
            self.__operation = attr
            logger.info("[%s]Calling %s with: args(%s), kwargs(%s)",
                         attr, res.__name__, args, kw)
            return self._do_interaction(*res(*args, **kw))
        return caller

    def __repr__(self):
        return super(AsyncServerConn, self).__repr__()

    def __assert_server_not_waiting(self):
        if self.__waiting:
            raise ServerIsWaiting("%s currently waiting on a job!", self)

    def __writeline(self, line):
        return super(AsyncServerConn, self).__writeline(line)

    def __handle_reserve(self, handler):
        self.__assert_server_not_waiting()
        exception_thrown = False
        try:
            self.__waiting = True
            self.poller.select([self._socket])
        except (IOError, ServerIsWaiting), e:
            exception_thrown = True
        finally:
            self.__waiting = False

        if not exception_thrown:
            return super(AsyncServerConn, self)._get_response(handler)
        else:
            return None

    def _get_response(handler):
        #we do this here because we want to select on multiple fd instead
        #of doing a receive on the socket for infinite blocking
        if self.__operation in ["reserve", "reserve_with_timeout"]:
            self.__handle_reserve(handler)
        else:
            return super(AsyncServerConn, self)._get_response(handler)

    def _do_interaction(self, line, handler):
        self.__assert_server_not_waiting()
        self.__mutex.acquire()
        try:
            self.__writeline(line)
            return self._get_response(handler)
        finally:
            self.__mutex.release()


#TODO: Confirm that this is thread safe. There's no need to 
#block here at all
class ServerPool(object):
    """ServerPool is a queue implementation of ServerConns with distributed
    server support.

    @serverlist is a list of tuples as so: (ip, port, job)

    """
    def __init__(self, serverlist):
        #build servers into the self.servers list
        self.servers = []
        for ip, port, job in serverlist:
            self.add_server(ip, port, job)

    def __getattribute__(self, attr):
        try:
            res = super(ServerPool, self).__getattribute__(attr)
        except AttributeError:
            logger.debug("Attribute '%s' NOT found, delegating...", attr)
            pass
        else:
            logger.debug("Attribute found: %s...", res)
            return res

        random_server = self.get_random_server()
        logger.debug("Returning %s from %s", attr, random_server)
        return getattr(random_server, attr)

    def use(self, *args, **kwargs):
        """Use is overridden because we want to broadcast it to all our
        internal servers

        """
        for server in self.servers:
            server.use(*args, **kwargs)

    def ___do_interaction(self, line, handler):
        #for all servers, send them the line that we're looking
        #for
        for server in self.servers:
            server.io.send(line)

        #start the async loop to listen
        asyncore.loop()

    def _get_watchlist(self):
        """Returns the global watchlist for all servers"""
        #TODO: it's late and I'm getting tired, going to just make
        #a list for now and see maybe later if I want to do a dict
        #with the server IPs as the keys as well as their watchlist..
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
            matching_port = True
            if port:
                matching_port = cmp(server.port, port)
            return cmp(server.server, ip) and matching_port
        return comparison

    def get_random_server(self):
        #random seed by local time
        random.seed()
        return random.choice(self.servers)

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
        #if we got a server back
        if not target:
            self.servers.append(ServerConn(ip, port, job))
        #return teh opposite of target
        return not bool(target)

