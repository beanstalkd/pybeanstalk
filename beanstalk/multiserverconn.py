import socket
import select
import random
import logging
import threading
import asyncore
import asynchat

import protohandler
from serverconn import ServerConn
from job import Job

_debug = False
logger = logging.getLogger(__name__)

class ServerInUse(Exception): pass

class AsyncServerConn(object):
    def __init__(self, server, port, job = False):
        self.poller = getattr(select, 'poll', lambda : None)()
        self.job = job
        self.server = server
        self.port = port

        self._waiting = False
        self._mutex = threading.Lock()
        self._socket  = None

    def __repr__(self):
        s = "<[%(active)s][%(waiting)s]%(class)s(%(ip)s:%(port)s)>"
        active_ = "Open" if self._socket else "Closed"
        return s % {"class" : self.__class__.__name__,
                    "active" : active_,
                    "ip" : self.server,
                    "port" : self.port,
                    "waiting" : self._waiting}

    def __getattribute__(self, attr):
        res = getattr(protohandler, 'process_%s' % attr, None)
        if res:
            def caller(*args, **kw):
                logger.info("Calling %s on %s with args(%s), kwargs(%s)",
                             res.__name__, self, args, kw)

                func = self._do_interaction
                return func(*res(*args, **kw))

            return caller

        return super(AsyncServerConn, self).__getattribute__(attr)

    def __eq__(self, comparable):
        #for unit testing
        assert isinstance(comparable, ServerConn)
        return not any([cmp(self.server, comparable.server),
                        cmp(self.port, comparable.port)])

    def __assert_not_waiting(self):
        if self.waiting:
            raise ServerInUse("%s is currently in use!" % self)

    def _get_response(self, handler):
        data = ''
        pcount = 0
        while True:
            if _debug and self.poller and not self.poller.poll(1):
                pcount += 1
                if pcount >= 20:
                    raise Exception('poller timeout %s times in a row' % (pcount,))
                else: continue
            pcount = 0

            recv = self._socket.recv(handler.remaining)
            if not recv:
                closedmsg = "Remote server %(server)s:%(port)s has "\
                            "closed connection" % { "server" : server.ip,
                                                    "port" : server.port}
                self.close()
                raise protohandler.errors.ProtoError(closedmsg)
            res = handler(recv)
            if res: break

        if self.job and 'jid' in res:
            res = self.job(conn=self,**res)
        return res

    def _do_interaction(self, line, handler):
        self.__assert_not_waiting()
        self._mutex.acquire()
        try:
            self.interact(line)
            return self._get_response(handler)
        finally:
            self._mutex.release()

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

    def _set_waiting(self, waiting):
        self._waiting = waiting

    def _get_waiting(self):
        return self._waiting

    waiting = property(_get_waiting, _set_waiting)

    @property
    def tube(self):
        return self.list_tube_used()['tube']

    def connect(self):
        self._socket = socket.socket()
        self._socket.connect((self.server, self.port))
        if self.poller:
            self.poller.register(self._socket, select.POLLIN)
        protohandler.MAX_JOB_SIZE = self.stats()['data']['max-job-size']

    def interact(self, line):
        self.__assert_not_waiting()
        try:
            self._socket.sendall(line)
        except:
            raise protohandler.errors.ProtoError

    def close(self):
        self.poller.unregister(self._socket)
        self._socket.close()

    def fileno(self):
        return self._socket.fileno()

    def handle_read(self):
        pass
    def handle_write(self):
        pass
    def handle_connect(self):
        pass
    def handle_close(self):
        pass
    def handle_error(self):
        pass
    def handle_accept(self):
        pass
    def readable(self):
        pass
    def writable(self):
        pass

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
            server = AsyncServerConn(ip, port, job)
            server.pool_instance = self
            server.connect()
            self.servers.append(server)

        #return teh opposite of target
        return not bool(target)

    def multi_interact(self, line, handler):
        serverlist = []
        for server in self.servers:
            print "Sending :", line, " to ", server
            try:
                server.interact(line)
            except ServerInUse, e:
                #continue and ignore
                print e
            else:
                #successfully wrote to this server, so append to server list
                serverlist.append(server)

        try:
            return self.__handle_responses(serverlist, handler)
        finally:
            del serverlist[:]

    def __handle_responses(self, serverlist, handler):

        if not serverlist:
            print "No server was free..."
            return None

        for server in serverlist:
            server.waiting = True

        try:
            responses = select.select(serverlist, [], [])
        except IOError, e:
            print e, dir(e)
            if e != 4:
                raise

        print responses

        results = []
        #get all servers who are ready to be READ from
        for server in responses[0]:
            server._mutex.acquire()
            try:
                while True:
                    recv = server._socket.recv(handler.remaining)
                    if not recv:
                        closedmsg = "Remote server %(server)s:%(port)s has "\
                                    "closed connection" % { "server" : server.ip,
                                                        "port" : server.port}

                        self.remove_server(server.ip, server.port)
                        print closedmsg
                    res = handler(recv)
                    if res: break

                if server.job and 'jid' in res:
                    res = server.job(conn=server, **res)

                results.append(res)
            finally:
                server._mutex.release()
                server.waiting = False

        if len(results) == 1:
            results = results[0]

        return results

    def retry_until_succeeds(func):
        def retrier(self, *args, **kwargs):
            while True:
                try:
                    value = func(self, *args, **kwargs)
                except ServerInUse, e:
                    print e
                else:
                    return value
        return retrier

    @retry_until_succeeds
    def _all_broadcast(self, cmd, *args, **kwargs):
        func = getattr(protohandler, "process_%s" % cmd)
        return self.multi_interact(*func(*args, **kwargs))

    @retry_until_succeeds
    def _rand_broadcast(self, cmd, *args, **kwargs):
        random_server = self.get_random_server()
        return getattr(random_server, cmd)(*args, **kwargs)

    def _all_broadcast_get_first_response(self, cmd, *args, **kwargs):
        pass

    def put(self, *args, **kwargs):
        return self._rand_broadcast("put", *args, **kwargs)

    def reserve(self, *args, **kwargs):
        #reserve on all the connections
        return self._all_broadcast("reserve", *args, **kwargs)

    def reserve_with_timeout(self, *args, **kwargs):
        return self._all_broadcast("reserve_with_timeout", *args, **kwargs)

    def use(self, *args, **kwargs):
        return self._all_broadcast("use", *args, **kwargs)

    def peek(self, *args, **kwargs):
        return self._all_broadcast("peek", *args, **kwargs)

    def peek_delayed(self, *args, **kwargs):
        return self._all_broadcast("peek_delayed", *args, **kwargs)

    def peek_buried(self, *args, **kwargs):
        return self._all_broadcast("peek_buried", *args, **kwargs)

    def peek_ready(self, *args, **kwargs):
        return self._all_broadcast("peek_ready", *args, **kwargs)

    def stats(self, *args, **kwargs):
        return self._all_broadcast("stats", *args, **kwargs)

    def stats_tube(self, *args, **kwargs):
        return self._all_broadcast("stats_tube", *args, **kwargs)

    def list_tubes(self, *args, **kwargs):
        for server in self.servers:
            server.list_tubes(*args, **kwargs)

    def list_tubes_used(self, *args, **kwargs):
        for server in self.servers:
            server.list_tubes_used(*args, **kwargs)

    def list_tubes_watched(self, *args, **kwargs):
        for server in self.servers:
            server.list_tubes_watched(*args, **kwargs)

    def close(self):
        for server in self.servers:
            server.close()
        del self.servers[:]

    def clone(self):
        return ServerPool(map(lambda s: (s.ip, s.port, s.job), self.servers))

