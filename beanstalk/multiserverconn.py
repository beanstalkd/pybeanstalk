import socket
import select
import random
import logging

import protohandler
from serverconn import ServerConn
from job import Job

_debug = False
logger = logging.getLogger(__name__)

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

