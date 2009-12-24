import socket
import select
import random
import logging

import protohandler
from serverconn import ServerConn

_debug = False
logger = logging.getLogger(__name__)


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

        #random seed by local time
        random.seed()

    def __getattribute__(self, attr):
        logger.debug("Fetching: %s", attr)
        try:
            res = super(ServerPool, self).__getattribute__(attr)
        except AttributeError:
            logger.debug("Attribute '%s' NOT found, delegating...", attr)
        else:
            logger.debug("Attribute found: %s...", res)
            return res
        
        random_server = random.choice(self.servers)
        logger.debug("Returning %s from %s", attr, random_server)
        return getattr(random_server, attr)

    def ___writeline(self, line):
        #TODO: implement round robin, although honestly, random is 
        #slighly better
        try:
            #write a line by picking a random server
            random_server = random.choice(self.servers)
            retry = 0
            while not random_server.socket:
                if retry == 5: 
                    raise protohandler.errors.ProtoError("Not ready!")
                retry += 1
                random_server = random.choice(self.servers)
            print "&& Sending ", line, " to ", random_server
            #check if the server is connected here as well? 
            random_server.socket.sendall(line)
            #return the random_server that we sent to
            return random_server
        except Exception, e:
            raise protohandler.errors.ProtoError(str(e))

    def __do_interaction(self, line, handler):
        server = self.__writeline(line)
        return self._get_response(server, handler)

    def __get_response(self, server, handler):
        pcount = 0
        while True:
            if _debug and self.poller and not self.poller.poll(1):
                pcount += 1
                if pcount >= 20:
                    raise Exception('poller timeout %s times in a row' % (pcount,))
                else: continue
            pcount = 0
            recv = server.socket.recv(handler.remaining)
            if not recv:
                closedmsg = "Remote server %(server)s:%(port)s has "\
                            "closed connection" % { "server" : server.ip, 
                                                    "port" : server.port}
                #remove from poller
                self.poller.unregister(server.socket)
                #close the socket
                server.close_connection()
                try:
                    #try to remove from the server list
                    self.servers.remove(server)
                except ValueError, e:
                    #woah! wtf? server isnt in the server list
                    raise protohandler.errors.ProtoError(str(e))
                #let's check to make sure that servers isnt empty.
                if not self.servers:
                    msg = "Listening server list is empty." + closedmsg
                    raise protohandler.errors.ProtoError(msg)
                #else just raise nor
                raise protohandler.errors.ProtoError(closedmsg)
            res = handler(recv)
            if res: break

        if self.job and 'jid' in res:
            res = self.job(conn=self,**res)
        return res

    def _server_cmp(self, ip, port):
        def comparison(server):
            matching_port = True
            if port:
                matching_port = cmp(server.port, port)
            return cmp(server.server, ip) and matching_port
        return comparison

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

    def add_server(self, ip, port, job=None):
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


