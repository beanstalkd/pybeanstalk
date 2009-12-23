import socket
import select
import random

from protohandler.errors import ProtoError
from serverconn import ServerConn

_debug = False

class Server(object):
    """An incredibly lightweight implementation of a object that only has
    the desired properties. 

    Slots is used here because there can literally be millions of beanstalk
    servers, so this is to save some memory overhead. Since we're not worried
    about pickling these objects, __slots__ is actually a great way to save
    a few bytes of memory.

    """
    __slots__ = ("ip", "port", "socket")
    
    def __init__(self, ip=None, port=None, socket=None):
        for arg, value in locals().iteritems():
            if arg in self.__slots__: 
                setattr(self, arg, value)
    
    def __cmp__(self, comparable):
        #for unit testing
        assert isinstance(comparable, Server)
        return not any([cmp(self.ip, comparable.ip),
                        cmp(self.port, comparable.port)])
                        
    def close_connection(self):
        """Closes the socket connection if open"""
        if self.socket:
            self.socket.close()



class ServerPool(ServerConn):
    """ServerPool is a concrete implementation of ServerConn with distributed
    server support.

    @serverdict is a dictionary consisting of hostname mapping to port numbers

    """
    def __init__(self, serverdict, job = False):
        self.poller = getattr(select, 'poll', lambda : None)()
        self.job = job
        #build servers into the self.servers list
        self.servers = []
        for ip, port in serverdict.iteritems():
            self.add_server(ip, port)
        #random seed by local time
        random.seed()
        #initiate connections
        self.__makeConn()
    
    def remove_server(self, ip, port=None):
        """Removes the server from the server list and returns True on success.
        Else, if the target server doesn't exist, Returns false.
        
        If port is None, then all internal matching server ips are removed.

        """
        def compare_server(server):
            matching_port = True
            if port:
                matching_port = cmp(server.port, port)
            return cmp(server.ip, ip) and matching_port

        target = filter(compare_server, self.servers)
        if target:
            for t in target:
                self.poller.unregister(t)
                t.close_connection()
                self.servers.remove(t)
        return bool(target)

    def add_server(self, ip, port):
        """Checks if the server doesn't already exist and adds it. Returns
        True on successful addition or False if the server already exists. 

        Upon server addition, the server socket is automatically created
        and a connection is created.

        """
        s = Server(ip, port)
        for server in self.servers:
            if not cmp(s, server):
                return False
        
        self._add_server_socket(s)
        self.servers.append(s)
        return True

    def fileno(self):
        return map(lambda server: server.socket.fileno(), self.servers)

    def _add_server_socket(self, server):
        server.socket = socket.socket()
        #TODO: check if we can connect, if not, log and REMOVE SERVER
        #if there are no more servers left, raise the socket error
        server.socket.connect(tuple(server.ip, server.port))

    def __makeConn(self):
        for server in self.servers:
            self._add_server_socket(server)
            assert server.socket #sanity check
            if self.poller:
                self.poller.register(server.socket, select.POLLIN)
        #magic!
        protohandler.MAX_JOB_SIZE = self.stats()['data']['max-job-size']

    def __writeline(self, line):
        #TODO: implement round robin, although honestly, random is 
        #slighly better
        try:
            #write a line by picking a random server
            random_server = random.choice(self.servers)
            #check if the server is connected here as well? 
            random_server.socket.sendall(line)
            #return the random_server that we sent to
            return random_server
        except:
            raise ProtoError

    def _get_response(self, server, handler):
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
                    raise ProtoError(str(e))
                #let's check to make sure that servers isnt empty.
                if not self.servers:
                    msg = "Listening server list is empty." + closedmsg
                    raise ProtoError(msg)
                #else just raise nor
                raise ProtoError(closedmsg)
            res = handler(recv)
            if res: break

        if self.job and 'jid' in res:
            res = self.job(conn=self,**res)
        return res


    def _do_interaction(self, line, handler):
        server = self.__writeline(line)
        return self._get_response(server, handler)

