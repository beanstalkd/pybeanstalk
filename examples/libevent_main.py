''' libevent_main.py
A simple example for using the LibeventConn connection type. This just pulls
jobs and deletes them, but shows how to set up callbacks and whatnot. A few
varibales for your tweaking pleasure are:
SERVER and PORT -- set these to your beanstalkd
PUT_ERROR -- Intentinally cause an error from the beanstalkd, by requesting
             delete of an already deleted job. Value bool
FIX_ERROR -- If an error is encounterd handle it well and fix it. Otherwise,
             print it and abort. Value bool.
'''

import beanstalk
import event

SERVER = '127.0.0.1'
PORT = 11300
# change this to False to die on an error.
# Its set up a bit hokey, but its a demo anyway
PUT_ERROR = False
FIX_ERROR = True

CONN = None
MRJ = None

def got_response(response, conn):
    global MRJ
    if 'jid' in response:
        if response['data'] == 'stop':
            print 'finishing'
            event.abort()
            return
        print 'got a response!', response
        MRJ = response
        dnum = response['jid'] if not PUT_ERROR else response['jid']-1
        conn.delete(dnum)
    else:
        print 'deleted'
        conn.reserve()

def got_error(eclass, e, tb):
    import traceback
    traceback.print_exception(eclass, e, tb)
    if FIX_ERROR:
        CONN.delete(MRJ['jid'], result_callback=got_response,
            result_callback_args = (CONN,))
        return
    print 'aborting now'
    event.abort()

def start(conn):
    print 'start called'
    conn.reserve()
    return

def main():
    global CONN
    # setup the connection
    myconn = beanstalk.serverconn.LibeventConn(SERVER, PORT)
    #setup callbacks
    myconn.result_callback_args = (myconn,)
    myconn.result_callback = got_response
    myconn.error_callback = got_error

    #setup the callchain
    start(myconn)
    CONN = myconn
    print 'dispatching'
    event.dispatch()

if __name__ == '__main__':
    event.init()
    main()
