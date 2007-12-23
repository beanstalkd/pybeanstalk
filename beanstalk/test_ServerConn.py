
"""
ServerConn tests.

These tests are easiest run with nose, that's why they are free of
xUnit cruft ;)

There is a strong possibility of side effects from failing tests breaking
others.  Probably best to setup a new beanstalkd at each test.
"""

import os
import signal
import socket
import time
import pprint

from nose.tools import with_setup

from beanstalk import serverconn


# details of test server we spawn
BEANSTALKD = 'beanstalkd'
BEANSTALKD_PORT = 11301
BEANSTALKD_HOST = '127.0.0.1'


# created during setup
server_pid = None
conn = None


def setup():
    global server_pid, conn
    server_pid = os.spawnlp(os.P_NOWAIT,
                            BEANSTALKD,
                            BEANSTALKD,
                            '-l', BEANSTALKD_HOST,
                            '-p', str(BEANSTALKD_PORT)
                            )
    print "server started at process", server_pid
    conn = serverconn.ServerConn(BEANSTALKD_HOST, BEANSTALKD_PORT)

def teardown():
    print "terminating beanstalkd at", server_pid
    os.kill(server_pid, signal.SIGTERM)


def test_ServerConn_fails_to_connect_with_a_reasonable_exception():
    # it may be nicer not to throw a socket error here?
    try:
        serverconn.ServerConn(BEANSTALKD_HOST,
                              BEANSTALKD_PORT+1)
    except socket.error, reason:
        pass


# Test helpers:

def _test_put_reserve_delete_a_job(payload, pri):

    # check preconditions
    assert conn.stats()['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)
    print "created a job with id", put_id

    assert conn.stats()['current-jobs-ready'] == 1

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri

    # delete it
    conn.delete(res['id'])
    assert conn.stats()['current-jobs-ready'] == 0, "job was not deleted"


def _test_put_reserve_release_a_job(payload, pri):

    # check preconditions
    assert conn.stats()['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)
    print "created a job with id", put_id

    assert conn.stats()['current-jobs-ready'] == 1

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri

    # release it
    conn.release(res['id'])
    assert conn.stats()['current-jobs-ready'] == 1, "job was not released"

    # reserve again
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri

    # delete it
    conn.delete(res['id'])
    assert conn.stats()['current-jobs-ready'] == 0, "job was not deleted"


# Test Cases:

def test_ServerConn_can_put_reserve_delete_a_simple_job():
    _test_put_reserve_delete_a_job('abcdef', 0)

#def test_ServerConn_can_put_reserve_delete_a_long_job():
#    # FAILS: unexpected empty response on delete
#    _test_put_reserve_delete_a_job('abc'*100, 0)

#def test_ServerConn_can_put_reserve_delete_a_nasty_job():
#    # FAILS: should the library expect the client to escape ?
#    _test_put_reserve_delete_a_job('abc\r\nabc', 0)

def test_ServerConn_can_put_reserve_release_a_simple_job():
    _test_put_reserve_release_a_job('abcdef', 0)


