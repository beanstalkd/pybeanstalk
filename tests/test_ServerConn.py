"""
ServerConn tests.

These tests are easiest run with nose, that's why they are free of
xUnit cruft ;)

There is a strong possibility of side effects from failing tests breaking
others.  Probably best to setup a new beanstalkd at each test.
"""

import os, sys
import signal
import socket
import time
import pprint

from nose.tools import with_setup

from beanstalk import serverconn
import config



# created during setup
server_pid = None
conn = None


def setup():
    global server_pid, conn
    server_pid = os.spawnl(os.P_NOWAIT,
                            os.path.join(config.BPATH,config.BEANSTALKD),
                            os.path.join(config.BPATH,config.BEANSTALKD),
                            '-l', config.BEANSTALKD_HOST,
                            '-p', str(config.BEANSTALKD_PORT)
                            )
    print "server started at process", server_pid
    time.sleep(0.1)
    conn = serverconn.ServerConn(config.BEANSTALKD_HOST, config.BEANSTALKD_PORT)

def teardown():
    print "terminating beanstalkd at", server_pid
    os.kill(server_pid, signal.SIGTERM)


# Test helpers:

def _test_put_reserve_delete_a_job(payload, pri):
    # check preconditions
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)
    print "created a job with id", put_id

    assert conn.stats()['data']['current-jobs-ready'] == 1

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri
    assert res['jid'] == put_id['jid']

    # delete it
    conn.delete(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"


def _test_put_reserve_release_a_job(payload, pri):
    # check preconditions
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)
    print "created a job with id", put_id

    assert conn.stats()['data']['current-jobs-ready'] == 1

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri
    assert res['jid'] == put_id['jid']

    # release it
    conn.release(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 1, "job was not released"

    # reserve again
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['pri'] == pri
    assert res['jid'] == put_id['jid']

    # delete it
    conn.delete(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"


# Test Cases:

def test_ServerConn_can_put_reserve_delete_a_simple_job():
    _test_put_reserve_delete_a_job('abcdef', 0)

def test_ServerConn_can_put_reserve_delete_a_long_job():
    _test_put_reserve_delete_a_job('abc'*100, 0)

def test_ServerConn_can_put_reserve_delete_a_nasty_job():
    _test_put_reserve_delete_a_job('abc\r\nabc', 0)

def test_ServerConn_can_put_reserve_release_a_simple_job():
    _test_put_reserve_release_a_job('abcdef', 0)


def test_ServerConn_can_bury_and_kick_a_job():
    # check preconditions
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # put and reserve the job
    put = conn.put('simple job')
    res = conn.reserve()
    assert put['jid'] == res['jid']

    # bury it
    print 'burying'
    bury = conn.bury(res['jid'])
    assert conn.stats()['data']['current-jobs-buried'] == 1, \
        "job was not buried"

    # kick it back into the queue
    print 'kicking'
    kick = conn.kick(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 1, "job was not kicked"

    # Need to reget the job, then delete it
    job = conn.reserve()
    #while we are here, a sanity check to make sure the job is re-gettable
    assert job['jid'] == res['jid']
    delete = conn.delete(res['jid'])

    assert conn.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"


def test_ServerConn_fails_to_connect_with_a_reasonable_exception():
    # it may be nicer not to throw a socket error here?
    try:
        serverconn.ServerConn(config.BEANSTALKD_HOST,
                              config.BEANSTALKD_PORT+1)
    except socket.error, reason:
        pass
