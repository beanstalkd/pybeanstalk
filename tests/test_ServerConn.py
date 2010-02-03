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

from nose.tools import with_setup, assert_raises
import nose

from beanstalk import serverconn
from beanstalk import errors
from config import get_config

config = get_config("ServerConn")

# created during setup
server_pid = None
conn = None


def setup():
    global server_pid, conn, config
    server_pid = os.spawnl(os.P_NOWAIT,
                            os.path.join(config.BPATH,config.BEANSTALKD),
                            os.path.join(config.BPATH,config.BEANSTALKD),
                            '-l', config.BEANSTALKD_HOST,
                            '-p', config.BEANSTALKD_PORT
                            )
    print "server started at process", server_pid
    time.sleep(0.1)
    conn = serverconn.ServerConn(config.BEANSTALKD_HOST, int(config.BEANSTALKD_PORT))

def teardown():
    print "terminating beanstalkd at", server_pid
    os.kill(server_pid, signal.SIGTERM)


# Test helpers:

def _test_put_reserve_delete_a_job(payload, pri):
    # check preconditions
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)['jid']
    print "created a job with id", put_id

    assert conn.stats()['data']['current-jobs-ready'] == 1
    assert conn.stats_job(put_id)['data']['state'] == 'ready'

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['jid'] == put_id
    jstats = conn.stats_job(res['jid'])['data']
    assert jstats['pri'] == pri
    assert jstats['state'] == 'reserved'

    # delete it
    print 'about to delete'
    conn.delete(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"
    nose.tools.assert_raises(errors.NotFound, conn.stats_job, res['jid'])


def _test_put_reserve_release_a_job(payload, pri):
    # check preconditions
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."

    # create a job
    put_id = conn.put(payload, pri)['jid']
    print "created a job with id", put_id

    assert conn.stats()['data']['current-jobs-ready'] == 1

    # reserve it
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['jid'] == put_id

    # release it
    conn.release(res['jid'])
    assert conn.stats()['data']['current-jobs-ready'] == 1, "job was not released"
    assert conn.stats_job(put_id)['data']['state'] == 'ready'

    # reserve again
    res = conn.reserve()
    print "reserved a job", res

    assert res['data'] == payload
    assert res['jid'] == put_id

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
    assert conn.stats_job(put['jid'])['data']['state'] == 'buried'

    # kick it back into the queue
    print 'kicking'
    kick = conn.kick(1)
    assert conn.stats()['data']['current-jobs-ready'] == 1, "job was not kicked"

    # Need to reget the job, then delete it
    job = conn.reserve()
    #while we are here, a sanity check to make sure the job is re-gettable
    assert job == res, 'second job get is different from origninal get'
    jstats = conn.stats_job(job['jid'])['data']
    assert jstats['buries'] == 1
    assert jstats['kicks'] == 1

    delete = conn.delete(res['jid'])

    assert conn.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"


def test_ServerConn_fails_to_connect_with_a_reasonable_exception():
    # it may be nicer not to throw a socket error here?
    try:
        serverconn.ServerConn(config.BEANSTALKD_HOST,
                              int(config.BEANSTALKD_PORT)+1)
    except socket.error, reason:
        pass

def test_tube_operations():
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."
    # first make sure its watching default
    assert conn.watchlist == ['default']

    testlist = ['foo','bar','baz']
    conn.watchlist = testlist
    # ordering may not be garunteed, sets dont care!
    assert set(conn.watchlist) == set(testlist)
    assert set(conn.list_tubes_watched()['data']) == set(testlist)

    #use test
    assert conn.tube == 'default'
    # a dummy job for when we test a different tube...
    dummy_id = conn.put('dummy')['jid']

    conn.use('bar')
    assert conn.tube == 'bar'

    jid = conn.put('this is data', pri=100)['jid']
    assert conn.stats_tube('bar')['data']['current-jobs-ready'] == 1

    assert conn.stats()['data']['current-jobs-ready'] == 2
    # because the protocol blocks when we try to reserve a job, theres not a
    # good way to test that it does not return when the watchlist doesn't
    # include this job, untill threading/async is better anyway
    # out of orderness is a good test tho... :)

    job = conn.reserve()
    assert job['jid'] == jid, 'got wrong job from tube bar'
    conn.release(jid)

    conn.watchlist = ['default']
    job = conn.reserve()
    assert job['jid'] == dummy_id, 'got wrong job from default'
    print 'about to delete'
    conn.delete(dummy_id)

    conn.watchlist = testlist
    conn.reserve()
    print 'about to delete again'
    conn.delete(jid)

def test_reserve_timeout_works():
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."
    # essentially an instant poll. This should just timeout!
    x = conn.reserve_with_timeout(0)
    assert x['state'] == 'timeout'

def test_reserve_deadline_soon():
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."
    # Put a short running job
    jid = conn.put('foobarbaz job!', ttr=1)['jid']
    # Reserve it, so we can setup conditions to get a DeadlineSoon error
    job = conn.reserve()
    assert jid == job['jid'], "Didn't get test job, something funky is happening."
    # a bit of padding to make sure that deadline soon is encountered
    time.sleep(.2)
    assert_raises(errors.DeadlineSoon, conn.reserve), "Job should have warned "\
                  "of impending deadline. It did not. This is a problem!"
    x = conn.delete(jid)
    assert x['state'] == 'ok', "Didn't delete the job right. This could break future tests"

