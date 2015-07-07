"""
MultiServerConn tests.

These tests are easiest run with nose, that's why they are free of
xUnit cruft ;)

There is a strong possibility of side effects from failing tests breaking
others.  Probably best to setup a new beanstalkd at each test.
"""

import os
import sys
import signal
import socket
import time
import random
import subprocess
import itertools

from nose.tools import with_setup, assert_raises
import nose

from beanstalk import multiserverconn
from beanstalk import errors
from beanstalk import job

from config import get_config


# created during setup
config = get_config("MultiServerConn")

processes = []
conn = None

def setup():
    global processes, conn, config
    output = "server started on %(ip)s:%(port)s with PID: %(pid)s"

    H = config.BEANSTALKD_HOSTS.split(';')
    C = int(config.BEANSTALKD_COUNT)
    P = int(config.BEANSTALKD_PORT_START)
    J = getattr(job, config.BEANSTALKD_JOB_CLASS, None)

    binloc = os.path.join(config.BPATH, config.BEANSTALKD)
    conn = multiserverconn.ServerPool([])

    for ip, port in itertools.izip_longest(H, xrange(P, P+C), fillvalue=H[0]):
        process = subprocess.Popen([binloc, "-l", str(ip), "-p", str(port)])
        processes.append(process)
        print output % { "ip" : ip, "port" : port, "pid" : process.pid }
        time.sleep(0.1)
        try:
            conn.add_server(ip, port, J)
        except Exception, e:
            processes.pop().kill()
            raise

def teardown():
    global processes
    output = "terminating beanstalkd with PID: %(pid)s"
    for process in processes:
        print output % {"pid" : process.pid}
        process.kill()

def _clean_up():
    """Cleans up from previous test by closing whatever connection was waiting
    and primes the connection for the next test

    - Resets the waiting flag
    - Disconnects and reconnects to the beanstalk queue server to clean out
      any pending requests.

    """
    for server in itertools.ifilter(lambda s: s.waiting, conn.servers):
        server.waiting = False
        server.close()
        server.connect()

    for server in conn.servers:
        assert not server.waiting


# Test helpers:
def _test_putter_and_reserver(payload, pri):
    """Returns a tuple consisting of the job and the reserved job.

    This will create a job, and get the reserved job from the queue, providing
    all sanity checking so we can D.R.Y. some stuff up.

    """
    # no point in checking preconditions here because we don't know what
    # server we're going to be looking at.
    #
    # TODO: for sanity though, we should query all servers to check they're
    # empty.

    # create a job
    job_ = conn.put(payload, pri)
    put_id = job_['jid']

    print "created a job with id", put_id

    assert job_.Server.stats()['data']['current-jobs-ready'] == 1
    assert job_.Info['data']['state'] == 'ready'

    # reserve it
    res = conn.reserve()[0]
    # reserved here is a Job class
    print "reserved a job", res

    assert res['data'] == payload
    assert res['jid'] == put_id

    return (job_, res)


def _test_put_reserve_delete_a_job(payload, pri):

    job_, res = _test_putter_and_reserver(payload, pri)
    jstats = res.Info['data']

    assert jstats['pri'] == pri
    assert jstats['state'] == 'reserved'

    # delete it
    print 'about to delete'
    assert res.Finish()

    assert job_.Server.stats()['data']['current-jobs-ready'] == 0,\
            "job was not deleted"

    nose.tools.assert_raises(errors.NotFound, res.Server.stats_job, res['jid'])
    _clean_up()


def _test_put_reserve_release_a_job(payload, pri):

    job_, res = _test_putter_and_reserver(payload, pri)
    put_id = job_["jid"]

    # release it
    res.Return()
    assert res.Server.stats()['data']['current-jobs-ready'] == 1, "job was not released"
    assert job_.Info['data']['state'] == 'ready'

    # reserve again
    res = conn.reserve()[0]
    print "reserved a job", res

    assert res['data'] == payload
    assert res['jid'] == put_id

    # delete it
    res.Finish()
    assert job_.Server.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"
    _clean_up()



# Test Cases:

def test_remove_server():
    """Test if remove_server works appropriately.

    Not going to test add_server because I already use it in setup()

    """
    global config

    H = config.BEANSTALKD_HOSTS.split(';')
    C = int(config.BEANSTALKD_COUNT)
    P = int(config.BEANSTALKD_PORT_START)
    J = getattr(job, config.BEANSTALKD_JOB_CLASS, None)

    assert conn.remove_server(H[0], P)

    # restore server..
    assert conn.add_server(H[0], P, J)

    _clean_up()

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
    job_ = conn.put('simple job')
    res = conn.reserve()[0]
    assert job_['jid'] == res['jid']

    # bury it
    print 'burying'
    bury = res.Bury()
    assert res.Server.stats()['data']['current-jobs-buried'] == 1, \
        "job was not buried"
    assert job_.Info['data']['state'] == 'buried'

    # kick it back into the queue
    print 'kicking'
    kick = res.Server.kick(1)
    assert res.Server.stats()['data']['current-jobs-ready'] == 1, "job was not kicked"

    # Need to reget the job, then delete it
    # WOAH: SIDE EFFECT OF __DEL__ IMPLEMENTATION
    # if the statement below was:
    #     job_ = job_.Server.reserve()
    # the GC would've ate up job_ and SERVER would return NOT_FOUND!!
    # be very careful!!
    resurrected = conn.reserve()[0]
    #while we are here, a sanity check to make sure the job is re-gettable
    #
    #these are dicts.
    #
    assert resurrected == res,\
                'second job get is different from original get'

    jstats = resurrected.Info['data']
    assert jstats['buries'] == 1
    assert jstats['kicks'] == 1

    delete = res.Finish()

    assert job_.Server.stats()['data']['current-jobs-ready'] == 0, "job was not deleted"
    _clean_up()


def test_ServerConn_fails_to_connect_with_a_reasonable_exception():
    # it may be nicer not to throw a socket error here?
    try:
        H = config.BEANSTALKD_HOSTS.split(';')
        C = int(config.BEANSTALKD_COUNT)
        P = int(config.BEANSTALKD_PORT_START)
        J = getattr(job, config.BEANSTALKD_JOB_CLASS, None)
        #add a new server with a port that is most likely not open
        multiserverconn.ServerPool([(H[0], P+C+1, J)])
    except socket.error, reason:
        pass

def test_tube_operations():
    # first make sure its watching default
    # this check is useless for our purposes, but will work fine since
    # it will check all servers

    # test clone here
    assert conn.watchlist == ['default']

    # a dummy job for when we test a different tube...
    job_ = conn.put('dummy')
    dummy_id = job_['jid']

    testlist = ['foo','bar','baz']
    conn.watchlist = testlist

    # ordering may not be guaranteed, sets dont care!
    assert set(conn.watchlist) == set(testlist)

    # changes a bit since you're distributed..
    # returns a dict
    tubes = conn.list_tubes_watched()
    for server, tubes_watched in tubes.iteritems():
        # might as well assert that the server shoulnd't be waiting
        assert not server.waiting
        # check to make sur ethat the set is equal to the test list
        assert set(tubes_watched['data']) == set(testlist)

    #use test
    assert set(conn.tubes) == set(['default'])

    conn.use('bar')
    assert set(conn.tubes) == set(['bar'])

    newjob_ = conn.put('this is data', pri=100)
    jid = newjob_['jid']
    assert newjob_.Server.stats_tube('bar')['data']['current-jobs-ready'] == 1

    # because we're randomly choosing between servers, we shouldn't expect that
    # the current-jobs-ready will be the same, since they're on distributed
    # nodes (note this test assumes TWO beanstalkd servers!)

    # so if the jobs went to different servers, current job ready count is always 1;
    # on the other hand, if they went to same server, the count can be 2 or 0, depending
    # on which server the stats call hits (the one where jobs went, or the other one)

    readycount = conn.stats()['data']['current-jobs-ready'] # can be 0, 1 or 2

    if newjob_.Server != job_.Server:
       assert readycount == 1,  "Was expecting 1, got %i" % readycount
    else:
       assert readycount in (0,2),  "Was expecting 2, got %i" % readycount

    # because the protocol blocks when we try to reserve a job, theres not a
    # good way to test that it does not return when the watchlist doesn't
    # include this job, untill threading/async is better anyway
    # out of orderness is a good test tho... :)

    job = newjob_.Server.reserve()
    assert job['jid'] == jid, 'got wrong job from tube bar'
    job.Return()

    conn.watchlist = ['default']
    #job from default queue
    j_from_dq = job_.Server.reserve()
    assert j_from_dq['jid'] == dummy_id, 'got wrong job from default'
    print 'about to delete'
    j_from_dq.Finish()

    conn.watchlist = testlist
    j = newjob_.Server.reserve()
    print 'about to delete again'
    j.Finish()
    _clean_up()


def test_reserve_timeout_works():
    assert conn.stats()['data']['current-jobs-ready'] == 0, "The server is not empty "\
           "of jobs so test behaviour cannot be guaranteed.  Bailing out."
    # essentially an instant poll. This should just timeout!
    # remember that reserve with timeout is broadcasted to all servers
    # so all servers need to return timeout
    results = conn.reserve_with_timeout(0)
    for server in results:
        assert server['state'] == 'timeout'
    _clean_up()

def test_reserve_deadline_soon():

    # Put a short running job
    job_ = conn.put('foobarbaz job!', ttr=2)
    jid = job_["jid"]

    # Reserve it, so we can setup conditions to get a DeadlineSoon error
    res = conn.reserve()[0]
    assert jid == res['jid'], "Didn't get test job, something funky is happening."
    # a bit of padding to make sure that deadline soon is encountered
    time.sleep(1)
    assert_raises(errors.DeadlineSoon, conn.reserve), "Job should have warned "\
                  "of impending deadline. It did not. This is a problem!"

    job_.Finish()
    x = job_.Server.stats()
    assert x['state'] == 'ok', "Didn't delete the job right. This could break future tests"
    _clean_up()

