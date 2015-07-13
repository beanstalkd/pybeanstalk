"""
Twisted client tests. With some magic to make them run with nose or Twisted Trial
"""

import os, logging, subprocess, sys
import signal
import socket
import time

from zope.interface import provider
from twisted.trial import unittest
from twisted.internet import protocol, defer
from twisted.logger import Logger, globalLogPublisher, formatEvent, STDLibLogObserver, ILogObserver

# stuff under test
from beanstalk.twisted_client import Beanstalk, BeanstalkClientFactory
from beanstalk import errors
from beanstalk.job import Job

# test configuration
from config import get_config

#
# Set up nose testing
#

from nose.tools import with_setup, assert_raises
from nose.twistedtools import deferred, stop_reactor, threaded_reactor, reactor

logging.basicConfig()
logging.root.setLevel(logging.DEBUG)

# make Twisted log to Python std logging too
globalLogPublisher.addObserver(STDLibLogObserver())


#
# Common test setup and teardown
#

def setup():
   "start up beanstalkd"
   global process
   config = get_config("ServerConn")
   logger = logging.getLogger("setup")
   cmd = os.path.join(config.BPATH,config.BEANSTALKD)
   host = config.BEANSTALKD_HOST
   port = config.BEANSTALKD_PORT
   try:
      process = subprocess.Popen([cmd, "-l", host, "-p", port])
   except OSError as exc:
      logger.error("could not start beanstalkd test server at %s, exiting" % cmd)
      sys.exit()
   logger.info("beanstalkd server started at process %i" % process.pid)
   time.sleep(0.3)

def teardown():
   "shut down beanstalkd"
   stop_reactor() # required per nose docs on Twisted support
   logger = logging.getLogger("teardown")
   logger.info("terminating beanstalkd at %i" % process.pid)
   os.kill(process.pid, signal.SIGTERM)


#
#  Twisted Trial client tests
#

class TestTwistedClient(unittest.TestCase):
   "Twisted client tests"

   logger = Logger()

   def setUp(self):
      cfg = get_config("ServerConn")
      self.host, self.port = cfg.BEANSTALKD_HOST, int(cfg.BEANSTALKD_PORT)
      self.creator = protocol.ClientCreator(reactor, Beanstalk)

   def tearDown(self):
      self.conn.transport.loseConnection()

   @deferred() # required for nose integration
   def test_00_Job(self):
      "Twisted beanstalk Job instantiation succeeds"

      def onconn(conn):
         self.conn = conn
         rawjob =  {'jid': 1, 'bytes': 13, 'state': 'ok', 'data': 'Look!  A job!'}
         rawjob.update(conn=conn)
         j = Job(**rawjob)
         assert j["data"] == 'Look!  A job!'
         self.logger.debug("Job created ok")

      connector = self.creator.connectTCP(self.host, self.port)
      return connector.addCallback(onconn)


   @deferred() # required for nose integration
   def test_01_Stats(self):
      "Twisted client correctly gets stats from server"

      def onconn(conn):
         self.conn = conn
         statist = conn.stats()
         #
         def onstats(stats):
            self.assertEqual(stats["state"], "ok")
            self.logger.debug("got stats ok")
         #
         statist.addCallback(onstats)
      connector = self.creator.connectTCP(self.host, self.port)
      return connector.addCallback(onconn)

   @deferred() # required for nose integration
   def test_02_Put_Reserve(self):
      "putting and reserving works"

      def check_reserve_ok():
         reserver = self.conn.reserve()
         #
         def on_ok(job, ):
            expected = {'jid': 1, 'bytes': 13, 'state': 'ok', 'data': 'Look!  A job!'}
            self.assertEqual(job, expected)
            self.logger.debug("job reservation succeeded")
         reserver.addCallback(on_ok)
         #
         def on_fail(failure):
            raise Exception(failure)
         reserver.addErrback(on_fail)
         return reserver

      def check_put_ok():
         putter = self.conn.put('Look!  A job!', 8192, 0, 300)
         #
         def on_ok(job):
            self.assertEqual(job, {'jid': 1, 'state': 'ok'})
            self.logger.debug("job putting succeeded")
         putter.addCallback(on_ok)
         #
         def on_fail(failure):
            raise Exception(failure)
         putter.addErrback(on_fail)
         return putter

      def onconn(conn):
         "start both putting and reserving"
         self.conn = conn
         p = check_put_ok()
         r = check_reserve_ok()
         # set up the deferred returned to nose
         defer.DeferredList((p, r)).chainDeferred(self.r)

      connector = self.creator.connectTCP(self.host, self.port)
      self.r = defer.Deferred()
      connector.addCallback(onconn)
      return self.r # chained after completion of putting and reserving
