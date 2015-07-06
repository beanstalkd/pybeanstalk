"""
Twisted client tests. With some magic to make them run with nose or Twisted Trial
"""

import os, logging, subprocess, sys
import signal
import socket
import time

from zope.interface import provider
from twisted.trial import unittest
from twisted.internet import protocol
from twisted.logger import Logger, globalLogPublisher, formatEvent, STDLibLogObserver, ILogObserver

# stuff under test
from beanstalk.twisted_client import Beanstalk, BeanstalkClientFactory
from beanstalk import errors
from beanstalk.job import Job

# test configuration
from config import get_config


USE_NOSE = False

#
# Set up nose testing
#

if USE_NOSE:
   from nose.tools import with_setup, assert_raises
   from nose.twistedtools import deferred, stop_reactor, threaded_reactor, reactor

   # nose requires decorating deferred-returning Twisted Trial unittest methods
   def deferred_if_nose(timeout=None):
      return deferred(timeout=timeout)

   logging.basicConfig()
   logging.root.setLevel(logging.DEBUG)

   # make Twisted log to Python std logging too
   globalLogPublisher.addObserver(STDLibLogObserver())

#
# Alternatively, use Twisted Trial
#

else:
   from twisted.internet import reactor

   # passthru decorator for regular Twisted Trial use
   def deferred_if_nose(**kwargs):
      def wrapper(func):
         return lambda *args, **kwargs: func(*args, **kwargs)
      return wrapper

   @provider(ILogObserver)
   def PrintingObserver(event):
      print formatEvent(event)

   globalLogPublisher.addObserver(PrintingObserver)


#
# Common test setup and teardown
#

def _setup():
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
   time.sleep(0.2)

def _teardown():
   "shut down beanstalkd"
   logger = logging.getLogger("teardown")
   logger.info("terminating beanstalkd at %i" % process.pid)
   os.kill(process.pid, signal.SIGTERM)

def teardown():
   "additionally called only by nose (if USE_NOSE is set)"
   stop_reactor()


#
# Finally, Twisted Trial client tests
#

class TestClient(unittest.TestCase):

   def setUp(self):
      _setup()
      cfg = get_config("ServerConn")
      self.host, self.port = cfg.BEANSTALKD_HOST, int(cfg.BEANSTALKD_PORT)

   def test_00_Job(self):
      "Job instantiation works"

      @deferred_if_nose()
      def onconn(client):
         rawjob =  {'jid': 1, 'bytes': 13, 'state': 'ok', 'data': 'Look!  A job!'}
         rawjob.update(conn=client)
         j = Job(**rawjob)

      creator = protocol.ClientCreator(reactor, Beanstalk)
      connector = creator.connectTCP(self.host, self.port)
      return connector.addCallback(onconn)


   @deferred_if_nose()
   def test_01_Stats(self):
      "we get stats from server"

      creator = protocol.ClientCreator(reactor, Beanstalk)
      connector = creator.connectTCP(self.host, self.port)

      @deferred_if_nose()
      def onconn(client, *args, **kwargs):
         statist = client.stats()
         #
         def onstats(stats):
            self.assertEqual(stats["state"], "ok")
         #
         return statist.addCallback(onstats)

      connector.addCallback(onconn)
      return connector

   def tearDown(self):
      _teardown()

   @deferred_if_nose()
   def test_02_Put_Reserve(self):
      "putting and reserving works"

      self.client = protocol.ClientCreator(reactor, Beanstalk)
      connector = self.client.connectTCP(self.host, self.port)

      @deferred_if_nose()
      def check_reserve_ok(conn):
         reserver = conn.reserve()
         #
         def on_ok(job):
            expected = {'jid': 1, 'bytes': 13, 'state': 'ok', 'data': 'Look!  A job!'}
            self.assertEqual(job, expected)

         reserver.addCallback(on_ok)
         #
         def on_fail(failure):
            raise Exception(failure)
         reserver.addErrback(on_fail)
         #
         return reserver

      @deferred_if_nose()
      def check_put_ok(conn):
         putter = conn.put('Look!  A job!', 8192, 0, 300)
         #
         def on_ok(job):
            self.assertEqual(job, {'jid': 1, 'state': 'ok'})
            return check_reserve_ok(conn)
         putter.addCallback(on_ok)
         #
         def on_fail(failure):
            raise Exception(failure)
         putter.addErrback(on_fail)
         #
         return putter

      return connector.addCallback(check_put_ok)

