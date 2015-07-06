"""
Twisted client tests. With some magic to make them run with nose.
"""

import os, logging, subprocess, sys
import signal
import socket
import time

from zope.interface import provider

# import before stuff from twisted
from nose.twistedtools import reactor, deferred, stop_reactor

from twisted.trial import unittest
# do NOT import reactor from twisted
from twisted.internet import protocol #, reactor
from twisted.logger import Logger, globalLogPublisher, ILogObserver, formatEvent, STDLibLogObserver

#from nose.tools import with_setup, assert_raises
#import nose

# stuff under test
from beanstalk.twisted_client import Beanstalk, BeanstalkClientFactory
from beanstalk import errors

logging.basicConfig()
logging.root.setLevel(logging.DEBUG)

# test configuration
from config import get_config

# make Twisted log to Python std logging
globalLogPublisher.addObserver(STDLibLogObserver())


# Subclass the protocol under test to stop the reactor after connecting

class StoppingBeanstalkTestProtocol(Beanstalk):
    "be able to stop the reactor after connection"

    def connectionMade(self):
        Beanstalk.connectionMade(self)
        self.logger.info("{msg}", msg="test connection completed, disconnecting")
        # need even just a dummy callback, or reactor will be left unclean
        self.transport.loseConnection().addCallback(lambda x:x)


# nose test setup and teardown

def setup():
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
    time.sleep(0.1)


def teardown():
    # nose docs claim its stop_reactor should be called... not so sure
    #stop_reactor()
    logger = logging.getLogger("teardown")
    logger.info("terminating beanstalkd at %i" % process.pid)
    os.kill(process.pid, signal.SIGTERM)


# Finally, Twisted tests

class BasicTestCase(unittest.TestCase):

    def test_Basic(self):
        config = get_config("ServerConn")
        self.client = protocol.ClientCreator(reactor, StoppingBeanstalkTestProtocol)

        host, port = config.BEANSTALKD_HOST, int(config.BEANSTALKD_PORT)
        connector = self.client.connectTCP(host, port)

        # not needed for nose, uncomment for Twisted trial
        #return connector

