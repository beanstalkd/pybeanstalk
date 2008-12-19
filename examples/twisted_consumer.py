#!/usr/bin/env python

import os
import sys
sys.path.append("..")
sys.path.append(os.path.join(sys.path[0], '..'))

from twisted.internet import reactor, protocol, defer, task

import beanstalk

def executor(bs, jobdata):
    print "Running job %s" % `jobdata`
    bs.touch(jobdata['jid'])
    bs.delete(jobdata['jid'])

def error_handler(e):
    print "Got an error", e

def executionGenerator(bs):
    while True:
        print "Waiting for a job..."
        yield bs.reserve().addCallback(lambda v: executor(bs, v)).addErrback(
            error_handler)

def worker(bs):
    bs.watch("myqueue")
    bs.ignore("default")

    coop = task.Cooperator()
    coop.coiterate(executionGenerator(bs))

d=protocol.ClientCreator(reactor,
    beanstalk.twisted_client.Beanstalk).connectTCP(sys.argv[1], 11300)
d.addCallback(worker)

reactor.run()

