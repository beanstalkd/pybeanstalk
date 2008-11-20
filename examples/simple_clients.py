# stdlib imports
import sys
import time

# pybeanstalk imports
from beanstalk import serverconn
from beanstalk import job

def producer_main(connection):
    i = 0
    while True:
        data = 'This is data to be consumed (%s)!' % (i,)
        print data
        data = job.Job(data=data, conn=connection)
        data.Queue()
        time.sleep(1)
        i += 1;

def consumer_main(connection):
    while True:
        j = connection.reserve()
        print 'got job! job is: %s' % j.data
        j.Touch()
        j.Finish()

def main():
    try:
        print 'handling args'
        clienttype = sys.argv[1]
        server = sys.argv[2]
        try:
            port = int(sys.argv[3])
        except:
            port = 11300

        print 'setting up connection'
        connection = serverconn.ServerConn(server, port)
        connection.job = job.Job
        if clienttype == 'producer':
            print 'starting producer loop'
            producer_main(connection)
        elif clienttype == 'consumer':
            print 'starting consumer loop'
            consumer_main(connection)
        else:
            raise Exception('foo')
    except Exception, e:
        print "usage: example.py TYPE server [port]"
        print " TYPE is one of: [producer|consumer]"
        raise
        sys.exit(1)
if __name__ == '__main__':
    main()
