import yaml
from protohandler import FailureError

DEFALUT_CONN = None
class Job(object):
    ''' class Job is and optional class for keeping track of jobs returned
    by the beanstalk server.

    It is designed to be as flexible as possible, with a minimal number of extra
    methods. (See below).  It has 4 protocl methods, for dealing with the
    server via a connection object. It also has 2 methods, _serialize and
    _unserialize for dealing with the data returned by beanstalkd. These
    default to a simple yaml dump and load respectively.

    One intent is that in simple applications, the Job class can be a
    superclass or mixin, with a method run. In this case, the pybeanstalk.main()
    loop will get a Job, call its run method, and when finished delete the job.

    In more complex applications, where the pybeanstalk.main is insufficient,
    Job was designed so that processing data (e.g. data is more of a message),
    can be handled within the specific data object (JobObj.data) or by external
    means. In this case, Job is just a convenience class, to simplify job
    management on the consumer end.
    '''

    def __init__(self, conn = None, jid=0, pri=0, data=''):
        print "job initialized: conn: %s, id: %s, pri: %s, data: %s" %\
            (conn, jid, pri, data)
        if not conn and not DEFAULT_CONN:
            raise AttributeError("No connection specified")
        elif not conn:
            self._conn = DEFAULT_CONN
        else:
            self._conn = conn
        self.id = jid
        self.priority = pri
        self.delay = 0
        if data:
            self._unserialize(data)
        else:
            self.data = ''

    def __del__(self):
        self.delete()
        super(Job, self).__del__()

    def __str__(self):
        return self._serialize()

    def _unserialize(self, data):
        self.data = yaml.load(data)

    def _serialize(self):
        return yaml.dump(self.data)

    def run(self):
        raise NotImplemented('The Job.run method must be implemented in a subclass')

    def put(self):
        self._conn.put(self._serialize(), self.priority, self.delay)

    def release(self, delay = 0):
        try:
            self._conn.release(self.id, self.priority, delay)
        except FailureError:
            return False
        else:
            return True

    def delete(self):
        try:
            self._conn.delete(self.id)
        except FailureError:
            return False
        else:
            return True

    def bury(self, newpri = 0):
        if newpri:
            self.pri = newpri

        try:
            self._conn.bury(self.id, newpri)
        except FalureError:
            return False
        else:
            return True

