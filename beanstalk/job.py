import StringIO
from pprint import pformat
from functools import wraps

import yaml

import errors

DEFAULT_CONN = None

def honorimmutable(func):
    @wraps(func)
    def deco(*args, **kw):
        if args[0].imutable:
            raise errors.JobError("Cannot do that to a job you don't own")
        return func(*args, **kw)
    return deco

class Job(object):
    ''' class Job is an optional class for keeping track of jobs returned
    by the beanstalk server.

    It is designed to be as flexible as possible, with a minimal number of extra
    methods. (See below).  It has 4 protocol methods, for dealing with the
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

    def __init__(self, conn = None, jid=0, pri=0, data='', state = 'ok', **kw):

        if not any([conn, DEFAULT_CONN]):
            raise AttributeError("No connection specified")

        self._conn = conn if conn else DEFAULT_CONN
        self.jid = jid
        self.pri = pri
        self.delay = 0
        self.state = state
        self.data = data if data else ''

        self.imutable = bool(kw.get('imutable', False))
        self._from_queue = bool(kw.get('from_queue', False))
        self.tube = kw.get('tube', 'default')
        self.ttr = kw.get('ttr', 60)

    def __eq__(self, comparable):
        if not isinstance(comparable, Job):
            return False
        return not all([cmp(self.Server, comparable.Server),
                        cmp(self.jid, comparable.jid),
                        cmp(self.state, comparable.state),
                        cmp(self.data, comparable.data)])

    def __del__(self):
        self.Finish()

    def __str__(self):
        return pformat(self._serialize())

    def __getitem__(self, key):
        #validate key for TypeError
        #TODO: make elegant
        validkey = isinstance(key, basestring)
        if not validkey:
            raise TypeError, "Invalid subscript type: %s" % type(key)
        #return KeyError
        try:
            value = getattr(self, key)
        except AttributeError, e:
            raise KeyError, e
        else:
            return value

    def _unserialize(self, data):
        self.data = yaml.dump(data)

    def _serialize(self):
        handler = StringIO.StringIO({
            'data' : self.data,
            'jid' : self.jid,
            'state' : self.state,
            'conn' : str(self.Server)
        })
        return yaml.load(handler)

    def run(self):
        raise NotImplemented('The Job.run method must be implemented in a subclass')

    def Queue(self):
        if self._from_queue:
            self.Delay(self.delay)
            return
        oldtube = self.Server.tube
        if oldtube != self.tube:
            self.Server.use(self.tube)
        self.Server.put(self._serialize(), self.pri, self.delay, self.ttr)
        if oldtube != self.tube:
            self.Server.use(oldtube)

    @honorimmutable
    def Return(self):
        try:
            self.Server.release(self.jid, self.pri, 0)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Delay(self, delay):
        try:
            self.Server.release(self.jid, self.pri, delay)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Finish(self):
        try:
            self.Server.delete(self.jid)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Touch(self):
        try:
            self.Server.touch(self.jid)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Bury(self, newpri = 0):
        if newpri:
            self.pri = newpri

        try:
            self.Server.bury(self.jid, newpri)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @property
    def Info(self):
        try:
            stats = self.Server.stats_job(self.jid)
        except:
            raise
        else:
            return stats

    @property
    def Server(self):
        return self._conn

def newJob(**kw):
    kw['from_queue'] = False
    return Job(**kw)
