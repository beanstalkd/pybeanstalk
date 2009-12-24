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

        self.conn = conn if conn else DEFAULT_CONN
        self.jid = jid
        self.pri = pri
        self.delay = 0
        self.state = state
        if data:
            self._unserialize(data)
        else:
            self.data = ''

        self.imutable = bool(kw.get('imutable', False))
        self._from_queue = bool(kw.get('from_queue', False))
        self.tube = kw.get('tube', 'default')

    def __del__(self):
        super(Job, self).__del__()
        self.Finish()

    def __str__(self):
        return self._serialize()
    
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
        self.data = yaml.load(data)

    def _serialize(self):
        return yaml.dump(self.data)

    def run(self):
        raise NotImplemented('The Job.run method must be implemented in a subclass')

    def Queue(self):
        if self._from_queue:
            self.Delay(self.delay)
            return
        oldtube = self.conn.tube
        if oldtube != self.tube:
            self.conn.use(self.tube)
        self.conn.put(self._serialize(), self.pri, self.delay)
        if oldtube != self.tube:
            self.conn.use(oldtube)

    @honorimmutable
    def Return(self):
        try:
            self.conn.release(self.jid, self.pri, 0)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Delay(self, delay):
        try:
            self.conn.release(self.jid, self.pri, delay)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Finish(self):
        try:
            self.conn.delete(self.jid)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Touch(self):
        try:
            self.conn.touch(self.jid)
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
            self.conn.bury(self.jid, newpri)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @property
    def Info(self):
        try:
            stats=self.conn.stats_job(self.jid)
        except:
            raise
        else:
            return stats

def newJob(**kw):
    kw['from_queue'] = False
    return Job(**kw)
