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

    def __init__(self, conn = None, jid=0, pri=0, data='', state = 'ok',**kw):
        if not conn and not DEFAULT_CONN:
            raise AttributeError("No connection specified")
        elif not conn:
            self._conn = DEFAULT_CONN
        else:
            self.conn = conn
        self.id = jid
        self.priority = pri
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
        self.Finish()

    def __str__(self):
        return self._serialize()

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
        self.conn.put(self._serialize(), self.priority, self.delay)
        if oldtube != self.tube:
            self.conn.use(oldtube)


    @honorimmutable
    def Return(self):
        try:
            self.conn.release(self.id, self.priority, 0)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Delay(self, delay):
        try:
            self.conn.release(self.id, self.priority, delay)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @honorimmutable
    def Finish(self):
        try:
            self.conn.delete(self.id)
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
            self.conn.bury(self.id, newpri)
        except errors.NotFound:
            return False
        except:
            raise
        else:
            return True

    @property
    def Info(self):
        try:
            stats=self.conn.stats_job(self.id)
        except:
            raise
        else:
            return stats

def newJob(**kw):
    kw['from_queue'] = False
    return Job(**kw)

