import yaml
from protohandler import FailureError

DEFALUT_CONN = None
class Job(object):
    ''' Job class is an optional class. The intent is for program specific
    classes to have this as a base class, to handle processing as the
    deveolper sees fit. Essentially it a convenience class.

    The reccomended way of using this class is to pass it into the a
    connection constructor, so job getting commands (reserve and peek)
    will return a Job instead of a dict. Also, this will wrap the connection,
    so that you can have the job do its own commands (e.g. jobinst.release())

    A reccomended way for transforming beanstalk data into application data is
    to make the data attribute a property, and have the _set_data() function
    do all the relevant transforms. This is a fairly elegant use of properties,
    avoiding the need to override the __init__ function.

    Also note, the _serialize and _unserialize methods are basic YAML
    representations of data objects. It may be in your best interst to change
    this. The pyYAML documentation is very good, and there is much you can
    do to make this transformation work best for you.
    See: http://pyyaml.org for more information.
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

