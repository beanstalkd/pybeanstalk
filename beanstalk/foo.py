class foo(object):
    class regfunc(object):
        def __get__(self, obj, typ=None):
            def fake(stuff):
                return obj._val(obj, stuff)
            return lambda stuff: obj._val(obj, stuff)

        def __set__(self, obj, val):
            obj._val = val

    command = regfunc()
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.__d = ''
        self.data_expected = False

    def register_command(self, func):
        self.command = func
        self._current = self.command

    def handle_data(self, d):
        self.__d += str(d)

    def _error(self, arg = ''):
        raise Exception(arg)

    def __call__(self, text):
        if not self._current:
            self._error('foo')
        ret = self._current(text)
        if self.data_expected:
            self._current = self.handle_data
        else:
            self._current = self._error
        return ret

    def print_data(self):
        print self.__d

def xcommand(self, stuff):
    print 'xcommand got: %s' % stuff
    self.data_expected = True

def ycommand(self, stuff):
    print 'ycommand got: %s' % stuff

x = foo('a','b')
y = foo('c','d')
print 'registering'
x.register_command(xcommand)
y.register_command(ycommand)
print 'calling 1st time'
x('things')
y('things')
print 'calling 2nd time'
x('datahere!')
x('moredata!')
x.print_data()
y('foo')
