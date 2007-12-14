def foo(val):
    def func():
        print "my val is %s" % func.val
    func.val = val
    return func

x = foo('a')
y = foo('b')

print "calling x, val should be a:"
x()
print "calling y, val should be b:"
y()
